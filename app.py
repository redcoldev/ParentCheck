import os
import json
import csv
from datetime import datetime
from functools import wraps

from flask import (
    Flask, render_template, request, redirect,
    url_for, flash, session
)
from flask_login import (
    LoginManager, UserMixin, login_user,
    logout_user, login_required, current_user
)
from openpyxl import load_workbook
import psycopg
import requests
import bcrypt

from sanctions_dataset import SANCTIONS_DATA   # your scrolling lists


# =====================================================================
#   FLASK APP CONFIG
# =====================================================================

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "changeme")

DB_URL = os.environ.get("DATABASE_URL")
if DB_URL is None:
    raise RuntimeError("DATABASE_URL is required")
DB_URL = DB_URL.replace("postgres://", "postgresql://")


# =====================================================================
#   LOGIN SYSTEM
# =====================================================================

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"


class User(UserMixin):
    def __init__(self, id_, username, password_hash, is_admin):
        self.id = id_
        self.username = username
        self.password_hash = password_hash
        self.is_admin = is_admin


@login_manager.user_loader
def load_user(user_id):
    conn = psycopg.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("SELECT id, username, password_hash, is_admin FROM users WHERE id=%s", (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if row:
        return User(*row)
    return None


# =====================================================================
#   DB HELPERS
# =====================================================================

def get_db():
    conn = psycopg.connect(DB_URL)
    return conn, conn.cursor()


def admin_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            return redirect(url_for("login"))
        return view(*args, **kwargs)
    return wrapped


# =====================================================================
#   UNIVERSAL FILE LOADER (CSV + XLSX)
# =====================================================================

def load_uploaded_file(file):
    name = file.filename.lower()

    # CSV
    if name.endswith(".csv"):
        decoded = file.read().decode("utf-8", errors="ignore").splitlines()
        rows = list(csv.reader(decoded))
        return rows

    # XLSX
    if name.endswith(".xlsx"):
        wb = load_workbook(file, read_only=True)
        ws = wb.active
        rows = []
        for row in ws.iter_rows(values_only=True):
            rows.append([str(c).strip() if c else "" for c in row])
        return rows

    raise ValueError("Invalid file: must be CSV or XLSX")


# =====================================================================
#   CLEAN + MAP ROWS TO OUR INTERNAL FORMAT
# =====================================================================

def normalise_rows(rows):
    if not rows:
        return []

    # Remove header row if it has letters
    if any(char.isalpha() for char in "".join(rows[0])):
        rows = rows[1:]

    # Remove fully blank rows
    rows = [r for r in rows if any(cell.strip() for cell in r)]

    # Ensure at least 4 columns
    clean = []
    for r in rows:
        while len(r) < 4:
            r.append("")
        clean.append({
            "first_name": r[0].strip(),
            "last_name": r[1].strip(),
            "citizenship": r[2].strip(),
            "dob": r[3].strip(),
        })

    return clean


# =====================================================================
#   ROUTE: LOGIN
# =====================================================================

@app.route("/", methods=["GET"])
def index():
    if not current_user.is_authenticated:
        return redirect(url_for("login"))
    return redirect(url_for("dashboard"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        conn, cur = get_db()
        cur.execute("SELECT id, username, password_hash, is_admin FROM users WHERE username=%s", (username,))
        row = cur.fetchone()

        if not row:
            flash("Invalid login")
            return render_template("login.html")

        uid, usern, pw_hash, is_admin = row
        if bcrypt.checkpw(password.encode(), pw_hash.encode()):
            login_user(User(uid, usern, pw_hash, is_admin))
            return redirect(url_for("dashboard"))

        flash("Invalid login")
    return render_template("login.html")


@app.route("/logout")
def logout():
    logout_user()
    return redirect(url_for("login"))


# =====================================================================
#   ROUTE: ADMIN REGISTER
# =====================================================================

@app.route("/admin/register", methods=["GET", "POST"])
@admin_required
def admin_register():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]

        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

        conn, cur = get_db()
        cur.execute(
            "INSERT INTO users (username, password_hash, is_admin) VALUES (%s, %s, TRUE)",
            (username, hashed)
        )
        conn.commit()
        cur.close()
        conn.close()

        flash("Admin created")
        return redirect(url_for("dashboard"))

    return render_template("admin/register.html")


# =====================================================================
#   ROUTE: DASHBOARD
# =====================================================================

@app.route("/dashboard")
@login_required
def dashboard():
    conn, cur = get_db()
    cur.execute("SELECT id, filename, uploaded_at FROM batches WHERE user_id=%s ORDER BY uploaded_at DESC",
                (current_user.id,))
    batches = cur.fetchall()
    cur.close()
    conn.close()

    return render_template("dashboard.html", batches=batches)


# =====================================================================
#   ROUTE: UPLOAD
# =====================================================================

@app.route("/upload", methods=["GET", "POST"])
@login_required
def upload():
    if request.method == "POST":
        file = request.files.get("file")
        if not file:
            flash("Upload a CSV or XLSX file")
            return render_template("upload.html")

        try:
            rows_raw = load_uploaded_file(file)
        except Exception:
            flash("Invalid file")
            return render_template("upload.html")

        rows_clean = normalise_rows(rows_raw)

        # save batch
        conn, cur = get_db()
        cur.execute(
            "INSERT INTO batches (user_id, filename, preview_data, total_rows) "
            "VALUES (%s, %s, %s, %s) RETURNING id",
            (current_user.id, file.filename, json.dumps(rows_clean[:10]), len(rows_clean))
        )
        batch_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()

        # TEMP store full rows for processing
        session[f"batch_{batch_id}_rows"] = rows_clean

        return redirect(url_for("preview", batch_id=batch_id))

    return render_template("upload.html")


# =====================================================================
#   ROUTE: PREVIEW
# =====================================================================

@app.route("/preview/<int:batch_id>")
@login_required
def preview(batch_id):
    conn, cur = get_db()
    cur.execute("SELECT filename, preview_data, total_rows FROM batches WHERE id=%s", (batch_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        flash("Batch not found")
        return redirect(url_for("dashboard"))

    filename, preview_rows, total = row

    return render_template(
        "preview.html",
        filename=filename,
        preview=json.loads(preview_rows),
        total=total,
        batch_id=batch_id
    )


# =====================================================================
#   ROUTE: PROCESSING (slot machine)
# =====================================================================

@app.route("/processing/<int:batch_id>")
@login_required
def processing(batch_id):
    rows = session.get(f"batch_{batch_id}_rows", [])
    total_rows = len(rows)

    delay = max(30, min(90, int(total_rows * 0.06)))

    return render_template(
        "processing.html",
        dataset_json=json.dumps(SANCTIONS_DATA),
        delay_seconds=delay,
        batch_id=batch_id
    )


# =====================================================================
#   MATCH ENGINE
# =====================================================================

def process_batch(batch_id, rows):
    results = []

    for r in rows:
        payload = {
            "queries": [
                {"string": f"{r['first_name']} {r['last_name']}"}
            ]
        }
        try:
            resp = requests.post("https://api.opensanctions.org/match/default", json=payload)
            data = resp.json()
        except Exception:
            data = {"error": True}

        results.append({
            "first": r["first_name"],
            "last": r["last_name"],
            "cit": r["citizenship"],
            "dob": r["dob"],
            "match": data
        })

    # store in DB
    conn, cur = get_db()
    for res in results:
        cur.execute(
            "INSERT INTO results (batch_id, first_name, last_name, citizenship, dob, raw_json) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (batch_id, res["first"], res["last"], res["cit"], res["dob"], json.dumps(res["match"]))
        )
    conn.commit()
    cur.close()
    conn.close()


# =====================================================================
#   ROUTE: AFTER PROCESSING — RUN MATCH + REDIRECT
# =====================================================================

@app.route("/finish/<int:batch_id>")
@login_required
def finish(batch_id):
    rows = session.get(f"batch_{batch_id}_rows", [])
    process_batch(batch_id, rows)
    return redirect(url_for("results", batch_id=batch_id))


# =====================================================================
#   ROUTE: RESULTS
# =====================================================================

@app.route("/results/<int:batch_id>")
@login_required
def results(batch_id):
    conn, cur = get_db()
    cur.execute(
        "SELECT first_name, last_name, citizenship, dob, raw_json "
        "FROM results WHERE batch_id=%s",
        (batch_id,)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return render_template("results.html", rows=rows, batch_id=batch_id)


# =====================================================================
#   DATABASE INIT + PATCH
# =====================================================================

@app.cli.command("init-db")
def init_db_command():
    conn, cur = get_db()

    # USERS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(100) UNIQUE NOT NULL,
            password_hash VARCHAR(200) NOT NULL,
            is_admin BOOLEAN DEFAULT FALSE
        )
    """)

    # BATCHES
    cur.execute("""
        CREATE TABLE IF NOT EXISTS batches (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            filename VARCHAR(255) NOT NULL,
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            preview_data JSONB,
            total_rows INTEGER
        )
    """)

    # RESULTS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS results (
            id SERIAL PRIMARY KEY,
            batch_id INTEGER REFERENCES batches(id) ON DELETE CASCADE,
            first_name TEXT,
            last_name TEXT,
            citizenship TEXT,
            dob TEXT,
            raw_json JSONB
        )
    """)

    conn.commit()
    cur.close()
    conn.close()

    print("Database initialized.")


# =====================================================================
#   RUN
# =====================================================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
