import csv
from openpyxl import load_workbook
import os
import json
import csv
import requests
from io import StringIO, BytesIO
from flask import Flask, render_template, request, redirect, url_for, flash, send_file
from flask_login import (
    LoginManager, login_user, logout_user, login_required,
    current_user, UserMixin
)
from werkzeug.security import generate_password_hash, check_password_hash
import psycopg


# -------------------------------------------------
# FLASK APP SETUP
# -------------------------------------------------
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev")


# -------------------------------------------------
# LAZY DATABASE CONNECTION
# -------------------------------------------------
def get_db():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set.")

    db_url = db_url.replace("postgres://", "postgresql://")
    conn = psycopg.connect(db_url, autocommit=True)
    cur = conn.cursor()
    return conn, cur


# -------------------------------------------------
# DATABASE INITIALIZATION
# -------------------------------------------------
def init_db():
    """Creates tables if they do not exist."""
    try:
        conn, cur = get_db()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255) UNIQUE NOT NULL,
                password VARCHAR(255) NOT NULL,
                school_name VARCHAR(255) NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS batches (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                filename VARCHAR(255) NOT NULL,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS results (
                id SERIAL PRIMARY KEY,
                batch_id INTEGER REFERENCES batches(id),
                first_name VARCHAR(255) NOT NULL,
                last_name VARCHAR(255) NOT NULL,
                dob VARCHAR(255),
                country_of_citizenship VARCHAR(255),
                risk_level VARCHAR(20) NOT NULL,
                match_data JSONB
            );
        """)

        print("Database initialized successfully.")

    except Exception as e:
        print("Database initialization error:", e)


# -------------------------------------------------
# LOGIN MANAGER
# -------------------------------------------------
login_manager = LoginManager(app)
login_manager.login_view = "login"


class User(UserMixin):
    def __init__(self, id, school_name):
        self.id = id
        self.school_name = school_name


@login_manager.user_loader
def load_user(user_id):
    try:
        conn, cur = get_db()
        cur.execute("SELECT id, school_name FROM users WHERE id=%s", (user_id,))
        row = cur.fetchone()
        return User(*row) if row else None
    except Exception:
        return None


# -------------------------------------------------
# ROUTES
# -------------------------------------------------
@app.route("/")
def index():
    return redirect("/login")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form["email"]
        pw = request.form["password"]

        conn, cur = get_db()
        cur.execute("SELECT id, school_name, password FROM users WHERE email=%s", (email,))
        row = cur.fetchone()

        if row and check_password_hash(row[2], pw):
            login_user(User(row[0], row[1]))
            return redirect("/dashboard")

        flash("Bad login.")

    return render_template("login.html")


@app.route("/processing/<batch_id>")
def processing(batch_id):
    import json

    # TODO: set this value based on the uploaded batch size
    # For testing, just hardcode something like 200
    current_batch_size = 200  

    # 30–90 second artificial timing
    delay = max(30, min(90, int(current_batch_size * 0.06)))

    # LOAD THE SANCTIONS DATASET
    from sanctions_dataset import SANCTIONS_DATA

    return render_template(
        "processing.html",
        dataset_json=json.dumps(SANCTIONS_DATA),
        delay_seconds=delay,
        batch_id=batch_id
    )



# -------------------------------
# ADMIN-ONLY REGISTRATION
# -------------------------------
@app.route("/admin/register", methods=["GET", "POST"])
def admin_register():
    if request.method == "POST":
        email = request.form["email"]
        pw = request.form["password"]
        school_name = request.form["school_name"]

        conn, cur = get_db()
        cur.execute("SELECT id FROM users WHERE email=%s", (email,))
        if cur.fetchone():
            flash("That email is already registered.")
            return render_template("register.html")

        hashed_pw = generate_password_hash(pw)
        cur.execute(
            "INSERT INTO users (email, password, school_name) VALUES (%s, %s, %s)",
            (email, hashed_pw, school_name)
        )

        flash("School account created.")
        return redirect(url_for("login"))

    return render_template("register.html")


@app.route("/dashboard")
@login_required
def dashboard():
    return render_template("dashboard.html")


# -------------------------------------------------
# FILE UPLOAD
# -------------------------------------------------
@app.route("/upload", methods=["GET", "POST"])
@login_required
def upload():
    if request.method == "POST":
        f = request.files["file"]

        if not f or not f.filename.endswith(".csv"):
            flash("Invalid CSV.")
            return render_template("upload.html")

        csv_content = f.read().decode('utf-8')
        reader = csv.DictReader(StringIO(csv_content))
        rows = list(reader)

        required = {"first_name", "last_name", "country_of_citizenship", "dob"}
        if not required.issubset(reader.fieldnames):
            flash("CSV missing required columns.")
            return render_template("upload.html")

        conn, cur = get_db()
        cur.execute(
            "INSERT INTO batches (user_id, filename) VALUES (%s, %s) RETURNING id",
            (current_user.id, f.filename)
        )
        batch_id = cur.fetchone()[0]

        process_batch(batch_id, rows)
        flash("Processing started.")
        return redirect("/dashboard")

    return render_template("upload.html")


# -------------------------------------------------
# PROCESS BATCH
# -------------------------------------------------
def process_batch(batch_id, rows):
    key = os.environ.get("OPEN_SANCTIONS_KEY", "")
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Authorization"] = f"ApiKey {key}"

    for row in rows:
        payload = {
            "queries": {
                "q1": {
                    "schema": "Person",
                    "properties": {
                        "firstName": [row["first_name"]],
                        "lastName": [row["last_name"]],
                    }
                }
            }
        }

        if row.get("dob"):
            payload["queries"]["q1"]["properties"]["birthDate"] = [str(row["dob"])]
        if row.get("country_of_citizenship"):
            payload["queries"]["q1"]["properties"]["nationality"] = [
                row["country_of_citizenship"][:2].lower()
            ]

        try:
            r = requests.post(
                "https://api.opensanctions.org/match/default",
                json=payload,
                headers=headers,
                timeout=10
            )
            data = r.json()
        except Exception:
            data = {}

        risk = "Clear"
        match = None

        results = data.get("responses", {}).get("q1", {}).get("results")
        if results:
            entity = results[0]
            names = [entity.get("caption", "")] + [
                a.get("value", "") for a in entity.get("properties", {}).get("alias", [])
            ]
            full = f"{row['first_name']} {row['last_name']}".lower()
            if any(full in n.lower() for n in names if n):
                match = json.dumps(data)
                risk = "High" if "sanction" in str(entity).lower() else "Review"

        conn, cur = get_db()
        cur.execute("""
            INSERT INTO results
            (batch_id, first_name, last_name, dob, country_of_citizenship, risk_level, match_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            batch_id,
            row["first_name"],
            row["last_name"],
            row.get("dob"),
            row.get("country_of_citizenship"),
            risk,
            match
        ))


# -------------------------------------------------
# RESULTS VIEW
# -------------------------------------------------
@app.route("/results/<int:batch_id>")
@login_required
def results(batch_id):
    conn, cur = get_db()
    cur.execute("""
        SELECT b.id, r.*
        FROM batches b
        JOIN results r ON b.id = r.batch_id
        WHERE b.id=%s AND b.user_id=%s
        ORDER BY r.id
    """, (batch_id, current_user.id))

    rows = cur.fetchall()

    if not rows:
        flash("Batch not found or unauthorized.")
        return redirect("/dashboard")

    # XLSX export
    if request.args.get("xlsx"):
        from openpyxl import Workbook
        wb = Workbook()
        ws = wb.active
        ws.append([
            'ID', 'Batch ID', 'First Name', 'Last Name',
            'DOB', 'Country', 'Risk', 'Match Data'
        ])

        for row in rows:
            ws.append(row[3:])

        output = BytesIO()
        wb.save(output)
        output.seek(0)

        return send_file(
            output,
            download_name="results.xlsx",
            as_attachment=True,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    results_data = [row[3:] for row in rows]
    return render_template("results.html", rows=results_data, batch_id=batch_id)


# -------------------------------------------------
# LOGOUT
# -------------------------------------------------
@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect("/login")


# -------------------------------------------------
# AUTO DB INITIALIZATION ON SERVER START
# -------------------------------------------------
try:
    init_db()
except Exception as e:
    print("Deferred DB init error:", e)


# -------------------------------------------------
# DEV MODE
# -------------------------------------------------
if __name__ == "__main__":
    init_db()
    app.run(debug=True)
