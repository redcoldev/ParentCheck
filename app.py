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

from sanctions_dataset import SANCTIONS_DATA

# =====================================================================
#   FLASK CONFIG
# =====================================================================

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "changeme")

DB_URL = os.environ.get("DATABASE_URL")
OPEN_SANCTIONS_KEY = os.environ.get("OPEN_SANCTIONS_KEY")

if not DB_URL:
    raise RuntimeError("DATABASE_URL missing")
DB_URL = DB_URL.replace("postgres://", "postgresql://")

# =====================================================================
#   LOGIN
# =====================================================================

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"


class User(UserMixin):
    def __init__(self, id_, email, school_name):
        self.id = id_
        self.email = email
        self.school_name = school_name


@login_manager.user_loader
def load_user(user_id):
    conn = psycopg.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("SELECT id, email, school_name FROM users WHERE id=%s", (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    return User(*row) if row else None

# =====================================================================
#   DB HELPERS
# =====================================================================

def get_db():
    conn = psycopg.connect(DB_URL)
    return conn, conn.cursor()

# =====================================================================
#   FILE LOADER (CSV + XLSX)
# =====================================================================

def load_uploaded_file(file):
    name = file.filename.lower()

    if name.endswith(".csv"):
        data = file.read().decode("utf-8", errors="ignore").splitlines()
        return list(csv.reader(data))

    if name.endswith(".xlsx"):
        wb = load_workbook(file, read_only=True)
        ws = wb.active
        return [[str(c or "").strip() for c in row] for row in ws.iter_rows(values_only=True)]

    raise ValueError("Invalid file")

# =====================================================================
#   NORMALISE ROWS
# =====================================================================

def normalise_rows(rows):
    if not rows:
        return []

    if any(ch.isalpha() for ch in "".join(rows[0])):
        rows = rows[1:]

    rows = [r for r in rows if any(cell.strip() for cell in r)]

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
#   ROUTES
# =====================================================================

@app.route("/")
def index():
    if not current_user.is_authenticated:
        return redirect("/login")
    return redirect("/dashboard")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form.get("email","").strip()
        password = request.form.get("password","").strip()

        conn, cur = get_db()
        cur.execute("SELECT id, email, password, school_name FROM users WHERE email=%s", (email,))
        user = cur.fetchone()
        cur.close()
        conn.close()

        if not user:
            flash("Invalid credentials", "danger")
            return redirect("/login")

        uid, uemail, stored_pw, school = user

        if stored_pw != password:
            flash("Invalid credentials", "danger")
            return redirect("/login")

        login_user(User(uid, uemail, school))
        return redirect("/dashboard")

    return render_template("login.html")


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


@app.route("/upload", methods=["GET","POST"])
@login_required
def upload():
    if request.method == "POST":
        file = request.files.get("file")
        if not file:
            flash("Upload a file")
            return render_template("upload.html")

        try:
            rows_raw = load_uploaded_file(file)
        except:
            flash("Invalid file")
            return render_template("upload.html")

        rows_clean = normalise_rows(rows_raw)

        conn, cur = get_db()
        cur.execute("""
            INSERT INTO batches (user_id, filename, preview_data, total_rows)
            VALUES (%s,%s,%s,%s)
            RETURNING id
        """, (current_user.id, file.filename, json.dumps(rows_clean[:10]), len(rows_clean)))
        batch_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()

        session[f"batch_{batch_id}_rows"] = rows_clean

        return redirect(f"/preview/{batch_id}")

    return render_template("upload.html")


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
        return redirect("/dashboard")

    filename, preview_data, total = row

    return render_template(
        "preview.html",
        filename=filename,
      preview=preview_data,

        total=total,
        batch_id=batch_id
    )


@app.route("/processing/<int:batch_id>")
@login_required
def processing(batch_id):
    rows = session.get(f"batch_{batch_id}_rows", [])
    total = len(rows)
    delay = max(30, min(90, int(total * 0.06)))

    return render_template(
        "processing.html",
        dataset_json=json.dumps(SANCTIONS_DATA),
        delay_seconds=delay,
        batch_id=batch_id
    )


def process_batch(batch_id, rows):
    API_KEY = os.environ.get("OPEN_SANCTIONS_KEY")
    headers = {
        "Authorization": f"Apikey {API_KEY}",
        "Content-Type": "application/json"
    }

    def dob_matches(user_dob, os_birth_dates):
        """DOB RULE:
        - User always gives full DOB (DD/MM/YYYY or YYYY-MM-DD)
        - OS may have: full DOB OR year-only
        - Match if year matches AND:
           * if OS has full DOB → full date must match exactly
           * if OS has only year → accept
        """

        if not user_dob:
            return True

        # Extract user year/month/day from any format
        digits = ''.join(ch for ch in user_dob if ch.isdigit())
        if len(digits) != 8:
            return False

        user_day = digits[0:2]
        user_month = digits[2:4]
        user_year = digits[4:8]

        for bd in os_birth_dates:
            if not bd or len(bd) < 4:
                continue

            os_year = bd[:4]
            os_month = bd[5:7] if len(bd) >= 7 else None
            os_day = bd[8:10] if len(bd) >= 10 else None

            # YEAR MUST MATCH
            if os_year != user_year:
                continue

            os_has_full = (
                os_month not in (None, "00") and
                os_day not in (None, "00")
            )

            if not os_has_full:
                # OS only has year → year match is enough
                return True

            # OS has full date → must match exactly
            if os_day == user_day and os_month == user_month:
                return True

        return False

    def matches_citizenship(user_country, props):
        if not user_country:
            return True

        nat = props.get("nationality", []) + props.get("citizenship", [])
        nat = [x.lower() for x in nat]

        return user_country.lower() in nat

    results_to_store = []

    # -------------------------------------------------------
    # PROCESS EACH ROW
    # -------------------------------------------------------
    for idx, r in enumerate(rows):
        query_id = f"row{idx}"

        properties = {
            "firstName": [r["first_name"]],
            "lastName": [r["last_name"]]
        }

        if r.get("dob"):
            properties["birthDate"] = [r["dob"]]

        if r.get("citizenship"):
            properties["country"] = [r["citizenship"]]

        payload = {
            "queries": {
                query_id: {
                    "schema": "Person",
                    "properties": properties
                }
            }
        }

        # Call OpenSanctions
        try:
            resp = requests.post(
                "https://api.opensanctions.org/match/default",
                headers=headers,
                json=payload,
                timeout=10
            )
            match_data = resp.json()
        except Exception as e:
            match_data = {"error": str(e)}

        os_results = match_data.get("responses", {}).get(query_id, {}).get("results", [])

        # -------------------------------------------------------
        # APPLY STRICT MATCHING RULES
        # -------------------------------------------------------
        true_matches = []

        for m in os_results:
            score = m.get("score", 0)
            if score < 0.75:
                continue

            props = m.get("properties", {})

            # citizenship rule
            if not matches_citizenship(r.get("citizenship"), props):
                continue

            # DOB rule
            if not dob_matches(r.get("dob"), props.get("birthDate", [])):
                continue

            # Passed all filters → REAL MATCH
            true_matches.append(m)

        # RISK ASSESSMENT
        risk = "High" if true_matches else "Clear"

        # Store final result row
        results_to_store.append((
            batch_id,
            r["first_name"],
            r["last_name"],
            r.get("citizenship"),
            r.get("dob"),
            risk,
            json.dumps(true_matches)  # Only TRUE matches, not whole API JSON
        ))

    # Save into DB
    conn, cur = get_db()
    for res in results_to_store:
        cur.execute("""
            INSERT INTO results (batch_id, first_name, last_name, citizenship, dob, risk_level, raw_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, res)
    conn.commit()
    cur.close()
    conn.close()





@app.route("/finish/<int:batch_id>")
@login_required
def finish(batch_id):
    rows = session.get(f"batch_{batch_id}_rows", [])
    process_batch(batch_id, rows)
    return redirect(f"/results/{batch_id}")


@app.route("/results/<int:batch_id>")
def results(batch_id):
    conn, cur = get_db()
    cur.execute("""
        SELECT first_name, last_name, dob, citizenship, risk_level
        FROM results
        WHERE batch_id = %s
        ORDER BY id ASC
    """, (batch_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return render_template("results.html", rows=rows, batch_id=batch_id)



# =====================================================================
#   LOCAL RUN
# =====================================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
