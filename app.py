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

# =====================================================================
# CONFIG
# =====================================================================

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "changeme")

DB_URL = os.environ.get("DATABASE_URL")
OPEN_SANCTIONS_KEY = os.environ.get("OPEN_SANCTIONS_KEY")

if not DB_URL:
    raise RuntimeError("DATABASE_URL missing")

DB_URL = DB_URL.replace("postgres://", "postgresql://")


# =====================================================================
# LOGIN
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
# DB HELPERS
# =====================================================================

def get_db():
    conn = psycopg.connect(DB_URL)
    return conn, conn.cursor()


# =====================================================================
# FILE LOADER (CSV + XLSX)
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
# NORMALISE ROWS
# Expecting: first_name, last_name, citizenship, dob
# =====================================================================

def normalise_rows(rows):
    if not rows:
        return []

    # Remove header row if alphabetical values exist in row 0
    if any(ch.isalpha() for ch in "".join(rows[0])):
        rows = rows[1:]

    # Remove completely empty rows
    rows = [r for r in rows if any(cell.strip() for cell in r)]

    clean = []
    for r in rows:
        # pad row up to 4 columns
        while len(r) < 4:
            r.append("")

        clean.append({
            "first_name": r[0].strip(),
            "last_name": r[1].strip(),
            "country_of_citizenship": r[2].strip(),   # ✔ clean naming
            "dob": r[3].strip(),
        })

    return clean


# =====================================================================
# ROUTES
# =====================================================================

@app.route("/")
def index():
    if not current_user.is_authenticated:
        return redirect("/login")
    return redirect("/dashboard")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "").strip()

        conn, cur = get_db()
        cur.execute("""
            SELECT id, email, password, school_name
            FROM users WHERE email=%s
        """, (email,))
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
    cur.execute("""
        SELECT id, filename, uploaded_at
        FROM batches
        WHERE user_id=%s
        ORDER BY uploaded_at DESC
    """, (current_user.id,))
    batches = cur.fetchall()
    cur.close()
    conn.close()

    return render_template("dashboard.html", batches=batches)


@app.route("/upload", methods=["GET", "POST"])
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
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """, (
            current_user.id,
            file.filename,
            json.dumps(rows_clean[:10]),
            len(rows_clean)
        ))
        batch_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()

        # Store rows in session for further processing
        session[f"batch_{batch_id}_rows"] = rows_clean

        return redirect(f"/preview/{batch_id}")

    return render_template("upload.html")


@app.route("/preview/<int:batch_id>")
@login_required
def preview(batch_id):
    conn, cur = get_db()
    cur.execute("""
        SELECT filename, preview_data, total_rows
        FROM batches WHERE id=%s
    """, (batch_id,))
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
    return render_template(
        "processing.html",
        rows_json=json.dumps(rows),
        batch_id=batch_id
    )



# =====================================================================
# PROCESSING PAGE 
# =====================================================================

@app.post("/api/screen")
@login_required
def api_screen():
    row = request.get_json()

    # -----------------------------
    # NORMALISE DOB → YYYY-MM-DD
    # -----------------------------
    def normalize_dob(d):
        if not d:
            return None
        d = d.replace("/", "-").strip()

        parts = d.split("-")
        if len(parts) == 3 and len(parts[0]) == 2 and len(parts[2]) == 4:
            return f"{parts[2]}-{parts[1]}-{parts[0]}"

        if len(parts[0]) == 4:
            return d

        return None

    norm_dob = normalize_dob(row.get("dob", ""))

    # -----------------------------
    # NORMALISE COUNTRY
    # -----------------------------
    def normalize_country(c):
        if not c:
            return None
        c = c.strip().lower()
        mapping = {
            "russia": "russian federation",
            "ru": "russian federation",
            "china": "china",
            "prc": "china",
            "united kingdom": "united kingdom",
            "uk": "united kingdom",
            "britain": "united kingdom"
        }
        return mapping.get(c, c)

    norm_country = normalize_country(row.get("country_of_citizenship", ""))

    first = row["first_name"].strip().lower()
    last = row["last_name"].strip().lower()

    API_KEY = os.environ.get("OPEN_SANCTIONS_KEY")
    headers = {"Authorization": f"ApiKey {API_KEY}"}

    url = "https://api.opensanctions.org/match/sanctions"

    query_properties = {
        "firstName": [row["first_name"]],
        "lastName": [row["last_name"]],
    }

    if norm_dob:
        query_properties["birthDate"] = [norm_dob]
    if norm_country:
        query_properties["citizenship"] = [norm_country]

    payload = {
        "queries": {
            "q": {
                "schema": "Person",
                "properties": query_properties
            }
        }
    }

    # -----------------------------
    # CALL OS API
    # -----------------------------
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        data = r.json()
        results = data.get("responses", {}).get("q", {}).get("results", [])
    except Exception as e:
        return {"risk": "Error", "summary": str(e)}, 200

    # -----------------------------
    # MATCH LOGIC
    # -----------------------------
    matched = None

    for m in results:
        score = m.get("score", 0)
        if score < 0.90:
            continue

        props = m.get("properties", {})
        os_first = props.get("firstName", [""])[0].lower()
        os_last = props.get("lastName", [""])[0].lower()

        if os_first != first or os_last != last:
            continue

        if norm_dob:
            os_dobs = props.get("birthDate", [])
            normalized = []
            for dob in os_dobs:
                dob = dob.replace("/", "-")
                if len(dob) == 8 and dob.isdigit():
                    dob = f"{dob[0:4]}-{dob[4:6]}-{dob[6:8]}"
                normalized.append(dob)
            if norm_dob not in normalized:
                continue

        if norm_country:
            os_countries = [
                normalize_country(c)
                for c in props.get("citizenship", []) + props.get("nationality", [])
            ]
            if norm_country not in os_countries:
                continue

        matched = m
        break

    # -----------------------------
    # NO MATCH CASE
    # -----------------------------
    if not matched:
        return {
            "risk": "Clear",
            "summary": "No matches",
            "debug": {
                "input_first": first,
                "input_last": last,
                "input_dob": norm_dob,
                "input_country": norm_country,
                "raw_results": results
            }
        }, 200

    # -----------------------------
    # FETCH FULL ENTITY DETAILS
    # -----------------------------
    entity_id = matched.get("id")
    detail_url = f"https://api.opensanctions.org/entities/{entity_id}"

    try:
        full = requests.get(detail_url, headers=headers, timeout=10).json()
    except Exception as e:
        full = {}

    # -----------------------------
    # MATCH FOUND CASE
    # -----------------------------
    return {
        "risk": "High",
        "summary": f"{matched.get('caption')} (score {matched.get('score')})",
        "datasets": full.get("datasets", []),
        "aliases": full.get("aliases", []),
        "birth_date": full.get("properties", {}).get("birthDate", [None])[0],
        "birth_place": full.get("properties", {}).get("birthPlace", [None])[0],
        "topics": full.get("topics", []),
        "profile": full.get("profile", ""),
        "debug": {
            "score": matched.get("score"),
            "matched_first": matched.get("properties", {}).get("firstName"),
            "matched_last": matched.get("properties", {}).get("lastName"),
            "matched_birthDate": matched.get("properties", {}).get("birthDate"),
            "matched_citizenships": matched.get("properties", {}).get("citizenship"),
            "raw_results": results
        }
    }, 200



# =====================================================================
# MAIN BATCH PROCESSOR — CALLS OPENSANCTIONS
# =====================================================================

def process_batch(batch_id, rows):
    API_KEY = os.environ.get("OPEN_SANCTIONS_KEY")

    headers = {
        "Authorization": f"ApiKey {API_KEY}",
        "Content-Type": "application/json",
    }

    def dob_matches(user_dob, os_birth_dates):
        """
        DOB RULE:
        - User gives full DOB (DD/MM/YYYY or YYYY-MM-DD)
        - OS may give full DOB OR year-only
        - Accept match if year matches AND:
             * OS has only year → accept
             * OS has full DOB → must match exactly
        """
        if not user_dob:
            return True

        # Normalise user DOB to digits only
        digits = ''.join(ch for ch in user_dob if ch.isdigit())
        if len(digits) != 8:
            return False

        user_day = digits[0:2]
        user_month = digits[2:4]
        user_year = digits[4:8]

        for bd in os_birth_dates:
            if not bd:
                continue

            if len(bd) < 4:
                continue

            os_year = bd[:4]
            os_month = bd[5:7] if len(bd) >= 7 else None
            os_day = bd[8:10] if len(bd) >= 10 else None

            # Year must match
            if os_year != user_year:
                continue

            # If OS only gives year, accept
            if os_month in (None, "00") or os_day in (None, "00"):
                return True

            # Full DOB match
            if os_day == user_day and os_month == user_month:
                return True

        return False

    def citizenship_matches(user_country, props):
        if not user_country:
            return True

        nat = props.get("nationality", []) + props.get("citizenship", [])
        nat = [x.lower() for x in nat]

        return user_country.lower() in nat

    # Store results for DB write
    batch_results = []

    # ================================================================
    # PROCESS EACH ROW
    # ================================================================
    for idx, r in enumerate(rows):
        query_id = f"row{idx}"

        properties = {
            "firstName": [r["first_name"]],
            "lastName": [r["last_name"]],
        }

        if r.get("dob"):
            properties["birthDate"] = [r["dob"]]

        if r.get("country_of_citizenship"):
            properties["country"] = [r["country_of_citizenship"]]

        payload = {
            "queries": {
                query_id: {
                    "schema": "Person",
                    "properties": properties
                }
            }
        }

        # Perform OS API request
        try:
            resp = requests.post(
                "https://api.opensanctions.org/match/sanctions",
                headers=headers,
                json=payload,
                timeout=12
            )
            os_json = resp.json()
        except Exception as e:
            os_json = {"error": str(e)}

        results_raw = os_json.get("responses", {}).get(query_id, {}).get("results", [])

        # Apply rules to determine TRUE matches
        true_matches = []

        for m in results_raw:
            score = m.get("score", 0)

            # Score threshold
            if score < 0.75:
                continue

            props = m.get("properties", {})

            # Citizenship test
            if not citizenship_matches(r.get("country_of_citizenship"), props):
                continue

            # DOB test
            if not dob_matches(r.get("dob"), props.get("birthDate", [])):
                continue

            true_matches.append(m)

        risk = "High" if true_matches else "Clear"

        batch_results.append({
            "batch_id": batch_id,
            "first_name": r["first_name"],
            "last_name": r["last_name"],
            "dob": r["dob"],
            "country": r["country_of_citizenship"],
            "risk_level": risk,
            "match_data": true_matches,    # jsonb
            "raw_json": results_raw        # store full API response raw if needed
        })

    # ================================================================
    # WRITE RESULTS TO DATABASE
    # ================================================================
    conn, cur = get_db()

    for row in batch_results:
        cur.execute("""
            INSERT INTO results
                (batch_id, first_name, last_name, dob, country_of_citizenship,
                 risk_level, match_data, raw_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            row["batch_id"],
            row["first_name"],
            row["last_name"],
            row["dob"],
            row["country"],
            row["risk_level"],
            json.dumps(row["match_data"]),
            json.dumps(row["raw_json"]),
        ))

    conn.commit()
    cur.close()
    conn.close()


# =====================================================================
# FINISH ROUTE — executes the processing
# =====================================================================

@app.route("/finish/<int:batch_id>")
@login_required
def finish(batch_id):
    rows = session.get(f"batch_{batch_id}_rows", [])

    process_batch(batch_id, rows)   # Run OS checks

    return redirect(f"/results/{batch_id}")


# =====================================================================
# RESULTS PAGE
# =====================================================================

@app.route("/results/<int:batch_id>")
@login_required
def results(batch_id):
    return redirect("/dashboard")




# =====================================================================
# OPTIONAL DEBUG ROUTE
# =====================================================================

@app.route("/processing_debug/<int:batch_id>")
def processing_debug(batch_id):
    # Placeholder for logs or debugging instrumentation
    # (Empty for now unless you want live debug output.)
    return {"log": "processing…"}
