from flask import Flask, render_template, request, redirect, url_for, flash, send_file
from flask_login import LoginManager, login_user, logout_user, login_required, current_user, UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
import psycopg
import requests
import json
import csv
from io import StringIO, BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
import os
import click # Required for Flask CLI command registration

app = Flask(__name__)
# IMPORTANT: Ensure SECRET_KEY is set in your Render environment variables!
app.secret_key = os.environ.get("SECRET_KEY", "dev")

# --- DATABASE CONNECTION ---
# This uses the DATABASE_URL environment variable provided by Render.
DB_URL = os.environ["DATABASE_URL"].replace("postgres://", "postgresql://")
conn = psycopg.connect(DB_URL, autocommit=True)
cur = conn.cursor()

# --- DATABASE INITIALIZATION FUNCTION ---
def init_db():
    """Creates the users, batches, and results tables if they do not exist."""
    print("Initializing database...")
    try:
        # 1. Create the users table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255) UNIQUE NOT NULL,
                password VARCHAR(255) NOT NULL,
                school_name VARCHAR(255) NOT NULL
            )
        """)
        # 2. Create the batches table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS batches (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                filename VARCHAR(255) NOT NULL,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # 3. Create the results table
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
            )
        """)
        print("Database tables ensured (users, batches, results).")
    except psycopg.Error as e:
        print(f"Database Initialization Error: {e}")

# --- FLASK CLI COMMAND TO INITIALIZE DB ---
# This registers a command that Render's start script will call.
@app.cli.command("init-db")
def init_db_command():
    """Create new tables. Run this on first deployment!"""
    init_db()
    click.echo('Initialized the database via Flask CLI.')


# --- LOGIN MANAGER SETUP ---
login_manager = LoginManager(app)
login_manager.login_view = "login"

class User(UserMixin):
    def __init__(self, id, school_name):
        self.id = id
        self.school_name = school_name

@login_manager.user_loader
def load_user(user_id):
    cur.execute("SELECT id, school_name FROM users WHERE id=%s", (user_id,))
    row = cur.fetchone()
    return User(*row) if row else None

@app.route("/")
def index():
    return redirect("/login")

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email, pw = request.form["email"], request.form["password"]
        cur.execute("SELECT id, school_name, password FROM users WHERE email=%s", (email,))
        row = cur.fetchone()
        
        if row and check_password_hash(row[2], pw):
            login_user(User(row[0], row[1]))
            return redirect("/dashboard")
        flash("Bad login")
    return render_template("login.html")

@app.route("/register", methods=["GET", "POST"])
def register():
    """Allows new schools to register accounts."""
    if request.method == "POST":
        email = request.form["email"]
        pw = request.form["password"]
        school_name = request.form["school_name"]

        # Check if user already exists
        cur.execute("SELECT id FROM users WHERE email=%s", (email,))
        if cur.fetchone():
            flash("That email is already registered.")
            return render_template("register.html")

        # Hash password and insert new user
        hashed_pw = generate_password_hash(pw)
        cur.execute("INSERT INTO users (email, password, school_name) VALUES (%s, %s, %s)",
                    (email, hashed_pw, school_name))
        flash("Registration successful. Please log in.")
        return redirect(url_for("login"))
        
    return render_template("register.html")

@app.route("/dashboard")
@login_required
def dashboard():
    return render_template("dashboard.html")

@app.route("/upload", methods=["GET", "POST"])
@login_required
def upload():
    if request.method == "POST":
        f = request.files["file"]
        if f and f.filename.endswith(".csv"):
            csv_content = f.read().decode('utf-8')
            reader = csv.DictReader(StringIO(csv_content))
            rows = list(reader)
            required = {"first_name", "last_name", "country_of_citizenship", "dob"}
            if required.issubset(reader.fieldnames):
                cur.execute("INSERT INTO batches (user_id, filename) VALUES (%s, %s) RETURNING id",
                            (current_user.id, f.filename))
                batch_id = cur.fetchone()[0]
                process_batch(batch_id, rows)
                flash("Processing started")
                return redirect("/dashboard")
        flash("Invalid CSV")
    return render_template("upload.html")

def process_batch(batch_id, rows):
    key = os.environ.get("OPEN_SANCTIONS_KEY", "")
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Authorization"] = f"ApiKey {key}"

    for row in rows:
        payload = {
            "queries": {"q1": {
                "schema": "Person",
                "properties": {
                    "firstName": [row["first_name"]],
                    "lastName": [row["last_name"]],
                }
            }}
        }
        if row.get("dob"):
            payload["queries"]["q1"]["properties"]["birthDate"] = [str(row["dob"])]
        if row.get("country_of_citizenship"):
            country_code = row["country_of_citizenship"][:2].lower()
            payload["queries"]["q1"]["properties"]["nationality"] = [country_code]

        try:
            r = requests.post("https://api.opensanctions.org/match/default", json=payload, headers=headers, timeout=10)
            r.raise_for_status() 
            data = r.json()
        except requests.RequestException as e:
            print(f"API Request failed: {e}")
            data = {} 

        risk = "Clear"
        match = None
        
        results = data.get("responses", {}).get("q1", {}).get("results")
        if results:
            entity = results[0]
            names = [entity.get("caption", "")] + [a.get("value", "") for a in entity.get("properties", {}).get("alias", [])]
            full = f"{row['first_name']} {row['last_name']}".lower()

            if any(full in n.lower() for n in names if n):
                match = json.dumps(data)
                risk = "High" if "sanction" in str(entity).lower() else "Review"

        cur.execute("""INSERT INTO results (batch_id, first_name, last_name, dob, country_of_citizenship, risk_level, match_data)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (batch_id, row["first_name"], row["last_name"], row.get("dob"), row.get("country_of_citizenship"), risk, match))

@app.route("/results/<int:batch_id>")
@login_required
def results(batch_id):
    cur.execute("SELECT b.id, r.* FROM batches b JOIN results r ON b.id = r.batch_id WHERE b.id=%s AND b.user_id=%s ORDER BY r.id", 
                (batch_id, current_user.id))
    rows = cur.fetchall()
    
    if not rows and not cur.rowcount:
        flash("Batch not found or unauthorized access.")
        return redirect(url_for('dashboard'))

    if request.args.get("xlsx"):
        from openpyxl import Workbook
        wb = Workbook()
        ws = wb.active
        ws.append(['ID', 'Batch ID', 'First Name', 'Last Name', 'DOB', 'Country', 'Risk', 'Match Data'])
        for row in rows:
            ws.append(row[3:]) 
            
        output = BytesIO()
        wb.save(output)
        output.seek(0)
        return send_file(output, download_name="results.xlsx", as_attachment=True, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    
    results_data_for_template = [row[3:] for row in rows]
    return render_template("results.html", rows=results_data_for_template, batch_id=batch_id)

@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect("/login")

if __name__ == "__main__":
    app.run(debug=True)
