from flask import Flask, render_template, request, redirect, url_for, flash, send_file
from flask_login import LoginManager, login_user, logout_user, login_required, current_user, UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
import psycopg2
import requests
import json
import pandas as pd
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
import os

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev")

# DB
conn = psycopg2.connect(os.environ["DATABASE_URL"], sslmode="require")
cur = conn.cursor()

# Login
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
            df = pd.read_csv(f)
            required = {"first_name", "last_name", "country_of_citizenship", "dob"}
            if required.issubset(df.columns):
                cur.execute("INSERT INTO batches (user_id, filename) VALUES (%s, %s) RETURNING id",
                            (current_user.id, f.filename))
                batch_id = cur.fetchone()[0]
                conn.commit()
                process_batch.delay(batch_id, df.to_dict("records"))
                flash("Processing started")
                return redirect("/dashboard")
    return render_template("upload.html")

from celery import Celery
celery = Celery(app.name, broker=os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0"))

@celery.task
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
        if pd.notna(row.get("dob")):
            payload["queries"]["q1"]["properties"]["birthDate"] = [str(row["dob"])]
        if pd.notna(row.get("country_of_citizenship")):
            payload["queries"]["q1"]["properties"]["nationality"] = [row["country_of_citizenship"][:2].lower()]

        r = requests.post("https://api.opensanctions.org/match/default", json=payload, headers=headers)
        data = r.json()

        risk = "Clear"
        match = None
        if data.get("responses", {}).get("q1", {}).get("results"):
            entity = data["responses"]["q1"]["results"][0]
            names = [entity["caption"]] + [a["value"] for a in entity.get("properties", {}).get("alias", [])]
            full = f"{row['first_name']} {row['last_name']}".lower()
            if any(full in n.lower() for n in names):
                match = json.dumps(data)
                risk = "High" if "sanction" in str(entity).lower() else "Review"

        cur.execute("""INSERT INTO results (batch_id, first_name, last_name, dob, country_of_citizenship, risk_level, match_data)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (batch_id, row["first_name"], row["last_name"], row.get("dob"), row.get("country_of_citizenship"), risk, match))
        conn.commit()

@app.route("/results/<int:batch_id>")
@login_required
def results(batch_id):
    cur.execute("SELECT * FROM results WHERE batch_id=%s ORDER BY id", (batch_id,))
    rows = cur.fetchall()
    if request.args.get("xlsx"):
        df = pd.DataFrame(rows)
        output = BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)
        return send_file(output, download_name="results.xlsx", as_attachment=True, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    return render_template("results.html", rows=rows, batch_id=batch_id)

@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect("/login")

if __name__ == "__main__":
    app.run(debug=True)
