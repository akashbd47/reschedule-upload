from flask import Flask, request
import pdfplumber
import firebase_admin
from firebase_admin import credentials, firestore
import os
import json
import re
import datetime

app = Flask(__name__)

# Firebase Init
firebase_json = os.environ.get("FIREBASE_KEY")
cred_dict = json.loads(firebase_json)

cred = credentials.Certificate(cred_dict)
firebase_admin.initialize_app(cred)

db = firestore.client()


def extract_loan_sl(text):

    m = re.search(r"\d{4}-\d{4}-\d{5}", text)
    return m.group() if m else None


def extract_case(text, loan_sl):

    left = text.split(loan_sl)[0]
    parts = left.split()

    return parts[-1] if parts else ""


def extract_name(text, loan_sl):

    right = text.split(loan_sl)[1]

    right = re.sub(r"\d{2}[/-]\d{2}[/-]\d{4}", "", right)

    words = re.findall(r"[A-Za-z\.]+", right)

    return " ".join(words[:4])


def extract_date(text):

    m = re.search(r"\d{2}[/-]\d{2}[/-]\d{4}", text)
    return m.group() if m else ""


def parse_pdf(path):

    rows = []

    with pdfplumber.open(path) as pdf:

        for page in pdf.pages:

            text = page.extract_text()

            if not text:
                continue

            for line in text.split("\n"):

                loan_sl = extract_loan_sl(line)

                if not loan_sl:
                    continue

                case = extract_case(line, loan_sl)
                name = extract_name(line, loan_sl)
                date = extract_date(line)

                rows.append({
                    "loanCaseNo": case,
                    "loanSlNo": loan_sl,
                    "customerName": name,
                    "rescheduleDate": date
                })

    return rows


def upload_reschedule(data):

    col = db.collection("RescheduleData")

    # delete old
    docs = col.stream()

    batch = db.batch()

    deleted = 0

    for d in docs:

        batch.delete(d.reference)
        deleted += 1

    batch.commit()

    # count rs times
    counter = {}

    for r in data:

        key = r["loanSlNo"]

        if key not in counter:
            counter[key] = 0

        counter[key] += 1

    # insert new
    batch = db.batch()

    inserted = 0

    for r in data:

        r["rsTimes"] = counter[r["loanSlNo"]]

        ref = col.document(r["loanSlNo"])

        batch.set(ref, r)

        inserted += 1

    batch.commit()

    # metadata update
    meta = db.collection("MetaData").document("Reshedule Update")

    meta.set({
        "updateTime": firestore.SERVER_TIMESTAMP
    })

    return deleted, inserted


@app.route("/", methods=["GET"])
def home():

    return open("templates/index.html").read()


@app.route("/upload", methods=["POST"])
def upload():

    file = request.files["file"]

    path = "temp.pdf"
    file.save(path)

    data = parse_pdf(path)

    deleted, inserted = upload_reschedule(data)

    return f"Completed | Deleted : {deleted} | Inserted : {inserted}"


if __name__ == "__main__":
    app.run()
