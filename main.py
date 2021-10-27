import time
from datetime import datetime

import pandas as pd
from flask import Flask, flash, render_template, request, send_from_directory, url_for
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import redirect

import functions

app = Flask(__name__)
auth = HTTPBasicAuth()
app.secret_key = "9a5MbU-dYtC2OTZ4df5MfnBwijkty9dX"  # not actually a secret, safe to ship with app
app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024

users = {"data-management": generate_password_hash("leave-slab-sausage")}  # Will add Azure AD auth in future


@auth.verify_password
def verify_password(username, password):
    if username in users and check_password_hash(users.get(username), password):
        return username


@app.route("/", methods=["GET", "POST"])
@auth.login_required
def home():
    message = ""
    if request.method == "POST":
        if "1strun" in request.form:
            if request.files["wasabi"].filename == "" or request.files["hn_iceland"].filename == "":
                message = "File(s) missing"
                flash(message, category="error")
                return render_template("upload.html")
            if "Wasabi" not in request.files["wasabi"].filename:
                message = (
                    'Wasabi file should contain "Wasabi" in the file name, '
                    "please make sure you have selected the correct file"
                )
                flash(message, category="error")
                return render_template("upload.html")
            if "Iceland and HN" not in request.files["hn_iceland"].filename:
                message = (
                    'Iceland/Harvey Nichols file should contain "Iceland and HN" in the file name, '
                    "please make sure you have selected the correct file"
                )
                flash(message, category="error")
                return render_template("upload.html")
            tic = time.perf_counter()

            df = pd.read_excel(request.files.get("wasabi"), engine="openpyxl")  # Get Wasabi raw file
            wasabi = functions.wasabi(df)  # Format Wasabi file and do DB checks
            df = pd.read_excel(
                request.files.get("hn_iceland"),
                engine="openpyxl",
                dtype={
                    (
                        "MID (Merchant ID) - AS PRINTED ON RECEIPT\nOnly enter the numbers - no other characters. "
                        "The receipt may not contain the full MID (first characters may be obscured). "
                        "This is normal - please type in..."
                    ): str,
                    "Auth code - AS PRINTED ON RECEIPT": str,
                },
            )  # Get HN/Iceland file
            hn, iceland = functions.hniceland(df)  # Format HN/Iceland files into separate dataframes

            hn = functions.atlas(hn, "harvey-nichols")
            wasabi = functions.atlas(wasabi, "wasabi-club")
            hn = functions.matched(hn)
            wasabi = functions.matched(wasabi)
            hn = functions.tlog(hn, "harvey-nichols")
            wasabi = functions.tlog(wasabi, "wasabi-club")
            iceland = functions.iceland(iceland)

            file_path = "/tmp/download.xlsx"
            with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
                iceland.to_excel(writer, sheet_name="Iceland", index=False)
                hn.to_excel(writer, sheet_name="HN", index=False)
                wasabi.to_excel(writer, sheet_name="Wasabi", index=False)

            toc = time.perf_counter()
            return redirect(url_for("download", timetaken=round(toc - tic)))
        elif "2ndrun" in request.form:
            if request.files["2nd_File"].filename == "":
                message = "File(s) missing"
                flash(message, category="error")
                return render_template("upload.html")
            tic = time.perf_counter()

            iceland = pd.read_excel(
                request.files.get("2nd_File"),
                engine="openpyxl",
                sheet_name="Iceland",
                usecols="A:K",
                dtype={"MID": str, "Auth code": str, "Transaction ID": str},
            )
            hn = pd.read_excel(
                request.files.get("2nd_File"),
                engine="openpyxl",
                sheet_name="HN",
                usecols="A:J",
                dtype={"MID": str, "Auth code": str, "Transaction ID": str},
            )
            wasabi = pd.read_excel(
                request.files.get("2nd_File"),
                engine="openpyxl",
                sheet_name="Wasabi",
                usecols="A:I",
                dtype={"MID": str, "Auth code": str, "Transaction ID": str},
            )

            hn = functions.atlas(hn, "harvey-nichols")
            wasabi = functions.atlas(wasabi, "wasabi-club")
            hn = functions.matched(hn)
            wasabi = functions.matched(wasabi)
            hn = functions.tlog(hn, "harvey-nichols")
            wasabi = functions.tlog(wasabi, "wasabi-club")
            iceland = functions.iceland(iceland)

            file_path = "/tmp/download.xlsx"
            with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
                iceland.to_excel(writer, sheet_name="Iceland", index=False)
                hn.to_excel(writer, sheet_name="HN", index=False)
                wasabi.to_excel(writer, sheet_name="Wasabi", index=False)

            toc = time.perf_counter()

            return redirect(url_for("download", timetaken=round(toc - tic)))

    return render_template("upload.html")


@app.route("/download/<timetaken>", methods=["GET", "POST"])
def download(timetaken):
    if request.method == "POST":
        date = functions.ord(int(datetime.today().strftime("%d"))) + " " + datetime.today().strftime("%b")
        file_path = f"E2E Transactions {date}" + ".xlsx"
        return send_from_directory("/tmp", "download.xlsx", attachment_filename=file_path, as_attachment=True)
    return render_template("download.html", message=timetaken)


if __name__ == "__main__":
    app.run()
