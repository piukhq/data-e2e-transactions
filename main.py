from datetime import datetime
from os import getenv

import pandas as pd
from azure.storage.blob import BlobServiceClient
from flask import Flask, flash, render_template, request
from flask_httpauth import HTTPBasicAuth
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import redirect

import functions

app = Flask(__name__)
auth = HTTPBasicAuth()
app.secret_key = "9a5MbU-dYtC2OTZ4df5MfnBwijkty9dX"  # not actually a secret, safe to ship with app
app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024

users = {"data-management": generate_password_hash("leave-slab-sausage")}  # Will add Azure AD auth in future

app.wsgi_app = ProxyFix(
    app.wsgi_app,
    x_proto=int(getenv("use_x_forwarded_proto", default=False)),
    x_host=int(getenv("use_x_forwarded_host", default=False)),
)

blob_storage_connection_string = getenv("blob_storage_connection_string")
blob_storage_container = getenv("blob_storage_container", "data-management")


def upload_blob(filename: str) -> str:
    client = BlobServiceClient.from_connection_string(blob_storage_connection_string)
    blobname = f"e2e-transactions-{datetime.utcnow().strftime('%FT%H%M%SZ')}.xlsx"
    blob = client.get_blob_client(container=blob_storage_container, blob=blobname)
    with open(filename, "rb") as f:
        blob.upload_blob(f)
    return blob.url


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
            # hn = functions.tlog(hn, "harvey-nichols")
            # wasabi = functions.tlog(wasabi, "wasabi-club")
            iceland = functions.iceland(iceland)

            file_path = "/tmp/download.xlsx"
            with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
                iceland.to_excel(writer, sheet_name="Iceland", index=False)
                hn.to_excel(writer, sheet_name="HN", index=False)
                wasabi.to_excel(writer, sheet_name="Wasabi", index=False)

            blob_url = upload_blob("/tmp/download.xlsx")
            return redirect(location=blob_url)
        elif "2ndrun" in request.form:
            if request.files["2nd_File"].filename == "":
                message = "File(s) missing"
                flash(message, category="error")
                return render_template("upload.html")

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

            blob_url = upload_blob("/tmp/download.xlsx")
            return redirect(location=blob_url)

    return render_template("upload.html")


if __name__ == "__main__":
    app.run()
