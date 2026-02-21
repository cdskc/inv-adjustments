import base64
import os
from datetime import date

import requests

DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.file"]


def send_email_report(html_content: str) -> None:
    """Send the HTML report as an email attachment via Resend API."""
    api_key = os.environ["RESEND_API_KEY"]
    email_from = os.environ["EMAIL_FROM"]
    recipients = [r.strip() for r in os.environ["EMAIL_TO"].split(",")]

    report_date = date.today().strftime("%B %d, %Y")
    filename = f"flagged_adjustments_{date.today().strftime('%Y_%m_%d')}.html"

    response = requests.post(
        "https://api.resend.com/emails",
        headers={"Authorization": f"Bearer {api_key}"},
        json={
            "from": email_from,
            "to": recipients,
            "subject": f"CS Inventory Adjustments Report – {report_date}",
            "text": (
                "The attached report contains controlled substance inventory "
                "adjustments flagged for review. Offsetting (zero-sum) groups "
                "have been removed."
            ),
            "attachments": [
                {
                    "filename": filename,
                    "content": base64.b64encode(
                        html_content.encode("utf-8")
                    ).decode("ascii"),
                }
            ],
        },
        timeout=15,
    )
    if not response.ok:
        raise RuntimeError(
            f"Resend API error {response.status_code}: {response.text}"
        )


def upload_to_google_drive(html_content: str) -> str:
    """Upload the HTML report to a Google Drive folder. Returns the file ID."""
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaInMemoryUpload

    creds_file = os.environ["GOOGLE_SERVICE_ACCOUNT_FILE"]
    folder_id = os.environ["GOOGLE_DRIVE_FOLDER_ID"]

    creds = Credentials.from_service_account_file(creds_file, scopes=DRIVE_SCOPES)
    service = build("drive", "v3", credentials=creds, cache_discovery=False)

    filename = f"flagged_adjustments_{date.today().strftime('%Y_%m_%d')}.html"

    file_metadata = {
        "name": filename,
        "parents": [folder_id],
    }
    media = MediaInMemoryUpload(
        html_content.encode("utf-8"),
        mimetype="text/html",
        resumable=False,
    )
    result = (
        service.files()
        .create(body=file_metadata, media_body=media, fields="id")
        .execute()
    )

    return result["id"]
