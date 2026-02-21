import logging
import os

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from delivery import send_email_report, upload_to_google_drive
from processing import GROUP_COLS, load_csv_from_bytes, process_csv

load_dotenv()

logger = logging.getLogger("inv-adjustments")

app = FastAPI(title="Inventory Adjustments Webhook")

WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"]


async def verify_token(request: Request):
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {WEBHOOK_SECRET}":
        raise HTTPException(status_code=401, detail="Invalid or missing token")


@app.post("/webhook", dependencies=[Depends(verify_token)])
async def receive_csv(request: Request):
    body = await request.body()
    if not body:
        raise HTTPException(status_code=400, detail="Empty request body")

    try:
        df = load_csv_from_bytes(body)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"CSV parse error: {e}")

    flagged, removed, html_content = process_csv(df)

    errors = []

    try:
        send_email_report(html_content)
    except Exception as e:
        logger.exception("Email delivery failed")
        errors.append(f"email: {e}")

    try:
        upload_to_google_drive(html_content)
    except Exception as e:
        logger.exception("Google Drive upload failed")
        errors.append(f"drive: {e}")

    return JSONResponse(
        status_code=202,
        content={
            "status": "processed",
            "rows_flagged": len(flagged),
            "groups_removed": removed.drop_duplicates(subset=GROUP_COLS).shape[0],
            "rows_removed": len(removed),
            "delivery_errors": errors or None,
        },
    )


@app.get("/health")
async def health():
    return {"status": "ok"}
