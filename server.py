import json
import logging
import os

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from delivery import send_email_report, upload_to_google_drive
from processing import GROUP_COLS, load_csv_from_bytes, process_csv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("inv-adjustments")

app = FastAPI(title="Inventory Adjustments Webhook")

WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"]


async def verify_token(request: Request):
    # Accept token via Authorization header OR ?token= query parameter.
    # Query parameter support allows platforms like Looker that don't
    # provide custom header fields for webhook deliveries.
    auth = request.headers.get("Authorization", "")
    query_token = request.query_params.get("token", "")
    if auth == f"Bearer {WEBHOOK_SECRET}" or query_token == WEBHOOK_SECRET:
        return
    raise HTTPException(status_code=401, detail="Invalid or missing token")


def _extract_csv_from_looker(payload: dict) -> str | None:
    """Pull the CSV string out of a Looker webhook JSON payload.

    Looker structure (CSV format):
      { "type": "query",
        "scheduled_plan": { ... },
        "attachment": { "mimetype": "text/csv", "extension": "csv", "data": "<csv>" },
        "data": null,
        "form_params": {} }
    """
    attachment = payload.get("attachment")
    if isinstance(attachment, dict):
        logger.info("attachment keys: %s", list(attachment.keys()))
        # The CSV content lives in attachment["data"]
        csv_data = attachment.get("data")
        if isinstance(csv_data, str) and csv_data:
            return csv_data
    if isinstance(attachment, str) and attachment:
        return attachment

    # Fall back to top-level "data"
    data = payload.get("data")
    if isinstance(data, str) and data:
        return data

    return None


@app.post("/webhook", dependencies=[Depends(verify_token)])
async def receive_csv(request: Request):
    content_type = request.headers.get("Content-Type", "")
    body = await request.body()
    logger.info("Webhook received: Content-Type=%s, body length=%d", content_type, len(body))

    if not body:
        raise HTTPException(status_code=400, detail="Empty request body")

    # Looker sends JSON with the CSV nested inside.  Structure:
    #   { "type": "query",
    #     "scheduled_plan": { ... },
    #     "attachment": { "<filename>.csv": "<csv content>" },
    #     "data": null,
    #     "form_params": {} }
    # We need to extract the CSV string from whichever field holds it.
    csv_bytes = body
    if "json" in content_type:
        try:
            payload = json.loads(body)
            logger.info("JSON keys: %s", list(payload.keys()))
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=422, detail=f"Invalid JSON: {e}")

        csv_str = _extract_csv_from_looker(payload)
        if csv_str is None:
            raise HTTPException(
                status_code=422,
                detail=f"Could not find CSV in JSON payload. Keys: {list(payload.keys())}",
            )
        csv_bytes = csv_str.encode("utf-8")
        logger.info("Extracted CSV from JSON payload (%d bytes)", len(csv_bytes))

    # Log the CSV header row to help debug column name mismatches
    first_line = csv_bytes.split(b"\n", 1)[0].decode("utf-8", errors="replace")
    logger.info("CSV header row: %s", first_line)

    try:
        df = load_csv_from_bytes(csv_bytes)
    except Exception as e:
        logger.exception("CSV parse error")
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
