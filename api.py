import os
import json
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from engine import get_client, analyze_review, normalize_platform, append_log, summarize_reviews
import time

RATE_LIMIT_PER_MIN = 30  # adjust later
_hits = {}  # dict: api_key -> list[timestamps]

app = FastAPI(title="Restaurant Review Assistant API")

client = get_client()

class ReviewRequest(BaseModel):
    review_text: str
    platform: str = "other"
    customer_name: str = ""
    order_number: str = ""
    run_id: str = ""

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/analyze")
def analyze(req: ReviewRequest, x_api_key: str | None = Header(default=None)):
    verify_api_key(x_api_key)
    rate_limit(x_api_key)

    record = analyze_review(
        client=client,
        review_text=req.review_text,
        platform=normalize_platform(req.platform),
        customer_name=req.customer_name,
        order_number=req.order_number,
        run_id=req.run_id,
    )

    log_path = os.path.join("logs", "review_log.json")
    added, total = append_log(log_path, [record])

    return {
        "saved_to": log_path,
        "added": added,
        "total": total,
        "record": record
    }

@app.get("/summary")
def summary(x_api_key: str | None = Header(default=None)):
    verify_api_key(x_api_key)
    rate_limit(x_api_key)

    log_path = "logs/review_log.json"
    if not os.path.exists(log_path):
        return {"message": "No reviews logged yet."}

    with open(log_path, "r") as f:
        records = json.load(f)

    return summarize_reviews(records)

def verify_api_key(x_api_key: str | None):
    expected = os.getenv("INTERNAL_API_KEY")
    if not expected or x_api_key != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")

def rate_limit(api_key: str):
    now = time.time()
    window_start = now - 60

    timestamps = _hits.get(api_key, [])
    # keep only hits in the last 60 seconds
    timestamps = [t for t in timestamps if t >= window_start]

    if len(timestamps) >= RATE_LIMIT_PER_MIN:
        raise HTTPException(status_code=429, detail="Too Many Requests")

    timestamps.append(now)
    _hits[api_key] = timestamps
