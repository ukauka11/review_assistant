import os
import json
import time
import requests
from contextlib import asynccontextmanager
from fastapi import FastAPI, Header, HTTPException, Query
from pydantic import BaseModel
from engine import db_list_businesses, get_client, analyze_review, normalize_platform, db_insert_review, db_fetch_reviews, summarize_reviews, db_init
from dotenv import load_dotenv
load_dotenv()

RATE_LIMIT_PER_MIN = 30  # adjust later
_hits = {}  # dict: api_key -> list[timestamps]

@asynccontextmanager
async def lifespan(app: FastAPI):
    db_init()  # runs once when app starts
    yield
    # (nothing needed on shutdown yet)

app = FastAPI(title="Restaurant Review Assistant API", lifespan=lifespan)

client = get_client()

class ReviewRequest(BaseModel):
    review_text: str
    platform: str = "other"
    customer_name: str = ""
    order_number: str = ""
    run_id: str | None = None

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/analyze")
def analyze(req: ReviewRequest, x_api_key: str | None = Header(default=None)):
    verify_api_key(x_api_key)
    rate_limit(x_api_key)

    business_from_key = get_business_from_key(x_api_key)  # returns business_id or "__admin__"
    if business_from_key == "__admin__":
        raise HTTPException(status_code=400, detail="Use /analyze_admin for admin requests")

    record = analyze_review(
        client=client,
        review_text=req.review_text,
        platform=normalize_platform(req.platform),
        customer_name=req.customer_name,
        order_number=req.order_number,
        run_id=req.run_id,
    )

    business_id = business_from_key.strip().lower()
    db_insert_review(record, business_id=business_id)

    return {"business_id": business_id, "record": record}

class AdminAnalyzeRequest(ReviewRequest):
    business_id: str

def require_admin(x_api_key: str | None):
    admin = os.getenv("INTERNAL_API_KEY")
    if not admin or not x_api_key or x_api_key.strip() != admin.strip():
        raise HTTPException(status_code=403, detail="Admin only")

@app.post("/analyze_admin")
def analyze_admin(req: AdminAnalyzeRequest, x_api_key: str | None = Header(default=None)):
    require_admin(x_api_key)
    rate_limit(x_api_key)

    record = analyze_review(
        client=client,
        review_text=req.review_text,
        platform=normalize_platform(req.platform),
        customer_name=req.customer_name,
        order_number=req.order_number,
        run_id=req.run_id,
    )

    business_id = req.business_id.strip().lower()
    db_insert_review(record, business_id=business_id)

    return {"business_id": business_id, "record": record}

@app.get("/summary")
def summary(x_api_key: str | None = Header(default=None)):
    verify_api_key(x_api_key)
    rate_limit(x_api_key)

    business_from_key = get_business_from_key(x_api_key)
    if business_from_key == "__admin__":
        raise HTTPException(status_code=400, detail="Use /summary_admin?business_id=... for admin")

    records = db_fetch_reviews(business_id=business_from_key.strip().lower(), limit=500)
    return summarize_reviews(records)

@app.get("/summary_admin")
def summary_admin(business_id: str = Query(...), x_api_key: str | None = Header(default=None)):
    require_admin(x_api_key)
    rate_limit(x_api_key)

    records = db_fetch_reviews(business_id=business_id.strip().lower(), limit=500)
    return summarize_reviews(records)

@app.post("/jobs/daily-summary")
def daily_summary_job(x_api_key: str | None = Header(default=None)):
    require_admin(x_api_key)

    webhook = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook:
        raise HTTPException(status_code=500, detail="DISCORD_WEBHOOK_URL not set")

    businesses = db_list_businesses(limit=1000)
    sent = 0

    for biz in businesses:
        records = db_fetch_reviews(business_id=biz, limit=200)
        summary = summarize_reviews(records)

        text = (
            f"**Daily Summary — {biz}**\n"
            f"Total: {summary.get('total_reviews', 0)} | "
            f"Positive: {summary.get('positive', 0)} | "
            f"Neutral: {summary.get('neutral', 0)} | "
            f"Negative: {summary.get('negative', 0)}\n"
            f"Top issues: {summary.get('top_issues', [])}\n"
        )

        r = requests.post(webhook, json={"content": text}, timeout=15)
        r.raise_for_status()
        sent += 1

    return {"businesses": len(businesses), "sent": sent}

def verify_api_key(x_api_key: str | None) -> None:
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Missing x-api-key")

    key = x_api_key.strip()

    admin = os.getenv("INTERNAL_API_KEY")
    if admin and key == admin.strip():
        return  # admin ok

    raw = (os.getenv("CUSTOMER_KEYS_JSON") or "{}").strip()

    try:
        mapping = json.loads(raw)
        # If Render stored it as a quoted JSON string, decode twice
        if isinstance(mapping, str):
            mapping = json.loads(mapping)
    except Exception:
        raise HTTPException(status_code=500, detail="CUSTOMER_KEYS_JSON is invalid JSON")

    if isinstance(mapping, dict) and key in mapping:
        return  # customer ok

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

def get_business_from_key(x_api_key: str | None) -> str:
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Missing x-api-key")

    admin = os.getenv("INTERNAL_API_KEY")
    if x_api_key == admin:
        return "__admin__"

    raw = os.getenv("CUSTOMER_KEYS_JSON", "{}")
    try:
        mapping = json.loads(raw)
    except Exception:
        raise HTTPException(status_code=500, detail="CUSTOMER_KEYS_JSON is invalid JSON")

    if x_api_key in mapping:
        return str(mapping[x_api_key])

    raise HTTPException(status_code=401, detail="Unauthorized")
