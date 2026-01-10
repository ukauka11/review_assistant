import os
import json
import time

from contextlib import asynccontextmanager
from fastapi import FastAPI, Header, HTTPException, Query
from pydantic import BaseModel
from engine import get_client, analyze_review, normalize_platform, db_insert_review, db_fetch_reviews, summarize_reviews, db_init
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

    business_from_key = get_business_from_key(x_api_key)

    # Admin is allowed to act on behalf of any business via a query param (optional)
    if business_from_key == "__admin__":
        raise HTTPException(status_code=400, detail="Admin should use /analyze_admin")
    
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

@app.post("/analyze_admin")
def analyze_admin(req: AdminAnalyzeRequest, x_api_key: str | None = Header(default=None)):
    # admin only
    expected = os.getenv("INTERNAL_API_KEY")
    if not expected or x_api_key != expected:
        raise HTTPException(status_code=403, detail="Admin only")

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
        raise HTTPException(status_code=400, detail="Admin should use /summary_admin?business_id=...")

    records = db_fetch_reviews(business_id=business_from_key.strip().lower(), limit=500)
    return summarize_reviews(records)

@app.get("/summary_admin")
def summary_admin(
    business_id: str = Query(...),
    x_api_key: str | None = Header(default=None)
):
    expected = os.getenv("INTERNAL_API_KEY")
    if not expected or x_api_key != expected:
        raise HTTPException(status_code=403, detail="Admin only")

    rate_limit(x_api_key)

    records = db_fetch_reviews(business_id=business_id.strip().lower(), limit=500)
    return summarize_reviews(records)

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
