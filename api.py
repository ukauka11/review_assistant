import os
import json
import time
import requests
import secrets
import stripe
from contextlib import asynccontextmanager
from fastapi import FastAPI, Header, HTTPException, Query, Request
from pydantic import BaseModel
from engine import (
    db_list_businesses,
    get_client,
    analyze_review,
    normalize_platform,
    db_insert_review,
    db_fetch_reviews,
    summarize_reviews,
    db_init,
    db_get_webhook,
    db_set_webhook,
    db_add_customer_key, 
    db_deactivate_customer_key, 
    db_get_business_for_key,
    db_stripe_event_seen,
    db_mark_stripe_event,
    db_ensure_business
)
from dotenv import load_dotenv
load_dotenv()

RATE_LIMIT_PER_MIN = 30  # adjust later
_hits = {}  # dict: api_key -> list[timestamps]

stripe.api_key = os.getenv("STRIPE_API_KEY")

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

class CreateCheckoutRequest(BaseModel):
    business_id: str
    email: str

@app.post("/billing/create-checkout")
def create_checkout(req: CreateCheckoutRequest):
    # Basic validation
    business_id = req.business_id.strip().lower()
    email = req.email.strip().lower()

    if not business_id:
        raise HTTPException(status_code=400, detail="business_id required")
    if "@" not in email:
        raise HTTPException(status_code=400, detail="valid email required")

    price_id = os.getenv("STRIPE_PRICE_ID_MONTHLY")
    if not price_id:
        raise HTTPException(status_code=500, detail="STRIPE_PRICE_ID_MONTHLY not set")

    # IMPORTANT: set success/cancel URLs to something you control later
    success_url = "https://example.com/success"
    cancel_url = "https://example.com/cancel"

    session = stripe.checkout.Session.create(
        mode="subscription",
        customer_email=email,
        line_items=[{"price": price_id, "quantity": 1}],
        success_url=success_url,
        cancel_url=cancel_url,
        metadata={"business_id": business_id},
    )

    return {"checkout_url": session.url}

stripe.api_key = os.getenv("STRIPE_API_KEY")

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

    businesses = db_list_businesses(limit=1000)
    sent = 0
    skipped = 0

    for biz in businesses:
        webhook = db_get_webhook(biz)
        if not webhook:
            print(f"No webhook set for {biz}, skipping")
            skipped += 1
            continue

        try:
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

        except Exception as e:
            print(f"Failed to send summary for {biz}: {e}")

    return {"businesses": len(businesses), "sent": sent, "skipped": skipped}

class SetWebhookRequest(BaseModel):
    business_id: str
    discord_webhook_url: str

@app.post("/admin/set_webhook")
def set_webhook(req: SetWebhookRequest, x_api_key: str | None = Header(default=None)):
    require_admin(x_api_key)

    biz = req.business_id.strip().lower()
    db_set_webhook(biz, req.discord_webhook_url)

    return {"status": "ok", "business_id": biz}

class CreateCustomerRequest(BaseModel):
    business_id: str

@app.post("/admin/create_customer")
def create_customer(req: CreateCustomerRequest, x_api_key: str | None = Header(default=None)):
    require_admin(x_api_key)

    new_key = "cust_" + secrets.token_urlsafe(24)
    business_id = req.business_id.strip().lower()

    return {
        "business_id": business_id,
        "customer_api_key": new_key,
        "next_step": "Add this key to CUSTOMER_KEYS_JSON in Render (or store it in DB in the next step)."
    }

class OnboardCustomerRequest(BaseModel):
    business_id: str
    discord_webhook_url: str

@app.post("/admin/onboard_customer")
def onboard_customer(req: OnboardCustomerRequest, x_api_key: str | None = Header(default=None)):
    require_admin(x_api_key)

    business_id = req.business_id.strip().lower()
    webhook = req.discord_webhook_url.strip()

    new_key = "cust_" + secrets.token_urlsafe(24)

    db_add_customer_key(new_key, business_id)
    db_set_webhook(business_id, webhook)

    return {
        "business_id": business_id,
        "customer_api_key": new_key,
        "summary_url": "/summary",
        "analyze_url": "/analyze",
        "notes": "Customer should use x-api-key header with customer_api_key. Summary/analyze automatically use their business."
    }

class AdminAddCustomerKeyRequest(BaseModel):
    business_id: str
    api_key: str

@app.post("/admin/customer_keys/add")
def admin_add_customer_key(req: AdminAddCustomerKeyRequest, x_api_key: str | None = Header(default=None)):
    require_admin(x_api_key)
    biz = req.business_id.strip().lower()
    key = req.api_key.strip()
    db_add_customer_key(key, biz)
    return {"status": "ok", "business_id": biz, "api_key": key}

class AdminDeactivateCustomerKeyRequest(BaseModel):
    api_key: str

@app.post("/admin/customer_keys/deactivate")
def admin_deactivate_customer_key(req: AdminDeactivateCustomerKeyRequest, x_api_key: str | None = Header(default=None)):
    require_admin(x_api_key)
    key = req.api_key.strip()
    db_deactivate_customer_key(key)
    return {"status": "ok", "api_key": key}

@app.post("/stripe/webhook")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")
    webhook_secret = os.getenv("STRIPE_WEBHOOK_SECRET")

    if not webhook_secret:
        raise HTTPException(status_code=500, detail="STRIPE_WEBHOOK_SECRET not set")

    try:
        event = stripe.Webhook.construct_event(
            payload=payload,
            sig_header=sig_header,
            secret=webhook_secret
        )
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid Stripe signature")

    event_id = event["id"]

    if db_stripe_event_seen(event_id):
        return {"status": "already_processed"}

    db_mark_stripe_event(event_id)

    # Trigger on subscription/payment success
    if event["type"] in ["checkout.session.completed", "invoice.payment_succeeded"]:
        data = event["data"]["object"]

        # Pull customer email
        email = data.get("customer_details", {}).get("email") or data.get("customer_email")

        # For now: create a business_id from email prefix
        # (Next: you’ll collect a real business name in checkout)
        if email:
            biz = email.split("@")[0].lower().replace(".", "_")
            new_key = "cust_" + secrets.token_urlsafe(24)
            db_add_customer_key(new_key, biz)

            # No webhook yet; customer sets later
            print(f"Provisioned {biz} for {email} with key {new_key}")

        return {"ok": True}
    
    event_type = event["type"]

    if event_type == "checkout.session.completed":
        session = event["data"]["object"]

        business_id = (session.get("metadata") or {}).get("business_id", "")
        email = (session.get("customer_details") or {}).get("email") or session.get("customer_email")

        if not business_id:
            # still return 200 so Stripe doesn't retry forever
            print("[stripe] Missing metadata.business_id, skipping provisioning")
            return {"ok": True}

        business_id = business_id.strip().lower()

        # 1) Ensure business exists (you likely already have a businesses table by now)
        db_ensure_business(business_id=business_id, email=email)

        # 2) Create and store a new API key (active)
        customer_key = "cust_" + secrets.token_urlsafe(24)
        db_add_customer_key(business_id=business_id, api_key=customer_key)

        # 3) Optional: notify you in Discord
        admin_hook = os.getenv("ADMIN_DISCORD_WEBHOOK_URL")
        if admin_hook:
            msg = f"✅ **New Subscriber**\nBusiness: `{business_id}`\nEmail: `{email}`\nAPI Key: `{customer_key}`"
            try:
                requests.post(admin_hook, json={"content": msg}, timeout=15).raise_for_status()
            except Exception as e:
                print(f"[stripe] Failed to notify Discord: {e}")

        print(f"[stripe] Provisioned business={business_id}")
        return {"ok": True}


def verify_api_key(x_api_key: str | None) -> None:
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Missing x-api-key")

    key = x_api_key.strip()

    admin = os.getenv("INTERNAL_API_KEY")
    if admin and key == admin.strip():
        return

    biz = db_get_business_for_key(key)
    if biz:
        return

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

    key = x_api_key.strip()

    admin = os.getenv("INTERNAL_API_KEY")
    if admin and key == admin.strip():
        return "__admin__"

    biz = db_get_business_for_key(key)
    if biz:
        return biz

    raise HTTPException(status_code=401, detail="Unauthorized")
