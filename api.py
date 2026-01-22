import os
import time
import requests
import secrets
import stripe
from contextlib import asynccontextmanager
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from emailer import send_email
from ai import get_client, analyze_review, normalize_platform, summarize_reviews
from db import (
    db_list_businesses,
    db_insert_review,
    db_fetch_reviews,
    db_init,
    db_get_webhook,
    db_set_webhook,
    db_add_customer_key,
    db_deactivate_customer_key,
    db_get_business_for_key,
    db_stripe_event_seen,
    db_mark_stripe_event,
    db_ensure_business,
    db_get_subscription_status,
    db_set_subscription,
    db_deactivate_business_keys,
    db_get_business_by_stripe,
    db_conn,
    db_get_subscription_info,
)

from dotenv import load_dotenv
load_dotenv()

RATE_LIMIT_PER_MIN = 30  # adjust later
_hits = {}  # dict: api_key -> list[timestamps]

stripe.api_key = os.getenv("STRIPE_API_KEY")

@asynccontextmanager
async def lifespan(app: FastAPI):
    if os.getenv("DATABASE_URL"):
        db_init()
    else:
        print("⚠️ DATABASE_URL not set — skipping db_init()")
    yield

app = FastAPI(title="Restaurant Review Assistant API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://restaurantassist.app",
        "https://www.restaurantassist.app",
        "http://localhost:5173",
        "http://127.0.0.1:5500",
        "http://localhost:5500",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

    frontend = os.getenv("FRONTEND_URL", "https://restaurantassist.app").rstrip("/")
    success_url = f"{frontend}/success.html?session_id={{CHECKOUT_SESSION_ID}}"
    cancel_url = f"{frontend}/cancel.html"


    session = stripe.checkout.Session.create(
        mode="subscription",
        customer_email=email,
        line_items=[{"price": price_id, "quantity": 1}],
        success_url=success_url,
        cancel_url=cancel_url,
        metadata={"business_id": business_id},
        subscription_data={"metadata": {"business_id": business_id}},
    )

    return {"checkout_url": session.url}

@app.post("/billing/portal")
def billing_portal(x_api_key: str | None = Header(default=None)):
    verify_api_key(x_api_key)
    rate_limit(x_api_key)

    business_id = get_business_from_key(x_api_key)
    if business_id == "__admin__":
        raise HTTPException(status_code=400, detail="Admin cannot open billing portal without a business context")

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT stripe_customer_id FROM subscriptions WHERE business_id=%s LIMIT 1",
                (business_id,),
            )
            row = cur.fetchone()

    if not row or not row[0]:
        raise HTTPException(status_code=400, detail="No Stripe customer found for this business")

    stripe_customer_id = row[0]

    # Set this to your real dashboard URL later
    return_url = os.getenv("APP_RETURN_URL", "https://restaurantassist.app/dashboard")

    session = stripe.billing_portal.Session.create(
        customer=stripe_customer_id,
        return_url=return_url,
    )
    return {"url": session.url}

@app.get("/me")
def me(x_api_key: str | None = Header(default=None)):
    verify_api_key(x_api_key)
    rate_limit(x_api_key)

    business_id = get_business_from_key(x_api_key)
    if business_id == "__admin__":
        raise HTTPException(status_code=400, detail="Admin key not allowed for /me")

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT b.business_id, b.email,
                    s.status, s.plan, s.current_period_end
                FROM businesses b
                LEFT JOIN subscriptions s ON s.business_id = b.business_id
                WHERE b.business_id = %s
                LIMIT 1
            """, (business_id,))
            row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Business not found")

    biz_id, email, status, plan, current_period_end = row

    return {
        "business_id": biz_id,
        "email": email,
        "subscription": {
            "status": status or "inactive",
            "plan": plan or "starter",
            "current_period_end": current_period_end.isoformat() if current_period_end else None,
        }
    }

@app.post("/keys/rotate")
def rotate_key(x_api_key: str | None = Header(default=None)):
    verify_api_key(x_api_key)
    rate_limit(x_api_key)

    business_id = get_business_from_key(x_api_key)
    if business_id == "__admin__":
        raise HTTPException(status_code=400, detail="Admin key cannot rotate customer keys")

    old_key = x_api_key.strip()
    new_key = "cust_" + secrets.token_urlsafe(24)

    # Create new key + deactivate old key atomically-ish in one transaction
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO customer_keys (api_key, business_id, is_active)
                VALUES (%s, %s, TRUE)
                ON CONFLICT (api_key)
                DO UPDATE SET business_id = EXCLUDED.business_id, is_active = TRUE
            """, (new_key, business_id))

            cur.execute("""
                UPDATE customer_keys
                SET is_active = FALSE
                WHERE api_key = %s
            """, (old_key,))

        conn.commit()

    return {"api_key": new_key}

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

    business_id = business_from_key.strip().lower()

    status = db_get_subscription_status(business_id)
    if status != "active":
        raise HTTPException(status_code=402, detail="Subscription inactive")

    record = analyze_review(
        client=client,
        review_text=req.review_text,
        platform=normalize_platform(req.platform),
        customer_name=req.customer_name,
        order_number=req.order_number,
        run_id=req.run_id,
    )

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

    business_id = business_from_key.strip().lower()

    status = db_get_subscription_status(business_id)
    if status != "active":
        raise HTTPException(status_code=402, detail="Subscription inactive")

    records = db_fetch_reviews(business_id=business_id, limit=500)
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

            sent = summary.get("sentiment_breakdown", {})

            text = (
                f"**Daily Summary — {biz}**\n"
                f"Total: {summary.get('total_reviews', 0)} | "
                f"Positive: {sent.get('positive', 0)} | "
                f"Neutral: {sent.get('neutral', 0)} | "
                f"Negative: {sent.get('negative', 0)}\n"
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
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid payload")
    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid Stripe signature")

    print(f"[stripe] received type={event['type']} id={event['id']}")
    event_id = event["id"]

    if db_stripe_event_seen(event_id):
        return {"status": "already_processed"}

    db_mark_stripe_event(event_id)

    event_type = event["type"]

    if event_type == "checkout.session.completed":
        session = event["data"]["object"]

        business_id = (session.get("metadata") or {}).get("business_id", "")
        email = (session.get("customer_details") or {}).get("email") or session.get("customer_email")
        stripe_customer_id = session.get("customer")
        stripe_subscription_id = session.get("subscription")

        if not business_id:
            # still return 200 so Stripe doesn't retry forever
            print("[stripe] Missing metadata.business_id, skipping provisioning")
            return {"ok": True}

        business_id = business_id.strip().lower()

        # 1) Ensure business exists
        db_ensure_business(business_id=business_id, email=email)

        # 2) Store mapping + set initial status (ONE TIME)
        db_set_subscription(
            business_id=business_id,
            status="active",  # or "trialing"
            stripe_customer_id=stripe_customer_id,
            stripe_subscription_id=stripe_subscription_id,
            plan="starter",
        )

        print(
            f"[stripe][checkout] wrote subscription "
            f"biz={business_id} "
            f"cus={stripe_customer_id} "
            f"sub={stripe_subscription_id}"
        )

        # 3) Create and store a new API key (active)
        customer_key = "cust_" + secrets.token_urlsafe(24)
        db_add_customer_key(api_key=customer_key, business_id=business_id)

        frontend = os.getenv("FRONTEND_URL", "https://restaurantassist.app").rstrip("/")
        dashboard_url = f"{frontend}/dashboard.html"

        html = f"""
        <h2>Welcome to RestaurantAssist</h2>
        <p>Your subscription is active.</p>

        <p><b>Your API Key (save this):</b></p>
        <pre style="padding:12px;background:#111;color:#0f0;border-radius:6px;">{customer_key}</pre>

        <p><b>Dashboard:</b> <a href="{dashboard_url}">{dashboard_url}</a></p>

        <p><b>API Base URL:</b><br/>
        <code>https://review-assistant-api.onrender.com</code></p>

        <p><b>Next step:</b> Open the dashboard, paste your API key, and you can start using /analyze.</p>

        <p style="color:#777;font-size:12px;">
        Security note: This key grants access to your account. If you ever need a new one, contact support.
        </p>
        """
        html += """
        <hr style="margin-top:24px; opacity:0.3;" />

        <p style="font-size:12px; opacity:0.8;">
        You’re receiving this email because you signed up at
        <a href="https://restaurantassist.app">restaurantassist.app</a>.
        <br><br>
        Need help? Just reply to this email or contact
        <a href="mailto:support@restaurantassist.app">support@restaurantassist.app</a>.
        </p>
        """

        # Don't break provisioning if email fails
        try:
            if email:
                send_email(email, "Your RestaurantAssist API Key (Action Required)", html)
        except Exception as e:
            print(f"⚠️ Email send failed: {e}")

        # 4) Optional: notify you in Discord
        admin_hook = os.getenv("ADMIN_DISCORD_WEBHOOK_URL")
        if admin_hook:
            msg = (
                f"✅ **New Subscriber**\n"
                f"Business: `{business_id}`\n"
                f"Email: `{email}`\n"
                f"API Key: `{customer_key}`"
            )
            try:
                requests.post(admin_hook, json={"content": msg}, timeout=15).raise_for_status()
            except Exception as e:
                print(f"[stripe] Failed to notify Discord: {e}")

        print(f"[stripe] Provisioned business={business_id}")
        print(f"[stripe] checkout.completed business_id={business_id} email={email} cus={session.get('customer')} sub={session.get('subscription')}")
        return {"ok": True}

    elif event_type == "invoice.payment_succeeded":
        invoice = event["data"]["object"]
        sub_id = invoice.get("subscription")
        cus_id = invoice.get("customer")

        print(f"[stripe] invoice event: sub_id={sub_id} cus_id={cus_id}")
        business_id = db_get_business_by_stripe(sub_id, cus_id)
        print(f"[stripe] mapped business_id={business_id}")

        if not business_id:
            print("[stripe] Could not map invoice to business, skipping")
            return {"ok": True}

        db_set_subscription(business_id=business_id, status="active")

        print(
            f"[stripe][invoice.succeeded] set ACTIVE "
            f"biz={business_id} "
            f"sub={sub_id} "
            f"cus={cus_id}"
        )
        print(f"[stripe] Payment succeeded for business={business_id}")
        print(f"[stripe] invoice.succeeded metadata={invoice.get('metadata')} customer={invoice.get('customer')} subscription={invoice.get('subscription')}")
        return {"ok": True}

    elif event_type == "invoice.payment_failed":
        invoice = event["data"]["object"]
        sub_id = invoice.get("subscription")
        cus_id = invoice.get("customer")

        business_id = db_get_business_by_stripe(sub_id, cus_id)
        if not business_id:
            print("[stripe] Could not map invoice to business, skipping")
            return {"ok": True}

        db_set_subscription(business_id=business_id, status="past_due")

        print(
            f"[stripe][invoice.failed] set PAST_DUE "
            f"biz={business_id} "
            f"sub={sub_id} "
            f"cus={cus_id}"
        )
        print(f"[stripe] Payment failed for business={business_id}")
        print(f"[stripe] invoice.failed metadata={invoice.get('metadata')} customer={invoice.get('customer')} subscription={invoice.get('subscription')}")
        return {"ok": True}
    
    elif event_type == "customer.subscription.deleted":
        sub = event["data"]["object"]
        sub_id = sub.get("id")
        cus_id = sub.get("customer")

        business_id = db_get_business_by_stripe(sub_id, cus_id)
        if not business_id:
            print("[stripe] Could not map subscription to business, skipping")
            return {"ok": True}

        db_set_subscription(business_id=business_id, status="canceled")
        disabled = db_deactivate_business_keys(business_id)

        print(
            f"[stripe][sub.deleted] CANCELED "
            f"biz={business_id} "
            f"keys_disabled={disabled} "
            f"sub={sub_id} "
            f"cus={cus_id}"
        )
        print(f"[stripe] Subscription canceled for business={business_id}")
        print(f"[stripe] Disabled {disabled} API key(s) for business={business_id}")
        print(f"[stripe] sub.deleted metadata={sub.get('metadata')} customer={sub.get('customer')} sub_id={sub.get('id')}")
        return {"ok": True}

    else:
        return {"status": "ignored", "type": event_type}

@app.get("/billing/status")
def billing_status(session_id: str = Query(...)):
    # 1) Retrieve session from Stripe
    try:
        session = stripe.checkout.Session.retrieve(session_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid session_id")

    # 2) Extract Stripe IDs
    stripe_customer_id = session.get("customer")
    stripe_subscription_id = session.get("subscription")

    if not stripe_customer_id and not stripe_subscription_id:
        return {
            "ready": False,
            "message": "Checkout found, but subscription is still being created. Please refresh in a few seconds."
        }
    
    # 3) Map to business_id using your existing mapping function
    business_id = db_get_business_by_stripe(stripe_subscription_id, stripe_customer_id)

    if not business_id:
        # Sometimes webhook is still processing; tell frontend to retry
        return {"ready": False, "message": "Provisioning in progress. Please refresh in a few seconds."}

    # 4) Return subscription info from DB
    info = db_get_subscription_info(business_id)
    return {
        "ready": True,
        "business_id": business_id,
        "subscription": info,
    }

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

@app.get("/debug/db")
def debug_db():
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database();")
            return {"db": cur.fetchone()[0]}