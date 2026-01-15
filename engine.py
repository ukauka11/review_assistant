import os
import json
from openai import OpenAI
from collections import Counter
from datetime import datetime
from psycopg import connect

ALLOWED_URGENCY = {"high", "medium", "low"}
ALLOWED_SENTIMENT = {"positive", "neutral", "negative"}
ALLOWED_CATEGORY = {"food_quality", "speed_wait_time", "service", "pricing", "order_accuracy", "other"}

def normalize_platform(p: str) -> str:
    p = (p or "").strip().lower()
    return p if p in {"google", "yelp", "facebook"} else "other"

def extract_tags(text: str) -> list[str]:
    t = text.lower()
    tags = set()

    if any(x in t for x in ["wait", "waiting", "took", "minutes", "long time", "slow", "forever"]):
        tags.add("wait_time")
    if any(x in t for x in ["salty", "too salty", "oversalted"]):
        tags.add("salty")
    if any(x in t for x in ["cold", "lukewarm", "not hot", "warm but"]):
        tags.add("cold_food")
    if any(x in t for x in ["missing", "forgot", "left out", "didn't get", "didnt get"]):
        tags.add("missing_items")
    if any(x in t for x in ["wrong order", "wrong item", "messed up"]):
        tags.add("wrong_order")
    if any(x in t for x in ["rude", "unfriendly", "attitude"]):
        tags.add("rude_service")
    if any(x in t for x in ["delicious", "amazing", "best", "so good", "ono"]):
        tags.add("great_food")
    if any(x in t for x in ["friendly", "great service", "awesome staff"]):
        tags.add("great_service")

    return sorted(tags)

def get_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set. Put it in .env or environment variables.")
    return OpenAI(api_key=api_key)

def sanitize_ai(ai_result: dict) -> tuple[str, str, str, str, list]:
    sentiment = str(ai_result.get("sentiment", "neutral")).lower()
    urgency = str(ai_result.get("urgency", "low")).lower()
    category = str(ai_result.get("category", "other")).lower()

    if sentiment not in ALLOWED_SENTIMENT:
        sentiment = "neutral"
    if urgency not in ALLOWED_URGENCY:
        urgency = "low"
    if category not in ALLOWED_CATEGORY:
        category = "other"

    reply = str(ai_result.get("reply", "")).strip()
    next_steps = ai_result.get("next_steps", [])
    if not isinstance(next_steps, list):
        next_steps = [str(next_steps)]

    return sentiment, urgency, category, reply, next_steps

def analyze_review(
    client: OpenAI,
    review_text: str,
    platform: str = "other",
    customer_name: str = "",
    order_number: str = "",
    run_id: str = ""
    ) -> dict:
    platform = normalize_platform(platform)
    tags = extract_tags(review_text)

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are an assistant for a takeout restaurant. "
                    "Respond ONLY in JSON with keys: sentiment, urgency, category, reply, next_steps. "
                    "Rules: "
                    "sentiment must be one of: positive, neutral, negative. "
                    "urgency must be one of: high, medium, low. "
                    "category must be one of: food_quality, speed_wait_time, service, pricing, order_accuracy, other. "
                    "reply should be a short, polite, public response (2-4 sentences). "
                    "next_steps should be a short list of internal actions. "
                    "Platform style: Google = professional and concise. "
                    "Yelp = a bit more detailed and empathetic. "
                    "Facebook = friendly and conversational."
                )
            },
            {
                "role": "user",
                "content": f"Platform: {platform}\nCustomer review:\n{review_text}"
            }
        ],
        response_format={"type": "json_object"}
    )

    ai_result = json.loads(response.choices[0].message.content)
    sentiment, urgency, category, reply, next_steps = sanitize_ai(ai_result)
    urgency = enforce_urgency_rules(urgency, tags)

    return {
        "run_id": run_id,
        "platform": platform,
        "customer_name": customer_name,
        "order_number": order_number,
        "review": review_text,
        "tags": tags,
        "sentiment": sentiment,
        "urgency": urgency,
        "category": category,
        "reply": reply,
        "next_steps": next_steps
    }

def db_ensure_business(business_id: str, email: str | None = None) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO businesses (business_id, email)
                VALUES (%s, %s)
                ON CONFLICT (business_id) DO UPDATE SET email = COALESCE(EXCLUDED.email, businesses.email)
            """, (business_id, email))
        conn.commit()

def enforce_urgency_rules(ai_urgency: str, tags: list[str]) -> str:
    order = {"low": 0, "medium": 1, "high": 2}

    # Hard business rules
    if any(t in tags for t in ["missing_items", "wrong_order", "rude_service"]):
        return "high"

    if any(t in tags for t in ["wait_time", "cold_food", "salty"]):
        return "medium" if order[ai_urgency] < order["medium"] else ai_urgency

    return ai_urgency

def append_log(log_path: str, new_records: list[dict]) -> tuple[int, int]:
    existing = []
    if os.path.exists(log_path):
        try:
            with open(log_path, "r") as file:
                existing = json.load(file)
            if not isinstance(existing, list):
                existing = []
        except Exception:
            existing = []

    combined = existing + new_records

    folder = os.path.dirname(log_path)
    if folder:
        os.makedirs(folder, exist_ok=True)

    with open(log_path, "w") as file:
        json.dump(combined, file, indent=2)

    return len(new_records), len(combined)

def summarize_reviews(records: list[dict]) -> dict:
    if not records:
        return {
            "total_reviews": 0,
            "message": "No reviews available."
        }

    today = datetime.now().strftime("%Y-%m-%d")

    # Filter to today's reviews if run_id exists
    today_records = [
        r for r in records
        if str(r.get("run_id", "")).startswith(today)
    ]

    rows = today_records if today_records else records

    tag_counter = Counter()
    category_counter = Counter()
    urgency_counter = Counter()
    sentiment_counter = Counter()

    for r in rows:
        for t in r.get("tags", []):
            tag_counter[t] += 1
        category_counter[r.get("category", "other")] += 1
        urgency_counter[r.get("urgency", "low")] += 1
        sentiment_counter[r.get("sentiment", "neutral")] += 1

    top_issues = []
    for tag, count in tag_counter.most_common():
        if tag in {"missing_items", "wrong_order", "wait_time", "cold_food", "salty", "rude_service"}:
            top_issues.append({"issue": tag, "count": count})

    return {
        "scope": "today" if today_records else "all_time",
        "total_reviews": len(rows),
        "urgency_breakdown": dict(urgency_counter),
        "sentiment_breakdown": dict(sentiment_counter),
        "top_categories": category_counter.most_common(5),
        "top_issues": top_issues[:5],
        "recommended_focus": top_issues[0]["issue"] if top_issues else "none"
    }

def db_conn():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    return connect(url)

def db_init():
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS reviews (
                    id SERIAL PRIMARY KEY,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    run_id TEXT,
                    business_id TEXT,
                    platform TEXT,
                    customer_name TEXT,
                    order_number TEXT,
                    review TEXT,
                    tags JSONB,
                    sentiment TEXT,
                    urgency TEXT,
                    category TEXT,
                    reply TEXT,
                    next_steps JSONB
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS processed_stripe_events (
                    event_id TEXT PRIMARY KEY,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS business_settings (
                    business_id TEXT PRIMARY KEY,
                    discord_webhook_url TEXT
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS customer_keys (
                    api_key TEXT PRIMARY KEY,
                    business_id TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    is_active BOOLEAN NOT NULL DEFAULT TRUE
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS subscriptions (
                    business_id TEXT PRIMARY KEY,
                    stripe_customer_id TEXT,
                    stripe_subscription_id TEXT,
                    status TEXT NOT NULL,
                    current_period_end TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)


        conn.commit()

def db_insert_review(record: dict, business_id: str) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reviews (
                    run_id, business_id, platform, customer_name, order_number,
                    review, tags, sentiment, urgency, category, reply, next_steps
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                record.get("run_id"),
                business_id,
                record.get("platform"),
                record.get("customer_name"),
                record.get("order_number"),
                record.get("review"),
                json.dumps(record.get("tags", [])),
                record.get("sentiment"),
                record.get("urgency"),
                record.get("category"),
                record.get("reply"),
                json.dumps(record.get("next_steps", [])),
            ))
        conn.commit()

def db_fetch_reviews(business_id: str, limit: int = 500) -> list[dict]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT run_id, business_id, platform, customer_name, order_number,
                       review, tags, sentiment, urgency, category, reply, next_steps,
                       created_at
                FROM reviews
                WHERE business_id = %s
                ORDER BY created_at DESC
                LIMIT %s
            """, (business_id, limit))
            rows = cur.fetchall()

    records = []
    for r in rows:
        records.append({
            "run_id": r[0],
            "business_id": r[1],
            "platform": r[2],
            "customer_name": r[3],
            "order_number": r[4],
            "review": r[5],
            "tags": r[6] or [],
            "sentiment": r[7],
            "urgency": r[8],
            "category": r[9],
            "reply": r[10],
            "next_steps": r[11] or [],
            "created_at": str(r[12]),
        })
    return records

def db_list_businesses(limit: int = 1000) -> list[str]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT business_id
                FROM reviews
                ORDER BY business_id
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
    return [r[0] for r in rows if r and r[0]]

def db_set_webhook(business_id: str, webhook_url: str) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO business_settings (business_id, discord_webhook_url)
                VALUES (%s, %s)
                ON CONFLICT (business_id)
                DO UPDATE SET discord_webhook_url = EXCLUDED.discord_webhook_url
            """, (business_id, webhook_url))
        conn.commit()

def db_get_webhook(business_id: str) -> str | None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT discord_webhook_url FROM business_settings WHERE business_id = %s",
                (business_id,)
            )
            row = cur.fetchone()
            return row[0] if row else None

def db_add_customer_key(api_key: str, business_id: str) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO customer_keys (api_key, business_id, is_active)
                VALUES (%s, %s, TRUE)
                ON CONFLICT (api_key)
                DO UPDATE SET business_id = EXCLUDED.business_id, is_active = TRUE
            """, (api_key, business_id))
        conn.commit()

def db_get_business_by_stripe(subscription_id: str | None, customer_id: str | None) -> str | None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            if subscription_id:
                cur.execute(
                    "SELECT business_id FROM subscriptions WHERE stripe_subscription_id=%s LIMIT 1",
                    (subscription_id,),
                )
                row = cur.fetchone()
                if row:
                    return row[0]

            if customer_id:
                cur.execute(
                    "SELECT business_id FROM subscriptions WHERE stripe_customer_id=%s LIMIT 1",
                    (customer_id,),
                )
                row = cur.fetchone()
                if row:
                    return row[0]
    return None

def db_get_business_for_key(api_key: str) -> str | None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT business_id
                FROM customer_keys
                WHERE api_key = %s AND is_active = TRUE
                LIMIT 1
            """, (api_key,))
            row = cur.fetchone()
    return row[0] if row else None

def db_deactivate_business_keys(business_id: str) -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE customer_keys
                SET is_active = FALSE
                WHERE business_id = %s
            """, (business_id,))
            updated = cur.rowcount
        conn.commit()
    return updated

def db_deactivate_customer_key(api_key: str) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE customer_keys
                SET is_active = FALSE
                WHERE api_key = %s
            """, (api_key,))
        conn.commit()
        
def db_stripe_event_seen(event_id: str) -> bool:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM processed_stripe_events WHERE event_id = %s LIMIT 1",
                (event_id,),
            )
            return cur.fetchone() is not None

def db_mark_stripe_event(event_id: str) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO processed_stripe_events (event_id) VALUES (%s) ON CONFLICT DO NOTHING",
                (event_id,),
            )
        conn.commit()

def db_set_subscription(
    business_id: str,
    status: str,
    stripe_customer_id: str | None = None,
    stripe_subscription_id: str | None = None,
    current_period_end: str | None = None,
):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO subscriptions
                (business_id, stripe_customer_id, stripe_subscription_id, status, current_period_end)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (business_id)
                DO UPDATE SET
                    stripe_customer_id = COALESCE(EXCLUDED.stripe_customer_id, subscriptions.stripe_customer_id),
                    stripe_subscription_id = COALESCE(EXCLUDED.stripe_subscription_id, subscriptions.stripe_subscription_id),
                    status = EXCLUDED.status,
                    current_period_end = EXCLUDED.current_period_end,
                    updated_at = NOW()
            """, (
                business_id,
                stripe_customer_id,
                stripe_subscription_id,
                status,
                current_period_end,
            ))
        conn.commit()

def db_get_subscription_status(business_id: str) -> str:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM subscriptions WHERE business_id = %s",
                (business_id,),
            )
            row = cur.fetchone()
            return row[0] if row else "inactive"
