import os
import json
from psycopg import connect
from datetime import date

print("✅ db.py loaded")


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
                    plan TEXT NOT NULL DEFAULT 'starter',
                    current_period_end TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS businesses (
                    business_id TEXT PRIMARY KEY,
                    email TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS subscriptions_stripe_customer_id_key
                ON subscriptions(stripe_customer_id)
                WHERE stripe_customer_id IS NOT NULL
            """)

            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS subscriptions_stripe_subscription_id_key
                ON subscriptions(stripe_subscription_id)
                WHERE stripe_subscription_id IS NOT NULL
            """)

        conn.commit()

PLAN_LIMITS = {
    "starter": {"analyze_per_day": 30},
    "pro": {"analyze_per_day": 200},
}

def db_get_plan_for_business(conn, business_id: str) -> str:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COALESCE(s.plan, 'starter')
            FROM subscriptions s
            WHERE s.business_id = %s
            LIMIT 1
        """, (business_id,))
        row = cur.fetchone()
    return (row[0] if row else "starter") or "starter"

def db_get_usage_today(conn, business_id: str) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT analyze_count
            FROM usage_daily
            WHERE business_id=%s AND day=CURRENT_DATE
        """, (business_id,))
        row = cur.fetchone()
    return int(row[0]) if row else 0

def db_inc_usage_today(conn, business_id: str, inc: int = 1) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO usage_daily (business_id, day, analyze_count)
            VALUES (%s, CURRENT_DATE, %s)
            ON CONFLICT (business_id, day)
            DO UPDATE SET analyze_count = usage_daily.analyze_count + EXCLUDED.analyze_count
            RETURNING analyze_count
        """, (business_id, inc))
        new_count = cur.fetchone()[0]
    conn.commit()
    return int(new_count)

def enforce_plan_limit(conn, business_id: str):
    plan = db_get_plan_for_business(conn, business_id)
    limit = PLAN_LIMITS.get(plan, PLAN_LIMITS["starter"])["analyze_per_day"]
    used = db_get_usage_today(conn, business_id)
    if used >= limit:
        raise HTTPException(status_code=429, detail=f"Daily limit reached ({used}/{limit}) for plan={plan}")
    return plan, used, limit

def db_ensure_business(business_id: str, email: str | None = None) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO businesses (business_id, email)
                VALUES (%s, %s)
                ON CONFLICT (business_id) DO UPDATE
                SET email = COALESCE(EXCLUDED.email, businesses.email)
            """, (business_id, email))
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
    plan: str = "starter",
):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO subscriptions
                (
                    business_id,
                    stripe_customer_id,
                    stripe_subscription_id,
                    status,
                    current_period_end,
                    plan
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (business_id)
                DO UPDATE SET
                    stripe_customer_id = COALESCE(EXCLUDED.stripe_customer_id, subscriptions.stripe_customer_id),
                    stripe_subscription_id = COALESCE(EXCLUDED.stripe_subscription_id, subscriptions.stripe_subscription_id),
                    status = EXCLUDED.status,
                    current_period_end = EXCLUDED.current_period_end,
                    plan = EXCLUDED.plan,
                    updated_at = NOW()
            """, (
                business_id,
                stripe_customer_id,
                stripe_subscription_id,
                status,
                current_period_end,
                plan,
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

def db_get_subscription_info(business_id: str) -> dict:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT status, plan, current_period_end
                FROM subscriptions
                WHERE business_id = %s
                LIMIT 1
            """, (business_id,))
            row = cur.fetchone()

    if not row:
        return {"status": "inactive", "plan": "starter", "current_period_end": None}

    status, plan, period_end = row
    return {
        "status": status,
        "plan": plan or "starter",
        "current_period_end": period_end.isoformat() if period_end else None,
    }
