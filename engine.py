import os
import json
from openai import OpenAI
from collections import Counter
from datetime import datetime

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
