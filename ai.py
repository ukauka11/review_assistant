import os
import json
from openai import OpenAI
from collections import Counter
from datetime import datetime

print("✅ ai.py loaded")

ALLOWED_URGENCY = {"high", "medium", "low"}
ALLOWED_SENTIMENT = {"positive", "neutral", "negative"}
ALLOWED_CATEGORY = {
    "food_quality",
    "speed_wait_time",
    "service",
    "pricing",
    "order_accuracy",
    "other",
}


def get_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")
    return OpenAI(api_key=api_key)


def normalize_platform(p: str) -> str:
    p = (p or "").strip().lower()
    return p if p in {"google", "yelp", "facebook"} else "other"


def extract_tags(text: str) -> list[str]:
    t = text.lower()
    tags = set()

    if any(x in t for x in ["wait", "waiting", "slow", "long time"]):
        tags.add("wait_time")
    if any(x in t for x in ["missing", "forgot", "wrong order"]):
        tags.add("missing_items")
    if any(x in t for x in ["rude", "attitude"]):
        tags.add("rude_service")
    if any(x in t for x in ["delicious", "amazing", "ono"]):
        tags.add("great_food")

    return sorted(tags)


def enforce_urgency_rules(ai_urgency: str, tags: list[str]) -> str:
    if any(t in tags for t in ["missing_items", "rude_service"]):
        return "high"
    if "wait_time" in tags:
        return "medium"
    return ai_urgency


def sanitize_ai(ai_result: dict):
    sentiment = ai_result.get("sentiment", "neutral").lower()
    urgency = ai_result.get("urgency", "low").lower()
    category = ai_result.get("category", "other").lower()

    if sentiment not in ALLOWED_SENTIMENT:
        sentiment = "neutral"
    if urgency not in ALLOWED_URGENCY:
        urgency = "low"
    if category not in ALLOWED_CATEGORY:
        category = "other"

    reply = (ai_result.get("reply") or ai_result.get("response") or ai_result.get("message") or "").strip()
    if not reply:
        reply = "Thanks for the feedback — we really appreciate it. We’re looking into this and would love to make it right. Please contact us so we can follow up."

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
    run_id: str = "",
) -> dict:
    platform = normalize_platform(platform)
    tags = extract_tags(review_text)

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": (
                "You are RestaurantAssist. Output ONLY valid JSON (no markdown, no extra text). "
                "Return this exact schema:\n"
                "{\n"
                '  "sentiment": "positive|neutral|negative",\n'
                '  "urgency": "low|medium|high",\n'
                '  "category": "food|service|speed|cleanliness|pricing|other",\n'
                '  "reply": "string - a professional restaurant owner reply",\n'
                '  "next_steps": ["string", "string"]\n'
                "}\n"
                "Rules:\n"
                "- reply must NEVER be empty.\n"
                "- next_steps must be an array (can be empty).\n"
                )
            },
            {
                "role": "user",
                "content": (
                f"Platform: {platform}\n"
                f"Customer name: {customer_name}\n"
                f"Order number: {order_number}\n"
                "Review:\n"
                f"{review_text}"
                )
            },
        ],
        response_format={"type": "json_object"},
    )
    print("🧪 raw model JSON:", repr(response.choices[0].message.content))

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
        "next_steps": next_steps,
    }


def summarize_reviews(records: list[dict]) -> dict:
    if not records:
        return {"total_reviews": 0}

    tag_counter = Counter()
    sentiment_counter = Counter()

    for r in records:
        for t in r.get("tags", []):
            tag_counter[t] += 1
        sentiment_counter[r.get("sentiment", "neutral")] += 1

    return {
        "total_reviews": len(records),
        "sentiment_breakdown": dict(sentiment_counter),
        "top_issues": tag_counter.most_common(5),
    }
