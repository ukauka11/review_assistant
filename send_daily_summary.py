import os
import json
from dotenv import load_dotenv
import requests

from engine import summarize_reviews

load_dotenv()

LOG_PATH = os.path.join("logs", "review_log.json")
WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL")


def build_message(summary: dict) -> str:
    scope = summary.get("scope", "unknown")
    total = summary.get("total_reviews", 0)

    urgency = summary.get("urgency_breakdown", {})
    high = urgency.get("high", 0)
    med = urgency.get("medium", 0)
    low = urgency.get("low", 0)

    top_issues = summary.get("top_issues", [])
    focus = summary.get("recommended_focus", "none")

    lines = []
    lines.append("📊 **Daily Review Summary**")
    lines.append(f"- Scope: **{scope}**")
    lines.append(f"- Total reviews: **{total}**")
    lines.append(f"- Urgency: 🔴 {high} | 🟠 {med} | 🟢 {low}")

    if top_issues:
        lines.append("\n🔥 **Top issues**")
        for x in top_issues[:5]:
            lines.append(f"- {x['issue']}: {x['count']}")
        lines.append(f"\n✅ **Recommended focus:** `{focus}`")
    else:
        lines.append("\n✅ No major issues detected today.")

    return "\n".join(lines)


def main():
    if not WEBHOOK:
        raise SystemExit("DISCORD_WEBHOOK_URL is missing in .env")

    if not os.path.exists(LOG_PATH):
        raise SystemExit("No logs found yet (logs/review_log.json). Run the analyzer first.")

    with open(LOG_PATH, "r") as f:
        records = json.load(f)

    summary = summarize_reviews(records)
    message = build_message(summary)

    # Discord webhook payload
    payload = {"content": message}

    r = requests.post(WEBHOOK, json=payload, timeout=20)
    if r.status_code >= 300:
        raise SystemExit(f"Webhook failed: {r.status_code} {r.text}")

    print("Daily summary sent successfully.")


if __name__ == "__main__":
    main()
