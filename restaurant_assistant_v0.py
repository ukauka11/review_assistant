import os
import json
import datetime
from dotenv import load_dotenv

from engine import get_client, analyze_review, normalize_platform, append_log

load_dotenv()

def normalize_platform(p: str) -> str:
    p = (p or "").strip().lower()
    return p if p in {"google", "yelp", "facebook"} else "other"

def collect_reviews() -> list[dict]:
    reviews = []
    print("Paste a customer review and press Enter.")
    print("Type 'done' when you're finished.\n")

    while True:
        text = input("Review: ").strip()
        if text.lower() == "done":
            break
        if text == "":
            print("Please type a review or 'done'.")
            continue

        customer_name = input("Customer name (optional): ").strip()
        order_number = input("Order number (optional): ").strip()
        platform = normalize_platform(input("Platform (google/yelp/facebook/other): ").strip())

        reviews.append({
            "review": text,
            "customer_name": customer_name,
            "order_number": order_number,
            "platform": platform
        })
        print("")

    return reviews

#--------------------------------------
# OpenAI client setup
#--------------------------------------

client = get_client()

#--------------------------------------
# Input collection
#--------------------------------------

reviews = collect_reviews()
if not reviews:
    raise SystemExit("No reviews entered. Exiting.")

results = []
run_id = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

for item in reviews:
    record = analyze_review(
        client=client,
        review_text=item["review"],
        platform=item.get("platform", "other"),
        customer_name=item.get("customer_name", ""),
        order_number=item.get("order_number", ""),
        run_id=run_id
    )
    results.append(record)

    print("\n--- REVIEW ---")
    print(record["review"])
    print("Tags:", record["tags"])
    print("Sentiment:", record["sentiment"], "| Urgency:", record["urgency"], "| Category:", record["category"])
    print("Reply:", record["reply"])
    print("Next steps:", record["next_steps"])

log_path = os.path.join("logs", "review_log.json")
added, total = append_log(log_path, results)

print(f"\nAppended {added} review(s). Total saved: {total}")
print(f"Saved to {log_path}")

