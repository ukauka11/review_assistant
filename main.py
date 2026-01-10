import json
from openai import OpenAI
from dotenv import load_dotenv
import os

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

results = []

tasks = [
    {
        "name": "Order supplies",
        "priority": "high",
        "completed": False
    },
    {
        "name": "Clean fryers",
        "priority": "medium",
        "completed": False
    },
    {
        "name": "Reply to reviews",
        "priority": "low",
        "completed": True
    }
]

for task in tasks:
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": "You are a business assistant. Respon ONLY in JSON with keys: action, urgency." 
            },
            {
                "role": "user",
                "content": f"Analyze this task and decide what to do:\n{json.dumps(task)}"
            }
        ],
        response_format={"type": "json_object"}
    )

    ai_result = json.loads(response.choices[0].message.content)

    urgency = ai_result.get("urgency", "low")
    if urgency not in ["high", "medium", "low"]:
        urgency = "low"

    print("\nTask:", task["name"])
    print("AI decision:", ai_result)

    if urgency == "high":
        print("EXECUTE NOW")
    elif urgency == "medium":
        print("SCHEDULE SOON")
    else:
        print("LOG FOR LATER")

    results.append({
        "task": task["name"],
        "priority": task["priority"],
        "ai_action": ai_result.get("action"),
        "ai_urgency": urgency
    })

with open("batch_results.json", "a") as file:
    file.write(json.dumps(results, indent=2))
    file.write("\n")

print("\nSaved AI decisions to batch_results.json")