import os
import requests

def send_email(to_email: str, subject: str, html: str) -> None:
    api_key = os.getenv("RESEND_API_KEY")
    email_from = os.getenv("EMAIL_FROM")

    if not api_key or not email_from:
        print("⚠️ Email not configured (RESEND_API_KEY / EMAIL_FROM missing). Skipping email.")
        return

    r = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "from": email_from,
            "to": [to_email],
            "subject": subject,
            "html": html,
        },
        timeout=15,
    )
    if r.status_code >= 300:
        print(f"⚠️ Resend email failed: {r.status_code} {r.text}")
