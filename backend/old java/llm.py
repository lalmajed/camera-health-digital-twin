import os
import requests

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

def chat_with_llm(prompt):
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    body = {
        "model": "llama-3.1-70b-versatile",
        "messages": [
            {"role": "system", "content": "You are a smart agent that analyzes traffic camera data."},
            {"role": "user", "content": prompt}
        ]
    }

    r = requests.post(GROQ_URL, json=body, headers=headers)

    if r.status_code != 200:
        print("Error:", r.text)
        return None

    return r.json()["choices"][0]["message"]["content"]
