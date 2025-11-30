import os
from together import Together
from dotenv import load_dotenv
load_dotenv()

client = Together(api_key=os.getenv("TOGETHER_API_KEY"))

MODEL = "qwen2.5-72b-instruct"   # Qwen agent

def ask_llm(prompt):
    response = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=500
    )

    return response.choices[0].message["content"]
