from llm_agent import ask_llm
from tools import get_city_totals, get_site_status, get_trips

print("\n🔵 Agent ready!\n")

while True:
    user = input("You: ")

    if user.lower() in ["exit", "quit"]:
        break

    prompt = f"""
You are an agent for a traffic camera health system.

Available tools:

1. get_city_totals(day)
2. get_site_status(site, day)
3. get_trips(plate, day)

User query: "{user}"

Think step-by-step. If data is needed, call the correct tool.
Then produce the final human-friendly answer.
"""

    answer = ask_llm(prompt)
    print("\nAgent:", answer, "\n")
