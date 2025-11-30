import os
import json
from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()

client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

TOOL_SCHEMA = """
VALID TOOLS (choose exactly one):

1. getCityTotals
   params: { "day": "YYYY-MM-DD" }

2. getSiteTotals
   params: { "day": "YYYY-MM-DD", "site": "RUHSMxxx" }

3. getSiteDayStatus
   params: { "day": "YYYY-MM-DD", "site": "RUHSMxxx" }

4. getVehicleDegradeStatus
   params: { "plate": "PLATENUMBER" }

5. getTripsForDay
   params: { "plate": "PLATENUMBER", "day": "YYYY-MM-DD" }

6. getTripsAllDays
   params: { "plate": "PLATENUMBER" }

THE ONLY VALID JSON FORMAT YOU ARE ALLOWED TO OUTPUT IS:

{
  "tool": "<tool_name>",
  "params": { ... }
}

NEVER invent new keys like:
- vehicle_id
- date
- parameters

NEVER output markdown ```json
NEVER output explanation.
ONLY output the raw JSON object.
"""

def chat_with_llm(prompt: str):
    """
    Strict JSON-only tool decision enforced.
    """

    full_prompt = f"""
You are the Digital Twin Tool Router.
Your ONLY job is to choose the correct backend tool.

{TOOL_SCHEMA}

User request:
{prompt}

OUTPUT RULES (IMPORTANT):
- Respond ONLY with raw JSON.
- NO markdown fences.
- NO natural language.
- NO comments.
"""

    try:
        resp = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=400,
            temperature=0,
            messages=[{
                "role": "user",
                "content": full_prompt
            }]
        )

        return resp.content[0].text

    except Exception as e:
        print("⚠️ LLM ERROR:", e)
        return None

