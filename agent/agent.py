import json
from llm import chat_with_llm
from analyze import analyze_result   # <-- we use your analyzer layer
import requests

BACKEND = "http://localhost:8080"


# -----------------------------------------
# BACKEND TOOL CALLERS
# -----------------------------------------

def getCityTotals(params):
    return requests.get(
        f"{BACKEND}/getCityTotals?day={params['day']}"
    ).json()

def getSiteTotals(params):
    return requests.get(
        f"{BACKEND}/getSiteTotals?day={params['day']}&site={params['site']}"
    ).json()

def getSiteDayStatus(params):
    return requests.get(
        f"{BACKEND}/getSiteDayStatus?day={params['day']}&site={params['site']}"
    ).json()

def getVehicleDegradeStatus(params):
    return requests.get(
        f"{BACKEND}/getVehicleDegradeStatus?plate={params['plate']}"
    ).json()

def getTripsForDay(params):
    return requests.get(
        f"{BACKEND}/getTripsForDay?plate={params['plate']}&day={params['day']}"
    ).json()

def getTripsAllDays(params):
    return requests.get(
        f"{BACKEND}/getTripsAllDays?plate={params['plate']}"
    ).json()


# MAP tool name → function
TOOL_MAP = {
    "getCityTotals": getCityTotals,
    "getSiteTotals": getSiteTotals,
    "getSiteDayStatus": getSiteDayStatus,
    "getVehicleDegradeStatus": getVehicleDegradeStatus,
    "getTripsForDay": getTripsForDay,
    "getTripsAllDays": getTripsAllDays
}


# -----------------------------------------
# MAIN AGENT LOOP
# -----------------------------------------

def run_agent():
    while True:
        question = input("\nAsk me anything: ")

        if question.lower() in ["exit", "quit"]:
            break

        # -------------------------------
        # STEP 1 — ask Claude what tool to call
        # -------------------------------
        llm_output = chat_with_llm(question)

        print("\nLLM OUTPUT:", llm_output)

        # safety
        if llm_output is None:
            print("❌ LLM returned nothing.")
            continue

        # parse JSON
        try:
            tool_call = json.loads(llm_output)
        except Exception as e:
            print("❌ Invalid JSON:", llm_output)
            continue

        tool = tool_call.get("tool")
        params = tool_call.get("params", {})

        # -------------------------------
        # STEP 2 — tool=none → LLM can't answer
        # -------------------------------
        if tool == "none":
            print("🤖 LLM says: can't answer with tools.")
            continue

        if tool not in TOOL_MAP:
            print("❌ Unknown tool:", tool)
            continue

        # -------------------------------
        # STEP 3 — call backend tool
        # -------------------------------
        result = TOOL_MAP[tool](params)

        print("\n🔍 RAW RESULT:")
        print(json.dumps(result, indent=2))

        # -------------------------------
        # STEP 4 — run AGENTIC ANALYSIS layer
        # -------------------------------
        final = analyze_result(question, result)

        print("\n🤖 FINAL ANSWER:")
        print(final)


if __name__ == "__main__":
    run_agent()

