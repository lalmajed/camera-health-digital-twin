import json
import os

MEMORY_FILE = "memory.json"

DEFAULT_MEMORY = {
    "conversation": [],
    "vehicles": {},
    "sites": {},
    "global_patterns": []
}

def load_memory():
    if not os.path.exists(MEMORY_FILE):
        save_memory(DEFAULT_MEMORY)
        return DEFAULT_MEMORY
    try:
        with open(MEMORY_FILE, "r") as f:
            return json.load(f)
    except:
        return DEFAULT_MEMORY

def save_memory(memory):
    with open(MEMORY_FILE, "w") as f:
        json.dump(memory, f, indent=2)

def add_conversation(memory, user, agent):
    memory["conversation"].append({"user": user, "agent": agent})
    memory["conversation"] = memory["conversation"][-15:]  # keep last 15 messages

def add_vehicle_memory(memory, plate, insights):
    memory["vehicles"][plate] = insights

def add_site_memory(memory, site, insights):
    memory["sites"][site] = insights

def add_global_pattern(memory, pattern):
    if pattern not in memory["global_patterns"]:
        memory["global_patterns"].append(pattern)
