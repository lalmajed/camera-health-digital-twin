#!/usr/bin/env python
import requests
import json
from mcp.server import Server
from mcp.types import CallToolResult

BACKEND = "http://localhost:8080"

server = Server("twin-greycat")

def jget(path):
    try:
        return requests.get(BACKEND + path, timeout=10).json()
    except Exception as e:
        return {"error": str(e)}


# -------------------------------------------------------
# GreyCat tools
# -------------------------------------------------------

@server.call_tool("site_trend")
def site_trend(params):
    site = params["site"]
    data = jget(f"/siteTrend?site={site}")
    return CallToolResult(contents=[json.dumps(data)])

@server.call_tool("vehicle_history")
def vehicle_history(params):
    plate = params["plate"]
    data = jget(f"/vehicleHistory?plate={plate}")
    return CallToolResult(contents=[json.dumps(data)])

@server.call_tool("route_pattern")
def route_pattern(params):
    plate = params["plate"]
    data = jget(f"/routePattern?plate={plate}")
    return CallToolResult(contents=[json.dumps(data)])


if __name__ == "__main__":
    print("Twin GreyCat MCP Running (stdio mode)")
    server.run()
