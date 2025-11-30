from mcp.server import Server
from mcp.types import CallToolResult
import json
import requests

server = Server("twin-greycat")

BACKEND = "http://localhost:8080"

def call_java(path):
    return requests.get(f"{BACKEND}{path}").json()

@server.call_tool("site_trend")
def site_trend(params):
    site = params["site"]
    data = call_java(f"/siteTrend?site={site}")
    return CallToolResult(contents=[json.dumps(data)])

@server.call_tool("vehicle_history")
def vehicle_history(params):
    plate = params["plate"]
    data = call_java(f"/vehicleHistory?plate={plate}")
    return CallToolResult(contents=[json.dumps(data)])

@server.call_tool("route_pattern")
def route_pattern(params):
    plate = params["plate"]
    data = call_java(f"/routePattern?plate={plate}")
    return CallToolResult(contents=[json.dumps(data)])
