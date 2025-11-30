#!/usr/bin/env python
import json
import requests
from mcp.server import FastMCP

# Your Java backend HTTP server
BACKEND = "http://localhost:8080"

# Create MCP server instance
mcp = FastMCP("twin")


def safe_get(url: str):
    """Call backend safely and always return a JSON-serializable object."""
    try:
        resp = requests.get(url, timeout=10)
        return resp.json()
    except Exception as e:
        return {"error": str(e), "url": url}


# ------------------------------------------------
#   TOOLS (these are what MCP clients will see)
# ------------------------------------------------

@mcp.tool()
def getCityTotals(day: str) -> dict:
    """
    Get city-level degraded vs non-degraded vehicle counts for a given day
    (YYYY-MM-DD).
    """
    url = f"{BACKEND}/getCityTotals?day={day}"
    return safe_get(url)


@mcp.tool()
def getSiteTotals(day: str, site: str) -> dict:
    """
    Get degraded vs non-degraded vehicle counts for a specific site on a day.
    """
    url = f"{BACKEND}/getSiteTotals?day={day}&site={site}"
    return safe_get(url)


@mcp.tool()
def getSiteDayStatus(day: str, site: str) -> dict:
    """
    Get full health status of a site on a specific day (detections, good rate,
    color, etc.).
    """
    url = f"{BACKEND}/getSiteDayStatus?day={day}&site={site}"
    return safe_get(url)


@mcp.tool()
def getVehicleDegradeStatus(plate: str) -> dict:
    """
    Get latest degradation profile for a vehicle (plate).
    """
    url = f"{BACKEND}/getVehicleDegradeStatus?plate={plate}"
    return safe_get(url)


@mcp.tool()
def getTripsForDay(plate: str, day: str) -> dict:
    """
    Get all 30-min trips for a plate on a given day.
    """
    url = f"{BACKEND}/getTripsForDay?plate={plate}&day={day}"
    return safe_get(url)


@mcp.tool()
def getTripsAllDays(plate: str) -> dict:
    """
    Get all trips for a plate across all loaded days.
    """
    url = f"{BACKEND}/getTripsAllDays?plate={plate}"
    return safe_get(url)


# ------------------------------------------------
#   RUN AS STDIO MCP SERVER
# ------------------------------------------------

if __name__ == "__main__":
    print(" twin MCP server started (STDIO mode)")
    # This blocks and waits for MCP messages on stdin/stdout
    mcp.run()
