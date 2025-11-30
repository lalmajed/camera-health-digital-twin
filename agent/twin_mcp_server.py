import requests
from mcp.server.fastmcp import FastMCP

BACKEND = "http://localhost:8080"

# Name of your MCP server
mcp = FastMCP("twin-backend")

# ---------- TOOLS ---------- #

@mcp.tool()
def getCityTotals(day: str) -> dict:
    """
    City-level degraded vs not degraded vehicles for a given day.
    day format: YYYY-MM-DD (e.g. 2025-08-16)
    """
    resp = requests.get(f"{BACKEND}/getCityTotals", params={"day": day})
    resp.raise_for_status()
    return resp.json()


@mcp.tool()
def getSiteTotals(site: str, day: str) -> dict:
    """
    Site totals for a given site and day.
    site: e.g. 'RUHSM173'
    day: '2025-08-20'
    """
    resp = requests.get(
        f"{BACKEND}/getSiteTotals",
        params={"site": site, "day": day},
    )
    resp.raise_for_status()
    return resp.json()


@mcp.tool()
def getSiteDayStatus(site: str, day: str) -> dict:
    """
    Detailed site-day status (detections, good/bad, color, etc.).
    """
    resp = requests.get(
        f"{BACKEND}/getSiteDayStatus",
        params={"site": site, "day": day},
    )
    resp.raise_for_status()
    return resp.json()


@mcp.tool()
def getVehicleDegradeStatus(plate: str) -> dict:
    """
    Latest degradation status for a plate.
    """
    resp = requests.get(
        f"{BACKEND}/getVehicleDegradeStatus",
        params={"plate": plate},
    )
    resp.raise_for_status()
    return resp.json()


@mcp.tool()
def getTripsForDay(plate: str, day: str) -> list:
    """
    All 30-min trips for a plate on a given day.
    """
    resp = requests.get(
        f"{BACKEND}/getTripsForDay",
        params={"plate": plate, "day": day},
    )
    resp.raise_for_status()
    return resp.json()


@mcp.tool()
def getTripsAllDays(plate: str) -> list:
    """
    All trips for a plate across all days.
    """
    resp = requests.get(
        f"{BACKEND}/getTripsAllDays",
        params={"plate": plate},
    )
    resp.raise_for_status()
    return resp.json()


# ---------- ENTRY POINT ---------- #

if __name__ == "__main__":
    # Runs an MCP server over stdio (used by the VS Code Azure MCP extension)
    mcp.run()
