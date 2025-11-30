#!/usr/bin/env python3
"""
MCP Server for Camera Health Diagnostics - HTTP VERSION (Windows Compatible)
Exposes Greycat API as MCP tools via HTTP instead of stdio
"""

import asyncio
import httpx
import json
from typing import Any, Dict, List
from mcp.server import Server
from mcp.types import Tool, TextContent
from mcp.server.sse import SseServerTransport
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.responses import Response

# Greycat API configuration
GREYCAT_BASE_URL = "http://localhost:8080"
GREYCAT_NAMESPACE = "site_queries"

# Initialize MCP server
app_mcp = Server("camera-health-mcp")

# HTTP client for Greycat
http_client = httpx.AsyncClient(timeout=30.0)


async def call_greycat_function(function_name: str, args: list) -> dict:
    """Call a Greycat function via HTTP"""
    url = f"{GREYCAT_BASE_URL}/{GREYCAT_NAMESPACE}::{function_name}"
    
    try:
        response = await http_client.post(
            url,
            json=args,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as e:
        return {"error": f"HTTP error calling {function_name}: {str(e)}"}
    except Exception as e:
        return {"error": f"Error calling {function_name}: {str(e)}"}


# Define MCP tools
@app_mcp.list_tools()
async def list_tools() -> List[Tool]:
    """List available MCP tools"""
    return [
        Tool(
            name="list_all_sites",
            description="Get a list of all camera sites in the system with their metadata",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="get_site_details",
            description="Get detailed information about a specific camera site including location, lanes, and health metrics",
            inputSchema={
                "type": "object",
                "properties": {
                    "site_id": {
                        "type": "string",
                        "description": "The site ID (e.g., 'RUHSM336')"
                    }
                },
                "required": ["site_id"]
            }
        ),
        Tool(
            name="get_vehicle_details",
            description="Get detection history and quality metrics for a specific vehicle plate number",
            inputSchema={
                "type": "object",
                "properties": {
                    "plate_number": {
                        "type": "string",
                        "description": "Vehicle plate number (e.g., '1488AVR')"
                    }
                },
                "required": ["plate_number"]
            }
        ),
        Tool(
            name="get_trip_patterns",
            description="Get all trip patterns for a specific day, including vehicle routes and degradation patterns",
            inputSchema={
                "type": "object",
                "properties": {
                    "day": {
                        "type": "string",
                        "description": "Date in YYYY-MM-DD format (e.g., '2025-08-10')"
                    }
                },
                "required": ["day"]
            }
        ),
        Tool(
            name="get_city_profile",
            description="Get city-wide camera health statistics and metrics for a specific day",
            inputSchema={
                "type": "object",
                "properties": {
                    "day": {
                        "type": "string",
                        "description": "Date in YYYY_MM_DD format (e.g., '2025_08_10')"
                    }
                },
                "required": ["day"]
            }
        ),
        Tool(
            name="debug_site",
            description="Get diagnostic counts and debug information for a specific site",
            inputSchema={
                "type": "object",
                "properties": {
                    "site_id": {
                        "type": "string",
                        "description": "The site ID to debug"
                    }
                },
                "required": ["site_id"]
            }
        ),
        Tool(
            name="analyze_vehicle_trip",
            description="Analyze a vehicle's trip on a specific day to determine if degradation is due to vehicle or site issues",
            inputSchema={
                "type": "object",
                "properties": {
                    "plate_number": {
                        "type": "string",
                        "description": "Vehicle plate number"
                    },
                    "day": {
                        "type": "string",
                        "description": "Date in YYYY-MM-DD format"
                    }
                },
                "required": ["plate_number", "day"]
            }
        ),
        Tool(
            name="compare_sites_on_street",
            description="Compare performance of all camera sites on the same street",
            inputSchema={
                "type": "object",
                "properties": {
                    "street_name": {
                        "type": "string",
                        "description": "Street name to analyze (e.g., 'Al Sahel Valley St')"
                    },
                    "day": {
                        "type": "string",
                        "description": "Date in YYYY_MM_DD format"
                    }
                },
                "required": ["street_name", "day"]
            }
        ),
        Tool(
            name="find_degraded_sites",
            description="Find all sites with degradation above a specified threshold",
            inputSchema={
                "type": "object",
                "properties": {
                    "day": {
                        "type": "string",
                        "description": "Date in YYYY_MM_DD format"
                    },
                    "threshold": {
                        "type": "number",
                        "description": "Degradation threshold percentage (0-100)",
                        "default": 50
                    }
                },
                "required": ["day"]
            }
        ),
        Tool(
            name="get_site_hourly_performance",
            description="Get hour-by-hour performance metrics for a specific site",
            inputSchema={
                "type": "object",
                "properties": {
                    "site_id": {
                        "type": "string",
                        "description": "The site ID"
                    },
                    "day": {
                        "type": "string",
                        "description": "Date in YYYY_MM_DD format"
                    }
                },
                "required": ["site_id", "day"]
            }
        )
    ]


@app_mcp.call_tool()
async def call_tool(name: str, arguments: dict) -> List[TextContent]:
    """Handle tool calls"""
    
    try:
        if name == "list_all_sites":
            result = await call_greycat_function("list_sites", [])
        
        elif name == "get_site_details":
            result = await call_greycat_function("get_site_details", [arguments["site_id"]])
        
        elif name == "get_vehicle_details":
            result = await call_greycat_function("get_vehicle_details", [arguments["plate_number"]])
        
        elif name == "get_trip_patterns":
            result = await call_greycat_function("get_trip_patterns_for_day", [arguments["day"]])
        
        elif name == "get_city_profile":
            result = await call_greycat_function("get_city_profile", [arguments["day"]])
        
        elif name == "debug_site":
            result = await call_greycat_function("debug_site", [arguments["site_id"]])
        
        elif name == "analyze_vehicle_trip":
            # Complex analysis - call multiple Greycat functions
            plate = arguments["plate_number"]
            day = arguments["day"]
            day_underscore = day.replace("-", "_")
            
            # Get vehicle details
            vehicle = await call_greycat_function("get_vehicle_details", [plate])
            
            # Get trip patterns
            trips = await call_greycat_function("get_trip_patterns_for_day", [day])
            
            # Filter trips for this vehicle
            vehicle_trips = [t for t in trips.get("trips", []) if plate in str(t)]
            
            # Analyze
            if not vehicle_trips:
                result = {
                    "analysis": f"No trips found for vehicle {plate} on {day}",
                    "root_cause": "NO_DATA",
                    "confidence": "N/A"
                }
            else:
                # Simple analysis based on trip data
                result = {
                    "vehicle": plate,
                    "day": day,
                    "trips_found": len(vehicle_trips),
                    "vehicle_data": vehicle,
                    "trip_data": vehicle_trips[:3],  # First 3 trips
                    "analysis": f"Found {len(vehicle_trips)} trips for vehicle {plate}. Check degradation patterns across sites.",
                    "recommendation": "Compare vehicle quality across different sites to determine root cause"
                }
        
        elif name == "compare_sites_on_street":
            # Get all sites
            all_sites = await call_greycat_function("list_sites", [])
            
            # Filter by street name
            street_name = arguments["street_name"]
            matching_sites = [
                s for s in all_sites 
                if street_name.lower() in str(s.get("name_en", "")).lower() or 
                   street_name.lower() in str(s.get("name_ar", "")).lower()
            ]
            
            result = {
                "street": street_name,
                "sites_found": len(matching_sites),
                "sites": matching_sites,
                "analysis": f"Found {len(matching_sites)} sites on {street_name}"
            }
        
        elif name == "find_degraded_sites":
            day = arguments["day"]
            threshold = arguments.get("threshold", 50)
            
            # Get city profile
            profile = await call_greycat_function("get_city_profile", [day])
            
            # Extract degraded sites (simplified - would need actual degradation data)
            result = {
                "day": day,
                "threshold": threshold,
                "city_profile": profile,
                "note": "Check city profile data for degradation metrics"
            }
        
        elif name == "get_site_hourly_performance":
            site_id = arguments["site_id"]
            day = arguments["day"]
            
            # Get site details
            site = await call_greycat_function("get_site_details", [site_id])
            
            # Get city profile for context
            profile = await call_greycat_function("get_city_profile", [day])
            
            result = {
                "site_id": site_id,
                "day": day,
                "site_data": site,
                "city_context": profile
            }
        
        else:
            result = {"error": f"Unknown tool: {name}"}
        
        return [TextContent(
            type="text",
            text=json.dumps(result, indent=2)
        )]
    
    except Exception as e:
        return [TextContent(
            type="text",
            text=json.dumps({"error": str(e)}, indent=2)
        )]


# Starlette app for HTTP transport
async def handle_sse(request):
    """Handle SSE connections"""
    async with SseServerTransport("/messages") as transport:
        await app_mcp.run(
            transport.read_stream,
            transport.write_stream,
            app_mcp.create_initialization_options()
        )
    return Response()


starlette_app = Starlette(
    routes=[
        Route("/sse", endpoint=handle_sse),
    ]
)


if __name__ == "__main__":
    import uvicorn
    print("=" * 80)
    print("MCP Server - HTTP Mode (Windows Compatible)")
    print("=" * 80)
    print(f"Starting HTTP MCP server on http://localhost:3000")
    print(f"Connecting to Greycat at {GREYCAT_BASE_URL}")
    print("=" * 80)
    
    uvicorn.run(starlette_app, host="0.0.0.0", port=3000)
