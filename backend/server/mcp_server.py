#!/usr/bin/env python3
"""
MCP Server for Camera Health Digital Twin
Exposes Greycat API as MCP tools for AI agent analysis
"""

import asyncio
import json
import httpx
from typing import Any, Optional
from mcp.server import Server
from mcp.types import Tool, TextContent
from mcp.server.stdio import stdio_server

# Greycat API configuration
GREYCAT_BASE_URL = "http://localhost:8080"
GREYCAT_NAMESPACE = "site_queries"

class GreycatClient:
    """Client for interacting with Greycat API"""
    
    def __init__(self, base_url: str, namespace: str):
        self.base_url = base_url
        self.namespace = namespace
        self.client = httpx.AsyncClient(timeout=30.0)
    
    async def call_function(self, function_name: str, params: list) -> dict:
        """Call a Greycat function via HTTP"""
        url = f"{self.base_url}/{self.namespace}::{function_name}"
        try:
            response = await self.client.post(
                url,
                json=params,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            return {"error": str(e), "function": function_name}
    
    async def close(self):
        await self.client.aclose()


# Initialize Greycat client
greycat = GreycatClient(GREYCAT_BASE_URL, GREYCAT_NAMESPACE)

# Initialize MCP server
app = Server("camera-health-mcp")


@app.list_tools()
async def list_tools() -> list[Tool]:
    """List all available MCP tools"""
    return [
        Tool(
            name="list_all_sites",
            description="Get a list of all camera sites with their metadata (location, lanes, direction, names)",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="get_site_details",
            description="Get detailed information about a specific camera site including all health metrics",
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
            description="Get details about a specific vehicle including its detection quality across all sites",
            inputSchema={
                "type": "object",
                "properties": {
                    "plate_number": {
                        "type": "string",
                        "description": "The vehicle plate number (e.g., '1488AVR')"
                    }
                },
                "required": ["plate_number"]
            }
        ),
        Tool(
            name="get_trip_patterns",
            description="Get all trip patterns for a specific day, showing vehicle routes and detection quality",
            inputSchema={
                "type": "object",
                "properties": {
                    "day": {
                        "type": "string",
                        "description": "The day in format YYYY-MM-DD (e.g., '2025-08-10')"
                    }
                },
                "required": ["day"]
            }
        ),
        Tool(
            name="get_city_profile",
            description="Get city-wide camera health statistics for a specific day",
            inputSchema={
                "type": "object",
                "properties": {
                    "day": {
                        "type": "string",
                        "description": "The day in format YYYY_MM_DD (e.g., '2025_08_10')"
                    }
                },
                "required": ["day"]
            }
        ),
        Tool(
            name="debug_site",
            description="Get diagnostic counts for a site (number of records in each health metric category)",
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
            name="analyze_vehicle_trip",
            description="Analyze a specific vehicle's trip to determine if degradation is due to vehicle or site issues. Returns detailed analysis of each detection point.",
            inputSchema={
                "type": "object",
                "properties": {
                    "plate_number": {
                        "type": "string",
                        "description": "The vehicle plate number"
                    },
                    "day": {
                        "type": "string",
                        "description": "The day in format YYYY-MM-DD"
                    }
                },
                "required": ["plate_number", "day"]
            }
        ),
        Tool(
            name="compare_sites_on_street",
            description="Compare performance of all sites on a specific street",
            inputSchema={
                "type": "object",
                "properties": {
                    "street_name": {
                        "type": "string",
                        "description": "Street name in English or Arabic"
                    },
                    "day": {
                        "type": "string",
                        "description": "The day in format YYYY_MM_DD"
                    }
                },
                "required": ["street_name", "day"]
            }
        ),
        Tool(
            name="find_degraded_sites",
            description="Find all sites with poor performance (high degradation rate) for a given day",
            inputSchema={
                "type": "object",
                "properties": {
                    "day": {
                        "type": "string",
                        "description": "The day in format YYYY_MM_DD"
                    },
                    "threshold": {
                        "type": "number",
                        "description": "Degradation threshold percentage (default 50)",
                        "default": 50
                    }
                },
                "required": ["day"]
            }
        ),
        Tool(
            name="get_site_hourly_performance",
            description="Get hour-by-hour performance for a site on a specific day",
            inputSchema={
                "type": "object",
                "properties": {
                    "site_id": {
                        "type": "string",
                        "description": "The site ID"
                    },
                    "day": {
                        "type": "string",
                        "description": "The day in format YYYY_MM_DD"
                    }
                },
                "required": ["site_id", "day"]
            }
        )
    ]


@app.call_tool()
async def call_tool(name: str, arguments: Any) -> list[TextContent]:
    """Handle tool calls"""
    
    try:
        if name == "list_all_sites":
            result = await greycat.call_function("list_sites", [])
            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]
        
        elif name == "get_site_details":
            site_id = arguments["site_id"]
            result = await greycat.call_function("get_site_details", [site_id])
            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]
        
        elif name == "get_vehicle_details":
            plate_number = arguments["plate_number"]
            result = await greycat.call_function("get_vehicle_details", [plate_number])
            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]
        
        elif name == "get_trip_patterns":
            day = arguments["day"]
            result = await greycat.call_function("get_trip_patterns_for_day", [day])
            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]
        
        elif name == "get_city_profile":
            day = arguments["day"]
            result = await greycat.call_function("get_city_profile", [day])
            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]
        
        elif name == "debug_site":
            site_id = arguments["site_id"]
            result = await greycat.call_function("debug_site", [site_id])
            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]
        
        elif name == "analyze_vehicle_trip":
            return await analyze_vehicle_trip(arguments["plate_number"], arguments["day"])
        
        elif name == "compare_sites_on_street":
            return await compare_sites_on_street(arguments["street_name"], arguments["day"])
        
        elif name == "find_degraded_sites":
            threshold = arguments.get("threshold", 50)
            return await find_degraded_sites(arguments["day"], threshold)
        
        elif name == "get_site_hourly_performance":
            return await get_site_hourly_performance(arguments["site_id"], arguments["day"])
        
        else:
            return [TextContent(
                type="text",
                text=json.dumps({"error": f"Unknown tool: {name}"})
            )]
    
    except Exception as e:
        return [TextContent(
            type="text",
            text=json.dumps({"error": str(e), "tool": name})
        )]


async def analyze_vehicle_trip(plate_number: str, day: str) -> list[TextContent]:
    """
    Complex analysis: Determine if vehicle degradation is due to vehicle or site issues
    """
    # Get vehicle details
    vehicle = await greycat.call_function("get_vehicle_details", [plate_number])
    
    # Get trip patterns for the day
    day_formatted = day  # YYYY-MM-DD
    trips = await greycat.call_function("get_trip_patterns_for_day", [day_formatted])
    
    # Find trips for this vehicle
    vehicle_trips = []
    for pattern in trips:
        for trip in pattern.get("trips", []):
            if trip.get("plate_number") == plate_number:
                vehicle_trips.append(trip)
    
    if not vehicle_trips:
        return [TextContent(
            type="text",
            text=json.dumps({
                "analysis": "No trips found for this vehicle on this day",
                "vehicle": vehicle,
                "plate_number": plate_number,
                "day": day
            }, indent=2)
        )]
    
    # Analyze each trip
    analysis = {
        "vehicle": vehicle,
        "plate_number": plate_number,
        "day": day,
        "trips": [],
        "summary": {
            "total_trips": len(vehicle_trips),
            "total_detections": 0,
            "degraded_detections": 0,
            "sites_visited": set(),
            "always_degraded": vehicle.get("vehicle_label") == "always_degraded"
        }
    }
    
    for trip in vehicle_trips:
        trip_analysis = {
            "window": trip.get("window30"),
            "sites": trip.get("site_list"),
            "min_quality": trip.get("min_quality"),
            "max_quality": trip.get("max_quality"),
            "issue_label": trip.get("issue_label"),
            "steps": []
        }
        
        # Analyze each step in the trip
        for step in trip.get("steps", []):
            site_id = step.get("site")
            quality = step.get("img_quality")
            hour = step.get("hour")
            
            analysis["summary"]["sites_visited"].add(site_id)
            analysis["summary"]["total_detections"] += 1
            
            if quality < 0.5:  # Threshold for degraded
                analysis["summary"]["degraded_detections"] += 1
            
            # Get site details for comparison
            site_info = await greycat.call_function("get_site_details", [site_id])
            
            step_analysis = {
                "site_id": site_id,
                "timestamp": step.get("ts"),
                "hour": hour,
                "quality": quality,
                "is_degraded": quality < 0.5,
                "site_name": site_info.get("name_en", "Unknown")
            }
            
            trip_analysis["steps"].append(step_analysis)
        
        analysis["trips"].append(trip_analysis)
    
    # Convert set to list for JSON serialization
    analysis["summary"]["sites_visited"] = list(analysis["summary"]["sites_visited"])
    
    # Determine root cause
    degradation_rate = (analysis["summary"]["degraded_detections"] / 
                       analysis["summary"]["total_detections"] * 100 
                       if analysis["summary"]["total_detections"] > 0 else 0)
    
    if analysis["summary"]["always_degraded"]:
        analysis["conclusion"] = {
            "root_cause": "VEHICLE_ISSUE",
            "confidence": "HIGH",
            "reasoning": f"Vehicle is marked as 'always_degraded' with {degradation_rate:.1f}% degradation rate across {len(analysis['summary']['sites_visited'])} sites",
            "recommendation": "Vehicle camera/plate requires maintenance or replacement"
        }
    elif degradation_rate > 80:
        analysis["conclusion"] = {
            "root_cause": "LIKELY_VEHICLE_ISSUE",
            "confidence": "MEDIUM",
            "reasoning": f"High degradation rate ({degradation_rate:.1f}%) across multiple sites suggests vehicle issue",
            "recommendation": "Inspect vehicle plate quality and compare with other vehicles at same sites"
        }
    elif len(analysis["summary"]["sites_visited"]) == 1:
        analysis["conclusion"] = {
            "root_cause": "LIKELY_SITE_ISSUE",
            "confidence": "MEDIUM",
            "reasoning": "Degradation only at single site suggests site-specific issue",
            "recommendation": "Inspect site camera and compare with other vehicles at same time"
        }
    else:
        analysis["conclusion"] = {
            "root_cause": "MIXED_OR_UNCLEAR",
            "confidence": "LOW",
            "reasoning": f"Degradation rate {degradation_rate:.1f}% across {len(analysis['summary']['sites_visited'])} sites requires deeper analysis",
            "recommendation": "Compare site performance at same time of day with other vehicles"
        }
    
    return [TextContent(
        type="text",
        text=json.dumps(analysis, indent=2)
    )]


async def compare_sites_on_street(street_name: str, day: str) -> list[TextContent]:
    """Compare performance of sites on a specific street"""
    # Get all sites
    all_sites = await greycat.call_function("list_sites", [])
    
    # Filter sites by street name
    matching_sites = [
        site for site in all_sites
        if (street_name.lower() in site.get("name_en", "").lower() or
            street_name in site.get("name_ar", ""))
    ]
    
    if not matching_sites:
        return [TextContent(
            type="text",
            text=json.dumps({
                "error": f"No sites found on street: {street_name}",
                "searched_street": street_name
            }, indent=2)
        )]
    
    # Get performance data for each site
    comparison = {
        "street_name": street_name,
        "day": day,
        "sites": []
    }
    
    for site in matching_sites:
        site_id = site["siteId"]
        
        # Get site counts
        site_details = await greycat.call_function("get_site_details", [site_id])
        
        site_perf = {
            "site_id": site_id,
            "name_en": site.get("name_en"),
            "name_ar": site.get("name_ar"),
            "location": {
                "lat": site.get("lat"),
                "lon": site.get("lon")
            },
            "lanes": site.get("numberOfLanes"),
            "direction": site.get("direction"),
            "vehicle_counts": len(site_details.get("vehicle_counts_total", []))
        }
        
        comparison["sites"].append(site_perf)
    
    return [TextContent(
        type="text",
        text=json.dumps(comparison, indent=2)
    )]


async def find_degraded_sites(day: str, threshold: float) -> list[TextContent]:
    """Find sites with high degradation rates"""
    # Get all sites
    all_sites = await greycat.call_function("list_sites", [])
    
    degraded_sites = []
    
    for site in all_sites:
        site_id = site["siteId"]
        site_details = await greycat.call_function("get_site_details", [site_id])
        
        # Check vehicle counts
        counts = site_details.get("vehicle_counts_total", [])
        for count in counts:
            if count.get("day") == day:
                total = count.get("unique_vehicles", 0)
                degraded = count.get("always_degraded_vehicles", 0)
                
                if total > 0:
                    degradation_rate = (degraded / total) * 100
                    
                    if degradation_rate >= threshold:
                        degraded_sites.append({
                            "site_id": site_id,
                            "name_en": site.get("name_en"),
                            "name_ar": site.get("name_ar"),
                            "total_vehicles": total,
                            "degraded_vehicles": degraded,
                            "degradation_rate": round(degradation_rate, 2),
                            "location": {
                                "lat": site.get("lat"),
                                "lon": site.get("lon")
                            }
                        })
    
    # Sort by degradation rate
    degraded_sites.sort(key=lambda x: x["degradation_rate"], reverse=True)
    
    return [TextContent(
        type="text",
        text=json.dumps({
            "day": day,
            "threshold": threshold,
            "degraded_sites_count": len(degraded_sites),
            "sites": degraded_sites
        }, indent=2)
    )]


async def get_site_hourly_performance(site_id: str, day: str) -> list[TextContent]:
    """Get hour-by-hour performance for a site"""
    site_details = await greycat.call_function("get_site_details", [site_id])
    
    # Extract hourly quality data
    hourly_data = site_details.get("hourly_quality", [])
    
    # Filter by day
    day_data = [h for h in hourly_data if h.get("day") == day]
    
    # Sort by hour
    day_data.sort(key=lambda x: x.get("hour", 0))
    
    performance = {
        "site_id": site_id,
        "day": day,
        "hourly_performance": day_data
    }
    
    return [TextContent(
        type="text",
        text=json.dumps(performance, indent=2)
    )]


async def main():
    """Run the MCP server"""
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options()
        )


if __name__ == "__main__":
    asyncio.run(main())
