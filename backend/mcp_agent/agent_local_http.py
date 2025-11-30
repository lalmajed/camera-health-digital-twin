#!/usr/bin/env python3
"""
Camera Health Diagnostic Agent - LOCAL VERSION (HTTP MCP - Windows Compatible)
Uses Ollama with Llama 3.3 and connects to MCP server via HTTP
"""

import asyncio
import json
import subprocess
import httpx
from typing import Any, Dict, List

# Configuration
OLLAMA_MODEL = "llama3.3"
MCP_SERVER_URL = "http://localhost:3000"


class OllamaClient:
    """Client for Ollama local LLM"""
    
    def __init__(self, model: str = OLLAMA_MODEL):
        self.model = model
        self._check_ollama()
    
    def _check_ollama(self):
        """Check if Ollama is installed and model is available"""
        try:
            result = subprocess.run(
                ["ollama", "list"],
                capture_output=True,
                text=True,
                check=True
            )
            if self.model not in result.stdout:
                print(f"⚠️  Model {self.model} not found. Pulling it now...")
                subprocess.run(["ollama", "pull", self.model], check=True)
                print(f"✓ Model {self.model} downloaded")
        except FileNotFoundError:
            print("❌ Ollama not found! Please install from https://ollama.ai")
            raise
        except subprocess.CalledProcessError as e:
            print(f"❌ Error checking Ollama: {e}")
            raise
    
    def chat(self, messages: List[Dict], tools: List[Dict] = None) -> Dict:
        """Send chat request to Ollama"""
        request = {
            "model": self.model,
            "messages": messages,
            "stream": False
        }
        
        if tools:
            request["tools"] = tools
        
        result = subprocess.run(
            ["ollama", "run", self.model, "--format", "json"],
            input=json.dumps(request),
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            raise Exception(f"Ollama error: {result.stderr}")
        
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return {
                "message": {
                    "role": "assistant",
                    "content": result.stdout
                }
            }


class CameraHealthAgentHTTP:
    """AI Agent using HTTP MCP (Windows compatible)"""
    
    def __init__(self, model: str = OLLAMA_MODEL):
        self.llm = OllamaClient(model)
        self.http_client = httpx.AsyncClient(timeout=60.0)
        self.available_tools = []
    
    async def connect_to_mcp(self):
        """Connect to HTTP MCP server"""
        print("\nConnecting to MCP server via HTTP...")
        
        try:
            # Check if MCP server is running
            response = await self.http_client.get(f"{MCP_SERVER_URL}/")
            print(f"✓ MCP server is running at {MCP_SERVER_URL}")
            
            # For now, define tools manually (in full version would fetch from server)
            self.available_tools = [
                {
                    "name": "list_all_sites",
                    "description": "Get all camera sites"
                },
                {
                    "name": "get_site_details",
                    "description": "Get site details",
                    "parameters": ["site_id"]
                },
                {
                    "name": "get_vehicle_details",
                    "description": "Get vehicle details",
                    "parameters": ["plate_number"]
                },
                {
                    "name": "get_trip_patterns",
                    "description": "Get trip patterns for a day",
                    "parameters": ["day"]
                },
                {
                    "name": "get_city_profile",
                    "description": "Get city-wide statistics",
                    "parameters": ["day"]
                }
            ]
            
            print(f"✓ Loaded {len(self.available_tools)} tools")
            
        except httpx.ConnectError:
            print(f"\n❌ Cannot connect to MCP server at {MCP_SERVER_URL}")
            print("Make sure to start the MCP server first:")
            print("  python mcp_server_http.py")
            raise
        except Exception as e:
            print(f"\n❌ Error connecting: {e}")
            raise
    
    async def call_greycat_direct(self, function_name: str, args: list):
        """Call Greycat API directly (fallback)"""
        url = f"http://localhost:8080/site_queries::{function_name}"
        
        try:
            # Greycat expects compact JSON with no extra whitespace
            # Use separators to ensure no spaces after : and ,
            json_body = json.dumps(args, separators=(',', ':'))
            
            # Debug: show exact payload
            print(f"  Sending to {url}")
            print(f"  Payload: {repr(json_body)} ({len(json_body)} bytes)")
            print(f"  Payload bytes: {json_body.encode('utf-8')}")
            
            # Try using data parameter instead of content
            # This might handle Content-Length differently
            response = await self.http_client.post(
                url,
                data=json_body,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            print(f"  Response status: {e.response.status_code}")
            print(f"  Response body: {e.response.text}")
            return {"error": f"HTTP {e.response.status_code}: {e.response.text}"}
        except Exception as e:
            return {"error": str(e)}
    
    def _create_system_prompt(self) -> str:
        """Create system prompt"""
        return """You are a camera health diagnostic expert AI agent.

Available tools:
- list_all_sites(): Get all camera sites
- get_site_details(site_id): Get detailed site information
- get_vehicle_details(plate_number): Get vehicle detection history
- get_trip_patterns(day): Get all trips for a day (format: YYYY-MM-DD)
- get_city_profile(day): Get city statistics (format: YYYY_MM_DD)

When analyzing:
1. Use tools to gather data
2. Analyze the data
3. Provide clear conclusions
4. Give actionable recommendations

Be specific and helpful!"""
    
    async def analyze(self, query: str, max_iterations: int = 5):
        """Perform analysis"""
        print(f"\n🤖 Query: {query}\n")
        print("=" * 80)
        
        messages = [
            {"role": "system", "content": self._create_system_prompt()},
            {"role": "user", "content": query}
        ]
        
        iteration = 0
        
        while iteration < max_iterations:
            iteration += 1
            print(f"\n--- Iteration {iteration} ---")
            
            # For simplicity, directly call Greycat based on query keywords
            # In full version, would use LLM to decide which tools to call
            
            result = None
            
            if "list" in query.lower() and "site" in query.lower():
                print("Calling: list_sites()")
                result = await self.call_greycat_direct("list_sites", [])
                
            elif "vehicle" in query.lower():
                # Extract plate number (simple regex)
                import re
                plates = re.findall(r'\b[A-Z0-9]{6,8}\b', query)
                if plates:
                    plate = plates[0]
                    print(f"Calling: get_vehicle_details('{plate}')")
                    result = await self.call_greycat_direct("get_vehicle_details", [plate])
            
            elif "site" in query.lower() and "details" in query.lower():
                import re
                sites = re.findall(r'\b[A-Z]{3,6}SM\d{3}\b', query)
                if sites:
                    site = sites[0]
                    print(f"Calling: get_site_details('{site}')")
                    result = await self.call_greycat_direct("get_site_details", [site])
            
            elif "city" in query.lower() or "profile" in query.lower():
                # Extract date
                import re
                dates = re.findall(r'202\d[_-]\d{2}[_-]\d{2}', query)
                if dates:
                    day = dates[0].replace("-", "_")
                    print(f"Calling: get_city_profile('{day}')")
                    result = await self.call_greycat_direct("get_city_profile", [day])
            
            if result:
                print(f"\n📊 Result preview: {str(result)[:300]}...")
                
                # Format response
                response = f"\n{'=' * 80}\n\n📊 Analysis Results:\n\n"
                response += json.dumps(result, indent=2)
                response += f"\n\n{'=' * 80}\n"
                
                print(response)
                return response
            else:
                print("Could not determine which tool to call from query.")
                print("Try being more specific, e.g.:")
                print("  - 'List all camera sites'")
                print("  - 'Get details for vehicle 1488AVR'")
                print("  - 'Get city profile for 2025_08_10'")
                return "Please rephrase your query to be more specific."
        
        return "Analysis incomplete"
    
    async def close(self):
        """Close connections"""
        await self.http_client.aclose()


async def interactive_mode():
    """Run in interactive mode"""
    print("=" * 80)
    print("Camera Health Diagnostic Agent - LOCAL VERSION (HTTP)")
    print("Using Ollama with Llama 3.3 (fully open source, no API costs!)")
    print("=" * 80)
    
    agent = CameraHealthAgentHTTP()
    
    try:
        await agent.connect_to_mcp()
        
        print("\nExamples:")
        print("  - List all camera sites")
        print("  - Get details for vehicle 1488AVR")
        print("  - Get city profile for 2025_08_10")
        print("\nType 'exit' to quit\n")
        
        while True:
            query = input("\n🔍 Your question: ").strip()
            
            if query.lower() in ['exit', 'quit', 'q']:
                print("Goodbye!")
                break
            
            if not query:
                continue
            
            await agent.analyze(query)
    
    finally:
        await agent.close()


if __name__ == "__main__":
    asyncio.run(interactive_mode())
