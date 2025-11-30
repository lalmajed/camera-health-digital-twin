#!/usr/bin/env python3
"""
Camera Health Diagnostic Agent - LOCAL VERSION
Uses Ollama with Llama 3.3 (fully open source, runs locally)
"""

import asyncio
import json
import subprocess
from typing import Any, Dict, List
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# Configuration
OLLAMA_MODEL = "llama3.3"  # or "llama3.1", "qwen2.5", etc.
MCP_SERVER_COMMAND = "python"
MCP_SERVER_ARGS = ["mcp_server.py"]


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
        """
        Send chat request to Ollama
        """
        # Prepare the request
        request = {
            "model": self.model,
            "messages": messages,
            "stream": False
        }
        
        if tools:
            request["tools"] = tools
        
        # Call Ollama via subprocess
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
            # Fallback: use the text output directly
            return {
                "message": {
                    "role": "assistant",
                    "content": result.stdout
                }
            }


class CameraHealthAgentLocal:
    """AI Agent for camera health diagnostics using local LLM"""
    
    def __init__(self, model: str = OLLAMA_MODEL):
        self.llm = OllamaClient(model)
        self.session = None
        self.available_tools = []
    
    async def connect_to_mcp(self):
        """Connect to the MCP server"""
        server_params = StdioServerParameters(
            command=MCP_SERVER_COMMAND,
            args=MCP_SERVER_ARGS,
            env=None
        )
        
        # Use context manager properly
        self.stdio_context = stdio_client(server_params)
        self.stdio, self.write = await self.stdio_context.__aenter__()
        self.session = ClientSession(self.stdio, self.write)
        
        await self.session.initialize()
        
        # Get available tools
        response = await self.session.list_tools()
        self.available_tools = response.tools
        print(f"✓ Connected to MCP server with {len(self.available_tools)} tools")
    
    async def call_tool(self, tool_name: str, arguments: dict):
        """Call an MCP tool"""
        result = await self.session.call_tool(tool_name, arguments)
        return result.content[0].text
    
    def _format_tools_for_llm(self) -> List[Dict]:
        """Convert MCP tools to Ollama tool format"""
        return [
            {
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.inputSchema
                }
            }
            for tool in self.available_tools
        ]
    
    def _create_system_prompt(self) -> str:
        """Create system prompt for the agent"""
        return """You are a camera health diagnostic expert AI agent. You have access to tools that query a camera monitoring system.

Your job is to:
1. Understand user queries about camera health, vehicle detection, and site performance
2. Use the available tools to gather necessary data
3. Analyze the data to provide insights and recommendations
4. Be specific and actionable in your responses

When analyzing issues:
- Determine if problems are vehicle-related or site-related
- Compare performance across sites and time periods
- Provide confidence levels for your conclusions
- Give clear maintenance recommendations

Available tools:
""" + "\n".join([f"- {tool.name}: {tool.description}" for tool in self.available_tools])
    
    async def analyze(self, query: str, max_iterations: int = 10):
        """
        Perform complex analysis using local LLM with MCP tools
        """
        messages = [
            {
                "role": "system",
                "content": self._create_system_prompt()
            },
            {
                "role": "user",
                "content": query
            }
        ]
        
        print(f"\n🤖 Agent Query: {query}\n")
        print("=" * 80)
        
        iteration = 0
        tools_formatted = self._format_tools_for_llm()
        
        while iteration < max_iterations:
            iteration += 1
            print(f"\n--- Iteration {iteration} ---")
            
            # Call local LLM
            try:
                response = self.llm.chat(messages, tools=tools_formatted)
            except Exception as e:
                print(f"❌ Error calling LLM: {e}")
                return f"Error: {e}"
            
            # Check if LLM wants to use tools
            message = response.get("message", {})
            tool_calls = message.get("tool_calls", [])
            
            if tool_calls:
                # Process tool calls
                print(f"\n🔧 LLM wants to use {len(tool_calls)} tool(s)")
                
                tool_results = []
                for tool_call in tool_calls:
                    tool_name = tool_call.get("function", {}).get("name")
                    tool_args = tool_call.get("function", {}).get("arguments", {})
                    
                    if isinstance(tool_args, str):
                        tool_args = json.loads(tool_args)
                    
                    print(f"   Tool: {tool_name}")
                    print(f"   Args: {json.dumps(tool_args, indent=2)}")
                    
                    # Call the MCP tool
                    try:
                        result = await self.call_tool(tool_name, tool_args)
                        print(f"   Result preview: {result[:200]}...")
                        tool_results.append({
                            "role": "tool",
                            "name": tool_name,
                            "content": result
                        })
                    except Exception as e:
                        print(f"   Error: {e}")
                        tool_results.append({
                            "role": "tool",
                            "name": tool_name,
                            "content": json.dumps({"error": str(e)})
                        })
                
                # Add assistant response and tool results to messages
                messages.append({
                    "role": "assistant",
                    "content": message.get("content", ""),
                    "tool_calls": tool_calls
                })
                messages.extend(tool_results)
            
            else:
                # LLM has finished reasoning
                final_response = message.get("content", "")
                
                if not final_response:
                    # Try to extract from response
                    final_response = str(response)
                
                print("\n" + "=" * 80)
                print("\n📊 Final Analysis:\n")
                print(final_response)
                print("\n" + "=" * 80)
                
                return final_response
        
        return "Analysis incomplete - reached maximum iterations"
    
    async def close(self):
        """Close connections"""
        if self.session:
            await self.session.__aexit__(None, None, None)
        if hasattr(self, 'stdio_context'):
            await self.stdio_context.__aexit__(None, None, None)


async def main():
    """Run example analyses"""
    
    print("=" * 80)
    print("Camera Health Diagnostic Agent - LOCAL VERSION")
    print("Using Ollama with Llama 3.3 (fully open source)")
    print("=" * 80)
    
    # Initialize agent
    agent = CameraHealthAgentLocal()
    
    try:
        # Connect to MCP server
        await agent.connect_to_mcp()
        
        # Example 1: Simple query
        print("\n" + "=" * 80)
        print("EXAMPLE 1: List Camera Sites")
        print("=" * 80)
        
        await agent.analyze("List all camera sites and show me the first 5")
        
        # Example 2: Vehicle analysis
        print("\n\n" + "=" * 80)
        print("EXAMPLE 2: Vehicle Analysis")
        print("=" * 80)
        
        await agent.analyze(
            "Get details for vehicle 1488AVR and tell me if it has detection quality issues"
        )
        
        # Example 3: Complex analysis
        print("\n\n" + "=" * 80)
        print("EXAMPLE 3: Degraded Sites Detection")
        print("=" * 80)
        
        await agent.analyze(
            "Find degraded camera sites on 2025_08_10 with degradation above 50% and list them"
        )
        
    finally:
        await agent.close()


async def interactive_mode():
    """Run agent in interactive mode"""
    
    print("=" * 80)
    print("Camera Health Diagnostic Agent - LOCAL VERSION")
    print("Using Ollama with Llama 3.3 (fully open source, no API costs!)")
    print("=" * 80)
    
    agent = CameraHealthAgentLocal()
    
    try:
        await agent.connect_to_mcp()
        
        print("\nExamples of what you can ask:")
        print("  - List all camera sites")
        print("  - Get details for vehicle 1488AVR")
        print("  - Find degraded sites on 2025_08_10")
        print("  - Compare sites on Al Sahel Valley Street")
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
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--interactive":
        asyncio.run(interactive_mode())
    else:
        asyncio.run(main())
