import asyncio
import os
from dotenv import load_dotenv
from langchain.agents import create_agent
from langchain_openai import ChatOpenAI
from langchain_groq import ChatGroq
from langchain_anthropic import ChatAnthropic
from langchain_ollama import ChatOllama
from langchain.agents.middleware import wrap_tool_call, wrap_model_call
from langchain.messages import ToolMessage
from langchain_core.rate_limiters import InMemoryRateLimiter
from pydantic import ValidationError

from .prompts import agent_system_prompt
from .extended_mcp_client import ExtendedMCPClient

load_dotenv(".env")

provider = os.getenv("LLM_EVAL_PROVIDER", "ollama")
model_name = os.getenv("LLM_EVAL_MODEL", "gpt-oss:120b")
recursion_limit = int(os.getenv("GRAPH_RECURSION_LIMIT", "10"))
mcp_url = os.getenv("DOREMUS_MCP_URL", "http://localhost:8000/mcp")
mcp_transport = os.getenv("DOREMUS_MCP_TRANSPORT", "streamable_http")


evaluation_models = {
    "openai": "gpt-4.1", 
    "groq": "llama-3.3-70b-versatile", # "meta-llama/llama-4-scout-17b-16e-instruct", 
    "anthropic": "claude-sonnet-4-5-20250929", 
    "mistral": "mistral-7b-instant",
    "ollama": "gpt-oss:120b",
    "cerebras": "llama3.1-70b",
    "zai": "glm-4.7-flash"
}

connections = {
        "DOREMUS_MCP": {
            "transport": mcp_transport,
            "url": mcp_url
    }
}

client = ExtendedMCPClient(
    connections=connections
)

cerebras_rate_limiter = InMemoryRateLimiter(
    requests_per_second=0.4, 
    check_every_n_seconds=0.1,
    max_bucket_size=10,
)

# Helper function to create model based on provider
def create_model(provider: str, model_name=None, api_key=None):
    """Create a chat model based on provider"""
    if model_name is None:
        model_name = evaluation_models[provider]
    
    if provider == "openai":
        return ChatOpenAI(model=model_name, temperature=0, api_key=api_key or os.getenv("OPENAI_API_KEY"))
    elif provider == "groq":
        return ChatGroq(model=model_name, temperature=0, api_key=api_key or os.getenv("GROQ_API_KEY"))
    elif provider == "anthropic":
        return ChatAnthropic(model=model_name, temperature=0, api_key=api_key or os.getenv("ANTHROPIC_API_KEY"))
    elif provider == "cerebras":
        return ChatOpenAI(
            base_url="https://api.cerebras.ai/v1",
            api_key=api_key or os.getenv("CEREBRAS_API_KEY"),
            model=model_name,
            temperature=0,
            rate_limiter=cerebras_rate_limiter
        )
    elif provider == "custom":
        return ChatOpenAI(
            base_url="http://localhost:8964/v1/",
            api_key="1234",
            model=model_name,
            temperature=0
        )
    elif provider == "zai":
        return ChatOpenAI(
            base_url="https://api.z.ai/api/paas/v4",
            api_key=api_key or os.getenv("ZAI_API_KEY"),
            model=model_name,
            temperature=0
        )
    elif provider == "ollama":
        if api_key:
            return ChatOllama(
                base_url="https://ollama.com",
                model=model_name,
                client_kwargs={"headers": {"Authorization": f"Bearer {api_key}"}},
                stream=True,
                temperature=0,
                num_ctx=32768,
            )
        else:
            return ChatOllama(
                base_url=os.getenv("OLLAMA_API_URL"),
                model=model_name,
                client_kwargs={"headers": {"Authorization": f"Basic {os.getenv('OLLAMA_API_KEY')}"}},
                stream=True,
                temperature=0,
                num_ctx=32768,
            )

    else:
        raise ValueError(f"Unknown provider: {provider}")

@wrap_tool_call
async def handle_tool_errors(request, handler):
    """Handle tool execution errors with custom messages."""
    try:
        response = await handler(request)
        return response
    except ValidationError as e:
        return ToolMessage(
            content=f"Tool Validation Error: The tool output format was unexpected. ({str(e)})",
            tool_call_id=request.tool_call["id"],
            name=request.tool_call["name"],
            status="error"
        )
    except Exception as e:
        # Return a custom error message to the model
        return ToolMessage(
            content=f"Tool error: Please check your input and try again. ({str(e)})",
            tool_call_id=request.tool_call["id"],
            name=request.tool_call["name"],
            status="error"
        )

import json
import uuid

@wrap_model_call
async def fix_hallucinated_json(request, handler):
    response = await handler(request)
    
    try:
        # 1. Extract the message
        message = response.result[0] if hasattr(response, 'result') else response
        content = getattr(message, 'content', "")
        
        # DEBUG: Log raw content if it appears empty or short to catch "thinking" traces
        if not content and not getattr(message, 'tool_calls', None):
             print(f"DEBUG: Empty content detected. Raw message: {message}")
             if hasattr(message, 'additional_kwargs'):
                  print(f"DEBUG: Additional Kwargs: {message.additional_kwargs}")

        # 2. Check if it's "Hallucinated JSON" (Text that should have been a Tool Call)
        if content and '"name":' in content and not getattr(message, 'tool_calls', None):
            print("🔧 Repairing hallucinated tool call...")
            
            # Extract the JSON block from the text
            try:
                # Find the start of the JSON-like structure
                start_marker = '{'
                start_pos = content.find(start_marker)
                
                if start_pos == -1:
                    raise ValueError("No JSON tool call found")
                    
                json_candidate = content[start_pos:]
                tool_data = None
                
                try:
                    # First try parsing the substring to the end
                    tool_data = json.loads(json_candidate)
                except json.JSONDecodeError:
                    # Heuristic: Find valid JSON by brace balancing
                    balance = 0
                    for i, char in enumerate(json_candidate):
                        if char == '{':
                            balance += 1
                        elif char == '}':
                            balance -= 1
                            if balance == 0:
                                tool_data = json.loads(json_candidate[:i+1])
                                break
                    
                    if tool_data is None:
                        raise ValueError("Could not extract valid JSON")

                # 3. MANUALLY INJECT the tool call into the message
                # This trick prevents the Graph from reaching __end__
                message.tool_calls = [{
                    "name": tool_data.get("name"),
                    "args": tool_data.get("arguments", tool_data.get("args", tool_data.get("parameters", {}))),
                    "id": f"call_{uuid.uuid4().hex[:12]}",
                    "type": "tool_call"
                }]
                
                # Clear the JSON from the content but keep the thought/reasoning
                message.content = content[:start_pos].strip()
                
            except (json.JSONDecodeError, ValueError):
                # If it's not valid JSON, we just append an error and let it end 
                # (or the LLM will see the error if you manually loop)
                message.content += "\n\nERROR: Invalid tool call format."
                
    except Exception as e:
        print(f"Middleware Error: {e}")
        
    return response
    
# AGENT LLM: Initialize the LLM, bind the tools from the MCP client
async def initialize_agent(api_key=None):
    tools = await client.get_tools()
    
    # Patch tools to ignore 'runtime' argument injected by LangGraph
    # functionality that conflicts with MCP tools
    for tool in tools:
        if hasattr(tool, "coroutine") and tool.coroutine:
            original_coro = tool.coroutine
            async def wrapped_coro(*args, original_coro=original_coro, **kwargs):
                kwargs.pop("runtime", None)
                return await original_coro(*args, **kwargs)
            tool.coroutine = wrapped_coro
            
        if hasattr(tool, "func") and tool.func:
            original_func = tool.func
            def wrapped_func(*args, original_func=original_func, **kwargs):
                kwargs.pop("runtime", None)
                return original_func(*args, **kwargs)
            tool.func = wrapped_func
    llm = create_model(provider, model_name, api_key=api_key)

    print("DOREMUS Assistant configuration:")
    print(f"  provider: {provider}")
    print(f"  selected_model: {model_name}")
    print(f"  recursion_limit: {recursion_limit}")
    print(f"  MCP server: {mcp_url}, transport type: {mcp_transport}\n")

    # Compile the agent using LangGraph's create_react_agent
    agent = create_agent(
        model=llm,
        tools=tools,
        system_prompt=agent_system_prompt,
        middleware=[handle_tool_errors, fix_hallucinated_json],
    )
    return agent.with_config({"recursion_limit": recursion_limit})

api_key = None
if os.getenv("API_KEYS_LIST", None):
    api_key = os.getenv("API_KEYS_LIST", "").split(",")[0]
doremus_assistant = asyncio.run(initialize_agent(api_key))