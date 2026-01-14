import asyncio
import os
from dotenv import load_dotenv
from langchain.agents import create_agent
from langchain_openai import ChatOpenAI
from langchain_groq import ChatGroq
from langchain_anthropic import ChatAnthropic
from langchain_ollama import ChatOllama
from langchain.agents.middleware import wrap_tool_call
from langchain.messages import ToolMessage

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
    "ollama": "gpt-oss:120b"
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

# Helper function to create model based on provider
def create_model(provider: str, model_name=None):
    """Create a chat model based on provider"""
    if model_name is None:
        model_name = evaluation_models[provider]
    
    if provider == "openai":
        return ChatOpenAI(model=model_name, temperature=0)
    elif provider == "groq":
        return ChatGroq(model=model_name, temperature=0)
    elif provider == "anthropic":
        return ChatAnthropic(model=model_name, temperature=0)
    elif provider == "ollama":
        return ChatOllama(
            base_url=os.getenv("OLLAMA_API_URL"),
            model=model_name,
            client_kwargs={"headers": {"Authorization": f"Basic {os.getenv('OLLAMA_API_KEY')}"}},
            stream=True,
            temperature=0
            )
    else:
        raise ValueError(f"Unknown provider: {provider}")

@wrap_tool_call
async def handle_tool_errors(request, handler):
    """Handle tool execution errors with custom messages."""
    try:
        return await handler(request)
    except Exception as e:
        # Return a custom error message to the model
        return ToolMessage(
            content=f"Tool error: Please check your input and try again. ({str(e)})",
            tool_call_id=request.tool_call["id"],
            name=request.tool_call["name"]
        )
    
# AGENT LLM: Initialize the LLM, bind the tools from the MCP client
async def initialize_agent():
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
    llm = create_model(provider, model_name)

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
        middleware=[handle_tool_errors],
    )
    return agent.with_config({"recursion_limit": recursion_limit})

doremus_assistant = asyncio.run(initialize_agent())