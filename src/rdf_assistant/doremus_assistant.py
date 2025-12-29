import asyncio
import os
from dotenv import load_dotenv
from langgraph.prebuilt import create_react_agent
from langchain_openai import ChatOpenAI
from langchain_groq import ChatGroq
from langchain_anthropic import ChatAnthropic
from langchain_ollama import ChatOllama

from .prompts import agent_system_prompt
from .extended_mcp_client import ExtendedMCPClient

load_dotenv(".env")

provider = os.getenv("LLM_EVAL_PROVIDER", "ollama")
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
def create_model(provider: str):
    """Create a chat model based on provider"""
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
            temperature=0)
    else:
        raise ValueError(f"Unknown provider: {provider}")
    
# AGENT LLM: Initialize the LLM, bind the tools from the MCP client
async def initialize_agent():
    tools = await client.get_tools()
    llm = create_model(provider)

    print("DOREMUS Assistant configuration:")
    print(f"  provider: {provider}")
    print(f"  selected_model: {evaluation_models.get(provider)}")
    print(f"  recursion_limit: {recursion_limit}")
    print(f"  MCP server: {mcp_url}, transport type: {mcp_transport}\n")

    # Compile the agent using LangGraph's create_react_agent
    agent = create_react_agent(
        llm,
        tools=tools,
        state_modifier=agent_system_prompt
    )
    return agent.with_config({"recursion_limit": recursion_limit})

doremus_assistant = asyncio.run(initialize_agent())