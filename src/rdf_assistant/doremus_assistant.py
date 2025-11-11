from typing import Literal
import asyncio
import os
import json

from src.rdf_assistant.extended_mcp_client import ExtendedMCPClient
from langgraph.prebuilt import create_react_agent
from langchain_openai import ChatOpenAI
from langchain_groq import ChatGroq
from langchain_anthropic import ChatAnthropic

from src.rdf_assistant.prompts import agent_system_prompt

#TODO: maybe utils
#from rdf_assistant.utils import parse_content_input, show_graph

from langgraph.graph import StateGraph, START, END
from langgraph.types import Command
from dotenv import load_dotenv

# Load environmental variables
load_dotenv(".env")

evaluation_models = {
    "openai": "gpt-4.1", 
    "groq": "llama-3.1-8b-instant", 
    "anthropic": "claude-sonnet-4-5-20250929", 
    "mistral": "mistral-7b-instant"
}

# Choose which provider to use
provider = "openai"

connections = {
        "DOREMUS_MCP": {
            "transport": "sse",
            "url": os.getenv("DOREMUS_MCP_URL", "http://localhost:8000/sse")
    }
}

client = ExtendedMCPClient(
    connections=connections
)



# Helper function to create model based on provider
def create_model(provider: str, model_name: str):
    """Create a chat model based on provider"""
    if provider == "openai":
        return ChatOpenAI(model=model_name, temperature=0)
    elif provider == "groq":
        return ChatGroq(model=model_name, temperature=0)
    elif provider == "anthropic":
        return ChatAnthropic(model=model_name, temperature=0)
    else:
        raise ValueError(f"Unknown provider: {provider}")

model_name = evaluation_models[provider]
    
# AGENT LLM: Initialize the LLM, bind the tools from the MCP client
async def initialize_agent():
    tools = await client.get_tools()
    llm = create_model(provider, model_name)

    # Compile the agent using LangGraph's create_react_agent
    return create_react_agent(
        llm,
        tools=tools,
        state_modifier=agent_system_prompt
    )

doremus_assistant = asyncio.run(initialize_agent())