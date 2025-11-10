from typing import Literal
import asyncio
import os
import json

from rdf_assistant.extended_mcp_client import ExtendedMCPClient
from langchain.agents import create_agent

from rdf_assistant.prompts import agent_system_prompt

#TODO: maybe utils
#from rdf_assistant.utils import parse_content_input, show_graph

from langgraph.graph import StateGraph, START, END
from langgraph.types import Command
from dotenv import load_dotenv

# Load environmental variables
load_dotenv(".env")

evaluation_models = {
    "openai": "openai:gpt-4.1", 
    "groq": "groq:llama-3.1-8b-instant", 
    "anthropic": "claude-sonnet-4-5-20250929", 
    "mistral": "mistral-7b-instant"
}

connections = {
        "DOREMUS_MCP": {
            "transport": "sse",
            "url": os.getenv("DOREMUS_MCP_URL", "http://localhost:8000/sse")
    }
}

client = ExtendedMCPClient(
    connections=connections
)


model_name = evaluation_models["openai"]
    
# AGENT LLM: Initialize the LLM, bind the tools from the MCP client
async def initialize_agent():
    tools = await client.get_tools()

    # Compile the agent
    return create_agent(
        model_name,
        tools=tools,
        system_prompt=agent_system_prompt
    )

doremus_assistant = asyncio.run(initialize_agent())