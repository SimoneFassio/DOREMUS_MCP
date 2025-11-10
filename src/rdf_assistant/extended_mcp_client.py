from langsmith.client import Client
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_mcp_adapters.sessions import create_session
from langchain_mcp_adapters.tools import load_mcp_tools
from langchain_core.tools import BaseTool
from langchain_core.documents.base import Blob
from langchain_core.messages import AIMessage, HumanMessage
from typing import Any, AsyncIterator, Optional
from contextlib import asynccontextmanager

class ExtendedMCPClient(Client):
    """
    Extended MCP Client that combines the functionality of LangSmith's Client
    and MultiServerMCPClient.
    """

    def __init__(self, connections: Optional[dict[str, Any]] = None):
        super().__init__()
        self.connections = connections if connections else {}

    @asynccontextmanager
    async def session(self, server_name: str, *, auto_initialize: bool = True) -> AsyncIterator[Any]:
        """
        Connect to an MCP server and initialize a session.

        Args:
            server_name: Name to identify this server connection
            auto_initialize: Whether to automatically initialize the session

        Yields:
            An initialized ClientSession
        """
        if server_name not in self.connections:
            raise ValueError(f"Server '{server_name}' not found in connections.")

        async with create_session(self.connections[server_name]) as session:
            if auto_initialize:
                await session.initialize()
            yield session

    async def get_tools(self, *, server_name: Optional[str] = None) -> list[BaseTool]:
        """
        Get a list of tools from a specific server or all servers.

        Args:
            server_name: Optional name of the server to get tools from.

        Returns:
            A list of tools.
        """
        if server_name:
            if server_name not in self.connections:
                raise ValueError(f"Server '{server_name}' not found in connections.")

            return await load_mcp_tools(None, connection=self.connections[server_name])

        tools = []
        for name, connection in self.connections.items():
            tools.extend(await load_mcp_tools(None, connection=connection))
        return tools

    async def get_resources(self, server_name: str, *, uris: Optional[list[str]] = None) -> list[Blob]:
        """
        Get resources from a specific server.

        Args:
            server_name: Name of the server to get resources from.
            uris: Optional list of resource URIs to fetch.

        Returns:
            A list of resources.
        """
        async with self.session(server_name) as session:
            return await session.get_resources(uris=uris)

    async def get_prompt(self, server_name: str, prompt_name: str, *, arguments: Optional[dict[str, Any]] = None) -> list[HumanMessage | AIMessage]:
        """
        Get a prompt from a specific server.

        Args:
            server_name: Name of the server to get the prompt from.
            prompt_name: Name of the prompt to fetch.
            arguments: Optional arguments for the prompt.

        Returns:
            A list of messages representing the prompt.
        """
        async with self.session(server_name) as session:
            return await session.get_prompt(prompt_name, arguments=arguments)