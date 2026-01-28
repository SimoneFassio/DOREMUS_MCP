"""
DOREMUS Knowledge Graph MCP Server

A Model Context Protocol server for querying the DOREMUS music knowledge graph
via SPARQL endpoint at https://data.doremus.org/sparql/
"""
import os
import logging
from typing import Any, Optional, Dict, Callable, Set
from fastmcp import FastMCP, Context
from fastmcp.prompts.prompt import Message, PromptMessage, TextContent
from starlette.requests import Request
from starlette.responses import PlainTextResponse, JSONResponse
from server.config_loader import load_tool_config
from server.template_parser import initialize_templates
from server.tools_internal import (
    graph,
    find_candidate_entities_internal,
    get_entity_properties_internal,
    build_query_internal,
    filter_internal,
    execute_query_from_id_internal,
    associate_to_N_entities_internal,
    has_quantity_of_internal,
    groupBy_having_internal,
    groupBy_having_internal,
    add_triplet_internal,
    add_select_variable_internal,
    QUERY_STORAGE
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("doremus-mcp")

# ------------------------
# TOOL ACTIVATIONS
# ------------------------

def _parse_csv_env(name: str) -> Set[str]:
    raw = os.getenv(name, "build_query,apply_filter,add_component_constraint,groupBy_having,filter_by_quantity,add_triplet,select_aggregate_variable").strip()
    if not raw:
        return set()
    return {x.strip() for x in raw.split(",") if x.strip()}

MCP_ENABLED_TOOLS = _parse_csv_env("MCP_ENABLED_TOOLS")

def is_tool_enabled(tool_name: str) -> bool:
    if tool_name not in MCP_ENABLED_TOOLS:
        return False
    else:
        return tool_name in MCP_ENABLED_TOOLS
    
def tool_if_enabled(tool_name: str, description: Optional[str] = None) -> Callable:
    """
    Decorator: registers the function as an MCP tool only if enabled.
    Disabled tools won't appear in the server's advertised tool list.
    """
    def _decorator(fn):
        if is_tool_enabled(tool_name):
            logger.info(f"[tools] enabled: {tool_name}")
            desc = description or load_tool_config(tool_name)
            return mcp.tool(name=tool_name, description=desc)(fn)
        logger.info(f"[tools] disabled: {tool_name}")
        return fn  # not registered as a tool
    return _decorator

# Initialize templates at startup
try:
    initialize_templates()
except Exception as e:
    logger.error(f"Failed to initialize templates: {e}")

# Initialize FastMCP server
mcp = FastMCP("DOREMUS Knowledge Graph Server")


@mcp.custom_route("/health", methods=["GET"])
async def health_check(request: Request) -> PlainTextResponse:
    return PlainTextResponse("OK")


@mcp.custom_route("/sampling/{query_id}", methods=["GET"])
async def get_sampling_logs(request: Request) -> Any:
    """
    Get sampling logs for a specific query ID.
    """
    query_id = request.path_params["query_id"]
    
    if query_id not in QUERY_STORAGE:
        return JSONResponse({"error": f"Query ID {query_id} not found"}, status_code=404)
        
    qc = QUERY_STORAGE[query_id]
    return JSONResponse(qc.sampling_logs)


@mcp.prompt()
def activate_doremus_agent():
    """Activates the agent expert mode with special instructions."""
    
    instructions = load_tool_config("activate_doremus_agent")
    
    return PromptMessage(role="user", content=TextContent(type="text", text=instructions))


@tool_if_enabled("build_query")
async def build_query(
    question: str,
    template: str
) -> Dict[str, Any]:
    return await build_query_internal(question, template)


@tool_if_enabled("apply_filter")
async def apply_filter(
    query_id: str,
    target_variable: str,
    schema_template: str,
    filters: Dict[str, str]
) -> Dict[str, Any]:
    return await filter_internal(query_id, target_variable, schema_template, filters)


@tool_if_enabled("add_component_constraint")
async def add_component_constraint(
    source_variable: str, 
    target_component: str, 
    query_id: str, 
    exact_count: int | str | None = None
) -> Dict[str, Any]:
    return await associate_to_N_entities_internal(source_variable, target_component, query_id, exact_count)


@tool_if_enabled("groupBy_having")
async def groupBy_having(
        group_by_variable: str, 
        query_id: str, 
        aggregated_variable: str | None = None,
        aggregate_function: str | None = None,  
        having_logic_type: str | None = None, 
        having_value_start: str | None = None, 
        having_value_end: str | None = None
) -> Dict[str, Any]:
    return await groupBy_having_internal(group_by_variable, query_id, aggregate_function, aggregated_variable, having_logic_type, having_value_start, having_value_end)


@tool_if_enabled("filter_by_quantity")
async def filter_by_quantity(
    filter_target_variable: str, 
    quantity_property: str, 
    math_operator: str, 
    value_start: str, 
    value_end: str | None, 
    query_id: str
) -> Dict[str, Any]:
    return await has_quantity_of_internal(filter_target_variable, quantity_property, math_operator, value_start, value_end, query_id)


@tool_if_enabled("add_triplet")
async def add_triplet(
    subject: str, 
    subject_class: str, 
    property: str, 
    obj: str, 
    obj_class: str, 
    query_id: str
) -> Dict[str, Any]:
    return await add_triplet_internal(subject, subject_class, property, obj, obj_class, query_id)


@tool_if_enabled("select_aggregate_variable")
async def select_aggregate_variable(
    projection_variable: str,
    query_id: str,
    select_aggregator: Optional[str] = None
) -> Dict[str, Any]:
    return await add_select_variable_internal(projection_variable, select_aggregator, query_id)


@mcp.tool(name="execute_query", description=load_tool_config("execute_query"))
async def execute_query(
    query_id: str, 
    limit: int = 10, 
    order_by_variable: str | None = None, 
    order_by_desc: bool = False
) -> Dict[str, Any]:
    return execute_query_from_id_internal(query_id, limit, order_by_variable, order_by_desc)


@mcp.tool(name="find_candidate_entities", description=load_tool_config("find_candidate_entities"))
async def find_candidate_entities(
    name: str, entity_type: str = "others"
) -> dict[str, Any]:
    return find_candidate_entities_internal(name, entity_type)


@mcp.tool(name="get_entity_properties", description=load_tool_config("get_entity_properties"))
async def get_entity_properties(
    entity_uri: str
) -> dict[str, Any]:
    return get_entity_properties_internal(entity_uri)


if __name__ == "__main__":
    # Run the MCP server
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))

    mcp.run(transport="http", host=host, port=port)
