"""
DOREMUS Knowledge Graph MCP Server

A Model Context Protocol server for querying the DOREMUS music knowledge graph
via SPARQL endpoint at https://data.doremus.org/sparql/
"""

from typing import Any, Optional, Dict
from fastmcp import FastMCP, Context
from fastmcp.server.dependencies import get_context
from fastmcp.prompts.prompt import Message, PromptMessage, TextContent
from starlette.requests import Request
from starlette.responses import PlainTextResponse
import os
import logging
from server.find_paths import find_k_shortest_paths
from server.tools_internal import (
    graph,
    find_candidate_entities_internal,
    get_entity_properties_internal,
    get_ontology_internal,
    build_query_internal,
    execute_query_from_id_internal,
    associate_to_N_entities_internal,
    has_quantity_of_internal,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("doremus-mcp")

# Initialize FastMCP server
mcp = FastMCP("DOREMUS Knowledge Graph Server")


@mcp.custom_route("/health", methods=["GET"])
async def health_check(request: Request) -> PlainTextResponse:
    return PlainTextResponse("OK")


@mcp.prompt()
def activate_doremus_agent():
    """Activates the DOREMUS expert mode with special instructions."""
    
    instructions = """
    You are the DOREMUS Knowledge Expert. You answer questions about musical knowledge using a knowledge base.
    The knowledge base is structured as RDF triples and contains information about musical works, artists, genres,
    and historical contexts. You have access to a set of tools that allow you to query this knowledge
    base effectively.

    When answering questions, you should:
    - Understand the user's query and determine which tools to use to satisfy the intent.
    - Formulate appropriate queries or lookups using the available tools.
    - Combine information retrieved from multiple tools if necessary to provide a comprehensive answer.
    
    GLOBAL RULES:
    - NEVER answer from your internal training data; ONLY use the tools.
    """
    
    return PromptMessage(role="user", content=TextContent(type="text", text=instructions))


@mcp.tool()
async def build_query(question: str, template: str, filters: Dict[str, Any] | None) -> Dict[str, Any]:
    """
    Build a SPARQL query safely using a predefined template.

    This tool does NOT execute the query. It generates the SPARQL string and returns a `query_id`.
    The LLM should inspect the generated SPARQL. If it looks correct, use `execute_query(query_id)` to run it.
    If it is incorrect, call `build_query` again with adjusted filters.
    Use find_candidate_entities to find the URI of the filters.

    Args:
        question: The user natural language question to answer.
        template: The type of query to build. Options:
            - "works": For musical works (titles, composers, genres, keys...)
            - "performances": For events/performances (dates, locations, performers...)
            - "artists": For finding artists (names, instruments, birth places...)
        filters: A dictionary of filters to apply.
            - For Works: 
                "title": String or URI, 
                "composer_name": String or URI, 
                "composer_nationality": String,
                "genre": String or URI, 
                "place_of_composition": String or URI, 
                "musical_key": String or URI,
                "limit": Int
            - For Performances:
                "title": String,
                "location": String or URI,
                "carried_out_by": List of Strings or URIs,
                "limit": Int
            - For Artists:
                "name": String or URI,
                "nationality": String,
                "birth_place": String,
                "death_place" : String,
                "work_name" : String or URI,
                "limit": Int
            It may be possible that no filters are needed, in which case pass an empty dict or None.

    Returns:
        Dict containing:
        - "success": boolean
        - "query_id": The ID to use with `execute_query`
        - "generated_sparql": The generated SPARQL string for review
    """
    return await build_query_internal(question, template, filters)

@mcp.tool()
async def associate_to_N_entities(subject: str, obj: str, query_id: str, n: int | None) -> Dict[str, Any]:
    """
    Tool that inserts in the query a pattern associating the subject entity (i.e. "expression"), usually from the select
    and an object entity (i.e. "violin") n times (the number of entities).

    Use cases are: "Find all works composed for 3 violins", "Find all works performed by 2 pianists and 1 violinist", etc.

    Args:
        subject: The subject entity name for which we want to find a subgraph connected to object (e.g., "expression")
        obj: The object entity name to which the subject is connected (e.g., "violin")
        query_id: The ID of the query being built onto which this pattern will be applied.
        n (optional): The number of entities to associate. If we don't want to specify a number, pass None.
    
    Returns:
        Dict containing:
            - "success": boolean
            - "query_id": The ID to use with `execute_query`
            - "generated_sparql": The generated SPARQL string for review
    
    Example:
        Suppose that we receive as input prompt from the user to select all the musical works that were written for 3 violins.
        In this case, the tool will be called with:
        Input: subject="expression", object="violin", query_id="d75H8V9AWH", N=3
        Output: generated_sparql="... ?expression mus:U13_has_casting ?casting .
                                 ?casting mus:U23_has_casting_detail ?castingDet .
                                 ?castingDet mus:U2_foresees_use_of_medium_of_performance ?Violin .
                                 ?castingDet mus:U30_foresees_quantity_of_mop 3 . ..."
    """
    
    return await associate_to_N_entities_internal(subject, obj, query_id, n)

@mcp.tool()
async def has_quantity_of(subject: str, property: str, type: str, value: str, valueEnd: str | None, query_id: str) -> Dict[str, Any]:
    """
    Tool that receives as input the `subject` entity (i.e. “expression”, the name of the variable already present in the query) and the property to apply the pattern to (i.e. “mus:U78_estimated_duration”)
    For ecrm:P4_has_time-span, input format YYYY-MM-DD or YYYY is supported.

    Args:
        subject: The subject entity variable name (e.g., "expression")
        property: The property uri (e.g., "mus:U78_estimated_duration" or "time-span")
        type: "less", "more", "equal", or "range"
        value: value (number or date), in case of "range" type, it is the start value
        valueEnd: End value (number or date), required only for "range" type
        query_id: The ID of the query being built.

    Returns:
        Dict containing success status and generated SPARQL.

    Examples:
        - Input: subject="expression", property="mus:U78_estimated_duration", type="less", value="900", valueEnd="", query_id="..."
          Output: generated_sparql="... FILTER ( ?quantity_val <= 900) ..."
        - Input: subject="expCreation", property="ecrm:P4_has_time-span", type="range", value="1870", valueEnd="1913", query_id="..."
          Output: generated_sparql="... FILTER ( ?start >= "1870"^^xsd:gYear AND ?end <= "1913"^^xsd:gYear) ..."
    """
    return await has_quantity_of_internal(subject, property, type, value, valueEnd, query_id)


@mcp.tool()
async def execute_query(query_id: str) -> Dict[str, Any]:
    """
    Execute a previously built SPARQL query by its ID.

    Use this tool AFTER calling `build_query`, any other optional tools and verifying the generated SPARQL.

    Args:
        query_id: The UUID returned by `build_query`.

    Returns:
        The results of the SPARQL query execution.
    """
    return execute_query_from_id_internal(query_id)

@mcp.tool()
async def find_candidate_entities(
    name: str, entity_type: str = "others"
) -> dict[str, Any]:
    """
    Use this tool to discover the URI identifier for an entity before retrieving
    detailed information or using it in other queries.
    Entity names may have variations, and you need the exact URI to query reliably.

    Args:
        name: The name or keyword to search for (e.g., "Wolfgang Amadeus Mozart", "violin", "Radio France")
        entity_type: Search scope. Options:
            - "artist": Broad artist bucket covering people, ensembles, broadcasters, etc. Use COMPLETE names
            - "vocabulary": SKOS concepts such as genres, media of performance(instruments, etc.), keys (skos:Concept)
            - "others": Everything else; falls back to rdfs:label search (default)

    Returns:
        Dictionary with matching entities, including their URIs, labels, and reported RDF types

    Examples:
        - find_candidate_entities("Ludwig van Beethoven", "artist")
        - find_candidate_entities("violin", "vocabulary")
        - find_candidate_entities("Berlin", "others")
    """
    return find_candidate_entities_internal(name, entity_type)


@mcp.tool()
async def get_entity_properties(entity_uri: str) -> dict[str, Any]:
    """
    It shows all direct properties of a specific entity.

    Args:
        entity_uri: The full URI of the entity (e.g., "http://data.doremus.org/artist/...")

    Returns:
        Dictionary with:
        - entity_uri: The requested entity
        - entity_label: Human-readable name
        - entity_type: Class of the entity
        - properties: All properties as key-value pairs
    """
    return get_entity_properties_internal(entity_uri)


@mcp.tool()
def get_ontology(path: str) -> str:
    """
    Explore the DOREMUS ontology graph schema hierarchically.

    This tool helps you understand the structure of the knowledge graph by providing
    a hierarchical view of node types (classes) and their relationships (edges).

    Use this tool to:
    - Get an overview of the most important node types and connections (path='/')
    - Explore a specific class and its direct relationships

    Args:
        path: Navigation path for exploration:
            - '/' - Get a high-level summary of the top 15 most important node types
                   and their top 20 most common relationships
            - '/{ClassName}' - Explore a specific class (e.g., '/efrbroo:F28_Expression_Creation')

    Returns:
        Markdown-formatted visualization of the ontology subgraph, showing:
        - Node types (classes) in the knowledge graph
        - Edge types (predicates/relationships) connecting them
        - Hierarchical structure for easy understanding

    Examples:
        - get_ontology('/')
          Returns overview of the most important 15 nodes and their relationships

        - get_ontology('/efrbroo:F22_Self-Contained_Expression')
          Shows what properties and relationships a musical work has
    """
    return get_ontology_internal(path=path, depth=1)

# # Documentation tools

@mcp.tool()
def get_usage_guide() -> str:
    """
    USE THIS TOOL FIRST TO RETRIEVE GUIDANCE ON QUERYING DOREMUS

    Get a comprehensive usage guide and prompt for LLMs interacting with DOREMUS.

    This tool provides guidance on:
    - How to effectively use the available tools
    - Best practices for entity resolution
    """

    guide = """
# DOREMUS MCP Server - LLM Usage Guide

## Purpose
This MCP server provides access to the DOREMUS Knowledge Graph, a comprehensive
database of classical music metadata including works, composers, performances,
recordings, and instrumentation.
DOREMUS is based on the CIDOC-CRM ontology, using the EFRBROO (Work-Expression-Manifestation-Item) extension.
It is designed to describe how a musical idea is created, realized, and performed — connecting the intellectual, artistic, and material aspects of a work.
Work -> conceptual idea (idea of a sonata)
Expression -> musical realization (written notation of the sonata, with his title, composer, etc.)
Event -> performance or recording

It defines 7 vocabularies categories:
- Musical keys
- Modes
- Genres
- Media of performance (MoP)
- Thematic catalogs
- Derivation types
- Functions

## Workflow
Build the SPARQL query step by step:
1. get_ontology: explore the DOREMUS ontology graph schema
2. find_candidate_entities: discover the unique URI identifier for an entity
3. get_entity_properties: retrieve detailed information about a specific entity (all property)
4. build_query: build the base query using information collected
5. Use the most appropriate tool to write complex filters (like associate_to_N_entities)
6. execute_query: execute the query built
7. Check the query result, refine and use again tool to explore more the graph or restart from beginning if necessary
8. Once the result is ok, format it in a proper manner and write the response

## Remember
- The database is authoritative but not complete
- Always verify entity resolution before complex queries
- When in doubt, start simple and iterate
- Provide context and explanations, not just raw data
- Acknowledge limitations when encountered
"""

    return guide


if __name__ == "__main__":
    # Run the MCP server
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))

    mcp.run(transport="http", host=host, port=port)
