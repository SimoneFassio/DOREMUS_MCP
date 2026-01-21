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
from starlette.responses import PlainTextResponse, JSONResponse
import os
import logging
from server.find_paths import find_k_shortest_paths
from server.template_parser import initialize_templates

from server.tools_internal import (
    graph,
    find_candidate_entities_internal,
    get_entity_properties_internal,

    build_query_v2_internal,
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
async def build_query(
    question: str,
    template: str
) -> Dict[str, Any]:
    """
    **STEP 1: Create a query from a template.**
    
    This creates a base SPARQL query and returns available filters.
    Use `apply_filter` as STEP 2 to add constraints.
    Choose the template based on the type of entity the question is asking about.
    
    Args:
        question: The user's natural language question.
        template: Template to use. Options:
            - "expression": Musical works/expressions (efrbroo:F22_Self-Contained_Expression)
            - "performance": Performances/concerts (efrbroo:F31_Performance)
            - "artist": Artists/composers/performers (ecrm:E21_Person)
            - "recording_event": Recording sessions (efrbroo:F29_Recording_Event)
            - "track": Recorded tracks, e.g. recordings (mus:M24_Track)
    
    Returns:
        Dict with query_id, generated_query, and available_filters list.
    
    Example:
        build_query(question="Works by Mozart", template="expression")
        build_query(question="Name of artist that ...", template="artist")
        build_query(question="Concerts/performances at ...", template="performance")
        build_query(question="Recordings of ...", template="track") #Attention! a recording is a track
        build_query(question="Have been recorded ...", template="recording_event") #This is an EVENT
    """
    return await build_query_v2_internal(question, template)


@mcp.tool()
async def apply_filter(
    query_id: str,
    base_variable: str,
    template: str,
    filters: Dict[str, str]
) -> Dict[str, Any]:
    """
    Use after `build_query` to add constraints. Pass filter values as strings
    (labels like "Mozart", "opera") - URIs are resolved automatically.
    The base_variable can be every variable present in the query, choose the template based on the class of that variable, among the available ones.
    If the filter contains only filter_name, and "" as filter_value, triplets corresponding to the filter are added, but no filtering applied.
    
    Args:
        query_id: The query ID from build_query.
        base_variable: The variable to filter on (usually from build_query response).
        template: The template, same as build_query, containing the filter definitions.
        filters: Dict of filter_name -> filter_value.
            Example: {"composer_name": "Mozart", "genre": "opera"}
    
    Returns:
        Dict with updated SPARQL query.
    
    Example:
        apply_filter(query_id="abc123", base_variable="work", template="expression", filters={"composer_name": "Wolfgang Amadeus Mozart"})
        apply_filter(query_id="abc123", base_variable="performance2", template="performance", filters={"location": "{placeName}"})
    """
    return await filter_internal(query_id, base_variable, template, filters)


@mcp.tool()
async def associate_to_N_entities(
    subject: str, 
    obj: str, 
    query_id: str, 
    n: int | None = None) -> Dict[str, Any]:
    """
    Adds a constraint to the query to filter items based on their components or instrumentation. 
    It answers questions like "Find [Subject] that has [N] [Objects]".

    **WHEN TO USE:**
    Use this tool when the user specifies a QUANTITY of a specific COMPONENT.
    - "Works written for **3 violins**"
    - "Bands with **2 drummers**"
    - "Performances with a **string quarted** (2 violins, 1 viola and 1 cello)

    **CRITICAL CONSTRAINTS:**
    1. **Subject Existence:** The `subject` MUST be a variable that is ALREADY defined in the current query (e.g., 'expression', 'work').
    2. **Object vs Subject:** - `subject` = The "Container" or "Main Entity" (e.g., The Symphony).
       - `obj` = The "Ingredient" or "Instrument" (e.g., The Violin).

    Args:
        subject: The variable name of the PARENT entity currently being filtered. This variable must typically be contained in the `SELECT`. (e.g., "expression").
        obj: The specific COMPONENT or INSTRUMENT required. (e.g., "violin", "piano", "cello").
        query_id: The ID of the active query to modify.
        n: The specific QUANTITY of the object required. 
           - Pass an integer (e.g., 3) for exact matches ("for 3 violins") ONLY if the user explicitly asks for an exact number of components.
           - Pass `None` if the user just asks for the *presence* of the object without a specific count ("for violin").

    Returns:
        Dict: {"success": bool, "query_id": str, "generated_query": str}
    
    **FEW-SHOT EXAMPLES:**

    User: "Find all musical works composed for exactly 3 violins."
    Context: We are looking for 'works' (subject) that use 'violins' (obj).
    Call: associate_to_N_entities(
        subject="expression",
        obj="violin", 
        n=3,
        query_id="current_id"
    )

    User: "Show me pieces that use a piano." (No specific count)
    Call: associate_to_N_entities(
        subject="expression",
        obj="piano",
        n=None,
        query_id="current_id"
    )
    """
    if n is not None:
        try:
            n = int(n)
        except Exception:
            raise Exception(f"Invalid n: expected integer, got {n!r}")
        if n <= 0:
            raise Exception(f"Invalid n={n}. n must be a positive integer (or omit it).")

    return await associate_to_N_entities_internal(subject, obj, query_id, n)


@mcp.tool()
async def groupBy_having(
        subject: str, 
        query_id: str, 
        obj: str | None = None,
        function: str | None = None,  
        logic_type: str | None = None, 
        valueStart: str | None = None, 
        valueEnd: str | None = None) -> Dict[str, Any]:
    """
    Applies a GROUP BY aggregation to an existing SPARQL query, specifically to filter groups based on calculated metrics (like counts or averages).
    
    **WHEN TO USE:**
    Use this tool ONLY when the user asks for:
    1. Aggregations: "Count the number of...", "Calculate the average..."
    2. Group Filters: "...which are written for a string quarted (exactly 4 instruments)", "...with an average rating LESS than 3".
    
    **DO NOT USE:**
    - For simple property filters (e.g., "Find works released in 2020"). Use the standard `build_query` tool for that.
    - If the user has not yet started a query (requires a valid `query_id`).

    Args:
        subject: The variable/entity to GROUP BY. This is the "bucket" or "category". (e.g., If counting instruments per Casting, this is 'casting').
        query_id: The ID of the active query to modify.
        obj: The variable/entity to MEASURE or COUNT inside the group. (e.g., If counting movies per Director, this is 'Movie'). 
             REQUIRED if a 'function' is specified.
        function: The mathematical operation to apply to the 'obj'. 
             Valid options: 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX'.
        logic_type: The comparison operator for the HAVING clause. 
             Valid options:
             - 'more' (applies >)
             - 'less' (applies <)
             - 'equal' (applies =)
             - 'range' (applies a filter between valueStart and valueEnd)
        valueStart: The threshold number for the logic_type. (e.g., if logic_type is 'more', and valueStart is '5', it means '> 5').
        valueEnd: The upper bound number. ONLY used if logic_type is 'range'.

    Returns:
        Dict: {"success": bool, "query_id": str, "generated_query": str}

    **FEW-SHOT EXAMPLES:**
    
    User: "Give me works that are written for three instruments"
    Context: We are grouping castings by the count of casting details
    Call: groupBy_having(
        subject="Casting",
        obj="castingDetail",
        function="COUNT",
        logic_type="equal",
        valueStart="3"
    )

    User: "List directors with an average movie rating higher than 8."
    Call: groupBy_having(
        subject="Director", 
        obj="Rating", 
        function="AVG", 
        logic_type="more", 
        valueStart="8"
    )
    """
    return await groupBy_having_internal(subject.lower(), query_id, function, obj, logic_type, valueStart, valueEnd)


@mcp.tool()
async def has_quantity_of(subject: str, property: str, type: str, value: str, valueEnd: str | None, query_id: str) -> Dict[str, Any]:
    """
    Tool that receives as input the `subject` entity (i.e. “expression”, the name of the variable already present in the query) and the property to apply the pattern to (i.e. “mus:U78_estimated_duration”)
    For ecrm:P4_has_time-span, input format YYYY-MM-DD or YYYY is supported.

    Args:
        subject: The subject entity variable name (e.g., "expCreation")
        property: The property uri (e.g., "mus:U78_estimated_duration" or "ecrm:P4_has_time-span")
        type: "less", "more", "equal", or "range". Do not use "equal" for dates, use "range" with the same value for start and end.
        value: value (number or date), in case of "range" type, it is the start value
        valueEnd: End value (number or date), required only for "range" type
        query_id: The ID of the query being built.

    Returns:
        Dict containing success status and generated SPARQL.

    Examples:
        - Input: subject="expression", property="mus:U78_estimated_duration", type="less", value="PT1H10M", valueEnd="", query_id="..."
          Output: generated_query="... ?expression mus:U78_estimated_duration ?quantity_val ... FILTER ( ?quantity_val <= "PT1H10M"^^xsd:duration) ..." (ISO 8601 duration format)
        - Input: subject="expCreation", property="ecrm:P4_has_time-span", type="range", value="1870", valueEnd="1913", query_id="..."
          Output: generated_query="... ?expCreation ecrm:P4_has_time-span/... ?start ... FILTER ( ?start >= "1870"^^xsd:gYear AND ?end <= "1913"^^xsd:gYear) ..."
        - Input: subject="expression", property="ecrm:P4_has_time-span", type="more", value="1870", query_id="..."
          Output: generated_query="... ?expCreation ecrm:P4_has_time-span/... ?start ... FILTER ( ?start >= "1870"^^xsd:gYear) ..."
    """
    return await has_quantity_of_internal(subject, property, type, value, valueEnd, query_id)


@mcp.tool()
async def add_triplet(
    subject: str, 
    subject_class: str, 
    property: str, 
    obj: str, 
    obj_class: str, 
    query_id: str
) -> Dict[str, Any]:
    """
    Add a general triplet to the query, validating it with a dry run.
    Use this tool ONLY when with the other tools is impossible to write the correct pattern, NO EXCEPTION.
    Use the get_ontology tool to explore the DOREMUS ontology graph schema.

    Args:
        subject: The variable name of the subject (e.g. "expression").
        subject_class: The URI class of the subject (e.g. "efrbroo:F22_Self-Contained_Expression").
        property: The property URI (e.g. "efrbroo:R17_created").
        obj: The variable name of the object (e.g. "creation").
        obj_class: The URI class of the object (e.g. "efrbroo:F28_Expression_Creation").
        query_id: The active query ID.

    Returns:
        Dict containing success status and the updated SPARQL query if successful.
        If the triplet causes an error or returns 0 results, it is discarded and an error is returned.
    """
    return await add_triplet_internal(subject, subject_class, property, obj, obj_class, query_id)


@mcp.tool()
async def select_aggregate_variable(
    variable: str,
    query_id: str,
    aggregator: Optional[str] = None
) -> Dict[str, Any]:
    """
    Add a variable to the SELECT clause of the query, optionally with an aggregator.
    
    This tool is used to explicitly include a variable in the final result.
    If the variable is already selected, this tool can be used to update its aggregator (e.g. adding 'COUNT' or 'SAMPLE').
    
    Args:
        variable: The name of the variable to select (e.g., "title", "composer").
        query_id: The active query ID.
        aggregator: Optional aggregator function (e.g., "COUNT", "SAMPLE", "MIN", "MAX", "AVG").
                    If None, the variable is selected as is.
    
    Returns:
        Dict containing success status and the updated SPARQL query.

    Examples:
        - Input: variable="expression", query_id="...", aggregator="COUNT"
          Output: generated_query="... SELECT (COUNT(?expression) AS ?expression) ..."
    """
    return await add_select_variable_internal(variable, aggregator, query_id)


@mcp.tool()
async def execute_query(query_id: str, limit: int = 10, order_by_variable: str | None = None, order_by_desc: bool = False) -> Dict[str, Any]:
    """
    Execute a previously built SPARQL query by its ID.

    Use this tool AFTER calling `build_query`, any other optional tools and verifying the generated SPARQL.

    Args:
        query_id: The UUID returned by `build_query`.
        limit: Max results (default is 10, max 50).
        order_by_variable: Optional variable name to sort results by (e.g., "date", "title"). Use only if required.
        order_by_desc: If True, sort in descending order. Default is False (ascending).

    Returns:
        The results of the SPARQL query execution.
    """
    return execute_query_from_id_internal(query_id, limit, order_by_variable, order_by_desc)


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
            - "place": ECRM places and geonames (ecrm:E53_Place)
            - "others": Everything else (rdfs:label), automatic fallback in case no other result is found

    Returns:
        Dictionary with matching entities, including their URIs, labels, and reported RDF types

    Examples:
        - find_candidate_entities("Ludwig van Beethoven", "artist")
        - find_candidate_entities("violin", "vocabulary")
        - find_candidate_entities("Berlin", "place")
    """
    return find_candidate_entities_internal(name, entity_type)


@mcp.tool()
async def get_entity_properties(entity_uri: str) -> dict[str, Any]:
    """
    It shows all direct properties of a specific entity (e.g., "http://data.doremus.org/artist/...") or of a class (e.g., "ecrm:E21_Person").
    """
    return get_entity_properties_internal(entity_uri)


# # Documentation tools

# @mcp.tool()
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
TODO add high level description of the graph

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
- Answer only with information provided by the execution of the query.
"""

    return guide


if __name__ == "__main__":
    # Run the MCP server
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))

    mcp.run(transport="http", host=host, port=port)
