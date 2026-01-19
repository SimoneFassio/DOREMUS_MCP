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
from server.tools_internal import (
    graph,
    find_candidate_entities_internal,
    get_entity_properties_internal,
    build_query_internal,
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
async def build_query(question: str, 
                      template: str, 
                      filters: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """
    **STEP 1: ALWAYS CALL THIS FIRST.**
    Build a SPARQL query safely using a predefined template.

    **CRITICAL INSTRUCTION:** Before calling this, if the user mentions specific names (e.g. "Beethoven", "Magic Flute"), 
    you SHOULD use the `search_entity` (or `find_candidate_entities`) tool first to get their URIs. 
    Passing URIs in `filters` is much more accurate than passing raw strings.

    This tool does NOT execute the query. It generates the SPARQL string and returns a `query_id`.
    The LLM should inspect the generated SPARQL. If it looks correct, use `execute_query(query_id)` to run it.
    If it is incorrect, call `build_query` again with adjusted filters.
    Use find_candidate_entities to find the URI of the filters.
    Use full names (always prefer name surname) for the filters, do not use initials for composers, places or instruments.

    Args:
        question: The user natural language question to answer.
        template: The conceptual domain to search within. CHOOSE CAREFULLY:
            - "works": For musical works (titles, composers, genres, keys...)
                       Use this even if searching by creator (e.g. "Works by Bach").
            - "performances": For events/performances (dates, locations, performers...)
                              Focuses on *when* and *where* something happened.
            - "artists": For finding artists (names, instruments, birth places...)
                         Use this to find *people*, not their works.
        filters: A dictionary of filters to apply.
            - For Works: 
                "title": String or URI, 
                "composer_name": String or URI, 
                "composer_nationality": String,
                "genre": String or URI, 
                "place_of_composition": String or URI, 
                "musical_key": String or URI
            - For Performances:
                "title": String,
                "location": String or URI,
                "carried_out_by": List of Strings or URIs
            - For Artists:
                "name": String or URI,
                "nationality": String,
                "birth_place": String,
                "death_place" : String,
                "work_name" : String or URI
            It may be possible that no filters are needed, in which case pass an empty dict or None.

    Returns:
        Dict containing:
        - "success": boolean
        - "query_id": The ID to use with `execute_query`
        - "generated_query": The generated SPARQL string for review
        - "message": The query output and a set of few-shot examples to follow
    """

    strategy_guide = ""
    # CATEGOPRY 3: Strict filtering
    if "strictly" in question.lower() or "exactly" in question.lower() or "quartet" in question.lower() or "trio" in question.lower():
        strategy_guide = """
        ### TYPE 3: Strict/Exact Instrumentation (Closed Sets)
        *Trigger:* "Strictly...", "Exactly...", "String Quartet" (implied set), "Trio"
        *Strategy:*
        1. `build_query`
        2. `associate_to_N_entities` for EACH instrument.
        3. `groupBy` to count the Total Number of Parts (ensuring no extra instruments).
        
        *Example:* "Works written for violin, clarinet and piano (strictly)"
        -> build_query(...)
        -> find_candidate_entities("violin", "vocabulary") -> violin_uri
        -> find_candidate_entities("clarinet", "vocabulary") -> clarinet_uri
        -> find_candidate_entities("piano", "vocabulary") -> piano_uri
        -> associate_to_N_entities(expression, violin_uri, q_id)
        -> associate_to_N_entities(expression, clarinet_uri, q_id)
        -> associate_to_N_entities(expression, piano_uri, q_id)
        -> groupBy(casting, q_id, castingDetail, COUNT, equal, 3) 
        (Note: Logic is 'equal 3' because we have 3 distinct instrument parts)
        
        *Example:* "Works for String Quartet" (2 Violins, 1 Viola, 1 Cello = 3 distinct parts usually)
        -> build_query(...)
        -> find_candidate_entities("violin", "vocabulary") -> violin_uri
        -> find_candidate_entities("viola", "vocabulary") -> viola_uri
        -> find_candidate_entities("cello", "vocabulary") -> cello_uri
        -> associate_to_N_entities(expression, violin_uri, q_id, 2)
        -> associate_to_N_entities(expression, viola_uri, q_id, 1)
        -> associate_to_N_entities(expression, cello_uri, q_id, 1)
        -> groupBy(casting, q_id, castingDetail, COUNT, equal, 3)
        """

    # CATEGORY 2: Open filters
    elif "for" in question.lower() or "at least" in question.lower() or "involving at least" in question.lower():
        strategy_guide = """
        ### TYPE 2: Open Instrumentation (Inclusion)
        *Trigger:* "Works for oboe...", "involving at least...", "for choir and orchestra"
        *Strategy:* 1. `build_query` (set template="Works")
        2. `associate_to_N_entities` for EACH instrument mentioned.
        3. `has_quantity_of` if a date/time is mentioned.
        4. DO NOT use `groupBy` (we allow other instruments to be present).
        
        *Example:* "Works written for oboe and orchestra after 1900"
        -> build_query(..., filters={})
        -> find_candidate_entities("oboe", "vocabulary") -> oboe_uri
        -> find_candidate_entities("orchestra", "vocabulary") -> orchestra_uri
        -> associate_to_N_entities(expression, oboe_uri, q_id)
        -> associate_to_N_entities(expression, orchestra_uri, q_id)
        -> has_quantity_of(expCreation, time-span, more, "01-01-1900", q_id)
        """

    # CATEGORY 1: simple metadata queries that can be asked with query builder -> default
    else: 
        strategy_guide = """
        ### TYPE 1: Simple Metadata (Composer, Genre, Title)
        *Trigger:* "Who composed...", "Works by...", "Sacred music..."
        *Strategy:* Use `build_query` with filters. Do NOT use entity associations unless instruments are mentioned.
        *Example:* "Works by Mozart"
        -> build_query(template="Artists", filters={"name": "Wolfgang Amadeus Mozart"})
        Review what has been done by the build_query tool and if necessary call it again
        """
    return await build_query_internal(question, template, filters, strategy_guide)

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
           - Pass an integer (e.g., 3) for exact matches ("for 3 violins").
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
          Output: generated_query="... FILTER ( ?quantity_val <= 900) ..."
        - Input: subject="expCreation", property="ecrm:P4_has_time-span", type="range", value="1870", valueEnd="1913", query_id="..."
          Output: generated_query="... FILTER ( ?start >= "1870"^^xsd:gYear AND ?end <= "1913"^^xsd:gYear) ..."
    """
    return await has_quantity_of_internal(subject, property, type, value, valueEnd, query_id)


# @mcp.tool()
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
async def add_select(
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
    """
    return await add_select_variable_internal(variable, aggregator, query_id)


@mcp.tool()
async def execute_query(query_id: str, limit: int = 50) -> Dict[str, Any]:
    """
    Execute a previously built SPARQL query by its ID.

    Use this tool AFTER calling `build_query`, any other optional tools and verifying the generated SPARQL.

    Args:
        query_id: The UUID returned by `build_query`.

    Returns:
        The results of the SPARQL query execution.
    """
    return execute_query_from_id_internal(query_id, limit)


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
