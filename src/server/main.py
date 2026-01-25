"""
DOREMUS Knowledge Graph MCP Server

A Model Context Protocol server for querying the DOREMUS music knowledge graph
via SPARQL endpoint at https://data.doremus.org/sparql/
"""
import os
import logging
from typing import Any, Optional, Dict, Callable, Set
from fastmcp import FastMCP, Context
from fastmcp.server.dependencies import get_context
from fastmcp.prompts.prompt import Message, PromptMessage, TextContent
from starlette.requests import Request
from starlette.responses import PlainTextResponse, JSONResponse
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
    
def tool_if_enabled(tool_name: str) -> Callable:
    """
    Decorator: registers the function as an MCP tool only if enabled.
    Disabled tools won't appear in the server's advertised tool list.
    """
    def _decorator(fn):
        if is_tool_enabled(tool_name):
            logger.info(f"[tools] enabled: {tool_name}")
            return mcp.tool()(fn)
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
    """Activates the DOREMUS expert mode with special instructions."""
    
    instructions = """
<IDENTITY>
You are the DOREMUS Knowledge Expert, an autonomous agent specialized in the DOREMUS musical ontology (FRBRoo/CIDOC-CRM).
Your mission is to translate natural language questions into precise SPARQL query chains to answer the user's questions.
</IDENTITY>

<DISCOVERY_PROTOCOL>
CRITICAL: You must NEVER guess a URI, a property name, or a graph structure. 
Before building a query, you MUST use discovery tools in these scenarios:
1. UNCERTAIN ENTITY: Use `find_candidate_entities` if the user mentions a specific artist, instrument, or genre.
2. SCHEMA UNCERTAINTY: Use `get_entity_properties` on a class or a specific URI to see which properties are available for filtering.
3. ONTOLOGY NAVIGATION: If you don't know how a Work connects to a specific attribute, use discovery tools to find the path.
</DISCOVERY_PROTOCOL>

<QUERY_CONSTRUCTION_STEPS>
You must follow this sequence for every request:
1. ANALYSIS: Identify the core entity (Work, Performance, Artist, etc.).
2. RESOLUTION: Call `find_candidate_entities` for any named entities in the prompt.
3. INITIALIZATION: Call `build_query` using the appropriate template.
4. REFINEMENT: 
   - Use `apply_filter` for standard attributes (title, name).
   - Use `add_component_constraint` for instrumentation counts (e.g., "3 violins").
   - Use `filter_by_quantity` for Dates (Creation Event) and Durations (Expression).
   - Use `groupBy_having` for complex counts (e.g., "Exactly 4 instruments").
5. PROJECTION: Use `select_aggregate_variable` to ensure the correct columns (or COUNTs) are returned.
6. EXECUTION: Call `execute_query`.
</QUERY_CONSTRUCTION_STEPS>

<CONSTRAINTS>
- SUBJECT LOGIC: When filtering by DATE, the subject must be the Event (eg. `expCreation`), not the Work itself.
- AGGREGATION: Only use `groupBy_having` if you are filtering the results. Use `select_aggregate_variable` if you are just displaying a count.
- NO HALLUCINATION: Only answer based on tool outputs. If the tools return no results, explain that the information is missing from the DOREMUS KG.
- THINKING: Do not call tools inside <think> tags. Output tool calls as pure JSON according to the MCP protocol.
</CONSTRAINTS>

<DOREMUS_SCHEMA_MAP>
DOREMUS uses the EFRBROO (Work-Expression-Manifestation-Item) extension.
It is designed to describe how a musical idea is created, realized, and performed
- Work/Expression: Concept/Title/Composer.
- Performance/Recording_Event: Live concerts and events.
- Track: The actual recording/audio file.
- Media of Performance (MoP): Instruments and voices.
</DOREMUS_SCHEMA_MAP>    """
    
    return PromptMessage(role="user", content=TextContent(type="text", text=instructions))


@tool_if_enabled("build_query")
async def build_query(
    question: str,
    template: str
) -> Dict[str, Any]:
    """
INITIALIZATION TOOL: Sets the entry point for the graph walk. 
This tool defines the core variables that ALL subsequent tools will reference.

**TEMPLATE ARCHITECTURE (KNOW YOUR VARIABLES):**
Each template initializes a specific set of variables in the SPARQL graph:

1. "artist": Focuses on People (ecrm:E21_Person).
    - Variables: ?artist, ?name
2. "expression": Focuses on Musical Works (efrbroo:F22_Self-Contained_Expression).
    - Variables: ?expression, ?title, ?expCreation (The creation event)
3. "performance": Focuses on Live Events (efrbroo:F31_Performance).
    - Variables: ?performance, ?title
4. "recording_event": Focuses on the Recording Session (efrbroo:F29_Recording_Event).
    - Variables: ?recordingEvent, ?title
5. "track": Focuses on Audio Tracks (mus:M24_Track).
    - Variables: ?track, ?title, ?performance, ?work, ?workTitle
    - NOTE: This template automatically links the audio track to its underlying performance and work.

**DISTINCTION GUIDE:**
- Use "performance" for live concerts/historical recitals.
- Use "recording_event" for the event of a recording session.
- Use "track" for questions about digital files, albums, or specific audio recordings of a work.

**WHEN TO USE:**
- Always use this tool as the **FIRST STEP** for any new question.
- Select the template based on the *primary subject* the user is asking about.

Args:
    question: The user's natural language question.
    template: Options: "expression", "performance", "artist", "recording_event", "track".

Returns:
    Dict: Includes `query_id` (required for all other tools) and the `generated_sparql`.

**FEW-SHOT EXAMPLES:**
- User: "Find sonatas by Beethoven" -> template="expression"
- User: "Who is the composer of..." -> template="artist"
- User: "Concerts in Paris during 2024" -> template="performance"
- User: "Recordings of 'The Magic Flute'" -> template="track" #Attention! a recording is a track
- User: "Have been recorded ..." -> template="recording_event" #This is an EVENT
"""
    return await build_query_v2_internal(question, template)


@tool_if_enabled("apply_filter")
async def apply_filter(
    query_id: str,
    target_variable: str,
    schema_template: str,
    filters: Dict[str, str]
) -> Dict[str, Any]:
    """
REFINEMENT TOOL: Adds RDF triplets to the query (WHERE clauses) to bind new variables or filter existing ones.
Use this to filter by names, titles, locations, etc.

**CRITICAL USAGE RULES:**
1. `target_variable`: The variable ALREADY in the query you want to filter (e.g., 'expression').
    - *Usually* this is the main variable chosen in build_query (e.g., 'expression', 'work', 'artist', 'recording_event', 'track').
2. `schema_template`: You MUST choose the correct category to unlock specific filter keys.
3. `filters`: A dictionary of { "filter_key": "value" }.
    - **Value = URI/String**: If you have a URI from `find_candidate_entities`, use it as the value!
    - **Value = ""**: Simply adds the triplets to the graph so the variable can be used later.

**VALID FILTER KEYS BY TEMPLATE:**
- "expression": title, composer_name, composer_nationality(Country Code), genre, composition_place, musical_key
- "performance": date, location, performer(Name)
- "artist": name, birth_place, nationality(Country Code), death_place, work_title
- "recording_event": title, recorded_by, performed_by, location, recorded_performance
- "track": work_title, composer_name, genre

Args:
    query_id: The ID of the active query.
    target_variable: The variable name to attach the filter/triplets to (e.g., "expression").
    schema_template: Options: "expression", "performance", "artist", "recording_event", "track".
    filters: Dictionary of attributes to bind/filter.

Returns:
    Dict: {"success": bool, "query_id": str, "generated_sparql": str}

**FEW-SHOT EXAMPLES:**

Example 1: (Binding a variable for display)
User: "Show me works and their genres"
Logic: Bind the 'genre' triplets so 'genre' can be selected later.
Call: apply_filter(target_variable="expression", schema_template="expression", 
                    filters={"genre": ""}, query_id="...")

Example 2: (Strict filtering)
User: "Operas by Mozart"
Logic: Restrict composer to Mozart AND genre to Opera.
Call: apply_filter(target_variable="expression", schema_template="expression", 
                    filters={"composer_name": "Wolfgang Amadeus Mozart", "genre": "Opera"}, query_id="...")
"""
    return await filter_internal(query_id, target_variable, schema_template, filters)


@tool_if_enabled("add_component_constraint")
async def add_component_constraint(
    source_variable: str, 
    target_component: str, 
    query_id: str, 
    exact_count: int | str | None = None) -> Dict[str, Any]:
    """
RELATIONSHIP TOOL: Automatically finds the path to link a component to a parent entity.
Use this for any entity found in the vocabulary (Instruments, Genres, Roles, etc.).
It answers questions like "Find [source_variable] that has optional(exact_count) [target_component]".

**CAPABILITIES:**
- AUTOMATIC PATHFINDING: This tool discovers the necessary RDF triplets to connect your 
    `source_variable` to the `target_component` within the DOREMUS ontology.
- SMART FILTERING: It can handle both existence ("with a violin") and specific 
    quantities ("for exactly 3 violins").

**WHEN TO USE:**
Use this tool when the user specifies a QUANTITY of a specific COMPONENT.
- "Works written for **3 violins**"
- "Bands with **2 drummers**"
- "Performances with a **string quarted** (2 violins, 1 viola and 1 cello)

**CRITICAL CONSTRAINTS:**
1. `source_variable`: Must be an existing variable in your query (e.g., 'expression', 'performance').
2. `target_component`: The name of the concept to link (e.g., 'piano', 'baritone', 'sonata'). 
    *Recommendation: Use `find_candidate_entities` first.*
3. `exact_count`: 
    - Pass an **Integer** ONLY if the user specifies a number (e.g., "for 2 flutes").
    - Pass **None** if the user just mentions the item (e.g., "pieces with flute").

Args:
    source_variable: The existing variable to start the path from (e.g., "expression").
    target_component: The entity/concept to find and link to (e.g., "cello").
    query_id: The ID of the active query.
    exact_count: The specific number required (Optional).
Returns:
    Dict: {"success": bool, "query_id": str, "generated_query": str}

**FEW-SHOT EXAMPLES:**

Example 1: "Find works for exactly 3 violins"
Logic: Link 'expression' to 'violin' with a specific count.
Call: add_component_constraint(source_variable="expression", target_component="violin", exact_count=3, query_id="...")

Example 2: "Show me pieces that use a piano"
Logic: Link 'expression' to 'piano' without requiring a specific quantity.
Call: add_component_constraint(source_variable="expression", target_component="piano", exact_count=None, query_id="...")
"""
    return await associate_to_N_entities_internal(source_variable, target_component, query_id, exact_count)


@tool_if_enabled("groupBy_having")
async def groupBy_having(
        group_by_variable: str, 
        query_id: str, 
        aggregated_variable: str | None = None,
        aggregate_function: str | None = None,  
        having_logic_type: str | None = None, 
        having_value_start: str | None = None, 
        having_value_end: str | None = None) -> Dict[str, Any]:
    """
Performs a GROUP BY aggregation and applies an optional HAVING filter to the results.

**SPARQL STRUCTURE:**
SELECT ?group_by_variable (?aggregate_function(?aggregated_variable) AS ?count)
WHERE { ... }
GROUP BY ?group_by_variable
HAVING (?count [having_logic_type] having_value_start)

**WHEN TO USE:**
Use this to filter "buckets" of data based on a count or average.
- "Find works (group_by) with exactly 3 (value) instruments (aggregated)."
- "List composers (group_by) with more than 10 (value) performances (aggregated)."

**DO NOT USE:**
- For simple property filters (e.g., "Find works released in 2020"). Use the standard `build_query` tool for that.
- If the user has not yet started a query (requires a valid `query_id`).

Args:
    group_by_variable: The main entity that stays in the result list (e.g., "expression", "artist", "genre").
    query_id: The ID of the active query.
    aggregated_variable: The sub-entity being counted or measured (e.g., "castingDetail", "track").
    aggregate_function: The math operation: 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX'.
    having_logic_type: Comparison for the HAVING clause: 'more' (>), 'less' (<), 'equal' (=), 'range' (BETWEEN).
    having_value_start: The threshold number for the logic.
    having_value_end: Only required if having_logic_type is 'range'.

Returns:
    Dict: {"success": bool, "query_id": str, "generated_query": str}

**FEW-SHOT EXAMPLES:**

User: "Give me works that are written for 3 instruments"
Context: We are grouping castings by the count of casting details
Call: groupBy_having(
    group_by_variable="Casting",
    aggregated_variable="castingDetail",
    aggregate_function="COUNT",
    having_logic_type="equal",
    having_value_start="3"
)

User: "List directors with an average movie rating higher than 8."
Call: groupBy_having(
    group_by_variable="Director", 
    aggregated_variable="Rating", 
    aggregate_function="AVG", 
    having_logic_type="more", 
    having_value_start="8"
)
    """
    return await groupBy_having_internal(group_by_variable, query_id, aggregate_function, aggregated_variable, having_logic_type, having_value_start, having_value_end)


@tool_if_enabled("filter_by_quantity")
async def filter_by_quantity(filter_target_variable: str, quantity_property: str, math_operator: str, value_start: str, value_end: str | None, query_id: str) -> Dict[str, Any]:
    """
NUMERICAL/TEMPORAL FILTER: Applies filters for Dates, Durations, and Quantities.

**SPARQL MAPPING:**
?filter_target_variable quantity_property ?value .
FILTER ( ?value [math_operator] value_start )

**WHEN TO USE:**
1. **Dates/Time:** "Composed before 1850", "Written between 1900 and 1920".
2. **Durations:** "Longer than 10 minutes", "Short pieces under 3 minutes".

**CRITICAL SCENARIOS:**
1. DATES (Composed in/before/after):
    - target_variable: "expCreation" (The Creation Event).
    - property_uri: "ecrm:P4_has_time-span".
    - Format: "YYYY" (e.g., "1850").
    - math_operator: use "range" for specific years/dates, "more" for after, "less" for before.
2. DURATIONS (Longer/Shorter than):
    - target_variable: "expression" (The Work).
    - property_uri: "mus:U78_estimated_duration".
    - Format: ISO 8601 (e.g., "PT10M" for 10 mins, "PT1H" for 1 hour).

Args:
    filter_target_variable: The variable to filter (e.g., "expCreation" for dates).
    quantity_property: The RDF property (e.g., "ecrm:P4_has_time-span").
    math_operator: 'less' (<), 'more' (>), 'equal' (=), 'range' (BETWEEN).
    value_start: The threshold or start value.
    value_end: Only required if math_operator is 'range'.
    query_id: The active query ID.

Returns:
    Dict: {"success": bool, "query_id": str, "generated_sparql": str}

**FEW-SHOT EXAMPLES:**

User: "... in 1900"
Context: Date filter. Must apply to the 'Creation Event', not the 'Work'.
Call: filter_by_quantity(
    filter_target_variable="expCreation", 
    quantity_property="ecrm:P4_has_time-span", 
    math_operator="range", 
    value_start="1900",
    value_end="1900", 
    query_id="..."
)

User: "... longer than 15 minutes"
Context: Duration filter. Applies to 'expression'. Format must be ISO.
Call: filter_by_quantity(
    filter_target_variable="expression", 
    quantity_property="mus:U78_estimated_duration", 
    math_operator="more", 
    value_start="PT15M", 
    query_id="..."
)
    """
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
    """
**ADVANCED TOOL: USE ONLY AS A LAST RESORT**
Adds a raw RDF triplet (`?s ?p ?o`) to the query graph.

**WARNING:** - This tool is the "Last Resort". 
- **DO NOT USE** for standard filters (use `apply_filter`).
- **DO NOT USE** for instrument/component connections (use `add_component_constraint`).
- **DO NOT USE** for adding a time-span, dates or other quantities (use `filter_by_quantity`).
- **ONLY USE** when you need to traverse the graph in a way no other tool supports (e.g., connecting a Work to its Publisher, or a Performance to its Premiere).

**SAFETY LOCK:**
Before calling this, you **MUST** have called `get_entity_properties` to verify that the `property` URI actually exists in the DOREMUS schema. Do not guess URIs.

Args:
    subject: The variable name of the start node (MUST already exist in the query, e.g. "expression").
    subject_class: The full URI Class of the subject (e.g. "efrbroo:F22_Self-Contained_Expression"). Used for validation.
    property: The specific property URI connecting them (e.g. "efrbroo:R17_created").
    obj: The variable name for the new target node (e.g. "creationEvent").
    obj_class: The full URI Class of the new object (e.g. "efrbroo:F28_Expression_Creation").
    query_id: The active query ID.

Returns:
    Dict: {"success": bool, "query_id": str}

**FEW-SHOT EXAMPLE (Connecting a Work to its Publisher):**

User: "Who published the score for 'The Magic Flute'?"
Context: 'Publisher' is not in the standard tools. We must graph-walk manually.
Step 1: check ontology -> finds "ecrm:P48_has_preferred_identifier" is wrong, finds "efrbroo:F30_Publication_Event" path.
Step 2:
Call: add_triplet(
    subject="expression", 
    subject_class="efrbroo:F22_Self-Contained_Expression",
    property="mus:U4_had_princeps_publication", 
    obj="pubEvent", 
    obj_class="efrbroo:F30_Publication_Event",
    query_id="..."
)
    """
    return await add_triplet_internal(subject, subject_class, property, obj, obj_class, query_id)


@tool_if_enabled("select_aggregate_variable")
async def select_aggregate_variable(
    projection_variable: str,
    query_id: str,
    select_aggregator: Optional[str] = None
) -> Dict[str, Any]:
    """
PROJECTION TOOL: Determines which variables appear in the final answer table.
    
**SPARQL MAPPING:**
SELECT ?projection_variable -> (When select_aggregator is None)
SELECT ([select_aggregator](?projection_variable) AS ?result) -> (When aggregator is used)

**WHEN TO USE:**
1. DISPLAYING DATA: Use this to ensure a variable (like 'title' or 'date') is shown to the user.
2. COUNTING TOTALS: Use this for "How many..." questions (set select_aggregator="COUNT").

**CRITICAL DISTINCTION:**
- Use this tool to **DISPLAY** a calculation (e.g., "Show the number of tracks").
- Use `groupBy_having` to **FILTER** by a calculation (e.g., "Find artists with more than 5 tracks").

Args:
    projection_variable: The variable name to include in the SELECT clause. 
                        *Must be a variable already initialized or bound in the query.*
    query_id: The ID of the active query.
    select_aggregator: Optional function: "COUNT", "SAMPLE", "MIN", "MAX", "AVG".

**FEW-SHOT EXAMPLES:**
- User: "How many works?" -> projection_variable="expression", select_aggregator="COUNT"
- User: "What are the titles?" -> projection_variable="title", select_aggregator=None
"""
    return await add_select_variable_internal(projection_variable, select_aggregator, query_id)


@mcp.tool()
async def execute_query(query_id: str, limit: int = 10, order_by_variable: str | None = None, order_by_desc: bool = False) -> Dict[str, Any]:
    """
FINAL STEP: Executes the SPARQL query associated with the given ID.

**WHEN TO USE:**
- Use this ONLY after you have finished building the query logic with other tools.
- Call this to retrieve the actual data needed to answer the user's question.

Args:
    query_id: The active query ID.
    limit: Maximum number of results to return (Default: 10, max 50).
    order_by_variable: A variable from the query to sort by (e.g. "date", "title").
    order_by_desc: Set to True for descending order (e.g. newest first).

Returns:
    A dictionary containing the query results.
**INSTRUCTION:** Analyze these results to provide your final answer. 
If results are empty, inform the user that no records were found matching their specific criteria.
"""
    return execute_query_from_id_internal(query_id, limit, order_by_variable, order_by_desc)


@mcp.tool()
async def find_candidate_entities(
    name: str, entity_type: str = "others"
) -> dict[str, Any]:
    """
DISCOVERY TOOL: Converts a natural language name into a unique DOREMUS URI.
    
**CRITICAL USAGE RULE:**
You must call this tool for every named entity (Composer, Instrument, Genre, Place) mentioned in the user's question.

Args:
    name: The search term (e.g., "Wolfgang Amadeus Mozart", "piano", "Paris").
    entity_type: Use "artist" for people/groups, "vocabulary" for instruments/genres, 
                    "place" for locations, or "others" for general search.

Returns:
    A list of candidates. 
    IMPORTANT: Extract the 'uri' field from the best match and use that URI 
    in your subsequent SPARQL filter tools.
"""
    return find_candidate_entities_internal(name, entity_type)


@mcp.tool()
async def get_entity_properties(entity_uri: str) -> dict[str, Any]:
    """
DISCOVERY TOOL: Retrieves all available RDF properties and values for a specific URI or Class.

**WHEN TO USE (CRITICAL):**
1. BEFORE building a query: If you don't know which filters (properties) are available for a template.
2. SCHEMA EXPLORATION: To understand the relationship between classes (e.g., how an 'Expression' connects to a 'Creation Event').
3. URI VERIFICATION: After finding a URI with `find_candidate_entities`, use this to see its actual data before using it in a filter.

**EXAMPLE:**
- To see what you can filter on for a Person: `get_entity_properties("ecrm:E21_Person")`
- To see the details of Mozart: `get_entity_properties("http://data.doremus.org/artist/4802a043...")`

Args:
    entity_uri: The full URI or compact URI (prefixed) of the entity or class to inspect.

Returns:
    A dictionary where keys are Property URIs and values are their corresponding RDF values/literals.
"""
    return get_entity_properties_internal(entity_uri)


if __name__ == "__main__":
    # Run the MCP server
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))

    mcp.run(transport="http", host=host, port=port)
