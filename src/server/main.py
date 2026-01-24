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
**STEP 1: Create a query from a template.**

This creates a base SPARQL query and returns available filters.

**WHEN TO USE:**
1. Use `build_query` as STEP 1 to create the base query.

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


@tool_if_enabled("apply_filter")
async def apply_filter(
    query_id: str,
    base_variable: str,
    template: str,
    filters: Dict[str, str]
) -> Dict[str, Any]:
    """
Step 2 (Optional): Refines the query by adding specific constraints (WHERE clauses).
Use this after `build_query` if the initial filters were not enough, or to filter specific sub-variables.

**WHEN TO USE:**
- To restrict results by attribute (e.g. "Written by Mozart", "In Key of C").
- To check for the *existence* of an attribute without filtering a specific value (pass an empty string "").

**ARGUMENT RULES:**
- `base_variable`: The specific variable name in the SPARQL query you are filtering. 
    *Usually* this is the main variable chosen in Step 1 (e.g., 'expression', 'work', 'artist', 'recording_event', 'track'), but it can be any variable currently in the graph.
- `template`: The class/category of the `base_variable`. This determines valid filter keys.
    Options: "expression", "performance", "artist", "recording_event", "track".

Args:
    query_id: The active query ID from Step 1.
    base_variable: The SPARQL variable to attach the filter to (e.g. "expression").
    template: The schema template to use ("expression", "performance", "artist", "recording_event", "track").
    filters: A dictionary of { "filter_name": "value" }.
        *Tip: If you have a URI from `search_entity`, use it! Otherwise, passing a string (label) is acceptable.*
        
        **Valid Keys per Template:**
        - Template "expression":
            "title", "composer_name", "composer_nationality", "genre", "composition_place", "musical_key"
        - Template "performance":
            "date", "location", "performer"
        - Template "artist":
            "name", "birth_place", "nationality", "death_place", "work_title"
        - Template "recording_event":
            "title", "recorded_by", "performed_by", "location", "recorded_performance"
        - Template "track":
            "work_title", "composer_name", "genre"

Returns:
    Dict: {"success": bool, "query_id": str, "generated_sparql": str}

**FEW-SHOT EXAMPLES:**

User: "...written by Mozart" (Refining a Work)
Call: apply_filter(
    query_id="...", 
    base_variable="expression", 
    template="expression", 
    filters={"composer_name": "Wolfgang Amadeus Mozart"}
)

User: "...that has a genre defined" (Existence Check)
Call: apply_filter(
    query_id="...", 
    base_variable="expression", 
    template="expression", 
    filters={"genre": ""}  <-- Empty string adds the triplet but no filter logic
)
    """
    return await filter_internal(query_id, base_variable, template, filters)


@tool_if_enabled("add_component_constraint")
async def add_component_constraint(
    subject: str, 
    obj: str, 
    query_id: str, 
    n: int | str | None = None) -> Dict[str, Any]:
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
Call: add_component_constraint(
    subject="expression",
    obj="violin", 
    n=3,
    query_id="current_id"
)

User: "Show me pieces that use a piano." (No specific count)
Call: add_component_constraint(
    subject="expression",
    obj="piano",
    n=None,
    query_id="current_id"
)
    """
    return await associate_to_N_entities_internal(subject, obj, query_id, n)


@tool_if_enabled("groupBy_having")
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


@tool_if_enabled("filter_by_quantity")
async def filter_by_quantity(subject: str, property: str, type: str, value: str, valueEnd: str | None, query_id: str) -> Dict[str, Any]:
    """
Applies NUMERICAL or TEMPORAL constraints to the query. 
Use this for questions involving Dates ("after 1900"), Durations ("longer than 5 minutes"), or Quantities.
This tool adds the property and the filter to the query.

**WHEN TO USE:**
1. **Dates/Time:** "Composed before 1850", "Written between 1900 and 1920".
2. **Durations:** "Longer than 10 minutes", "Short pieces under 3 minutes".

**CRITICAL CONFIGURATION RULES:**

--- SCENARIO A: FILTERING BY DATE ---
* **subject:** MUST be the *Creation Event* variable (usually `expCreation`), NOT the Work itself (`expression`).
* **property:** Use `"ecrm:P4_has_time-span"`, `schema:deathDate` or another date property if it is in the query (e.g., `schema:deathDate` because deathDate is in the query).
* **value format:** "YYYY" (e.g., "1850") or "YYYY-MM-DD".
* **type:** "less" (before), "more" (after), "range" (between and for specific years/dates).
* **IMPORTANT** if the user asks for a specific year (e.g., "in 1900"), use type="range" with value="1900" and valueEnd="1900".

--- SCENARIO B: FILTERING BY DURATION ---
* **subject:** The Work/Expression variable (e.g., `expression`).
* **property:** Use `"mus:U78_estimated_duration"`.
* **value format:** MUST use ISO 8601 Duration standard.
    - "10 minutes" -> "PT10M"
    - "1 hour" -> "PT1H"
    - "4 minutes 33 seconds" -> "PT4M33S"
* **type:** "less" (shorter than), "more" (longer than).

Args:
    subject: The variable name to filter (See Scenarios above to choose the right one).
    property: The URI of the property. Select from:
                - "ecrm:P4_has_time-span" (for Dates)
                - "mus:U78_estimated_duration" (for Durations)
    type: The operator: "less", "more", "equal", "range".
    value: The threshold value. (Start value if range).
    valueEnd: The end value. REQUIRED if type="range".
    query_id: The active query ID.

Returns:
    Dict: {"success": bool, "query_id": str, "generated_sparql": str}

**FEW-SHOT EXAMPLES:**

User: "... in 1900"
Context: Date filter. Must apply to the 'Creation Event', not the 'Work'.
Call: filter_by_quantity(
    subject="expCreation", 
    property="ecrm:P4_has_time-span", 
    type="range", 
    value="1900",
    valueEnd="1900", 
    query_id="..."
)

User: "... longer than 15 minutes"
Context: Duration filter. Applies to 'expression'. Format must be ISO.
Call: filter_by_quantity(
    subject="expression", 
    property="mus:U78_estimated_duration", 
    type="more", 
    value="PT15M", 
    query_id="..."
)
    """
    return await has_quantity_of_internal(subject, property, type, value, valueEnd, query_id)


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
Before calling this, you **MUST** have called `get_ontology` (or `search_entity`) to verify that the `property` URI actually exists in the DOREMUS schema. Do not guess URIs.

Args:
    subject: The variable name of the start node (MUST already exist in the query, e.g. "expression").
    subject_class: The full URI Class of the subject (e.g. "http://erlangen-crm.org/efrbroo/F22_Self-Contained_Expression"). Used for validation.
    property: The specific property URI connecting them (e.g. "http://erlangen-crm.org/efrbroo/R17_created").
    obj: The variable name for the new target node (e.g. "creationEvent").
    obj_class: The full URI Class of the new object.
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
    variable: str,
    query_id: str,
    aggregator: Optional[str] = None
) -> Dict[str, Any]:
    """
Modifies the final `SELECT` clause of the SPARQL query. 
Use this to specify exactly WHAT to show in the final answer, or to count results.

**WHEN TO USE:**
1. **Counting Results:** "How many works...", "Count the number of..."
    -> Use aggregator="COUNT".
2. **Displaying Extras:** "Show me the title AND the composer", "List the dates".
    -> Use aggregator=None.

**CRITICAL DISTINCTION - READ CAREFULLY:**
* If the user asks: "Find directors who have **more than 5** movies" 
    -> DO NOT USE THIS TOOL. Use `groupBy_having` (because we are filtering).
* If the user asks: "Show me **how many** movies each director made" 
    -> USE THIS TOOL (because we are displaying the count).

Args:
    variable: The variable name to select/count (e.g., "expression", "creationDate").
                *Must be a variable that already exists in the query logic.*
    query_id: The active query ID.
    aggregator: Optional math function to apply to the output.
                - "COUNT": Counts the number of items.
                - "SAMPLE": Pick one random example (good for de-duplicating).
                - "MIN" / "MAX": First/Last values (e.g. earliest date).
                - "AVG": Average value.
                - None: Just display the raw value.

Returns:
    Dict: {"success": bool, "query_id": str, "generated_sparql": str}

**FEW-SHOT EXAMPLES:**

User: "How many works did Mozart compose?"
Context: We need to see the number (COUNT) in the final answer.
Call: select_aggregate_variable(
    variable="expression", 
    aggregator="COUNT", 
    query_id="..."
)

User: "List the titles of works by Bach."
Context: We just want to ensure 'title' is in the output table.
Call: select_aggregate_variable(
    variable="title", 
    aggregator=None, 
    query_id="..."
)
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

    Args:
        entity_uri: The URI of the entity to inspect.

    Returns:
        A dictionary containing the properties and corresponding values of the entity.
    """
    return get_entity_properties_internal(entity_uri)


if __name__ == "__main__":
    # Run the MCP server
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))

    mcp.run(transport="http", host=host, port=port)
