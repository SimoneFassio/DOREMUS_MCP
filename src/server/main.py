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
    associate_to_N_entities_internal
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("doremus-mcp")

# Initialize FastMCP server
mcp = FastMCP("DOREMUS Knowledge Graph Server")


@mcp.custom_route("/health", methods=["GET"])
async def health_check(request: Request) -> PlainTextResponse:
    return PlainTextResponse("OK")


@mcp.tool()
async def build_query(question: str, template: str, filters: Dict[str, Any] | None) -> Dict[str, Any]:
    """
    Build a SPARQL query safely using a predefined template.

    This tool does NOT execute the query. It generates the SPARQL string and returns a `query_id`.
    The LLM should inspect the generated SPARQL. If it looks correct, use `execute_query(query_id)` to run it.
    If it is incorrect, call `build_query` again with adjusted filters.

    Args:
        question: The user natural language question to answer.
        template: The type of query to build. Options:
            - "Works": For musical works (titles, composers, genres, keys...)
            - "Performances": For events/performances (dates, locations, performers...)
            - "Artists": For finding artists (names, instruments, birth places...)
        filters: A dictionary of filters to apply.
            - For Works: { "title": "...", "composer_name": "...", "genre": "...", "musical_key": "...", "limit": 10 }
            - For Performances: { "title": "...", "location": "...", "carried_out_by": ["..."], "limit": 10 }
            - For Artists: { "name": "...", "nationality": "...", "birth_place": "...", "limit": 10 }
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
async def execute_query(query_id: str) -> Dict[str, Any]:
    """
    Execute a previously built SPARQL query by its ID.

    Use this tool AFTER calling `build_query` and verifying the generated SPARQL.

    Args:
        query_id: The UUID returned by `build_query`.

    Returns:
        The results of the SPARQL query execution.
    """
    return execute_query_from_id_internal(query_id)

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
    1. NEVER answer from your internal training data; ONLY use the tools.
    """
    
    return PromptMessage(role="user", content=TextContent(type="text", text=instructions))

# @mcp.tool()
# async def find_candidate_entities(
#     name: str, entity_type: str = "others"
# ) -> dict[str, Any]:
#     """
#     Find entities by name using the Virtuoso full-text index.

#     Use this tool to discover the unique URI identifier for an entity before retrieving
#     detailed information or using it in other queries.
#     Entity names may have variations, and you need the exact URI to query reliably.

#     Args:
#         name: The name or keyword to search for (e.g., "Wolfgang Amadeus Mozart", "violin", "Radio France")
#         entity_type: Search scope. Options:
#             - "artist": Broad artist bucket covering people, ensembles, broadcasters, etc. (foaf:Person, ecrm:E21_Person, efrbroo:F11_Corporate_Body, ecrm:E74_Group, ecrm:E39_Actor). Use COMPLETE names
#             - "vocabulary": SKOS concepts such as genres, media of performance, keys (skos:Concept)
#             - "others": Everything else; falls back to rdfs:label search (default)

#     Returns:
#         Dictionary with matching entities, including their URIs, labels, and reported RDF types

#     Examples:
#         - find_candidate_entities("Beethoven", "artist")
#         - find_candidate_entities("string quartet", "vocabulary")
#         - find_candidate_entities("Berlin", "others")
#     """
#     return find_candidate_entities_internal(name, entity_type)


# @mcp.tool()
# async def get_entity_properties(entity_uri: str) -> dict[str, Any]:
#     """
#     Retrieve detailed information about a specific entity.

#     Use this as the first step after finding an entity with find_candidate_entities.
#     It shows all direct properties of an entity.

#     Args:
#         entity_uri: The full URI of the entity (e.g., "http://data.doremus.org/artist/...")

#     Returns:
#         Dictionary with:
#         - entity_uri: The requested entity
#         - entity_label: Human-readable name
#         - entity_type: Class of the entity
#         - properties: All properties as key-value pairs

#     Examples:
#         # Basic usage - get entity properties with labels
#         get_entity_properties("http://data.doremus.org/artist/123")
#     """
#     return get_entity_properties_internal(entity_uri)


# @mcp.tool()
# def find_paths(start_entity: str, end_entity: str, k: int = 5) -> str:
#     """
#     Find the top k shortest paths between two node types in the local graph.

#     Use this tool to explore the topology and connecting two node types e.g. ecrm:E21_Person and mus:M42_Performed_Expression_Creation
#     Args:
#         start_entity: Prefixed URI of the type start node
#         end_entity: Prefixed URI of the type end node
#         k: Number of shortest paths to return (5-10 works most of the times)
#     Returns:
#         The paths
#     """
#     paths = find_k_shortest_paths(graph, start_entity, end_entity, k)

#     # format the output
#     res = ""
#     for i, path in enumerate(paths):
#         res += f"{i+1}# "
#         for idx, triplet in enumerate(path):
#             if idx == 0:
#                 res += f"{triplet[0]}->"
#             else:
#                 res += "->"
#             res += f"{triplet[1]}->{triplet[2]}"
#         res += "\n"

#     return res


# @mcp.tool()
# def get_ontology(path: str) -> str:
#     """
#     Explore the DOREMUS ontology graph schema hierarchically.

#     This tool helps you understand the structure of the knowledge graph by providing
#     a hierarchical view of node types (classes) and their relationships (edges).

#     Use this tool to:
#     - Get an overview of the most important node types and connections (path='/')
#     - Explore a specific class and its direct relationships

#     Args:
#         path: Navigation path for exploration:
#             - '/' - Get a high-level summary of the top 15 most important node types
#                    and their top 20 most common relationships
#             - '/{ClassName}' - Explore a specific class (e.g., '/efrbroo:F28_Expression_Creation')
#                    Use the exact node type name as shown in get_nodes_list tool

#     Returns:
#         Markdown-formatted visualization of the ontology subgraph, showing:
#         - Node types (classes) in the knowledge graph
#         - Edge types (predicates/relationships) connecting them
#         - Hierarchical structure for easy understanding

#     Examples:
#         - get_ontology('/')
#           Returns overview of the most important 15 nodes and their relationships

#         - get_ontology('/efrbroo:F22_Self-Contained_Expression', depth=1)
#           Shows what properties and relationships a musical work has

#         - get_ontology('/ecrm:E21_Person', depth=2)
#           Shows person connections and what those connected entities relate to

#     Note:
#         Use get_nodes_list() first to see all available node types you can explore.
#     """
#     return get_ontology_internal(path=path, depth=1)

# # Documentation tools


# @mcp.tool()
# def get_kg_structure() -> str:
#     """
#     Get a comprehensive description of the DOREMUS Knowledge Graph structure.

#     This tool provides essential information about the ontology, including:
#     - Main entity types (classes)
#     - Key properties and relationships
#     - Common URI patterns
#     - Ontology prefixes

#     Essential for understanding how to write custom SPARQL queries.

#     Returns:
#         Detailed documentation of the DOREMUS ontology structure
#     """
#     guide = """
#         # DOREMUS Knowledge Graph Structure

#         ## Overview
#         The DOREMUS Knowledge Graph describes classical music metadata using the FRBRoo
#         (Functional Requirements for Bibliographic Records - object oriented) and
#         CIDOC-CRM ontologies, extended with a music-specific ontology.

#         ## Core Entity Types

#         ### 1. Musical Works & Expressions
#         - **efrbroo:F22_Self-Contained_Expression**: A musical work/composition
#         - Properties:
#             - `rdfs:label`: Title of the work
#             - `mus:U12_has_genre`: Genre/type (symphony, sonata, concerto, etc.)
#             - `mus:U13_has_casting`: Instrumentation specification
#             - `mus:U11_has_key`: Musical key
#             - `mus:U78_estimated_duration`: Duration in seconds
#             - `mus:U16_has_catalogue_statement`: Catalogue number (BWV, K., Op., etc.)

#         - **efrbroo:F14_Individual_Work**: Abstract work concept
#         - `efrbroo:R9_is_realised_in`: Links to expressions
#         - `ecrm:P148_has_component`: Links to movements/parts

#         ### 2. Composers & Artists
#         - **foaf:Person**: Composers, performers, conductors
#         - Properties:
#             - `foaf:name`: Full name
#             - `schema:birthDate`: Birth date
#             - `schema:deathDate`: Death date
#             - `schema:birthPlace`: Birth location
#             - `ecrm:P107_has_current_or_former_member`: For ensembles

#         ### 3. Performances & Recordings
#         - **efrbroo:F31_Performance**: A performance event
#         - `ecrm:P7_took_place_at`: Performance venue
#         - `ecrm:P4_has_time-span`: When it occurred
#         - `ecrm:P9_consists_of`: Component activities (conducting, playing)
#         - `efrbroo:R25_performed`: What was performed

#         - **mus:M42_Performed_Expression_Creation**: Performance of a work
#         - `efrbroo:R17_created`: Creates a performed expression
#         - `mus:U54_is_performed_expression_of`: Links to original work

#         - **efrbroo:F29_Recording_Event**: Audio/video recording
#         - `efrbroo:R20_recorded`: Links to performance

#         - **mus:M24_Track**: Individual track on an album
#         - `mus:U51_is_partial_or_full_recording_of`: Links to performed expression
#         - `mus:U10_has_order_number`: Track number

#         ### 4. Instrumentation (Casting)
#         - **mus:M6_Casting**: Instrumentation specification
#         - `mus:U23_has_casting_detail`: properties for each instrument

#         - **mus:M7_Casting_Detail**: Specific instrument detail
#         - `mus:U2_foresees_use_of_medium_of_performance`: Instrument URI
#         - `mus:U30_foresees_quantity_of_mop`: Number of instruments

#         ### 5. Creation & Composition
#         - **efrbroo:F28_Expression_Creation**: Composition activity
#         - `efrbroo:R17_created`: Links to created work
#         - `ecrm:P9_consists_of`: Component activities
#         - `ecrm:P4_has_time-span`: Composition date
#         - `ecrm:P7_took_place_at`: Composition location

#         - **ecrm:P14_carried_out_by**: Links activity to person
#         - `mus:U31_had_function`: Role (composer, librettist, arranger)

#         ### 6. Genres & Types
#         Common genre URIs:
#         - `<http://data.doremus.org/vocabulary/iaml/genre/sy>` - Symphony
#         - `<http://data.doremus.org/vocabulary/iaml/genre/sn>` - Sonata
#         - `<http://data.doremus.org/vocabulary/iaml/genre/co>` - Concerto
#         - `<http://data.doremus.org/vocabulary/iaml/genre/op>` - Opera
#         - `<http://data.doremus.org/vocabulary/iaml/genre/mld>` - Melody

#         ### 7. Instruments
#         Common instrument URIs (with MIMO equivalents):
#         - Violin: `<http://data.doremus.org/vocabulary/iaml/mop/svl>` or `<http://www.mimo-db.eu/InstrumentsKeywords/3573>`
#         - Piano: `<http://data.doremus.org/vocabulary/iaml/mop/kpf>` or `<http://www.mimo-db.eu/InstrumentsKeywords/2299>`
#         - Cello: `<http://data.doremus.org/vocabulary/iaml/mop/svc>` or `<http://www.mimo-db.eu/InstrumentsKeywords/3582>`
#         - Flute: `<http://data.doremus.org/vocabulary/iaml/mop/wfl>` or `<http://www.mimo-db.eu/InstrumentsKeywords/3955>`
#         - Orchestra: `<http://data.doremus.org/vocabulary/iaml/mop/o>`

#         ### 8. Functions/Roles
#         - `<http://data.doremus.org/vocabulary/function/composer>` - Composer
#         - `<http://data.doremus.org/vocabulary/function/conductor>` - Conductor
#         - `<http://data.doremus.org/vocabulary/function/librettist>` - Librettist

#         ## Common SPARQL Patterns

#         ### Find works by composer:
#         ```sparql
#         ?expression a efrbroo:F22_Self-Contained_Expression ;
#             rdfs:label ?title .
#         ?expCreation efrbroo:R17_created ?expression ;
#             ecrm:P9_consists_of / ecrm:P14_carried_out_by ?composer .
#         ?composer foaf:name "Wolfgang Amadeus Mozart" .
#         ```

#         ### Filter by composition date:
#         ```sparql
#         ?expCreation efrbroo:R17_created ?expression ;
#             ecrm:P4_has_time-span ?ts .
#         ?ts time:hasEnd / time:inXSDDate ?end ;
#             time:hasBeginning / time:inXSDDate ?start .
#         FILTER (?start >= "1800"^^xsd:gYear AND ?end <= "1850"^^xsd:gYear)
#         ```

#         ### Filter by instrumentation:
#         ```sparql
#         ?expression mus:U13_has_casting ?casting .
#         ?casting mus:U23_has_casting_detail ?castingDet .
#         ?castingDet mus:U2_foresees_use_of_medium_of_performance ?instrument .
#         VALUES ?instrument { <http://data.doremus.org/vocabulary/iaml/mop/svl> }
#         ```

#         ### Filter by genre:
#         ```sparql
#         ?expression mus:U12_has_genre <http://data.doremus.org/vocabulary/iaml/genre/sn> .
#         ```

#         ## URI Patterns
#         - Works: `http://data.doremus.org/expression/{uuid}`
#         - Artists: `http://data.doremus.org/artist/{uuid}`
#         - Vocabularies: `http://data.doremus.org/vocabulary/{domain}/{term}`
#         - Places: `http://data.doremus.org/place/{uuid}` or `http://sws.geonames.org/{id}/`

#         ## Tips for Query Writing
#         1. Use `SAMPLE()` aggregation when grouping to avoid duplicates
#         2. Use `skos:exactMatch*` for instrument matching (connects to MIMO vocabulary)
#         3. Add `LIMIT` clauses to prevent timeouts
#         4. Use `FILTER` for text matching with `REGEX()` or `contains()`
#         5. Use `OPTIONAL` blocks for properties that may not exist
#         6. COUNT grouped casting properties with HAVING to filter by instrumentation size
#         """

#     return guide


# @mcp.tool()
# def get_usage_guide() -> str:
#     """
#     USE THIS TOOL FIRST TO RETRIEVE GUIDANCE ON QUERYING DOREMUS
    
#     Get a comprehensive usage guide and prompt for LLMs interacting with DOREMUS.

#     This tool provides guidance on:
#     - How to effectively use the available tools
#     - Best practices for entity resolution
#     - Tips for handling ambiguous requests

#     Returns:
#         Detailed guide for effectively querying the DOREMUS knowledge graph
#     """

#     guide = """
# # DOREMUS MCP Server - LLM Usage Guide

# ## Purpose
# This MCP server provides access to the DOREMUS Knowledge Graph, a comprehensive
# database of classical music metadata including works, composers, performances,
# recordings, and instrumentation.

# ## Workflow
# 1. get_ontology: explore the DOREMUS ontology graph schema
# 2. find_candidate_entities: discover the unique URI identifier for an entity
# 3. get_entity_properties: retrieve detailed information about a specific entity (all property)
# 4. find_paths: connect two nodes types exploring the best graph traversal to use in the query
# 5. execute_sparql: execute the query built using information collected
# 6. Check the query result, refine and use again tool to explore more the graph if necessary
# 7. Once the result is ok, format it in a proper manner and write the response

# ## Best Practices

# ### Entity Resolution
# 1. **Always search before assuming**: Don't assume you know the exact URI or name

# 2. **Handle ambiguity**: If multiple matches, ask user to clarify
#    ```
#    Found 3 composers named "Bach":
#    - Johann Sebastian Bach
#    - Carl Philipp Emanuel Bach
#    - Johann Christian Bach
#    Which one did you mean?
#    ```

# ### Query Building
# 1. **Start specific, broaden if needed**: Begin with restrictive filters, relax if no results

# 2. **Use appropriate limits**: Default to 20-50 results for exploration, higher for comprehensive searches

# 3. **Combine tools strategically**:
#    - Discovery: find_candidate_entities
#    - Deep dive: get_entity_properties
#    - Analysis: execute_sparql with aggregations

# ### Performance
# 1. **Date ranges**: Narrower is faster
# 2. **Instrumentation**: Specific instruments faster than "any strings"
# 3. **Limits**: Keep reasonable (50-100), paginate if needed
# 4. **Timeouts**: If query times out, simplify or add more filters

# ### Error Handling
# 1. **No results**: Try broader search or check spelling
# 2. **Timeout**: Reduce scope or limit, add more specific filters
# 3. **Multiple URIs**: Present options to user

# ## Handling Ambiguous Requests

# ### "Chamber music"
# - Broad genre category
# - Filter by: small instrumentation (2-10 instruments), no orchestra
# - Consider suggesting specific formats (string quartet, piano trio)

# ### "Modern"/"Contemporary"
# - Define timeframe (20th century = 1900-2000, contemporary = 2000+)
# - Ask user to clarify or assume based on context

# ### "Famous works"
# - No "fame" metric in database
# - Proxy: works by well-known composers, frequently performed/recorded
# - Use custom SPARQL with COUNT of performances/recordings

# ### Instrument variations
# - Piano vs. keyboard vs. harpsichord
# - Violin vs. strings
# - Use skos:broader relationships or suggest alternatives

# ## Remember
# - The database is authoritative but not complete
# - Always verify entity resolution before complex queries
# - When in doubt, start simple and iterate
# - Provide context and explanations, not just raw data
# - Acknowledge limitations when encountered
# """

#     return guide


if __name__ == "__main__":
    # Run the MCP server
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))

    mcp.run(transport="sse", host=host, port=port)
