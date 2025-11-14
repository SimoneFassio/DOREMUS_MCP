"""
DOREMUS Knowledge Graph MCP Server

A Model Context Protocol server for querying the DOREMUS music knowledge graph
via SPARQL endpoint at https://data.doremus.org/sparql/
"""

from typing import Any, Optional
from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import PlainTextResponse
import os
from src.server.find_paths import find_k_shortest_paths
from src.server.tools_internal import (
    graph,
    find_candidate_entities_internal,
    get_entity_details_internal,
    search_musical_works_internal,
    get_ontology_internal,
)
from src.server.utils import execute_sparql_query, logger

# Initialize FastMCP server
mcp = FastMCP("DOREMUS Knowledge Graph Server")

@mcp.custom_route("/health", methods=["GET"])
async def health_check(request: Request) -> PlainTextResponse:
    return PlainTextResponse("OK")


@mcp.tool()
async def find_candidate_entities(name: str, entity_type: str = "others") -> dict[str, Any]:
    """
    Find entities by name using the Virtuoso full-text index.

    Use this tool to discover the unique URI identifier for an entity before retrieving
    detailed information or using it in other queries.

    Args:
        name: The name or keyword to search for (e.g., "Wolfgang Amadeus Mozart", "violin", "Radio France")
        entity_type: Search scope. Options:
            - "artist": Broad artist bucket covering people, ensembles, broadcasters, etc. (foaf:Person, ecrm:E21_Person, efrbroo:F11_Corporate_Body, ecrm:E74_Group, ecrm:E39_Actor). Use COMPLETE names
            - "vocabulary": SKOS concepts such as genres, media of performance, keys (skos:Concept)
            - "others": Everything else; falls back to rdfs:label search (default)

    Returns:
        Dictionary with matching entities, including their URIs, labels, and reported RDF types

    Examples:
        - find_candidate_entities("Beethoven", "artist")
        - find_candidate_entities("string quartet", "vocabulary")
        - find_candidate_entities("Berlin", "others")
    """
    return find_candidate_entities_internal(name, entity_type)

@mcp.tool()
async def get_entity_details(entity_uri: str) -> dict[str, Any]:
    """
    Retrieve detailed information about a specific entity with optional recursive resolution.
    
    Use this as the first step after finding an entity with find_candidate_entities.
    It shows all direct properties of an entity.
    
    Args:
        entity_uri: The full URI of the entity (e.g., "http://data.doremus.org/artist/...")
        
    Returns:
        Dictionary with:
        - entity_uri: The requested entity
        - entity_label: Human-readable name
        - entity_type: Class of the entity
        - properties: All properties as key-value pairs
        
    Examples:
        # Basic usage - get entity properties with labels
        get_entity_details("http://data.doremus.org/artist/123")
    """
    return get_entity_details_internal(entity_uri)

@mcp.tool()
def find_paths(start_entity: str, end_entity: str, k: int = 5) -> str:
    """
    Find the top k shortest paths between two node types in the local graph.
    
    Use this tool to explore the topology and connnecting two node types e.g. ecrm:E21_Person and mus:M42_Performed_Expression_Creation
    Args:
        start_entity: Prefixed URI of the type start node
        end_entity: Prefixed URI of the type end node
        k: Number of shortest paths to return (5-10 works most of the times)
    Returns:
        The formatted paths
    """
    paths = find_k_shortest_paths(graph, start_entity, end_entity, k)
    
    # format the output
    res = ""
    for i, path in enumerate(paths):
        res += f"{i+1}# "
        for idx, triplet in enumerate(path):
            if idx==0:
                res += f"{triplet[0]}->"
            else:
                res += "->"
            res += f"{triplet[1]}->{triplet[2]}"
        res += "\n"
        
    return res

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
                   Use the exact node type name as shown in get_nodes_list tool
    
    Returns:
        Markdown-formatted visualization of the ontology subgraph, showing:
        - Node types (classes) in the knowledge graph
        - Edge types (predicates/relationships) connecting them
        - Hierarchical structure for easy understanding
    
    Examples:
        - get_ontology('/') 
          Returns overview of the most important 15 nodes and their relationships
        
        - get_ontology('/efrbroo:F22_Self-Contained_Expression', depth=1)
          Shows what properties and relationships a musical work has
        
        - get_ontology('/ecrm:E21_Person', depth=2)
          Shows person connections and what those connected entities relate to
    
    Note:
        Use get_nodes_list() first to see all available node types you can explore.
    """
    return get_ontology_internal(path=path, depth=1)

# @mcp.tool()
# async def search_musical_works(
#         composers: Optional[list[str]] = None,
#         work_type: Optional[str] = None,
#         date_start: Optional[int] = None,
#         date_end: Optional[int] = None,
#         instruments: Optional[list[dict[str, Any]]] = None,
#         place_of_composition: Optional[str] = None,
#         place_of_performance: Optional[str] = None,
#         duration_min: Optional[int] = None,
#         duration_max: Optional[int] = None,
#         topic: Optional[str] = None,
#         limit: int = 50
#     ) -> dict[str, Any]:
#     """
#     Search for musical works with flexible filtering criteria.
    
#     This is the main tool for querying works in the DOREMUS knowledge graph with
#     support for multiple filter combinations.
    
#     Args:
#         composers: List of composer names or URIs (e.g., ["Mozart", "Beethoven"])
#         work_type: Type/genre of work (e.g., "sonata", "symphony", "concerto")
#         date_start: Start year for composition date range (e.g., 1800)
#         date_end: End year for composition date range (e.g., 1850)
#         instruments: List of instrument specifications, each with:
#             - name: instrument name/URI (e.g., "violin", "piano")
#             - quantity: exact number (optional)
#             - min_quantity: minimum number (optional)
#             - max_quantity: maximum number (optional)
#         place_of_composition: Place where work was composed
#         place_of_performance: Place where work was performed
#         duration_min: Minimum duration in seconds
#         duration_max: Maximum duration in seconds
#         topic: Topic or subject matter of the work
#         limit: Maximum number of results (default: 50, max: 200)
        
#     Returns:
#         Dictionary with matching works and their details
        
#     Examples:
#         - search_musical_works(composers=["Mozart"], work_type="sonata")
#         - search_musical_works(instruments=[{"name": "violin", "quantity": 2}, {"name": "viola"}])
#         - search_musical_works(date_start=1800, date_end=1850, place_of_composition="Vienna")
#     """
#     return search_musical_works_internal(composers, work_type, date_start, date_end, instruments, place_of_composition, place_of_performance, duration_min, duration_max, topic, limit)

@mcp.tool()
async def execute_custom_sparql(query: str, limit: int = 100) -> dict[str, Any]:
    """
    Execute a custom SPARQL query against the DOREMUS knowledge graph.
    
    Use this tool when the pre-built tools don't cover your specific use case.
    You have full control over the SPARQL query, but should be familiar with
    the DOREMUS ontology structure.
    
    Args:
        query: Complete SPARQL query string (SELECT, CONSTRUCT, or ASK)
        limit: Maximum number of results to return (default: 100, max: 500)
        
    Returns:
        Raw query results from the SPARQL endpoint
        
    Note:
        For complex queries, consider checking the knowledge graph structure
        resource first to understand available classes and properties.
        
    Example:
        ```sparql
        SELECT ?work ?title
        WHERE {
            ?work a efrbroo:F22_Self-Contained_Expression ;
                  rdfs:label ?title .
        }
        LIMIT 10
        ```
    """
    return execute_sparql_query(query, limit)


# Documentation tools

@mcp.tool()
def get_kg_structure() -> str:
    """
    Get a comprehensive description of the DOREMUS Knowledge Graph structure.
    
    This tool provides essential information about the ontology, including:
    - Main entity types (classes)
    - Key properties and relationships
    - Common URI patterns
    - Ontology prefixes
    
    Essential for understanding how to write custom SPARQL queries.
    
    Returns:
        Detailed documentation of the DOREMUS ontology structure
    """
    guide = """
        # DOREMUS Knowledge Graph Structure

        ## Overview
        The DOREMUS Knowledge Graph describes classical music metadata using the FRBRoo
        (Functional Requirements for Bibliographic Records - object oriented) and
        CIDOC-CRM ontologies, extended with a music-specific ontology.

        ## Core Entity Types

        ### 1. Musical Works & Expressions
        - **efrbroo:F22_Self-Contained_Expression**: A musical work/composition
        - Properties:
            - `rdfs:label`: Title of the work
            - `mus:U12_has_genre`: Genre/type (symphony, sonata, concerto, etc.)
            - `mus:U13_has_casting`: Instrumentation specification
            - `mus:U11_has_key`: Musical key
            - `mus:U78_estimated_duration`: Duration in seconds
            - `mus:U16_has_catalogue_statement`: Catalogue number (BWV, K., Op., etc.)
            
        - **efrbroo:F14_Individual_Work**: Abstract work concept
        - `efrbroo:R9_is_realised_in`: Links to expressions
        - `ecrm:P148_has_component`: Links to movements/parts

        ### 2. Composers & Artists
        - **foaf:Person**: Composers, performers, conductors
        - Properties:
            - `foaf:name`: Full name
            - `schema:birthDate`: Birth date
            - `schema:deathDate`: Death date
            - `schema:birthPlace`: Birth location
            - `ecrm:P107_has_current_or_former_member`: For ensembles

        ### 3. Performances & Recordings
        - **efrbroo:F31_Performance**: A performance event
        - `ecrm:P7_took_place_at`: Performance venue
        - `ecrm:P4_has_time-span`: When it occurred
        - `ecrm:P9_consists_of`: Component activities (conducting, playing)
        - `efrbroo:R25_performed`: What was performed

        - **mus:M42_Performed_Expression_Creation**: Performance of a work
        - `efrbroo:R17_created`: Creates a performed expression
        - `mus:U54_is_performed_expression_of`: Links to original work

        - **efrbroo:F29_Recording_Event**: Audio/video recording
        - `efrbroo:R20_recorded`: Links to performance
        
        - **mus:M24_Track**: Individual track on an album
        - `mus:U51_is_partial_or_full_recording_of`: Links to performed expression
        - `mus:U10_has_order_number`: Track number

        ### 4. Instrumentation (Casting)
        - **mus:M6_Casting**: Instrumentation specification
        - `mus:U23_has_casting_detail`: Details for each instrument

        - **mus:M7_Casting_Detail**: Specific instrument detail
        - `mus:U2_foresees_use_of_medium_of_performance`: Instrument URI
        - `mus:U30_foresees_quantity_of_mop`: Number of instruments

        ### 5. Creation & Composition
        - **efrbroo:F28_Expression_Creation**: Composition activity
        - `efrbroo:R17_created`: Links to created work
        - `ecrm:P9_consists_of`: Component activities
        - `ecrm:P4_has_time-span`: Composition date
        - `ecrm:P7_took_place_at`: Composition location

        - **ecrm:P14_carried_out_by**: Links activity to person
        - `mus:U31_had_function`: Role (composer, librettist, arranger)

        ### 6. Genres & Types
        Common genre URIs:
        - `<http://data.doremus.org/vocabulary/iaml/genre/sy>` - Symphony
        - `<http://data.doremus.org/vocabulary/iaml/genre/sn>` - Sonata
        - `<http://data.doremus.org/vocabulary/iaml/genre/co>` - Concerto
        - `<http://data.doremus.org/vocabulary/iaml/genre/op>` - Opera
        - `<http://data.doremus.org/vocabulary/iaml/genre/mld>` - Melody

        ### 7. Instruments
        Common instrument URIs (with MIMO equivalents):
        - Violin: `<http://data.doremus.org/vocabulary/iaml/mop/svl>` or `<http://www.mimo-db.eu/InstrumentsKeywords/3573>`
        - Piano: `<http://data.doremus.org/vocabulary/iaml/mop/kpf>` or `<http://www.mimo-db.eu/InstrumentsKeywords/2299>`
        - Cello: `<http://data.doremus.org/vocabulary/iaml/mop/svc>` or `<http://www.mimo-db.eu/InstrumentsKeywords/3582>`
        - Flute: `<http://data.doremus.org/vocabulary/iaml/mop/wfl>` or `<http://www.mimo-db.eu/InstrumentsKeywords/3955>`
        - Orchestra: `<http://data.doremus.org/vocabulary/iaml/mop/o>`

        ### 8. Functions/Roles
        - `<http://data.doremus.org/vocabulary/function/composer>` - Composer
        - `<http://data.doremus.org/vocabulary/function/conductor>` - Conductor
        - `<http://data.doremus.org/vocabulary/function/librettist>` - Librettist

        ## Common SPARQL Patterns

        ### Find works by composer:
        ```sparql
        ?expression a efrbroo:F22_Self-Contained_Expression ;
            rdfs:label ?title .
        ?expCreation efrbroo:R17_created ?expression ;
            ecrm:P9_consists_of / ecrm:P14_carried_out_by ?composer .
        ?composer foaf:name "Wolfgang Amadeus Mozart" .
        ```

        ### Filter by composition date:
        ```sparql
        ?expCreation efrbroo:R17_created ?expression ;
            ecrm:P4_has_time-span ?ts .
        ?ts time:hasEnd / time:inXSDDate ?end ;
            time:hasBeginning / time:inXSDDate ?start .
        FILTER (?start >= "1800"^^xsd:gYear AND ?end <= "1850"^^xsd:gYear)
        ```

        ### Filter by instrumentation:
        ```sparql
        ?expression mus:U13_has_casting ?casting .
        ?casting mus:U23_has_casting_detail ?castingDet .
        ?castingDet mus:U2_foresees_use_of_medium_of_performance ?instrument .
        VALUES ?instrument { <http://data.doremus.org/vocabulary/iaml/mop/svl> }
        ```

        ### Filter by genre:
        ```sparql
        ?expression mus:U12_has_genre <http://data.doremus.org/vocabulary/iaml/genre/sn> .
        ```

        ## URI Patterns
        - Works: `http://data.doremus.org/expression/{uuid}`
        - Artists: `http://data.doremus.org/artist/{uuid}`
        - Vocabularies: `http://data.doremus.org/vocabulary/{domain}/{term}`
        - Places: `http://data.doremus.org/place/{uuid}` or `http://sws.geonames.org/{id}/`

        ## Tips for Query Writing
        1. Use `SAMPLE()` aggregation when grouping to avoid duplicates
        2. Use `skos:exactMatch*` for instrument matching (connects to MIMO vocabulary)
        3. Add `LIMIT` clauses to prevent timeouts
        4. Use `FILTER` for text matching with `REGEX()` or `contains()`
        5. Use `OPTIONAL` blocks for properties that may not exist
        6. COUNT grouped casting details with HAVING to filter by instrumentation size
        """
    
    return guide

@mcp.tool()
def get_usage_guide() -> str:
    """
    Get a comprehensive usage guide and prompt for LLMs interacting with DOREMUS.
    
    This tool provides guidance on:
    - How to effectively use the available tools
    - Best practices for entity resolution
    - Tips for handling ambiguous requests
    
    Returns:
        Detailed guide for effectively querying the DOREMUS knowledge graph
    """
    
    guide = """
# DOREMUS MCP Server - LLM Usage Guide

## Purpose
This MCP server provides access to the DOREMUS Knowledge Graph, a comprehensive
database of classical music metadata including works, composers, performances,
recordings, and instrumentation.

## Available Tools

### 1. find_candidate_entities
**When to use**: As the first step when you need to reference a specific composer,
work, or place by name.

**Why**: Entity names may have variations, and you need the exact URI to query
reliably.

**Example workflow**:
```
User: "Find sonatas by Beethoven"
1. find_candidate_entities("Beethoven", "composer")
2. Note the composer URI from results
3. search_musical_works(composers=[uri], work_type="sonata")
```

### 2. get_entity_details
**When to use**: After finding an entity URI, to get comprehensive information
about that entity.

**Why**: Provides all available properties like birth/death dates, alternative
names, relationships, etc.

**Example workflow**:
```
User: "Tell me about Mozart"
1. find_candidate_entities("Mozart", "composer")
2. get_entity_details(mozart_uri)
3. Present formatted information to user
```

### 3. execute_custom_sparql
**When to use**: For execcuting queries

**Before using**:
1. Check the knowledge graph structure resource
2. Look at example queries
3. Test incrementally, starting simple

## Best Practices

### Entity Resolution
1. **Always search before assuming**: Don't assume you know the exact URI or name
   - ❌ Bad: search_musical_works(composers=["Mozart"])
   - ✅ Good: find_candidate_entities("Mozart") → use returned URI

2. **Handle ambiguity**: If multiple matches, ask user to clarify
   ```
   Found 3 composers named "Bach":
   - Johann Sebastian Bach
   - Carl Philipp Emanuel Bach  
   - Johann Christian Bach
   Which one did you mean?
   ```

### Query Building
1. **Start specific, broaden if needed**: Begin with restrictive filters, relax if no results

2. **Use appropriate limits**: Default to 20-50 results for exploration, higher for comprehensive searches

3. **Combine tools strategically**:
   - Discovery: find_candidate_entities
   - Deep dive: get_entity_details
   - Analysis: execute_custom_sparql with aggregations

### Performance
1. **Date ranges**: Narrower is faster
2. **Instrumentation**: Specific instruments faster than "any strings"
3. **Limits**: Keep reasonable (50-100), paginate if needed
4. **Timeouts**: If query times out, simplify or add more filters

### Error Handling
1. **No results**: Try broader search or check spelling
2. **Timeout**: Reduce scope or limit, add more specific filters
3. **Multiple URIs**: Present options to user

## Handling Ambiguous Requests

### "Chamber music"
- Broad genre category
- Filter by: small instrumentation (2-10 instruments), no orchestra
- Consider suggesting specific formats (string quartet, piano trio)

### "Modern"/"Contemporary"
- Define timeframe (20th century = 1900-2000, contemporary = 2000+)
- Ask user to clarify or assume based on context

### "Famous works"
- No "fame" metric in database
- Proxy: works by well-known composers, frequently performed/recorded
- Use custom SPARQL with COUNT of performances/recordings

### Instrument variations
- Piano vs. keyboard vs. harpsichord
- Violin vs. strings
- Use skos:broader relationships or suggest alternatives

## Formatting Results

### For Lists
- Group by logical categories (composer, date, type)
- Include key identifying info (title, composer, date)
- Limit long lists, offer to show more

### For Details
- Organize by topic (biographical, compositional, performance)
- Format dates human-readably
- Translate technical terms (genre codes, URIs) to readable labels

### For Comparisons
- Use tables when appropriate
- Highlight similarities and differences
- Provide context for numbers

## Remember
- The database is authoritative but not complete
- Always verify entity resolution before complex queries
- When in doubt, start simple and iterate
- Provide context and explanations, not just raw data
- Acknowledge limitations when encountered
"""

    return guide

# @mcp.tool()
# def get_nodes_list() ->str:
#     """
#     Get the list of all node types, use this to identify useful nodes before find_path tool
#     """
#     nodes = """
#     time:Instant
#     mus:M28_Individual_Performance
#     ecrm:E52_Time-Span
#     time:Interval
#     ecrm:E7_Activity
#     efrbroo:F28_Expression_Creation
#     mus:M156_Title_Statement
#     ecrm:E13_Attribute_Assignment
#     efrbroo:F22_Self-Contained_Expression
#     mus:M46_Set_of_Tracks
#     mus:M44_Performed_Work
#     mus:M43_Performed_Expression
#     mus:M42_Performed_Expression_Creation
#     efrbroo:F14_Individual_Work
#     efrbroo:F15_Complex_Work
#     mus:M19_Categorization
#     mus:M23_Casting_Detail
#     efrbroo:F26_Recording
#     efrbroo:F21_Recording_Work
#     ecrm:E21_Person
#     mus:M157_Statement_of_Responsibility
#     efrbroo:F24_Publication_Expression
#     efrbroo:F20_Performance_Work
#     efrbroo:F30_Publication_Event
#     efrbroo:F25_Performance_Plan
#     mus:M160_Publication_Statement
#     mus:M161_Distribution_Statement
#     efrbroo:F31_Performance
#     efrbroo:F3_Manifestation_Product_Type
#     mus:M6_Casting
#     efrbroo:F19_Publication_Work
#     mus:M158_Title_and_Statement_of_Responsibility
#     ecrm:E42_Identifier
#     mus:M155_Cast_Statement
#     efrbroo:F29_Recording_Event
#     efrbroo:F42_Representative_Expression_Assignment
#     ecrm:E54_Dimension
#     ecrm:E67_Birth
#     mus:M31_Actor_Function
#     mus:M29_Editing
#     efrbroo:F38_Character
#     mus:M24_Track
#     mus:M171_Container
#     ecrm:E69_Death
#     efrbroo:F11_Corporate_Body
#     mus:M2_Opus_Statement
#     mus:M1_Catalogue_Statement
#     ecrm:E53_Place
#     efrbroo:F25_PerformancePlan
#     mus:M27_Foreseen_Individual_Performance
#     mus:M167_Publication_Expression_Fragment
#     efrbroo:F4_Manifestation_Singleton
#     mus:M39_Derivation_Type_Assignment
#     skos:Concept
#     mus:M15_Dedication_Statement
#     mus:M33_Set_of_Characters
#     mus:M45_Descriptive_Expression_Assignment
#     mus:M15_Dedication
#     mus:M14_Medium_Of_Performance
#     efrbroo:F19_Publication_Expression
#     ecrm:E1_CRM_Entity
#     mus:M154_Label_Name
#     mus:M26_Foreseen_Performance
#     geonames:Feature
#     foaf:Document
#     efrbroo:F32_Carrier_Production_Event
#     ecrm:E39_Actor
#     mus:M40_Context
#     mus:M159_Edition_Statement
#     ecrm:E66_Formation
#     mus:M50_Creation_or_Performance_Mode
#     mus:M4_Key
#     mus:M25_Foreseen_Activity
#     mus:M5_Genre
#     mus:M36_Award
#     modsrdf:NoteGroup
#     modsrdf:ModsResource
#     rdfs:Class
#     ecrm:E22_Man-Made_Object
#     ecrm:E68_Dissolution
#     skos:ConceptScheme
#     rdfs:Datatype
#     ecrm:E4_Period
#     """
#     return nodes

if __name__ == "__main__":
    # Run the MCP server
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    
    mcp.run(transport="sse", host=host, port=port)
