"""
DOREMUS Knowledge Graph MCP Server

A Model Context Protocol server for querying the DOREMUS music knowledge graph
via SPARQL endpoint at https://data.doremus.org/sparql/
"""

import logging
from typing import Any, Optional
import requests
from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import PlainTextResponse
import os
from query_builder import build_works_query
from find_paths import load_graph, find_k_shortest_paths

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("doremus-mcp")

# Initialize FastMCP server
mcp = FastMCP("DOREMUS Knowledge Graph Server")

@mcp.custom_route("/health", methods=["GET"])
async def health_check(request: Request) -> PlainTextResponse:
    return PlainTextResponse("OK")

# SPARQL endpoint configuration
SPARQL_ENDPOINT = "https://data.doremus.org/sparql/"
REQUEST_TIMEOUT = 60

PREFIXES = {
    "mus": "http://data.doremus.org/ontology#",
    "ecrm": "http://erlangen-crm.org/current/",
    "efrbroo": "http://erlangen-crm.org/efrbroo/",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "modsrdf": "http://www.loc.gov/standards/mods/rdf/v1/#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "geonames": "http://www.geonames.org/ontology#",
    "time": "http://www.w3.org/2006/time#",
    "rdf" : "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dbpprop" : "http://dbpedia.org/property/",
    "schema" : "http://schema.org/"
}

# Helper: expand prefixed URI to full URI
def expand_prefixed_uri(uri: str) -> str:
    if uri.startswith("<") and uri.endswith(">"):
        uri = uri[1:-1]
    if ":" in uri:
        prefix, local = uri.split(":", 1)
        if prefix in PREFIXES:
            return f"{PREFIXES[prefix]}{local}"
    return uri

# Helper: contract full URI to prefixed name
def contract_uri(uri: str) -> str:
    for prefix, base in PREFIXES.items():
        if uri.startswith(base):
            return f"{prefix}:{uri[len(base):]}"
    return uri

def contract_uri_restrict(uri: str) -> str:
    """
    Contract a given uri if present in PREFIXES, else return None
    """
    for prefix, base in PREFIXES.items():
        if uri.startswith(base):
            return f"{prefix}:{uri[len(base):]}"
    return None

def execute_sparql_query(query: str, limit: int = 100) -> dict[str, Any]:
    """
    Execute a SPARQL query against the DOREMUS endpoint.
    
    Args:
        query: SPARQL query string
        limit: Maximum number of results (default: 100)
        
    Returns:
        Dictionary containing query results or error information
    """
    try:
        logger.info(f"Executing SPARQL query with limit {limit}")
        
        # Prepend standard PREFIX declarations
        prefix_lines = "".join(f"PREFIX {p}: <{uri}>\n" for p, uri in PREFIXES.items())
        query = prefix_lines + "\n" + query
        
        response = requests.get(
            SPARQL_ENDPOINT,
            params={"query": query},
            headers={"Accept": "application/sparql-results+json"},
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        
        data = response.json()
        results = data.get("results", {}).get("bindings", [])
        
        logger.info(f"Query returned {len(results)} results")
        
        # Simplify result structure
        simplified_results = []
        for binding in results[:limit]:
            simplified = {}
            for key, value in binding.items():
                simplified[key] = value.get("value")
            simplified_results.append(simplified)
        
        return {
            "success": True,
            "count": len(simplified_results),
            "results": simplified_results
        }
        
    except requests.exceptions.Timeout:
        logger.error("Query timeout")
        return {
            "success": False,
            "error": "Query timeout - try simplifying your query or reducing the scope"
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {str(e)}")
        return {
            "success": False,
            "error": f"Request error: {str(e)}"
        }
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {
            "success": False,
            "error": f"Unexpected error: {str(e)}"
        }
    
# Helper to sample entities for a given class URI
def sample_for_class(class_uri: str, sample_limit: int = 5) -> list[tuple[str, str]]:
    class_uri_expanded = expand_prefixed_uri(class_uri)
    q = f"""
    SELECT DISTINCT ?entity ?label
    WHERE {{
        ?entity a <{class_uri_expanded}> .
        OPTIONAL {{ ?entity rdfs:label ?label }}
    }} LIMIT {sample_limit}
    """
    res = execute_sparql_query(q, limit=sample_limit)
    samples = []
    if res.get("success"):
        for r in res.get("results", []):
            ent_uri = r.get("entity", "")
            ent_label = r.get("label", "") or ""
            samples.append((contract_uri(ent_uri), ent_label))
    return samples


def find_candidate_entities_internal(
    name: str,
    entity_type: str = "any"
) -> dict[str, Any]:
    # Build type filter (all use prefixes)
    type_filter = ""
    if entity_type == "artist":
        type_filter = "{ ?entity a foaf:Person } UNION { ?entity a ecrm:E21_Person }"
    elif entity_type == "work":
        type_filter = "?entity a efrbroo:F22_Self-Contained_Expression ."
    elif entity_type == "place":
        type_filter = "?entity a ecrm:E53_Place ."
    elif entity_type == "performance":
        type_filter = "?entity a efrbroo:F31_Performance ."
    elif entity_type == "track":
        type_filter = "?entity a mus:M24_Track ."

    query = f"""
    SELECT DISTINCT ?entity ?label ?type
    WHERE {{
        {type_filter}
        ?entity rdfs:label ?label .
        ?entity a ?type .
        FILTER (REGEX(?label, "{name}", "i"))
    }}
    LIMIT 50
    """

    result = execute_sparql_query(query, limit=50)

    def prefixify_entity(ent):
        ent = ent.copy()
        if "entity" in ent:
            ent["entity"] = contract_uri(ent["entity"])
        if "type" in ent:
            ent["type"] = contract_uri(ent["type"])
        return ent

    if result.get("success"):
        entities = [prefixify_entity(e) for e in result.get("results", [])]
        return {
            "query": name,
            "entity_type": entity_type,
            "matches_found": len(entities),
            "entities": entities
        }
    else:
        return result


# Register the callable as an MCP tool but keep a plain Python function exported for tests
@mcp.tool()
async def find_candidate_entities(name: str, entity_type: str = "any") -> dict[str, Any]:
    """
    Find entities by name using case-insensitive search.
    
    Use this tool to discover the unique URI identifier for an entity before retrieving
    detailed information or using it in other queries.
    
    Args:
        name: The name to search for (e.g., "Mozart", "Symphony No. 5", "Vienna", "Radio France")
        entity_type: Type of entity to search for. Options:
            - "artist": Composers, performers, conductors (foaf:Person or ecrm:E21_Person)
            - "work": Musical works/expressions (efrbroo:F22_Self-Contained_Expression)
            - "place": Geographic locations/venues (ecrm:E53_Place)
            - "performance": Live performances (efrbroo:F31_Performance)
            - "track": Individual tracks on albums (mus:M24_Track)
            - "any": Search across all entity types (default)
        
    Returns:
        Dictionary with matching entities, including their URIs, labels, and types
        
    Examples:
        - find_candidate_entities("Beethoven", "artist")
        - find_candidate_entities("Don Giovanni", "work")
        - find_candidate_entities("Royal Albert Hall", "place")
        - find_candidate_entities("Radio France", "organization")
        - find_candidate_entities("violin", "any")
    """
    return find_candidate_entities_internal(name, entity_type)

def get_entity_details_internal(
    entity_uri: str,
    include_labels: bool = True,
    depth: int = 1
) -> dict[str, Any]:
    """
    Internal function to retrieve entity details with recursion support.
    """
    # Expand input URI if prefixed
    entity_uri_expanded = expand_prefixed_uri(entity_uri)
    query = f"""
    SELECT DISTINCT ?property ?value
    WHERE {{
           <{entity_uri_expanded}> ?property ?value .
           FILTER (
              !(?property = rdfs:comment) || lang(?value) = "en"
           )
    }}
    LIMIT 200
    """
    result = execute_sparql_query(query, limit=200)
    if not result["success"]:
        return result
    # Organize all properties
    properties = {}
    entity_label = None
    linked_entity_uris = set()
    for binding in result["results"]:
        prop = binding.get("property", "")
        value = binding.get("value", "")
        # Contract URIs to prefixes
        prop_prefixed = contract_uri_restrict(prop)
        # If uri not present in PREFIXES ignore the property
        if prop_prefixed is None:
            continue
        value_prefixed = contract_uri(value) if value.startswith("http://") or value.startswith("https://") else value
        
        if prop_prefixed.endswith(":label") and not entity_label:
            entity_label = value
        # Track URIs for linked entities
        if value.startswith("http://") or value.startswith("https://"): 
            linked_entity_uris.add(value) #TODO not always works
        # Store property
        if prop_prefixed not in properties:
            properties[prop_prefixed] = []
        properties[prop_prefixed].append(value_prefixed)
    # Build basic response
    response = {
        "entity_uri": contract_uri(entity_uri_expanded),
        "entity_label": entity_label,
        "properties": properties
    }
    # Optionally resolve labels for linked entities
    if include_labels and linked_entity_uris:
        linked_entities = {}
        uris_str = " ".join([f"<{uri}>" for uri in list(linked_entity_uris)[:50]])  # Limit to 50
        label_query = f"""
        SELECT DISTINCT ?entity ?label
        WHERE {{
            VALUES ?entity {{ {uris_str} }}
            OPTIONAL {{ ?entity rdfs:label ?label }}
        }}
        """
        label_result = execute_sparql_query(label_query, limit=100)
        if label_result["success"]:
            for binding in label_result["results"]:
                uri = binding.get("entity", "")
                label = binding.get("label", "")
                if label:
                    linked_entities[contract_uri(uri)] = label
        response["linked_entities"] = linked_entities
    # Optionally fetch details of linked entities (depth >= 2)
    if depth > 1 and linked_entity_uris:
        related_details = {}
        # Only fetch details for first 20 linked entities to avoid timeout
        for linked_uri in list(linked_entity_uris)[:20]:
            nested_result = get_entity_details_internal(
                contract_uri(linked_uri),
                include_labels=False,
                depth=depth - 1
            )
            if nested_result.get("entity_label"):
                related_details[contract_uri(linked_uri)] = {
                    "label": nested_result.get("entity_label"),
                    "properties": nested_result.get("properties", {})
                }
        if related_details:
            response["related_entity_details"] = related_details
    return response

# Register tool wrapper and export plain callable
@mcp.tool()
async def get_entity_details(entity_uri: str, include_labels: bool = True, depth: int = 1) -> dict[str, Any]:
    """
    Retrieve detailed information about a specific entity with optional recursive resolution.
    
    Use this as the first step after finding an entity with find_candidate_entities.
    This is the "business card" tool - it shows all direct properties of an entity.
    
    Args:
        entity_uri: The full URI of the entity (e.g., "http://data.doremus.org/artist/...")
        include_labels: If True, automatically fetch human-readable labels for all linked entity URIs (default: True).
                       This helps you understand what entities are connected without needing separate lookups.
        depth: How deep to fetch related entity details:
               - 1 (default): Only this entity's properties, with labels for linked entities
               - 2 or more: Also fetch full details of linked entities (slower but more complete)
        
    Returns:
        Dictionary with:
        - entity_uri: The requested entity
        - entity_label: Human-readable name
        - properties: All properties as key-value pairs (property name → list of values)
        - linked_entities: Dict mapping entity URIs to their labels (if include_labels=True)
        - related_entity_details: Full details of linked entities (if depth >= 2)
        
    Examples:
        # Basic usage - get entity properties with labels
        get_entity_details("http://data.doremus.org/artist/123")
        
        # Deep dive - get composer details from a work in one call
        get_entity_details("http://data.doremus.org/expression/456", depth=2)
        
        # Fast mode - skip label resolution
        get_entity_details("http://data.doremus.org/performance/789", include_labels=False)
    """
    return get_entity_details_internal(entity_uri, include_labels, depth)

@mcp.tool()
def find_paths(start_entity: str, end_entity: str, k: int = 5) -> dict[str, Any]:
    """
    Find the top k shortest paths between two node types in the local graph.
    
    Use this tool to explore the topology and connnecting two node types e.g. ecrm:E21_Person and mus:M42_Performed_Expression_Creation
    Args:
        start_entity: Prefixed URI of the type start node
        end_entity: Prefixed URI of the type end node
        k: Number of shortest paths to return (5-10 works most of the times)
    Returns:
        Dict with 'paths': list of paths, each path is a list of triples (subject, predicate, object) in prefix form
    """
    # Load graph from CSV (cache in memory for repeated calls)
    graph = load_graph("graph.csv")
    # Find paths
    paths = find_k_shortest_paths(graph, start_entity, end_entity, k)
    return {"paths": paths, "count": len(paths)}

def search_musical_works_internal(
    composers: Optional[list[str]] = None,
    work_type: Optional[str] = None,
    date_start: Optional[int] = None,
    date_end: Optional[int] = None,
    instruments: Optional[list[dict[str, Any]]] = None,
    place_of_composition: Optional[str] = None,
    place_of_performance: Optional[str] = None,
    duration_min: Optional[int] = None,
    duration_max: Optional[int] = None,
    topic: Optional[str] = None,
    limit: int = 50
) -> dict[str, Any]:
    
    try:
        query = build_works_query(
            composers=composers,
            work_type=work_type,
            date_start=date_start,
            date_end=date_end,
            instruments=instruments,
            place_of_composition=place_of_composition,
            place_of_performance=place_of_performance,
            duration_min=duration_min,
            duration_max=duration_max,
            topic=topic,
            limit=min(limit, 200)
        )
        
        result = execute_sparql_query(query, limit=limit)

        if result.get("success"):
            return {
                "filters_applied": {
                    "composers": composers,
                    "work_type": work_type,
                    "date_range": f"{date_start}-{date_end}" if date_start or date_end else None,
                    "instruments": instruments,
                    "duration_range": f"{duration_min}-{duration_max}s" if duration_min or duration_max else None
                },
                "total_results": result.get("count", 0),
                "works": result.get("results", [])
            }
        else:
            return result
            
    except Exception as e:
        logger.error(f"Error building works query: {str(e)}")
        return {
            "success": False,
            "error": f"Query building error: {str(e)}"
        }

@mcp.tool()
async def search_musical_works(
        composers: Optional[list[str]] = None,
        work_type: Optional[str] = None,
        date_start: Optional[int] = None,
        date_end: Optional[int] = None,
        instruments: Optional[list[dict[str, Any]]] = None,
        place_of_composition: Optional[str] = None,
        place_of_performance: Optional[str] = None,
        duration_min: Optional[int] = None,
        duration_max: Optional[int] = None,
        topic: Optional[str] = None,
        limit: int = 50
    ) -> dict[str, Any]:
    """
    Search for musical works with flexible filtering criteria.
    
    This is the main tool for querying works in the DOREMUS knowledge graph with
    support for multiple filter combinations.
    
    Args:
        composers: List of composer names or URIs (e.g., ["Mozart", "Beethoven"])
        work_type: Type/genre of work (e.g., "sonata", "symphony", "concerto")
        date_start: Start year for composition date range (e.g., 1800)
        date_end: End year for composition date range (e.g., 1850)
        instruments: List of instrument specifications, each with:
            - name: instrument name/URI (e.g., "violin", "piano")
            - quantity: exact number (optional)
            - min_quantity: minimum number (optional)
            - max_quantity: maximum number (optional)
        place_of_composition: Place where work was composed
        place_of_performance: Place where work was performed
        duration_min: Minimum duration in seconds
        duration_max: Maximum duration in seconds
        topic: Topic or subject matter of the work
        limit: Maximum number of results (default: 50, max: 200)
        
    Returns:
        Dictionary with matching works and their details
        
    Examples:
        - search_musical_works(composers=["Mozart"], work_type="sonata")
        - search_musical_works(instruments=[{"name": "violin", "quantity": 2}, {"name": "viola"}])
        - search_musical_works(date_start=1800, date_end=1850, place_of_composition="Vienna")
    """
    return search_musical_works_internal(composers, work_type, date_start, date_end, instruments, place_of_composition, place_of_performance, duration_min, duration_max, topic, limit)


def execute_custom_sparql_internal(query: str, limit: int = 100) -> dict[str, Any]:
    limited_query = query
    if "LIMIT" not in query.upper():
        limited_query = f"{query}\nLIMIT {min(limit, 500)}"

    return execute_sparql_query(limited_query, limit=min(limit, 500))


# Register tool wrapper and export plain callable
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
    return execute_custom_sparql_internal(query, limit)


# Documentation tools

@mcp.tool()
def get_kg_structure() -> str:
    """
    Dynamically retrieve a description of the DOREMUS Knowledge Graph structure.

    This tool provides essential information about the ontology, including:
        - Ontology prefixes
        - Main entity types (classes)
        - Key properties and relationships
        - Common URI patterns

    Returns:
        str: Markdown-formatted description (classes, properties, samples, prefixes).
    """

    # Schema query (classes & properties)
    schema_query = """
    SELECT DISTINCT ?entity ?type
    WHERE {
        { ?entity a rdfs:Class . BIND("rdfs:Class" as ?type) } UNION
        { ?entity a owl:Class . BIND("owl:Class" as ?type) } UNION
        { ?entity a rdf:Property . BIND("rdf:Property" as ?type) } UNION
        { ?entity a owl:ObjectProperty . BIND("owl:ObjectProperty" as ?type) } UNION
        { ?entity a owl:DatatypeProperty . BIND("owl:DatatypeProperty" as ?type) } UNION
        { ?entity a owl:AnnotationProperty . BIND("owl:AnnotationProperty" as ?type) }
    }
    ORDER BY ?type ?entity
    LIMIT 500
    """

    try:
        schema_results = execute_sparql_query(schema_query, limit=500)
        if not schema_results.get("success"):
            raise RuntimeError(f"Schema query failed: {schema_results.get('error')}")

        # Group schema entities by type
        from collections import defaultdict
        schema_by_type = defaultdict(list)
        for row in schema_results.get("results", []):
            etype = row.get("type", "unknown")
            entity = row.get("entity", "")
            schema_by_type[etype].append(entity)

        # Candidate class URIs to sample (prefer common ones used in previous static doc)
        candidate_classes = {
            "Expression (work/expression)": [
                "http://erlangen-crm.org/efrbroo/F22_Self-Contained_Expression",
                "http://data.doremus.org/ontology#F22_Self-Contained_Expression"
            ],
            "Individual Work (abstract work)": [
                "http://erlangen-crm.org/efrbroo/F14_Individual_Work",
                "http://data.doremus.org/ontology#F14_Individual_Work"
            ],
            "Artist / Person": [
                "http://xmlns.com/foaf/0.1/Person",
                "http://erlangen-crm.org/current/E21_Person"
            ],
            "Performance (event)": [
                "http://erlangen-crm.org/efrbroo/F31_Performance",
                "http://data.doremus.org/ontology#F31_Performance"
            ],
            "Track": [
                "http://data.doremus.org/ontology#M24_Track"
            ],
            "Vocabulary term (genre/instrument)": [
                "http://www.w3.org/2004/02/skos/core#Concept"  # vocab terms are often SKOS concepts
            ]
        }

        # Collect samples
        samples_md = []
        sampled_uris_for_patterns = []
        for label, uris in candidate_classes.items():
            samples_md.append(f"### Samples for {label}")
            found_any = False
            for uri in uris:
                s = sample_for_class(uri, sample_limit=5)
                if s:
                    found_any = True
                    samples_md.append(f"- Class URI tried: <{uri}>")
                    for ent_uri, ent_label in s:
                        samples_md.append(f"  - {ent_label or '(no label)'} — <{ent_uri}>")
                        sampled_uris_for_patterns.append(ent_uri)
                    break
            if not found_any:
                samples_md.append(f"- No sample entities found for known URIs for `{label}` (endpoint may have different class URIs).")

        # Infer URI patterns from sampled URIs
        patterns = set()
        for u in sampled_uris_for_patterns:
            if "/expression/" in u:
                patterns.add("http://data.doremus.org/expression/{uuid}")
            if "/artist/" in u:
                patterns.add("http://data.doremus.org/artist/{uuid}")
            if "/place/" in u:
                patterns.add("http://data.doremus.org/place/{uuid}")
            if "/vocabulary/" in u or "/vocabularies/" in u:
                patterns.add("http://data.doremus.org/vocabulary/{domain}/{term}")
            # fallback heuristics
            if u.startswith("http://sws.geonames.org/"):
                patterns.add("http://sws.geonames.org/{geonameid}/")
            if "mimo-db.eu" in u:
                patterns.add("MIMO URIs (e.g. http://www.mimo-db.eu/InstrumentsKeywords/{id})")

        patterns_md = ["## Inferred URI patterns"]
        if patterns:
            for p in sorted(patterns):
                patterns_md.append(f"- `{p}`")
        else:
            patterns_md.append("- No reliable URI patterns inferred from samples.")

        # Summarize schema counts and list a few top classes/properties
        classes = schema_by_type.get("owl:Class", []) + schema_by_type.get("rdfs:Class", [])
        properties = (
            schema_by_type.get("rdf:Property", [])
            + schema_by_type.get("owl:ObjectProperty", [])
            + schema_by_type.get("owl:DatatypeProperty", [])
            + schema_by_type.get("owl:AnnotationProperty", [])
        )

        schema_summary = [
            f"Found {len(classes)} classes and {len(properties)} properties (unique entities from schema query).",
            "",
            "### A few notable classes (sample)",
        ]
        for c in classes[:25]:
            schema_summary.append(f"- <{c}>")

        schema_summary.append("")
        schema_summary.append("### A few notable properties (sample)")
        for p in properties[:25]:
            schema_summary.append(f"- <{p}>")

        # Build final markdown
        md_parts = [
            "# DOREMUS Knowledge Graph - Dynamic Structure",
            "",
            "## Schema summary",
            ""
        ]
        md_parts.extend(schema_summary)
        md_parts.append("")
        md_parts.append("## Representative samples")
        md_parts.append("")
        md_parts.extend(samples_md)
        md_parts.append("")
        md_parts.extend(patterns_md)
        md_parts.append("")
        md_parts.append("## Notes and tips")
        md_parts.append(
            "- The schema query lists declared classes and properties; not every class has many instances.\n"
            "- If a sample query returned no results for a class, the endpoint may expose the concept under a different URI. Use `find_candidate_entities()` and `get_entity_details()` to discover exact URIs.\n"
            "- Use `LIMIT` and `SAMPLE()` in SPARQL to avoid duplicates and timeouts.\n"
        )
        md_parts.append("")
        md_parts.append("## How to get more (examples)")
        md_parts.append(
            "- Find a composer: `find_candidate_entities(\"Beethoven\", \"artist\")`\n"
            "- Get full details: `get_entity_details(<uri>, depth=2)`\n"
            "- Run a custom query: `execute_custom_sparql(<sparql>)`\n"
        )

        return "\n".join(md_parts)

    except Exception as e:
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
        # Fallback: return compact static description (keeps the server useful if endpoint is unreachable)
        logger.exception("Dynamic get_kg_structure failed, returning fallback structure.")
        fallback = """
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
    
    return fallback


@mcp.tool()
def get_usage_guide() -> str:
    """
    Get a comprehensive usage guide and prompt for LLMs interacting with DOREMUS.
    
    This tool provides guidance on:
    - How to effectively use the available tools
    - Common query patterns and workflows
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

### 3. search_musical_works
**When to use**: For most work discovery queries with filtering criteria.

**Why**: This is optimized for the most common use cases with a flexible
parameter-based interface.

**Key features**:
- Composer filtering (by name or URI)
- Genre/type filtering (sonata, symphony, concerto, etc.)
- Date range filtering
- Instrumentation filtering with quantity specifications
- Duration filtering
- Place filtering (composition or performance)

**Example workflows**:

Simple composer search:
```
User: "Show me works by Mozart"
search_musical_works(composers=["Wolfgang Amadeus Mozart"], limit=20)
```

Complex instrumentation:
```
User: "Find works for 2 violins, viola, and cello"
search_musical_works(
    instruments=[
        {"name": "violin", "quantity": 2},
        {"name": "viola", "quantity": 1},
        {"name": "cello", "quantity": 1}
    ]
)
```

Combined filters:
```
User: "German chamber music from 1800-1850"
1. First search for works with date range
2. Consider using custom SPARQL for nationality filter
```

### 4. execute_custom_sparql
**When to use**: For complex queries not covered by search_musical_works.

**Why**: Provides maximum flexibility for specialized queries.

**When NOT to use**: If search_musical_works can handle it - the query builder
is optimized and tested.

**Before using**:
1. Check the knowledge graph structure resource
2. Look at example queries from competency questions
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
   - Discovery: find_candidate_entities → search_musical_works
   - Deep dive: search_musical_works → get_entity_details for each result
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

## Common Query Patterns

### Pattern 1: Composer Catalog
```
Goal: List all works by a specific composer
Steps:
1. find_candidate_entities(composer_name, "composer")
2. search_musical_works(composers=[uri], limit=100)
```

### Pattern 2: Genre Exploration
```
Goal: Explore a musical genre/type
Steps:
1. search_musical_works(work_type="sonata", limit=50)
2. Optionally filter by composer, date, instruments
```

### Pattern 3: Instrumentation Discovery
```
Goal: Find works for specific ensemble
Steps:
1. find_candidate_entities for each instrument (if needed)
2. search_musical_works with instruments list
3. Consider strict vs. flexible matching (exactly these vs. including these)
```

### Pattern 4: Historical Period
```
Goal: Works from a specific time period
Steps:
1. search_musical_works(date_start=X, date_end=Y)
2. Optionally filter by place or composer nationality
```

### Pattern 5: Performance Research
```
Goal: When/where was a work performed?
Steps:
1. find_candidate_entities for the work
2. execute_custom_sparql to find performance history
```

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

## Data Limitations & Workarounds

### Missing Data
- Not all works have all properties
- Use OPTIONAL in custom SPARQL
- Report what's available, note what's missing

### Incomplete Coverage
- Focus on European classical music
- Better coverage for certain periods/composers
- Set expectations with user

### No "popularity" metric
- Can count performances/recordings
- Cannot directly rank by "importance"
- Suggest manual curation or external sources

## Example User Interactions

### Example 1: Simple Discovery
```
User: "Show me Mozart's piano concertos"
Assistant:
1. find_candidate_entities("Mozart", "composer")
   → Found: Wolfgang Amadeus Mozart, URI: ...
2. search_musical_works(
     composers=[mozart_uri],
     work_type="concerto",
     instruments=[{"name": "piano"}],
     limit=30
   )
   → Found 27 piano concertos
3. Present formatted list with K. numbers if available
```

### Example 2: Complex Research
```
User: "What chamber music for strings was composed in Vienna between 1780 and 1800?"
Assistant:
1. search_musical_works(
     date_start=1780,
     date_end=1800,
     place_of_composition="Vienna",
     limit=100
   )
2. Filter results programmatically for string instruments only
3. Group by composer
4. Present organized results
Note: Place of composition data may be incomplete - consider mentioning this
```

### Example 3: Exploration
```
User: "I like Beethoven's late quartets. What's similar?"
Assistant:
1. find_candidate_entities("Beethoven", "composer")
2. search_musical_works(
     composers=[beethoven_uri],
     work_type="quartet",
     date_start=1820  # Late period
   )
3. Note instrumentation pattern (2 violins, viola, cello)
4. search_musical_works(
     work_type="quartet",
     date_start=1820,
     instruments=[same pattern]
   )
5. Exclude Beethoven from results
6. Present recommendations with context
```

## Advanced: Custom SPARQL Scenarios

### Scenario 1: Works frequently performed together
Query concerts/albums where work X and work Y appear together

### Scenario 2: Composer collaborations
Find works where two people collaborated (different functions)

### Scenario 3: Temporal analysis
Count works per decade, identify compositional trends

### Scenario 4: Instrument evolution
Track instrument usage over time periods

### Scenario 5: Geographic mapping
Distribution of composers by birthplace or composition location

For these scenarios, consult the knowledge graph structure resource and
build appropriate SPARQL queries using execute_custom_sparql.

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


def get_nodes_list() ->str:
    """
    Get the list of all node types, use this to identify useful nodes before find_path tool
    """
    nodes = """
    time:Instant
    mus:M28_Individual_Performance
    ecrm:E52_Time-Span
    time:Interval
    ecrm:E7_Activity
    efrbroo:F28_Expression_Creation
    mus:M156_Title_Statement
    ecrm:E13_Attribute_Assignment
    efrbroo:F22_Self-Contained_Expression
    mus:M46_Set_of_Tracks
    mus:M44_Performed_Work
    mus:M43_Performed_Expression
    mus:M42_Performed_Expression_Creation
    efrbroo:F14_Individual_Work
    efrbroo:F15_Complex_Work
    mus:M19_Categorization
    mus:M23_Casting_Detail
    efrbroo:F26_Recording
    efrbroo:F21_Recording_Work
    ecrm:E21_Person
    mus:M157_Statement_of_Responsibility
    efrbroo:F24_Publication_Expression
    efrbroo:F20_Performance_Work
    efrbroo:F30_Publication_Event
    efrbroo:F25_Performance_Plan
    mus:M160_Publication_Statement
    mus:M161_Distribution_Statement
    efrbroo:F31_Performance
    efrbroo:F3_Manifestation_Product_Type
    mus:M6_Casting
    efrbroo:F19_Publication_Work
    mus:M158_Title_and_Statement_of_Responsibility
    ecrm:E42_Identifier
    mus:M155_Cast_Statement
    efrbroo:F29_Recording_Event
    efrbroo:F42_Representative_Expression_Assignment
    ecrm:E54_Dimension
    ecrm:E67_Birth
    mus:M31_Actor_Function
    mus:M29_Editing
    efrbroo:F38_Character
    mus:M24_Track
    mus:M171_Container
    ecrm:E69_Death
    efrbroo:F11_Corporate_Body
    mus:M2_Opus_Statement
    mus:M1_Catalogue_Statement
    ecrm:E53_Place
    efrbroo:F25_PerformancePlan
    mus:M27_Foreseen_Individual_Performance
    mus:M167_Publication_Expression_Fragment
    efrbroo:F4_Manifestation_Singleton
    mus:M39_Derivation_Type_Assignment
    skos:Concept
    mus:M15_Dedication_Statement
    mus:M33_Set_of_Characters
    mus:M45_Descriptive_Expression_Assignment
    mus:M15_Dedication
    mus:M14_Medium_Of_Performance
    efrbroo:F19_Publication_Expression
    ecrm:E1_CRM_Entity
    mus:M154_Label_Name
    mus:M26_Foreseen_Performance
    geonames:Feature
    foaf:Document
    efrbroo:F32_Carrier_Production_Event
    ecrm:E39_Actor
    mus:M40_Context
    mus:M159_Edition_Statement
    ecrm:E66_Formation
    mus:M50_Creation_or_Performance_Mode
    mus:M4_Key
    mus:M25_Foreseen_Activity
    mus:M5_Genre
    mus:M36_Award
    modsrdf:NoteGroup
    modsrdf:ModsResource
    rdfs:Class
    ecrm:E22_Man-Made_Object
    ecrm:E68_Dissolution
    skos:ConceptScheme
    rdfs:Datatype
    ecrm:E4_Period
    """
    return nodes

if __name__ == "__main__":
    # Run the MCP server
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    
    mcp.run(transport="sse", host=host, port=port)
