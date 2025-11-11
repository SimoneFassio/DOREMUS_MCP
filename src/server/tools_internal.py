import pathlib
from typing import Any, Optional
from src.server.query_builder import build_works_query
from src.server.find_paths import load_graph
from src.server.utils import (
    execute_sparql_query,
    contract_uri,
    contract_uri_restrict,
    expand_prefixed_uri,
    logger
)

#load graph for find_path
project_root = pathlib.Path(__file__).parent.parent.parent
graph_path = project_root / "data" / "graph.csv"
graph = load_graph(str(graph_path))

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
