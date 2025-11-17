import pathlib
from typing import Any, Optional
from src.server.query_builder import build_works_query
from src.server.find_paths import load_graph
from src.server.graph_schema_explorer import GraphSchemaExplorer
from src.server.utils import (
    execute_sparql_query,
    contract_uri,
    contract_uri_restrict,
    expand_prefixed_uri,
    logger,
    get_entity_label
)

#load graph for find_path
project_root = pathlib.Path(__file__).parent.parent.parent
graph_path = project_root / "data" / "graph.csv"
graph = load_graph(str(graph_path))

#load graph schema explorer for ontology exploration
explorer = GraphSchemaExplorer.load_from_csv()

def find_candidate_entities_internal(
    name: str,
    entity_type: str = "others"
) -> dict[str, Any]:
    normalized_type = (entity_type or "").strip().lower()
    if normalized_type not in {"artist", "vocabulary", "others"}:
        normalized_type = "others"

    label_predicates = {
        "artist": "foaf:name",
        "vocabulary": "skos:prefLabel",
        "others": "rdfs:label",
    }

    label_predicate = label_predicates[normalized_type]

    search_term = (name or "").strip()
    if not search_term:
        return {
            "success": False,
            "error": "Name is required to search for entities."
        }

    search_term_escaped = search_term.replace("'", "''").replace('"', '\\"')
    search_literal = f"'{search_term_escaped}'"

    query = f"""
    SELECT DISTINCT ?entity ?label ?type
    WHERE {{
        ?entity {label_predicate} ?label .
        ?entity a ?type .
        ?label bif:contains "{search_literal}" .
    }}
    ORDER BY STRLEN(?label)
    """

    result = execute_sparql_query(query, limit=10)
    
    # Eliminate duplicates based on entity URI and type
    unique_entities = {}
    for e in result.get("results", []):
        ent_uri = e.get("entity")
        ent_type = e.get("type")
        key = (ent_uri, ent_type)
        if key not in unique_entities:
            unique_entities[key] = e

    if result.get("success"):
        entities = []
        for e in unique_entities.values():
            e["type"] = contract_uri(e["type"])
            entities.append(e)
        return {
            "query": name,
            "entity_type": entity_type,
            "matches_found": len(entities),
            "entities": entities
        }
    else:
        return result
    
    
def get_entity_properties_internal(
    entity_uri: str
) -> dict[str, Any]:
    """
    Internal function to retrieve entity properties
    """
    query = f"""
    SELECT DISTINCT ?property ?value
    WHERE {{
           <{entity_uri}> ?property ?value .
           FILTER (
              !(?property = rdfs:comment) || lang(?value) = "en"
           )
    }}
    """
    result = execute_sparql_query(query, limit=50)
    
    if not result["success"]:
        return result
    
    # Organize all properties
    properties = {}
    entity_label = None
    entity_type = None
    for binding in result["results"]:
        prop = binding.get("property", "")
        value = binding.get("value", "")
        # Contract URIs to prefixes
        prop_prefixed = contract_uri_restrict(prop) # If uri not present in PREFIXES ignore the property
        
        if prop_prefixed is None:
            continue
        if prop_prefixed.endswith(":label") and not entity_label:
            entity_label = value
            continue
        if prop_prefixed.endswith("type") and not entity_type:
            entity_type = contract_uri_restrict(value)
            continue
        
        # Get label for linked URIs
        if value.startswith("http://") or value.startswith("https://"): 
            label = get_entity_label(value)
            if label:
                value += f"  ({label})"
                
        # Store property
        if prop_prefixed not in properties:
            properties[prop_prefixed] = []
        properties[prop_prefixed].append(value)
    
    for key, prop in properties.items():
        if len(prop)==1:
            properties[key] = prop[0]
        else:
            properties[key] = ""
            for p in prop:
                properties[key] += f"{p}, "
        
    response = {
        "entity_uri": entity_uri,
        "entity_label": entity_label,
        "entity_type": entity_type,
        "properties": properties
    }
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


def get_ontology_internal(path: str, depth: int = 1) -> str:
    """
    Explore the DOREMUS ontology graph schema hierarchically.
    
    Args:
        path: Navigation path - use '/' for summary, or '/{ClassName}' for class properties
        depth: Exploration depth (1 or 2) for class neighborhoods
        
    Returns:
        Markdown-formatted ontology subgraph
    """
    try:
        return explorer.explore_graph_schema(path=path, depth=depth)
    except Exception as e:
        logger.error(f"Error exploring ontology: {str(e)}")
        return f"Error exploring ontology: {str(e)}"
