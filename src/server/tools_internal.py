import pathlib
import logging
import os
from nanoid import generate
from typing import Any, Optional, Dict, List
from src.server.find_paths import load_graph
from src.server.graph_schema_explorer import GraphSchemaExplorer
from src.server.query_container import QueryContainer
from src.server.query_builder import query_works, query_performance, query_artist
from src.server.utils import (
    execute_sparql_query,
    contract_uri,
    contract_uri_restrict,
    expand_prefixed_uri,
    get_entity_label
)

logger = logging.getLogger("doremus-mcp")

#load graph for find_path
project_root = pathlib.Path(__file__).parent.parent.parent
graph_path = project_root / "data" / "graph.csv"
graph = load_graph(str(graph_path))

#load graph schema explorer for ontology exploration
explorer = GraphSchemaExplorer.load_from_csv()

# Storage for generated queries
QUERY_STORAGE: Dict[str, QueryContainer] = {}


def find_candidate_entities_internal(
    name: str,
    entity_type: str = "others"
) -> Dict[str, Any]:
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
        ?label bif:contains "{search_literal}" option (score ?sc) .
    }}
    ORDER BY DESC(?sc)
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
) -> Dict[str, Any]:
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
    

def get_ontology_internal(path: str, depth: int = 1) -> str:
    try:
        return explorer.explore_graph_schema(path=path, depth=depth)
    except Exception as e:
        logger.error(f"Error exploring ontology: {str(e)}")
        return f"Error exploring ontology: {str(e)}"

def build_query_internal(
    question: str,
    template: str,
    filters: Dict[str, Any]
) -> Dict[str, Any]:
    try:
        # Standardize template name
        template = template.lower().strip()

        # Generate ID and Store
        query_id = generate(size=10)
        
        if template == "works":
            qc = query_works(
                query_id=query_id,
                **filters
            )
        elif template == "performances":
            qc = query_performance(
                query_id=query_id,
                **filters
            )
        elif template == "artists":
            qc = query_artist(
                query_id=query_id,
                **filters
            )
        else:
            return {
                "success": False,
                "error": f"Unknown template: {template}. Supported templates: Works, Performances, Artists"
            }

        sparql_query = qc.to_string()
        qc.set_question(question)
        
        QUERY_STORAGE[query_id] = qc
        
        return {
            "success": True,
            "query_id": query_id,
            "generated_sparql": sparql_query,
            "message": "Query built successfully. Review the SPARQL. If correct, use execute_query(query_id) to run it."
        }
        
    except Exception as e:
        logger.error(f"Error building query: {e}")
        return {
            "success": False,
            "error": str(e)
        }

def execute_query_from_id_internal(query_id: str) -> Dict[str, Any]:
    qc = QUERY_STORAGE.get(query_id)
    if not qc:
        return {
            "success": False,
            "error": f"Query ID {query_id} not found or expired."
        }
    
    # Write query and ID to file, create directory if it doesn't exist
    os.makedirs("queries", exist_ok=True)
    with open(f"queries/{query_id}.txt", "w") as f:
        f.write("Question: \n" + qc.get_question())
        f.write("\n\n")
        f.write("SPARQL Query: \n" + qc.to_string())
        
    return execute_sparql_query(qc.to_string())
