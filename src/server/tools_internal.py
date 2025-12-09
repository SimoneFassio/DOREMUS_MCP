import pathlib
import logging
import os
import re 
from nanoid import generate
from typing import Any, Optional, Dict, List
from difflib import get_close_matches
from src.server.find_paths import load_graph
from src.server.graph_schema_explorer import GraphSchemaExplorer
from src.server.query_container import QueryContainer
from src.server.query_builder import query_works, query_performance, query_artist
from src.server.find_paths import find_k_shortest_paths
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

def find_linked_entities(subject: str, obj: str) -> List[str] | None:
    object_entity = find_candidate_entities_internal(obj, "vocabulary")
    if not object_entity.get("success"):
        return None
    return [obj for obj in object_entity.get("entities", [])]
    
    
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
    
# helper that searches in the provided graph for a match with the term
def find_term_in_graph_internal(term: str, graph, node=True) -> List[str]:
    if node:
        # use the nodes of the graph
        vals = graph.keys()
    else:
        # use the predicates of the graph
        vals = set()
        for edges in graph.values():
            for pred, neighbor in edges:
                vals.add(pred)
    
    # 1. Regex-based substring match (case-insensitive)
    regex = re.compile(re.escape(term), re.IGNORECASE)  # Create a case-insensitive regex pattern
    regex_matches = [node for node in vals if regex.search(node)]
    
    if regex_matches:
        return regex_matches  # Return matches in their original form (un-lowered)

    # 2. Raise an error if no matches are found
    raise ValueError(f"No matches found in graph for term: {term} in nodes {list(vals)}")
    
def associate_to_N_entities_internal(subject: str, obj: str, query_id: str, N: int | None) -> List[dict]:
    # Check for query existance
    qc = QUERY_STORAGE.get(query_id)
    if not qc:
        return {
            "success": False,
            "error": f"Query ID {query_id} not found or expired."
        }
    
    # Find object entity URI
    object_ents = find_candidate_entities_internal(obj, "vocabulary")
    object_names = [obj.get("label") for obj in object_ents.get("entities", [])]
    # Compute edit distance to find best match
    object_entity_uri = ""
    best_match = get_close_matches(obj, object_names, n=1)
    for o in object_ents.get("entities", []):
        if o.get("label") == best_match[0]:
            object_entity_uri = o.get("entity")
            break
    
    # Find inverse arcs
    query_inverse = f"""
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

        SELECT ?incoming_property (SAMPLE(?item_pointing_at_me) AS ?single_example)
        WHERE {{
        # 1. FIX THE TARGET
        VALUES ?my_entity {{ <{object_entity_uri}> }} .

        # 2. Get basic info
        ?my_entity skos:prefLabel ?label .
        ?my_entity a ?type .

        # 3. Find incoming links
        ?item_pointing_at_me ?incoming_property ?my_entity .

        }} 
        # Group by the "Keys" (The things that should be unique per row)
        GROUP BY ?incoming_property
    """
    result_inverse = execute_sparql_query(query_inverse, limit=50)
    
    # Build subpath from subject to object
    subgraphs = [res for res in result_inverse.get("results", [])]
    subpath = []
    full_path = []
    for subgraph in subgraphs:
        incoming_property = subgraph.get("incoming_property").split("#")[-1]
        path = [elem for elem in subgraph.get("single_example").split("/") if elem]
        #TODO: fix to better generalize
        if subject.lower() in path:
            subpath = path
            full_path.append(obj)
            actual_incoming_property = find_term_in_graph_internal(incoming_property, graph, False)[0]
            full_path.append(actual_incoming_property)
            break
    #TODO: fix to better generalize    
    entities = subpath[2::2]
    if len(entities) < 2:
        raise ValueError("The entities list must contain at least 2 elements to compute paths.")
    for k in range(len(entities)-1, 0, -1):
        current_entity = entities[k]
        full_path.append(entities[k])
        previous_entity = entities[k-1]
        current_entity_uris = find_term_in_graph_internal(current_entity, graph)
        if not current_entity_uris:
            raise ValueError(f"No matches found in graph for entity: {current_entity} with entity list {entities}")
        current_entity_uri = current_entity_uris[0]
        previous_entity_uris = find_term_in_graph_internal(previous_entity, graph)
        if not previous_entity_uris:
            raise ValueError(f"No matches found in graph for entity: {previous_entity} with entity list {entities}")
        previous_entity_uri = previous_entity_uris[0]
        logger.debug(f"Entities: {entities}")
        logger.debug(f"Current entity: {current_entity}")
        logger.debug(f"Previous entity URI: {previous_entity_uri}")
        logger.debug(f"Current entity URI: {current_entity_uri}")
        path = find_k_shortest_paths(graph, previous_entity_uri, current_entity_uri, 1)[0]
        # First triplet + second element (the predicate)
        #TODO: check path consistency
        """
        # CHECK IF COMPLETE PATH IS FOUND
        tentative_best_paths = find_k_shortest_paths(graph, previous_entity_uri, last_entity_uri, len(entities)-k)
        #find the best path that includes the path computed so far
        for tentative_path in tentative_best_paths:
            if tentative_path[2:] == full_path:
                path = tentative_path
                break
        """
        full_path.append(path[0][1])
    full_path.append(subject)
    full_path.reverse()
    pattern_list = [(f"?{full_path[i]} {full_path[i+1]} ?{full_path[i+2]} .", f"Link from {full_path[i]} to {full_path[i+2]}") for i in range(0, len(full_path)-2, 2)]
    #TODO: Find occurrency in a dynamic way
    if N is not None:
        pattern_list.append((f"?{full_path[-3]} mus:U30_foresees_quantity_of_mop {str(N)} .", "Get the number of medium of performances"))
    pattern_list.append((f"VALUES (?{obj}) {{ (<{object_entity_uri}>) }} .", "Save the variable for the object entity"))
    qc.add_module({
        "id": f"associate_N_entities_module_{full_path[-1]}",
        "triples": pattern_list,
        "type": "associate_N_entities_pattern",
        "required_vars": [f"?{subject}"],
        "defined_vars": [f"?{ent}" for ent in entities]
    })
    sparql_query = qc.to_string()
    return {
            "success": True,
            "query_id": query_id,
            "generated_sparql": sparql_query,
            "message": "Query pattern added successfully. Review the SPARQL. If correct, use execute_query(query_id) to run it."
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
        f.write("LIMIT: " + str(qc.get_limit()))
        
    return execute_sparql_query(qc.to_string(), qc.get_limit())


if __name__ == "__main__":
    # Example usage
    test_entity = "violin"
    result = find_linked_entities("Casting", test_entity)
    print(f"Linked entities for '{test_entity}': {result}")