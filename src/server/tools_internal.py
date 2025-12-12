import pathlib
import logging
import os
import re 
from nanoid import generate
from fastmcp import Context
from typing import Any, Optional, Dict, List
from difflib import get_close_matches
from src.server.find_paths import load_graph
from src.server.graph_schema_explorer import GraphSchemaExplorer
from src.server.query_container import QueryContainer
from src.server.query_builder import query_works, query_performance, query_artist
from src.server.find_paths import find_k_shortest_paths, find_term_in_graph_internal, find_inverse_arcs_internal
from src.server.utils import (
    execute_sparql_query,
    contract_uri,
    contract_uri_restrict,
    expand_prefixed_uri,
    get_entity_label,
    find_candidate_entities_utils
)
from src.server.utils import extract_label, convert_to_variable_name
from src.server.tool_sampling import format_paths_for_llm, tool_sampling_request

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
    return find_candidate_entities_utils(name, entity_type)


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

#-------------------------------
# QUERY BUILDER INTERNALS
#-------------------------------

async def build_query_internal(
    question: str,
    template: str,
    filters: Dict[str, Any] | None,
) -> Dict[str, Any]:
    try:
        # Standardize template name
        template = template.lower().strip()

        # Generate ID and Store
        query_id = generate(size=10)

        if filters is None:
            filters = {}
        
        if template == "works":
            logger.info(f"Building 'Works' query for question: {question} with filters: {filters}")
            qc = await query_works(
                query_id=query_id,
                question=question,
                **filters
            )
        elif template == "performances":
            qc = await query_performance(
                query_id=query_id,
                question=question,
                **filters
            )
        elif template == "artists":
            qc = await query_artist(
                query_id=query_id,
                question=question,
                **filters
            )
        else:
            return {
                "success": False,
                "error": f"Unknown template: {template}. Supported templates: Works, Performances, Artists"
            }

        if not qc.dry_run_test():
            return {
                "success": False,
                "error": "Malformed query generated. Please check the provided filters -> query is: " + qc.to_string()
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

#-------------------------------
# ASSOCIATE N ENTITIES INTERNALS
#-------------------------------

# helper that finds the property to filter based on number of entities
def get_quantity_property(entity_uri: str) -> str | None:
    for node, edges in graph.items():
        for pred, _ in edges:
            if node == entity_uri and "quantity" in pred.lower():
                return pred
            
#-------------------------------
# RECURSIVE PATHFINDER
#-------------------------------
def recur_domain(current_entity: str, target_entity: str, graph, depth: int, path: List[str]) -> List[str]:
    # 1. PRUNING: Depth Limit
    if depth > 6 or current_entity not in graph:
        return []
    # 2. SUCCESS: Target Found
    if current_entity == target_entity:
        return [path]
    
    # RECURSION: Explore Parents
    res = find_inverse_arcs_internal(current_entity, graph)
    if not res.get("success"):
        # We reached a dead end
        return []
    
    parents = res.get("parents", [])
    results = []
    for neighbor, predicate in parents:
        neighbor_var = convert_to_variable_name(neighbor)
        predicate_var = convert_to_variable_name(predicate)

        # avoid cycles
        if any(neighbor == uri for _, uri in path):
            continue

        new_path = [(neighbor_var, neighbor), (predicate_var, predicate)] + path
        child_paths = recur_domain(neighbor, target_entity, graph, depth + 1, new_path)
        
        # Collect valid paths
        results.extend(child_paths)
    
    return results
        
    
async def associate_to_N_entities_internal(subject: str, obj: str, query_id: str, N: int | None, ctx: Context) -> List[dict]:
    #-------------------------------
    # CHECK IF QUERY EXISTS
    #-------------------------------
    qc = QUERY_STORAGE.get(query_id)
    # FAILSAFE: debug
    if not qc:
        return {
            "success": False,
            "error": f"Query ID {query_id} not found or expired."
        }
    subject_uri = qc.get_variable_uri(subject)
    # FAILSAFE: debug
    if not subject_uri:
        return {
            "success": False,
            "error": f"Subject variable ?{subject} not found in query."
        }
    
    #-------------------------------
    # VOCAB/ONTOLOGY SWITCH
    #-------------------------------
    if obj.startswith("http://"):
        # Vocabulary entity provided as URI
        object_entity_uri = obj
        # Find inverse arcs
        info_query = f"""
        SELECT ?label ?type
        WHERE {{
            VALUES ?my_entity {{ <{object_entity_uri}> }} . 
            ?my_entity skos:prefLabel ?label .
            ?my_entity a ?type .
        }}
        """
        info_result = execute_sparql_query(info_query, limit=1)
        # FAILSAFE: debug
        if not info_result.get("success") or len(info_result.get("results", []))==0:
            return {
                "success": False,
                "error": f"Vocabulary entity {obj} not found in the knowledge base."
            }
        object_label = info_result["results"][0].get("label")
        # Find incoming links
        query_inverse = f"""
            SELECT ?incoming_property (SAMPLE(?item_pointing_at_me) AS ?single_example)
            WHERE {{
            # 1. FIX THE TARGET
            VALUES ?my_entity {{ <{object_entity_uri}> }} .

            # 2. Find incoming links
            ?item_pointing_at_me ?incoming_property ?my_entity .

            }} 
            # Group by the "Keys" (The things that should be unique per row)
            GROUP BY ?incoming_property
        """
        inverse_arcs = execute_sparql_query(query_inverse, limit=50)
        # FAILSAFE: debug
        if len(inverse_arcs.get("results", []))==0:
            return {
                "success": False,
                "error": f"No incoming arcs found for vocabulary entity: {obj}"
            }
        parents = []
        for arc_val in inverse_arcs.get("results", []):
            arc = arc_val.get("incoming_property")
            arc_label = extract_label(arc)
            for subj, edges in graph.items():
                for pred, _ in edges:
                    if pred == arc_label:
                        parents.append((subj, pred))
        # FAILSAFE: debug
        if not parents:
            return {
                "success": False,
                "error": f"No parent entities found while incoming arcs are {[extract_label(arc_val.get('incoming_property')) for arc_val in inverse_arcs.get('results', [])]} for vocabulary entity: {obj}"
            }
    else:
        # Onthology term provided as label -> use Graph
        res = find_inverse_arcs_internal(obj, graph)
        if not res.get("success"):
            return res
        parents = res.get("parents", [])

    #-------------------------------
    # CALL RECURSIVE PATHFINDER
    #-------------------------------
    possible_paths = []
    properties_paths = {str(extract_label(arc_uri)): [] for _, arc_uri in parents}
    selected_path = None
    logger.info(f"Found parents: {parents} for object {obj}")
    try:
        for parent_entity_uri, arc_uri in parents:
            #TODO: handle the noise introduced by the many skos properties
            if "skos" in arc_uri:
                # Skip skos:broader/narrower relations
                continue
            logger.info(f"Finding paths from parent entity {parent_entity_uri} to subject {subject_uri}...")
            possible_subpaths = recur_domain(parent_entity_uri, subject_uri, graph, 1, [(convert_to_variable_name(parent_entity_uri), parent_entity_uri)])
            # FAILSAFE: debug
            if not possible_subpaths:
                # Do not consider paths with no results
                logger.info(f"No paths found from {convert_to_variable_name(parent_entity_uri)} -> {extract_label(arc_uri)} to subject {obj}.")
                continue
                # return {
                #     "success": False,
                #     "error": f"No paths found from parent entity {parent_entity_uri} to subject {subject}."
                # }
            
            for subpath in possible_subpaths:
                full_path = subpath + [(convert_to_variable_name(extract_label(arc_uri)), extract_label(arc_uri)), (object_label, object_entity_uri)]
                possible_paths.append(full_path)
                properties_paths[str(extract_label(arc_uri))].append(full_path)
    # FAILSAFE: debug
    except ValueError as ve:
        return {
            "success": False,
            "error": str(ve)
        }
    
    # ---------------------------------------------------------
    # DECISION LOGIC FOR SAMPLING
    # ---------------------------------------------------------
    # FAILSAFE: debug
    if not possible_paths:
         return {"success": False, "error": "No paths found."}
         
    elif len(possible_paths) == 1:
        selected_path = possible_paths[0]
        
    else:
        # MULTIPLE PATHS FOUND: Use MCP Sampling to decide

        # Reduce the number of paths by length (keep shortest 5 for each property)
        reduced_paths = []
        for prop, paths in properties_paths.items():
            sorted_paths = sorted(paths, key=len)
            reduced_paths.extend(sorted_paths[:5])
        possible_paths = reduced_paths
        

        path_options_text = format_paths_for_llm(possible_paths)
        
        # Create the prompt for the LLM
        system_prompt = "You are a SPARQL ontology expert. Choose the most semantically relevant path for the user's query."
        pattern_intent = f"""
        associating '{subject}' to {N} '{obj}'/s.
        
        I found multiple ways to link them in the database:
        {path_options_text}
        """

        # Send Sampling request to LLM
        llm_answer = await tool_sampling_request(system_prompt, pattern_intent, ctx)
        try:
            # simple extraction of the number
            match = re.search(r'\d+', llm_answer)
            if match:
                index = int(match.group())
                selected_path = possible_paths[index]
            else:
                # Fallback to shortest if LLM output is weird
                selected_path = sorted(possible_paths, key=len)[0]
        except (IndexError, ValueError):
            selected_path = possible_paths[0]
    # FAILSAFE: debug
    if not selected_path:
        return {
            "success": False,
            "error": "Failed to select a path."
        }
    
    # Impose subject at the beginning of the path
    selected_path[0] = (subject, subject_uri)
    pattern_list = [(f"?{selected_path[i][0]} {selected_path[i+1][1]} ?{selected_path[i+2][0]} .", f"Path step {i//2 + 1}") for i in range(0, len(selected_path)-2, 2)]
    if not pattern_list:
        return {
            "success": False,
            "error": f"No path found from {subject} to {obj} because parents are {parents}."
        }
    if N is not None:
        quantity_property = get_quantity_property(selected_path[-3][1])
        logger.info(f"Quantity property for entity {selected_path[-3][1]} is {quantity_property}")
        if quantity_property:
            pattern_list.append((f"?{selected_path[-3][0]} {quantity_property} {str(N)} .", "Get the number of medium of performances"))
        
    pattern_list.append((f"VALUES (?{object_label}) {{ (<{object_entity_uri}>) }} .", "Save the variable for the object entity"))
    #TODO: check for the workings of defined_vars
    # Extract the variable names
    def_vars = [var for var in selected_path[2::2]]
    qc.add_module({
        "id": f"associate_N_entities_module_{selected_path[-1][0]}",
        "triples": pattern_list,
        "type": "associate_N_entities_pattern",
        "required_vars": [f"?{subject}"],
        "defined_vars": def_vars
    })
    sparql_query = qc.to_string()
    return {
            "success": True,
            "query_id": query_id,
            "generated_sparql": sparql_query,
            "message": "Query pattern added successfully. Review the SPARQL. If correct, use execute_query(query_id) to run it."
        }

#-------------------------------
# EXECUTE QUERY INTERNALS
#-------------------------------

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