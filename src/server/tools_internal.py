import pathlib
import logging
import os
import re 
from nanoid import generate
from fastmcp import Context
from typing import Any, Optional, Dict, List
from difflib import get_close_matches
from server.find_paths import load_graph
from server.graph_schema_explorer import GraphSchemaExplorer
from server.query_container import QueryContainer, create_triple_element, create_select_element
from server.query_builder import query_works, query_performance, query_artist
from server.find_paths import find_k_shortest_paths, find_term_in_graph_internal, find_inverse_arcs_internal
from server.utils import (
    execute_sparql_query,
    contract_uri,
    contract_uri_restrict,
    expand_prefixed_uri,
    get_entity_label,
    find_candidate_entities_utils,
    remove_redundant_paths
)
from server.utils import extract_label, convert_to_variable_name
from server.tool_sampling import format_paths_for_llm, tool_sampling_request

logger = logging.getLogger("doremus-mcp")

SAVE_QUERIES = False

#load graph for find_path
project_root = pathlib.Path(__file__).parent
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
    strategy: str
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
            "message": "Query built successfully. Review the SPARQL. It is strongly suggested to follow this strategy:\n"+strategy+ "\nIf correct, then use execute_query(query_id) to run it."
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
    if not res:
        logger.error("find_inverse_arcs error found")
        return []
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
        
    
async def associate_to_N_entities_internal(subject: str, obj: str, query_id: str, N: int | None) -> List[dict]:
    #-------------------------------
    # CHECK IF QUERY EXISTS
    #-------------------------------
    if not QUERY_STORAGE:
        logger.error("Query storage not initialized")
        return {
            "success": False,
            "error": f"Query storage not initialized"
        }
    qc = QUERY_STORAGE.get(query_id)
    # FAILSAFE: debug
    if not qc:
        return {
            "success": False,
            "error": f"Query ID {query_id} not found or expired."
        }
    
    # RETRIEVE SUBJECT URI
    subject = subject.strip().lower()
    subject_uri = qc.get_variable_uri(subject)
    # FAILSAFE: debug
    if not subject_uri:
        return {
            "success": False,
            "error": f"Subject variable ?{subject} not found in query."
        }
    
    # RETRIEVE OBJECT URI
    if obj.startswith("http://") or obj.startswith("https://"):
        obj_uri = obj
        obj = (obj.split("/")[-1]).strip().lower()
    else:
        obj = obj.strip().lower()
        res_obj = find_candidate_entities_internal(obj, "vocabulary")
        if res_obj['matches_found']==0:
            res_obj = find_candidate_entities_internal(obj, "others")
            if res_obj['matches_found']>0:
                obj_uri_complete = res_obj.get("entities", [])[0]["entity"]
                obj_uri = extract_label(obj_uri_complete)
            else:
                return {
                    "success": False,
                    "error": f"Object entity '{obj}' not found in the knowledge base."
                }
        else:
            obj_uri_complete = res_obj.get("entities", [])[0]["entity"]
            obj_uri = obj_uri_complete
    
    #-------------------------------
    # VOCAB/ONTOLOGY SWITCH
    #-------------------------------
    if obj_uri.startswith("http://"):
        # Find inverse arcs
        query_inverse = f"""
            SELECT ?incoming_property (SAMPLE(?item_pointing_at_me) AS ?single_example)
            WHERE {{
            # 1. FIX THE TARGET
            VALUES ?my_entity {{ <{obj_uri}> }} .

            # 2. Find incoming links
            ?item_pointing_at_me ?incoming_property ?my_entity .

            }} 
            # Group by the "Keys" (The things that should be unique per row)
            GROUP BY ?incoming_property
        """
        inverse_arcs = execute_sparql_query(query_inverse, limit=50)
        if not inverse_arcs:
            logger.error(f"No inverse arc found for {obj_uri}")
            return {
                "success": False,
                "error": f"No inverse arc found for {obj_uri}"
            }
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
        res = find_inverse_arcs_internal(obj_uri, graph)
        if not res:
            logger.error(f"find_inverse_arcs_internal found no matches for {obj_uri}")
            return {
                "success":False,
                "error": f"find_inverse_arcs_internal found no matches for {obj_uri}"
            }
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
            # #TODO: handle the noise introduced by the many skos properties
            # if "skos" in arc_uri:
            #     # Skip skos:broader/narrower relations
            #     continue
            logger.info(f"Finding paths from parent entity {parent_entity_uri} to subject {subject_uri}...")
            possible_subpaths = recur_domain(parent_entity_uri, subject_uri, graph, 1, [(convert_to_variable_name(parent_entity_uri), parent_entity_uri)])
            # FAILSAFE: debug
            if not possible_subpaths:
                # Do not consider paths with no results
                logger.info(f"No paths found from {convert_to_variable_name(parent_entity_uri)} -> {extract_label(arc_uri)} to subject {obj}.")
                continue
            
            for subpath in possible_subpaths:
                full_path = subpath + [(convert_to_variable_name(extract_label(arc_uri)), extract_label(arc_uri)), (obj, obj_uri)]
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
        # MULTIPLE PATHS FOUND: Use MCP Sampling to decide: reduce the number of paths by length (keep shortest 5 for each property)
        reduced_paths = []
        for prop, paths in properties_paths.items():
            pruned_paths = remove_redundant_paths(paths)
            sorted_paths = sorted(pruned_paths, key=len)
            reduced_paths.extend(sorted_paths[:5])
        possible_paths = sorted(reduced_paths, key=len)
        
        path_options_text = format_paths_for_llm(possible_paths)

        current_query = qc.to_string()
        
        # Create the prompt for the LLM
        system_prompt = """You are a SPARQL ontology expert. Choose the most semantically relevant path for the user's query.
Note the shortest path is rarely the best option, compare the semantic relevance of the paths.
DOREMUS is based on the CIDOC-CRM ontology, using the EFRBROO (Work-Expression-Manifestation-Item) extension.
Work -> conceptual idea (idea of a sonata)
Expression -> musical realization (written notation of the sonata, with his title, composer, etc.)
Event -> performance or recording"""
        pattern_intent = f"""which of these paths is the best for associating '{subject}' to {N} '{obj}'/s, 
given that the current question being asked is: '{qc.get_question()}'.

The current query is:
{current_query}
        
The options available are:
{path_options_text}
        """
        # Send Sampling request to LLM
        llm_answer = await tool_sampling_request(system_prompt, pattern_intent)
        try:
            # simple extraction of the number
            match = re.search(r'\d+', llm_answer)
            if match:
                # CASE 1: valid index returned
                index = int(match.group())
                selected_path = possible_paths[index]
            else:
                # CASE 2: Fallback to shortest if LLM output is weird
                selected_path = sorted(possible_paths, key=len)[0]
        except (IndexError, ValueError):
            # CASE 3: Error in sampling process
            return {
                "success": False,
                "error": "Failed sampling selection, an error occurred"
            }
    # FAILSAFE: debug
    if not selected_path:
        return {
            "success": False,
            "error": "Failed to select a path."
        }
    
    # Impose subject at the beginning of the path
    selected_path[0] = (subject, subject_uri)
    triples = []
    for i in range(0, len(selected_path)-2, 2):
        triples.append({
            "subj": create_triple_element(selected_path[i][0], selected_path[i][1], "var"),
            "pred": create_triple_element(selected_path[i+1][0], selected_path[i+1][1], "uri"),
            "obj": create_triple_element(selected_path[i+2][0], selected_path[i+2][1], "var")
        })
    logger.info(f"Selected path for associating {subject} to {obj} is: {triples}")
    if N is not None:
        quantity_property = get_quantity_property(selected_path[-3][1])
        logger.info(f"Quantity property for entity {selected_path[-3][1]} is {quantity_property}")
        if quantity_property:
            triples.append({
                "subj": create_triple_element(selected_path[-3][0], selected_path[-3][1], "var"),
                "pred": create_triple_element(convert_to_variable_name(quantity_property), quantity_property, "uri"),
                "obj": create_triple_element(N, "", "literal")
            })
    def_vars = qc.extract_defined_variables(triples)
    logger.info(f"Defined vars after adding triples: {def_vars}")
    triples.append({
        "subj": create_triple_element(obj, obj_uri, "var"),
        "pred": create_triple_element("VALUES", "VALUES", "uri"),
        "obj": create_triple_element(obj_uri, obj_uri, "uri")
    })
    await qc.add_module({
            "id": f"associate_N_entities_module_{selected_path[-1][0]}",
            "type": "associate_N_entities",
            "scope": "main",
            "triples": triples,
            "required_vars": [create_triple_element(subject, subject_uri, "var")],
            "defined_vars": def_vars[1:] # Exclude subject from defined vars
    })
    sparql_query = qc.to_string()
    return {
            "success": True,
            "query_id": query_id,
            "generated_sparql": sparql_query,
            "message": "Query pattern added successfully. Review the SPARQL. If correct, use execute_query(query_id) to run it."
        }


def _process_date(date_str: str) -> str:
    """
    Helper to process date strings into XSD format.
    Supports: YYYY, DD-MM-YYYY, YYYY-MM-DD
    Returns: "value"^^xsd:type
    """
    date_str = date_str.strip()
    # YYYY
    if re.match(r"^\d{4}$", date_str):
        return f'"{date_str}"^^xsd:gYear'
    
    # DD-MM-YYYY
    match_dmy = re.match(r"^(\d{2})-(\d{2})-(\d{4})$", date_str)
    if match_dmy:
        d, m, y = match_dmy.groups()
        return f'"{y}-{m}-{d}"^^xsd:date'
    
    # YYYY-MM-DD
    match_ymd = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", date_str)
    if match_ymd:
        return f'"{date_str}"^^xsd:date'
    # Error
    return None


async def has_quantity_of_internal(subject: str, property: str, type: str, valueStart: str, valueEnd: str | None, query_id: str) -> Dict[str, Any]:
    qc = QUERY_STORAGE.get(query_id)
    if not qc:
        return {
            "success": False,
            "error": f"Query ID {query_id} not found or expired."
        }
    
    # 1. Check if the property exists for the subject class
    subject_var = subject
    subject_label = qc.get_variable_uri(subject) 
    if not explorer.class_has_property(subject_label, property):
        return {
            "success": False,
            "error": f"Property {property} does not exist for class {subject_label}."
        }
         
    # 2. Resolve Property & Construct Triples
    propery = property.strip()
    triples = []
    filter_st = []
    defined_vars = []
    
    # Value processing (dates vs numbers)
    # Check if property implies date
    is_date = "time-span" in propery
    
    def format_value(val):
        if is_date:
            return _process_date(val)
        else:
            # Check if number
            if val.replace('.', '', 1).isdigit():
                return val
            return f'"{val}"'

    if valueEnd == "":
        valueEnd = None

    val_start_fmt = format_value(valueStart)
    val_end_fmt = format_value(valueEnd) if valueEnd else None

    if not val_start_fmt:
        return {
            "success": False,
            "error": "Invalid start date format."
        }
    if not val_end_fmt and valueEnd is not None:
        return {
            "success": False,
            "error": "Invalid end date format."
        }
    
    # Type processing
    if type not in ["less", "more", "equal", "range"]:
        return {
            "success": False,
            "error": "Invalid filter type."
        }

    if type in ["less", "more", "equal"] and valueEnd is not None:
        return {
            "success": False,
            "error": "Value End is not allowed for this type."
        }
    if type == "range" and valueEnd is None:
        return {
            "success": False,
            "error": "Value End is required for this type."
        }
    
    prop_module_id = f"has_quantity_of_{property}"
        
    if is_date:
        # Time-Span Pattern: 
        # ?subject ecrm:P4_has_time-span ?time_span . 
        # ?time_span time:hasBeginning / time:inXSDDate ?start .
        # ?time_span time:hasEnd / time:inXSDDate ?end .
        
        ts_var = "time_span"
        triples.append({
            "subj": create_triple_element(subject_var, subject_label if subject_label else "", "var"),
            "pred": create_triple_element("ecrm:P4_has_time-span", "ecrm:P4_has_time-span", "uri"),
            "obj": create_triple_element(ts_var, "ecrm:E52_Time-Span", "var")
        })
        defined_vars.append({"var_name": ts_var, "var_label": "ecrm:E52_Time-Span"})
        
        # We add triples for start/end based on filter type to be efficient?
        # Or always add them? 
        # If we only need start (e.g. > 1900), only add start.
        
        if type in ["more", "range", "equal"] or (type == "less" and not valueEnd): # Logic check
             # For 'more than' we usually check start date. 
             # For 'range' we check start and end.
             # Let's define:
             # start var ?start
             start_var = "start"
             triples.append({
                 "subj": create_triple_element(ts_var, "ecrm:E52_Time-Span", "var"),
                 "pred": create_triple_element("time:hasBeginning / time:inXSDDate", "time:hasBeginning / time:inXSDDate", "uri"),
                 "obj": create_triple_element(start_var, "", "var")
             })
             defined_vars.append({"var_name": start_var, "var_label": ""})
        
        if type in ["less", "range", "equal"]:
             end_var = "end"
             triples.append({
                 "subj": create_triple_element(ts_var, "ecrm:E52_Time-Span", "var"),
                 "pred": create_triple_element("time:hasEnd / time:inXSDDate", "time:hasEnd / time:inXSDDate", "uri"),
                 "obj": create_triple_element(end_var, "", "var")
             })
             defined_vars.append({"var_name": end_var, "var_label": ""})
        target_var = None
        
    else:
        # Generic Property
        # ?subject property ?quantity_val
        qty_var = "quantity_val"
        triples.append({
            "subj": create_triple_element(subject_var, subject_label if subject_label else "", "var"),
            "pred": create_triple_element(propery, propery, "uri"),
            "obj": create_triple_element(qty_var, "", "var")
        })
        defined_vars.append({"var_name": qty_var, "var_label": ""}) 
        target_var = f"?{qty_var}"

    # 3. Construct Filters
    # Filter ops: <=, >=, =, and logical combinations
    
    if type == "less":
        # <= valueStart
        # If time-span: usually "end time is before X" ?
        # Or "duration less than X".
        if is_date:
             # "less than 1900" -> Ends before 1900? or Starts before? 
             # Usually "written before 1900" -> End date < 1900.
             filter_st.append({'function': '', 'args': [f'?end <= {val_start_fmt}']})
        else:
             filter_st.append({'function': '', 'args': [f'{target_var} <= {val_start_fmt}']})
             
    elif type == "more":
        # >= valueStart
        # "more than 1900" -> Starts after 1900
        if is_date:
            filter_st.append({'function': '', 'args': [f'?start >= {val_start_fmt}']})
        else:
            filter_st.append({'function': '', 'args': [f'{target_var} >= {val_start_fmt}']})
            
    elif type == "equal":
        # = valueStart
        if is_date:
            # Start = val OR End = val? Or contains?
            # Let's assume start = val for simplicity or create a generic overlap?
            # For "written in 1900", it means start >= 1900-01-01 AND end <= 1900-12-31?
            # But here user says "equal". 
            filter_st.append({'function': '', 'args': [f'?start = {val_start_fmt}']})
        else:
            filter_st.append({'function': '', 'args': [f'{target_var} = {val_start_fmt}']})
            
    elif type == "range":
        # >= valueStart AND <= valueEnd
        if is_date:
            # Between 1870 and 1913
            # Means Start >= 1870 AND End <= 1913 (Inclusive containment)
            # OR Overlaps? The user example:
            # ?start >= "1870"^^xsd:gYear AND ?end <= "1913"^^xsd:gYear
            filter_st.append({'function': '', 'args': [f'?start >= {val_start_fmt} AND ?end <= {val_end_fmt}']})
        else:
            filter_st.append({'function': '', 'args': [f'{target_var} >= {val_start_fmt} AND {target_var} <= {val_end_fmt}']})

    # 4. Add Module
    module = {
        "id": prop_module_id,
        "type": "has_quantity_of",
        "scope": "main",
        "triples": triples,
        "filter_st": filter_st,
        "required_vars": [{"var_name": subject_var, "var_label": subject_label if subject_label else ""}],
        "defined_vars": defined_vars
    }

    await qc.add_module(module)
    
    return {
        "success": True,
        "query_id": query_id,
        "generated_sparql": qc.to_string(),
        "message": "Quantity filter added."
    }

    
    
    

#-------------------------------
# GROUP BY HAVING INTERNALS
#-------------------------------

# async def _find_path_helper(subj, subj_uri, obj_name, obj_uri, qc):
#     """
#     Internal wrapper re-implementing the core logic of associate_to_N_entities
#     to find a path between subject and object.
#     """
#     # 1. Find Inverse Arcs of Object
#     if obj_uri.startswith("http://"):
#         # Find inverse arcs
#         query_inverse = f"""
#             SELECT ?incoming_property (SAMPLE(?item_pointing_at_me) AS ?single_example)
#             WHERE {{
#             # 1. FIX THE TARGET
#             VALUES ?my_entity {{ <{obj_uri}> }} .

#             # 2. Find incoming links
#             ?item_pointing_at_me ?incoming_property ?my_entity .

#             }} 
#             # Group by the "Keys" (The things that should be unique per row)
#             GROUP BY ?incoming_property
#         """
#         inverse_arcs = execute_sparql_query(query_inverse, limit=50)
#         # FAILSAFE: debug
#         if len(inverse_arcs.get("results", []))==0:
#             logger.error(f"No incoming arcs found for vocabulary entity: {obj_name}")
#             return []
#         parents = []
#         for arc_val in inverse_arcs.get("results", []):
#             arc = arc_val.get("incoming_property")
#             arc_label = extract_label(arc)
#             for subj, edges in graph.items():
#                 for pred, _ in edges:
#                     if pred == arc_label:
#                         parents.append((subj, pred))
#         # FAILSAFE: debug
#         if not parents:
#             logger.error(f"No parent entities found while incoming arcs are {[extract_label(arc_val.get('incoming_property')) for arc_val in inverse_arcs.get('results', [])]} for vocabulary entity: {obj_name}")
#             return []
#     else:
#         # Onthology term provided as label -> use Graph
#         res = find_inverse_arcs_internal(obj_uri, graph)
#         if not res.get("success"):
#             logger.error("No entity found in the inverse arcs")
#             return []
#         parents = res.get("parents", [])

#     #-------------------------------
#     # CALL RECURSIVE PATHFINDER
#     #-------------------------------
#     possible_paths = []
#     properties_paths = {str(extract_label(arc_uri)): [] for _, arc_uri in parents}
#     selected_path = None
#     logger.info(f"Found parents: {parents} for object {obj_name}")
#     try:
#         for parent_entity_uri, arc_uri in parents:
#             logger.info(f"Finding paths from parent entity {parent_entity_uri} to subject {subj_uri}...")
#             possible_subpaths = recur_domain(parent_entity_uri, subj_uri, graph, 1, [(convert_to_variable_name(parent_entity_uri), parent_entity_uri)])
#             # FAILSAFE: debug
#             if not possible_subpaths:
#                 # Do not consider paths with no results
#                 logger.info(f"No paths found from {convert_to_variable_name(parent_entity_uri)} -> {extract_label(arc_uri)} to subject {obj_name}.")
#                 continue
            
#             for subpath in possible_subpaths:
#                 full_path = subpath + [(convert_to_variable_name(extract_label(arc_uri)), extract_label(arc_uri)), (obj_name, obj_uri)]
#                 possible_paths.append(full_path)
#                 properties_paths[str(extract_label(arc_uri))].append(full_path)
#     # FAILSAFE: debug
#     except ValueError as ve:
#         logger.error(str(ve))
#         return []
    
#     # ---------------------------------------------------------
#     # DECISION LOGIC FOR SAMPLING
#     # ---------------------------------------------------------
#     # FAILSAFE: debug
#     if not possible_paths:
#         logger.error("No paths found.")
#         return []
         
#     elif len(possible_paths) == 1:
#         selected_path = possible_paths[0]
        
#     else:
#         # MULTIPLE PATHS FOUND: Use MCP Sampling to decide: reduce the number of paths by length (keep shortest 5 for each property)
#         reduced_paths = []
#         for prop, paths in properties_paths.items():
#             pruned_paths = remove_redundant_paths(paths)
#             sorted_paths = sorted(pruned_paths, key=len)
#             reduced_paths.extend(sorted_paths[:5])
#         possible_paths = sorted(reduced_paths, key=len)[:5]
        
#         path_options_text = format_paths_for_llm(possible_paths)
        
#         # Create the prompt for the LLM
#         system_prompt = "You are a SPARQL ontology expert. Choose the most semantically relevant path for the user's query."
#         pattern_intent = f"""which of these paths is the best for associating '{subj}' to '{obj_name}'/s, 
# given that the current question being asked is: '{qc.get_question()}'.
        
# The options available are:
# {path_options_text}
#         """
#         # Send Sampling request to LLM
#         llm_answer = await tool_sampling_request(system_prompt, pattern_intent)
#         try:
#             # simple extraction of the number
#             match = re.search(r'\d+', llm_answer)
#             if match:
#                 # CASE 1: valid index returned
#                 index = int(match.group())
#                 selected_path = possible_paths[index]
#             else:
#                 # CASE 2: Fallback to shortest if LLM output is weird
#                 selected_path = sorted(possible_paths, key=len)[0]
#         except (IndexError, ValueError):
#             # CASE 3: Error in sampling process
#             logger.error(f"An error occurred in finding the recursive path from {subj} to {obj_name}")
#             return []
    
#     # Return format: List of tuples [(var, uri), (pred_var, pred_uri), (var, uri)...]
#     return selected_path

async def groupBy_having_internal(
    subject: str, 
    query_id: str, 
    function: Optional[str] = None, 
    obj: Optional[str] = None, 
    logic_type: Optional[str] = None, 
    valueStart: Optional[str] = None, 
    valueEnd: Optional[str] = None
) -> Dict[str, Any]:
    """
    Applies a GROUP BY statement and optionally a HAVING clause.
    It automatically finds the path between the subject and the object to be counted.
    """
    
    # SETUP & VALIDATION
    qc = QUERY_STORAGE.get(query_id)
    if not qc:
        return {"success": False, "error": f"Query ID {query_id} not found."}

    # Validate Subject (Must exist in query)
    subject = subject.strip()
    if ":" in subject:
        # The LLM passed a label
        subject_uri = subject
        tmp_subj = qc.get_varName_from_uri(subject_uri)
        if not tmp_subj:
            return {"success": False, "error": f"No subject was found with URI {subject_uri}"}
        subject = tmp_subj
    else:
        subject_uri = qc.get_variable_uri(subject)
        if not subject_uri:
            return {"success": False, "error": f"Subject variable ?{subject} not found in query."}

    if obj:
        # Validate Object
        if ":" in obj:
            #the LLM passed a label or URI
            obj_uri = obj
            tmp_obj = qc.get_varName_from_uri(obj_uri)
            if not tmp_obj:
                return {"success": False, "error": f"No object was found with URI {obj_uri}"}
            obj = tmp_obj
        

        triple = qc.get_triple_object(subject, obj)
        if not triple:
            return {"success": False, "error": f"Unable to find a triple between {subject} and {obj}"}
        
        aggr_obj_name = "all"+triple["obj"]["var_name"]
        new_triple = {
            "subj": triple["subj"],
            "pred": triple["pred"],
            "obj": {
                "var_name": aggr_obj_name,
                "uri": triple["obj"]["var_label"],
                "type": "var"
            }
        }
        logger.info(f"New triple for aggregation: {new_triple}")
        triples = [new_triple]
                
        # Add module to QC
        await qc.add_module({
            "id": f"group_by_path_{subject}_{obj}",
            "type": "pattern",
            "scope": "main",
            "triples": triples,
            "required_vars": [create_triple_element(subject, subject_uri, "var")],
            "defined_vars": []
        })

    # CONSTRUCT GROUP BY: Group by the subject + any other non-aggregated variable in SELECT
    # We first update the selct to include the subject of the group By (which might not be
    # in it) and then extract the variables in the select that are not sampled
    
    qc.add_select(create_select_element(subject, subject_uri))
    group_vars = qc.get_non_aggregated_vars()

    qc.set_group_by(group_vars)

    # CONSTRUCT HAVING
    if function and aggr_obj_name:
        
        # Determine Operator
        operator = "=" # Default
        if logic_type == "more": operator = ">"
        elif logic_type == "less": operator = "<"
        elif logic_type == "equal": operator = "="
        elif logic_type == "range": operator = "range"

        having_clause = {
                "function":function.upper(),
                "variable":aggr_obj_name,
                "operator":operator,
            }
        
        # Build Clause
        if operator == "range":
            if valueStart and valueEnd:
                having_clause["valueStart"] = valueStart
                having_clause["valueEnd"] = valueEnd
        else:
            if valueStart:
                having_clause["valueStart"] = valueStart
        
        if having_clause:
            qc.add_having(having_clause)

    return {
        "success": True, 
        "query_id": query_id, 
        "generated_sparql": qc.to_string(),
        "message": "Group By and Having clauses applied successfully."
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
    
    if SAVE_QUERIES:
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