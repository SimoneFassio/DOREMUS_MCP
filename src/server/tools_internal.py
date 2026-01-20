import pathlib
import logging
import os
import re 
from nanoid import generate
from fastmcp import Context
from fastmcp.exceptions import ToolError
from typing import Any, Optional, Dict, List, Callable
from difflib import get_close_matches
from server.find_paths import load_graph, find_k_shortest_paths, find_term_in_graph_internal, find_inverse_arcs_internal, recur_domain
from server.graph_schema_explorer import GraphSchemaExplorer
from server.query_container import QueryContainer, create_triple_element
# query_builder imports removed as old build_query is removed
from server.utils import (
    execute_sparql_query,
    contract_uri,
    contract_uri_restrict,
    expand_prefixed_uri,
    get_entity_label,
    find_candidate_entities_utils,
    remove_redundant_paths,
    resolve_entity_uri,
    extract_label,
    convert_to_variable_name,
    get_quantity_property
)
from server.tool_sampling import format_paths_for_llm, tool_sampling_request
from server.template_parser import get_cached_template, TemplateParseError, TemplateValidationError, convert_triples_to_module, parse_triple_string

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
    try:
        return find_candidate_entities_utils(name, entity_type)
    except Exception as e:
        raise ToolError(f"Error finding candidate entities: {e}")



    
    
def get_entity_properties_internal(
    entity_uri: str
) -> Dict[str, Any]:
    try:
        if entity_uri.startswith("http://") or entity_uri.startswith("https://"):
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
                raise ToolError(f"Error getting entity properties: {result['error']}")
            
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
        else:
            path = '/' + entity_uri
            answer = explorer.explore_graph_schema(path=path)
            return {
                "properties": answer
            }
    except Exception as e:
        raise ToolError(f"Error getting entity properties: {e}")

#-------------------------------
# QUERY BUILDER INTERNALS
#-------------------------------



#-------------------------------
# NEW QUERY BUILDER V2 INTERNALS
#-------------------------------




async def build_query_v2_internal(
    question: str,
    template: str
) -> Dict[str, Any]:
    """
    Build a query using the new template-based approach.
    
    Args:
        question: The user's natural language question
        template: Template name (expression, performance, artist, etc.)
    
    Returns:
        Dict with query_id, generated_query, available_filters
    """
    try:
        # Load template
        template = template.lower().strip()
        template_def = get_cached_template(template)
        
        # Generate ID
        query_id = generate(size=10)
        
        # Create QueryContainer
        qc = QueryContainer(query_id, question)
        
        # Define logging callback
        def log_sampling(log_data: Dict[str, Any]):
            qc.sampling_logs.append(log_data)
        
        # Convert core triples to module
        core_module = convert_triples_to_module(
            template_def.core_triples,
            f"{template}_core",
            template_def.base_variable,
            template_def.base_variable,  # No renaming for core
            template_def.var_classes
        )
        
        await qc.add_module(core_module)
        
        # Add base variable to SELECT
        qc.add_select(template_def.base_variable, template_def.base_class)
        
        # Store query
        QUERY_STORAGE[query_id] = qc
        
        # Build list of available filters
        available_filters = []
        for filter_name, filter_def in template_def.filters.items():
            available_filters.append({
                "name": filter_name,
                "entity_type": filter_def.entity_type,
                "supports_uri": filter_def.values_var is not None,
                "supports_regex": filter_def.regex_var is not None
            })
        
        # Generate strategy guide based on template
        strategy_guide = f"""
### Query Strategy for {template} template

**STEP 2 - Apply Filters:**
Use `apply_filter(query_id="{query_id}", base_variable="{template_def.base_variable}", template="{template}", filters={{...}})`

Available filters: {[f['name'] for f in available_filters]}

**After basic filters:**
- For INSTRUMENT constraints: Use `associate_to_N_entities(subject, instrument, query_id)`
- For TIME/DATE constraints: Use `has_quantity_of(entity, time-span, ...)`
- For AGGREGATIONS: Use `groupBy_having(query_id, group_var, aggregate, ...)`

**Example for "{template}":**
apply_filter(query_id="{query_id}", base_variable="{template_def.base_variable}", template="{template}", 
             filters={{"{available_filters[0]['name'] if available_filters else 'filter_name'}": "value"}})
"""
        
        return {
            "success": True,
            "query_id": query_id,
            "generated_query": qc.to_string(),
            "template": template,
            "base_variable": template_def.base_variable,
            "base_class": template_def.base_class,
            "available_filters": available_filters,
            "strategy_guide": strategy_guide,
            "message": f"Query built. Use apply_filter() to add constraints. Follow the strategy guide below."
        }
        
    except TemplateParseError as e:
        raise ToolError(f"Template error: {e}")
    except Exception as e:
        raise ToolError(f"Error building query: {e}")


async def filter_internal(
    query_id: str,
    base_variable: str,
    template: str,
    filters: Dict[str, str]
) -> Dict[str, Any]:
    """
    Apply filters to an existing query.
    
    Args:
        query_id: ID of the query to modify
        base_variable: Variable in the query to filter on (e.g., "work", "expression")
        template: Template name to use for filter definitions
        filters: Dict of filter_name -> filter_value
    
    Returns:
        Dict with success status and updated query
    """
    try:
        # Get the query container
        if query_id not in QUERY_STORAGE:
            raise ToolError(f"Query ID not found: {query_id}")
        
        qc = QUERY_STORAGE[query_id]
        
        # Load template
        template = template.lower().strip()
        template_def = get_cached_template(template)
        
        # Define logging callback
        def log_sampling(log_data: Dict[str, Any]):
            qc.sampling_logs.append(log_data)
        
        # Check that base_variable exists in query
        if base_variable not in qc.variable_registry:
            raise ToolError(
                f"Variable '{base_variable}' not found in query. "
                f"Available variables: {list(qc.variable_registry.keys())}"
            )
        
        # Apply each filter
        for filter_name, filter_value in filters.items():
            # Check filter exists in template
            if filter_name not in template_def.filters:
                raise ToolError(
                    f"Filter '{filter_name}' not found in template '{template}'. "
                    f"Available filters: {list(template_def.filters.keys())}"
                )
            
            filter_def = template_def.filters[filter_name]
            
            # Determine if filter_value is a URI
            is_uri = filter_value.startswith("http://") or filter_value.startswith("https://")
            
            # Validate argument type
            if is_uri and not filter_def.values_var:
                raise ToolError(
                    f"Filter '{filter_name}' does not support URI arguments (values_var is empty). "
                    f"Pass a string label instead."
                )
            
            if not is_uri and not filter_def.regex_var:
                raise ToolError(
                    f"Filter '{filter_name}' does not support string arguments (regex_var is empty). "
                    f"Pass a URI instead."
                )
            
            # Resolve entity if not literal type
            resolved_uri = None
            if filter_def.entity_type != "literal" and not is_uri:
                resolved_uri = await resolve_entity_uri(
                    filter_value, 
                    filter_def.entity_type, 
                    qc.question, 
                    log_sampling
                )
            elif is_uri:
                resolved_uri = filter_value
            
            # Create module from filter triples
            module_id = f"{base_variable}_{filter_name}"
            filter_module = convert_triples_to_module(
                filter_def.triples,
                module_id,
                template_def.base_variable,
                base_variable,
                template_def.var_classes
            )
            
            # Add filter expression
            if resolved_uri and filter_def.values_var:
                # Use VALUES clause
                values_var = filter_def.values_var.lstrip('?')
                if base_variable != template_def.base_variable:
                    values_var = f"{values_var}_{base_variable}"
                
                filter_module["triples"].append({
                    "subj": create_triple_element(values_var, "", "var"),
                    "pred": create_triple_element("VALUES", "VALUES", "uri"),
                    "obj": create_triple_element(resolved_uri, resolved_uri, "uri")
                })
            else:
                # Use FILTER REGEX
                regex_var = filter_def.regex_var.lstrip('?')
                if base_variable != template_def.base_variable:
                    regex_var = f"{regex_var}_{base_variable}"
                
                filter_module["filter_st"] = [{
                    "function": "REGEX",
                    "args": [f"?{regex_var}", f"'{filter_value}'", "'i'"]
                }]
            
            # Deduplicate triplets - ONLY skip exact triples from modules with SAME base_variable
            # Collisions with other modules are handled by add_module via sampling
            
            def to_hashable(val):
                """Convert any value to a hashable string representation."""
                if isinstance(val, list):
                    return str(val)
                if isinstance(val, dict):
                    return str(val)
                return val if val is not None else ""
            
            existing_triples_same_base = set()
            for module in qc.where:
                module_id = module.get("id", "")
                # Check if module belongs to the same base_variable
                # Module IDs follow pattern: {base_variable}_{filter_name} or {template}_core
                if module_id.startswith(f"{base_variable}_") or module_id == f"{base_variable}_core":
                    for t in module.get("triples", []):
                        # Create a hashable key for each triple
                        subj = t.get("subj", {})
                        pred = t.get("pred", {})
                        obj = t.get("obj", {})
                        triple_key = (
                            to_hashable(subj.get("var_name", "")), to_hashable(subj.get("var_label", "")), to_hashable(subj.get("type", "")),
                            to_hashable(pred.get("var_name", "")), to_hashable(pred.get("var_label", "")), to_hashable(pred.get("type", "")),
                            to_hashable(obj.get("var_name", "")), to_hashable(obj.get("var_label", "")), to_hashable(obj.get("type", ""))
                        )
                        existing_triples_same_base.add(triple_key)
            
            # Filter out duplicate triples from same base_variable modules only
            deduplicated_triples = []
            for t in filter_module.get("triples", []):
                subj = t.get("subj", {})
                pred = t.get("pred", {})
                obj = t.get("obj", {})
                triple_key = (
                    to_hashable(subj.get("var_name", "")), to_hashable(subj.get("var_label", "")), to_hashable(subj.get("type", "")),
                    to_hashable(pred.get("var_name", "")), to_hashable(pred.get("var_label", "")), to_hashable(pred.get("type", "")),
                    to_hashable(obj.get("var_name", "")), to_hashable(obj.get("var_label", "")), to_hashable(obj.get("type", ""))
                )
                if triple_key not in existing_triples_same_base:
                    deduplicated_triples.append(t)
                else:
                    logger.debug(f"Skipping duplicate triple (same base_variable): {triple_key}")
            
            filter_module["triples"] = deduplicated_triples
            
            # Only add module if it has triples or filters
            if deduplicated_triples or filter_module.get("filter_st"):
                await qc.add_module(filter_module)
        
        return {
            "success": True,
            "query_id": query_id,
            "generated_query": qc.to_string(),
            "filters_applied": list(filters.keys()),
            "message": "Filters applied successfully. Review the SPARQL and execute when ready."
        }
        
    except ToolError:
        raise
    except Exception as e:
        raise ToolError(f"Error applying filter: {e}")

#-------------------------------
# ASSOCIATE N ENTITIES INTERNALS
#-------------------------------

async def associate_to_N_entities_internal(subject: str, obj: str, query_id: str, n: int | None) -> List[dict]:
    try:
        #-------------------------------
        # CHECK IF QUERY EXISTS
        #-------------------------------
        if not QUERY_STORAGE:
            logger.error("Query storage not initialized")
            raise Exception("Query storage not initialized")
        qc = QUERY_STORAGE.get(query_id)
        # FAILSAFE: debug
        if not qc:
            raise Exception(f"Query ID {query_id} not found or expired.")
        
        # Send Sampling request to LLM
        def log_sampling(log_data: Dict[str, Any]):
            qc.sampling_logs.append(log_data)

        # RETRIEVE SUBJECT URI
        subject = subject.strip().lower()
        subject_uri = qc.get_variable_uri(subject)
        # FAILSAFE: debug
        if not subject_uri:
            raise Exception(f"Subject variable ?{subject} not found in query.")
        
        # TYPE GUARD: obj must be a string
        if not isinstance(obj, str):
            raise Exception(f"Invalid argument type: 'obj' must be a string (the entity name, e.g., 'violin'). Received: {type(obj).__name__} ({obj}). If you meant to specify a quantity, use the 'n' parameter.")

        # RETRIEVE OBJECT URI
        if obj.startswith("http://") or obj.startswith("https://"):
            obj_uri = obj
            obj = (obj.split("/")[-1]).strip().lower()
        else:
            obj = obj.strip().lower()
            obj_uri = await resolve_entity_uri(obj, "vocabulary", log_callback=log_sampling)
            if obj_uri is None:
                obj_uri = await resolve_entity_uri(obj, "others", log_callback=log_sampling)
                if obj_uri is None:
                    raise Exception(f"Object entity '{obj}' not found in the knowledge base.")
        
        #-------------------------------
        # VOCAB/ONTOLOGY SWITCH
        #-------------------------------
        if obj_uri.startswith("http://") or obj_uri.startswith("https://"):
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
                raise Exception(f"No inverse arc found for {obj_uri}")
            # FAILSAFE: debug
            if len(inverse_arcs.get("results", []))==0:
                raise Exception(f"No incoming arcs found for vocabulary entity: {obj}")
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
                raise Exception(f"No parent entities found while incoming arcs are {[extract_label(arc_val.get('incoming_property')) for arc_val in inverse_arcs.get('results', [])]} for vocabulary entity: {obj}")
        else:
            # Ontology term provided as label -> use Graph
            res = find_inverse_arcs_internal(obj_uri, graph)
            if not res:
                logger.error(f"find_inverse_arcs_internal found no matches for {obj_uri}")
                raise Exception(f"find_inverse_arcs_internal found no matches for {obj_uri}")
            if not res.get("success"):
                raise Exception(res.get("error"))
            parents = res.get("parents", [])

        #-------------------------------
        # CALL RECURSIVE PATHFINDER
        #-------------------------------
        possible_paths = []
        properties_paths = {str(extract_label(arc_uri)): [] for _, arc_uri in parents}
        selected_path = None
        logger.info(f"Found parents: {parents} for object {obj}")
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
        
        # ---------------------------------------------------------
        # DECISION LOGIC FOR SAMPLING
        # ---------------------------------------------------------
        # FAILSAFE: debug
        if not possible_paths:
             raise Exception("No paths found.")
             
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

            # Exclude the paths that lead to zero results dry run
            for k, path in enumerate(possible_paths):
                triples = []
                for i in range(0, len(path)-2, 2):
                    triples.append({
                        "subj": create_triple_element(path[i][0], path[i][1], "var"),
                        "pred": create_triple_element(path[i+1][0], path[i+1][1], "uri"),
                        "obj": create_triple_element(path[i+2][0], path[i+2][1], "var")
                    })
                def_vars = qc.extract_defined_variables(triples)
                # Add VALUES for object
                triples.append({
                    "subj": create_triple_element(obj, obj_uri, "var"),
                    "pred": create_triple_element("VALUES", "VALUES", "uri"),
                    "obj": create_triple_element(obj_uri, obj_uri, "uri")
                })
                module = {
                    "id": f"dry_run_path_module_{k}",
                    "type": "pattern",
                    "scope": "main",
                    "triples": triples,
                    "required_vars": [create_triple_element(subject, subject_uri, "var")],
                    "defined_vars": def_vars[1:]
                }
                try:
                    await qc.test_add_module(module)
                    # If dry run passes, keep the path but remove the module afterwards
                except Exception as e:
                    logger.info(f"Excluding path {path} due to dry run failure: {e}")
                    possible_paths.remove(path)
            
            path_options_text = format_paths_for_llm(possible_paths)

            current_query = qc.to_string()
            
            # Create the prompt for the LLM
            system_prompt = """You are a SPARQL ontology expert. Choose the most semantically relevant path for the user's query.
    Note the shortest path is rarely the best option, compare the semantic relevance of the paths.
    DOREMUS is based on the CIDOC-CRM ontology, using the EFRBROO (Work-Expression-Manifestation-Item) extension.
    Work -> conceptual idea (idea of a sonata)
    Expression -> musical realization (written notation of the sonata, with his title, composer, etc.)
    Event -> performance or recording"""
            pattern_intent = f"""which of these paths is the best for associating '{subject}' to {n} '{obj}'/s, 
    given that the current question being asked is: '{qc.get_question()}'.

    The current query is:
    {current_query}
            
    The options available are:
    {path_options_text}
            """
            
            llm_answer = await tool_sampling_request(system_prompt, pattern_intent, log_callback=log_sampling, caller_tool_name="associate_to_N_entities")
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
                raise Exception("Failed sampling selection, an error occurred")
        # FAILSAFE: debug
        if not selected_path:
            raise Exception("Failed to select a path.")
        
        # Impose subject at the beginning of the path
        selected_path[0] = (subject, subject_uri)
        
        # Check for name collision: if target variable name (obj) is used in intermediate nodes
        target_var_name = obj
        intermediate_vars = {selected_path[i][0] for i in range(0, len(selected_path)-1, 2)}
        if target_var_name in intermediate_vars:
            target_var_name = f"{obj}_target"
            # Update path
            selected_path[-1] = (target_var_name, obj_uri)

        triples = []
        for i in range(0, len(selected_path)-2, 2):
            triples.append({
                "subj": create_triple_element(selected_path[i][0], selected_path[i][1], "var"),
                "pred": create_triple_element(selected_path[i+1][0], selected_path[i+1][1], "uri"),
                "obj": create_triple_element(selected_path[i+2][0], selected_path[i+2][1], "var")
            })
        logger.info(f"Selected path for associating {subject} to {obj} is: {triples}")

        logger.info(f"associate_to_N_entities called with raw n={n!r}")
        
        if n is not None:
            quantity_property = get_quantity_property(selected_path[-3][1], graph)
            logger.info(f"Quantity property for entity {selected_path[-3][1]} is {quantity_property}")
            if quantity_property:
                triples.append({
                    "subj": create_triple_element(selected_path[-3][0], selected_path[-3][1], "var"),
                    "pred": create_triple_element(convert_to_variable_name(quantity_property), quantity_property, "uri"),
                    "obj": create_triple_element(n, "", "literal")
                })
        def_vars = qc.extract_defined_variables(triples)
        logger.info(f"Defined vars after adding triples: {def_vars}")
        triples.append({
            "subj": create_triple_element(target_var_name, obj_uri, "var"),
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
                "generated_query": sparql_query,
                "message": "Query pattern added successfully. Review the SPARQL. If correct, use execute_query(query_id) to run it."
            }
            
    except Exception as e:
        raise ToolError(f"Error associating N entities: {e}")


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
    try:
        qc = QUERY_STORAGE.get(query_id)
        if not qc:
            raise Exception(f"Query ID {query_id} not found or expired.")
        
        # 1. Check if the property exists for the subject class
        subject_var = subject
        subject_label = qc.get_variable_uri(subject) 
        if not explorer.class_has_property(subject_label, property):
            raise Exception(f"Property {property} does not exist for class {subject_label}.")
             
        # 2. Resolve Property & Construct Triples
        propery = property.strip()
        triples = []
        filter_st = []
        defined_vars = []
        
        # Value processing (dates vs numbers)
        # Check if property implies date
        is_date = "time-span" in propery
        is_duration = "duration" in propery
        
        def format_value(val):
            if is_date:
                return _process_date(val)
            elif is_duration:
                # ISO 8601 Duration check (Regex)
                # Matches PnYnMnDTnHnMnS format, allowing optional parts but ensuring structure.
                iso8601_pattern = r"^P(?!$)(?:\d+Y)?(?:\d+M)?(?:\d+D)?(?:T(?!$)(?:\d+H)?(?:\d+M)?(?:\d+(?:\.\d+)?S)?)?$"
                if not re.match(iso8601_pattern, val):
                    raise Exception("Duration value must be in valid ISO 8601 format (e.g. 'PT1H', 'P10M').")
                return f'"{val}"^^xsd:duration'
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
            raise Exception("Invalid start date format.")
        if not val_end_fmt and valueEnd is not None:
            raise Exception("Invalid end date format.")
        
        # Type processing
        if type not in ["less", "more", "equal", "range"]:
            raise Exception("Invalid filter type.")

        if type in ["less", "more", "equal"] and valueEnd is not None:
            raise Exception("Value End is not allowed for this type.")
        if type == "range" and valueEnd is None:
            raise Exception("Value End is required for this type.")
        
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
        
        # Helpler for duration comparison syntax
        def fmt_cmp(var, op, val):
            if is_duration:
                return f"xsd:dayTimeDuration(str({var})) {op} {val}"
            else:
                return f"{var} {op} {val}"

        if type == "less":
            # <= valueStart
            # If time-span: usually "end time is before X" ?
            # Or "duration less than X".
            if is_date:
                 # "less than 1900" -> Ends before 1900? or Starts before? 
                 # Usually "written before 1900" -> End date < 1900.
                 filter_st.append({'function': '', 'args': [f'?end <= {val_start_fmt}']})
            else:
                 filter_st.append({'function': '', 'args': [fmt_cmp(target_var, '<=', val_start_fmt)]})
                 
        elif type == "more":
            # >= valueStart
            # "more than 1900" -> Starts after 1900
            if is_date:
                filter_st.append({'function': '', 'args': [f'?start >= {val_start_fmt}']})
            else:
                filter_st.append({'function': '', 'args': [fmt_cmp(target_var, '>=', val_start_fmt)]})
                
        elif type == "equal":
            # = valueStart
            if is_date:
                # Start = val OR End = val? Or contains?
                # Let's assume start = val for simplicity or create a generic overlap?
                # For "written in 1900", it means start >= 1900-01-01 AND end <= 1900-12-31?
                # But here user says "equal". 
                filter_st.append({'function': '', 'args': [f'?start = {val_start_fmt}']})
            else:
                filter_st.append({'function': '', 'args': [fmt_cmp(target_var, '=', val_start_fmt)]})
                
        elif type == "range":
            # >= valueStart AND <= valueEnd
            if is_date:
                # Between 1870 and 1913
                # Means Start >= 1870 AND End <= 1913 (Inclusive containment)
                # OR Overlaps? The user example:
                # ?start >= "1870"^^xsd:gYear AND ?end <= "1913"^^xsd:gYear
                filter_st.append({'function': '', 'args': [f'?start >= {val_start_fmt} AND ?end <= {val_end_fmt}']})
            else:
                filter_st.append({'function': '', 'args': [f'{fmt_cmp(target_var, ">=", val_start_fmt)} AND {fmt_cmp(target_var, "<=", val_end_fmt)}']})

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
            "query_id": query_id,
            "generated_query": qc.to_string(),
            "message": "Quantity filter added."
        }
    except Exception as e:
        raise ToolError(f"Error checking quantity filter: {e}")

    

#-------------------------------
# ADD TRIPLET INTERNALS
#-------------------------------

async def add_triplet_internal(
    subject: str, 
    subject_class: str, 
    property: str, 
    obj: str, 
    obj_class: str, 
    query_id: str
) -> Dict[str, Any]:
    try:
        qc = QUERY_STORAGE.get(query_id)
        if not qc:
            raise Exception(f"Query ID {query_id} not found or expired.")
            
        # 1. Check property existence
        if not explorer.class_has_property(subject_class, property):
             raise Exception(f"Property {property} does not exist for class {subject_class}.")
            
        # 2. Prepare Triples
        triples = [{
            "subj": create_triple_element(subject, subject_class, "var"),
            "pred": create_triple_element(property, property, "uri"),
            "obj": create_triple_element(obj, obj_class, "var")
        }]
        
        module_id = f"add_triplet_{subject}_{property}_{obj}"
        
        module = {
            "id": module_id,
            "type": "add_triplet",
            "scope": "main",
            "triples": triples
        }

        # 3. Add Module with Dry Run
        await qc.add_module(module, dry_run=True)
        
        return {
            "query_id": query_id,
            "generated_query": qc.to_string(),
            "message": "Triplet added successfully."
        }

    except Exception as e:
        raise ToolError(f"Error adding triplet: {e}")

#-------------------------------
# ADD SELECT VARIABLE INTERNALS
#-------------------------------

async def add_select_variable_internal(
    variable: str,
    aggregator: Optional[str],
    query_id: str
) -> Dict[str, Any]:
    try:
        qc = QUERY_STORAGE.get(query_id)
        if not qc:
            raise Exception(f"Query ID {query_id} not found or expired.")
        
        # Check if variable exists in registry (it must be defined somewhere)
        var_label = ""
        # Try to find label from registry
        if variable in qc.variable_registry:
            var_label = qc.variable_registry[variable]["var_label"]
        else:
            # Check if it was already in SELECT
            for s in qc.select:
                if s["var_name"] == variable:
                    var_label = s["var_label"]
                    break
        
        # Add/Update select
        qc.add_select(variable, var_label, aggregator=aggregator)
        
        return {
            "query_id": query_id,
            "generated_query": qc.to_string(),
            "message": f"Variable ?{variable} added to SELECT list."
        }
    except Exception as e:
        raise ToolError(f"Error adding selection variable: {e}")

#-------------------------------
# GROUP BY HAVING INTERNALS
#-------------------------------

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
    
    try:
        # SETUP & VALIDATION
        qc = QUERY_STORAGE.get(query_id)
        if not qc:
            raise Exception(f"Query ID {query_id} not found.")

        # Validate Subject (Must exist in query)
        subject = subject.strip()
        if ":" in subject:
            # The LLM passed a label
            subject_uri = subject
            tmp_subj = qc.get_varName_from_uri(subject_uri)
            if not tmp_subj:
                raise Exception(f"No subject was found with URI {subject_uri}")
            subject = tmp_subj
        else:
            subject_uri = qc.get_variable_uri(subject)
            if not subject_uri:
                raise Exception(f"Subject variable ?{subject} not found in query.")

        if obj:
            obj = obj.lower()
            # Validate Object
            if ":" in obj:
                #the LLM passed a label or URI
                obj_uri = obj
                tmp_obj = qc.get_varName_from_uri(obj_uri)
                if not tmp_obj:
                    raise Exception(f"No object was found with URI {obj_uri}")
                obj = tmp_obj
            

            triple = qc.get_triple_object(subject, obj)
            if not triple:
                raise Exception(f"Unable to find a triple between {subject} and {obj}")
            
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
                "triples": triples
            })

        # CONSTRUCT GROUP BY: Group by the subject + any other non-aggregated variable in SELECT
        # We first update the select to include the subject of the group By (which might not be
        # in it) and then extract the variables in the select that are not sampled
        
        qc.add_select(subject, subject_uri)
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
            "query_id": query_id, 
            "generated_query": qc.to_string(),
            "message": "Group By and Having clauses applied successfully."
        }
    except Exception as e:
        raise ToolError(f"Error applying Group By/Having: {e}")

#-------------------------------
# EXECUTE QUERY INTERNALS
#-------------------------------

def execute_query_from_id_internal(query_id: str, limit: int) -> Dict[str, Any]:
    try:
        qc = QUERY_STORAGE.get(query_id)
        if not qc:
            raise Exception(f"Query ID {query_id} not found or expired.")
        
        if SAVE_QUERIES:
            # Write query and ID to file, create directory if it doesn't exist
            os.makedirs("queries", exist_ok=True)
            with open(f"queries/{query_id}.txt", "w") as f:
                f.write("Question: \n" + qc.get_question())
                f.write("\n\n")
                f.write("SPARQL Query: \n" + qc.to_string())
                f.write("LIMIT: " + str(limit))
        logger.info(f"Executing query : {qc.to_string(for_execution=True)} with limit {limit}")        
        return execute_sparql_query(qc.to_string(for_execution=True), limit)
    except Exception as e:
        raise ToolError(f"Error executing query: {e}")


if __name__ == "__main__":
    # Example usage
    test_entity = "violin"
    result = find_linked_entities("Casting", test_entity)
    print(f"Linked entities for '{test_entity}': {result}")