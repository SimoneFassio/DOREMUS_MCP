from typing import Any, Optional, List, Dict
import logging
import re
from server.tool_sampling import tool_sampling_request
from server.utils import execute_sparql_query, validate_doremus_uri, get_entity_label

logger = logging.getLogger("doremus-mcp")

# ----------------------
# ELEMENT FORMATTERS
# ----------------------

def create_triple_element(var_name: str, var_label: str, vtype: str) -> Dict[str, Any]:
    if vtype not in ["var", "uri", "literal"]:
        raise ValueError(f"Invalid type '{vtype}' for query element. Must be 'var', 'uri', or 'literal'.")
    return {"var_name": var_name, "var_label": var_label, "type": vtype}

#----------------------
# QUERY CONTAINER
#----------------------

class QueryContainer:
    """
    A container for building SPARQL queries incrementally using modular components.
    
    This class manages the state of a SPARQL query, including SELECT variables,
    WHERE clause patterns, and structure (GROUP BY, LIMIT, etc.). It handles
    variable naming conflicts and ensures consistency between modules.
    """
    
    def __init__(self, query_id: str, question: str = ""):
        self.query_id = query_id
        
        # Select: List of dicts e.g., {"var_name": "title", "var_label": "", "aggregator": "COUNT"}
        self.select: List[Dict[str, Any]] = []
        self.distinct_select: bool = True
        
        # Where: List of modules (dictionaries)
        self.where: List[Dict[str, Any]] = []

        # Filters: List of filter expressions (dictionaries)
        # {"function": str, "args": List[str]}
        self.filter_st: List[Dict[str, Any]] = []

        # Modifiers: Lists of dicts with {"var_name": str, "var_label": str}
        self.group_by: List[Dict[str, Any]] = []
        self.having: List[Dict[str, Any]] = []
        self.order_by: List[Dict[str, Any]] = []

        # Metadata
        self.question: str = question
        
        # Variable dependency tracking
        # Map of var_name -> { "var_label": str, "count": int }
        self.variable_registry: Dict[str, Dict[str, Any]] = {}
        self.track_dep: bool = True
        
        # Sampling Logs
        self.sampling_logs: List[Dict[str, Any]] = []


    def _auto_categorize_variables(self, module: Dict[str, Any]) -> tuple[List[Dict[str, str]], List[Dict[str, str]]]:
        """
        Automatically categorize variables in the module into required (existing) and defined (new).
        """
        vars_in_module = {} # name -> label
        
        # 1. Scan Triples
        if "triples" in module:
            for t in module["triples"]:
                for part in ["subj", "pred", "obj"]:
                    elem = t.get(part)
                    if elem and elem.get("type") == "var":
                        name = elem["var_name"]
                        label = elem.get("var_label", "")
                        # Store label if present/not empty, or keep existing
                        if name not in vars_in_module:
                            vars_in_module[name] = label
                        elif label:
                            vars_in_module[name] = label
                            
        # 2. Scan Filters
        if "filter_st" in module:
            for fl in module["filter_st"]:
                for arg in fl.get("args", []):
                    # args are strings like "?title" or "'value'"
                    # Extract variables starting with ?
                    matches = re.findall(r'\?(\w+)', arg)
                    for match in matches:
                        if match not in vars_in_module:
                            vars_in_module[match] = "" # filter vars might not have label available here
        
        required_vars = []
        defined_vars = []
        
        for name, label in vars_in_module.items():
            if name in self.variable_registry:
                # Existing variable -> Required (Connection)
                # Use the label from registry to ensure consistency if module didn't provide one
                reg_label = self.variable_registry[name]["var_label"]
                required_vars.append({"var_name": name, "var_label": reg_label})
            else:
                # Not in registry -> Defined (New)
                if not label:
                    logger.warning(f"Variable ?{name} is being defined without a semantic label (URI). This may prevent correct variable linking.")
                defined_vars.append({"var_name": name, "var_label": label})
                
        return required_vars, defined_vars  

    # ----------------------
    # MODULE MANAGEMENT
    # ----------------------
    async def add_module(self, module: Dict[str, Any], dry_run: bool = True) -> bool:
        """
        Add a query module to the container after validation and connectivity checks.
        
        Args:
            module: Dictionary containing module definition.
                {
                    "id": str,
                    "type": str, # e.g., "associate_N_entities", "query_builder"
                    "scope": str, # e.g., "main", "optional" (Placeholder for future)
                    "triples": List[Dict[str, Any]], Structured triples {"subj":{"var_name": str, "var_label": str}, "pred":{...}, "obj":{...}}
                    "filter_st": List[Dict[str, Any]], Optional filters associated with this module
                    "branches": List[Dict[str, Any]], Optional branching patterns, used for UNIONs or OPTIONALs
                    "required_vars": List[Tuple[str, str]], variables that MUST be already defined
                    "defined_vars": List[Tuple[str, str]], variables that this module defines
                }
            dry_run: If True, adds module temporarily, tests query, and reverts if it fails.

        Returns:
            True if module was added (or would be valid in dry_run), False otherwise.
        """
        # Validate module structure
        if not self._validate_module(module):
            error_msg = f"Invalid module structure for ID: {module.get('id', 'unknown')}"
            logger.error(error_msg)
            raise Exception(error_msg)

        # Validate DOREMUS URIs to prevent hallucinations
        if "triples" in module:
            for t in module["triples"]:
                for part in ["subj", "pred", "obj"]:
                    elem = t.get(part)
                    if elem:
                        val = elem.get("var_name", "")
                        if val.startswith("http://") or val.startswith("https://"):
                            if not validate_doremus_uri(val):
                                error_msg = f"Hallucinated URI detected: {val}"
                                logger.error(error_msg)
                                raise Exception(error_msg)
                            else:
                                try:
                                    label_found = get_entity_label(val)
                                    if label_found:
                                        elem["hum_readable_label"] = label_found
                                except Exception as e:
                                    logger.warning(f"Could not fetch label for {val}: {e}")

        # Auto-categorize variables if not explicitly provided
        if "required_vars" not in module and "defined_vars" not in module:
            required_vars, defined_vars = self._auto_categorize_variables(module)
            module["required_vars"] = required_vars
            module["defined_vars"] = defined_vars

        # BACKUP STATE
        state_backup = {
            "where": self.where.copy(),
            "filter_st": self.filter_st.copy(),
            "variable_registry": {k: v.copy() for k, v in self.variable_registry.items()},
            # deeply copy select to avoid issues with modifications
            "select": [s.copy() for s in self.select] 
        }

        # 1. ADD MODULE (TEMPORARILY)
        if module["scope"] == "main":
            # Process variable renaming (if needed for uniqueness or linking)
            processed_module = await self._process_variables(module)
            
            if processed_module.get("filter_st"):
                for fl in processed_module.get("filter_st", []):
                    self.filter_st.append(fl)
            
            if processed_module.get("triples"):
                self.where.append(processed_module)
        
        elif module["scope"] == "optional":
            logger.warning("Optional modules not yet implemented.")
            raise Exception("Optional modules not yet implemented.")

        # 2. DRY RUN TEST
        if dry_run:
            try:
                self.dry_run_test()
            except Exception as e:
                # REVERT STATE
                self.where = state_backup["where"]
                self.filter_st = state_backup["filter_st"]
                self.variable_registry = state_backup["variable_registry"]
                self.select = state_backup["select"]
                raise e
        
        return True

    def set_order_by(self, variables: List[Dict[str, Any]]) -> None:
        self.order_by = variables

    def set_group_by(self, variables: List[Dict[str, Any]]) -> None:
        self.group_by = variables

    def add_having(self, condition: Dict[str, Any]) -> None:
        self.having.append(condition)

    def _validate_module(self, module: Dict[str, Any]) -> bool:
        """Basic validation of module structure."""
        required_keys = ["id", "triples"]
        required_filter_keys = ["id", "filter_st"]
        return all(key in module for key in required_keys) or all(key in module for key in required_filter_keys)
    
    def _modify_var(self, module: Dict[str, Any], old_var: str, new_var: str) -> None:
        """Helper to rename variables in triples."""
        if "triples" in module.keys():
            for t in module.get("triples", []):
                for part in ["subj", "pred", "obj"]:
                    if t[part]["var_name"] == old_var:
                        t[part]["var_name"] = new_var
        if "filter_st" in module.keys():
            for f in module.get("filter_st", []):
                for i, arg in enumerate(f.get("args", [])):
                    if arg == f"?{old_var}":
                        f["args"][i] = f"?{new_var}"
        # Also update defined_vars and required_vars if present
        if "defined_vars" in module:
            for dv in module["defined_vars"]:
                if dv["var_name"] == old_var:
                    dv["var_name"] = new_var
        if "required_vars" in module:
            for rv in module["required_vars"]:
                if rv["var_name"] == old_var:
                    rv["var_name"] = new_var
    
    def _update_variable_counter(self, var_uri: str) -> None:
        """Increment the counter for each variable having the same URI."""
        for var_name, var_info in self.variable_registry.items():
            if var_info["var_label"] == var_uri:
                var_info["count"] += 1

    def _parse_for_llm(self, new_module: Dict[str, Any], conflict_var: Dict[str, str]) -> str:
        """
        Parse the current query and the module to be added into a human-readable format
        for the LLM to understand, highlighting new modules, existing variables, and conflicts.

        Args:
            module: The module to be added, containing triples, filters, and other details.

        Returns:
            A string representation of the current query and the module to be added.
        """
        conflict_var_name = conflict_var["var_name"]
        conflict_var_label = conflict_var["var_label"]
        query_parts = []

        # Build Select string
        select_mod = "DISTINCT " if self.distinct_select else ""
        select_vars_str = []

        for item in self.select:
            var_name = item["var_name"]
            if var_name == conflict_var_name:
                highlighted_name = f"**?{var_name}**"
            else:
                highlighted_name = f"?{var_name}"
            if item.get("aggregator"):
                agg = item["aggregator"]
                select_vars_str.append(f"{agg}({highlighted_name}) as {highlighted_name}")
            else:
                select_vars_str.append(highlighted_name)
        
        query_parts.append(f"  SELECT {select_mod}{', '.join(select_vars_str)}")
        
        # Build Where string
        query_parts.append("  WHERE {")
        logger.debug(f"Parsing current query for LLM with conflict variable: {conflict_var_name}")
        for module in self.where:
            mod_id = module.get("id", "unnamed")
            query_parts.append(f"    # Module: {mod_id}")
            if module.get("scope") == "main":
                triples = module.get("triples", [])
                for t in triples:
                    s_str = self._format_term(t.get("subj"))
                    if not t.get("subj"):
                        logger.error(f"Malformed triple in module {mod_id}: {t}")
                    if t.get("subj").get("var_label") == conflict_var_label:
                        s_str = f"**{s_str}**"
                    p_str = self._format_term(t.get("pred"))
                    o_str = self._format_term(t.get("obj"))
                    if t.get("obj").get("var_label") == conflict_var_label:
                        o_str = f"**{o_str}**"
                    if p_str == "VALUES":
                        query_parts.append(f"      {p_str} {s_str} {{ {o_str} }}")
                    else:
                        query_parts.append(f"      {s_str} {p_str} {o_str} .")
            else:
                query_parts.append(f"    # Unsupported scope: {module.get('scope')}")

        # Add the new module with "+" annotations
        query_parts.append("\n    + New Module:")
        if "triples" in new_module:
            for t in new_module["triples"]:
                s_str = self._format_term(t.get("subj"))
                if s_str == f"?{conflict_var_name}":
                    s_str = f"<<{s_str}>>"
                p_str = self._format_term(t.get("pred"))
                o_str = self._format_term(t.get("obj"))
                if o_str == f"?{conflict_var_name}":
                    o_str = f"<<{o_str}>>"
                if p_str == "VALUES":
                    query_parts.append(f"    + {p_str} {s_str} {{ {o_str} }}")
                else:
                    query_parts.append(f"    + {s_str} {p_str} {o_str} .")

        if "filter_st" in new_module:
            query_parts.append("\n    + New Filters:")
            for f in new_module["filter_st"]:
                func = f.get("function")
                args = f.get("args", [])
                formatted_args = args.copy()
                for i, arg in enumerate(formatted_args):
                    if arg == f"?{conflict_var_name}":
                        formatted_args[i] = f"<<{arg}>>"
                if func == "":
                    args_str = " ".join(formatted_args)
                    query_parts.append(f"    + FILTER ({args_str})")
                elif func:
                    args_str = ", ".join(formatted_args)
                    query_parts.append(f"    + FILTER {func}({args_str})")

        query_parts.append("  }")

        return "\n".join(query_parts)

    async def _process_variables(self, module: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle variable naming conventions and collision resolution.

        Procedure:
        - For each required variable in the module, ensure it matches an existing variable.
        - For each defined variable, check for naming collisions:
            - If no collision, register it.
            - If collision, use LLM sampling to decide whether to rename or reuse an existing variable.
        
        Args:
            module: The module to be processed.
        Returns:
            The processed module with variables renamed as needed.
        """

        new_module = module.copy()

        if self.track_dep:
            # Check on required variables
            if "required_vars" in module.keys():
                for req_elem in module["required_vars"]:
                    if req_elem["var_name"] not in [v["var_name"] for v in self.select]:
                        for sel_elem in self.select:
                            if sel_elem["var_label"] == req_elem["var_label"]:
                                self._modify_var(new_module, req_elem["var_name"], sel_elem["var_name"])
                                break
            # Check on defined variables
            if "defined_vars" in module.keys():
                for def_elem in module["defined_vars"]:
                    var_name = def_elem["var_name"]
                    var_label = def_elem["var_label"]
                    # New variable -> register and update the count of all the others with same URI
                    if var_name not in self.variable_registry.keys():
                        if var_label not in [v["var_label"] for v in self.variable_registry.values()]:
                            self.variable_registry[var_name] = {"var_label": var_label, "count": 1}
                        else:
                            for existing_var, var_info in self.variable_registry.items():
                                if var_info["var_label"] == var_label:
                                    # Maintain count
                                    count = var_info["count"]
                                    self.variable_registry[var_name] = {"var_label": var_label, "count": count}
                                    self._update_variable_counter(var_label)
                                    break
                    # Do not change name for query builder generated variables, TODO it works but logic is not most efficient
                    elif "query_builder" in module["type"]:
                        option_list = []
                        for reg_var_name, reg_var_el in self.variable_registry.items():
                            if reg_var_el["var_label"] == var_label:
                                option_list.append(reg_var_name)
                        chosen_var = option_list[0]
                        self._modify_var(new_module, var_name, chosen_var)
                    # Handle collision using LLM-based sampling
                    else:
                        count = self.variable_registry[var_name]["count"]
                        working_query = self._parse_for_llm(module, def_elem)
                        option_list = []
                        for reg_var_name, reg_var_el in self.variable_registry.items():
                            if reg_var_el["var_label"] == var_label:
                                option_list.append(reg_var_name)
                        options = "\n".join([f"- Option {i}: '{opt}'" for i, opt in enumerate(option_list)])
                        options += f"\n- Option {len(option_list)}: Rename to '{var_name}_{count}'"
                        pattern_intent = f"""solving the conflict for '{var_name}' by determining whether to 
rename it or use one of the existing variables.

This is the current query structure, where:
- The "+" in front of a line indicates the new module being added.
- The **bolded** variable names are the existing ones in the query.
- The <<variable>> indicates the conflicting variable in the new module.

The current query is asking about: '{self.question}'
--
{working_query}
--

Therefore, the current options to put in place of '<<{var_name}>>' are:
{options}
                        """
                        system_prompt = """You are an expert SPARQL query builder assisting in variable naming.
You are given a SPARQL query and a list of options to replace a variable in the query.
You must choose one of the options and return the index of the chosen option.
You should select an option different to 0 ONLY if the variable represent a new entity of the same class of the one in the query, used for example for comparison or for checking relations.
"""
                        # Define callback to capture sampling logs
                        def log_sampling(log_data: Dict[str, Any]):
                            self.sampling_logs.append(log_data)
                            
                        llm_answer = await tool_sampling_request(system_prompt, pattern_intent, log_callback=log_sampling)
                        try:
                            match = re.search(r'\d+', llm_answer)
                            if match:
                                index = int(match.group())
                            else:
                                index = len(option_list)
                            if index == len(option_list):
                                # Rename + add to registry
                                new_var_name = f"{var_name}_{count}"
                                self._modify_var(new_module, var_name, new_var_name)
                                self._update_variable_counter(var_label)
                            else:
                                # Use existing variable
                                chosen_var = option_list[index]
                                self._modify_var(new_module, var_name, chosen_var)
                        except (ValueError, IndexError):
                            logger.error(f"LLM returned invalid index '{llm_answer}' for variable conflict resolution.")
                            # Default to renaming
                            new_var_name = f"{var_name}_{count}"
                            self._modify_var(new_module, var_name, new_var_name)
                            self._update_variable_counter(var_label)
                    
        return new_module
    
    def get_variable_uri(self, var_name: str) -> Optional[str]:
        """Retrieve variable info from the registry."""
        if var_name in self.variable_registry.keys():
            return self.variable_registry[var_name]["var_label"]
        return None

    def get_varName_from_uri(self, var_uri: str) -> Optional[str]:
        for var, val in self.variable_registry:
            if val["var_label"] == var_uri:
                return var
        return None
    
    def get_triple_object(self, subj_name: str, obj_name: str) -> Dict[str, Any]:
        #TODO: check that group by link is only in where
        best_match = None
        best_score = -1

        for mod in self.where:
            triples = mod["triples"]
            for t in triples:
                score = 0

                # Check subject match
                if t["subj"]["var_name"] == subj_name:
                    score += 2  # Exact match gets higher weight
                elif subj_name in t["subj"]["var_name"]:
                    score += 1  # Partial match gets lower weight

                # Check object match
                if t["obj"]["var_name"] == obj_name:
                    score += 2  # Exact match gets higher weight
                elif isinstance(t["obj"]["var_name"], str):
                    if obj_name in t["obj"]["var_name"]:
                        score += 1  # Partial match gets lower weight
                
                # Update best match if this triple has a higher score
                if score > best_score:
                    best_match = t
                    best_score = score
        if best_match:
            return best_match

        logger.error(f"Impossible to find a matching triple with subj: {subj_name} and obj: {obj_name}")
        return {}

    def add_select(self, var_name: str, var_label: str = "", aggregator: Optional[str] = None) -> None:
        """Add a single variable to the SELECT list or update its aggregator."""
        existing = next((v for v in self.select if v["var_name"] == var_name), None)
        if var_name not in self.variable_registry.keys():
            raise Exception(f"Variable {var_name} not found in varaible registry")
        
        if aggregator:
            aggregator = aggregator.upper()
            if aggregator not in ["COUNT", "SUM", "AVG", "MAX", "MIN"]:
                raise Exception(f"Invalid aggregator {aggregator}")
        if existing:
            # Update aggregator
            existing["aggregator"] = aggregator
        else:
            self.select.append({"var_name": var_name, "var_label": var_label, "aggregator": aggregator})

    def set_question(self, question: str) -> None:
        self.question = question

    def get_question(self) -> str:
        return self.question
    
    def get_non_aggregated_vars(self) -> List[str]:
        """Return a list of variable names in SELECT that are NOT aggregated."""
        return [item for item in self.select if not item.get("aggregator")]

    def dry_run_test(self) -> bool:
        """
        Basic sanity check for the query structure.
        Connectivity is already checked in add_module, so this checks buildability.
        Also executes the query with LIMIT 1 to check for 0 results or errors.
        """
        if not self.where:
            logger.warning("Dry Run Failed: WHERE clause is empty.")
            raise Exception("Dry Run Failed: WHERE clause is empty.")
            
        # Execute Query with LIMIT 1
        query_str = self.to_string(eliminate_dead_code=True)
        res = execute_sparql_query(query_str, limit=1)
        
        if not res["success"]:
            logger.warning(f"Dry Run Failed: Query execution error: {res.get('error')}")
            raise Exception(f"Dry Run Failed: Query execution error: {res.get('error')}")
        
        if res.get("count", 0) == 0:
            logger.warning("Dry Run Failed: Query returned 0 results.")
            raise Exception("Dry Run Failed: Query returned 0 results.")

        return True

    def _count_variable_usage(self) -> Dict[str, int]:
        counts = {}
        
        def inc(name):
            counts[name] = counts.get(name, 0) + 1
            
        # Select
        for s in self.select:
            inc(s["var_name"])
            
        # Group By
        for g in self.group_by:
            inc(g["var_name"])
            
        # Order By
        for o in self.order_by:
            inc(o["var_name"])
            
        # Having (vars inside aggregation)
        for h in self.having:
             if "variable" in h and h["variable"]:
                 inc(h["variable"])
        
        # Filters (extract from args)
        for f in self.filter_st:
            for arg in f.get("args", []):
                matches = re.findall(r'\?(\w+)', arg)
                for m in matches:
                    inc(m)
                    
        # Triples
        for mod in self.where:
            if mod.get("scope") == "main":
                for t in mod.get("triples", []):
                    # Subj
                    if t.get("subj", {}).get("type") == "var":
                        inc(t["subj"]["var_name"])
                    # Pred
                    if t.get("pred", {}).get("type") == "var":
                        inc(t["pred"]["var_name"])
                    # Obj
                    if t.get("obj", {}).get("type") == "var":
                        inc(t["obj"]["var_name"])
                        
        return counts

    def to_string(self, eliminate_dead_code: bool = False) -> str:
        """
        Compile the internal state into a valid SPARQL query string.
        """
        query_parts = []

        # Build Select string
        select_mod = "DISTINCT " if self.distinct_select else ""
        select_vars_str = []

        for item in self.select:
            var_name = item["var_name"]
            
            if self.group_by:
                # Check if this variable is part of the grouping
                is_grouped = any(g["var_name"] == var_name for g in self.group_by)
                
                if is_grouped:
                    # Case 1: Variable is grouped -> Select as is
                    select_vars_str.append(f"?{var_name}")
                
                elif item.get("aggregator"):
                    # Case 2: Variable has explicit aggregator
                    agg = item["aggregator"]
                    if agg.upper() == "COUNT":
                        select_vars_str.append(f"{agg}(DISTINCT ?{var_name}) as ?{var_name}_{agg.lower()}")
                    else:
                        select_vars_str.append(f"{agg}(?{var_name}) as ?{var_name}_{agg.lower()}")
                
                else:
                    # Case 3: Variable is NOT grouped and NO aggregator -> Auto-SAMPLE
                    select_vars_str.append(f"SAMPLE(?{var_name}) as ?{var_name}")
            
            else:
                # No GROUP BY -> Select as is
                if item.get("aggregator"):
                    agg = item["aggregator"]
                    if agg.upper() == "COUNT":
                        select_vars_str.append(f"{agg}(DISTINCT ?{var_name}) as ?{var_name}")
                    else:
                        select_vars_str.append(f"{agg}(?{var_name}) as ?{var_name}")
                else:
                    select_vars_str.append(f"?{var_name}")

        if not select_vars_str:
            select_vars_str.append("*")
        
        query_parts.append(f"SELECT {select_mod}{' '.join(select_vars_str)}")
        
        # Build Where string
        query_parts.append("WHERE {")
        
        # Calculate variable usage for dead code elimination
        var_counts = self._count_variable_usage()

        for module in self.where:
            mod_id = module.get("id", "unnamed")
            query_parts.append(f"  # Module: {mod_id}")
            if module.get("scope") == "main":
                triples = module.get("triples", [])
                for t in triples:
                    # Triples are now Dicts: {"subj": {...}, "pred": {...}, "obj": {...}} -> sanity check
                    if not all(k in t for k in ("subj", "pred", "obj")):
                        logger.warning(f"Module {mod_id} has malformed triple: {t}")
                        continue
                    
                    # DEAD CODE ELIMINATION CHECK
                    # If object is a variable and it is used only once (here), skip it.
                    if t.get("obj", {}).get("type") == "var":
                        obj_name = t["obj"]["var_name"]
                        if var_counts.get(obj_name, 0) <= 1:
                            logger.info(f"Dead code elimination: Skipping triple {t} because variable {obj_name} is used only once.")
                            continue

                    s_str = self._format_term(t.get("subj"))
                    p_str = self._format_term(t.get("pred"))
                    o_str = self._format_term(t.get("obj"))
                    
                    if p_str == "VALUES":
                        # Special handling for VALUES clause
                        line = f"  {p_str} {s_str} {{ {o_str} }}"
                    else:
                        line = f"  {s_str} {p_str} {o_str} ."
                    # Add comments
                    comments = []
                    for part in ["subj", "pred", "obj"]:
                        part_elem = t.get(part)
                        if part_elem and "hum_readable_label" in part_elem:
                            comments.append(part_elem["hum_readable_label"])
                    
                    if comments:
                        line += f" # {', '.join(comments)}"
                        
                    query_parts.append(line)
            else:
                logger.warning(f"Module {mod_id} has unsupported scope: {module.get('scope')}")
                continue
        
        # Build Filters
        if self.filter_st:
            query_parts.append("\n  # Filters")
            filter_parts = []
            for i, filter_cond in enumerate(self.filter_st):
                if i == 0:
                    filter_parts.append("  FILTER (")
                func = filter_cond.get("function")
                args = filter_cond.get("args", [])
                if func == "":
                    args_str = " ".join(args)
                    filter_parts.append(f"{args_str}")
                if func == "||":
                    logger.warning("OR filters not yet implemented.")
                elif func and args:
                    args_str = ", ".join(args)
                    filter_parts.append(f"{func}({args_str})")
                if i != len(self.filter_st) - 1:
                    # By default, we AND filters
                    filter_parts.append("AND")
                else:
                    filter_parts.append(")")
            query_parts.append(" ".join(filter_parts))
        
        query_parts.append("}")

        # Build Group By
        if self.group_by:
            g_vars = [f"?{v['var_name']}" for v in self.group_by]
            query_parts.append(f"GROUP BY {' '.join(g_vars)}")

        # Build Having
        if self.having:
            having_parts = []
            for condition in self.having:
                function = condition.get("function", "")
                var = condition.get("variable", "")
                agg_expr = f"{function}(?{var})"
                if condition.get("operator",""):
                    operator = condition.get("operator","")
                    if operator == "range":
                        if condition.get("valueStart","") and condition.get("valueEnd",""):
                            valueStart = condition.get("valueStart","")
                            valueEnd = condition.get("valueEnd","")
                            having_parts.append(f"{agg_expr} >= {valueStart} && {agg_expr} <= {valueEnd}")
                    else:
                        valueStart = condition.get("valueStart","")
                        having_parts.append(f"{agg_expr} {operator} {valueStart}")

            query_parts.append(f"HAVING ({' && '.join(having_parts)})")

        # Build Order By
        if self.order_by:
            o_vars = [f"?{v['var_name']}" for v in self.order_by]
            query_parts.append(f"ORDER BY {' '.join(o_vars)}")
        
        return "\n".join(query_parts)

    def _format_term(self, term: Dict[str, Any]) -> str:
        """
        Helper to format a Subject/Predicate/Object dictionary into a SPARQL string.
        
        Expected term structure:
        { "var_name": str, "var_label": "uri", type: "var"|"uri"|"literal"}
        type: "var", "uri", "literal"
        """
        if not term:
            logger.error("Term is None or empty.")
            return ""
        
        if "type" not in term:
            logger.warning(f"Term missing 'type': {term}")
            return str(term)
        
        t_type = term.get("type")

        if t_type == "var":
            val = term.get("var_name")
            # It's a variable -> Prepend '?'
            return f"?{val}"
        elif t_type == "uri":
            val = term.get("var_label")
            # It's a full URI -> Wrap in <>
            if not val:
                logger.error(f"URI term missing 'var_label': {term}")
                return ""
            if val.startswith("http"):
                return f"<{val}>"
            return val # It might be a prefixed URI like mus:U13...
        elif t_type == "literal":
            val = term.get("var_name")
            # It's a value -> Wrap in quotes if string, else as is
            if isinstance(val, str):
                return f'"{val}"'
            return str(val)
        
        return str(val)