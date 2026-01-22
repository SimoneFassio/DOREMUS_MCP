from email.mime import base
from os import name
from typing import Any, Optional, List, Dict
import logging
import re
import copy

from requests import options
from server.tool_sampling import tool_sampling_request
from server.utils import execute_sparql_query, validate_doremus_uri, get_entity_label, find_equivalent_uris

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

    _PREFERRED_URI_PREFIXES = (
        "http://data.doremus.org/vocabulary/iaml/",
    )
    
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

    
    #-----------------------
    # EXTRACT NEW VARIABLES
    # -----------------------
    def extract_defined_variables(self, triples: Dict[str, Any]) -> List[Dict[str, str]]:
        new_vars = []
        for t in triples:
            subj = t["subj"]
            if subj["type"] == "var":
                if subj["var_name"] not in [new_var["var_name"] for new_var in new_vars]:
                    new_vars.append({"var_name": subj["var_name"], "var_label": subj["var_label"]})
            obj = t["obj"]
            if obj["type"] == "var":
                if obj["var_name"] not in [new_var["var_name"] for new_var in new_vars] and isinstance(obj["var_name"], str):
                    new_vars.append({"var_name": obj["var_name"], "var_label": obj["var_label"]}) 
        return new_vars


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
    async def test_add_module(self, module: Dict[str, Any]) -> bool:
        """
        Test if a module can be added to the query without permanently modifying the state.
        Checks that among all the variable combinations there is one that allows the query to be valid.
        
        Args:
            module: Dictionary containing module definition.
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
                        if isinstance(val, str) and (val.startswith("http://") or val.startswith("https://")):
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
            "where": copy.deepcopy(self.where),
            "filter_st": copy.deepcopy(self.filter_st),
            "variable_registry": copy.deepcopy(self.variable_registry),
            "select": copy.deepcopy(self.select),
            "group_by": copy.deepcopy(self.group_by),
            "having": copy.deepcopy(self.having),
            "order_by": copy.deepcopy(self.order_by),
        }

        # 1. ADD MODULE (TEMPORARILY)
        if module["scope"] == "main":
            # Process variable renaming (if needed for uniqueness or linking)
            try:
                processed_module = await self._process_variables(module, dry_run=True)
                return True
            except Exception as e:
                raise e
            finally:
                # REVERT STATE
                self.where = state_backup["where"]
                self.filter_st = state_backup["filter_st"]
                self.variable_registry = state_backup["variable_registry"]
                self.select = state_backup["select"]
                self.group_by = state_backup["group_by"]
                self.having = state_backup["having"]
                self.order_by = state_backup["order_by"]
            
            
        elif module["scope"] == "optional":
            logger.warning("Optional modules not yet implemented.")
            raise Exception("Optional modules not yet implemented.")
        
        return True
    
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
                        if isinstance(val, str) and (val.startswith("http://") or val.startswith("https://")):
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
            "where": copy.deepcopy(self.where),
            "filter_st": copy.deepcopy(self.filter_st),
            "variable_registry": copy.deepcopy(self.variable_registry),
            "select": copy.deepcopy(self.select)
        }

        # 1. ADD MODULE
        if module["scope"] == "main":
            # Process variable renaming (if needed for uniqueness or linking)
            processed_module = await self._process_variables(module)
            
            # --- URI EXPANSION LOGIC ---
            self._expand_values_uris_in_module(processed_module, max_uris=4)
            # ---------------------------

            if processed_module.get("filter_st"):
                for fl in processed_module.get("filter_st", []):
                    self.filter_st.append(fl)
            
            if processed_module.get("triples"):
                self.where.append(processed_module)
            logger.info(f"Module {processed_module.get('id', 'unknown')} added successfully.")
        
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
                logger.info(f"Module {processed_module.get('id', 'unknown')} reverted due to dry run failure.")
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
    
    def _base_var_name(self, name: str) -> str:
        """Strip a trailing _<digits> suffix: castingDetail_1 -> castingDetail."""
        return re.sub(r"_\d+$", "", name)

    def _next_free_var_name(self, base: str) -> str:
        """
        Return base if unused, else base_1, base_2, ... not present in variable_registry.
        """
        if base not in self.variable_registry:
            return base
        i = 1
        while f"{base}_{i}" in self.variable_registry:
            i += 1
        return f"{base}_{i}"
    
    def _prune_equivalent_uris(self, orig_uri: str, expanded_uris: List[str], max_uris: int = 6) -> List[str]:
        """
        Keep only a few equivalent URIs, restricted to preferred namespaces.
        Always returns at least [orig_uri].
        """
        # Dedup while preserving order (orig first)
        seen = set()
        candidates: List[str] = []
        for u in [orig_uri, *expanded_uris]:
            if isinstance(u, str) and u not in seen:
                seen.add(u)
                candidates.append(u)
        preferred = [u for u in candidates if u.startswith(self._PREFERRED_URI_PREFIXES)]
        pruned = preferred[:max_uris] if preferred else [orig_uri]

        if len(candidates) != len(pruned):
            logger.info(
                f"Pruned URI expansion for VALUES: {orig_uri} "
                f"from {len(candidates)} to {len(pruned)} (preferred-only={bool(preferred)})."
            )
        return pruned

    def _expand_values_uris_in_module(self, module: Dict[str, Any], max_uris: int = 6) -> None:
        """
        Expand VALUES object URI into a pruned list of equivalent URIs.
        Mutates module in-place.
        """
        if not module.get("triples"):
            return

        for t in module["triples"]:
            p_str = self._format_term(t.get("pred"))
            if p_str != "VALUES":
                continue

            obj = t.get("obj", {})
            if obj.get("type") != "uri":
                continue
            orig_uri = obj.get("var_label")
            if not isinstance(orig_uri, str):
                # already expanded or malformed
                continue
            expanded = find_equivalent_uris(orig_uri) or []
            pruned = self._prune_equivalent_uris(orig_uri, expanded, max_uris=max_uris)

            # store as list so execution-mode can emit multiple URIs
            obj["var_label"] = pruned

    
    def _recursive_variable_dry_run(self, new_module: Dict[str, Any], def_elem: Dict[str, Any], def_var: List[Dict[str, Any]], state_backup_v: Dict[str, Any], depth: int = 0) -> bool:
        # TERMINATION CONDITION: we reach depth equal to length of defined variables
        if depth >= len(def_var):
            # --- URI EXPANSION LOGIC ---
            self._expand_values_uris_in_module(new_module, max_uris=4)
            # ---------------------------

            if new_module.get("filter_st"):
                for fl in new_module.get("filter_st", []):
                    self.filter_st.append(fl)
                                        
            if new_module.get("triples"):
                self.where.append(new_module)

            logger.info(f"Testing module {new_module.get('id', 'unknown')} in dry run.")

            try:
                self.dry_run_test()
                return True
            except Exception as e:
                logger.error(e)
                return False
            finally:
                # ALWAYS revert container state
                self.where = copy.deepcopy(state_backup_v["where"])
                self.filter_st = copy.deepcopy(state_backup_v["filter_st"])
                self.variable_registry = copy.deepcopy(state_backup_v["variable_registry"])
                self.select = copy.deepcopy(state_backup_v["select"])
                logger.info(f"Testing module {new_module.get('id', 'unknown')} removed after dry run.")
            

        var_name = def_elem["var_name"]
        var_label = def_elem["var_label"]
        next_var = def_var[depth + 1] if depth + 1 < len(def_var) else None

        # New variable: we don't have to modify the module
        if var_name not in self.variable_registry:
            return self._recursive_variable_dry_run(
                copy.deepcopy(new_module),
                next_var,
                def_var,
                state_backup_v,
                depth + 1
            )
        # Collision: try reusing any registry var with same label
        for reg_var_name, reg_var_el in self.variable_registry.items():
            if reg_var_el["var_label"] == var_label and var_label != "":
                candidate_module = copy.deepcopy(new_module)
                self._modify_var(candidate_module, var_name, reg_var_name)

                if self._recursive_variable_dry_run(
                    candidate_module,
                    next_var,
                    def_var,
                    state_backup_v,
                    depth + 1
                ):
                    return True
                logger.info(
                    f"Dry run failed when testing variable '{reg_var_name}' "
                    f"for conflict resolution at depth {depth}."
                )

        # Collision: try renaming (base + next free)
        base = self._base_var_name(var_name)
        new_var_name = self._next_free_var_name(base)

        candidate_module = copy.deepcopy(new_module)
        self._modify_var(candidate_module, var_name, new_var_name)

        if self._recursive_variable_dry_run(
            candidate_module,
            next_var,
            def_var,
            state_backup_v,
            depth + 1
        ):
            return True

        logger.info(
            f"Dry run failed when testing renamed variable '{new_var_name}' "
            f"for conflict resolution at depth {depth}."
        )
        return False
    
    def _recursive_variable_retr_opt(self, new_module: Dict[str, Any], def_elem: Dict[str, Any], def_var: List[Dict[str, Any]], state_backup_v: Dict[str, Any], options: Dict[str, Any], current_config: Dict[str, str], depth: int = 0,) -> Dict[str, Any]:
        # TERMINATION CONDITION: we reach depth equal to length of defined variables
        if depth >= len(def_var):
            # --- URI EXPANSION LOGIC ---
            self._expand_values_uris_in_module(new_module, max_uris=4)
            # ---------------------------

            if new_module.get("filter_st"):
                for fl in new_module.get("filter_st", []):
                    self.filter_st.append(fl)
                                        
            if new_module.get("triples"):
                self.where.append(new_module)
            
            logger.info(f"Testing module {new_module.get('id', 'unknown')} in retrieval of options.")
            try:
                self.dry_run_test()
                for var_name, assigned_name in current_config.items():
                    if assigned_name not in options[var_name]:
                        options[var_name].append(assigned_name)
            except Exception as e:
                logger.error(e)
            finally:
                # ALWAYS revert container state
                self.where = copy.deepcopy(state_backup_v["where"])
                self.filter_st = copy.deepcopy(state_backup_v["filter_st"])
                self.variable_registry = copy.deepcopy(state_backup_v["variable_registry"])
                self.select = copy.deepcopy(state_backup_v["select"])
                logger.info(f"Testing module {new_module.get('id', 'unknown')} removed after retrieval of options.")

            return options
            

        var_name = def_elem["var_name"]
        var_label = def_elem["var_label"]
        next_var = def_var[depth + 1] if depth + 1 < len(def_var) else None

        # New variable: we don't have to modify the module
        if var_name not in self.variable_registry.keys():
            cfg = dict(current_config)
            cfg[var_name] = var_name
            logger.info(f"Variable '{var_name}' is new, proceeding without modification.")
            return self._recursive_variable_retr_opt(
                copy.deepcopy(new_module),
                next_var,
                def_var,
                state_backup_v,
                options,
                cfg,
                depth + 1,
            )
        # Handle collision: branch over reuse candidates (same var_label)
        for reg_var_name, reg_var_el in self.variable_registry.items():
            if reg_var_el["var_label"] == var_label and var_label != "":
                candidate_module = copy.deepcopy(new_module)
                self._modify_var(candidate_module, var_name, reg_var_name)

                cfg = dict(current_config)
                cfg[var_name] = reg_var_name

                options = self._recursive_variable_retr_opt(
                    candidate_module,
                    next_var,
                    def_var,
                    state_backup_v,
                    options,
                    cfg,
                    depth + 1,
                )
        # Collision: branch over rename option
        base = self._base_var_name(var_name)
        new_var_name = self._next_free_var_name(base)

        candidate_module = copy.deepcopy(new_module)
        self._modify_var(candidate_module, var_name, new_var_name)

        cfg = dict(current_config)
        cfg[var_name] = new_var_name

        options = self._recursive_variable_retr_opt(
            candidate_module,
            next_var,
            def_var,
            state_backup_v,
            options,
            cfg,
            depth + 1,
        )

        return options
    
    def _find_var_in_module_by_label(self, module: Dict[str, Any], var_label: str) -> Optional[str]:
        """
        Return the current var_name in `module` that corresponds to `var_label`
        (e.g. 'casting_1' for label 'mus:M6_Casting').
        """
        for elem in module.get("defined_vars", []) or []:
            if elem.get("var_label") == var_label:
                return elem.get("var_name")
        return None

    async def _process_variables(self, module: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
        """
        Handle variable naming conventions and collision resolution.

        Procedure:
        - For each required variable in the module, ensure it matches an existing variable.
        - For each defined variable, check for naming collisions:
            - If no collision, register it.
            - If collision, use LLM sampling to decide whether to rename or reuse an existing variable.
        
        Args:
            module: The module to be processed.
            dry_run: If True, the method will simulate the processing without making permanent changes.
        Returns:
            The processed module with variables renamed as needed.
        """

        new_module = copy.deepcopy(module)
        # BACKUP STATE
        state_backup_v = {
            "where": copy.deepcopy(self.where),
            "filter_st": copy.deepcopy(self.filter_st),
            "variable_registry": copy.deepcopy(self.variable_registry),
            "select": copy.deepcopy(self.select)
        }

        if self.track_dep:
            # Check on required variables
            if "required_vars" in module.keys():
                for req_elem in module["required_vars"]:
                    # Check against the full variable registry, not just selected variables
                    if req_elem["var_name"] not in self.variable_registry:
                        for reg_name, reg_info in self.variable_registry.items():
                            if reg_info["var_label"] == req_elem["var_label"]:
                                self._modify_var(new_module, req_elem["var_name"], reg_name)
                                break
            # Check on defined variables
            if "defined_vars" in module.keys() and len(module["defined_vars"]) > 0:
                if all(def_elem["var_name"] not in self.variable_registry for def_elem in module["defined_vars"]):
                    # No conflicts, simply register all defined variables
                    logger.info("No variable conflicts detected; registering defined variables directly.")
                    if not dry_run:
                        # If not a dry run, update registry
                        for def_elem in module["defined_vars"]:
                            var_name = def_elem["var_name"]
                            var_label = def_elem["var_label"]
                            self._update_variable_counter(var_label)
                            self.variable_registry[var_name] = {
                                "var_label": var_label,
                                "count": 1
                            }
                    else:
                        # In dry run, we have to test adding the module as is
                        try:
                            # --- URI EXPANSION LOGIC ---
                            self._expand_values_uris_in_module(module, max_uris=4)
                            # ---------------------------

                            if module.get("filter_st"):
                                for fl in module.get("filter_st", []):
                                    self.filter_st.append(fl)

                            if module.get("triples"):
                                self.where.append(module)

                            logger.info(f"Testing module {module.get('id', 'unknown')} in dry run check.")
                            self.dry_run_test()
                        finally:
                            # ALWAYS revert container state
                            self.where = copy.deepcopy(state_backup_v["where"])
                            self.filter_st = copy.deepcopy(state_backup_v["filter_st"])
                            self.variable_registry = copy.deepcopy(state_backup_v["variable_registry"])
                            self.select = copy.deepcopy(state_backup_v["select"])
                            logger.info(f"Testing module {module.get('id', 'unknown')} removed after dry run check.")
                    final_module = new_module
                elif dry_run:
                    if not self._recursive_variable_dry_run(new_module, new_module["defined_vars"][0], new_module["defined_vars"], state_backup_v):
                        raise Exception("Dry run variable conflict resolution failed.")
                    else:
                        return new_module
                else:
                    options_dict = {var_name : [] for var_name in [def_elem["var_name"] for def_elem in module["defined_vars"]]}
                    current_config = {var_name : "" for var_name in [def_elem["var_name"] for def_elem in module["defined_vars"]]}
                    logger.info(f"The defined variables are {module['defined_vars']}")
                    logger.info(f"The options for conflict resolution are: {options_dict}")
                    options_dict = self._recursive_variable_retr_opt(new_module, new_module["defined_vars"][0], new_module["defined_vars"], state_backup_v, options_dict, current_config)
                    logger.info(f"Currently, the variable registry is: {self.variable_registry}")
                    logger.info(f"The final options for conflict resolution are: {options_dict}")
                    
                    final_module = copy.deepcopy(new_module)
                    
                    for var_name, opts in options_dict.items():
                        # Find metadata (label) from ORIGINAL module, because it's stable
                        current_var_in_module = copy.deepcopy(var_name)
                        def_elem = None
                        for d in module.get("defined_vars", []):
                            if d.get("var_name") == var_name:
                                def_elem = d
                                break
                        if def_elem is None:
                            raise Exception(f"Defined variable '{var_name}' not found in module during conflict resolution.")

                        var_label = def_elem["var_label"]
                        if len(opts) == 0:
                            logger.error(f"No valid options found for variable '{var_name}' during conflict resolution.")
                            raise Exception(f"No valid options found for variable '{var_name}' during conflict resolution.")
                        if len(opts) == 1:
                            chosen_var = opts[0]
                        else:
                            working_query = self._parse_for_llm(new_module, def_elem)
                            count = self.variable_registry[var_name]["count"]
                            options = "\n".join([f"- Option {i}: '{opt}'" for i, opt in enumerate(opts)])
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
                                
                            llm_answer = await tool_sampling_request(system_prompt, pattern_intent, log_callback=log_sampling, caller_tool_name="_process_variables")
                            try:
                                match = re.search(r'\d+', llm_answer)
                                if match:
                                    index = int(match.group())
                                    # Use existing variable
                                    chosen_var = opts[index]
                                    logger.info(f"The options were: {opts}, LLM chose index: {index} corresponding to variable '{chosen_var}'")
                                else:
                                    # DEFAULTING
                                    index = len(opts)
                                    # Rename + add to registry
                                    chosen_var = f"{var_name}_{count}"
                                    logger.warning(f"LLM response did not contain a valid index. Defaulting to renaming with '{chosen_var}'")
                            except (ValueError, IndexError):
                                logger.error(f"LLM returned invalid index '{llm_answer}' for variable conflict resolution.")
                                # Default to renaming
                                chosen_var = f"{var_name}_{count}"

                        logger.info(
                            f"Applying resolution for '{var_name}' ({var_label}): "
                            f"current='{current_var_in_module}' -> chosen='{chosen_var}'"
                        )

                        self._modify_var(final_module, current_var_in_module, chosen_var)
                        if chosen_var not in self.variable_registry:
                            self._update_variable_counter(var_label)
                            self.variable_registry[chosen_var] = {
                                "var_label": var_label,
                                "count": 1
                            }
                        else:
                            self._update_variable_counter(var_label)
            else:
                final_module = new_module
        logger.info(f"Final module after variable processing: {final_module['triples'] if 'triples' in final_module else 'No triples'}")     
        return final_module
    
    def get_variable_uri(self, var_name: str) -> Optional[str]:
        """Retrieve variable info from the registry."""
        if var_name in self.variable_registry.keys():
            return self.variable_registry[var_name]["var_label"]
        return None

    def get_varName_from_uri(self, var_uri: str) -> Optional[str]:
        for var, val in self.variable_registry.items():
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
            raise Exception(f"Variable {var_name} not found in variable registry")
        
        if aggregator:
            aggregator = aggregator.upper()
            if aggregator not in ["COUNT", "SUM", "AVG", "MAX", "MIN", "SAMPLE"]:
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
        query_str = self.to_string(for_execution=True)
        res = execute_sparql_query(query_str, limit=1, timeout=15)
        
        if not res["success"]:
            logger.warning(f"Dry Run Failed: Query execution error: {res.get('error')}")
            raise Exception(f"Dry Run Failed: Query execution error: {res.get('error')}")
        
        if res.get("count", 0) == 0:
            logger.warning("Dry Run Failed: Query returned 0 results.")
            raise Exception("Dry Run Failed: Query returned 0 results. Here the executed_query for debug purposes: \n" + query_str)

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

    def to_string(self, for_execution: bool = False) -> str:
        """
        Compile the internal state into a valid SPARQL query string.
        """
        query_parts = []

        # Build Select string
        select_mod = "DISTINCT " if self.distinct_select else ""
        select_vars_str = []

        has_aggregator = any(item.get("aggregator") for item in self.select)

        for item in self.select:
            var_name = item["var_name"]
            
            if self.group_by or has_aggregator:
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
                    select_vars_str.append(f"SAMPLE(?{var_name}) as ?{var_name}_sample")
            
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
                    if for_execution and t.get("obj", {}).get("type") == "var":
                        obj_name = t["obj"]["var_name"]
                        if var_counts.get(obj_name, 0) <= 1:
                            logger.info(f"Dead code elimination: Skipping triple {t} because variable {obj_name} is used only once.")
                            continue

                    s_str = self._format_term(t.get("subj"), for_execution=for_execution)
                    p_str = self._format_term(t.get("pred"), for_execution=for_execution)
                    o_str = self._format_term(t.get("obj"), for_execution=for_execution)
                    
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

    def _format_term(self, term: Dict[str, Any], for_execution: bool = False) -> str:
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
            
            # Handle List of URIs (VALUES expansion)
            if isinstance(val, list):
                if not for_execution:
                    # Display/LLM mode: Return only one representative URI
                    chosen = val[0]
                    if chosen.startswith("http"):
                        return f"<{chosen}>"
                    return chosen

                # Execution mode: Format all URIs in the list
                formatted_uris = []
                for v in val:
                    if v.startswith("http"):
                        formatted_uris.append(f"<{v}>")
                    else:
                        formatted_uris.append(v)
                return " ".join(formatted_uris)

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