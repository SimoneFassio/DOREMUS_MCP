from typing import Any, Optional, List, Dict
import logging
import re
from fastmcp import Context
from fastmcp.server.dependencies import get_context
from src.server.tool_sampling import tool_sampling_request

logger = logging.getLogger("doremus-mcp")

# ----------------------
# ELEMENT FORMATTERS
# ----------------------

def create_triple_element(var_name: str, var_label: str, vtype: str) -> Dict[str, Any]:
    if vtype not in ["var", "uri", "literal"]:
        raise ValueError(f"Invalid type '{vtype}' for query element. Must be 'var', 'uri', or 'literal'.")
    return {"var_name": var_name, "var_label": var_label, "type": vtype}
    
def create_select_element(var_name: str, var_label: str, is_sample: bool = False) -> Dict[str, Any]:
    return {"var_name": var_name, "var_label": var_label, "is_sample": is_sample}

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
        
        # Select: List of dicts e.g., {"var_name": "title", "var_label": "", "is_sample": True}
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
        self.limit: int = 50

        # Metadata
        self.question: str = question
        
        # Variable dependency tracking
        # Map of var_name -> { "uri": str, "count": int }
        self.variable_registry: Dict[str, Dict[str, Any]] = {}
        self.track_dep: bool = True

    # ----------------------
    # MODULE MANAGEMENT
    # ----------------------
    async def add_module(self, module: Dict[str, Any]) -> None:
        """
        Add a query module to the container after validation and connectivity checks.
        
        Args:
            module: Dictionary containing module definition.
                {
                    "id": str,
                    "type": str, # e.g., "filter", "pattern"
                    "scope": str, # e.g., "main", "optional" (Placeholder for future)
                    "triples": List[Dict[str, Any]], Structured triples {"subj":{"var_name": str, "var_label": str}, "pred":{...}, "obj":{...}}
                    "filter_st": List[Dict[str, Any]], Optional filters associated with this module
                    "branches": List[Dict[str, Any]], Optional branching patterns, used for UNIONs or OPTIONALs
                    "required_vars": List[Tuple[str, str]], variables that MUST be already defined
                    "defined_vars": List[Tuple[str, str]], variables that this module defines
                }
        Returns:
            The processed module that was added (useful for logging/debugging).
        """
        # Validate module structure
        if not self._validate_module(module):
            error_msg = f"Invalid module structure for ID: {module.get('id', 'unknown')}"
            logger.error(error_msg)
            return {"error": error_msg}

        if module["scope"] == "main":
            # Process variable renaming (if needed for uniqueness or linking)
            processed_module = await self._process_variables(module)
            
            if processed_module.get("filter_st"):
                for fl in processed_module.get("filter_st", []):
                    self.filter_st.append(fl)
            
            if processed_module.get("triples"):
                self.where.append(processed_module)
        if module["scope"] == "optional":
            #TODO: implement OPTIONAL module handling
            logger.warning("Optional modules not yet implemented.")
            # Placeholder for future implementation of OPTIONAL patterns
        
    

    def set_order_by(self, variables: List[Dict[str, Any]]) -> None:
        self.order_by = variables

    def set_group_by(self, variables: List[Dict[str, Any]]) -> None:
        self.group_by = variables

    def add_having(self, condition: Dict[str, Any]) -> None:
        self.having.append(condition)

    def _validate_module(self, module: Dict[str, Any]) -> bool:
        """Basic validation of module structure."""
        required_keys = ["id", "triples"]
        return all(key in module for key in required_keys)
    
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
            if item.get("is_sample"):
                select_vars_str.append(f"SAMPLE({highlighted_name}) as {highlighted_name}")
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
                        system_prompt = "You are an expert SPARQL query builder assisting in variable naming."
                        try:
                            ctx = get_context()
                        except Exception as e:
                            logger.error(f"Failed to get MCP context for tool sampling: {e}")
                            # Default to renaming
                            new_var_name = f"{var_name}_{count}"
                            self._modify_var(new_module, var_name, new_var_name)
                            self._update_variable_counter(var_label)
                            continue
                        llm_answer = await tool_sampling_request(system_prompt, pattern_intent, ctx)
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

    def set_select(self, variables: List[Dict[str, Any]], distinct: bool = True) -> None:
        """Set the SELECT variables."""
        self.select = variables
        self.distinct_select = distinct
        # Initialize variable registry entries
        for var in variables:
            var_name = var["var_name"]
            var_label = var["var_label"]
            if var_name not in self.variable_registry:
                self.variable_registry[var_name] = {"var_label": var_label, "count": 1}

    def add_select(self, var_elem: Dict[str, Any]) -> None:
        """Add a single variable to the SELECT list."""
        self.select.append(var_elem)

    def set_limit(self, limit: int) -> None:
        self.limit = limit
    
    def get_limit(self) -> int:
        return self.limit

    def set_question(self, question: str) -> None:
        self.question = question

    def get_question(self) -> str:
        return self.question

    def dry_run_test(self) -> bool:
        """
        Basic sanity check for the query structure.
        Connectivity is already checked in add_module, so this checks buildability.
        """
        if not self.select:
            logger.warning("Dry Run Failed: No SELECT variables defined.")
            return False
        
        if not self.where:
            logger.warning("Dry Run Failed: WHERE clause is empty.")
            return False
            
        return True

    def to_string(self) -> str:
        """
        Compile the internal state into a valid SPARQL query string.
        """
        query_parts = []

        # Build Select string
        select_mod = "DISTINCT " if self.distinct_select else ""
        select_vars_str = []

        for item in self.select:
            var_name = item["var_name"]
            if item.get("is_sample"):
                # Example: SAMPLE(?title) as ?title
                select_vars_str.append(f"SAMPLE(?{var_name}) as ?{var_name}")
            else:
                # Example: ?expression
                select_vars_str.append(f"?{var_name}")
        
        query_parts.append(f"SELECT {select_mod}{' '.join(select_vars_str)}")
        
        # Build Where string
        query_parts.append("WHERE {")
        where_body = []
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
                    s_str = self._format_term(t.get("subj"))
                    p_str = self._format_term(t.get("pred"))
                    o_str = self._format_term(t.get("obj"))
                    
                    if p_str == "VALUES":
                        # Special handling for VALUES clause
                        query_parts.append(f"  {p_str} {s_str} {{ {o_str} }}")
                    else:
                        query_parts.append(f"  {s_str} {p_str} {o_str} .")
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
                    filter_parts.append(f"{args_str})")
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
            h_vars = [f"?{v['var_name']}" for v in self.having]
            query_parts.append(f"HAVING ({' && '.join(h_vars)})")

        # Build Order By
        if self.order_by:
            o_vars = [f"?{v['var_name']}" for v in self.order_by]
            query_parts.append(f"ORDER BY {' '.join(o_vars)}")
        
        # Build Limit
        query_parts.append(f"LIMIT {self.limit}")
        
        return "\n".join(query_parts)

    def _format_term(self, term: Dict[str, Any]) -> str:
        """
        Helper to format a Subject/Predicate/Object dictionary into a SPARQL string.
        
        Expected term structure:
        { "var_name": str, "var_label": "uri", type: "var"|"uri"|"literal"}
        type: "var", "uri", "literal"
        """
        if not term:
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