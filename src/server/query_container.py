from typing import Any, Optional, List, Dict
import logging

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
    
    def __init__(self, query_id: str):
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
        self.question: str = ""
        
        # Variable dependency tracking
        # Map of var_name -> { "uri": str, "count": int }
        self.variable_registry: Dict[str, Dict[str, Any]] = {}

    # ----------------------
    # MODULE MANAGEMENT
    # ----------------------
    def add_module(self, module: Dict[str, Any]) -> None:
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
            processed_module = self._process_variables(module)
            
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

    def _process_variables(self, module: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle variable naming conventions and collision resolution.
        
        If a variable is new (in defined_vars) and already exists in registry,
        rename it (e.g., ?var -> ?var_1).
        If a variable is required (in required_vars), ensure it matches existing.
        """
        # This is a high-level skeleton. 
        # In a full implementation, we would iterate through 'defined_vars',
        # check against self.variable_registry, and rewrite 'triples' with new names.
        
        # For now, we mock the logic:
        new_module = module.copy()

        #TODO: implement internal checks
        #TODO: handle different types of modules (e.g., filter_st, where)
                    
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
            query_parts.append("  # Filters")
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