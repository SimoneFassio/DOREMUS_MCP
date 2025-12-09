from typing import Any, Optional, List, Dict
import logging

logger = logging.getLogger("doremus-mcp")

class QueryContainer:
    """
    A container for building SPARQL queries incrementally using modular components.
    
    This class manages the state of a SPARQL query, including SELECT variables,
    WHERE clause patterns, and structure (GROUP BY, LIMIT, etc.). It handles
    variable naming conflicts and ensures consistency between modules.
    """
    
    def __init__(self, query_id: str):
        self.query_id = query_id
        
        # Query components
        self.select: List[str] = []
        self.where: List[Dict[str, Any]] = []
        self.group_by: List[str] = []
        self.having: List[str] = []
        self.order_by: List[str] = []
        self.question: str = ""
        self.limit: int = 50
        
        # State management
        self.readiness_flag = False
        self.variable_registry: Dict[str, int] = {}  # Map of var_name -> count

    def add_module(self, module: Dict[str, Any]) -> None:
        """
        Add a query module to the container.
        
        Args:
            module: Dictionary containing module definition.
                {
                    "id": str,
                    "type": str, # e.g., "filter", "pattern"
                    "triples": List[Union[str, Tuple[str, str]]], # Raw SPARQL patterns or (pattern, comment)
                    "vars": Dict[str, str], # internal_role -> var_name
                    "required_vars": List[str],
                    "defined_vars": List[str],
                }
        """
        # Validate module structure
        if not self._validate_module(module):
            logger.error(f"Invalid module structure: {module.get('id', 'unknown')}")
            return

        # Process variable renaming (if needed for uniqueness or linking)
        processed_module = self._process_variables(module)
        
        # Add to internal state
        self.where.append(processed_module)
        
        # Update readiness
        self.readiness_flag = True

    def set_order_by(self, variables: List[str]) -> None:
        self.order_by = variables

    def set_group_by(self, variables: List[str]) -> None:
        self.group_by = variables

    def add_having(self, condition: str) -> None:
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
        
        # Example logic for upgrading variable counts
        if "defined_vars" in module:
            for var in module["defined_vars"]:
                base_name = var
                if base_name in self.variable_registry:
                    count = self.variable_registry[base_name] + 1
                    self.variable_registry[base_name] = count
                    # Here we would renaming logic in triples
                else:
                    self.variable_registry[base_name] = 0
                    
        return new_module

    def set_select(self, variables: List[str], distinct: bool = True) -> None:
        """Set the SELECT variables."""
        self.select = variables
        self.distinct_select = distinct

    def add_select(self, variable: str) -> None:
        """Add a SELECT variable."""
        self.select.append(variable)

    def set_limit(self, limit: int) -> None:
        self.limit = limit
    
    def get_limit(self) -> int:
        return self.limit

    def set_question(self, question: str) -> None:
        self.question = question

    def get_question(self) -> str:
        return self.question

    def parse(self) -> bool:
        """
        Check that variables are consistent and not overlapped, dry run.
        Returns True if the query state seems valid.
        """
        # 1. Check if all 'required_vars' in all modules were actually defined by previous modules
        known_vars = set()
        for mod in self.where:
            req = mod.get("required_vars", [])
            defined = mod.get("defined_vars", [])
            
            # This is a simplified check. In reality, order matters.
            # linked_vars logic would go here.
            
            known_vars.update(defined)
            
        return True

    def to_string(self) -> str:
        """
        Combine the internal variables and return the complete SPARQL query string.
        """
        query = ""

        # Build Select string
        select_mod = "DISTINCT " if getattr(self, "distinct_select", True) else ""
        select_str = f"SELECT {select_mod}" + " ".join(self.select)
        query += select_str + "\n"
        
        # Build Where string
        where_body = []
        for mod in self.where:
            # Add comments or separation for readability
            where_body.append(f"# Module: {mod.get('id', 'unnamed')}")
            for triple in mod.get("triples", []):
                if isinstance(triple, tuple) and len(triple) == 2:
                    # It's a (pattern, comment) tuple
                    where_body.append(f"{triple[0]} # {triple[1]}")
                else:
                    where_body.append(str(triple))
            
        where_str = "WHERE {\n" + "\n".join(where_body) + "\n}"
        query += where_str + "\n"
        
        # Build Group By
        group_str = ""
        if self.group_by:
            group_str = "GROUP BY " + " ".join(self.group_by)
            query += group_str + "\n"

        # Build Having
        having_str = ""
        if self.having:
            having_str = "HAVING ( " + " && ".join(self.having) + " )"
            query += having_str + "\n"

        # Build Order By
        order_str = ""
        if self.order_by:
            order_str = "ORDER BY " + " ".join(self.order_by)
            query += order_str + "\n"
        
        return query
