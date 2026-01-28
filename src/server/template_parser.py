"""
Template Parser for Query Builder

Parses template files (.rq) to extract:
- Template name and base variable
- Core triples for build_query
- Filter definitions with their triples
"""

import os
import re
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger("doremus-mcp")

# Import for KG queries - delayed to avoid circular imports
def get_sparql_executor():
    """Get SPARQL executor lazily to avoid circular imports."""
    from server.utils import execute_sparql_query
    return execute_sparql_query

from server.config_loader import PREFIXES, CONFIG_DIR

def contract_uri_simple(uri: str) -> str:
    """Contract a full URI to a prefixed one using server.config.PREFIXES."""
    if uri is None:
        return None
    if uri == "a":
        return "a"
    if not uri or not uri.startswith("http"):
        return uri
        
    for prefix, base in PREFIXES.items():
        if uri.startswith(base):
            return f"{prefix}:{uri[len(base):]}"
    return uri

# Directory containing template files
TEMPLATES_DIR = CONFIG_DIR / "templates"


@dataclass
class FilterDefinition:
    """Represents a single filter definition from a template."""
    name: str
    values_var: Optional[str]  # Variable for VALUES clause (can be None)
    regex_var: Optional[str]   # Variable for REGEX clause (can be None)
    entity_type: str           # artist, vocabulary, place, others, literal
    triples: List[str]         # Raw triple strings


@dataclass
class TemplateSelectVariable:
    """Represents a variable in the SELECT clause."""
    name: str          # The output variable name (alias if AS is used)
    aggregator: Optional[str] = None # e.g., "SAMPLE", "COUNT"


@dataclass
class TemplateDefinition:
    """Represents a parsed template."""
    name: str
    base_variable: str
    base_class: str
    core_triples: List[str]
    filters: Dict[str, FilterDefinition] = field(default_factory=dict)
    var_classes: Dict[str, str] = field(default_factory=dict)  # Maps variable names to their classes
    default_select_vars: List[TemplateSelectVariable] = field(default_factory=list) # Variables to select by default


class TemplateParseError(Exception):
    """Raised when template parsing fails."""
    pass


class TemplateValidationError(Exception):
    """Raised when template validation fails."""
    pass


def parse_filter_header(header: str) -> Tuple[str, Optional[str], Optional[str], str]:
    """
    Parse a filter header line.
    
    Format: # filter: "name":"values_var":"regex_var":"entity_type"
    
    Returns: (name, values_var, regex_var, entity_type)
    """
    # Remove '# filter:' prefix and strip whitespace
    content = header.replace("# filter:", "").strip()
    
    # Parse the quoted fields
    pattern = r'"([^"]*)":"([^"]*)":"([^"]*)":"([^"]*)"'
    match = re.match(pattern, content)
    
    if not match:
        raise TemplateParseError(f"Invalid filter header format: {header}")
    
    name, values_var, regex_var, entity_type = match.groups()
    
    # Convert empty strings to None
    values_var = values_var if values_var else None
    regex_var = regex_var if regex_var else None
    
    # Validate entity_type
    valid_types = {"artist", "vocabulary", "place", "others", "literal"}
    if entity_type not in valid_types:
        raise TemplateValidationError(
            f"Invalid entity_type '{entity_type}' in filter '{name}'. "
            f"Must be one of: {valid_types}"
        )
    
    # Validate that at least one var is defined
    if not values_var and not regex_var:
        raise TemplateValidationError(
            f"Filter '{name}' must define at least one of values_var or regex_var"
        )
    
    return name, values_var, regex_var, entity_type


def extract_base_class(triples: List[str]) -> Optional[str]:
    """Extract the base class from core triples (looks for 'a <class>' pattern)."""
    for triple in triples:
        # Look for pattern: ?var a <class> or ?var a prefix:Class
        match = re.search(r'\?\w+\s+a\s+(\S+)', triple)
        if match:
            return match.group(1).rstrip(' .')
    return None

# SPARQL queries for class resolution (configurable for different KGs)
CLASS_FROM_PREDICATE_RANGE_QUERY = """
SELECT DISTINCT ?class
WHERE {{
    {predicate} rdfs:range ?class .
}}
LIMIT 1
"""

CLASS_FROM_SAMPLING_RANGE_QUERY = """
SELECT DISTINCT ?class
WHERE {{
    ?instance a ?class .
    ?subject {predicate} ?instance .
}}
ORDER BY ?class
LIMIT 1
"""

CLASS_FROM_PREDICATE_DOMAIN_QUERY = """
SELECT DISTINCT ?class
WHERE {{
    {predicate} rdfs:domain ?class .
}}
LIMIT 1
"""

CLASS_FROM_SAMPLING_DOMAIN_QUERY = """
SELECT DISTINCT ?class
WHERE {{
    ?instance {predicate} ?object .
    ?instance a ?class .
}}
ORDER BY ?class
LIMIT 1
"""


def resolve_variable_class(predicate: str, position: str = "object") -> Optional[str]:
    """
    Resolve a variable's class by querying the KG.
    position: "object" (use rdfs:range) or "subject" (use rdfs:domain)
    """
    execute_sparql = get_sparql_executor()
    
    # Select queries based on position
    if position == "object":
        range_query = CLASS_FROM_PREDICATE_RANGE_QUERY
        sample_query = CLASS_FROM_SAMPLING_RANGE_QUERY
    else:
        range_query = CLASS_FROM_PREDICATE_DOMAIN_QUERY
        sample_query = CLASS_FROM_SAMPLING_DOMAIN_QUERY
    
    # Try schema lookup first (range or domain)
    try:
        query = range_query.format(predicate=predicate)
        result = execute_sparql(query, limit=1)
        if result.get("success") and result.get("results"):
            return result["results"][0].get("class")
    except Exception:
        pass
    
    # Fall back to sampling
    try:
        query = sample_query.format(predicate=predicate)
        result = execute_sparql(query, limit=1)
        if result.get("success") and result.get("results"):
            return result["results"][0].get("class")
    except Exception:
        pass
    
    return None


def extract_var_classes_from_triples(all_triples: List[str]) -> Dict[str, str]:
    """
    Extract variable-to-class mappings from triples.
    
    1. Extract classes from explicit 'a' predicates
    2. Query KG for remaining variables using their predicates (range and domain)
    """
    var_classes: Dict[str, str] = {}
    all_variables: set = set()
    
    # Store predicates where variable is OBJECT (variable class is defined by predicate RANGE)
    var_predicates_range: Dict[str, List[str]] = {}
    # Store predicates where variable is SUBJECT (variable class is defined by predicate DOMAIN)
    var_predicates_domain: Dict[str, List[str]] = {}
    
    for triple in all_triples:
        triple = triple.strip().rstrip('.')
        if not triple:
            continue
        
        parts = triple.split()
        if len(parts) < 3:
            continue
        
        subj, pred = parts[0], parts[1]
        obj = ' '.join(parts[2:])
        
        # Handle Subject
        if subj.startswith('?'):
            s_name = subj[1:]
            all_variables.add(s_name)
            if pred == 'a':
                var_classes[s_name] = contract_uri_simple(obj.strip())
            else:
                if s_name not in var_predicates_domain:
                    var_predicates_domain[s_name] = []
                var_predicates_domain[s_name].append(pred) # Class of s_name is DOMAIN of pred

        # Handle Object
        if obj.startswith('?'):
            o_name = obj[1:]
            all_variables.add(o_name)
            if pred not in ['a', 'rdf:type']:
                if o_name not in var_predicates_range:
                    var_predicates_range[o_name] = []
                var_predicates_range[o_name] = [] # Fix initialization bug
                var_predicates_range[o_name].append(pred) # Class of o_name is RANGE of pred
    
    # Query KG for variables without explicit type
    for var_name in all_variables - set(var_classes.keys()):
        found = False
        
        # Try range (variable appears as object)
        if var_name in var_predicates_range:
            for predicate in var_predicates_range[var_name]:
                resolved = resolve_variable_class(predicate, position="object")
                if resolved:
                    var_classes[var_name] = contract_uri_simple(resolved)
                    found = True
                    break
        
        if found:
            continue
            
        # Try domain (variable appears as subject)
        if var_name in var_predicates_domain:
            for predicate in var_predicates_domain[var_name]:
                resolved = resolve_variable_class(predicate, position="subject")
                if resolved:
                    var_classes[var_name] = contract_uri_simple(resolved)
                    break
    
    return var_classes


def parse_triples(content: str) -> List[str]:
    """Parse triple lines from content, handling multi-line triples and skipping headers."""
    lines = content.strip().split('\n')
    triples = []
    current_triple = ""
    
    for line in lines:
        line = line.strip()
        # Skip comments and empty lines
        if not line or line.startswith('#'):
            continue
            
        # Skip SPARQL structure lines that aren't triples
        if (line.upper().startswith('SELECT ') or 
            line.upper().startswith('WHERE') or 
            line == '}'):
            continue
            
        current_triple += " " + line
        if line.endswith('.'):
            triples.append(current_triple.strip())
            current_triple = ""
            
    return triples


def parse_triple_string(triple_str: str, base_variable: str, suffix: str) -> Dict[str, Any]:
    """
    Parse a raw SPARQL triple string into a module triple dictionary.
    
    Args:
        triple_str: Raw triple string like "?expression a efrbroo:F22_Self-Contained_Expression ."
        base_variable: The template's base variable name (for renaming)
        suffix: Suffix to append to variables (e.g., "_work")
    
    Returns:
        Dictionary with subj, pred, obj keys for add_module
    """
    # Clean the string
    triple_str = triple_str.strip().rstrip('.')
    
    # Split into parts (handle property paths with /)
    parts = []
    current = ""
    in_uri = False
    
    for char in triple_str:
        if char == '<':
            in_uri = True
        elif char == '>':
            in_uri = False
        
        if char == ' ' and not in_uri and current:
            parts.append(current)
            current = ""
        else:
            current += char
    
    if current:
        parts.append(current)
    
    if len(parts) < 3:
        raise ValueError(f"Invalid triple format: {triple_str}")
    
    subj_str = parts[0]
    pred_str = parts[1]
    obj_str = ' '.join(parts[2:])  # Handle multi-part objects
    
    def parse_element(elem_str: str) -> Dict[str, Any]:
        """Parse a single element (subject, predicate, or object)."""
        elem_str = elem_str.strip()
        
        # Variable: ?varName
        if elem_str.startswith('?'):
            var_name = elem_str[1:]
            
            # Variable renaming logic:
            # - If var_name is the template's base_variable, rename to new_base_variable
            # - All other variables get the suffix appended
            if var_name == base_variable:
                # This is the template's base variable - will be renamed to new_base_variable by caller
                pass  # Don't add suffix, caller will handle renaming
            elif suffix:
                # All other variables get the suffix
                var_name = f"{var_name}{suffix}"
            
            return {"var_name": var_name, "var_label": "", "type": "var"}
        
        # Full URI: <http://...>
        elif elem_str.startswith('<') and elem_str.endswith('>'):
            uri = elem_str[1:-1]
            return {"var_name": uri.split('/')[-1], "var_label": uri, "type": "uri"}
        
        # Prefixed URI: prefix:LocalName
        elif ':' in elem_str and not elem_str.startswith('"'):
            return {"var_name": elem_str, "var_label": elem_str, "type": "uri"}
        
        # Literal: "value"
        elif elem_str.startswith('"'):
            return {"var_name": elem_str.strip('"'), "var_label": "", "type": "literal"}
        
        # Special predicate 'a' (rdf:type)
        elif elem_str == 'a':
            return {"var_name": "a", "var_label": "a", "type": "uri"}
        
        else:
            # Assume prefixed URI
            return {"var_name": elem_str, "var_label": elem_str, "type": "uri"}
    
    return {
        "subj": parse_element(subj_str),
        "pred": parse_element(pred_str),
        "obj": parse_element(obj_str)
    }


def convert_triples_to_module(
    triples: List[str], 
    module_id: str,
    base_variable: str,
    new_base_variable: str,
    var_classes: Dict[str, str] = None
) -> Dict[str, Any]:
    """
    Convert a list of triple strings to a module dictionary.
    
    Args:
        triples: List of raw SPARQL triple strings
        module_id: Unique ID for the module
        base_variable: Original base variable from template
        new_base_variable: Variable to rename base_variable to
        var_classes: Dict mapping variable names to their class URIs
    
    Returns:
        Module dictionary ready for add_module()
    """
    suffix = f"_{new_base_variable}" if new_base_variable != base_variable else ""
    if var_classes is None:
        var_classes = {}
    
    parsed_triples = []
    for triple_str in triples:
        if not triple_str.strip():
            continue
        try:
            parsed = parse_triple_string(triple_str, base_variable, suffix)
            
            # Populate var_label from var_classes for variables
            for part in ["subj", "obj"]:
                if parsed[part]["type"] == "var":
                    # Strip suffix to look up original variable name
                    var_name = parsed[part]["var_name"]
                    original_var_name = var_name.rstrip(suffix) if suffix and var_name.endswith(suffix) else var_name
                    
                    # Look up class from var_classes
                    if original_var_name in var_classes:
                        parsed[part]["var_label"] = var_classes[original_var_name]
            
            # Rename base_variable to new_base_variable
            for part in ["subj", "obj"]:
                if parsed[part]["type"] == "var":
                    if parsed[part]["var_name"] == base_variable or parsed[part]["var_name"] == f"{base_variable}{suffix}":
                        parsed[part]["var_name"] = new_base_variable
            
            parsed_triples.append(parsed)
        except ValueError as e:
            logger.warning(f"Skipping invalid triple: {e}")
    
    return {
        "id": module_id,
        "type": "query_builder",
        "scope": "main",
        "triples": parsed_triples
    }


def parse_template_file(filepath: str) -> TemplateDefinition:
    """
    Parse a template file and return a TemplateDefinition.
    
    Args:
        filepath: Path to the .rq template file
        
    Returns:
        TemplateDefinition with all parsed components
    """
    path = Path(filepath)
    if not path.exists():
        raise TemplateParseError(f"Template file not found: {filepath}")
        
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
        
    # Extract template name
    name_match = re.search(r'# Template: (.+)', content)
    template_name = name_match.group(1).strip() if name_match else path.stem
    
    # Extract ALL variables from SELECT
    select_match = re.search(r'SELECT DISTINCT(.*?)(?=\s+WHERE)', content, re.IGNORECASE | re.DOTALL)
    if not select_match:
         raise TemplateParseError(f"Could not find SELECT DISTINCT clause in {path.name}")
    
    select_content = select_match.group(1).strip()
    
    # Parse select variables, handling aggregators
    # Pattern to match: 
    # 1. Simple var: ?var
    # 2. Aliased agg: SAMPLE(?var) AS ?alias
    # 3. Aliased simple (rare but possible): ?var AS ?alias
    
    # We iterate over the content to extract tokens
    default_select_vars = []
    
    # Clean up newlines
    select_content = select_content.replace('\n', ' ')
    
    # Regex for tokens: 
    # Group 1: Aggregator (SAMPLE, COUNT, etc)
    # Group 2: Inner variable (?var)
    # Group 3: Alias (?alias) - if AS is present
    # Group 4: Simple variable (?var) - if no aggregator
    
    # Regex explanation:
    # (?:(\w+)\(\s*\?(\w+)\s*\)\s+AS\s+\?(\w+))  -> Matches AGG(?var) AS ?alias
    # |                                          -> OR
    # \?(\w+)                                    -> Matches ?var (simple)
    
    token_pattern = r'(?:(\w+)\(\s*\?(\w+)\s*\)\s+AS\s+\?(\w+))|(?:\?(\w+))'
    
    matches = re.finditer(token_pattern, select_content, re.IGNORECASE)
    
    base_variable = None
    
    for match in matches:
        agg, inner_var, alias, simple_var = match.groups()
        
        if simple_var:
            # Simple variable case: ?var
            default_select_vars.append(TemplateSelectVariable(name=simple_var))
            if base_variable is None:
                base_variable = simple_var
                
        elif agg and inner_var and alias:
            # Aggregator case: SAMPLE(?var) AS ?alias
            # Note: We use the ALIAS as the variable name in our system, 
            # but we record the aggregator. 
            # The logic in build_query will re-apply the aggregator.
            default_select_vars.append(TemplateSelectVariable(name=alias, aggregator=agg.upper()))
            if base_variable is None:
                base_variable = alias # Should ideally be the first simple var, but fallback
                
        # Skip purely aliased vars without aggregator for now as they are rare in our templates
        # (e.g. ?x AS ?y) - our regex doesn't catch them explicitly but ?x catches the first part
        
    if not default_select_vars:
        raise TemplateParseError(f"No variables found in SELECT clause in {path.name}")
        
    if base_variable is None and default_select_vars:
        base_variable = default_select_vars[0].name
    
    # Split content into sections
    sections = re.split(r'(# filter: ".*")', content)
    
    # First section is core triples (after removing header)
    core_section = sections[0]
    
    # Parse core triples (skip headers)
    core_triples = parse_triples(core_section)
    
    # Remove SELECT/WHERE clauses if captured in triples (basic cleanup)
    core_triples = [t for t in core_triples if not t.upper().startswith('SELECT') and not t.upper().startswith('WHERE')]
    
    base_class = extract_base_class(core_triples)
    if not base_class:
        raise TemplateParseError("Could not find base class in core triples")
    
    # Parse filters
    filters = {}
    for i in range(1, len(sections), 2):
        header = sections[i]
        body = sections[i+1]
        
        name, values_var, regex_var, entity_type = parse_filter_header(header)
        filter_triples = parse_triples(body)
        
        filters[name] = FilterDefinition(
            name=name,
            values_var=values_var,
            regex_var=regex_var,
            entity_type=entity_type,
            triples=filter_triples
        )
    
    # Extract variable classes from all triples (core + all filters)
    all_triples = core_triples.copy()
    for filter_def in filters.values():
        all_triples.extend(filter_def.triples)
    
    var_classes = extract_var_classes_from_triples(all_triples)
    logger.debug(f"Extracted var_classes for template {template_name}: {var_classes}")
    
    return TemplateDefinition(
        name=template_name,
        base_variable=base_variable,
        base_class=base_class,
        core_triples=core_triples,
        filters=filters,
        var_classes=var_classes,
        default_select_vars=default_select_vars
    )


def load_all_templates() -> Dict[str, TemplateDefinition]:
    """Load all templates from the templates directory."""
    templates = {}
    
    if not TEMPLATES_DIR.exists():
        logger.warning(f"Templates directory not found: {TEMPLATES_DIR}")
        return templates
    
    for filepath in TEMPLATES_DIR.glob("*.rq"):
        try:
            template = parse_template_file(str(filepath))
            templates[template.name] = template
            logger.info(f"Loaded template: {template.name} with {len(template.filters)} filters")
        except (TemplateParseError, TemplateValidationError) as e:
            logger.error(f"Failed to load template {filepath}: {e}")
    
    return templates


def get_template(name: str) -> TemplateDefinition:
    """Get a specific template by name."""
    filepath = TEMPLATES_DIR / f"{name}.rq"
    
    if not filepath.exists():
        raise TemplateParseError(f"Template not found: {name}")
    
    return parse_template_file(str(filepath))


def list_available_templates() -> List[str]:
    """List all available template names."""
    if not TEMPLATES_DIR.exists():
        return []
    return [f.stem for f in TEMPLATES_DIR.glob("*.rq")]


# Preload templates at module import
_templates_cache: Dict[str, TemplateDefinition] = {}


def get_cached_template(name: str) -> TemplateDefinition:
    """Get a template from cache, loading if necessary."""
    global _templates_cache
    
    if not _templates_cache:
        logger.info(f"Templates not cached, loading from {TEMPLATES_DIR}")
        _templates_cache = load_all_templates()
    
    # helper: strip .rq extension if present
    if name.endswith('.rq'):
        name = name[:-3]
        
    if name not in _templates_cache:
        # Try finding it with case-insensitivity
        for cached_name in _templates_cache:
            if cached_name.lower() == name.lower():
                return _templates_cache[cached_name]
                
        raise TemplateParseError(f"Template not found: {name}")
    
    return _templates_cache[name]


def initialize_templates():
    """
    Initialize templates cache and validate variable classes.
    Logs warning for any template variable that lacks a resolved class.
    """
    global _templates_cache
    _templates_cache = load_all_templates()
    
    for tmpl_name, tmpl_def in _templates_cache.items():
        # Check if all default selected variables have a class
        missing_classes = []
        for var_obj in tmpl_def.default_select_vars:
            var = var_obj.name
            if var not in tmpl_def.var_classes:
                missing_classes.append(var)
        
        if missing_classes:
            logger.warning(
                f"Template '{tmpl_name}' initialized with missing class definitions "
                f"for variables: {missing_classes}. "
                "These variables may not have metadata in Generated Queries."
            )
        else:
             logger.info(f"Template '{tmpl_name}' validated successfully with {len(tmpl_def.default_select_vars)} variables.")
