import logging
import requests
import re
from typing import Any, Optional, Dict, List, Callable
from server.tool_sampling import tool_sampling_request
from server.config import (
    SPARQL_ENDPOINT,
    REQUEST_TIMEOUT,
    PREFIXES
)

logger = logging.getLogger("doremus-mcp")

# Helper: expand prefixed URI to full URI
def expand_prefixed_uri(uri: str) -> str:
    if uri.startswith("<") and uri.endswith(">"):
        uri = uri[1:-1]
    if ":" in uri:
        prefix, local = uri.split(":", 1)
        if prefix in PREFIXES:
            return f"{PREFIXES[prefix]}{local}"
    return uri

# Helper: contract full URI to prefixed name
def contract_uri(uri: str) -> str:
    for prefix, base in PREFIXES.items():
        if uri.startswith(base):
            return f"{prefix}:{uri[len(base):]}"
    return uri

def contract_uri_restrict(uri: str) -> str:
    """
    Contract a given uri if present in PREFIXES, else return None
    """
    for prefix, base in PREFIXES.items():
        if uri.startswith(base):
            return f"{prefix}:{uri[len(base):]}"
    return None

def get_entity_label(uri: str) -> str:
    """
    Given a uri return the label, if present
    """
    query = f"""
    SELECT ?label
    WHERE {{
        <{uri}> rdfs:label | skos:prefLabel | foaf:name ?label .
        BIND(IF(LANG(?label) = "en", 1, IF(LANG(?label) = "", 2, 3)) AS ?priority)
    }}
    ORDER BY ASC(?priority)
    """
    result = execute_sparql_query(query, 1)
    
    if result["success"] and len(result["results"])>0:
        return result["results"][0].get("label", None)
    else:
        return None
    
def execute_sparql_query(query: str, limit: int = 100, timeout: Optional[int] = None) -> Dict[str, Any]:
    """
    Execute a SPARQL query against the DOREMUS endpoint.
    
    Args:
        query: SPARQL query string
        limit: Maximum number of results (default: 100)
        
    Returns:
        Dictionary containing query results or error information, including the executed query.
    """
    try:
        logger.info(f"Executing SPARQL query with limit {limit}")
        # prefix_lines.join(f"PREFIX {p}: <{uri}>\n" for p, uri in PREFIXES.items())
        # query = prefix_lines + "\n" + query
        if "LIMIT" not in query.upper():
            query = f"{query}\nLIMIT {limit}"
        

        response = requests.get(
            SPARQL_ENDPOINT,
            params={"query": query},
            headers={"Accept": "application/sparql-results+json"},
            timeout=(timeout if timeout is not None else REQUEST_TIMEOUT)
        )
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            # Capture Virtuoso error message from response body
            error_text = response.text
            logger.error(f"SPARQL endpoint error: {error_text}")
            return {
                "success": False,
                "error": f"SPARQL endpoint error: {error_text}",
                "generated_query": query
            }

        data = response.json()
        results = data.get("results", {}).get("bindings", [])
        logger.info(f"Query returned {len(results)} results")
        simplified_results = []
        for binding in results[:limit]:
            simplified = {}
            for key, value in binding.items():
                simplified[key] = value.get("value")
            simplified_results.append(simplified)
        return {
            "success": True,
            "count": len(simplified_results),
            "results": simplified_results,
            "generated_query": query
        }
    except requests.exceptions.Timeout:
        logger.error("Query timeout")
        return {
            "success": False,
            "error": "Query timeout - try simplifying your query or reducing the scope",
            "generated_query": query
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {str(e)}")
        return {
            "success": False,
            "error": f"Request error: {str(e)}",
            "generated_query": query
        }
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {
            "success": False,
            "error": f"Unexpected error: {str(e)}",
            "generated_query": query
        }
    
def find_candidate_entities_utils(
    name: str,
    entity_type: str = "others",
    limit: int = 15
) -> Dict[str, Any]:
    normalized_type = (entity_type or "").strip().lower()
    if normalized_type not in {"artist", "vocabulary", "place", "others"}:
        normalized_type = "others"

    label_predicates = {
        "artist": "foaf:name",
        "vocabulary": "skos:prefLabel",
        "place": "geonames:name",
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
    SELECT ?entity ?label ?type
    WHERE {{
    {{
        SELECT ?entity ?type (SUBSTR(MIN(CONCAT(?priority, ?label_lang)), 2) AS ?label) (MAX(?sc) AS ?maxSc)
        WHERE {{
            ?entity {label_predicate} ?label_lang .
            ?entity a ?type .
            ?label_lang bif:contains "{search_literal}" option (score ?sc) .

            BIND(IF(LANG(?label_lang) = "en", "1", IF(LANG(?label_lang) = "", "2", "3")) AS ?priority)
        }}
        GROUP BY ?entity ?type
    }}

    # Calculate the length difference
    BIND(ABS(STRLEN(?label) - STRLEN("{search_literal}")) AS ?lenDiff)

    # We divide the score by the length difference (adding 1 to avoid division by zero)
    # This makes longer strings have a lower relevance.
    BIND((xsd:float(?maxSc) / (xsd:float(?lenDiff) + 1.0)) AS ?hybridScore)
    }}
    ORDER BY DESC(?hybridScore)
    """

    result = execute_sparql_query(query, limit=limit)
    
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
# helper to find equivalent URIs
def find_equivalent_uris(uri: str) -> List[str]:
    """
    Find all equivalent URIs for a given URI using skos:exactMatch, owl:sameAs and their inverses.
    Returns a list of unique URIs including the original one in the first position of the list.
    """
    # Only expand DOREMUS URIs or known namespaces
    if not (uri.startswith("http://") or uri.startswith("https://")):
         return [uri]
    
    # Simple query to get equivalents
    # Path: <uri> (skos:exactMatch|^skos:exactMatch|owl:sameAs|^owl:sameAs)* ?equivalent
    # Warning: * (zero or more) matches the node itself too.
    
    query = f"""
    SELECT DISTINCT ?equivalent
    WHERE {{
       <{uri}> (skos:exactMatch|^skos:exactMatch|owl:sameAs|^owl:sameAs)* ?equivalent .
    }}
    """
    
    result = execute_sparql_query(query, limit=10)
    
    equivalents = [uri]
    seen = {uri}
    
    if result["success"]:
        for binding in result.get("results", []):
            eq_val = binding.get("equivalent")
            if eq_val and eq_val not in seen:
                equivalents.append(eq_val)
                seen.add(eq_val)
    
    return equivalents

# helper that recieves the link to a property and retuns the label version of it
def extract_label(full_uri: str) -> str | None:
    name = re.split(r'[#/]', full_uri)[-1]
    pref = ""
    prefixes = {"doremus": "mus", "iaml": "iaml", "frbroo": "efrbroo", "erlangen": "ecrm", "rdf": "rdf", "rdfs": "rdfs", "skos": "skos", "foaf": "foaf"}
    for prefix in prefixes.keys():
        if prefix in full_uri:
            pref = prefixes[prefix]
            break
    if pref and pref != "efrbroo":
        return f"{pref}:{name}"
    else:
        return name

# helper that converts a name to a variable name
def convert_to_variable_name(name: str) -> str:
    label = name.split(":")[-1]
    # Use camel case for variable names
    parts = re.split(r'[_\s-]+', label)
    if len(parts) == 1:
        return label.lower()
    camel_case_name = parts[1].lower()
    if len(parts) > 2:
        for part in parts[2:]:
            camel_case_name += part.capitalize()
    return camel_case_name

# helper to remove the redundant paths
def remove_redundant_paths(paths: List[List[tuple]]) -> List[List[tuple]]:
    """
    Removes redundant paths by ensuring each path is unique based on var_name for nodes and var_label for edges.
    
    Args:
        paths (List[List[tuple]]): A list of paths, where each path is a list of (var_name, var_label) tuples.
    
    Returns:
        List[List[tuple]]: A list of unique paths in the same format as the input.
    """
    unique_paths = set()  # Use a set to store normalized paths
    result = []  # Store the final list of unique paths

    for path in paths:
        # Normalize the path by extracting var_name for nodes and var_label for edges
        normalized_path = tuple((var_name, var_label) for var_name, var_label in path)
        
        # Check if the normalized path is already in the set
        if normalized_path not in unique_paths:
            unique_paths.add(normalized_path)  # Add to the set
            result.append(path)  # Add the original path to the result

    return result

def validate_doremus_uri(uri: str) -> bool:
    """
    Validates a DOREMUS URI by checking if it returns a hallucination error message.
    Uses POST with h=1 to bypass the confirmation page.
    """
    # Optimization: Only check DOREMUS URIs
    if not uri.startswith("http://data.doremus.org") and not uri.startswith("https://data.doremus.org"):
        return True

    # Upgrade to HTTPS to match the server and avoid 301 redirects dropping POST data
    if uri.startswith("http://"):
        uri = uri.replace("http://", "https://", 1)

    try:
        # Use POST with h=1 to bypass confirmation page
        response = requests.post(
            uri, 
            data={'h': '1'}, 
            headers={"Accept": "text/html"}, 
            timeout=5
        )
        
        # Check for the specific error message
        if "No further information is available." in response.text:
            logger.warning(f"Hallucinated URI detected: {uri}")
            return False
            
        return True
        
    except Exception as e:
        # Fail open on network errors to avoid blocking valid queries due to connectivity issues
        logger.warning(f"Could not validate URI {uri}: {e}")
        return True

async def resolve_entity_uri(name: str, entity_type: str, question: str = "", log_callback: Optional[Callable[[Dict[str, Any]], None]] = None) -> Optional[str]:
    """
    Helper to resolve a name to a URI using internal tools with LLM sampling.
    Returns the most relevant matching URI or None.
    """
    
    # Check if name is already an uri
    if name.startswith("http:") or name.startswith("https:"):
        return name
    
    try:
        result = find_candidate_entities_utils(name, entity_type)

        if result.get("matches_found", 0) > 0:
            entities = result["entities"]
            
            # If only one match, return it directly
            if len(entities) == 1:
                return entities[0].get("entity")
            
            # Multiple matches: use sampling to choose best one
            entity_options_text = "\n".join([
                f"{i}. {entity.get('label', 'N/A')} ({entity.get('entity', 'N/A')}) ({entity.get('type', 'N/A')})"
                for i, entity in enumerate(entities)
            ])
            
            # Add REGEX fallback option as the last choice
            regex_option_index = len(entities)
            entity_options_text += f"\n{regex_option_index}. None of the above - use REGEX pattern matching instead"
            
            system_prompt = f"""You are an expert in entity resolution for the DOREMUS music knowledge base.
Choose the most semantically relevant entity that matches the user's query intent.
If none of the specific entities match well, choose the REGEX option to use pattern matching instead.
Choose REGEX when the user is not asking about a specific entity (like an exact work or concert) but about a group of entities (like a set of works)."""
            
            pattern_intent = f"""Which of these entities best represents '{name}' (type: {entity_type})?
{f"Given the question: '{question}'" if question else ""}

The options available are:
{entity_options_text}

Return only the number (index) of the best match."""
            
            # Send Sampling request to LLM
            llm_answer = await tool_sampling_request(system_prompt, pattern_intent, log_callback=log_callback, caller_tool_name="resolve_entity_uri")
            
            try:
                # Extract the number
                match = re.search(r'\d+', llm_answer)
                if match:
                    index = int(match.group())
                    # Check if LLM chose the REGEX option
                    if index == regex_option_index:
                        logger.info(f"LLM chose REGEX fallback for '{name}'")
                        return None
                    elif 0 <= index < len(entities):
                        return entities[index].get("entity")
                # Fallback to first if invalid index
                return entities[0].get("entity")
            except (IndexError, ValueError):
                # Fallback to first on error
                return entities[0].get("entity")
            
    except Exception as e:
        logger.warning(f"Failed to resolve entity {name}: {e}")
    
    return None

def get_quantity_property(entity_uri: str, graph: Dict) -> Optional[str]:
    """Helper that finds the property to filter based on number of entities."""
    for node, edges in graph.items():
        if node == entity_uri:
            for pred, _ in edges:
                if "quantity" in pred.lower():
                    return pred
    return None
