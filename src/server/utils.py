import logging
import requests
from typing import Any, Optional
from src.server.config import (
    SPARQL_ENDPOINT,
    REQUEST_TIMEOUT,
    PREFIXES
)

# Configure logging
logging.basicConfig(level=logging.INFO)
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

def execute_sparql_query(query: str, limit: int = 100) -> dict[str, Any]:
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
        
        # Prepend standard PREFIX declarations
        prefix_lines = "".join(f"PREFIX {p}: <{uri}>\n" for p, uri in PREFIXES.items())
        query = prefix_lines + "\n" + query
        
        if "LIMIT" not in query.upper():
            query = f"{query}\nLIMIT {limit}"
        
        response = requests.get(
            SPARQL_ENDPOINT,
            params={"query": query},
            headers={"Accept": "application/sparql-results+json"},
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        
        data = response.json()
        results = data.get("results", {}).get("bindings", [])
        
        logger.info(f"Query returned {len(results)} results")
        
        # Simplify result structure
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
            "executed_query": query  # Include the executed query even on error
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {str(e)}")
        return {
            "success": False,
            "error": f"Request error: {str(e)}",
            "executed_query": query  # Include the executed query even on error
        }
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {
            "success": False,
            "error": f"Unexpected error: {str(e)}",
            "executed_query": query  # Include the executed query even on error
        }
        

# Helper to sample entities for a given class URI, used in get_kg_structure
def sample_for_class(class_uri: str, sample_limit: int = 5) -> list[tuple[str, str]]:
    class_uri_expanded = expand_prefixed_uri(class_uri)
    q = f"""
    SELECT DISTINCT ?entity ?label
    WHERE {{
        ?entity a <{class_uri_expanded}> .
        OPTIONAL {{ ?entity rdfs:label ?label }}
    }} LIMIT {sample_limit}
    """
    res = execute_sparql_query(q, limit=sample_limit)
    samples = []
    if res.get("success"):
        for r in res.get("results", []):
            ent_uri = r.get("entity", "")
            ent_label = r.get("label", "") or ""
            samples.append((contract_uri(ent_uri), ent_label))
    return samples