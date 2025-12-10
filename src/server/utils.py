import logging
import requests
import re
from typing import Any, Optional, Dict
from src.server.config import (
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
    }}
    """
    result = execute_sparql_query(query, 1)
    
    if result["success"] and len(result["results"])>0:
        return result["results"][0].get("label", None)
    else:
        return None
    
def execute_sparql_query(query: str, limit: int = 100) -> Dict[str, Any]:
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
            timeout=REQUEST_TIMEOUT
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