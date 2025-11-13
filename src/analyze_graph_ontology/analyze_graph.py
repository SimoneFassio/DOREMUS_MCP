import requests
import pandas as pd
import logging
import argparse
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Doremus SPARQL endpoint
SPARQL_ENDPOINT = "https://data.doremus.org/sparql/"

PREFIXES = {
    # "rdf" : "http://www.w3.org/1999/02/22-rdf-syntax-ns#", skip those since are almost everywhere (type)
    # "rdfs": "http://www.w3.org/2000/01/rdf-schema#",          (comment, label), added in the guide
    "mus": "http://data.doremus.org/ontology#",
    "ecrm": "http://erlangen-crm.org/current/",
    "efrbroo": "http://erlangen-crm.org/efrbroo/",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "modsrdf": "http://www.loc.gov/standards/mods/rdf/v1/#",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "geonames": "http://www.geonames.org/ontology#",
    "time": "http://www.w3.org/2006/time#",
    "dbpprop" : "http://dbpedia.org/property/",
    "schema" : "http://schema.org/"
}

def run_sparql_query(query: str, endpoint: str = SPARQL_ENDPOINT) -> list:
    """
    Executes a SPARQL query against the specified endpoint and returns the results.
    """
    headers = {
        "Accept": "application/sparql-results+json"
    }
    params = {
        "query": query,
        "format": "application/sparql-results+json"
    }
    try:
        response = requests.get(endpoint, params=params, headers=headers, timeout=300)
        response.raise_for_status()
        data = response.json()
        return data["results"]["bindings"]
    except requests.exceptions.RequestException as e:
        logging.error(f"SPARQL query failed: {e}")
        return []

def apply_prefix_filter(uri: str, use_prefix: bool) -> str:
    """
    Applies prefix filtering and substitution.
    Returns None if URI should be filtered out, otherwise returns prefixed URI.
    """
    if not use_prefix:
        return uri
    
    for prefix, full_uri in PREFIXES.items():
        if uri.startswith(full_uri):
            return uri.replace(full_uri, f"{prefix}:")
    
    # URI doesn't match any prefix - filter it out
    return None

def get_edge_stats(use_prefix: bool = False):
    """
    Fetches all predicates (edges) and their counts.
    """
    logging.info("Fetching edge statistics...")
    query = """
    SELECT ?p (COUNT(?s) AS ?count)
    WHERE {
      ?s ?p ?o .
    }
    GROUP BY ?p
    ORDER BY DESC(?count)
    """
    results = run_sparql_query(query)
    if not results:
        logging.warning("No edge statistics found.")
        return

    data = []
    for r in results:
        edge = r["p"]["value"]
        count = int(r["count"]["value"])
        
        if use_prefix:
            edge = apply_prefix_filter(edge, use_prefix)
            if edge is None:
                continue
        
        data.append({"edge": edge, "count": count})
    
    df = pd.DataFrame(data)
    df.to_csv("edges_stats.csv", index=False)
    logging.info(f"Saved edge statistics to edges_stats.csv ({len(df)} edges)")

def get_node_type_stats(use_prefix: bool = False):
    """
    Fetches all node types (classes) and their instance counts.
    """
    logging.info("Fetching node type statistics...")
    query = """
    SELECT ?type (COUNT(?s) AS ?count)
    WHERE {
      ?s a ?type .
    }
    GROUP BY ?type
    ORDER BY DESC(?count)
    """
    results = run_sparql_query(query)
    if not results:
        logging.warning("No node type statistics found.")
        return

    data = []
    for r in results:
        node_type = r["type"]["value"]
        count = int(r["count"]["value"])
        
        if use_prefix:
            node_type = apply_prefix_filter(node_type, use_prefix)
            if node_type is None:
                continue
        
        data.append({"node_type": node_type, "count": count})
    
    df = pd.DataFrame(data)
    df.to_csv("node_type_stats.csv", index=False)
    logging.info(f"Saved node type statistics to node_type_stats.csv ({len(df)} node types)")

def get_ontology_structure(
    use_prefix: bool = False, 
    filter_cardinality_edges: int = 0, 
    filter_cardinality_nodes: int = 0,
    literal_mode: int = 1
):
    """
    Fetches the ontology structure by finding unique (source_type, edge, target_type) 
    triplets for each edge and saves them incrementally.
    
    Args:
        use_prefix: Whether to apply prefix filtering
        filter_cardinality_edges: Minimum count for edges to be included
        filter_cardinality_nodes: Minimum count for nodes to be included
        literal_mode: How to handle Literal nodes (0=ignore, 1=as-is, 2=unique)
    """
    logging.info("Fetching ontology structure incrementally...")
    
    # Load pre-computed stats
    try:
        edges_df = pd.read_csv("edges_stats.csv")
        nodes_df = pd.read_csv("node_type_stats.csv")
    except FileNotFoundError as e:
        logging.error(f"Required stats file not found: {e}")
        logging.error("Please run --edges and --nodes first")
        return
    
    # Apply cardinality filters
    if filter_cardinality_edges > 0:
        edges_df = edges_df[edges_df['count'] >= filter_cardinality_edges]
        logging.info(f"Filtered edges to {len(edges_df)} with count >= {filter_cardinality_edges}")
    
    if filter_cardinality_nodes > 0:
        valid_nodes = set(nodes_df[nodes_df['count'] >= filter_cardinality_nodes]['node_type'])
        logging.info(f"Filtered nodes to {len(valid_nodes)} with count >= {filter_cardinality_nodes}")
    else:
        valid_nodes = None
    
    all_edges = edges_df['edge'].tolist()
    
    output_file = "ontology_structure.csv"
    
    # Write header first
    pd.DataFrame(columns=["source_type", "edge", "target_type"]).to_csv(output_file, index=False)
    
    literal_counter = 0

    for edge in all_edges:
        # Convert back to full URI if using prefix
        edge_uri = edge
        if use_prefix:
            for prefix, full_uri in PREFIXES.items():
                if edge.startswith(f"{prefix}:"):
                    edge_uri = edge.replace(f"{prefix}:", full_uri)
                    break
        
        logging.info(f"Querying for edge: {edge}")
        query = f"""
        SELECT DISTINCT ?source_type ?target_type
        WHERE {{
          ?s a ?source_type .
          ?s <{edge_uri}> ?o .
          OPTIONAL {{
            ?o a ?o_type .
          }}
          BIND(IF(ISLITERAL(?o), "Literal", ?o_type) AS ?target_type)
          FILTER(BOUND(?target_type))
        }}
        LIMIT 1000
        """
        results = run_sparql_query(query)
        
        if not results:
            logging.warning(f"No structure found for edge: {edge}")
            continue

        data = []
        for r in results:
            source_type = r.get("source_type", {}).get("value")
            target_type = r.get("target_type", {}).get("value")
            
            if not source_type or not target_type:
                continue
            
            # Handle literal mode
            if target_type == "Literal":
                if literal_mode == 0:
                    continue  # Ignore literals
                elif literal_mode == 2:
                    # Make each literal unique
                    literal_counter += 1
                    target_type = f"Literal_{literal_counter}"
                # else: literal_mode == 1, keep as "Literal"
            
            # Apply prefix filtering
            if use_prefix:
                source_type = apply_prefix_filter(source_type, use_prefix)
                if target_type != "Literal" and not target_type.startswith("Literal_"):
                    target_type = apply_prefix_filter(target_type, use_prefix)
                
                if source_type is None or target_type is None:
                    continue
            
            # Apply node cardinality filter
            if valid_nodes is not None:
                if source_type not in valid_nodes:
                    continue
                if target_type not in valid_nodes and target_type != "Literal" and not target_type.startswith("Literal_"):
                    continue
            
            data.append({
                "source_type": source_type,
                "edge": edge,
                "target_type": target_type,
            })
        
        if data:
            df = pd.DataFrame(data)
            # Append to the CSV file without writing the header again
            df.to_csv(output_file, mode='a', header=False, index=False)
    
    logging.info(f"Saved ontology structure to {output_file}")


def main():
    """
    Main function to run the analysis with argument parsing.
    """
    parser = argparse.ArgumentParser(description="Analyze DOREMUS graph ontology structure")
    
    # Main operation arguments
    parser.add_argument('--nodes', action='store_true', help='Get node type statistics')
    parser.add_argument('--edges', action='store_true', help='Get edge statistics')
    parser.add_argument('--ontology', action='store_true', help='Get ontology structure')
    
    # Modifier arguments
    parser.add_argument('--prefix', action='store_true', help='Apply prefix filtering and substitution')
    parser.add_argument('--filter_cardinality_edges', type=int, default=0, 
                       help='Minimum count for edges (ontology only)')
    parser.add_argument('--filter_cardinality_nodes', type=int, default=0,
                       help='Minimum count for nodes (ontology only)')
    parser.add_argument('--literal', type=int, default=1, choices=[0, 1, 2],
                       help='Literal handling: 0=ignore, 1=as-is, 2=unique (ontology only)')
    
    args = parser.parse_args()
    
    # If no operation specified, run all
    if not (args.nodes or args.edges or args.ontology):
        args.nodes = True
        args.edges = True
        args.ontology = True
    
    if args.nodes:
        get_node_type_stats(use_prefix=args.prefix)
    
    if args.edges:
        get_edge_stats(use_prefix=args.prefix)
    
    if args.ontology:
        get_ontology_structure(
            use_prefix=args.prefix,
            filter_cardinality_edges=args.filter_cardinality_edges,
            filter_cardinality_nodes=args.filter_cardinality_nodes,
            literal_mode=args.literal
        )
    
    logging.info("Graph analysis complete. CSV files have been generated.")

if __name__ == "__main__":
    main()