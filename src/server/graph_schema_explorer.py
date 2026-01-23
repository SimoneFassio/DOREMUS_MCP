import pandas as pd
import pathlib
from collections import defaultdict, Counter

class GraphSchemaExplorer:
    """
    An intelligent tool to explore a graph schema hierarchically.
    
    This class is initialized once with the graph data (triples) and
    pre-computed node/edge counts. It provides a single 'explore' method
    for an LLM agent to call.
    """
    
    def __init__(self, all_triples: list, nodes_count: dict, edges_count: dict):
        """
        Initializes the explorer with the graph data structures.

        Args:
            all_triples (list): A list of (source, edge, target) tuples.
            nodes_count (dict): A dict of {node_type: count}.
            edges_count (dict): A dict of {edge_type: count}.
        """
        print("Initializing GraphSchemaExplorer...")
        self.all_triples = all_triples
        self.nodes_count = nodes_count
        self.edges_count = edges_count
        
        TOP_PREFIX = ['mus', 'efrbroo']
        
        # Pre-compute sorted lists for fast access, filter by TOP_PREFIX
        self.top_15_nodes = set(
            [node for node, count in 
             sorted(nodes_count.items(), key=lambda item: item[1], reverse=True)
             if any(node.startswith(prefix) for prefix in TOP_PREFIX)][:15]
        )
        self.sorted_edges = [
            (edge, count) for edge, count in 
            sorted(edges_count.items(), key=lambda item: item[1], reverse=True)
        ]
        
        print(f"Loaded {len(self.all_triples)} triples.")
        print(f"Top 15 nodes set with {len(self.top_15_nodes)} nodes.")
        print(f"Found {len(self.sorted_edges)} unique edge types.")

    
    def _format_output(self, title: str, triples: list, unconnected_nodes: set = None) -> str:
        """
        Formats a list of triples into an LLM-understandable Markdown string.
        """
        if not triples:
            return f"## {title}\n\nNo relevant subgraph found for this path."

        # Use defaultdict to group targets by source and edge
        graph_dict = defaultdict(lambda: defaultdict(list))
        for s, e, t in triples:
            graph_dict[s][e].append(t)

        # Build the Markdown string
        md = f"## {title}\n\n"
        
        for source, edges in graph_dict.items():
            md += f"### Node: `{source}`\n"
            for edge, targets in edges.items():
                md += f"- **`{edge}`** →\n"
                for target in targets:
                    md += f"    - `{target}`\n"
            md += "\n"
            
        if unconnected_nodes:
            md += "---\n"
            md += "**Unconnected Nodes:**\n"
            md += "The following nodes from the top-15 list were not connected in this summary graph:\n"
            for node in unconnected_nodes:
                md += f"- `{node}`\n"

        return md

    def _get_summary_subgraph(self) -> str:
        """
        Generates the top-level summary graph using the specific
        heuristic (top 15 nodes, top 20 edges with degree constraints).
        """
        
        result_edges_set = set()
        node_out_degree = defaultdict(int)
        node_in_degree = defaultdict(int)
        
        edge_iterator = iter(self.sorted_edges)
        
        try:
            # --- 1. Handle the 1st (highest cardinality) edge ---
            first_edge_type = next(edge_iterator)[0]
            # Find all triples for this edge connecting top nodes
            for s, e, t in self.all_triples:
                if e == first_edge_type and s in self.top_15_nodes and t in self.top_15_nodes and s != t:
                    if (s, e, t) not in result_edges_set:
                        result_edges_set.add((s, e, t))
                        node_out_degree[s] += 1
                        node_in_degree[t] += 1

            # --- 2. Handle subsequent edges until we have 20 ---
            while len(result_edges_set) < 20:
                current_edge_type = next(edge_iterator)[0]
                
                # Find all triples for this edge
                for s, e, t in self.all_triples:
                    if e == current_edge_type and s in self.top_15_nodes and t in self.top_15_nodes and s != t:
                        
                        # Check constraints
                        if (s, e, t) in result_edges_set:
                            continue
                        if node_out_degree[s] > 2:
                            continue
                        if node_in_degree[t] > 2:
                            continue
                        
                        # Add to results and update degrees
                        result_edges_set.add((s, e, t))
                        node_out_degree[s] += 1
                        node_in_degree[t] += 1
                        
                        if len(result_edges_set) >= 20:
                            break # Stop inner loop
            
        except StopIteration:
            # This happens if we run out of edge types before reaching 20 edges
            print("Warning: Ran out of edge types before reaching 20 edges.")

        # --- 3. Check for unconnected nodes ---
        connected_nodes = set()
        for s, e, t in result_edges_set:
            connected_nodes.add(s)
            connected_nodes.add(t)
        
        unconnected = self.top_15_nodes - connected_nodes
        
        return self._format_output(
            title="Graph Summary (`/`)",
            triples=list(result_edges_set),
            unconnected_nodes=unconnected
        )

    def _get_neighborhood(self, class_name: str, depth: int) -> list:
        """
        Recursive helper to get all triples in a neighborhood.
        """
        
        # Use prefixes for class matching (e.g., "efrbroo:F28_Expression_Creation")
        # This is more robust than exact matching.
        
        # 1. Get Depth 1 triples
        depth_1_triples = set()
        neighbors = set()
        
        for s, e, t in self.all_triples:
            if s == class_name:
                depth_1_triples.add((s, e, t))
                neighbors.add(t)
            elif t == class_name:
                depth_1_triples.add((s, e, t))
                neighbors.add(s)
        
        if depth == 1:
            return list(depth_1_triples)
        
        # 2. Get Depth 2 triples
        all_triples = set(depth_1_triples)
        
        for neighbor in neighbors:
            # Avoid re-exploring the original node
            if neighbor == class_name:
                continue
                
            # Find all connections for the neighbor
            for s, e, t in self.all_triples:
                if s == neighbor:
                    all_triples.add((s, e, t))
                elif t == neighbor:
                    all_triples.add((s, e, t))
        
        return list(all_triples)

    def _get_neighborhood_subgraph(self, class_name: str, depth: int) -> str:
        """
        Generates the neighborhood subgraph for a specific class.
        """
        triples = self._get_neighborhood(class_name, depth)
        
        return self._format_output(
            title=f"Neighborhood for `{class_name}`",
            triples=triples
        )

    # --- The Public Tool Method ---
    
    @classmethod
    def load_from_csv(cls, data_dir: str = None):
        """
        Class method to load the explorer from CSV files.
        
        Args:
            data_dir: Directory containing the CSV files. If None, uses default data folder.
        
        Returns:
            GraphSchemaExplorer instance initialized with data from CSVs.
        """
        if data_dir is None:
            # Default to data folder in project root
            project_root = pathlib.Path(__file__).parent
            data_dir = project_root / "data"
        else:
            data_dir = pathlib.Path(data_dir)
        
        # Load ontology structure (triples)
        ontology_path = data_dir / "ontology_structure.csv"
        df = pd.read_csv(ontology_path)
        all_triples = []
        for _, row in df.iterrows():
            all_triples.append((row['source_type'], row['edge'], row['target_type']))
        
        # Load edge counts
        edges_path = data_dir / "edges_stats.csv"
        df_edges = pd.read_csv(edges_path)
        edge_counts = {}
        for _, row in df_edges.iterrows():
            edge_counts[row['edge']] = row['count']
        
        # Load node counts
        nodes_path = data_dir / "node_type_stats.csv"
        df_nodes = pd.read_csv(nodes_path)
        node_counts = {}
        for _, row in df_nodes.iterrows():
            node_counts[row['node_type']] = row['count']
        
        return cls(all_triples, node_counts, edge_counts)

    def explore_graph_schema(self, path: str, depth: int = 1) -> str:
        """
        Explores the graph schema hierarchically. 
        This is the single tool to be called by the LLM.

        Args:
            path (str):
                - Use '/' for the top-level summary.
                - Use '/{ClassName}' (e.g., '/efrbroo:F28_Expression_Creation') 
                  to explore a class.
            
            depth (int, optional): 
                - Use 1 or 2. Applies to class neighborhood exploration. 
                - Defaults to 1.

        Returns:
            str: A Markdown-formatted string of the relevant schema subgraph,
                 ready for an LLM to understand.
        """
        if not path.startswith("/"):
            raise ValueError("Error: Invalid path. Path must start with /")

        if path == "/":
            return self._get_summary_subgraph()
        
        else:
            # Path is '/{ClassName}'
            class_name = path[1:]
            
            # Validate depth
            if depth not in [1, 2]:
                raise ValueError("Error: Invalid depth. Must be 1 or 2.")
            
            # Check if class exists before exploring
            if class_name not in self.nodes_count:
                raise ValueError(f"Error: Class '{class_name}' not found in the graph's node list.")
            
            return self._get_neighborhood_subgraph(class_name, depth)
                


    def class_has_property(self, class_name: str, property: str) -> bool:
        """
        Checks if a property exists for a class.
        """
        # Check if property exists for class
        for s, e, t in self.all_triples:
            if s == class_name and e == property:
                return True
        return False
        