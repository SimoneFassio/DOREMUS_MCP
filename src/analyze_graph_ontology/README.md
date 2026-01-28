# Analyze KG graph tools
Tool used to generate statistics about the KG graph and to extract the ontology.
Copy the 4 generated .csv files to /server/data/.

## analyze_graph.py
This tool query the KG and return 3 files:
- list of all nodes types (classes) and their cardinality
- list of all edges types (properties) and their cardinality
- list of the ontology filtered

### Usage examples
Get node statistics with prefix filtering
python analyze_graph.py --nodes --prefix

Get edge statistics only
python analyze_graph.py --edges

Get ontology, filtering edges with count < 10 and ignoring literals
python analyze_graph.py --ontology --filter_cardinality_edges 10 --literal 0

Run all with prefix filtering and cardinality filters
python analyze_graph.py --nodes --edges --ontology --prefix --filter_cardinality_edges 5 --filter_cardinality_nodes 100

Run all with default settings (no arguments)
python analyze_graph.py

used for this project:
python analyze_graph.py --nodes --edges --ontology --prefix --filter_cardinality_edges 10 --filter_cardinality_nodes 20