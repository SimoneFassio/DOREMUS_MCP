# to be done



## Ontology Tool

### Get Ontology Summary
```python
get_ontology('/')
```
Returns the top 15 most important node types and their top 20 most common relationships.

### Explore Specific Class (Depth 1)
```python
get_ontology('/efrbroo:F28_Expression_Creation', depth=1)
```
Shows direct connections to/from the specified class.

### Explore Specific Class (Depth 2)
```python
get_ontology('/efrbroo:F22_Self-Contained_Expression', depth=2)
```
Shows connections up to 2 hops away, including neighbors' neighbors.

## Data Files Used
The tool automatically loads data from:
- `/data/ontology_structure.csv` - Graph triples (source, edge, target)
- `/data/edges_stats.csv` - Edge type counts
- `/data/node_type_stats.csv` - Node type counts
