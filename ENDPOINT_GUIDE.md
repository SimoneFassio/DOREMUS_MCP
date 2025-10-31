# DOREMUS SPARQL Endpoint Quick Guide

## Base Endpoint
- URL: `https://data.doremus.org/sparql/`
- Implements the Virtuoso SPARQL protocol; standard HTTP `GET` or `POST` requests are supported.

## Request Basics
- Include the SPARQL text in the `query` parameter. Keep large queries intact by using `curl --data-urlencode` (GET) or `--data-binary` (POST).
- Ask for JSON with either of the following:
  - HTTP header: `Accept: application/sparql-results+json`
  - Query arg: `format=application/sparql-results+json`

## cURL Examples
```bash
# 1. Small sample query
curl -s -G https://data.doremus.org/sparql/ \
  --data-urlencode 'query=SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 5' \
  -H 'Accept: application/sparql-results+json'

# 2. Run a saved query (Mozart works)
curl -s -G https://data.doremus.org/sparql/ \
  --data-urlencode "query=$(cat 1.rq)" \
  -H 'Accept: application/sparql-results+json'
```

## Python Template
```python
import requests

query = """
SELECT ?expression ?title WHERE {
  ?expression a efrbroo:F22_Self-Contained_Expression ;
              rdfs:label ?title .
} LIMIT 5
"""

response = requests.get(
    "https://data.doremus.org/sparql/",
    params={"query": query},
    headers={"Accept": "application/sparql-results+json"},
    timeout=30,
)
response.raise_for_status()
data = response.json()
for row in data["results"]["bindings"]:
    print(row["expression"]["value"], row["title"]["value"])
```
- Use `requests.post` with `data={"query": query}` once the query text becomes too long for GET.
- For CONSTRUCT/DESCRIBE queries, change `Accept` to `text/turtle`, `application/ld+json`, etc.

## Optional Parameters
- `default-graph-uri`: restricts the query to a specific graph.
- `timeout`: query timeout in milliseconds.
- `should-sponge`, `debug`: Virtuoso diagnostic switches (see Virtuoso docs).

## Result Format
- JSON matches the W3C SPARQL Results spec: bindings include type info (e.g., `literal`, `uri`, language tags, datatype IRIs).

## Best Practices
- Add `LIMIT` while iterating on queries.
- Handle HTTP errors and implement retries or exponential backoff.
- When needed, pin the dataset with `default-graph-uri` to avoid surprises.
- Check the Virtuoso documentation for server-specific features: https://docs.openlinksw.com/virtuoso/rdfsparqlprotocolendpoint/
