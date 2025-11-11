# DOREMUS MCP Server - Usage Examples

This document provides concrete examples of how to use the DOREMUS MCP server for various musical queries.

## Table of Contents

1. [Basic Queries](#basic-queries)
2. [Composer Searches](#composer-searches)
3. [Instrumentation Queries](#instrumentation-queries)
4. [Historical Period Searches](#historical-period-searches)
5. [Genre-Specific Queries](#genre-specific-queries)
6. [Advanced Custom SPARQL](#advanced-custom-sparql)

---

## Basic Queries

### Finding a Composer

```python
# Search for Mozart
result = find_candidate_entities("Mozart", "composer")

# Returns:
{
  "query": "Mozart",
  "entity_type": "composer",
  "matches_found": 1,
  "entities": [
    {
      "entity": "http://data.doremus.org/artist/4802a043-23bb-3b8d-a443-4a3bd22ccc63",
      "label": "Wolfgang Amadeus Mozart",
      "type": "http://xmlns.com/foaf/0.1/Person"
    }
  ]
}
```

### Getting Details About an Entity

```python
# Get detailed information about Mozart
mozart_uri = "http://data.doremus.org/artist/4802a043-23bb-3b8d-a443-4a3bd22ccc63"
result = get_entity_details(mozart_uri)

# Returns comprehensive properties including:
# - name, birthDate, deathDate
# - birthPlace, deathPlace
# - relationships, works created, etc.
```

---

## Composer Searches

### All Works by a Composer

```python
# List Mozart's works
result = search_musical_works(
    composers=["Wolfgang Amadeus Mozart"],
    limit=50
)
```

### Works by Multiple Composers

```python
# Compare works by Beethoven and Brahms
result = search_musical_works(
    composers=["Ludwig van Beethoven", "Johannes Brahms"],
    limit=100
)
```

### Composer's Works in a Specific Period

```python
# Beethoven's late works (1815-1827)
result = search_musical_works(
    composers=["Ludwig van Beethoven"],
    date_start=1815,
    date_end=1827,
    limit=50
)
```

---

## Instrumentation Queries

### String Quartet

```python
# Find string quartets (2 violins, viola, cello)
result = search_musical_works(
    instruments=[
        {"name": "violin", "quantity": 2},
        {"name": "viola", "quantity": 1},
        {"name": "cello", "quantity": 1}
    ],
    limit=50
)
```

### Piano Trio

```python
# Find piano trios
result = search_musical_works(
    instruments=[
        {"name": "violin", "quantity": 1},
        {"name": "cello", "quantity": 1},
        {"name": "piano", "quantity": 1}
    ]
)
```

### Works with Flute

```python
# Any work featuring flute
result = search_musical_works(
    instruments=[{"name": "flute"}],
    limit=100
)
```

### Works for Solo Piano

```python
# Solo piano works
result = search_musical_works(
    instruments=[{"name": "piano", "quantity": 1}],
    limit=100
)
```

### Wind Quintet

```python
# Standard wind quintet
result = search_musical_works(
    instruments=[
        {"name": "flute", "quantity": 1},
        {"name": "oboe", "quantity": 1},
        {"name": "clarinet", "quantity": 1},
        {"name": "bassoon", "quantity": 1},
        {"name": "horn", "quantity": 1}
    ]
)
```

---

## Historical Period Searches

### Baroque Period (1600-1750)

```python
result = search_musical_works(
    date_start=1600,
    date_end=1750,
    limit=100
)
```

### Classical Period (1750-1820)

```python
result = search_musical_works(
    date_start=1750,
    date_end=1820,
    limit=100
)
```

### Romantic Period (1820-1900)

```python
result = search_musical_works(
    date_start=1820,
    date_end=1900,
    limit=100
)
```

### 20th Century

```python
result = search_musical_works(
    date_start=1900,
    date_end=2000,
    limit=100
)
```

---

## Genre-Specific Queries

### All Sonatas

```python
result = search_musical_works(
    work_type="sonata",
    limit=100
)
```

### Piano Sonatas by Beethoven

```python
result = search_musical_works(
    composers=["Ludwig van Beethoven"],
    work_type="sonata",
    instruments=[{"name": "piano"}],
    limit=50
)
```

### Symphonies from the Classical Period

```python
result = search_musical_works(
    work_type="symphony",
    date_start=1750,
    date_end=1820,
    limit=50
)
```

### Operas

```python
result = search_musical_works(
    work_type="opera",
    limit=100
)
```

### Concertos for Violin

```python
result = search_musical_works(
    work_type="concerto",
    instruments=[{"name": "violin"}],
    limit=50
)
```

---

## Advanced Custom SPARQL

### Find Works Performed at a Specific Venue

```python
query = """
PREFIX mus: <http://data.doremus.org/ontology#>
PREFIX efrbroo: <http://erlangen-crm.org/efrbroo/>
PREFIX ecrm: <http://erlangen-crm.org/current/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?expression SAMPLE(?title) as ?title
WHERE {
    ?expression a efrbroo:F22_Self-Contained_Expression ;
        rdfs:label ?title .
    
    ?performance a efrbroo:F31_Performance ;
        efrbroo:R25_performed / ecrm:P165_incorporates ?expression ;
        ecrm:P7_took_place_at <http://data.doremus.org/place/bd21be9c-3f2b-3aa3-a460-114d579eabe6> .
}
LIMIT 50
"""

result = execute_custom_sparql(query)
```

### Find Works with Duration Between 20-30 Minutes

```python
result = search_musical_works(
    duration_min=1200,  # 20 minutes in seconds
    duration_max=1800,  # 30 minutes in seconds
    limit=50
)
```

### Find All Works with Specific Catalogue Number

```python
query = """
PREFIX mus: <http://data.doremus.org/ontology#>
PREFIX efrbroo: <http://erlangen-crm.org/efrbroo/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX modsrdf: <http://www.loc.gov/standards/mods/rdf/v1/#>

SELECT DISTINCT ?expression SAMPLE(?title) as ?title ?catNum
WHERE {
    ?expression a efrbroo:F22_Self-Contained_Expression ;
        mus:U16_has_catalogue_statement ?catalogue ;
        rdfs:label ?title .
    
    ?catalogue mus:U40_has_catalogue_name / modsrdf:identifier "BWV" ;
        mus:U41_has_catalogue_number ?catNum .
    
    FILTER (?catNum >= 1 AND ?catNum <= 100)
}
ORDER BY ?catNum
LIMIT 100
"""

result = execute_custom_sparql(query)
```

### Find Works by Composer's Nationality

```python
query = """
PREFIX mus: <http://data.doremus.org/ontology#>
PREFIX ecrm: <http://erlangen-crm.org/current/>
PREFIX efrbroo: <http://erlangen-crm.org/efrbroo/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX schema: <http://schema.org/>
PREFIX geonames: <http://www.geonames.org/ontology#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?expression SAMPLE(?title) as ?title SAMPLE(?composerName) as ?composer
WHERE {
    ?expression a efrbroo:F22_Self-Contained_Expression ;
        rdfs:label ?title .
    
    ?expCreation efrbroo:R17_created ?expression ;
        ecrm:P9_consists_of / ecrm:P14_carried_out_by ?composer .
    
    ?composer foaf:name ?composerName ;
        schema:birthPlace / geonames:countryCode "DE" .
}
LIMIT 100
"""

result = execute_custom_sparql(query)
# German composers
```

### Find Recordings Made in a Specific Year

```python
query = """
PREFIX mus: <http://data.doremus.org/ontology#>
PREFIX efrbroo: <http://erlangen-crm.org/efrbroo/>
PREFIX ecrm: <http://erlangen-crm.org/current/>
PREFIX time: <http://www.w3.org/2006/time#>

SELECT DISTINCT ?rec ?concert SAMPLE(?title) as ?title
WHERE {
    ?rec a efrbroo:F29_Recording_Event ;
        ecrm:P4_has_time-span / time:hasBeginning / time:inXSDDate ?time ;
        efrbroo:R20_recorded ?concert .
    
    ?concert a efrbroo:F31_Performance ;
        rdfs:label ?title .
    
    FILTER (year(?time) = 2014)
}
LIMIT 100
"""

result = execute_custom_sparql(query)
```

### Composer Collaboration - Composer as Performer

```python
query = """
PREFIX mus: <http://data.doremus.org/ontology#>
PREFIX ecrm: <http://erlangen-crm.org/current/>
PREFIX efrbroo: <http://erlangen-crm.org/efrbroo/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?expression SAMPLE(?title) as ?title ?composerName ?performance
WHERE {
    ?expression a efrbroo:F22_Self-Contained_Expression ;
        rdfs:label ?title .
    
    ?expCreation efrbroo:R17_created ?expression ;
        ecrm:P9_consists_of / ecrm:P14_carried_out_by ?composer .
    
    ?composer foaf:name ?composerName .
    
    ?performance a mus:M42_Performed_Expression_Creation ;
        efrbroo:R17_created / mus:U54_is_performed_expression_of ?expression ;
        ecrm:P9_consists_of / ecrm:P14_carried_out_by ?composer .
}
LIMIT 50
"""

result = execute_custom_sparql(query)
```

---

## Complex Multi-Filter Queries

### Chamber Music from Romantic Period for Strings

```python
# Combining multiple filters
result = search_musical_works(
    work_type="chamber",
    date_start=1820,
    date_end=1900,
    instruments=[
        {"name": "violin"},
        {"name": "viola"},
        {"name": "cello"}
    ],
    limit=50
)
```

### Short Piano Works by French Composers

```python
# This requires custom SPARQL for nationality filter
query = """
PREFIX mus: <http://data.doremus.org/ontology#>
PREFIX ecrm: <http://erlangen-crm.org/current/>
PREFIX efrbroo: <http://erlangen-crm.org/efrbroo/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX schema: <http://schema.org/>
PREFIX geonames: <http://www.geonames.org/ontology#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?expression SAMPLE(?title) as ?title ?duration SAMPLE(?composerName) as ?composer
WHERE {
    ?expression a efrbroo:F22_Self-Contained_Expression ;
        rdfs:label ?title ;
        mus:U78_estimated_duration ?duration ;
        mus:U13_has_casting / mus:U23_has_casting_detail / 
        mus:U2_foresees_use_of_medium_of_performance <http://data.doremus.org/vocabulary/iaml/mop/kpf> .
    
    ?expCreation efrbroo:R17_created ?expression ;
        ecrm:P9_consists_of / ecrm:P14_carried_out_by ?composer .
    
    ?composer foaf:name ?composerName ;
        schema:birthPlace / geonames:countryCode "FR" .
    
    FILTER (?duration <= 600)
}
LIMIT 50
"""

result = execute_custom_sparql(query)
```

---

## Tips for Effective Queries

1. **Start Specific**: Begin with restrictive filters and broaden if needed
2. **Use Limits**: Keep result sets manageable (20-100 for exploration)
3. **Entity Resolution**: Always use `find_candidate_entities` before searching by name
4. **Combine Filters**: Use multiple criteria to narrow results
5. **Handle Timeouts**: If a query times out, add more filters or reduce scope
6. **Check Results**: Validate that returned data matches expectations

---

## Testing Queries

You can test these examples using the test script:

```bash
python test_server.py
```

Or by running the server and making HTTP requests:

```bash
# Start server
./start.sh

# Test with curl
curl -X POST http://127.0.0.1:8000/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "search_musical_works",
      "arguments": {
        "composers": ["Mozart"],
        "work_type": "sonata",
        "limit": 10
      }
    },
    "id": 1
  }'
```

---

For more information, see:
- [README.md](README.md) - Main documentation
- [ENDPOINT_GUIDE.md](ENDPOINT_GUIDE.md) - SPARQL endpoint details
- [cq.json](cq.json) - Competency questions and example queries
