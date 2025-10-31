# DOREMUS Music Knowledge Graph - MCP Server

A Model Context Protocol (MCP) server for accessing the DOREMUS Knowledge Graph, providing comprehensive access to classical music metadata including composers, works, performances, recordings, and instrumentation.

## Overview

This MCP server enables LLMs to query the DOREMUS Knowledge Graph (https://data.doremus.org) using natural language, with optimized tools for:

- Finding composers and musical works
- Searching works by composer, genre, date, instrumentation
- Retrieving detailed entity information
- Executing custom SPARQL queries

The server is built with FastMCP and designed for fast, efficient querying with extensive documentation for LLM guidance.

## Features

### 🎵 Core Tools

1. **find_candidate_entities** - Discover entities by name (composers, works, places)
2. **get_entity_details** - Retrieve comprehensive information about any entity
3. **search_musical_works** - Powerful parametric search for musical works with filters for:
   - Composers (by name or URI)
   - Work type/genre (sonata, symphony, concerto, etc.)
   - Date range (composition period)
   - Instrumentation (with quantity specifications)
   - Duration range
   - Places (composition/performance)
   - Topics
4. **execute_custom_sparql** - Full SPARQL query flexibility for advanced use cases

### 📚 Resources

So far the resources are implemented as tools because not all clients support them yet.

- **Knowledge Graph Structure** - Detailed ontology documentation
- **Usage Guide** - Comprehensive LLM interaction guide with examples

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Or Python 3.11+ for local development

### Using Docker (Recommended)

1. **Build and run the container:**

```bash
docker-compose up --build
```

2. **Access the MCP server at:**

```
http://localhost:8000/mcp
```

3. **Test the server:**

````bash
# Check health
curl -i http://localhost:8000/health

### Local Development

1. **Install dependencies:**
```bash
pip install -r requirements.txt
````

2. **Run the server:**

```bash
python3 server.py
```

## Architecture

```
DOREMUS_MCP/
├── server.py              # Main FastMCP server with tools and resources
├── query_builder.py       # Parametric SPARQL query builder
├── requirements.txt       # Python dependencies
├── Dockerfile            # Container configuration
├── docker-compose.yml    # Docker Compose setup
├── README.md             # This file
├── ENDPOINT_GUIDE.md     # SPARQL endpoint documentation
└── cq.json              # Competency questions and example queries
```

## Usage Examples

### Example 1: Find Works by Composer

```python
# Find Mozart's URI
find_candidate_entities("Mozart", "composer")
# Returns: URI and details

# Search Mozart's piano concertos
search_musical_works(
    composers=["Wolfgang Amadeus Mozart"],
    work_type="concerto",
    instruments=[{"name": "piano"}],
    limit=30
)
```

### Example 2: Instrumentation Search

```python
# Find string quartets
search_musical_works(
    instruments=[
        {"name": "violin", "quantity": 2},
        {"name": "viola", "quantity": 1},
        {"name": "cello", "quantity": 1}
    ]
)
```

### Example 3: Historical Period Search

```python
# Find chamber music from Romantic period
search_musical_works(
    date_start=1820,
    date_end=1900,
    work_type="chamber"
)
```

### Example 4: Complex Query

```python
# Works for flute and bassoon
execute_custom_sparql("""
PREFIX mus: <http://data.doremus.org/ontology#>
PREFIX efrbroo: <http://erlangen-crm.org/efrbroo/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT DISTINCT ?expression SAMPLE(?title) as ?title
WHERE {
  ?expression a efrbroo:F22_Self-Contained_Expression ;
      rdfs:label ?title ;
      mus:U13_has_casting ?casting .

  ?casting mus:U23_has_casting_detail ?det1, ?det2 .
  ?det1 mus:U2_foresees_use_of_medium_of_performance / skos:exactMatch*
        <http://www.mimo-db.eu/InstrumentsKeywords/3955> .  # Flute
  ?det2 mus:U2_foresees_use_of_medium_of_performance / skos:exactMatch*
        <http://www.mimo-db.eu/InstrumentsKeywords/3795> .  # Bassoon
}
LIMIT 50
""")
```

## Knowledge Graph Structure

The DOREMUS Knowledge Graph uses the FRBRoo and CIDOC-CRM ontologies extended with music-specific properties:

### Key Entity Types

- **Works/Expressions** (`efrbroo:F22_Self-Contained_Expression`)
- **Composers/Artists** (`foaf:Person`)
- **Performances** (`efrbroo:F31_Performance`)
- **Recordings** (`efrbroo:F29_Recording_Event`)
- **Instrumentation** (`mus:M6_Casting`)

### Common Properties

- `rdfs:label` - Title/name
- `mus:U12_has_genre` - Genre/type
- `mus:U13_has_casting` - Instrumentation
- `mus:U78_estimated_duration` - Duration (seconds)
- `ecrm:P4_has_time-span` - Date/time information

See the Knowledge Graph Structure resource in the server for complete documentation.

## Configuration

### Environment Variables

- `SPARQL_ENDPOINT` - DOREMUS SPARQL endpoint (default: https://data.doremus.org/sparql/)
- `REQUEST_TIMEOUT` - Query timeout in seconds (default: 60)

### Limits

- Default result limit: 50
- Maximum result limit: 500
- Query timeout: 60 seconds

## Data Coverage

The DOREMUS Knowledge Graph focuses on:

- **European classical music** (primarily French sources)
- **~200,000 musical works**
- **Detailed instrumentation** data
- **Performance history** from major French institutions
- **Recording metadata**

### Data Sources

- Bibliothèque nationale de France (BnF)
- Philharmonie de Paris
- Radio France

## Troubleshooting

### Query Timeouts

If queries timeout:

1. Add more specific filters (date range, composer)
2. Reduce the limit parameter
3. Check query complexity

### No Results

If searches return empty:

1. Check spelling and entity names
2. Try broader searches (remove filters incrementally)
3. Use `find_candidate_entities` to verify entity existence

### Connection Errors

If the server can't reach the endpoint:

1. Check internet connection
2. Verify SPARQL endpoint is accessible: https://data.doremus.org/sparql/
3. Check firewall settings

## Development

### Running Tests

The docker compose command will automatically run the necessary tests, however, it is possible to re-run them using the following command:

```bash
# Run tests
docker compose run --rm test
```

### Adding New Tools

1. Add tool function to `server.py` with `@mcp.tool()` decorator
2. Document parameters and return types
3. Add examples to usage guide
4. Test with example queries

### Query Builder Enhancements

To add new filter types:

1. Add parameter to `build_works_query()` in `query_builder.py`
2. Create SPARQL pattern in the function
3. Add to WHERE clause composition
4. Update documentation

## Contributing

Contributions welcome! Areas for improvement:

- Additional query patterns
- Performance optimizations
- Extended filtering options
- Better error messages
- Test coverage

## Resources

- **DOREMUS Project**: http://www.doremus.org/
- **SPARQL Endpoint**: https://data.doremus.org/sparql/
- **Ontology Documentation**: http://data.doremus.org/ontology/
- **FastMCP Documentation**: https://github.com/jlowin/fastmcp

## License

This MCP server implementation is provided as-is for accessing the publicly available DOREMUS Knowledge Graph.

## Acknowledgments

Built on top of the DOREMUS (DOing REusable MUSical data) project, funded by the French National Research Agency (ANR-14-CE24-0020).

Special thanks to:

- DOREMUS consortium members
- Bibliothèque nationale de France
- Philharmonie de Paris
- Radio France

## Support

For issues related to:

- **This MCP server**: Open an issue on the repository
- **DOREMUS data or ontology**: Contact the DOREMUS project
- **SPARQL endpoint**: Check https://data.doremus.org/

---

**Last Updated**: October 2025
