# DOREMUS Music Knowledge Graph - MCP Server

A Model Context Protocol (MCP) server for accessing the DOREMUS Knowledge Graph, providing comprehensive access to classical music metadata including composers, works, performances, recordings, and instrumentation.

> **General Purpose SPARQL Server**: While primarily tested with DOREMUS, this server is designed as a **general-purpose solution for any SPARQL-based Knowledge Graph**. Its unique template-driven architecture allows it to be adapted to any ontology (Wikidata, DBpedia, Corporate KGs) simply by modifying configuration files and SPARQL templates.

## Overview

This MCP server enables LLMs to query the DOREMUS Knowledge Graph (https://data.doremus.org) using natural language, with optimized tools for:
- **Entity Discovery**: Fuzzy searching for artists, works, and concepts.
- **Query Construction**: Building complex SPARQL queries step-by-step using a graph-based approach.
- **Data Retrieval**: Executing optimized queries to fetch structured data.

## Quick Start

### Prerequisites

- Docker and Docker Compose (for containerized deployment)
- Or Python 3.11+ and Poetry (for local development)

### Using Docker (Recommended for Production)

1. **Build and run the container:**

```bash
docker-compose up --build
```

2. **Access the MCP server at:**

```
http://localhost:8000/mcp
```

### Local Development with Poetry

1. **Install Poetry** (if not already installed):

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

2. **Install server dependencies:**

```bash
# Install core + server dependencies only
poetry install
```

3. **Run the server:**

```bash
poetry run python -m src.server.main
```

### Setting Up Evaluators/Client

For running evaluations and the LangChain client:

```bash
# Install with evaluation dependencies
poetry install --with eval

# Run evaluations
poetry run python evaluators/test_query.py
```

### Dependency Groups

This project uses Poetry with dependency groups:

- **main**: Core dependencies (langchain, requests, etc.)
- **server**: Server-only dependencies (fastmcp, uvicorn, starlette)
- **eval**: Evaluation/client dependencies (langgraph-cli, pandas, jupyter)

Install combinations as needed:

```bash
# Server only
poetry install

# Everything for development
poetry install --with eval,dev

# Production (no optional groups)
poetry install --only main
```

## Architecture

```
DOREMUS_MCP/
├── src/
│   ├── server/                  # MCP Server implementation
│   │   ├── config/              # Configuration & Templates (Package Data)
│   │   │   ├── templates/       # SPARQL Query Templates (.rq)
│   │   │   ├── server_config.yaml
│   │   │   ├── strategies.yaml
│   │   │   └── tools.yaml
│   │   ├── main.py              # Main FastMCP server with tools
│   │   ├── config_loader.py     # Configuration management
│   │   ├── tools_internal.py    # Core tool implementation
│   │   ├── template_parser.py   # SPARQL template engine
│   │   ├── query_container.py   # Dynamic query builder state
│   │   ├── graph_schema_explorer.py
│   │   ├── utils.py             # Internal utilities
│   │   └── __init__.py
│   └── rdf_assistant/           # LangChain assistant for evaluations
│       ├── doremus_assistant.py
│       ├── extended_mcp_client.py
│       ├── prompts.py
│       └── eval/
├── evaluators/              # Evaluation scripts
│   ├── client_geminiCLI.py
│   └── test_query.py
├── tests/                   # Unit tests
│   └── test_server.py
├── data/                    # Data files
│   ├── graph.csv
│   └── cq.json             # Competency questions
├── docs/                    # Documentation
│   ├── custom_kg_tutorial.md # GUIDE: Adpating to your own KG
│   ├── ENDPOINT_GUIDE.md
│   └── EXAMPLES.md
├── pyproject.toml          # Poetry dependencies and config
├── Dockerfile              # Container configuration
├── docker-compose.yml      # Docker Compose setup
└── README.md               # This file
```

## Adapt to Your Knowledge Graph

This server is designed to be ontology-agnostic. The "Tools" are abstract operations (Build Query, Apply Filter, Find Entities) that work on *any* graph.

*   **Configuration**: Define your SPARQL endpoint and Namespace prefixes in `src/server/config/server_config.yaml`.
*   **Templates**: Map user intent to your specific graph patterns using `.rq` templates in `src/server/config/templates/`.

👉 **[Read the Custom KG Tutorial](docs/custom_kg_config.md)** to learn how to adapt this server to your own data.

## Resources

- **DOREMUS Project**: http://www.doremus.org/
- **SPARQL Endpoint**: https://data.doremus.org/sparql/

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

**Last Updated**: January 2026
