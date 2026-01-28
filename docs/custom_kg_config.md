# How to Adapt Doremus MCP to a Custom Knowledge Graph

This guide explains how to configure the MCP server to work with a different SPARQL endpoint and Knowledge Graph (KG) schema.

Currently, the adaptation requires changes in two main areas:
1.  **Configuration**: Defining endpoint and namespace prefixes.
2.  **Templates**: Defining how to map natural language to SPARQL queries for your specific ontology.

---

## 1. Configuration

The configuration is now located in the `src/server/config/` directory.

### `src/server/config/server_config.yaml`
Contains the connection details and vocabulary definitions.

**Steps to Customize:**
1.  **SPARQL Endpoint**: Update `sparql_endpoint` to point to your KG's URL.
    ```yaml
    sparql_endpoint: "https://your-custom-kg.org/sparql/"
    request_timeout: 60
    ```
2.  **Namespace Prefixes**: Update the `prefixes` dictionary.
    *   **Keep Standard Prefixes**: It is highly recommended to keep `rdf`, `rdfs`, `skos`, `time`, and `schema`.
    *   **Add Your Prefixes**: Add the prefixes used in your ontology.
    ```yaml
    prefixes:
      mus: "http://data.doremus.org/ontology#" # Replace/remove if needed
      ecrm: "http://erlangen-crm.org/current/"
      mykg: "http://example.org/ontology#"     # Add your custom prefixes
    
3.  **Discovery Configuration**: The `discovery` section controls how the `find_candidate_entities` tool searches for entities in your KG.
    *   **`label_predicates`**: A dictionary mapping entity types (e.g., "artist", "place") to the specific RDF property used for their name/label (e.g., `foaf:name`, `geonames:name`). This allows the search tool to be precise based on the entity type inferred from the user's question.
    *   **`candidate_entities_query`**: The SPARQL query used to perform fuzzy search.
        *   **Inputs**: The system injects `{label_predicate}` (selected from the dict above) and `{search_literal}` (the user's search term).
        *   **Outputs**: The query MUST return three variables:
            *   `?entity`: The URI of the candidate.
            *   `?label`: The human-readable name.
            *   `?type`: The class of the entity.
    *   **`get_entity_label_query`**: A simple query to fetch the preferred label for a specific URI, used for display purposes.
        *   **Inputs**: `{uri}`.
        *   **Outputs**: `?label`.
    ```yaml
    discovery:
      label_predicates:
        artist: "foaf:name"
        vocabulary: "skos:prefLabel"
        place: "geonames:name"
        others: "rdfs:label"
      candidate_entities_query: |
        SELECT ?entity ?label ?type
        WHERE {
            ...
            ?entity {label_predicate} ?label  .
            ...
        }
      get_entity_label_query: |
        SELECT ?label WHERE { <{uri}> rdfs:label ?label }
    ```


### `config/strategies.yaml`
Defines the "thinking strategies" the agent uses to decompose complex queries.
*   **Categories**: You can define `strict` (closed sets), `open` (inclusion), or `default` strategies.
*   **Triggers**: List keywords that trigger a specific strategy (e.g., "strictly", "at least").
*   **Description**: The prompt injected into the LLM context. Customize this to teach the agent how to handle specific linguistic patterns for your data.

### `config/tools.yaml`
Contains the descriptions and documentation for the MCP tools.
*   **Tool Descriptions**: You can modify the `description` fields to change how the LLM understands and uses each tool. This is useful if you want to rename concepts or emphasize different capabilities for your KG.


---

## 2. Creating Query Templates

Templates are the core engine of this MCP server. They tell the system how to translate user intent (like "Find works by X") into valid SPARQL patterns.

Templates are located in `src/server/config/templates/`. Each template is a valid `.rq` (SPARQL) file with special comments used by the parser.

### Anatomy of a Template

A template consists of four parts:
1.  **Metadata Header**
2.  **SELECT Clause**
3.  **Core Triples** (The "Base" Pattern)
4.  **Filter Definitions** (Optional criteria)

#### 1. Metadata Header
The first line must define the template name.
```sparql
# Template: template_name
```

#### 2. SELECT Clause
Defines the **Base Variable** (the main entity this template finds) and the **Output Variables** (what to show the user).
```sparql
SELECT DISTINCT ?myEntity SAMPLE(?label) AS ?label
WHERE {
```
*   The **first variable** (e.g., `?myEntity`) is automatically detected as the "Base Variable".
*   Other variables (e.g., `?label`) are available for matching and display.

#### 3. Core Triples (`# build_query`)
This section contains the triples that are added to the graph effectively when the `build_query` tool is called.
*   **Starting Point**: It must include all fundamental triples that define the entity.
*   **Foundation**: It serves as the connection point for all other filters and tools.
*   **Typing**: It MUST effectively "type" the variable (e.g., `?var a <Class>`).

```sparql
# build_query
?myEntity a mykg:MyClass .
?myEntity rdfs:label ?label .
```

#### 4. Filter Definitions
Filters allow users to narrow down results (e.g., "by artist", "by date").

**Key Rule**: Each filter block must contain **ALL** triplets necessary to connect the **Template Variable** (the main one declared in `build_query`) to the **Target Variable** you are filtering. Do not rely on triples from other filters; each filter must be self-contained in its connection to the base.

**Header Format:**
```sparql
# filter: "filter_name":"values_variable":"regex_variable":"entity_type"
```

*   **`filter_name`**: The key used by the `apply_filter` tool (e.g., "instrument", "author").
*   **`values_variable`**: The variable to bind when the user provides a specific URI. **Can be left empty `""`** if you prefer to always filter using a text REGEX.
*   **`regex_variable`**: The variable to filter when the user provides a text string (search). Leave empty `""` if not applicable.
*   **`entity_type`**: Used to guide entity discovery. Options: `"artist"`, `"vocabulary"`, `"place"`, `"others"`, `"literal"`.

**Example Filter (Filtering by Author):**
```sparql
# filter: "author":"?authorUri":"?authorName":"artist"
?myEntity mykg:createdBy ?creation .
?creation mykg:hasAgent ?authorUri .
?authorUri foaf:name ?authorName .
```
*   If the user has a specific URI (e.g., `<Bob>`), the system binds `?authorUri` to `<Bob>`.
*   If the user searches for "Bob", the system applies a text filter to `?authorName`.

### Variable Renaming Rules
The `template_parser.py` automatically renames variables to avoid collisions when combining multiple modules.
*   **Base Variable**: The variable matching your Template Base Variable is safely renamed to match the active query.
*   **Suffixing**: Other variables in your filters (like `?creation` above) will automatically get a suffix (e.g., `?creation_work`) to ensure they don't accidentally clash with other parts of the query.

### Summary Checklist for New Templates
1.  [ ] Create `name.rq` in `config/templates/`.
2.  [ ] Add `# Template: name`.
3.  [ ] Write `SELECT DISTINCT ?mainVar ...`.
4.  [ ] Add `# build_query` section with `?mainVar a Class` and basic label.
5.  [ ] Add `# filter: ...` blocks for every property you want to allow filtering by.
