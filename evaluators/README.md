# DOREMUS Evaluation Pipeline

This directory contains the scripts necessary to run evaluations on the DOREMUS dataset using LangSmith. The pipeline consists of three main steps: calculate query complexity/splits, create/upload the dataset to LangSmith, and run the evaluation experiment.

## Prerequisites

1.  **Environment Variables**: Ensure you have a `.env` file in the project root with the following keys:
    *   `LANGCHAIN_API_KEY`: Your LangSmith API key.
    *   `LANGCHAIN_PROJECT`: Your LangSmith project name.
    *   `DOREMUS_MCP_URL`: URL of the MCP server (default: `http://localhost:8000/mcp`).

## Workflow

### Step 1: Calculate Splits (Local)

Before creating the dataset, you need to calculate the complexity of each SPARQL query (measured in "hops") and assign it a split (`easy`, `medium`, `hard`).

Run the following command to calculate splits and **write them back** to the `.rq` files in `data/`:

```bash
python src/rdf_assistant/eval/split_dataset.py --write
```

*   This script analyzes the SPARQL queries in `data/competency_questions` and `data/user_questions`.
*   It updates the `# split: "..."` header in each `.rq` file.
*   **Note**: Questions marked as `impossible` are skipped.

### Step 2: Create/Refresh Dataset (LangSmith)

Once the local files are updated with the correct splits, upload them to LangSmith to create or refresh the dataset.

Run:

```bash
python evaluators/create_dataset.py --dataset-name "Doremus Dataset"
```

*   **--dataset-name**: (Optional) The name of the dataset in LangSmith. Defaults to the `EVALUATION_DATASET_NAME` env var or "Doremus Dataset".
*   **Warning**: This script will **DELETE** any existing dataset with the same name and recreate it with the latest data from your local `.rq` files.

### Step 3: Run Evaluation

Finally, run the evaluation experiment. This script pulls the dataset from LangSmith, runs your assistant against the questions, and evaluates the results.

Run:

```bash
python evaluators/test_query.py
```

### Configuration via Environment Variables

You can control the evaluation behavior using these environment variables in your command line or `.env` file:

| Variable | Description | Default |
| :--- | :--- | :--- |
| `EVALUATION_DATASET_NAME` | The name of the dataset on LangSmith to use. | "Doremus Dataset" |
| `EVALUATION_DATASET_SPLITS` | Comma-separated list of splits to evaluate. e.g. "easy,medium" | "easy,medium,hard" |
| `EVALUATION_DATASET_ORIGIN` | Filter by question origin: `competency_question` or `user_question`. Leave empty for all. | "" (All) |
| `EXPERIMENT_PREFIX` | A prefix for the experiment run name in LangSmith. | "" |

#### Example: Running only "Hard" Competency Questions

```bash
export EVALUATION_DATASET_NAME="Doremus Dataset"
export EVALUATION_DATASET_SPLITS="hard"
export EVALUATION_DATASET_ORIGIN="competency_question"
python evaluators/test_query.py
```

## Evaluation Logic

The evaluation script `test_query.py` uses three methods to score performance:
1.  **Accuracy**: Checks if the URIs returned by the generated SPARQL query match the ground truth.
2.  **LLM Score (Semantic)**: Uses an LLM to compare the generated SPARQL with the reference SPARQL for semantic equivalence.

Results are logged to LangSmith.
