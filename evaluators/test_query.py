import asyncio
import json
import os
import sys
import logging
from dotenv import load_dotenv

# Add src to path for local development
sys.path.insert(0, 'src')

from rdf_assistant.doremus_assistant import doremus_assistant, client, create_model, provider
from rdf_assistant.eval.doremus_dataset import examples_queries
from server.utils import execute_sparql_query

# Suppress httpx INFO logs
logging.getLogger("httpx").setLevel(logging.WARNING)

load_dotenv(".env")

EXPERIMENT_PREFIX = os.getenv("EXPERIMENT_PREFIX", "")

# Set to True to reload the dataset and update it
RELOAD = False

async def main():
    # DATASET CREATION
    dataset_name = os.getenv( "EVALUATION_DATASET_NAME","Default Dataset")
    if RELOAD:
        if client.has_dataset(dataset_name=dataset_name):
            print("Reloading Dataset")
            dataset = client.read_dataset(dataset_name=dataset_name)
            client.delete_dataset(dataset_id=dataset.id)
            print(f"Dataset '{dataset_name}' has been deleted.")

    # Create dataset if it doesn't exist
    if not client.has_dataset(dataset_name=dataset_name):
        print("Created Dataset")
        dataset = client.create_dataset(
            dataset_name=dataset_name, 
            description="A dataset of competency questions and their SPARQL queries."
        )
        # Add examples to the dataset
        inputs = [example["inputs"] for example in examples_queries]
        outputs = [example["outputs"] for example in examples_queries]

        client.create_examples(dataset_id=dataset.id, inputs=inputs, outputs=outputs)

    # EVALUATION FUNCTIONS

    async def target_doremus_assistant(inputs: dict) -> dict:
        """Process a user input through the doremus assistant and capture the generated query."""
        response = await doremus_assistant.ainvoke(
            {"messages": [{"role": "user", "content": inputs["query_input"]}]}
        )

        # Check the messages field first
        messages = response.get("messages", [])
        generated_query = ""
        for message in messages:
            messContent = message.content if hasattr(message, "content") else ""
            if hasattr(message, "name") and message.name == "execute_query":
                content_json = json.loads(messContent)
                generated_query = content_json.get("generated_query", "")
        if generated_query:
            return {"generated_query": generated_query or ""}

    async def results_evaluator(outputs: dict, reference_outputs: dict) -> float:
        """Check the percentage of correct values returned by the query."""
        if not outputs.get("generated_query"):
            print("Error: The LLM couldn't generate the query.")
            return 0.0
        
        loop = asyncio.get_running_loop()
        
        # Run blocking reference query in executor
        reference_output_dict = await loop.run_in_executor(
            None, 
            lambda: execute_sparql_query(reference_outputs["rdf_query"], limit=10000)
        )
        
        if not reference_output_dict["success"] or reference_output_dict is None:
            print("Error: Failed to execute reference SPARQL query.")
            return 0.0
        reference_output = reference_output_dict["results"]

        # Run blocking generated query in executor
        query_output_dict = await loop.run_in_executor(
            None,
            lambda: execute_sparql_query(outputs["generated_query"], limit=100)
        )
        
        if not query_output_dict["success"] or query_output_dict is None:
            print("Error: Failed to execute generated SPARQL query.")
            return 0.0
        query_output = query_output_dict["results"]
        
        # Ensure both outputs are lists for comparison
        if not isinstance(reference_output, list) or not isinstance(query_output, list):
            print("Error: Outputs are not lists.")
            return 0.0
        
        # Helper to extract URI values from list of dicts
        def extract_uri_values(results):
            uri_values = set()
            if not results:
                return uri_values
            
            # Find columns that look like URIs (check first row as heuristic, or all rows)
            # Checking all rows to be safe, or iterate over keys of first row and check values
            keys = results[0].keys()
            
            for key in keys:
                # collecting all values for this key
                col_values = [r.get(key) for r in results if r.get(key)]
                # Check if majority or any starts with http. Let's assume if it looks like a URI column
                if any(isinstance(v, str) and v.startswith("http") for v in col_values):
                     uri_values.update(col_values)
            return uri_values

        ref_uris = extract_uri_values(reference_output)
        output_uris = extract_uri_values(query_output)
        
        query_total_uris = 0
        query_correct_uris = 0
        
        if not output_uris:
            # If empty query results
            if len(reference_output) == 0:
                percentage_correct = 100.0
            else:
                percentage_correct = 0.0
        else:
            for uri in output_uris:
                query_total_uris += 1
                if uri in ref_uris:
                    query_correct_uris += 1
            
            if query_total_uris > 0:
                percentage_correct = (query_correct_uris / query_total_uris) * 100.0
            else:
                # No URIs returned, but maybe expected? 
                # If reference also has no URIs, then it's a match (100%), otherwise 0%
                if len(ref_uris) == 0:
                    percentage_correct = 100.0
                else:
                    percentage_correct = 0.0
        
        ending_ref = "...]" if len(reference_output) > 2 else ""
        ending_que = "...]" if len(query_output) > 2 else ""
        # print(f"\nThe reference Output is: {reference_output[:2]}", ending_ref)
        # print(f"\nThe query Output is: {query_output[:2]}", ending_que)
        print(f" Percentage of correct values: {percentage_correct:.2f}%")
        
        return round(percentage_correct, 2)

    async def llm_evaluator(outputs: dict, reference_outputs: dict) -> dict:
        """Use an LLM to evaluate the semantic correctness of the generated query."""
        
        # Instantiate the judge model (using same config as agent)
        llm = create_model(provider)
        
        generated_query = outputs.get("generated_query", "")
        reference_query = reference_outputs["rdf_query"] # ground truth query
        
        if not generated_query:
            return {"score": 0, "comment": "No query generated"}

        # Define the prompt for the judge
        prompt = f"""
        You are an expert SPARQL query evaluator. Compare the GENERATED SPARQL query with the REFERENCE SPARQL query.
        
        REFERENCE QUERY:
        ```sparql
        {reference_query}
        ```
        
        GENERATED QUERY:
        ```sparql
        {generated_query}
        ```
        
        Task:
        1. Ignore minor whitespace or limit differences (e.g. LIMIT 100 vs LIMIT 50).
        2. Check if the intent filters and selection variables are semantically equivalent.
        3. Check if the graph patterns (triples) match the same logic.
        
        Output a JSON object with:
        - "score": A number between 0 and 1 (1 means semantically equivalent, 0 means completely wrong).
        - "reasoning": A brief explanation of the score where you point out the errors in the generated query.
        
        Only output the JSON.
        """
        
        try:
            response = await llm.ainvoke(prompt)
            content = response.content
            
            # extract json
            clean_content = content.strip()
            if clean_content.startswith("```json"):
                clean_content = clean_content[7:]
            if clean_content.endswith("```"):
                clean_content = clean_content[:-3]
            
            data = json.loads(clean_content.strip())
            return {"score": round(data.get("score", 0), 2), "comment": data.get("reasoning", "")}
        except Exception as e:
            print(f"LLM Evaluator Error: {e}")
            return {"score": 0, "comment": f"Evaluation failed: {e}"}

    # RUN EVALUATION
    run_expt = True
    if run_expt:
        evaluation = await client.aevaluate(
            target_doremus_assistant,
            data=dataset_name,
            evaluators=[results_evaluator, llm_evaluator],
            # Name of the experiment
            experiment_prefix=EXPERIMENT_PREFIX, 
            max_concurrency=2
        )

if __name__ == "__main__":
    asyncio.run(main())

