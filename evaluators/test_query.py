import asyncio
import json
import numpy as np

from src.rdf_assistant.doremus_assistant import doremus_assistant, client
from src.rdf_assistant.eval.doremus_dataset import examples_queries

from src.server.utils import execute_sparql_query

import logging

# Suppress httpx INFO logs
logging.getLogger("httpx").setLevel(logging.WARNING)

# Set to True to reload the dataset and update it
RELOAD = False

ClientEvalList = ["openai", "groq", "anthropic", "mistral"]

async def main():
    # DATASET CREATION
    dataset_name = "Competency Query Evaluation Dataset - 2.0"
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
            if hasattr(message, "name") and message.name == "execute_sparql":
                content_json = json.loads(messContent)
                generated_query = content_json.get("generated_query", "")
                if generated_query:
                    return {"generated_query": generated_query or ""}

    def query_evaluator(outputs: dict, reference_outputs: dict) -> float:
        """Check the percentage of correct values returned by the query."""
        if not outputs.get("generated_query"):
            print("Error: 'generated_query' is missing or empty in outputs.")
            return 0.0
        
        reference_output_dict = execute_sparql_query(reference_outputs["rdf_query"], limit=10000)
        if not reference_output_dict["success"] or reference_output_dict is None:
            print("Error: Failed to execute reference SPARQL query.")
            return 0.0
        reference_output = reference_output_dict["results"]

        query_output_dict = execute_sparql_query(outputs["generated_query"], limit=100)
        if not query_output_dict["success"] or query_output_dict is None:
            print("Error: Failed to execute generated SPARQL query.")
            return 0.0
        query_output = query_output_dict["results"]

        # Ensure both outputs are lists for comparison
        if not isinstance(reference_output, list) or not isinstance(query_output, list):
            print("Error: Outputs are not lists.")
            return 0.0
        
        reference_array = np.array(reference_output)
        query_array = np.array(query_output)

        correct_matches = np.isin(query_array, reference_array)

        # Percentage of correct values
        if len(reference_array) == 0:
            if len(query_array) == 0:
                percentage_correct = 100.0
            else:
                percentage_correct = 0.0
        else:
            if len(query_array) != 0:
                percentage_correct = (np.sum(correct_matches) / len(query_array)) * 100 if len(reference_array) > 0 else 100.0
            else:
                percentage_correct = 0.0
        
        ending_ref = "...]" if len(reference_output) > 2 else ""
        ending_que = "...]" if len(query_output) > 2 else ""
        print(f"\nThe reference Output is: {reference_output[:2]}", ending_ref)
        print(f"\nThe query Output is: {query_output[:2]}", ending_que)
        print(f"\nPercentage of correct values: {percentage_correct:.2f}%")
        
        return percentage_correct

    # RUN EVALUATION
    run_expt = True
    if run_expt:
        evaluation = await client.aevaluate(
            target_doremus_assistant,
            data=dataset_name,
            evaluators=[query_evaluator],
            # Name of the experiment
            experiment_prefix="Doremus Competency Query Evaluation", 
            max_concurrency=2
        )

if __name__ == "__main__":
    asyncio.run(main())

