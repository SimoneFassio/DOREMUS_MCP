import asyncio
import json

from rdf_assistant.doremus_assistant import doremus_assistant, client
from rdf_assistant.eval.doremus_dataset import examples_queries

ClientEvalList = ["openai", "groq", "anthropic", "mistral"]

async def main():
    for provider in ClientEvalList:
        # CLIENT INITIALIZATION
        # doremus_assistant, client = await create_doremus_assistant(provider=provider)

        # DATASET CREATION
        dataset_name = "Competency Query Evaluation Dataset"
        # Create dataset if it doesn't exist
        if not client.has_dataset(dataset_name=dataset_name):
            dataset = client.create_dataset(
                dataset_name=dataset_name, 
                description="A dataset of competency questions and their SPARQL queries."
            )
            # Add examples to the dataset
            client.create_examples(dataset_id=dataset.id, examples=examples_queries)

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
                if hasattr(message, "name") and message.name == "execute_custom_sparql_tool":
                    content_json = json.loads(messContent)
                    generated_query = content_json.get("generated_query", "")
                    if generated_query:
                        return {"generated_query": generated_query}

        # TODO: expand evaluation function to take into account different queries that are correct but phrased differently
        def query_evaluator(outputs: dict, reference_outputs: dict) -> bool:
            """Check if the answer exactly matches the expected answer."""
            return outputs["generated_query"].lower() == reference_outputs["rdf_query"].lower()

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

