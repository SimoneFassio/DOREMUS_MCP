import os
import sys
import argparse
import asyncio
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, 'src')

# Import client from existing module to share config/auth
from rdf_assistant.doremus_assistant import client
from rdf_assistant.eval.doremus_dataset import examples_queries

load_dotenv(".env")

def create_dataset_script():
    parser = argparse.ArgumentParser(description="Create and populate the LangSmith dataset.")
    parser.add_argument(
        "--dataset-name", 
        type=str, 
        default=os.getenv("EVALUATION_DATASET_NAME", "Doremus Dataset"),
        help="Name of the dataset to create"
    )
    args = parser.parse_args()
    
    dataset_name = args.dataset_name
    print(f"Creating dataset: {dataset_name}")

    # Check and delete existing dataset
    if client.has_dataset(dataset_name=dataset_name):
        print(f"Dataset '{dataset_name}' already exists. Deleting it to refresh...")
        dataset = client.read_dataset(dataset_name=dataset_name)
        client.delete_dataset(dataset_id=dataset.id)
        print("Deleted old dataset.")

    # Create new dataset
    dataset = client.create_dataset(
        dataset_name=dataset_name, 
        description="A dataset of competency and user questions with SPARQL queries and metadata."
    )
    print(f"Created dataset '{dataset_name}' with ID: {dataset.id}")

    # Prepare data
    inputs = [ex["inputs"] for ex in examples_queries]
    outputs = [ex["outputs"] for ex in examples_queries]
    metadatas = [ex["metadata"] for ex in examples_queries]

    # Validate data count
    print(f"Found {len(examples_queries)} examples to upload.")

    # Batch creation
    # client.create_examples handles batching internally or is fast enough for hundreds of examples
    client.create_examples(
        dataset_id=dataset.id,
        inputs=inputs,
        outputs=outputs,
        metadata=metadatas,
    )
    
    print("Successfully uploaded all examples.")
    print("Examples preview:")
    for i in range(min(3, len(metadatas))):
        print(f"[{i}] Input: {inputs[i]['query_input'][:50]}...")

if __name__ == "__main__":
    create_dataset_script()
