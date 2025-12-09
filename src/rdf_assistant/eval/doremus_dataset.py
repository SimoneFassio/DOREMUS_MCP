import json
import pathlib
import numpy as np

# Read cq json file from data directory
project_root = pathlib.Path(__file__).parent.parent.parent.parent
cq_path = project_root / "data" / "cq.json"

with open(cq_path, "r") as f:
    cq_data = json.load(f)

"""Text-to-SPARQL evaluation dataset with ground truth classifications."""

#Dataset examples
examples_queries = [
    {
        "inputs": {"query_input":data["question"]}, 
        "outputs": {"rdf_query":data["query"]}
        } 
    for data in cq_data]

# Shuffle the examples
np.random.shuffle(examples_queries)

def test_print_examples():
    """Prints the examples in the evaluation dataset."""
    for example in examples_queries:
        print("Input Question:", example["inputs"]["query_input"])
        print("Expected SPARQL Query:", example["outputs"]["rdf_query"])
        print("-----")

if __name__ == "__main__":
    test_print_examples()