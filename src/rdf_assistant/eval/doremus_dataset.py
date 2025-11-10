import json

# Read cq json file
with open("cq.json", "r") as f:
    cq_data = json.load(f)

"""Text-to-SPARQL evaluation dataset with ground truth classifications."""

#Dataset examples
examples_queries = [
    {
        "inputs": {"query_input":data["question"]}, 
        "outputs": {"rdf_query":data["query"]}
        } 
    for data in cq_data]

def test_print_examples():
    """Prints the examples in the evaluation dataset."""
    for example in examples_queries:
        print("Input Question:", example["inputs"]["query_input"])
        print("Expected SPARQL Query:", example["outputs"]["rdf_response"])
        print("-----")

if __name__ == "__main__":
    test_print_examples()