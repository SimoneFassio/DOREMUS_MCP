import re
import pathlib
import numpy as np

# Read cq .rq files from data directory
project_root = pathlib.Path(__file__).parent.parent.parent.parent
competency_dir = project_root / "data" / "competency_questions"
user_dir = project_root / "data" / "user_questions"

def load_rq_files(directory):
    examples = []
    if not directory.exists():
        return examples
        
    for file_path in sorted(directory.glob("*.rq")):
        with open(file_path, "r") as f:
            content = f.read()
            
        # Parse headers
        question = ""
        query_lines = []
        is_query_body = False
        
        for line in content.splitlines():
            if line.startswith("# question:"):
                # Extract question content, handling quotes
                match = re.search(r'# question:\s*"(.*)"', line)
                if match:
                    question = match.group(1)
                else:
                    # Fallback if quotes are missing or malformed
                    question = line.replace("# question:", "").strip().strip('"')
            elif line.startswith("# query:"):
                is_query_body = True
            elif is_query_body:
                query_lines.append(line)
            # Ignore other headers like category for now
            
        query = "\n".join(query_lines).strip()
        
        if question and query:
            examples.append({
                "inputs": {"query_input": question},
                "outputs": {"rdf_query": query}
            })
    return examples

cq_examples = load_rq_files(competency_dir)
user_examples = load_rq_files(user_dir)

examples_queries = user_examples + cq_examples 

def test_print_examples():
    """Prints the examples in the evaluation dataset."""
    print(f"Loaded {len(examples_queries)} examples.")
    for example in examples_queries:
        print("Input Question:", example["inputs"]["query_input"])
        print("Expected SPARQL Query:\n ", example["outputs"]["rdf_query"])
        print("-----")

if __name__ == "__main__":
    test_print_examples()