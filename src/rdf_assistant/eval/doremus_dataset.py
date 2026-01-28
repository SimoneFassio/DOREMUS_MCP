import re
import pathlib

# Read cq .rq files from data directory
project_root = pathlib.Path(__file__).parent.parent.parent.parent
competency_dir = project_root / "eval_dataset"

def load_rq_files(directory):
    examples = []
    if not directory.exists():
        return examples
        
    for file_path in sorted(directory.glob("*.rq")):
        with open(file_path, "r") as f:
            content = f.read()

        # get the relative path from the project root
        relative_path = file_path.relative_to(project_root)
            
        # Initialize fields
        metadata = {
            "category": None,
            "origin": None,
            "workflow": [],
            "file_path": str(relative_path)
        }
        question = ""
        query_lines = []
        
        # State tracking
        in_workflow = False
        
        lines = content.splitlines()
        for line in lines:
            stripped = line.strip()
            
            # Metadata Headers using Regex
            # Matches: # key: value (with optional spaces)
            
            # Category
            match_cat = re.match(r'^\s*#\s*category\s*:\s*(.*)', stripped, re.IGNORECASE)
            if match_cat:
                metadata["category"] = match_cat.group(1).strip().strip('"')
                in_workflow = False
                continue

            # Split
            match_split = re.match(r'^\s*#\s*split\s*:\s*(.*)', stripped, re.IGNORECASE)
            if match_split:
                metadata["split"] = match_split.group(1).strip().strip('"')
                in_workflow = False
                continue

            # Origin
            match_origin = re.match(r'^\s*#\s*origin\s*:\s*(.*)', stripped, re.IGNORECASE)
            if match_origin:
                metadata["origin"] = match_origin.group(1).strip().strip('"')
                in_workflow = False
                continue

            # Question
            # Try specific regex for question with quotes first
            match_q_quotes = re.search(r'#\s*question\s*:\s*"(.*)"', line, re.IGNORECASE)
            match_q_plain = re.match(r'^\s*#\s*question\s*:\s*(.*)', stripped, re.IGNORECASE)
            
            if match_q_quotes:
                question = match_q_quotes.group(1)
                in_workflow = False
                continue
            elif match_q_plain:
                question = match_q_plain.group(1).strip().strip('"')
                in_workflow = False
                continue

            # Workflow Marker
            if re.match(r'^\s*#\s*workflow\s*', stripped, re.IGNORECASE):
                in_workflow = True
                continue

            # Query Marker
            if re.match(r'^\s*#\s*query\s*:', stripped, re.IGNORECASE):
                in_workflow = False
                continue

            
            # Workflow content
            elif in_workflow:
                 # Clean up workflow line
                 wf_line = stripped.lstrip("#").strip()
                 if wf_line:
                     metadata["workflow"].append(wf_line)
            
            # Query content (non-comment lines that are not empty)
            elif not stripped.startswith("#") and stripped:
                query_lines.append(line)
            
        query = "\n".join(query_lines).strip()
        
        if question and query:
            examples.append({
                "inputs": {"query_input": question},
                "outputs": {"rdf_query": query},
                "metadata": metadata,
            })
    return examples

examples_queries = load_rq_files(competency_dir)

def test_print_examples():
    """Prints the examples in the evaluation dataset."""
    print(f"Loaded {len(examples_queries)} examples.")
    for example in examples_queries[:3]: # Show first 3
        print("Input Question:", example["inputs"]["query_input"])
        print("Metadata:", example["metadata"])
        print("Expected SPARQL Query:\n", example["outputs"]["rdf_query"])
        print("-----")

if __name__ == "__main__":
    test_print_examples()