import re
import sys
import pathlib
import argparse
from collections import Counter

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent.parent))
from rdf_assistant.eval.doremus_dataset import examples_queries

def remove_comments(query):
    lines = query.splitlines()
    cleaned = []
    for line in lines:
        if "#" in line:
            pass
        cleaned.append(line)
    return "\n".join(cleaned) # Placeholder, actual logic below

def count_hops(query):
    """
    Calculates the complexity of a SPARQL query by counting 'hops'.
    
    A hop is defined as a transition in the graph pattern. The calculation is based on:
    1. Property Paths: Each occurrence of '/' in a property path counts as additional hops.
    2. Triples: Each explicit triple statement counts as a hop. This is estimated by counting 
       sentence delimiters ('.', ';', ',') that signify the completion of a triple pattern component.
       
    The method:
    - Removes comments.
    - Extracts the main query body (content within outer {}).
    - Masks strings and URIs to avoid false positives with punctuation.
    - Counts occurrences of '.', ';', ',' (representing statements/branches) and '/' (path sequences).
    """
    
    # 1. Remove comments
    query = re.sub(r'(?m)^#.*\n?', '', query)
    
    # 2. Extract content inside WHERE { ... } or just the whole body
    start = query.find('{')
    end = query.rfind('}')
    
    if start != -1 and end != -1:
        body = query[start+1:end]
    else:
        body = query

    # 3. Mask strings and URIs to avoid parsing characters inside them
    # Mask strings: "..."
    body = re.sub(r'"[^"\\]*(?:\\.[^"\\]*)*"', ' "STR" ', body)
    
    # Mask URIs: <...> (to avoid counting slashes inside URIs)
    body = re.sub(r'<[^>]*>', ' <URI> ', body)

    # 4. Remove nested structures
    # Removing content inside parentheses (FILTERs, etc) simplifies analysis
    prev_len = -1
    while len(body) != prev_len:
        prev_len = len(body)
        body = re.sub(r'\([^\(\)]*\)', ' ', body) 

    # 5. Calculate Hops
    hops = 0
    
    # Count Property Paths (slashes)
    path_slashes = body.count('/') 
    hops += path_slashes
    
    # Pad punctuation for reliable counting
    for char in ['.', ';', ',']:
        body = body.replace(char, f' {char} ')
    
    # Count delimiters acting as triple terminators/connectors
    count_dot = body.count(' . ')
    count_semi = body.count(' ; ')
    count_comma = body.count(' , ')
    
    hops += count_dot + count_semi + count_comma
    
    return hops

def calculate_split(metadata, query):
    # 1. Check Impossible (case-insensitive)
    existing_split = metadata.get("split")
    if existing_split and existing_split.lower() == "impossible":
        return "impossible"
        
    # Clean query for analysis
    q_upper = query.upper()
    
    # 2. Check for keywords: GROUP BY, COUNT, ORDER BY
    # Use word boundaries
    has_group = bool(re.search(r'\bGROUP BY\b', q_upper))
    has_count = bool(re.search(r'\bCOUNT\b', q_upper))
    has_order = bool(re.search(r'\bORDER BY\b', q_upper))
    
    is_complex_op = has_group or has_count or has_order
    
    # 3. Calculate Hops
    hops = count_hops(query)
    
    # 4. Determine Split
    # "easy": query without the groupby or count or order by and number of hop <= 2
    if not is_complex_op and hops <= 5:
        return "easy"
        
    # "medium": query with number of hop >2 and no groupby order by or count
    if not is_complex_op and hops > 5 and hops <= 13:
        return "medium"
        
    # "hard": all the remaining (implies is_complex_op is True)
    return "hard"

def update_file_split(file_path, new_split):
    path = pathlib.Path(file_path)
    if not path.exists():
        return
    
    with open(path, 'r') as f:
        lines = f.readlines()
    
    new_lines = []
    split_pattern = re.compile(r'^\s*#\s*split\s*:', re.IGNORECASE)
    split_updated = False
    
    for line in lines:
        if split_pattern.match(line):
            new_lines.append(f'# split: "{new_split}"\n')
            split_updated = True
        else:
            new_lines.append(line)
            
    if not split_updated:
        # If not found, try to insert after category
        cat_pattern = re.compile(r'^\s*#\s*category\s*:', re.IGNORECASE)
        final_lines = []
        inserted = False
        for line in new_lines: # It's actually just lines copy if not updated
            final_lines.append(line)
            if not inserted and cat_pattern.match(line):
                final_lines.append(f'# split: "{new_split}"\n')
                inserted = True
        
        if not inserted:
             # Prepend if no category found
             final_lines.insert(0, f'# split: "{new_split}"\n')
        new_lines = final_lines

    with open(path, 'w') as f:
        f.writelines(new_lines)

def main():
    parser = argparse.ArgumentParser(description="Calculate and update dataset splits.")
    parser.add_argument("--write", action="store_true", help="Write the calculated split back to the .rq files.")
    args = parser.parse_args()

    print(f"Loaded {len(examples_queries)} examples.\n")
    
    counts = Counter()
    
    for example in examples_queries:
        query = example["outputs"]["rdf_query"]
        metadata = example["metadata"]
        question = example["inputs"]["query_input"]
                
        split = calculate_split(metadata, query)
        
        # Calculate hops for display (skip for impossible)
        if split == "impossible":
            hops = "N/A"
        else:
            hops = count_hops(query)
        
        # Update file if requested
        if args.write:
            update_file_split(metadata['file_path'], split)
            print(f"Updated split to: {split}")

        # Print info
        print(f"File: {pathlib.Path(metadata['file_path']).name}")
        print(f"Question: {question}")
        print(f"Hops: {hops}")
        print(f"Old Split: {metadata.get('split')}")
        print(f"New Split: {split}")
        print("-" * 20)
        
        counts[split] += 1
        
    print("\nSummary of Splits:")
    for split_name, count in counts.items():
        print(f"{split_name}: {count}")

if __name__ == "__main__":
    main()
