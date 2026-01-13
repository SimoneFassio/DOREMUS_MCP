import json
import argparse
import os

def export_to_text(input_file):
    if not os.path.exists(input_file):
        print(f"Error: File {input_file} not found.")
        return

    with open(input_file, 'r') as f:
        data = json.load(f)

    # Output filename: replace extension with .txt
    base, _ = os.path.splitext(input_file)
    output_file = f"{base}.txt"

    with open(output_file, 'w') as f:
        for run in data:
            question = run.get('question', 'Unknown')
            tools = run.get('tools', [])
            metrics = run.get('metrics', {})
            output = run.get('output')
            
            # Write Question
            f.write(f'question: "{question}"\n')
            
            # Write Tools
            f.write('tools:\n')
            for tool in tools:
                name = tool.get('name', 'unknown_tool')
                tool_input = tool.get('input', '')
                success = tool.get('success')
                
                # Format input representation if necessary
                # If it's a dict passed as string, it's already a string.
                # If it's a dict object, converting to string is fine.
                
                line = f"- {name}({tool_input})"
                
                # Check for failure
                # "here write ERROR if in the output the content.success=false"
                # If success is None (e.g. not captured) assume ok or ignore?
                # The prompt says explicitly "success=false".
                if success is False:
                    line += " ERROR"
                
                f.write(f"{line}\n")
            
            # Write Query / Output
            query_val = ""
            if isinstance(output, dict):
                query_val = output.get("generated_query", "")
            else:
                query_val = str(output)
            
            f.write(f"query:\n{query_val}\n")
            
            # Write Metrics
            acc = metrics.get('accuracy', 0.0)
            llm_c = metrics.get('llm_is_correct', 0.0)
            # Handle None
            if acc is None: acc = 0.0
            if llm_c is None: llm_c = 0.0
            
            f.write(f"accuracy: {acc}   llm: {llm_c}\n")
            
            # Separator? The user didn't request one explicitly but "...." implies separation.
            # I'll add a newline or separator.
            f.write("\n" + "-"*40 + "\n\n")

    print(f"Exported to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export analysis JSON to readable Text.")
    parser.add_argument("input_file", help="Path to the JSON analysis file")
    
    args = parser.parse_args()
    
    export_to_text(args.input_file)
