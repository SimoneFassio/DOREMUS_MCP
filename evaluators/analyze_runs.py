
import os
import asyncio
import json
import time
import argparse
from dotenv import load_dotenv
from langsmith import Client

# Load environment variables
load_dotenv(".env")

# Initialize LangSmith client with explicit API Key
api_key = os.getenv("LANGSMITH_API_KEY")
if not api_key:
    # Try looking in general environment if not found in .env
    pass 

client = Client(api_key=api_key)

def analyze_runs(project_name: str = None, limit: int = None):
    """
    Fetch and analyze runs from LangSmith.
    If limit is None, fetches all runs (default behavior of list_runs if limit kwarg not passed? No, client.list_runs defaults typically to 10 or 100 if not specified? 
    Actually LangSmith client documentation says limit defaults to 100 often. 
    But if we want ALL, we should probably set a high limit or handle pagination loops if the client doesn't auto-paginate indefinitely. 
    The python client's `list_runs` returns an iterator that auto-paginates. 
    If we pass `limit=None`, it might mean "no limit". Let's assume `limit` parameter in `list_runs` controls this.
    """
    
    if not project_name:
        project_name = os.getenv("EVALUATION_DATASET_NAME", "default")
        # Logic to find prefix if default not found
        all_projects = [p.name for p in client.list_projects()]
        if project_name not in all_projects:
            print(f"Project '{project_name}' not found via env var. Available projects: {all_projects}")
            prefix = os.getenv("EXPERIMENT_PREFIX")
            if prefix:
                 matches = [p for p in all_projects if p.startswith(prefix)]
                 if matches:
                     print(f"Switching to most recent matching project: {matches[0]}")
                     project_name = matches[0]
                 else:
                     print("No matching project found for EXPERIMENT_PREFIX.")
                     return []
            else:
                print("EXPERIMENT_PREFIX not set.")
                return []
    else:
        # User provided a project name, verify it exists or use it directly
        all_projects = [p.name for p in client.list_projects()]
        if project_name not in all_projects:
            print(f"Project '{project_name}' NOT found in LangSmith. Available projects:")
            for p in all_projects:
                print(f" - {p}")
            # Try fuzzy matching or prefix matching if user provided a partial name?
            # For now, strict match or startswith
            matches = [p for p in all_projects if p.startswith(project_name)]
            if matches:
                 print(f"Did you mean: {matches[0]}? Using it.")
                 project_name = matches[0]
            else:
                 return []
    
    print(f"Analyzing runs for project: {project_name}")
    print(f"Limit: {'ALL' if limit is None else limit}")
    
    # Strategy: Fetch ALL runs for the project in one go to avoid N+1 API calls.
    # We will group them by trace_id in memory.
    start_time = time.time()
    try:
        print("Fetching all runs from LangSmith (this may take a moment)...")
        # Ensure we fetch everything by not setting execution_order
        all_runs = list(client.list_runs(
            project_name=project_name,
            error=False
        ))
    except Exception as e:
        print(f"Error fetching runs: {e}")
        return []

    fetch_time = time.time() - start_time
    print(f"Fetched {len(all_runs)} items in {fetch_time:.2f}s. Grouping by trace...")
    
    # Group by trace_id
    traces = {}
    for run in all_runs:
        t_id = run.trace_id
        if t_id not in traces:
            traces[t_id] = []
        traces[t_id].append(run)
    
    print(f"Found {len(traces)} unique traces.")
    
    results = []
    
    
    # Sort traces by time (using start_time of the first/root run)
    # Finding root for each trace
    stats = []
    for t_id, runs_in_trace in traces.items():
        # Identify root: usually parent_run_id is None
        root = next((r for r in runs_in_trace if r.parent_run_id is None), None)
        if not root:
            # Fallback: earliest run
            runs_in_trace.sort(key=lambda x: x.start_time)
            root = runs_in_trace[0]
        
        stats.append((root, runs_in_trace))
        
    runs_in_trace.sort(key=lambda x: x.start_time)
    
    # Sort by root start time
    stats.sort(key=lambda x: x[0].start_time)
    
    # Apply limit
    if limit:
        stats = stats[:limit]
        
    # Prepare to fetch feedback for all roots to avoid N+1?
    # client.list_feedback can take multiple run_ids?
    # Signature: run_ids: Optional[Sequence[ID]] = None
    # Yes.
    root_ids = [root.id for root, _ in stats]
    print(f"Fetching feedback/metrics for {len(root_ids)} runs...")
    
    # Fetch all feedback in batch if possible or loop if list is too huge?
    # LangSmith usually handles list_feedback well.
    all_feedback = []
    try:
        # Note: client.list_feedback returns iterator
        for fb in client.list_feedback(run_ids=root_ids):
             all_feedback.append(fb)
    except Exception as e:
        print(f"Error fetching feedback: {e}")
    
    # Group feedback by run_id
    feedback_map = {}
    for fb in all_feedback:
        r_id = fb.run_id
        if r_id not in feedback_map:
            feedback_map[r_id] = {}
        # Store score/value. key is fb.key
        # Check if score exists
        val = fb.score if fb.score is not None else fb.value
        feedback_map[r_id][fb.key] = val

    simple_results = []
    
    dataset_name = os.getenv("EVALUATION_DATASET_NAME", "unknown_dataset")

    for i, (root, runs_in_trace) in enumerate(stats):
        print(f"Processing Trace {i+1}/{len(stats)} (ID: {root.id})")
        
        # 1. Question / Inputs
        inputs = root.inputs
        question = "Unknown"
        if isinstance(inputs, dict):
            if "messages" in inputs:
                 messages = inputs["messages"]
                 if isinstance(messages, list) and len(messages) > 0:
                     last_msg = messages[-1]
                     if isinstance(last_msg, dict):
                         question = last_msg.get("content", "")
                     elif hasattr(last_msg, "content"):
                         question = last_msg.content
                     else:
                         question = str(last_msg)
            elif "input" in inputs:
                question = inputs["input"]
            elif "query_input" in inputs:
                question = inputs["query_input"]
            elif "question" in inputs:
                question = inputs["question"]
            elif "inputs" in inputs:
                 inner = inputs["inputs"]
                 if isinstance(inner, dict):
                     question = inner.get("query_input", str(inner))
                 else:
                     question = str(inner)
        
        print(f"QUESTION: {question}")
        
        # 2. Process Tools
        runs_in_trace.sort(key=lambda x: x.start_time)
        filetered_tools = []
        
        for tr in runs_in_trace:
            if tr.run_type == "tool":
                # Extract input
                t_in = tr.inputs
                if isinstance(t_in, dict):
                    # Flatten if single key 'input' which is common
                    if "input" in t_in and len(t_in) == 1:
                         t_in = t_in["input"]
                
                # Extract output details
                t_out = tr.outputs
                success = None
                content = None
                
                if t_out and isinstance(t_out, dict):
                    # Check for nested 'output' key pattern from previous JSON
                    if "output" in t_out:
                        inner = t_out["output"]
                        if isinstance(inner, dict):
                            success = inner.get("status") == "success"
                            content = inner.get("content")
                        else:
                            content = inner
                    else:
                        content = str(t_out)
                else:
                    content = str(t_out)

                filetered_tools.append({
                    "name": tr.name,
                    "input": t_in,
                    "success": success,
                    "content": content
                })
        
        # 3. Output (Final Answer)
        final_output = root.outputs
        
        # 4. Metrics
        run_metrics = feedback_map.get(root.id, {})
        # Filter for specific keys if desired, or all? User asked for accuracy and llm_is_correct
        relevant_metrics = {
            "accuracy": run_metrics.get("accuracy"),
            "llm_is_correct": run_metrics.get("llm_is_correct")
        }
        
        simple_results.append({
            "run_id": str(root.id),
            "question": question,
            "output": final_output,
            "metrics": relevant_metrics,
            "tools": filetered_tools
        })

    # Save logic
    # Create experiments folder
    out_dir = "experiments"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
        
    # Safe filename: replace spaces/slashes
    safe_proj = project_name.replace(" ", "_").replace("/", "-").replace(":", "-")
    safe_data = dataset_name.replace(" ", "_").replace("/", "-")
    
    filename = f"{out_dir}/{safe_data}_{safe_proj}.json"
    
    return simple_results, filename

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze LangSmith runs.")
    parser.add_argument("project_name", nargs="?", help="Name of the LangSmith project to analyze")
    parser.add_argument("--limit", type=int, default=None, help="Number of runs to fetch (default: all)")
    
    args = parser.parse_args()
    
    # Check if project name is provided or defaults
    # Re-run logic mostly inside, but if we want to print usage...
    
    results = []
    outfile = ""
    
    try:
        data, name = analyze_runs(project_name=args.project_name, limit=args.limit)
        results = data
        outfile = name
    except Exception as e:
        print(f"Analysis failed: {e}")
        exit(1)
        
    if results:
        with open(outfile, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\nAnalysis saved to {outfile}")
        
        # Auto-export to human readable text
        try:
            # Import here to avoid circular dependencies or path issues if placed at top contextually
            import export_human_readable
            export_human_readable.export_to_text(outfile)
        except Exception as e:
            print(f"Failed to auto-export text format: {e}")
            
    else:
        print("No data collected.")
