import asyncio
import json
import os
import sys
import argparse
import subprocess
import time
import logging
from pathlib import Path
import httpx
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, 'src')

from server.utils import execute_sparql_query
from rdf_assistant.eval.doremus_dataset import examples_queries
from rdf_assistant.prompts import agent_system_prompt

# Import LangChain models for the judge
try:
    from langchain_openai import ChatOpenAI
    from langchain_groq import ChatGroq
    from langchain_anthropic import ChatAnthropic
    from langchain_ollama import ChatOllama
except ImportError:
    print("Warning: LangChain libraries not found. LLM evaluation might fail.")

# Load environment variables
# Load environment variables
load_dotenv(".env")
DOREMUS_MCP_URL = os.getenv("DOREMUS_MCP_URL", "http://localhost:8000/mcp")

# Configuration for Judge LLM
provider = os.getenv("LLM_EVAL_PROVIDER", "ollama")
model_name = os.getenv("LLM_EVAL_MODEL", "gpt-oss:120b")

RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(exist_ok=True)

def create_model_judge(provider: str, model_name: str):
    """Create a chat model for evaluation (judge)."""
    if provider == "openai":
        return ChatOpenAI(model=model_name, temperature=0)
    elif provider == "groq":
        return ChatGroq(model=model_name, temperature=0)
    elif provider == "anthropic":
        return ChatAnthropic(model=model_name, temperature=0)
    elif provider == "ollama":
        return ChatOllama(
            base_url=os.getenv("OLLAMA_API_URL"),
            model=model_name,
            client_kwargs={"headers": {"Authorization": f"Basic {os.getenv('OLLAMA_API_KEY')}"}},
            stream=True,
            temperature=0,
            num_ctx=32768,
        )
    else:
        raise ValueError(f"Provider {provider} not supported for judge.")

async def accuracy(outputs: dict, reference_outputs: dict) -> float:
    """Check the percentage of correct values returned by the query."""
    if not outputs.get("generated_query"):
        return 0.0
    
    loop = asyncio.get_running_loop()

    def execute_query_safe(query, limit, retry_limit=None):
        result = execute_sparql_query(query, limit=limit)
        if not result["success"] and retry_limit:
            error_msg = result.get("error", "")
            if "timeout" in error_msg.lower():
                return execute_sparql_query(query, limit=retry_limit)
        return result
    
    # Run blocking reference query
    reference_output_dict = await loop.run_in_executor(
        None, 
        lambda: execute_query_safe(reference_outputs["rdf_query"], limit=10000, retry_limit=100)
    )
    
    if not reference_output_dict["success"] or reference_output_dict is None:
        return 0.0
    reference_output = reference_output_dict["results"]

    # Run blocking generated query
    query_output_dict = await loop.run_in_executor(
        None,
        lambda: execute_query_safe(outputs["generated_query"], limit=100, retry_limit=10)
    )
    
    if not query_output_dict["success"] or query_output_dict is None:
        return 0.0
    query_output = query_output_dict["results"]
    
    if not isinstance(reference_output, list) or not isinstance(query_output, list):
        return 0.0
    
    def extract_uri_values(results):
        uri_values = set()
        if not results:
            return uri_values
        keys = results[0].keys()
        for key in keys:
            col_values = [r.get(key) for r in results if r.get(key)]
            if any(isinstance(v, str) and v.startswith("http") for v in col_values):
                    uri_values.update(col_values)
        return uri_values

    ref_uris = extract_uri_values(reference_output)
    output_uris = extract_uri_values(query_output)
    
    # Fallback if no URIs
    if not ref_uris or not output_uris:
        def extract_all_values(results):
            vals = set()
            if not results: return vals
            for row in results:
                for v in row.values():
                    if v: vals.add(str(v))
            return vals

        ref_vals = extract_all_values(reference_output)
        out_vals = extract_all_values(query_output)
        
        if not out_vals:
            percentage_correct = 100.0 if not ref_vals else 0.0
        else:
            correct = len(out_vals.intersection(ref_vals))
            percentage_correct = (correct / len(out_vals)) * 100.0
        return round(percentage_correct/100, 3)
    
    query_total_uris = 0
    query_correct_uris = 0
    for uri in output_uris:
        query_total_uris += 1
        if uri in ref_uris:
            query_correct_uris += 1
    
    if query_total_uris > 0:
        percentage_correct = (query_correct_uris / query_total_uris) * 100.0
    else:
        percentage_correct = 100.0 if len(ref_uris) == 0 else 0.0
    
    return round(percentage_correct/100, 3)

async def llm_score(outputs: dict, reference_outputs: dict) -> dict:
    """Use an LLM to evaluate the semantic correctness of the generated query."""
    llm = create_model_judge(provider, model_name)
    generated_query = outputs.get("generated_query", "")
    reference_query = reference_outputs["rdf_query"]
    
    if not generated_query:
        return {"score": 0, "comment": "No query generated"}

    prompt = f"""
    You are an expert SPARQL query evaluator. Compare the GENERATED SPARQL query with the REFERENCE SPARQL query.
    
    REFERENCE QUERY:
    ```sparql
    {reference_query}
    ```
    
    GENERATED QUERY:
    ```sparql
    {generated_query}
    ```
    
    Task:
    1. Ignore minor whitespace or limit differences.
    2. Check if the intent filters and selection variables are semantically equivalent.
    3. Check if the graph patterns match the same logic.
    
    Output a JSON object with:
    - "score": A number between 0 and 1.
    - "reasoning": A brief explanation.
    
    Only output the JSON.
    """
    
    try:
        response = await llm.ainvoke(prompt)
        content = response.content
        clean_content = content.strip().replace("```json", "").replace("```", "").strip()
        data = json.loads(clean_content)
        return {"score": round(data.get("score", 0), 2), "comment": "".join(data.get("reasoning", ""))}
    except Exception as e:
        return {"score": 0, "comment": f"Evaluation failed: {e}"}

async def llm_is_correct(outputs: dict, reference_outputs: dict) -> dict:
    """Use an LLM to evaluate if the query is practically correct."""
    llm = create_model_judge(provider, model_name)
    generated_query = outputs.get("generated_query", "")
    reference_query = reference_outputs["rdf_query"]
    
    if not generated_query:
        return {"score": 0, "comment": "No query generated"}

    prompt = f"""
    You are an expert SPARQL query evaluator. Determine if the GENERATED SPARQL query is effectively CORRECT compared to the REFERENCE SPARQL query.

    REFERENCE QUERY:
    ```sparql
    {reference_query}
    ```
    
    GENERATED QUERY:
    ```sparql
    {generated_query}
    ```
    
    Output a JSON object with:
    - "is_correct": boolean
    - "reasoning": A brief explanation.
    
    Only output the JSON.
    """
    
    try:
        response = await llm.ainvoke(prompt)
        content = response.content
        clean_content = content.strip().replace("```json", "").replace("```", "").strip()
        data = json.loads(clean_content)
        is_correct = data.get("is_correct", False)
        return {"score": 1 if is_correct else 0, "comment": "".join(data.get("reasoning", ""))}
    except Exception as e:
        return {"score": 0, "comment": f"Evaluation failed: {e}"}




def run_gemini_query(prompt: str, max_tool_calls: int = 10):
    """
    Executes a query using the gemini CLI in streaming mode and captures output.
    """
    command = [
        "gemini", "-p", prompt,
        "--output-format", "stream-json",
        "--yolo", "--model", "gemini-3-pro-preview"
    ]
    
    # Structure to return
    result = {
        "final_text_response": "",
        "generated_query": None,
        "query_id": None,
        "raw_events": [],
        "called_tools": []
    }

    tool_id_map = {} # Map tool_id -> tool_name

    try:
        # bufsize=1 means line buffered
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
        
        # Read stdout line by line
        for line in iter(process.stdout.readline, ''):
            if not line.strip():
                continue
            
            try:
                event = json.loads(line)
                result["raw_events"].append(event)
                
                event_type = event.get("type")
                
                if event_type == "message":
                    # Capture Text
                    role = event.get("role")
                    content = event.get("content", "")
                    if role == "assistant" or role == "model":
                         result["final_text_response"] += content
                
                elif event_type == "tool_use":
                    t_name = event.get("tool_name")
                    t_id = event.get("tool_id")
                    t_args = event.get("parameters", {})
                    
                    if t_id:
                        tool_id_map[t_id] = t_name

                    # Check tool limit BEFORE adding or executing
                    # The CLI usually emits tool_use when it DECIDES to use a tool.
                    # Since we use --yolo, it auto-executes.
                    # We can't prevent the CURRENT tool from executing if the CLI already sent it,
                    # but we can kill the process to stop FUTURE tools.
                    
                    result["called_tools"].append({
                        "name": t_name,
                        "args": t_args,
                        "id": t_id,
                        "status": "called"
                    })

                    # Extract query_id from args if available
                    if "query_id" in t_args and t_args["query_id"]:
                         result["query_id"] = t_args["query_id"]
                    
                    if len(result["called_tools"]) > max_tool_calls:
                        print(f"⚠️ Max tool calls ({max_tool_calls}) exceeded. Terminating Gemini CLI.")
                        process.terminate()
                        break

                    # Fallback: if tool is execute_query, check args for query (unlikely in this setup but possible)
                    if t_name == "execute_query":
                         q = t_args.get("query") or t_args.get("sparql")
                         if q:
                             result["generated_query"] = q

                elif event_type == "tool_result":
                    t_id = event.get("tool_id")
                    t_output_str = event.get("output", "")
                    t_name = tool_id_map.get(t_id)
                    
                    # Parse inner JSON output
                    try:
                        t_output = json.loads(t_output_str)
                    except (json.JSONDecodeError, TypeError):
                        t_output = t_output_str

                    # Check for generated_query and query_id in the output of the tool
                    if isinstance(t_output, dict):
                        if "generated_query" in t_output:
                             # We update generated_query. 
                             # Later tools might overwrite earlier ones (e.g. build_query -> add_select -> execute_query)
                             # This is desired as we want the final version.
                             result["generated_query"] = t_output["generated_query"]
                        if "query_id" in t_output and t_output["query_id"]:
                             result["query_id"] = t_output["query_id"]

            except json.JSONDecodeError:
                pass

        process.wait()
    except Exception as e:
        print(f"Error running Gemini CLI: {e}")
        
    return result

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--question", type=str, help="Run with custom question")
    args = parser.parse_args()

    examples_to_run = []
    
    if args.question:
        # Just run this one custom case. 
        # Since we might not have ground truth, evaluation metrics might be 0 or skipped.
        # We will set a dummy reference query to avoid crashes, but warn.
        examples_to_run = [{
            "inputs": {"query_input": args.question},
            "outputs": {"rdf_query": ""}
        }] 
    else:
        examples_to_run = examples_queries

    graph_recursion_limit = int(os.getenv("GRAPH_RECURSION_LIMIT", "10"))
    max_tool_calls = graph_recursion_limit // 2
    print(f"Recursion Limit: {graph_recursion_limit}, Max Tool Calls: {max_tool_calls}")

    print(f"Starting evaluation on {len(examples_to_run)} examples...")
    
    full_results = []
    
    for i, example in enumerate(examples_to_run):
        question = example["inputs"]["query_input"]
        reference_query = example["outputs"].get("rdf_query", "")
        
        print(f"\n[{i+1}/{len(examples_to_run)}] Testing: {question}")
        
        # Prepare Prompt
        prompt = f"""
{agent_system_prompt}

User's question: "{question}"
"""
        
        # Run Gemini
        start_time = time.time()
        gemini_result = run_gemini_query(prompt, max_tool_calls=max_tool_calls)
        duration = time.time() - start_time
        
        # Fetch Sampling Logs
        sampling_requests = []
        if gemini_result["query_id"]:
             try:
                # Assumes DOREMUS_MCP_URL ends with /mcp, so we strip it to get base url
                base_url = DOREMUS_MCP_URL[:-4] if DOREMUS_MCP_URL.endswith("/mcp") else DOREMUS_MCP_URL
                url = f"{base_url}/sampling/{gemini_result['query_id']}"
                async with httpx.AsyncClient() as client:
                    resp = await client.get(url, timeout=4.0)
                    if resp.status_code == 200:
                        sampling_requests = resp.json()
             except Exception as e:
                print(f"Failed to fetch sampling logs: {e}")

        # Evaluate
        metrics = {
            "accuracy": 0.0,
            "llm_score": {"score": 0, "comment": ""},
            "llm_is_correct": {"score": 0, "comment": ""}
        }
        
        if gemini_result["generated_query"] and reference_query:
            # We have both, run metrics
            eval_outputs = {"generated_query": gemini_result["generated_query"]}
            ref_outputs = {"rdf_query": reference_query}
            
            metrics["accuracy"] = await accuracy(eval_outputs, ref_outputs)
            metrics["llm_score"] = await llm_score(eval_outputs, ref_outputs)
            metrics["llm_is_correct"] = await llm_is_correct(eval_outputs, ref_outputs)

        # Compile Result
        record = {
            "question": question,
            "success": bool(gemini_result["generated_query"]),
            "metrics": metrics,
            "generated_query": gemini_result["generated_query"],
            "tool_calls": gemini_result["called_tools"],
            "final_answer": gemini_result["final_text_response"],
            "duration": duration,
            "raw_events": gemini_result["raw_events"],
            "sampling_requests": sampling_requests
        }
        
        full_results.append(record)
        
        # Live reporting
        print(f"  > Generated Query: {'YES' if record['generated_query'] else 'NO'}")
        print(f"  > Accuracy: {metrics['accuracy']}")
        print(f"  > LLM Score: {metrics['llm_score']['score']}")
        print(f"  > Is Correct: {metrics['llm_is_correct']['score']}")
        
        # Save individually
        with open(RESULTS_DIR / f"result_{i}.json", "w") as f:
            json.dump(record, f, indent=2)

    # Save summary
    summary_path = RESULTS_DIR / "evaluation_summary.json"
    with open(summary_path, "w") as f:
        json.dump(full_results, f, indent=2)
    
    print(f"\nEvaluation complete. Results saved to {RESULTS_DIR}")

if __name__ == "__main__":
    asyncio.run(main())
