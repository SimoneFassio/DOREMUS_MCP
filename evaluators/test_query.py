import asyncio
import json
import os
import sys
import logging
import httpx
from dotenv import load_dotenv
from langgraph.errors import GraphRecursionError

# Add src to path for local development
sys.path.insert(0, 'src')

from rdf_assistant.doremus_assistant import doremus_assistant, client, create_model, provider, model_name
from rdf_assistant.eval.doremus_dataset import examples_queries
from server.utils import execute_sparql_query

# Suppress httpx INFO logs
logging.getLogger("httpx").setLevel(logging.WARNING)

load_dotenv(".env")

EXPERIMENT_PREFIX = os.getenv("EXPERIMENT_PREFIX", "")
DOREMUS_MCP_URL = os.getenv("DOREMUS_MCP_URL", "http://localhost:8000/mcp")

# Set to True to reload the dataset and update it
RELOAD = False

async def main():
    # DATASET CREATION
    dataset_name = os.getenv( "EVALUATION_DATASET_NAME","Default Dataset")
    if RELOAD:
        if client.has_dataset(dataset_name=dataset_name):
            print("Reloading Dataset")
            dataset = client.read_dataset(dataset_name=dataset_name)
            client.delete_dataset(dataset_id=dataset.id)
            print(f"Dataset '{dataset_name}' has been deleted.")

    # Create dataset if it doesn't exist
    if not client.has_dataset(dataset_name=dataset_name):
        print("Created Dataset")
        dataset = client.create_dataset(
            dataset_name=dataset_name, 
            description="A dataset of competency questions and their SPARQL queries."
        )
        # Add examples to the dataset
        inputs = [example["inputs"] for example in examples_queries]
        outputs = [example["outputs"] for example in examples_queries]

        client.create_examples(dataset_id=dataset.id, inputs=inputs, outputs=outputs)


    async def target_doremus_assistant(inputs: dict) -> dict:
        """Process a user input through the doremus assistant and capture the generated query."""
        messages = []
        try:
            # Use astream with stream_mode="values" to capture state updates
            # This allows us to retain the latest messages even if recursion limit is hit
            async for chunk in doremus_assistant.astream(
                {"messages": [{"role": "user", "content": inputs["query_input"]}]},
                stream_mode="values"
            ):
                if "messages" in chunk:
                    messages = chunk["messages"]
        except GraphRecursionError:
            print(f"⚠️ Recursion Limit Hit for: {inputs.get('query_input', '')[:30]}... processing partial messages.")
        except Exception as e:
            print(f"Error during ainvoke: {e}")

        # State tracking
        query_map = {} # query_id -> generated_query content
        last_generated_query = None
        last_query_id = None
        executed_query_id = None
        final_answer = ""
        sampling_requests = []

        for message in messages:
            # 1. Capture Final Answer (Text from AI)
            # We assume the last message with text content from AI is the final answer
            if message.type == "ai" and message.content:
                if isinstance(message.content, str):
                   final_answer = message.content
            
            # 2. Capture Execute Query Tool Calls
            if hasattr(message, "tool_calls") and message.tool_calls:
                for tool_call in message.tool_calls:
                    if tool_call.get("name") == "execute_query":
                        args = tool_call.get("args", {})
                        if "query_id" in args:
                            executed_query_id = args["query_id"]

            # 3. Capture Generated Queries from Tool Outputs
            messContent = message.content if hasattr(message, "content") else ""
            
            # Helper to normalize content to string
            content_str = ""
            if isinstance(messContent, str):
                content_str = messContent
            elif isinstance(messContent, list):
                if not messContent:
                    content_str = ""
                elif all(isinstance(x, str) for x in messContent):
                    content_str = "".join(messContent)
                elif all(isinstance(x, dict) for x in messContent):
                    content_str = "".join(x.get("text", "") for x in messContent)
                else:
                    content_str = str(messContent)
            
            try:
                content_json = json.loads(content_str)
                # Check for generated_query in the output
                if isinstance(content_json, dict):
                    if "generated_query" in content_json and isinstance(content_json["generated_query"], str):
                        g_query = content_json["generated_query"]
                        last_generated_query = g_query
                        
                        if "query_id" in content_json:
                            q_id = content_json["query_id"]
                            if isinstance(q_id, str):
                                last_query_id = q_id
                                query_map[q_id] = g_query
            except (json.JSONDecodeError, TypeError):
                continue
        
        # DECISION LOGIC:
        final_query = ""
        final_query_id = None

        if executed_query_id and isinstance(executed_query_id, str) and executed_query_id in query_map:
            # Priority 1: The query that was actually executed
            final_query = query_map[executed_query_id]
            final_query_id = executed_query_id
        else:
            # Fallback: The last generated query
            final_query = last_generated_query or ""
            final_query_id = last_query_id


        # Fetch Sampling Logs if query_id exists
        if final_query_id:
            try:
                url = f"{DOREMUS_MCP_URL[:-4]}/sampling/{final_query_id}"
                async with httpx.AsyncClient() as client:
                    resp = await client.get(url, timeout=4.0)
                    if resp.status_code == 200:
                        sampling_requests = resp.json()
            except Exception as e:
                print(f"Failed to fetch sampling logs: {e}")

        return {
            "generated_query": final_query,
            "final_answer": final_answer,
            "sampling_requests": sampling_requests
        }

    async def accuracy(outputs: dict, reference_outputs: dict) -> float:
        """Check the percentage of correct values returned by the query."""
        if not outputs.get("generated_query"):
            print("Error: The LLM couldn't generate the query.")
            return 0.0
        
        loop = asyncio.get_running_loop()

        def execute_query_safe(query, limit, retry_limit=None):
            result = execute_sparql_query(query, limit=limit)
            if not result["success"] and retry_limit:
                error_msg = result.get("error", "")
                if "timeout" in error_msg.lower():
                    print(f"Query timed out with limit {limit}. Retrying with limit {retry_limit}...")
                    return execute_sparql_query(query, limit=retry_limit)
            return result
        
        # Run blocking reference query in executor with retry
        reference_output_dict = await loop.run_in_executor(
            None, 
            lambda: execute_query_safe(reference_outputs["rdf_query"], limit=10000, retry_limit=100)
        )
        
        if not reference_output_dict["success"] or reference_output_dict is None:
            print(f"Error: Failed to execute reference SPARQL query. {reference_output_dict.get('error')}")
            return 0.0
        reference_output = reference_output_dict["results"]

        # Run blocking generated query in executor with retry
        query_output_dict = await loop.run_in_executor(
            None,
            lambda: execute_query_safe(outputs["generated_query"], limit=100, retry_limit=10)
        )
        
        if not query_output_dict["success"] or query_output_dict is None:
            print(f"Error: Failed to execute generated SPARQL query. {query_output_dict.get('error')}")
            return 0.0
        query_output = query_output_dict["results"]
        
        # Ensure both outputs are lists for comparison
        if not isinstance(reference_output, list) or not isinstance(query_output, list):
            print("Error: Outputs are not lists.")
            return 0.0
        
        # Helper to extract URI values from list of dicts
        def extract_uri_values(results):
            uri_values = set()
            if not results:
                return uri_values
            
            # Find columns that look like URIs (check first row as heuristic, or all rows)
            # Checking all rows to be safe, or iterate over keys of first row and check values
            keys = results[0].keys()
            
            for key in keys:
                # collecting all values for this key
                col_values = [r.get(key) for r in results if r.get(key)]
                # Check if majority or any starts with http. Let's assume if it looks like a URI column
                if any(isinstance(v, str) and v.startswith("http") for v in col_values):
                        uri_values.update(col_values)
            return uri_values

        ref_uris = extract_uri_values(reference_output)
        output_uris = extract_uri_values(query_output)
        
        # Special case: If there are no URI detected in one of the two results, try to match directly all possible combination of rows and columns.
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

            print(f" Percentage of correct values (fallback): {percentage_correct:.2f}%")
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
            # No URIs returned, but maybe expected? 
            # If reference also has no URIs, then it's a match (100%), otherwise 0%
            if len(ref_uris) == 0:
                percentage_correct = 100.0
            else:
                percentage_correct = 0.0
        
        ending_ref = "...]" if len(reference_output) > 2 else ""
        ending_que = "...]" if len(query_output) > 2 else ""
        # print(f"\nThe reference Output is: {reference_output[:2]}", ending_ref)
        # print(f"\nThe query Output is: {query_output[:2]}", ending_que)
        print(f" Percentage of correct values: {percentage_correct:.2f}%")
        
        return round(percentage_correct/100, 3)

    async def llm_score(outputs: dict, reference_outputs: dict) -> dict:
        """Use an LLM to evaluate the semantic correctness of the generated query."""
        
        # Instantiate the judge model (using same config as agent)
        llm = create_model(provider, model_name)
        
        generated_query = outputs.get("generated_query", "")
        reference_query = reference_outputs["rdf_query"] # ground truth query
        
        if not generated_query:
            return {"score": 0, "comment": "No query generated"}

        # Define the prompt for the judge
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
        1. Ignore minor whitespace or limit differences (e.g. LIMIT 100 vs LIMIT 50).
        2. Check if the intent filters and selection variables are semantically equivalent.
        3. Check if the graph patterns (triples) match the same logic.
        
        Output a JSON object with:
        - "score": A number between 0 and 1 (1 means semantically equivalent, 0 means completely wrong).
        - "reasoning": A brief explanation of the score where you point out the errors in the generated query.
        
        Only output the JSON.
        """
        
        try:
            response = await llm.ainvoke(prompt)
            content = response.content
            
            # extract json
            clean_content = content.strip()
            if clean_content.startswith("```json"):
                clean_content = clean_content[7:]
            if clean_content.endswith("```"):
                clean_content = clean_content[:-3]
            
            data = json.loads(clean_content.strip())
            return {"score": round(data.get("score", 0), 2), "comment": "".join(data.get("reasoning", ""))}
        except Exception as e:
            print(f"LLM Evaluator Error: {e}")
            return {"score": 0, "comment": f"Evaluation failed: {e}"}
    
    async def llm_is_correct(outputs: dict, reference_outputs: dict) -> dict:
        """Use an LLM to evaluate if the query is practically correct ignoring minor details."""
        
        # Instantiate the judge model (using same config as agent)
        llm = create_model(provider, model_name)
        
        generated_query = outputs.get("generated_query", "")
        reference_query = reference_outputs["rdf_query"] # ground truth query
        
        if not generated_query:
            return {"score": 0, "comment": "No query generated"}

        # Define the prompt for the judge
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
        
        Evaluation Rules:
        1. **Ignore SELECT**: Do NOT evaluate the SELECT clause (variable names, order, or projection).
        2. **Semantic Equivalence**: strictness is relaxed. If the generated query implements the same filtering logic and graph patterns to retrieve the intended entities, it is CORRECT.
        3. **Ignore Benign Differences**:
           - Limit differences (e.g., LIMIT 50 vs 100) -> IGNORE (Correct)
           - Variable naming differences -> IGNORE (Correct)
           - Ordering of triple patterns (if logic is same) -> IGNORE (Correct)
           - Extra optional metadata retrieval -> IGNORE (Correct)
           - Extra triplets present in the query -> IGNORE (Correct)
           - Differences in how the date are filtered (consider only the year) -> IGNORE (Correct)
        4. **Focus on Results**: If the query would likely return the correct core entities (Subject/Object) as the reference, mark it as CORRECT.

        Output a JSON object with:
        - "is_correct": boolean (true if correct, false otherwise)
        - "reasoning": A brief explanation of the errors in the generated query. Be very concise, use bullet point to list the errors.
        
        Only output the JSON.
        """
        
        try:
            response = await llm.ainvoke(prompt)
            content = response.content
            
            # extract json
            clean_content = content.strip()
            if clean_content.startswith("```json"):
                clean_content = clean_content[7:]
            if clean_content.endswith("```"):
                clean_content = clean_content[:-3]
            
            data = json.loads(clean_content.strip())
            is_correct = data.get("is_correct", False)
            return {"score": 1 if is_correct else 0, "comment": "".join(data.get("reasoning", ""))}
        except Exception as e:
            print(f"LLM Evaluator Error: {e}")
            return {"score": 0, "comment": f"Evaluation failed: {e}"}

    # RUN EVALUATION
    run_expt = True
    if run_expt:
        evaluation = await client.aevaluate(
            target_doremus_assistant,
            data=dataset_name,
            evaluators=[accuracy, llm_score, llm_is_correct],
            # Name of the experiment
            experiment_prefix=EXPERIMENT_PREFIX, 
            max_concurrency=1
        )

if __name__ == "__main__":
    asyncio.run(main())

