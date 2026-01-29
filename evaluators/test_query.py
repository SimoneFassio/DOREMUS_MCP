import warnings
import logging

# Filter warnings immediately and aggressively
warnings.simplefilter("ignore")
warnings.filterwarnings("ignore")

# Also capture warnings to logging and suppress them
logging.captureWarnings(True)
logging.getLogger("py.warnings").setLevel(logging.ERROR)

import asyncio
import json
import os
import sys
import logging
import httpx
from dotenv import load_dotenv
from langgraph.errors import GraphRecursionError
from pydantic import ValidationError

# Add src to path for local development
sys.path.insert(0, 'src')

from rdf_assistant.doremus_assistant import client, create_model, initialize_agent
from server.utils import execute_sparql_query

# Suppress httpx INFO logs
logging.getLogger("httpx").setLevel(logging.ERROR)

load_dotenv(".env")

EXPERIMENT_PREFIX = os.getenv("EXPERIMENT_PREFIX", "")
LLM_EVAL_MODEL = os.getenv("LLM_EVAL_MODEL", "")
DOREMUS_MCP_URL = os.getenv("DOREMUS_MCP_URL", "http://localhost:8000/mcp")
API_KEYS_LIST = os.getenv("API_KEYS_LIST", "").split(",")
API_KEYS_LIST = [k.strip() for k in API_KEYS_LIST if k.strip()]
EVALUATION_NUM_REPETITIONS = os.getenv("EVALUATION_NUM_REPETITIONS", 1)

DATASET_NAME = os.getenv( "EVALUATION_DATASET_NAME","Doremus Dataset")
DATASET_SPLITS = [s.strip() for s in os.getenv("EVALUATION_DATASET_SPLITS", "").split(",") if s.strip()]
DATASET_ORIGIN = os.getenv("EVALUATION_DATASET_ORIGIN", "")
if DATASET_ORIGIN not in ["competency_question", "user_question"]:
    DATASET_ORIGIN = ""
print(f"Using dataset: {DATASET_NAME}")
print(f"Using splits: {DATASET_SPLITS}")
if DATASET_ORIGIN:
    print(f"Using origin: {DATASET_ORIGIN}")

# LLM evaluator setup
provider = "ollama"
model_name = "gpt-oss:120b"

class KeyManager:
    def __init__(self, keys):
        self.keys = keys
        self.current_index = 0
    
    def get_next_key(self):
        if not self.keys:
            return None
        self.current_index = (self.current_index + 1) % len(self.keys)
        return self.keys[self.current_index]
    
    def get_current_key(self):
        if not self.keys:
            return None
        return self.keys[self.current_index]

key_manager = KeyManager(API_KEYS_LIST)

EVALUATION_TIMEOUT_SECONDS = int(os.getenv("EVALUATION_TIMEOUT_SECONDS", 2000))

async def main():
    async def target_doremus_assistant(inputs: dict) -> dict:
        """Process a user input through the doremus assistant and capture the generated query."""
        messages = []
        # Retry loop for API errors
        max_retries = len(key_manager.keys) if key_manager.keys else 1
        # If no keys list, we just run once with default environment key (so essentially 1 try)
        if not key_manager.keys:
             max_retries = 1
        else:
             max_retries = len(key_manager.keys) + 1 # Allow initial run + retries for all keys

        current_agent = await initialize_agent(key_manager.get_current_key())
        
        # Helper to run the stream with timeout
        async def process_stream():
            nonlocal messages
            # Use astream with stream_mode="values" to capture state updates
            async for chunk in current_agent.astream(
                {"messages": [{"role": "user", "content": inputs["query_input"]}]},
                stream_mode="values"
            ):
                if "messages" in chunk:
                    messages = chunk["messages"]

        for attempt in range(max(max_retries, 5)):
            try:
                # Enforce time limit on the generation process
                await asyncio.wait_for(process_stream(), timeout=EVALUATION_TIMEOUT_SECONDS)
                
                # If we finish the stream successfully, break the retry loop
                break

            except asyncio.TimeoutError:
                print(f"⚠️ Timeout Limit ({EVALUATION_TIMEOUT_SECONDS}s) Hit for: {inputs.get('query_input', '')[:30]}... processing partial messages.")
                break # Don't retry on timeout, just process partial results
            except GraphRecursionError:
                print(f"⚠️ Recursion Limit Hit for: {inputs.get('query_input', '')[:30]}... processing partial messages.")
                break # Don't retry on recursion error, just process partial
            except ValidationError as e:
                 print(f"⚠️ Pydantic Validation Error for: {inputs.get('query_input', '')[:30]}... Error: {e}")
                 break
            except Exception as e:
                error_str = str(e).lower()
                if "429" in error_str or "limit" in error_str or "quota" in error_str:
                    if os.getenv("API_KEYS_LIST"):
                        print(f"⚠️ API Limit/Error encountered: {e}")
                        new_key = key_manager.get_next_key()
                        if new_key:
                            print(f"🔄 Switching to next API Key (Index {key_manager.current_index})...")
                            current_agent = await initialize_agent(api_key=new_key)
                            continue
                        else:
                            print("❌ No more API keys to try.")
                            break
                    else:
                        print(f"⚠️ API Limit/Error encountered: {e}, retrying in 10 seconds...")
                        await asyncio.sleep(10)
                        current_agent = await initialize_agent()
                        continue
                else:
                    print(f"Error during ainvoke: {e}")
                    break

        # State tracking
        query_map = {} # query_id -> generated_query content
        last_generated_query = None
        last_query_id = None
        executed_query_id = None
        final_answer = ""
        sampling_requests = []
        tool_calls_responses = []

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

            # 4 Capture Toolmessages Responses
            if message.type == "tool" and message.content:
                tool_calls_responses.append({
                    "tool_name": message.name,
                    "status": message.status,
                    "content": message.content
                })
            
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
            "sampling_requests": sampling_requests,
            "tool_calls_responses": tool_calls_responses,
            "question": inputs["query_input"]
        }

    async def accuracy(outputs: dict, reference_outputs: dict) -> float:
        """Check the percentage of correct values returned by the query."""
        ref_uris = []
        output_uris = []
        query_output = []
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

        if outputs.get("generated_query"):
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
            # Try LLM Judge Fallback
            print("⚠️ No URIs or Values found for comparison. Using LLM Judge fallback...")
            
            # Instantiate judge
            llm = create_model(provider, model_name, key_manager.get_current_key())
            
            question = outputs.get("question", "")
            final_answer = outputs.get("final_answer", "")
            # We use the reference query results as truth, but also the query itself might be useful context,
            # but here we rely on the reference answer data. 
            # Converting reference output to string representation
            ref_ans_str = json.dumps(reference_output)
            ref_ans_str = ref_ans_str.replace("\n", "")
            ref_ans_str = ref_ans_str[:1000] + "..." if len(ref_ans_str) > 1000 else ref_ans_str
            
            prompt = f"""
You are an expert evaluator. Determine if the candidate answer is correct based on the reference answer to the question.

Question: {question}

Reference Answer (Ground Truth Data):
{ref_ans_str}

Candidate Final Answer (from Assistant):
{final_answer}

Task:
- If the Candidate Answer matches the data/facts in the Reference Answer, accuracy is 1.0.
- If it is partially correct (e.g. missing some items but got others right), accuracy is 0.5.
- If it is incorrect or says "I don't know" when there is an answer, accuracy is 0.0.

Output ONLY a single number: 1.0, 0.5, or 0.0.
            """
            
            try:
                response = await llm.ainvoke(prompt)
                score_text = response.content.strip()
                # extract number
                import re
                match = re.search(r"0\.|1\.0|0|1", score_text)
                if match:
                        score = float(match.group())
                        print(f" LLM Judge Score (Accuracy): {score}")
                        return score
                else:
                    print(f" Could not parse LLM score: {score_text}")
                    return 0.0
            except Exception as e:
                print(f" LLM Judge Error: {e}")
                return f" LLM Judge Error: {e}"
        
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
        llm = create_model(provider, model_name, key_manager.get_current_key())
        
        generated_query = outputs.get("generated_query", "")
        reference_query = reference_outputs["rdf_query"] # ground truth query
        
        if not generated_query:
            return {"score": 0, "comment": "No query generated"}

        # Define the prompt for the judge
        question = outputs.get("question", "")
        prompt = f"""
        You are an expert SPARQL query evaluator. Compare the GENERATED SPARQL query with the REFERENCE SPARQL query.
        
        Question: {question}

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
        4. VERIFY if the generated query correctly answers the specific Question provided.
        
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
            return {"score": "LLM Error", "comment": f"Evaluation failed: {e}"}
    
    async def llm_is_correct(outputs: dict, reference_outputs: dict) -> dict:
        """Use an LLM to evaluate if the query is practically correct ignoring minor details."""
        
        # Instantiate the judge model (using same config as agent)
        llm = create_model(provider, model_name, key_manager.get_current_key())
        
        generated_query = outputs.get("generated_query", "")
        reference_query = reference_outputs["rdf_query"] # ground truth query
        
        if not generated_query:
            return {"score": 0, "comment": "No query generated"}

        # Define the prompt for the judge
        question = outputs.get("question", "")
        prompt = f"""
        You are an expert SPARQL query evaluator. Determine if the GENERATED SPARQL query is effectively CORRECT compared to the REFERENCE SPARQL query.

        Question: {question}

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
        5. **Verify Question Relevance**: Ensure the query answers the specific Question.

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
            return {"score": "LLM Error", "comment": f"Evaluation failed: {e}"}
        
    def type_I_error(outputs: dict) -> int:
        """
        Check for each tool call if there was a wrong use of the schema.
        If the arguments passed to the tool are not compatible or are not of the
        requested type, count as a type I error.

        Returns:
            int: the number of type I errors detected.
        """
        type_I_errors = 0
        tool_calls = outputs.get("tool_calls_responses", [])
        
        for tool_call in tool_calls:
            tool_name = tool_call.get("tool_name", "")
            status = tool_call.get("status", "")
            if status == "error" and "Dry Run" not in tool_call.get("content", ""):
                type_I_errors += 1
        
        return type_I_errors
    
    def type_II_error(outputs: dict, reference_metadata: dict) -> int:
        """
        Check the list of tool calls and compares it to the reference 
        workflow to identify wrong or missing tool calls.

        Are considered type II errors:
        - Missing tool calls that are in the reference workflow.
        - Extra tool calls that are not in the reference workflow.
        - Tool calls in the wrong order that lead to errors.

        Exploratory tools are ignored in the comparison.

        Returns:
            int: the number of type II errors detected.
        """
        type_II_errors = 0

        exploratory_tools = ["find_candidate_entities", "get_entity_properties", "get_usage_guide"]
        workflow = reference_metadata.get("workflow", [])
        if not workflow:
            # There is no reference workflow to compare against -> anything is acceptable
            return type_II_errors
        
        tool_calls = outputs.get("tool_calls_responses", [])
        reference_tool_calls = [{"name":line.split("(")[0], "used": False} for line in workflow]
        start = False
        for tool_call in tool_calls:
            tool_name = tool_call.get("tool_name", "")
            status = tool_call.get("status", "")
            if tool_name in exploratory_tools:
                continue
            if tool_name == reference_tool_calls[0]["name"] and status == "success":
                # Every time we see the first tool call successfully executed, we restart the matching
                # print(" Starting reference workflow matching...")
                start = True
                reference_tool_calls[0]["used"] = True
                # RESET all others to unused
                for rtc in reference_tool_calls[1:]:
                    rtc["used"] = False
                continue
            if start:
                remaining_list = [rtc for rtc in reference_tool_calls if not rtc["used"]]
                # print(f"Current tool call should be {remaining_list[0]['name'] if remaining_list else 'N/A'}, got {tool_name} (status: {status})")
                if tool_name not in [rtc["name"] for rtc in remaining_list]:
                    # Extra tool call not in reference
                    type_II_errors += 1
                    continue
                elif tool_name != remaining_list[0]["name"]:
                    # Tool call out of order -> either missing or wrong order
                    if status == "success":
                        # The wrong order did not create an error -> flexibility is possible
                        # find the first occurrence and mark as used
                        for rtc in reference_tool_calls:
                            if rtc["name"] == tool_name and not rtc["used"]:
                                rtc["used"] = True
                                break
                    else:
                        # Error occurred -> order matters
                        type_II_errors += 1
                else:
                    # Correct tool call in order
                    if status == "success":
                        for rtc in reference_tool_calls:
                            if rtc["name"] == tool_name and not rtc["used"]:
                                rtc["used"] = True
                                break
                    else:
                        # This will likely be a type I error, not type II
                        pass
            else:
                # Haven't started matching yet, wrong initial tool call
                type_II_errors += 1
        # Remainings not used are missing tool calls
        for rtc in reference_tool_calls:
            if not rtc["used"]:
                type_II_errors += 1
        if not start:
            # The agent never actually started the workflow correctly -> only this time cap the errors to the full length of the reference
            type_II_errors = len(reference_tool_calls)

        return type_II_errors
    
    def type_III_error(outputs: dict) -> dict:
        """
        Checks the final answer of the LLM to see if the LLM has actually
        answered the right question.
        The goal is to evaluate wether the LLM has understood the question
        and the user intent correctly.
        """

        # Instantiate the judge model (using same config as agent)
        llm = create_model(provider, model_name, key_manager.get_current_key())

        final_answer = outputs.get("final_answer", "")
        question = outputs.get("question", "")

        prompt = f"""
You are grading ONLY "intent understanding" (Type III). Not accuracy.

Given a Question and an Answer, decide whether the Answer shows the assistant understood the user's intent.
Return is_outOfTopic=true ONLY if the assistant answered a DIFFERENT question/intent than asked.

Important:
- DO NOT penalize missing results, inability, refusals, or "I couldn't find it". If the response is about the same intent but lacks data, is_outOfTopic=false.
- DO NOT penalize vague/partial but on-topic answers. If it’s clearly trying to answer the right question, is_outOfTopic=false.
- Penalize intent mismatch: wrong task type (count vs list, yes/no vs explanation), wrong entity, wrong relation, wrong target.

Steps:
1) Infer the expected answer type from the Question (e.g., COUNT/NUMBER, LIST, BOOLEAN, DESCRIPTION).
2) Check whether the Answer matches that intent and target.

Examples:
- Q: "How many operas by Schubert?"  A: "Here are some Schubert operas: ..." -> is_outOfTopic=true (list instead of count)
- Q: "List operas by Schubert"       A: "Schubert wrote 3 operas"          -> is_outOfTopic=true (count instead of list)
- Q: "How many operas by Schubert?"  A: "I couldn't find the number."      -> is_outOfTopic=false (intent understood, no result)
- Q: "How many operas by Schubert?"  A: "Schubert composed operas."        -> is_outOfTopic=false (vague but aligned)

Question: {question}
Answer: {final_answer}

Output ONLY valid JSON:
{{
  "is_outOfTopic": boolean,
  "reasoning": string
}}
Reasoning must be very concise bullet points (max 3 bullets).
        """
        try:
            response = llm.invoke(prompt)
            content = response.content

            # extract json
            clean_content = content.strip()
            if clean_content.startswith("```json"):
                clean_content = clean_content[7:]
            if clean_content.endswith("```"):
                clean_content = clean_content[:-3]
            data = json.loads(clean_content.strip())
            is_outOfTopic = data.get("is_outOfTopic", False)
            return {"score": 1 if is_outOfTopic else 0, "comment": "".join(data.get("reasoning", ""))}
        except Exception as e:
            print(f" LLM Judge Error: {e}")
            return {"score": "LLM Error", "comment": f"Evaluation failed: {e}"}


    async def combined_evaluator(run, example):
        """
        Combined evaluator that runs all three evaluation logics in a single trace.
        This reduces LangSmith traces from 3 to 1 per example.
        Disables LangChain tracing for internal LLM calls to save on span limits.
        """
        # Disable LangChain tracing for evaluator LLM calls to save on span limits
        prev_tracing = os.environ.get("LANGCHAIN_TRACING_V2")
        os.environ["LANGCHAIN_TRACING_V2"] = "false"
        
        try:
            # 1. Logic from accuracy()
            acc_score = await accuracy(run.outputs, example.outputs)
            
            # 2. Logic from llm_score()
            score_data = await llm_score(run.outputs, example.outputs)
            
            # 3. Logic from llm_is_correct()
            # is_correct_data = await llm_is_correct(run.outputs, example.outputs)

            # 4. Logic from type_I_error()
            type_I_errors = type_I_error(run.outputs)

            # 5. Logic from type_II_error()
            type_II_errors = type_II_error(run.outputs, example.metadata)

            # 6. Logic from type_III_error()
            type_III_data = type_III_error(run.outputs)

            return {
                "results": [
                    {"key": "accuracy", "score": acc_score},
                    {"key": "llm score", "score": score_data["score"], "comment": score_data.get("comment", "")},
                    {"key": "type 1 errors", "score": type_I_errors},
                    {"key": "type 2 errors", "score": type_II_errors},
                    {"key": "type 3 errors", "score": type_III_data["score"], "comment": type_III_data.get("comment", "")},
                    # {"key": "llm_is_correct", "score": is_correct_data["score"], "comment": is_correct_data.get("comment", "")}
                ]
            }
        finally:
            # Restore previous tracing state
            if prev_tracing is not None:
                os.environ["LANGCHAIN_TRACING_V2"] = prev_tracing
            else:
                os.environ.pop("LANGCHAIN_TRACING_V2", None)

    # RUN EVALUATION
    if DATASET_ORIGIN and DATASET_SPLITS:
        dataset = client.list_examples(
            dataset_name=DATASET_NAME,
            metadata={"origin": DATASET_ORIGIN, "split": DATASET_SPLITS}
            )
    elif DATASET_ORIGIN:
        dataset = client.list_examples(
            dataset_name=DATASET_NAME,
            metadata={"origin": DATASET_ORIGIN}
            )
    elif DATASET_SPLITS:
        dataset = client.list_examples(
            dataset_name=DATASET_NAME,
            metadata={"split": DATASET_SPLITS}
            )
    else:
        dataset = client.list_examples( dataset_name=DATASET_NAME )
    
    # Convert to list to support slicing/resumption
    dataset = list(dataset)
    total_examples = len(dataset)
    
    EVALUATION_START_OFFSET = int(os.getenv("EVALUATION_START_OFFSET", 0))
    if EVALUATION_START_OFFSET > 0:
        if EVALUATION_START_OFFSET >= total_examples:
            print(f"⚠️ Start offset {EVALUATION_START_OFFSET} is >= total examples {total_examples}. Nothing to run.")
            return
        
        print(f"⏩ Resuming evaluation: Skipping first {EVALUATION_START_OFFSET} examples. Running {total_examples - EVALUATION_START_OFFSET} remaining examples.")
        dataset = dataset[EVALUATION_START_OFFSET:]
    else:
        print(f"Running evaluation on all {total_examples} examples.")

    experiment_args = {
        "experiment_prefix": EXPERIMENT_PREFIX + "-" + LLM_EVAL_MODEL,
        "max_concurrency": 1,
        "num_repetitions": int(EVALUATION_NUM_REPETITIONS)
    }
    
    existing_experiment = os.getenv("EVALUATION_EXISTING_EXPERIMENT")
    if existing_experiment:
        print(f"🔗 Appending results to existing experiment: {existing_experiment}")
        experiment_args["experiment"] = existing_experiment
        # When appending, we don't need prefix
        del experiment_args["experiment_prefix"]

    evaluation = await client.aevaluate(
        target_doremus_assistant,
        data=dataset,
        evaluators=[combined_evaluator],
        **experiment_args
    )

if __name__ == "__main__":
    asyncio.run(main())
