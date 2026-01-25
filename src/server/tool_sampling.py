import os
import re
import logging
from dotenv import load_dotenv
import mcp.types as types
from fastmcp.server.dependencies import get_context
from openai import OpenAI
from groq import Groq
from ollama import Client
from zai import ZaiClient
from typing import Callable, Optional, Dict, Any
import time

logger = logging.getLogger("doremus-mcp")

# Sampling-specific provider/model selection
sampling_models = {
    "openai": "gpt-5.2",
    "groq": "llama-3.3-70b-versatile",
    "cerebras": "llama-3.3-70b",
    "ollama": "gpt-oss:120b",
    "zai": "glm-4.7-flash",
    "custom": "gpt-5.2",
    "nvidia": "gpt-oss:120b"
}
load_dotenv()

sampling_provider = os.getenv("LLM_SAMPLING_PROVIDER", os.getenv("LLM_EVAL_PROVIDER", "ollama")).lower()
if sampling_provider not in sampling_models:
    logger.warning(f"Unknown LLM_SAMPLING_PROVIDER '{sampling_provider}', defaulting to 'ollama'.")
    sampling_provider = "ollama"

sampling_model = os.getenv("LLM_SAMPLING_MODEL", os.getenv("LLM_EVAL_MODEL", "gpt-oss:120b"))


API_KEYS_LIST = os.getenv("API_KEYS_LIST", "").split(",")
API_KEYS_LIST = [k.strip() for k in API_KEYS_LIST if k.strip()]
current_key_index = 0

def create_fallback_client(api_key=None):
    if sampling_provider == "openai":
        return OpenAI(api_key=api_key or os.getenv("OPENAI_API_KEY"))
    elif sampling_provider == "groq":
        return Groq(api_key=api_key or os.getenv("GROQ_API_KEY"))
    elif sampling_provider == "cerebras":
        return OpenAI(
            api_key=api_key or os.getenv("CEREBRAS_API_KEY"),
            base_url="https://api.cerebras.ai/v1"
        )
    elif sampling_provider == "custom":
        return OpenAI(
            base_url="http://localhost:8964/v1/",
            api_key="1234"
        )
    elif sampling_provider == "nvidia":
        return OpenAI(
            base_url="https://integrate.api.nvidia.com/v1",
            api_key=api_key or os.getenv("NVIDIA_API_KEY"),
        )

    elif sampling_provider == "zai":
        return ZaiClient(api_key=api_key or os.getenv("ZAI_API_KEY"))
    elif api_key:
        return Client(
            host=os.getenv("OLLAMA_API_URL"),
            headers= {"Authorization": f"Bearer {api_key}"},
        )
    else:
        return Client(
            host=os.getenv("OLLAMA_API_URL"),
            headers= {"Authorization": f"Basic {os.getenv('OLLAMA_API_KEY')}"},
        )

fallback_client = create_fallback_client(os.getenv("API_KEYS_LIST", "").split(",")[0])

def rotate_fallback_client():
    global fallback_client, current_key_index
    if not API_KEYS_LIST:
        return False
    current_key_index = (current_key_index + 1) % len(API_KEYS_LIST)
    new_key = API_KEYS_LIST[current_key_index]
    logger.info(f"🔄 Rotating Sampling API Key to index {current_key_index}")
    fallback_client = create_fallback_client(new_key)
    return True

# Helper to format paths for the LLM
def format_paths_for_llm(paths):
    options = []
    for i, p in enumerate(paths):
        # FAILSAFE: debug
        if not p:
            raise ValueError(f"No elements found in path {p}")
        readable_chain = " -> ".join([elem[0] for elem in p]) + "\n"
        options.append(f"- Option {i}: {readable_chain}")
    return "\n".join(options)


async def tool_sampling_request(system_prompt: str, pattern_intent: str, log_callback: Optional[Callable[[Dict[str, Any]], None]] = None, caller_tool_name: str = "unknown") -> str:
    """
    Sends a sampling request to the client (LLM) to resolve ambiguity.
    This function will handle fallback to server-side LLM if client sampling fails.
    1. Try client sampling
    2. If fails, use server-side LLM (OpenAI, Groq, or Ollama based on env vars)
    3. Return the selected option index as string
    """
    start_time = time.time()
    used_model = "unknown"
    llm_response = ""
    error_msg = None
    
    try:
        try:
            ctx = get_context()
        except Exception as e:
            logger.error(f"Failed to get MCP context for tool sampling: {e}")
            raise e # Reraise to trigger fallback

        user_message = f"""
The user is asking about {pattern_intent}
Based on the user's intent, select the most appropriate option by its index number.
**Reply ONLY with the integer index of the best option (e.g., '0' or '1')**.
You MUST reply with exactly one token: the integer index only — nothing else, no punctuation, no commentary.
        """
            
        logger.info(f"LLM sampling message: {user_message}")
        # MODEL PREFERENCES
        preferences = types.ModelPreferences(
            hints=[
                types.ModelHint(name=sampling_model),
                # Standard VS Code / General High Performance
                types.ModelHint(name="gpt-4o"),
                types.ModelHint(name="claude-3-5-sonnet"),
                
                # Your Evaluation Models (from your python dict)
                types.ModelHint(name="gpt-4.1"), 
                types.ModelHint(name="llama-3.1-8b-instant"), 
                types.ModelHint(name="claude-sonnet-4-5-20250929"),
                types.ModelHint(name="mistral-7b-instant"),
                types.ModelHint(name="gpt-oss:120b")
            ],
            costPriority=0.3, # Prefer intelligence over cost
            speedPriority=0.5
        )

        # TRIGGER THE SAMPLING REQUEST: calls back to the LLM to get a "thought"
        try: 
            # We assume the first hint is the one used if client respects it, 
            # but we can't know for sure which model the client picked.
            used_model = sampling_model 
            
            result = await ctx.sample(
                messages=[
                    types.SamplingMessage(role="user", content=types.TextContent(type="text", text=user_message))
                ],
                max_tokens=10,
                system_prompt=system_prompt,
                model_preferences=preferences
            )
            if hasattr(result, "content") and hasattr(result.content, "text"):
                llm_response = result.content.text
            else:
                llm_response = result.text

            logger.info(f"LLM response: {llm_response}")

            # Extract the index from the response
            match = re.search(r'\d+', llm_response)
            if match:
                return match.group()  # Return the valid index
            else:
                logger.warning(f"LLM response did not contain a valid index. Response: {llm_response}")
                return f"0 (Fallback due to invalid response: {llm_response})"  # Fallback to first option with context
        except Exception as e:
            # CATCH THE "AUTO" ERROR (or any other client failure)
            logger.warning(f"Client sampling failed ({str(e)}). Switching to Server-Side Fallback.")
            raise e # Trigger outer catch for fallback

    except Exception as e:
        logger.info("Switching to Server-Side Fallback.")
        try:
            # 3. Manual Fallback 
            used_model = sampling_model # Fallback model
            user_message = f"""
The user is asking about {pattern_intent}
Based on the user's intent, select the most appropriate option by its index number.
**Reply ONLY with the integer index of the best option (e.g., '0' or '1')**.
You MUST reply with exactly one token: the integer index only — nothing else, no punctuation, no commentary.
            """
            
            # Retry loop for API errors
            max_retries = len(API_KEYS_LIST) + 1 if API_KEYS_LIST else 1
            for attempt in range(max_retries):
                try:
                    if sampling_provider in ["openai", "groq", "cerebras", "zai", "nvidia", "custom"]:
                        response = fallback_client.chat.completions.create(
                            model=sampling_model,
                            messages=[
                                {"role": "system", "content": system_prompt},
                                {"role": "user", "content": user_message}
                            ],
                            max_completion_tokens=20,
                            temperature=0
                        )
                        llm_response = response.choices[0].message.content
                    else:
                        response = fallback_client.chat(
                            model=sampling_model,
                            messages=[
                                {"role": "system", "content": system_prompt},
                                {"role": "user", "content": user_message}
                            ],
                            options={"temperature":0}
                        )
                        llm_response = response.message.content
                    # Success
                    break
                except Exception as call_error:
                    error_str = str(call_error).lower()
                    if ("429" in error_str or "limit" in error_str or "quota" in error_str) and API_KEYS_LIST:
                         if rotate_fallback_client():
                             continue
                    raise call_error
            logger.info(f"Fallback LLM response: {llm_response}")
            # Consider last number found, if the LLM is reasoning before it is ignored
            numbers = re.findall(r'\d+', llm_response)
            if numbers:
                return numbers[-1]
            else:
                logger.warning(f"Fallback LLM response did not contain a valid index. Response: {llm_response}")
                return f"0 (Fallback due to invalid response: {llm_response})"
            
        except Exception as openai_error:
            logger.error(f"Critical: Both Client and Fallback sampling failed. {openai_error}")
            error_msg = str(openai_error)
            return "0" # Ultimate failsafe: default to first option
            
    finally:
        latency = time.time() - start_time
        if log_callback:
            log_data = {
                "tool": caller_tool_name,
                "model": used_model,
                "inputs": {
                    "system_prompt": system_prompt,
                    "pattern_intent": pattern_intent
                },
                "output": llm_response,
                "latency": latency,
                "error": error_msg
            }
            log_callback(log_data)