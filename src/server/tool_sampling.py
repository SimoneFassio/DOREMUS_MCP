import os
import re
import logging
from dotenv import load_dotenv
import mcp.types as types
from fastmcp.server.dependencies import get_context
from openai import OpenAI
from groq import Groq
from ollama import Client

logger = logging.getLogger("doremus-mcp")

# Sampling-specific provider/model selection
sampling_models = {
    "openai": "gpt-4o",
    "groq": "llama-3.3-70b-versatile",
    "ollama": "gpt-oss:120b"
}
load_dotenv()

sampling_provider = os.getenv("LLM_SAMPLING_PROVIDER", os.getenv("LLM_EVAL_PROVIDER", "ollama")).lower()
if sampling_provider not in sampling_models:
    logger.warning(f"Unknown LLM_SAMPLING_PROVIDER '{sampling_provider}', defaulting to 'ollama'.")
    sampling_provider = "ollama"

sampling_model = os.getenv("LLM_SAMPLING_MODEL", os.getenv("LLM_EVAL_MODEL", "gpt-oss:120b"))

# Fallback client for sampling if needed (matches sampling provider)
if sampling_provider == "openai":
    fallback_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
elif sampling_provider == "groq":
    fallback_client = Groq(api_key=os.getenv("GROQ_API_KEY"))
else:
    fallback_client = Client(
        host=os.getenv("OLLAMA_API_URL"),
        headers= {"Authorization": f"Basic {os.getenv('OLLAMA_API_KEY')}"},
        )

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

async def tool_sampling_request(system_prompt: str, pattern_intent: str) -> str:
    """
    Sends a sampling request to the client (LLM) to resolve ambiguity.
    This function will handle fallback to server-side LLM if client sampling fails.
    1. Try client sampling
    2. If fails, use server-side LLM (OpenAI, Groq, or Ollama based on env vars)
    3. Return the selected option index as string
    """

    try:
        ctx = get_context()
    except Exception as e:
        logger.error(f"Failed to get MCP context for tool sampling: {e}")
        return "0"  # Fallback to first option

    user_message = f"""
The user is asking about {pattern_intent}
Based on the user's intent, select the most appropriate option by its index number.
**Reply ONLY with the integer index of the best option (e.g., '0' or '1')**.
You MUST reply with exactly one token: the integer index only — nothing else, no punctuation, no commentary.
        """
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
        try:
            # 3. Manual Fallback 
            if sampling_provider == "openai" or sampling_provider == "groq":
                response = fallback_client.chat.completions.create(
                    model=sampling_model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_message}
                    ],
                    max_tokens=20,
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
            logger.info(f"Fallback LLM response: {llm_response}")  # Log fallback response
            match = re.search(r'\d+', llm_response)
            if match:
                return match.group()
            else:
                logger.warning(f"Fallback LLM response did not contain a valid index. Response: {llm_response}")
                return f"0 (Fallback due to invalid response: {llm_response})"
            
        except Exception as openai_error:
            logger.error(f"Critical: Both Client and Fallback sampling failed. {openai_error}")
            return "0" # Ultimate failsafe: default to first option