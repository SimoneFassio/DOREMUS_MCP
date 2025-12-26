import os
import logging
from dotenv import load_dotenv

load_dotenv()
import mcp.types as types
from fastmcp.server.dependencies import get_context
from openai import OpenAI
from groq import Groq
from server.utils import (
    convert_to_variable_name,
    extract_label
)

OPENAI = True if os.getenv("LLM_EVAL_PROVIDER", "ollama").lower() == "openai" else False

logger = logging.getLogger("doremus-mcp")

# Fallback client for sampling if needed
if OPENAI:
    fallback_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
else:
    fallback_client = Groq(api_key=os.getenv("GROQ_API_KEY"))

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
    2. If fails, use server-side LLM (OpenAI or Groq)
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
- Reply ONLY with the integer index of the best option (e.g., '0' or '1').
        """
    # MODEL PREFERENCES
    preferences = types.ModelPreferences(
        hints=[
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
            return result.content.text
        return result.text
    except Exception as e:
        # CATCH THE "AUTO" ERROR (or any other client failure)
        logger.warning(f"Client sampling failed ({str(e)}). Switching to Server-Side Fallback.")
        try:
            # 3. Manual Fallback 
            if OPENAI:
                response = fallback_client.chat.completions.create(
                    model="gpt-4o",
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_message}
                    ],
                    max_tokens=20,
                    temperature=0
                )
            else:
                response = fallback_client.chat.completions.create(
                    model="llama-3.3-70b-versatile", #"llama-3.1-8b-instant", 
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_message}
                    ],
                    max_tokens=10,
                    temperature=0
                )
            return response.choices[0].message.content
            
        except Exception as openai_error:
            logger.error(f"Critical: Both Client and Fallback sampling failed. {openai_error}")
            return "0" # Ultimate failsafe: default to first option