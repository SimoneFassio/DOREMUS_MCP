import mcp.types as types
from fastmcp import Context
from src.server.utils import (
    convert_to_variable_name,
    extract_label
)

# Helper to format paths for the LLM
def format_paths_for_llm(paths):
    options = []
    for i, p in enumerate(paths):
        # FAILSAFE: debug
        if not p:
            raise ValueError(f"No elements found in path {p}")
        readable_chain = " -> ".join([elem[0] for elem in p]) + "\n"
        options.append(f"Option {i}: {readable_chain}")
    return "\n".join(options)

async def tool_sampling_request(system_prompt: str, pattern_intent: str, ctx: Context):
    user_message = f"""
        The user is asking about: {pattern_intent}.
        Based on the user's intent, select the most appropriate option by its index number.
        
        Reply ONLY with the integer index of the best option (e.g., '0' or '1').
        """
    #TRIGGER THE SAMPLING REQUEST
    # This calls back to the LLM to get a "thought"
    result = await ctx.sample(
        messages=[
            types.SamplingMessage(role="user", content=types.TextContent(type="text", text=user_message))
        ],
        max_tokens=10,
        system_prompt=system_prompt
    )
    
    return result.text