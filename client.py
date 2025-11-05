from mcp_use import MCPClient, MCPAgent
from langchain.chat_models import init_chat_model
import asyncio
from dotenv import load_dotenv

async def main():
    config = {
        "DOREMUS_MCP": {
          "type": "http",
          "url": "http://localhost:8000/sse"
        }
    }
    load_dotenv() 
    # Create MCPClient from configuration dictionary
    client = MCPClient.from_dict(config)

    # Create LLM
    llm = init_chat_model(
        "llama-3.1-8b-instant",
        model_provider="groq",
        temperature=0
    )

    # Create agent with the client
    agent = MCPAgent(llm=llm, client=client, max_steps=30)

    # Run the query
    user_input = input("Ask the chatbot about musical knowledge: ")
    result = await agent.run(
       user_input,
    )
    print(f"\nResult: {result}")

if __name__ == "__main__":
    asyncio.run(main())
  