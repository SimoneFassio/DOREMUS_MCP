import subprocess
import json
import sys
import argparse

def run_gemini_query(prompt: str, log_file: str):
    """
    Executes a query using the gemini CLI in streaming mode and logs the interaction.
    """
    command = [
        "gemini",
        "-p",
        prompt,
        "--output-format",
        "stream-json",  # Use streaming output
        "--yolo",
        "--model",
        "gemini-2.5-flash"
    ]
    
    final_text_response = ""
    process = None
    
    try:
        print("Executing Gemini CLI command in streaming mode (press Ctrl+C to stop)...")
        
        # Use Popen to stream output in real-time
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)

        # Open log file to append in real-time
        with open(log_file, "a") as f:
            f.write("--- New Streaming Query ---\n")
            f.write(f"Command: {' '.join(command)}\n")
            f.write("--- Gemini Event Stream ---\n")

            # Read and process output line by line
            for line in iter(process.stdout.readline, ''):
                if not line.strip():
                    continue
                
                print(line.strip()) # Print the raw JSON event
                f.write(line) # Log the raw JSON event
                
                try:
                    event = json.loads(line)
                    # The final text response is usually in a 'text' field.
                    # This will be overwritten by subsequent events until the final one.
                    if 'text' in event:
                        final_text_response = event['text']
                except json.JSONDecodeError:
                    # This might happen if a line is not a complete JSON object
                    pass

            f.write("--- End of Stream ---\n\n")

        # Check for errors after the stream is done
        process.wait()
        if process.returncode != 0:
            stderr_output = process.stderr.read()
            print(f"Error executing Gemini CLI:\n{stderr_output}", file=sys.stderr)
            with open(log_file, "a") as f:
                f.write(f"--- ERROR ---\nCommand failed with exit code {process.returncode}\nStderr:\n{stderr_output}\n--- End Error ---\n\n")
            return final_text_response # Return whatever we got before the error

        return final_text_response

    except FileNotFoundError:
        print("Error: 'gemini' command not found. Make sure it is installed and in your PATH.", file=sys.stderr)
        return None
    except KeyboardInterrupt:
        print("\n\n--- Execution Interrupted by User ---")
        print("Partial output has been logged.")
        if process:
            process.terminate() # Ensure the child process is killed
        return final_text_response # Return the last complete text response we received
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        return None

def main():
    """
    Main function to run the client.
    """
    parser = argparse.ArgumentParser(description="Query the DOREMUS MCP server via Gemini CLI.")
    parser.add_argument("query", type=str, help="The musical knowledge query to ask the chatbot.")
    args = parser.parse_args()

    log_file = "gemini_cli_log.txt"
    
    user_input = args.query

    # Construct a detailed prompt for the Gemini agent
    prompt = f"""
You are an expert assistant interacting with a Musical Concert Program (MCP) server.
Your task is to answer the user's question by using the DOREMUS MCP server's capabilities.

User's question: "{user_input}"

Based on this question, formulate a precise query for the MCP server, execute it, and provide the final answer based on the server's response. Show your reasoning.
"""

    result = run_gemini_query(prompt, log_file)

if __name__ == "__main__":
    main()
