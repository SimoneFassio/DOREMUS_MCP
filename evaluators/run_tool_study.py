
import os
import subprocess
import time
import sys
from typing import List, Dict, Union

COMMON_TOOLS = [
    "execute_query",
    "find_candidate_entities", 
    "get_entity_properties"
]

# Mapping codes to tool names
TOOL_MAP = {
    "BQ": "build_query",
    "AF": "apply_filter",
    "FBQ": "filter_by_quantity",
    "ACC": "add_component_constraint",
    "SAV": "select_aggregate_variable",
    "GH": "groupBy_having",
    "AT": "add_triplet"
}

# Define the configurations to test.
# Config 2: BQ + AF (Baseline)
# Config 3: BQ + AF + FBQ
# Config 4: BQ + AF + FBQ + ACC
# Config 5: BQ + AF + FBQ + ACC + SAV
# Config 6: BQ + AF + FBQ + ACC + SAV + GH

CONFIGURATIONS = [
    {
        "name": "Config_2_BQ_AF",
        "tools": COMMON_TOOLS + [TOOL_MAP["BQ"], TOOL_MAP["AF"]]
    },
    {
        "name": "Config_3_BQ_AF_FBQ",
        "tools": COMMON_TOOLS + [TOOL_MAP["BQ"], TOOL_MAP["AF"], TOOL_MAP["FBQ"]]
    },
    {
        "name": "Config_4_BQ_AF_FBQ_ACC",
        "tools": COMMON_TOOLS + [TOOL_MAP["BQ"], TOOL_MAP["AF"], TOOL_MAP["FBQ"], TOOL_MAP["ACC"]]
    },
    {
        "name": "Config_5_BQ_AF_FBQ_ACC_SAV",
        "tools": COMMON_TOOLS + [TOOL_MAP["BQ"], TOOL_MAP["AF"], TOOL_MAP["FBQ"], TOOL_MAP["ACC"], TOOL_MAP["SAV"]]
    },
    {
        "name": "Config_6_BQ_AF_FBQ_ACC_SAV_GH",
        "tools": COMMON_TOOLS + [TOOL_MAP["BQ"], TOOL_MAP["AF"], TOOL_MAP["FBQ"], TOOL_MAP["ACC"], TOOL_MAP["SAV"], TOOL_MAP["GH"]]
    },
    {
        "name": "FULL",
        "tools": COMMON_TOOLS + [TOOL_MAP["BQ"], TOOL_MAP["AF"], TOOL_MAP["FBQ"], TOOL_MAP["ACC"], TOOL_MAP["SAV"], TOOL_MAP["GH"], TOOL_MAP["AT"]]
    }
]

def restart_docker_and_run_eval(config_name: str, tools_setting: Union[str, List[str]]):
    print(f"\n{'='*60}")
    print(f"Starting Experiment Configuration: {config_name}")
    print(f"Tools Enabled: {tools_setting}")
    print(f"{'='*60}\n")

    # 1. Set Environment Variable
    env = os.environ.copy()
    
    if isinstance(tools_setting, list):
         env["MCP_ENABLED_TOOLS"] = ",".join(tools_setting)
    else:
         env["MCP_ENABLED_TOOLS"] = tools_setting

    # 2. Restart Docker Service
    print(f"[{config_name}] Restarting Doremus MCP Server...")
    try:
        # Using 'up -d --force-recreate' ensures the container is recreated with the new environment
        cmd = ["docker", "compose", "up", "-d", "--force-recreate", "doremus-mcp"]
        subprocess.run(cmd, env=env, check=True)
        
    except subprocess.CalledProcessError as e:
        print(f"Error restarting docker: {e}")
        return

    # 3. Wait for Server Readiness
    print(f"[{config_name}] Waiting 30s for server to be ready...")
    time.sleep(30)

    # 4. Run Evaluation Script
    print(f"[{config_name}] Running Evaluation...")
    
    # Set Experiment Name Prefix
    env["EXPERIMENT_PREFIX"] = f"{config_name}"
    
    try:
        cwd = os.getcwd()
        if not os.path.exists(os.path.join(cwd, "evaluators")):
             print("Warning: evaluators directory not found in CWD. Checking relative paths.")

        eval_cmd = ["poetry", "run", "python", "evaluators/test_query.py"]
        
        subprocess.run(eval_cmd, env=env, check=True)
        print(f"[{config_name}] Evaluation Completed Successfully.")
        
    except subprocess.CalledProcessError as e:
        print(f"[{config_name}] Evaluation Failed with exit code {e.returncode}")
    except Exception as e:
        print(f"[{config_name}] Unexpected error: {e}")

def main():
    for config in CONFIGURATIONS:
        config_name = config["name"]
        tools_list = config["tools"]
        restart_docker_and_run_eval(config_name, tools_list)
        time.sleep(5)

if __name__ == "__main__":
    main()
