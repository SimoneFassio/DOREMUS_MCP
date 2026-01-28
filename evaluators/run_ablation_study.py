
import os
import subprocess
import time
import sys
from typing import List, Dict, Union

CONFIGURATIONS = [
    {
        "name": "Sampling_OFF_DryRun_OFF",
        "ENABLE_SAMPLING": "false",
        "ENABLE_DRY_RUN": "false"
    },
    {
        "name": "Sampling_ON_DryRun_OFF",
        "ENABLE_SAMPLING": "true",
        "ENABLE_DRY_RUN": "false"
    },
    {
        "name": "Sampling_OFF_DryRun_ON",
        "ENABLE_SAMPLING": "false",
        "ENABLE_DRY_RUN": "true"
    },
    {
        "name": "FULL",
        "ENABLE_SAMPLING": "true",
        "ENABLE_DRY_RUN": "true"
    }
]

def restart_docker_and_run_eval(config: Dict[str, str]):
    config_name = config["name"]
    print(f"\n{'='*60}")
    print(f"Starting Experiment Configuration: {config_name}")
    print(f"Settings: SAMPLING={config['ENABLE_SAMPLING']}, DRY_RUN={config['ENABLE_DRY_RUN']}")
    print(f"{'='*60}\n")

    # 1. Set Environment Variable
    env = os.environ.copy()
    env["ENABLE_SAMPLING"] = config["ENABLE_SAMPLING"]
    env["ENABLE_DRY_RUN"] = config["ENABLE_DRY_RUN"]

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
        restart_docker_and_run_eval(config)
        time.sleep(5)

if __name__ == "__main__":
    main()
