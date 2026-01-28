import os
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger("doremus-mcp")

# Paths
ROOT_DIR = Path(__file__).parent
CONFIG_DIR = ROOT_DIR / "config"

def _load_yaml(filename: str) -> Dict[str, Any]:
    """Helper to load a YAML file from the config directory."""
    path = CONFIG_DIR / filename
    if not path.exists():
        logger.warning(f"Config file not found: {path}. Returning empty dict.")
        return {}
    
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.error(f"Error loading {filename}: {e}")
        return {}

def load_server_config() -> Dict[str, Any]:
    """Load server_config.yaml"""
    return _load_yaml("server_config.yaml")

def load_strategies() -> Dict[str, Any]:
    """Load strategies.yaml"""
    config = _load_yaml("strategies.yaml")
    return config.get("strategies", {})

def load_tool_config(tool_name: str) -> Optional[str]:
    """Load description for a specific tool from tools.yaml"""
    config = _load_yaml("tools.yaml")
    tools = config.get("tools", {})
    tool_info = tools.get(tool_name)
    
    if isinstance(tool_info, str):
        return tool_info
    elif isinstance(tool_info, dict):
        return tool_info.get("description")
    return None

def load_all_tool_configs() -> Dict[str, Any]:
    """Load all tool configurations."""
    config = _load_yaml("tools.yaml")
    return config.get("tools", {})

# Load Server Configuration
_server_config = load_server_config()

def _get_config_value(key: str, config: Dict[str, Any]) -> Any:
    if key not in config:
        logger.warning(f"Config parameter '{key}' tried to be loaded but not present in server_config.yaml")
        return None 
    return config[key]

# Exported Configuration Variables
SPARQL_ENDPOINT = _get_config_value("sparql_endpoint", _server_config)
REQUEST_TIMEOUT = _get_config_value("request_timeout", _server_config)
PREFIXES = _get_config_value("prefixes", _server_config)
DISCOVERY_CONFIG = _get_config_value("discovery", _server_config) or {}
