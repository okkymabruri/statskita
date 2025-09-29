"""Utility functions for configuration management."""

from pathlib import Path
from typing import Any, Dict

import yaml


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Deep merge two dictionaries, with override taking precedence.

    Args:
        base: Base dictionary
        override: Dictionary with values to override

    Returns:
        Merged dictionary
    """
    result = base.copy()

    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            # recursively merge nested dicts
            result[key] = deep_merge(result[key], value)
        else:
            # override value
            result[key] = value

    return result


def load_config_with_inheritance(config_path: Path) -> Dict[str, Any]:
    """Load YAML config with inheritance support.

    If config has 'extends' field, load base config and merge.

    Args:
        config_path: Path to config file

    Returns:
        Merged configuration
    """
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # check for inheritance
    if "extends" in config:
        base_path = config_path.parent / config["extends"]
        if base_path.exists():
            # load base config
            with open(base_path, "r") as f:
                base_config = yaml.safe_load(f)

            # merge overrides if present
            if "overrides" in config:
                # start with base config
                merged = base_config.copy()

                # merge overrides into fields section if it exists
                if "fields" in merged:
                    for field_name, field_overrides in config["overrides"].items():
                        if field_name in merged["fields"]:
                            # update existing field
                            merged["fields"][field_name] = deep_merge(
                                merged["fields"][field_name],
                                field_overrides
                            )
                        else:
                            # add new field
                            merged["fields"][field_name] = field_overrides

                # keep non-override fields from wave config
                for key in config:
                    if key not in ["extends", "overrides"]:
                        merged[key] = config[key]
                return merged
            else:
                # no overrides, just merge
                return deep_merge(base_config, config)

    return config
