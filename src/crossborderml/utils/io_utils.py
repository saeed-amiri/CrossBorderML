"""
A module for containing utility functions
"""

from pathlib import Path
import yaml


def load_yaml(yaml_path: Path,
              main_key: str
              ) -> dict[str, str]:
    """
    Load indicator names and codes from a YAML config file.

    Returns:
        A dict of {name: code}

    Raises:
        FileNotFoundError: if the file does not exist
        yaml.YAMLError: if the YAML is invalid
        KeyError: if 'main_key' section is missing
    """
    try:
        with open(yaml_path, 'r', encoding='utf-8') as f:
            yaml_data = yaml.load(f, Loader=yaml.SafeLoader)
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"Indicator file not found: {yaml_path}"
            ) from exc
    except PermissionError as exc:
        raise PermissionError(
            f"Permission denied when reading: {yaml_path}"
            ) from exc
    except yaml.YAMLError as exc:
        raise yaml.YAMLError(
            f"Invalid YAML in file: {yaml_path}\nError: {exc}"
            ) from exc

    if main_key not in yaml_data or \
       not isinstance(yaml_data[main_key], dict):
        raise KeyError(
            f"{main_key} section missing or not a dict in: {yaml_path}"
            )

    return yaml_data[main_key]
