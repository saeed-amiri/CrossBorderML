"""
A module for containing utility functions
"""

from pathlib import Path
import yaml


def load_yaml(yaml_path: Path, main_key: str) -> dict[str, str]:
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


class FileFinder:
    """
    Find files in a directory matching an optional prefix
    and extension, and (optionally) return either their full
    Path objects or just the „stem“ (filename with prefix
    and suffix removed).
    """
    # pylint: disable=too-few-public-methods

    def __init__(
        self,
        directory: Path,
        prefix: str = "",
        extension: str = ""
    ) -> None:
        """
        Args:
            - directory:     The folder in which to look for
            files.
            - prefix:        If non-empty, only consider
            filenames that start with this.
            - extension:     If non-empty, only consider files
            with this suffix (e.g. ".csv").
        """
        self.directory = directory
        self.prefix = prefix
        self.extension = extension

    def get_file_paths(self) -> set[Path]:
        """
        Returns a set of Path objects for every file in
        `directory` whose name starts with `prefix`.
        """
        if not self.directory.exists() or not self.directory.is_dir():
            raise FileNotFoundError(f"Directory not found: {self.directory}")

        results: set[Path] = set()
        for p in self.directory.iterdir():
            if not p.is_file():
                continue

            name = p.name
            # Check prefix
            if self.prefix and not name.startswith(self.prefix):
                continue

            results.add(p)

        return results

    def get_file_names(self,
                       strip_prefix: str,
                       strip_suffix: str
                       ) -> set[str]:
        """
        Args:
            - strip_prefix: remove this prefix from the
            returned stem.
            - strip_suffix: remove this suffix from the
            returned stem.
        Returns a set of „stem“ strings for every file
        meeting the same criteria (prefix + extension).
        From each matching file's stem (filename without its
        suffix), it removes `strip_prefix` (from the front)
        and `strip_suffix` (from the end) before returning.

        Example:
            If prefix="data_", extension=".csv",
            strip_prefix="data_", strip_suffix=".csv"
            and you have a file named „data_population.csv“
            in the directory, get_file_names() will include
            „population“ in its returned set.
        """
        # First, gather all matching Path objects
        paths = self.get_file_paths()
        names: set[str] = set()

        for p in paths:
            stem = p.stem
            # Remove strip_prefix if present
            if strip_prefix and stem.startswith(strip_prefix):
                stem = stem[len(strip_prefix):]
            # Remove strip_suffix if present
            if strip_suffix and stem.endswith(strip_suffix):
                stem = stem[: -len(strip_suffix)]
            names.add(stem)

        return names


def get_snippet(snipt: Path) -> str:
    """Read and return the snippet"""
    with open(snipt, 'r', encoding='utf-8') as sql:
        return ''.join(sql.readlines())
