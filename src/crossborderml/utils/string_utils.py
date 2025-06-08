"""Helper function do strings"""

from pathlib import Path


def derive_table_name(csv_path: Path, suffix: str = 'wide') -> str:
    """Return a name for the table based on the stem."""
    raw: str = f"{csv_path.stem}_{suffix}"
    cleaned: str = "".join(c if c.isalnum() or c == "_" else "_" for c in raw)
    return cleaned
