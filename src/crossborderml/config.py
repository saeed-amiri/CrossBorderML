"""Setting the paths and other constant needed in scripts"""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PathConfig:
    """Main paths"""
    project_root: Path = Path(__file__).resolve().parents[2]
    data_dir: Path = project_root / "data"
    raw_data_dir: Path = data_dir / "raw"
    processed_data_dir: Path = data_dir / "processed"
    extracted_data_dir: Path = data_dir / "extracted"
    sql_dir: Path = project_root / "sql" / "queries"
    indicator_yaml: Path = \
        project_root / "src" / "crossborderml" / "conf" / "indicators.yaml"
    data_readme: Path = data_dir / "README.md"


@dataclass(frozen=True)
class Config:
    """Binding them together"""
    paths: PathConfig = PathConfig()


CFG = Config()
