"""Setting the paths and other constant needed in scripts"""

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class UrlConfig:
    """Set the main URL requests and parameters"""
    world_bank_indicator = \
        "https://api.worldbank.org/v2/en/indicator/{code}?downloadformat=csv"
    timeout: float = 60.0

    def world_bank_indicator_url(self,
                                 code: str
                                 ) -> str:
        """Return the download URL for a given World Bank indicator code."""
        return self.world_bank_indicator.format(code=code)


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
class ValidData:
    """Configs for validation of data"""
    file_prefix: str = "API_"
    file_suffix: str = "_DS"
    file_extentions: str = "csv"


@dataclass(frozen=True)
class CsvConfig:
    """In reading CVS files"""
    header_rows: int = 2
    # The columns which should be there
    basic_cols: list[str] = \
        field(default_factory=lambda: ["Country Name", "Country Code"])


@dataclass(frozen=True)
class SqlConfig:
    """Configs for the SQL queries"""
    db_url: str = f"sqlite:///{PathConfig().project_root/'data'/'mydb.sqlite'}"
    queries_dir: Path = PathConfig().project_root / "sql" / "queries"
    snippets_dir: Path = PathConfig().project_root / "sql" / "snippets"
    year_range: tuple[int, int] = (1960, 2026)


@dataclass(frozen=True)
class Config:
    """Binding them together"""
    urls: UrlConfig = UrlConfig()
    paths: PathConfig = PathConfig()
    validd: ValidData = ValidData()
    csv: CsvConfig = CsvConfig()
    sql: SqlConfig = SqlConfig()


CFG = Config()
