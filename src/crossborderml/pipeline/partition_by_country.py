"""A side module to make table for each country"""

from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from crossborderml.config import CFG
from crossborderml.utils.io_utils import FileFinder, get_snippet
from crossborderml.utils.string_utils import derive_table_name


def get_files() -> set[Path]:
    """Return the paths for CSV files"""
    csv_finder = FileFinder(
        directory=CFG.paths.extracted_data_dir,
        prefix=CFG.validd.file_prefix,
        extension=CFG.validd.file_extentions)
    return csv_finder.get_file_paths()


def get_engine(db_url: str) -> Engine:
    """
    Create (or return a cached) SQLAlchemy engine based on
    CFG.sql.db_url
    """
    return create_engine(db_url)


def get_all_tables(files: set[Path], suffix: str) -> list[str]:
    """Get name of the all the table with _wide in their name"""
    tables: list[str] = []
    for csv_path in files:
        t_names: str = derive_table_name(csv_path, suffix=suffix)
        tables.append(t_names)
    return tables


def get_all_countries(
        engine: Engine,
        snipt_path: Path,
        in_tables: list[str]
        ) -> dict[str, set[tuple[str, str, str]]]:
    """
    Get the set of (Country Name, Country Code, Indicator
    Code) tuples for all specified tables.

    Args:
        engine: SQLAlchemy Engine connected to the database.
        snipt_path: Path to the SQL snippet file with a
        {table} placeholder.
        in_tables: List of table names to query.

    Returns:
        Dictionary mapping table name to a set of tuples
        with country info.
    """
    snippet: str = get_snippet(snipt_path)
    countries_per_table: dict[str, set[tuple[str, str, str]]] = {}

    for table in in_tables:
        sql_text = snippet.format(table=table)
        with engine.begin() as conn:
            results = conn.execute(text(sql_text))
            rows = results.fetchall()
            countries_per_table[table] = {tuple(row) for row in rows}

    return countries_per_table


if __name__ == '__main__':
    sql_engine: Engine = create_engine(CFG.sql.db_url)
    csv_files: set[Path] = get_files()

    all_tables: list[str] = \
        get_all_tables(csv_files, 'wide')
    all_countries: dict[str, set[tuple[str, str, str]]] = get_all_countries(
        sql_engine, CFG.sql.snippets_dir / 'countries_name', all_tables)
