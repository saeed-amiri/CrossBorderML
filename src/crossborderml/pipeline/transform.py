"""
Load all the data with SQL and pass them for the further
analysis.

transform.py
"""

from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from crossborderml.config import CFG
from crossborderml.utils import io_utils
from crossborderml.pipeline.pivot_wide_to_long import create_long_table


def pivot_all_indicators(snippet_path: Path, db_url: str) -> None:
    """
    1. Query sqlite_master for all table names ending in "_wide".
    2. For each name:
        create a long table with „create_long_table”
    """

    sql_select: str = io_utils.get_snippet(snippet_path)
    engine: Engine = create_engine(db_url)
    with engine.begin() as conn:
        result = conn.execute(text(sql_select))
        wide_tables = [row[0] for row in result]

    for wide_table in wide_tables:
        create_long_table(wide_table=wide_table)


if __name__ == '__main__':
    pivot_all_indicators(
        CFG.sql.queries_dir / "tables_name.sql", CFG.sql.db_url)
