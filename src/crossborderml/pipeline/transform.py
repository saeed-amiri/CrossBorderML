"""
Load all the data with SQL and pass them for the further
analysis.

transform.py only defines „how”, and the outer code decides
„when” and „for which inputs.”
"""

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from crossborderml.config import CFG


class PivotOneIndicator:
    """
    Its sole job is: given a “wide” table (one row per
    country, one column per year), produce a “long” table
    (rows = country × year × value).
    It should not run automatically—only when you explicitly
    call its method.
    """
    def __init__(self, wide_table: str, db_url: str) -> None:
        self.wide_table = wide_table

        if not wide_table.endswith("_wide"):
            raise ValueError(
                f"Expected wide_table to end with '_wide' got '{wide_table}")
        self.long_table: str = wide_table.replace("_wide", "_long")

        self.engine: Engine = create_engine(db_url)
        self.year_column: list[str] = \
            [str(y) for y in range(*CFG.sql.year_range)]


if __name__ == '__main__':
    pivot = PivotOneIndicator("GDP_wide", CFG.sql.db_url)
