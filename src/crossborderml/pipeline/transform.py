"""
Load all the data with SQL and pass them for the further
analysis.

transform.py only defines „how”, and the outer code decides
„when” and „for which inputs.”
"""

from pathlib import Path
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
    def __init__(
            self,
            wide_table: str,
            db_url: str,
            snipt_path: Path,
            ) -> None:
        self.wide_table = wide_table

        if not wide_table.endswith("_wide"):
            raise ValueError(
                f"Expected wide_table to end with '_wide' got '{wide_table}")
        self.long_table: str = wide_table.replace("_wide", "_long")

        self.engine: Engine = create_engine(db_url)
        self.year_column: list[str] = \
            [str(y) for y in range(*CFG.sql.year_range)]
        self.sql_select: str = self.get_snippt(snipt_path)

    @staticmethod
    def get_snippt(snipt: Path) -> str:
        """Read and return the snippt"""
        with open(snipt, 'r', encoding='utf-8') as sql:
            return ''.join(sql.readlines())

    def build_select_cluses(self) -> list[str]:
        """
        For each year in self.year_column, format the SQL
        snippet so that {country_code} → "Country Code",
        {year} → <year literal>, {value} → "<year>",
        and {wide_table} → self.wide_table.

        Returns a list of SELECT…WHERE… strings, one for
        each year.
        """
        clauses: list[str] = []
        for yr in self.year_column:
            filled = self.sql_select.format(
                year=yr,
                wide_table=self.wide_table
            )
            clauses.append(filled)

        return clauses

    def assemble_union_query(self, select_clauses: list[str]) -> str:
        """
        Given a list of per-year SELECT strings, produce one
        complete SQL statement that:
            1. Drops any existing `self.long_table`
            2. Creates `self.long_table` AS the UNION ALL of
            all those SELECTs.
        Returns a single SQL string ready for execution.
        """
        drop_line = f"DROP TABLE IF EXISTS {self.long_table};"
        create_line = f"CREATE TABLE {self.long_table} AS"
        # Join all SELECT clauses with “UNION ALL”
        union_block = "\nUNION ALL\n".join(select_clauses)
        # Put it all together
        full_sql = f"{drop_line}\n{create_line}\n{union_block};"
        return full_sql


def create_long_table() -> None:
    """Orchestrate function"""
    pivot = PivotOneIndicator(
        "GDP_wide",
        CFG.sql.db_url,
        CFG.sql.snippts_dir / "per_year_select")
    cluases: list[str] = pivot.build_select_cluses()
    main_sql = pivot.assemble_union_query(cluases)


if __name__ == '__main__':
    create_long_table()
