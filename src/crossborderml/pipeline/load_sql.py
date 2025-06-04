"""
Discover which CSVs to load:
  - Identify the folder where validated CSVs reside (e.g.
  data/extracted).
  - Gather the list of CSV filenames in that directory (for
  example, every file matching *.csv).
For each CSV file:
  - Derive a table name
  - Take the CSV's filename (without “.csv”) and append a
  suffix like _wide.
    Example: GDP.csv → table name GDP_wide.
  - Open a database connection
  - Use the configured SQL connection (SQLite URL or other)
  to obtain an engine/connection.
  - Ensure transactions are managed so that each table load
  either fully succeeds or rolls back on error.
  - Read the CSV into memory (or stream)
  - Load the entire CSV into a tabular structure (e.g.
  Pandas DataFrame), preserving all columns exactly as they
  appear.
  This step assumes the CSV already passed validation, so
  headers match expectations.
  - Write (or replace) the SQL table
  If a table with the target name (GDP_wide) already exists,
  drop it or replace it.
Create a new SQL table whose columns exactly mirror the
CSV's columns:
  - Column types for “Country Name” and “Country Code”
  become text/varchar.
  - Each year column (“1960,” “1961,” …) becomes a numeric
  or text column as appropriate.
  - Keep the header row names unchanged, since pivoting
  later expects those exact names.
  - Insert all rows
  - Bulk-insert every row from the DataFrame into the newly
  created table.
  - Commit the transaction once all rows are in place.
  - Log or record success/failure
  - After each table load, record that <Indicator>_wide now
  exists in the database.
  - If any error occurs (e.g. malformed CSV despite prior
  validation, or disk-full), catch it, roll back the partial
  load, and flag failure for that specific indicator.
Finalize
After looping through all CSVs, close the database
connection.
Optionally, return a summary of which tables succeeded and
which failed.
"""

from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from crossborderml.config import CFG
from crossborderml.utils.io_utils import FileFinder


def get_files() -> set[Path]:
    """Return the paths for CSV files"""
    csv_finder = FileFinder(
        directory=CFG.paths.extracted_data_dir,
        prefix=CFG.validd.file_prefix,
        extension=CFG.validd.file_extentions)
    return csv_finder.get_file_paths()


def derive_table_name(csv_path: Path) -> str:
    """Return a name for the table based on the stem."""
    raw: str = f"{csv_path.stem}_wide"
    cleaned: str = "".join(c if c.isalnum() or c == "_" else "_" for c in raw)
    return cleaned


def get_engine(db_url: str) -> Engine:
    """
    Create (or return a cached) SQLAlchemy engine based on
    CFG.sql.db_url
    """
    return create_engine(db_url)


def main() -> None:
    """self explanatory"""
    csv_files: set[Path] = get_files()

    engine: Engine = get_engine(CFG.sql.db_url)
    print(f"Using database URL: {CFG.sql.db_url}")

    for csv_path in sorted(csv_files):
        table_name = derive_table_name(csv_path)
        print(f"- CSV: {csv_path.name}  →  Table: {table_name}")

        df_i: pd.DataFrame = pd.read_csv(csv_path, header=CFG.csv.header_rows)

        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            df_i.to_sql(table_name, con=conn, if_exists="replace", index=False)
        del df_i

        print(f"[OK] Connection and transaction test for table '{table_name}'"
              " succeeded.\n")


if __name__ == '__main__':
    main()
