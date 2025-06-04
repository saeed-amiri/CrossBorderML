"""
Load each validated CSV into a SQLite table named:
<Indicator>_wide.

1. Find all CSV files under the “extracted_data_dir” that
   match the expected prefix/extension.
2. For each CSV:
   a. Derive a safe table name: use the filename
      stem + "_wide", replacing any non-alphanumeric
      characters with underscores.
      Example: "GDP.csv" → "GDP_wide";
               "API.AG.LND.ZS.csv" → "API_AG_LND_ZS_wide".
   b. Read the CSV into a pandas DataFrame (using the same
      header rows as validation).
   c. Inside a transaction:
      i.  Drop any existing table by that same name.
      ii. Write the DataFrame to SQL (creating a new
      "<Indicator>_wide" table whose columns exactly match
      the CSV).
   d. Delete the DataFrame to free memory before processing
      the next file.
3. After all CSVs are processed, the database file (at
   CFG.sql.db_url) contains one “_wide” table per indicator.
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
