"""
Check all the csv files and thier validability
"""

from pathlib import Path

import pandas as pd

from crossborderml.config import CFG


class FileLister:
    """
    List all the files in a directory that start with a given
    prefix.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self,
                 directory: Path,
                 prefix: str,
                 ) -> None:
        self.dir = directory
        self.prefix = prefix

    def list_existing(self) -> set[Path]:
        """Listing the main names"""
        return {
            p
            for p in self.dir.iterdir()
            if p.is_file() and p.name.startswith(self.prefix)
        }


class ReadCsv:
    """
    Read a CSV file (with a given number of header rows)
    into a DataFrame.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, file_path: Path, header_rows: int) -> None:
        try:
            self.df: pd.DataFrame = \
                pd.read_csv(file_path, header=header_rows)
        except PermissionError as exc:
            raise PermissionError(
                f"Permission Error in loading {file_path}"
                ) from exc
        except pd.errors.EmptyDataError as exc:
            raise pd.errors.EmptyDataError(
                f"Error! DataFrame is completely empty: {file_path}"
                ) from exc
        except pd.errors.ParserError as exc:
            raise pd.errors.ParserError(
                f"Error in interpreting the data in {file_path}"
                ) from exc
        except UnicodeDecodeError as exc:
            raise ValueError(
                f"Encoding error in reading {file_path}"
                ) from exc
        except ValueError as exc:
            raise ValueError(
                f"Unexpected parameters in loading {file_path}"
                ) from exc


class ValidateCsv:
    """Assert CSV data"""
    # pylint: disable=too-few-public-methods
    def __init__(self,
                 csv_df: pd.DataFrame,
                 ) -> None:
        self.csv_df = csv_df

    def assert_cols(self, base_cols: list[str]) -> None:
        """Make sure the basic columns exist"""
        missing = [col for col in base_cols if col not in self.csv_df.columns]
        if missing:
            raise AssertionError(f"Missing columns: {missing}")


def main() -> None:
    """Self explanatory"""
    get_files: FileLister = FileLister(
        directory=CFG.paths.extracted_data_dir,
        prefix=CFG.validd.file_prefix
    )
    csv_files: set[Path] = get_files.list_existing()
    for fpath in csv_files:
        csv = ReadCsv(fpath, CFG.cvs.header_rows)
        valid_df = ValidateCsv(csv.df)
        valid_df.assert_cols(CFG.cvs.basic_cols)


if __name__ == '__main__':
    main()
