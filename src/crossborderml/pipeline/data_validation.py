"""
Check all the csv files and thier validability
"""

from pathlib import Path

import pandas as pd

from crossborderml.config import CFG
from crossborderml.utils.io_utils import FileFinder


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

    def assert_not_empty(self) -> None:
        """Self explanatory"""
        if self.csv_df.shape[0] == 0:
            raise AssertionError("DataFrame is empty (no rows).")

    def assert_cols(self, base_cols: list[str]) -> None:
        """Make sure the basic columns exist"""
        missing = [col for col in base_cols if col not in self.csv_df.columns]
        if missing:
            raise AssertionError(f"Missing columns: {missing}")


def main() -> None:
    """Self explanatory"""
    get_files: FileFinder = FileFinder(
        directory=CFG.paths.extracted_data_dir,
        prefix=CFG.validd.file_prefix,
        )

    csv_files: set[Path] = get_files.get_file_paths()
    for fpath in csv_files:
        csv = ReadCsv(fpath, CFG.cvs.header_rows)
        valid_df = ValidateCsv(csv.df)
        valid_df.assert_cols(CFG.cvs.basic_cols)


if __name__ == '__main__':
    main()
