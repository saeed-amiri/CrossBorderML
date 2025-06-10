"""A side module to make table for each country"""

from pathlib import Path
from collections import Counter
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


def get_black_sheep(data: dict[str, set[str]]) -> list[str]:
    """
    Identify all 'black sheep' in the dataset — keys whose
    associated sets have a length that is unique (occurs
    only once).

    Parameters:
        data (dict[str, set[str]]): Dictionary mapping keys
        to sets.

    Returns:
        list[str]: List of keys corresponding to unique-length
        sets.
    """
    # Step 1: Compute the length of each set
    lengths = {key: len(value_set) for key, value_set in data.items()}

    # Step 2: Count how many times each length occurs
    length_counts = Counter(lengths.values())

    # Step 3: Identify all lengths that occur only once
    unique_lengths = {length for length, count in
                      length_counts.items() if count == 1}

    # Step 4: Return all keys with one of the unique lengths
    return [key for key, length in lengths.items() if length in unique_lengths]


def sanity_check_names(
        countries_dict: dict[str, set[tuple[str, str, str]]]
        ) -> dict[str, set[tuple[str, str, str]]]:
    """
    Filter out files whose country lists deviate from the
    others.

    The function assumes that the second element of each
    tuple is the country name.
    It uses set length comparison to detect outliers (black
    sheep).
    Parameters:
        countries_dict (dict[str, set[tuple]]): Mapping of
        filename -> set of (id, country, value)
    Returns:
        dict[str, set[tuple]]: Filtered dictionary excluding
        black sheep files
    """

    country_sets: dict[str, set[str]] = {
        key: {country for _, country, _ in entries}
        for key, entries in countries_dict.items()
    }

    black_sheep_keys = get_black_sheep(country_sets)

    if black_sheep_keys:
        print(f"Files with inconsistent country sets: {black_sheep_keys}"
              " -> droped!")

    return {
        key: entries for key, entries in countries_dict.items()
        if key not in black_sheep_keys
    }


if __name__ == '__main__':
    sql_engine: Engine = create_engine(CFG.sql.db_url)
    csv_files: set[Path] = get_files()

    all_tables: list[str] = \
        get_all_tables(csv_files, 'wide')
    all_countries: dict[str, set[tuple[str, str, str]]] = get_all_countries(
        sql_engine, CFG.sql.snippets_dir / 'countries_name', all_tables)
    all_countries = sanity_check_names(all_countries)
