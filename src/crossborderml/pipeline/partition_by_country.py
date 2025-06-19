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


def get_main_columns(
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
    columns_per_table: dict[str, set[tuple[str, str, str]]] = {}

    for table in in_tables:
        sql_text = snippet.format(table=table)
        with engine.begin() as conn:
            results = conn.execute(text(sql_text))
            rows = results.fetchall()
            columns_per_table[table] = {tuple(row) for row in rows}

    return columns_per_table


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


def get_country_set(
        countries_dict: dict[str, set[tuple[str, str, str]]]
        ) -> dict[str, set[str]]:
    """
    return the dict with only the ids of the countries
    """
    return {
        key: {country for _, country, _ in entries}
        for key, entries in countries_dict.items()
    }


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

    country_sets: dict[str, set[str]] = get_country_set(countries_dict)

    black_sheep_keys = get_black_sheep(country_sets)

    if black_sheep_keys:
        print(f"Files with inconsistent country sets: {black_sheep_keys}"
              " -> droped!")

    return {
        key: entries for key, entries in countries_dict.items()
        if key not in black_sheep_keys
    }


def mk_tables_name(
        data: dict[str, set[tuple[str, str, str]]]
        ) -> set[str]:
    """Get a set of all the countries"""
    countries_name: dict[str, set[str]] = get_country_set(data)
    first_key = next(iter(countries_name))
    return {f"country_{code}_wide" for code in countries_name[first_key]}


class CreateCountryWideTables:
    """
    Build and execute the SQL that creates
    `country_<country_code>_wide`, which stacks one row per
    indicator (from each wide_table) containing only that
    country's data.
    """

    def __init__(self,
                 engine: Engine,
                 tables_name: set[str],
                 countries_indicators: dict[str, set[str]],
                 const_str  # CFG.validd
                 ) -> None:
        self.engine = engine
        self.tables_name = tables_name
        self.const_str = const_str
        self.in_tables: list[str] = list(countries_indicators.keys())
        self.indicators: list[str] = self.get_indicators(self.in_tables)
        self.countries_code: set[str] = \
            next(iter(countries_indicators.values()))

    def create(self, snippet_path: Path, year_range: tuple[int, int]) -> None:
        """self explanytory"""
        sql_temp: str = get_snippet(snippet_path)
        year_cols: str = self.get_years(year_range)
        for country_code in self.countries_code:
            selects = self.mk_country(country_code, year_cols, sql_temp)

    def mk_country(
            self,
            country_code: str,
            years: str,
            sql_temp: str
            ) -> list[str]:
        """make a sql text for each country"""
        selects: list[str] = []
        for wt, indicator in zip(self.in_tables, self.indicators):
            selects.append(sql_temp.format(
                indicator=indicator,
                year_columns=years,
                wide_table=wt,
                country_code=country_code
            ))
        return selects

    def get_years(self, year_range: tuple[int, int]) -> str:
        """return columns of years """
        years = [str(y) for y in range(*year_range)]
        return ", ".join(f'"{y}"' for y in years)

    def get_indicators(self,
                       raw_keys: list[str]) -> list[str]:
        """Get the indicator of each file"""
        return [
            item
            .split(self.const_str.file_prefix)[1]
            .split(self.const_str.file_suffix)[0]
            for item in raw_keys]


def create_tables() -> None:
    """Orchestrate the actions"""
    sql_engine: Engine = create_engine(CFG.sql.db_url)
    csv_files: set[Path] = get_files()

    all_in_tables: list[str] = \
        get_all_tables(csv_files, 'wide')
    all_columns: dict[str, set[tuple[str, str, str]]] = get_main_columns(
        sql_engine, CFG.sql.snippets_dir / 'countries_name', all_in_tables)
    all_columns = sanity_check_names(all_columns)
    tables_name: set[str] = mk_tables_name(all_columns)
    countries_indicators: dict[str, set[str]] = get_country_set(all_columns)

    country_tables = CreateCountryWideTables(
        engine=sql_engine,
        tables_name=tables_name,
        countries_indicators=countries_indicators,
        const_str=CFG.validd
        )
    country_tables.create(CFG.sql.snippets_dir / "per_country_wide",
                          CFG.sql.year_range)


if __name__ == '__main__':
    create_tables()
