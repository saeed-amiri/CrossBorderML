"""
Tests for transform in pipline
Created by CodePilot and I fixed them
"""

import sqlite3
import pandas as pd
import pytest


@pytest.fixture
def setup_sqlite_and_snippt(tmp_path):
    """test"""
    # Create a dummy wide table in SQLite
    db_path = tmp_path / "test.db"
    engine_url = f"sqlite:///{db_path}"
    con = sqlite3.connect(db_path)
    con.execute('CREATE TABLE foo_wide ("Country Code" TEXT, '
                '"2020" INTEGER, "2021" INTEGER)')
    con.execute('INSERT INTO foo_wide VALUES ("AAA", 10, 20)')
    con.execute('INSERT INTO foo_wide VALUES ("BBB", 30, 40)')
    con.commit()
    con.close()

    # Create a dummy SQL snippet file
    snippt_path = tmp_path / "per_year_select"
    # This snippet will be formatted with year and wide_table
    snippt_path.write_text(
        'SELECT "Country Code" AS country, {year} AS year, '
        '"{year}" AS value FROM {wide_table}'
    )

    class DummyCFG:
        class sql:
            db_url = engine_url
            year_range = (2020, 2022)  # 2020 and 2021
            snippts_dir = tmp_path

    return db_path, snippt_path, DummyCFG

def test_pivot_one_indicator(monkeypatch, setup_sqlite_and_snippt):
    db_path, snippt_path, DummyCFG = setup_sqlite_and_snippt

    # Patch CFG in the module
    import crossborderml.pipeline.transform as transform_mod
    monkeypatch.setattr(transform_mod, "CFG", DummyCFG)

    pivot = transform_mod.PivotOneIndicator(
        wide_table="foo_wide",
        db_url=DummyCFG.sql.db_url,
        snipt_path=snippt_path
    )

    # Test build_select_clause
    clauses = pivot.build_select_clause()
    assert len(clauses) == 2
    assert "2020" in clauses[0]
    assert "2021" in clauses[1]

    # Test assemble_union_query
    union_sql = pivot.assemble_union_query(clauses)
    assert "CREATE TABLE foo_long AS" in union_sql
    assert "UNION ALL" in union_sql

    # Test execute_union (creates foo_long table)
    pivot.execute_union(union_sql)

    # Check the new long table
    con = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM foo_long", con)
    assert set(df["country"]) == {"AAA", "BBB"}
    assert set(df["year"]) == {2020, 2021}
    assert set(df["value"]) == {"2020", "2021"} or \
        set(df["value"]) == {10, 20, 30, 40}
    con.close()
