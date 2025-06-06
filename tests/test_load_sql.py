"""
Tests for data validation in pipline
Created by CodePilot and I fixed them
"""

import sqlite3
from pathlib import Path

import pandas as pd

from crossborderml.pipeline import load_sql


def test_derive_table_name():
    """test"""
    assert load_sql.derive_table_name(Path("GDP.csv")) == "GDP_wide"
    assert load_sql.derive_table_name(
        Path("API.AG.LND.ZS.csv")) == "API_AG_LND_ZS_wide"
    assert load_sql.derive_table_name(Path("foo-bar.csv")) == "foo_bar_wide"
    assert load_sql.derive_table_name(Path("foo bar.csv")) == "foo_bar_wide"


def test_get_engine(tmp_path):
    """test"""
    db_path = tmp_path / "test.db"
    engine = load_sql.get_engine(f"sqlite:///{db_path}")
    # Test that we can connect and create a table
    with engine.begin() as conn:
        conn.execute(load_sql.text("CREATE TABLE test (a INTEGER)"))
        conn.execute(load_sql.text("INSERT INTO test (a) VALUES (1)"))
        result = conn.execute(load_sql.text("SELECT a FROM test")).fetchone()
        assert result[0] == 1


def test_load_csv_to_sql(tmp_path, monkeypatch):
    """test"""
    # Prepare a fake config
    csv_path = tmp_path / "foo.csv"
    csv_path.write_text("a,b\n1,2\n3,4\n")
    db_path = tmp_path / "test.db"

    class DummyCFG:
        """set dummy configuration"""
        class paths:
            """set dummy configuration"""
            extracted_data_dir = tmp_path
        class validd:
            """set dummy configuration"""
            file_prefix = ""
            file_extentions = ".csv"
        class sql:
            """set dummy configuration"""
            db_url = f"sqlite:///{db_path}"
        class csv:
            """set dummy configuration"""
            header_rows = 0

    monkeypatch.setattr(load_sql, "CFG", DummyCFG)
    # Patch FileFinder to just return our file
    monkeypatch.setattr(
        load_sql,
        "FileFinder",
        lambda **kwargs: type(
            "FF", (), {"get_file_paths": lambda self=None: {csv_path}}
            )())

    load_sql.load_all_wide_tables()

    # Check the table exists in SQLite
    con = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM foo_wide", con)
    assert df.shape == (2, 2)
    assert list(df.columns) == ["a", "b"]
    con.close()
