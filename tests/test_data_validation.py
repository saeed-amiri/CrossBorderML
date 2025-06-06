"""
Tests for data validation in pipline
Created by CodePilot and I fixed them
"""


import pytest
import pandas as pd
from crossborderml.pipeline.data_validation import ReadCsv, ValidateCsv


def test_readcsv_valid(tmp_path):
    """test"""
    csv_path = tmp_path / "test.csv"
    csv_path.write_text("a,b\n1,2\n3,4\n")
    reader = ReadCsv(csv_path, header_rows=0)
    assert isinstance(reader.df, pd.DataFrame)
    assert list(reader.df.columns) == ["a", "b"]
    assert reader.df.shape == (2, 2)


def test_readcsv_empty(tmp_path):
    """test"""
    csv_path = tmp_path / "empty.csv"
    csv_path.write_text("")
    with pytest.raises(pd.errors.EmptyDataError):
        ReadCsv(csv_path, header_rows=0)


def test_readcsv_parser_error(tmp_path):
    """test"""
    csv_path = tmp_path / "bad.csv"
    # Unclosed quote to trigger ParserError
    csv_path.write_text('a,b\n1,"2\n3,4\n')
    with pytest.raises(pd.errors.ParserError):
        ReadCsv(csv_path, header_rows=0)


def test_readcsv_unicode_error(tmp_path):
    """test"""
    csv_path = tmp_path / "badenc.csv"
    # Write bytes that are not valid utf-8
    csv_path.write_bytes(b'\xff\xfe\x00\x00')
    with pytest.raises(ValueError):
        ReadCsv(csv_path, header_rows=0)


def test_validatecsv_assert_not_empty():
    """test"""
    df = pd.DataFrame({"a": [1]})
    val = ValidateCsv(df)
    val.assert_not_empty()  # Should not raise

    df_empty = pd.DataFrame(columns=["a"])
    val_empty = ValidateCsv(df_empty)
    with pytest.raises(AssertionError):
        val_empty.assert_not_empty()


def test_validatecsv_assert_cols():
    """test"""
    df = pd.DataFrame({"a": [1], "b": [2]})
    val = ValidateCsv(df)
    val.assert_cols(["a", "b"])  # Should not raise

    with pytest.raises(AssertionError) as excinfo:
        val.assert_cols(["a", "b", "c"])
    assert "Missing columns" in str(excinfo.value)
