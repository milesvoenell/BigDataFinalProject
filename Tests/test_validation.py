# Tests/test_validation.py

import pytest
import pandas as pd
import polars as pl
from typing import Any, Dict
from pathlib import Path

from Data.Validation import RunnerRecord, validate_csv


def test_runner_record_valid() -> None:
    data: Dict[str, Any] = {
        "Year": 2025,
        "Race": "NYC Marathon",
        "Name": "John Doe",
        "Gender": "M",
        "Age": 30,
        "State": "NY",
        "Country": "USA",
        "Overall": 100,
        "Finish_Time": "03:00:00",
        "finish_seconds": 10800,
        "Finish": 10800,
    }

    record: RunnerRecord = RunnerRecord(**data)
    assert record.Name == "John Doe"


def test_validate_csv(tmp_path: Path) -> None:
    test_file: Path = tmp_path / "test.csv"
    test_file.write_text(
        "Year,Race,Name,Gender,Age,State,Country,Overall,Finish_Time,finish_seconds,Finish\n"
        "2025,NYC Marathon,John Doe,M,30,NY,USA,100,03:00:00,10800,10800\n"
    )

    output_file: Path = tmp_path / "validated.csv"
    df: pl.DataFrame = validate_csv(str(test_file), str(output_file))

    df_pd: pd.DataFrame = df.to_pandas()

    assert len(df_pd) == 1
    assert df_pd.iloc[0]["Name"] == "John Doe"


def test_invalid_row_dropped(tmp_path: Path) -> None:
    test_file: Path = tmp_path / "invalid.csv"
    test_file.write_text(
        "Year,Race,Name,Gender,Age,State,Country,Overall,Finish_Time,finish_seconds,Finish\n"
        "2025,NYC Marathon,John Doe,M,30,NY,USA,100,03:00:00,10800,10800\n"
        "2025,NYC Marathon,Jane Doe,F,INVALID,CA,USA,150,03:15:00,11700,11700\n"
    )

    output_file: Path = tmp_path / "validated.csv"
    df: pl.DataFrame = validate_csv(str(test_file), str(output_file))

    df_pd: pd.DataFrame = df.to_pandas()

    # Only the valid row should remain
    assert len(df_pd) == 1
    assert df_pd.iloc[0]["Name"] == "John Doe"
