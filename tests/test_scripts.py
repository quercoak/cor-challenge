from collections.abc import Generator
from typing import Any

import pandas as pd
from fastapi.testclient import TestClient
from pandas.testing import assert_frame_equal
from pyprojroot import here

from scripts.load import main as load_main
from scripts.summarize import summarize_stations
from tests.conftest import SQLALCHEMY_DATABASE_URL, engine, remove_files


def test_load_data(
    client: Generator[TestClient, Any, None],
    create_files: None,
    expected_station_data: pd.DataFrame,
):
    """Test case includes 3 dates for 2 files including missing data.

    Include create_files fixture to make new files and client to create and flush database
    """
    try:
        dir = str(here() / "tests/data")
        load_main(data_dir=dir, db=SQLALCHEMY_DATABASE_URL)

        # read sql and compare as dataframes for convenience of assert_dataframe_equal
        with engine.connect() as connection:
            df_station_data = pd.read_sql("station_data", con=connection, parse_dates=["date"])
            assert_frame_equal(df_station_data, expected_station_data)
    finally:
        remove_files(dir)


def test_summarize_data(
    client: Generator[TestClient, Any, None],
    create_files: None,
    expected_summary_data: pd.DataFrame,
):
    """Test case includes 3 dates for 2 files including missing data resulting in 2 stations and 2 years.

    Include create_files fixture to make new files and client to create and flush database
    """
    try:
        # run load data script to fill db
        dir = str(here() / "tests/data")
        load_main(data_dir=dir, db=SQLALCHEMY_DATABASE_URL)

        # run summary
        rowcount = summarize_stations(engine=engine)
        assert rowcount == 2

        # read sql and compare as dataframes for convenience of assert_dataframe_equal
        with engine.connect() as connection:
            df_station_summary = pd.read_sql("station_summary", con=connection)
            assert_frame_equal(df_station_summary, expected_summary_data)
    finally:
        remove_files(dir)
