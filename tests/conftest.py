import os
from collections.abc import Generator
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pyprojroot import here
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.api.deps import get_db
from app.api.main import api_router
from app.core.db import Base


def remove_files(dir):
    try:
        for _root, _dirs, files in os.walk(dir):
            for f in files:
                os.remove(os.path.join(dir, f))
    except:
        pass


def start_application():
    app = FastAPI()
    app.include_router(api_router)
    return app


SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
# Use connect_args parameter only with sqlite
SessionTesting = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@pytest.fixture(scope="function")
def app() -> Generator[FastAPI, Any, None]:
    """Create a fresh database on each test case."""
    Base.metadata.create_all(engine)  # Create the tables.
    _app = start_application()
    yield _app
    Base.metadata.drop_all(engine)


@pytest.fixture(scope="function")
def db_session(app: FastAPI) -> Generator[SessionTesting, Any, None]:
    connection = engine.connect()
    transaction = connection.begin()
    session = SessionTesting(bind=connection)
    yield session  # use the session in tests.
    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture(scope="function")
def client(app: FastAPI, db_session: SessionTesting) -> Generator[TestClient, Any, None]:
    """TestClient

    Create a new FastAPI TestClient that uses the `db_session` fixture to override
    the `get_db` dependency that is injected into routes.
    """

    def _get_test_db():
        try:
            yield db_session
        finally:
            pass

    app.dependency_overrides[get_db] = _get_test_db
    with TestClient(app) as client:
        yield client


@pytest.fixture
def create_files() -> None:
    """Fixture to create test data files for scripts"""
    dir = Path(here() / "tests/data")
    dir.mkdir(parents=True, exist_ok=True)
    file_names = [dir / "USC00331541.txt", dir / "USC00123456.txt"]

    pd.DataFrame(
        data={1: [19850101, 19850102, 19850103], 2: [1, 2, 0], 3: [0, -2, -9999], 4: [1, 4, 1]}
    ).to_csv(file_names[0], sep="\t", header=False, index=False)
    pd.DataFrame(
        data={1: [19850101, 19850102, 19850103], 2: [0, 0, 0], 3: [-1, -2, -3], 4: [2, 2, -9999]}
    ).to_csv(file_names[1], sep="\t", header=False, index=False)


@pytest.fixture
def create_files__many() -> None:
    dir = Path(here() / "tests/data")
    dir.mkdir(parents=True, exist_ok=True)

    file_names = [dir / f"USC00{n}.txt" for n in range(1, 22)]

    for f in file_names:
        pd.DataFrame(
            data={
                1: [19850101, 19850102],
                2: np.random.randint(2),
                3: np.random.randint(2),
                4: np.random.randint(2),
            }
        ).to_csv(f, sep="\t", header=False, index=False)


@pytest.fixture
def expected_station_data() -> None:
    """Expected station data after loading into table"""
    return pd.DataFrame(
        data={
            "id": [1, 2, 3, 4, 5, 6],
            "station_id": [
                "USC00123456",
                "USC00123456",
                "USC00123456",
                "USC00331541",
                "USC00331541",
                "USC00331541",
            ],
            "date": [
                datetime(1985, 1, 1),
                datetime(1985, 1, 2),
                datetime(1985, 1, 3),
                datetime(1985, 1, 1),
                datetime(1985, 1, 2),
                datetime(1985, 1, 3),
            ],
            "max_temp": [0, 0, 0, 1.0, 2, 0],
            "min_temp": [-1, -2, -3, 0.0, -2, np.nan],
            "total_precip": [
                2,
                2,
                np.nan,
                1.0,
                4,
                1,
            ],
        }
    )


@pytest.fixture
def expected_summary_data() -> None:
    """Expected station summary data after loading to table"""
    return pd.DataFrame(
        data={
            "id": [1, 2],
            "station_id": ["USC00123456", "USC00331541"],
            "year": [1985, 1985],
            "avg_max_temp": [0, 1.0],
            "avg_min_temp": [
                -2.0,
                -1.0,
            ],
            "cumulative_precip": [4, 6.0],
        }
    )


@pytest.fixture
def weather__all():
    return [
        {
            "id": 1,
            "station_id": "USC00123456",
            "date": "1985-01-01",
            "max_temp": 0,
            "min_temp": -1,
            "total_precip": 2,
        },
        {
            "id": 2,
            "station_id": "USC00123456",
            "date": "1985-01-02",
            "max_temp": 0,
            "min_temp": -2,
            "total_precip": 2,
        },
        {
            "id": 3,
            "station_id": "USC00123456",
            "date": "1985-01-03",
            "max_temp": 0,
            "min_temp": -3,
            "total_precip": None,
        },
        {
            "id": 4,
            "station_id": "USC00331541",
            "date": "1985-01-01",
            "max_temp": 1,
            "min_temp": 0,
            "total_precip": 1,
        },
        {
            "id": 5,
            "station_id": "USC00331541",
            "date": "1985-01-02",
            "max_temp": 2,
            "min_temp": -2,
            "total_precip": 4,
        },
        {
            "id": 6,
            "station_id": "USC00331541",
            "date": "1985-01-03",
            "max_temp": 0,
            "min_temp": None,
            "total_precip": 1,
        },
    ]


@pytest.fixture
def weather__station():
    return [
        {
            "id": 1,
            "station_id": "USC00123456",
            "date": "1985-01-01",
            "max_temp": 0,
            "min_temp": -1.0,
            "total_precip": 2.0,
        },
        {
            "id": 2,
            "station_id": "USC00123456",
            "date": "1985-01-02",
            "max_temp": 0,
            "min_temp": -2.0,
            "total_precip": 2.0,
        },
        {
            "id": 3,
            "station_id": "USC00123456",
            "date": "1985-01-03",
            "max_temp": 0.0,
            "min_temp": -3.0,
            "total_precip": None,
        },
    ]


@pytest.fixture
def weather__station_date():
    return [
        {
            "id": 1,
            "station_id": "USC00123456",
            "date": "1985-01-01",
            "max_temp": 0.0,
            "min_temp": -1.0,
            "total_precip": 2.0,
        },
    ]


@pytest.fixture
def weather__date():
    return [
        {
            "id": 1,
            "station_id": "USC00123456",
            "date": "1985-01-01",
            "max_temp": 0.0,
            "min_temp": -1.0,
            "total_precip": 2.0,
        },
        {
            "id": 4,
            "station_id": "USC00331541",
            "date": "1985-01-01",
            "max_temp": 1.0,
            "min_temp": 0.0,
            "total_precip": 1.0,
        },
    ]


@pytest.fixture
def summary__all():
    return [
        {
            "id": 1,
            "station_id": "USC00123456",
            "year": 1985,
            "avg_max_temp": 0.0,
            "avg_min_temp": -2,
            "cumulative_precip": 4.0,
        },
        {
            "id": 2,
            "station_id": "USC00331541",
            "year": 1985,
            "avg_max_temp": 1.0,
            "avg_min_temp": -1.0,
            "cumulative_precip": 6.0,
        },
    ]


@pytest.fixture
def summary__station():
    return [
        {
            "id": 1,
            "station_id": "USC00123456",
            "year": 1985,
            "avg_max_temp": 0.0,
            "avg_min_temp": -2.0,
            "cumulative_precip": 4.0,
        }
    ]


@pytest.fixture
def summary__year():
    return [
        {
            "id": 1,
            "station_id": "USC00123456",
            "year": 1985,
            "avg_max_temp": 0.0,
            "avg_min_temp": -2.0,
            "cumulative_precip": 4.0,
        },
        {
            "id": 2,
            "station_id": "USC00331541",
            "year": 1985,
            "avg_max_temp": 1.0,
            "avg_min_temp": -1.0,
            "cumulative_precip": 6.0,
        },
    ]
