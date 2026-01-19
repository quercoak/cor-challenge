from collections.abc import Generator
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pyprojroot import here

from scripts.load import main as load_main
from scripts.summarize import summarize_stations
from tests.conftest import SQLALCHEMY_DATABASE_URL, engine, remove_files


def test_weather__all(
    client: Generator[TestClient, Any, None], create_files: None, weather__all: dict
) -> None:
    """Test returning all values."""
    try:
        # run load data script
        dir = str(here() / "tests/data")
        load_main(data_dir=dir, db=SQLALCHEMY_DATABASE_URL)

        response = client.get("http://localhost:8000/weather/")
        assert response.status_code == 200
        assert response.json() == weather__all
    finally:
        remove_files(dir)


def test_weather__station(
    client: Generator[TestClient, Any, None], create_files: None, weather__station: dict
) -> None:
    """Test returning a single station."""
    # run load data script
    try:
        dir = str(here() / "tests/data")
        load_main(data_dir=dir, db=SQLALCHEMY_DATABASE_URL)

        response = client.get("http://localhost:8000/weather/?station_id=USC00123456")
        assert response.status_code == 200
        assert response.json() == weather__station
    finally:
        remove_files(dir)


def test_weather__station_date(
    client: Generator[TestClient, Any, None], create_files: None, weather__station_date: dict
) -> None:
    """Test returning a station and date."""
    try:
        # run load data script
        dir = str(here() / "tests/data")
        load_main(data_dir=dir, db=SQLALCHEMY_DATABASE_URL)

        response = client.get(
            "http://localhost:8000/weather/?station_id=USC00123456&date=1985-01-01"
        )
        assert response.status_code == 200
        assert response.json() == weather__station_date
    finally:
        remove_files(dir)


def test_weather__date(
    client: Generator[TestClient, Any, None], create_files: None, weather__date: dict
) -> None:
    """Test returning a date."""
    try:
        # run load data script
        dir = str(here() / "tests/data")
        load_main(data_dir=dir, db=SQLALCHEMY_DATABASE_URL)

        response = client.get("http://localhost:8000/weather/?date=1985-01-01")
        assert response.status_code == 200
        assert response.json() == weather__date
    finally:
        remove_files(dir)


@pytest.mark.parametrize(
    "limit,offset,count,first_id",
    [
        pytest.param(20, 0, 20, 1, id="pagination page 1"),
        pytest.param(20, 40, 2, 41, id="pagination page 2"),
    ],
)
def test_weather__pagination(
    client: Generator[TestClient, Any, None],
    create_files__many: None,
    limit: int,
    offset: int,
    count: int,
    first_id: int,
) -> None:
    """Test pagination in weather endpoint. Upload 21 stations and 42 rows."""
    try:
        # run load data script
        dir = str(here() / "tests/data")
        load_main(data_dir=dir, db=SQLALCHEMY_DATABASE_URL)

        response = client.get(f"http://localhost:8000/weather/?limit={limit}&offset={offset}")
        assert response.status_code == 200
        assert len(response.json()) == count
        assert response.json()[0]["id"] == first_id
    finally:
        remove_files(dir)


def test_summary__all(
    client: Generator[TestClient, Any, None], create_files: None, summary__all
) -> None:
    """Test returning all."""
    try:
        # run load data script
        dir = str(here() / "tests/data")
        load_main(data_dir=dir, db=SQLALCHEMY_DATABASE_URL)
        summarize_stations(engine=engine)

        # run summarize script
        response = client.get("http://localhost:8000/weather/summary")
        assert response.status_code == 200
        assert response.json() == summary__all
    finally:
        remove_files(dir)


def test_summary__station(
    client: Generator[TestClient, Any, None], create_files: None, summary__station
) -> None:
    """Test returning a station."""
    try:
        # run load data script
        dir = str(here() / "tests/data")
        load_main(data_dir=dir, db=SQLALCHEMY_DATABASE_URL)
        summarize_stations(engine=engine)

        # run summarize script
        response = client.get("http://localhost:8000/weather/summary?station_id=USC00123456")
        assert response.status_code == 200
        assert response.json() == summary__station
    finally:
        remove_files(dir)


def test_summary__year(
    client: Generator[TestClient, Any, None], create_files: None, summary__year
) -> None:
    """Test returning a year."""
    try:
        # run load data script and summarize
        dir = str(here() / "tests/data")
        load_main(data_dir=dir, db=SQLALCHEMY_DATABASE_URL)
        summarize_stations(engine=engine)

        # run summarize script
        response = client.get("http://localhost:8000/weather/summary?year=1985")
        print(response.json())
        assert response.status_code == 200
        assert response.json() == summary__year

    finally:
        remove_files(dir)


@pytest.mark.parametrize(
    "limit,offset,count,first_id",
    [
        pytest.param(20, 0, 20, 1, id="pagination page 1"),
        pytest.param(20, 20, 1, 21, id="pagination page 2"),
    ],
)
def test_summary__pagination(
    client: Generator[TestClient, Any, None],
    create_files__many: None,
    limit: int,
    offset: int,
    count: int,
    first_id: int,
) -> None:
    """Test pagination in summary endpoint. Upload 21 stations and 42 rows."""
    try:
        dir = str(here() / "tests/data")
        load_main(data_dir=dir, db=SQLALCHEMY_DATABASE_URL)
        summarize_stations(engine=engine)

        # run summarize script
        response = client.get(
            f"http://localhost:8000/weather/summary?limit={limit}&offset={offset}"
        )
        assert response.status_code == 200
        assert len(response.json()) == count
        assert response.json()[0]["id"] == first_id
    finally:
        remove_files(dir)
