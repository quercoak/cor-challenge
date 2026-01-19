"""Script to load weather station data from text files to table."""

import argparse
from collections.abc import Iterable
from datetime import UTC, datetime
from os import walk
from pathlib import Path
from typing import TypeVar

import pandas as pd
from sqlalchemy.dialects.sqlite import insert as sqlite_upsert
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Session

from app.core.db import engine
from app.models import StationData

T = TypeVar("T")


def chunk_generator(data_list: list[T], chunk_size: int) -> Iterable[list[T]]:
    """Yield chunks from a list of n size.

    Parameters
    ----------
    data_list : list
        List of objects to chunk
    chunk_size : int
        Chunk size

    Yields
    ------
    Iterator[Iterable[list[T]]]
        Iterator of chunks

    """
    for i in range(0, len(data_list), chunk_size):
        yield data_list[i : i + chunk_size]


def get_files(dir: str) -> list[Path]:
    """Get list of files from a directory.

    Parameters
    ----------
    dir : str
        Directory to check

    Returns
    -------
    list[Path]
        List of paths

    """
    return [Path(dir) / f for f in next(walk(dir), (None, None, []))[2]]  # [] if no file


def read_data(file_path: Path | str, headers: list[str]) -> list[dict]:
    """Read data from csv with specified headers.

    Parameters
    ----------
    file_path : Path | str
        CSV to read from
    headers : list[str]
        List of headers for CSV

    Returns
    -------
    list[dict]
        _description_

    """
    if file_path.name.str.split(".")[1] == "csv":
        df = pd.read_csv(file_path, sep=r"\s+", names=headers, header=None, parse_dates=["date"])
        df.insert(0, "station_id", file_path.name.split(".")[0])
        df = df.replace(-9999, pd.NA)
        return df.to_dict(orient="records")
    else:
        print(f"Skipping {file_path} as it is not a csv")
        return []


def load_data(engine: Engine, records: list[dict], chunk_size: int = 999) -> int:
    """Load data from records to table.

    Upserts based on station_id and date unique constraint. If present, update the statistics categories
    Upsert will disallow duplicates.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine
    records : list[dict]
        List of records to load
    chunk_size : int, optional
        Chunk size to upload, by default 999 for SQLite limit

    Returns
    -------
    int
        numbers of rows touched
        Note that this may not be inserted or updated due to using upsert

    """
    # start counter
    row_count = 0

    # iterate through chunks
    with Session(engine) as session:
        for chunk in chunk_generator(records, chunk_size):
            # upsert: constraint of unique station_id and date
            # if conflict, statistics values will be updated
            # will not create duplicates
            stmt = sqlite_upsert(StationData).values(chunk)
            stmt = stmt.on_conflict_do_update(
                index_elements=[StationData.station_id, StationData.date],
                set_={
                    "max_temp": stmt.excluded.max_temp,
                    "min_temp": stmt.excluded.min_temp,
                    "total_precip": stmt.excluded.total_precip,
                },
            )
            result = session.execute(stmt)
            row_count += result.rowcount
        session.commit()
    return row_count


def main(data_dir: str, chunk_size: int = 999) -> None:
    """Pipeline to load data from station CSV to table.

    Parameters
    ----------
    data_dir : str
        Directory to find station text files
    chunk_size : int, optional
        Chunk size to upload, by default 999 for SQLite limit

    """
    print(f"Starting ingestion {datetime.now(UTC)}")
    file_list = get_files(dir=data_dir)
    headers = ["date", "max_temp", "min_temp", "total_precip"]
    row_count = 0
    for f in file_list:
        records = read_data(f, headers=headers)
        result = load_data(engine, records, chunk_size)
        row_count += result
    print(f"Finished ingestion {datetime.now(UTC)}.")
    print(f"Touched {row_count} rows in upsert. All rows may not be inserts")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="A script to load weather station data to database"
    )
    parser.add_argument(
        "-d",
        "--dir",
        default="./data",
        help="Data directory to load from (default: local 'data' dir)",
    )
    parser.add_argument(
        "-c",
        "--chunk-size",
        default=999,
        help="Chunk size for loading (default: 999 for sqlite)",
    )

    args = parser.parse_args()
    main(args.dir, args.chunk_size)
