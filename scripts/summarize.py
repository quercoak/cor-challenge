"""Script to summarize weather station data and load to table."""

from datetime import UTC, datetime

from sqlalchemy import Integer, func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_upsert
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Session

from app.core.db import engine as base_engine
from app.models import StationData, StationSummary


def summarize_stations(engine: Engine) -> int:
    """Summarize weather station data annually.

    Calculate the maximum temperature, minimum temperature, and cumulative precipitation for each year in record
    Load to station_summary
    Nulls are by default skipped

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine

    Returns
    -------
    int
        Count of rows touched

    """
    # Group by year and station to get average max temp, average min temp, and cumulative precipitation
    stmt = select(
        StationData.station_id,
        func.strftime("%Y", StationData.date).cast(Integer).label("year"),
        func.round(func.avg(StationData.max_temp), 1).label("avg_max_temp"),
        func.round(func.avg(StationData.min_temp), 1).label("avg_min_temp"),
        func.sum(StationData.total_precip).label("cumulative_precip"),
    ).group_by(StationData.station_id, "year")

    # Upsert to new table with select
    upsert_stmt = sqlite_upsert(StationSummary).from_select(
        ["station_id", "year", "avg_max_temp", "avg_min_temp", "cumulative_precip"], stmt
    )

    # Do not duplicate if year and station exist; on conflict update the summaries
    upsert_stmt = upsert_stmt.on_conflict_do_update(
        index_elements=[StationSummary.station_id, StationSummary.year],
        set_={
            "avg_max_temp": upsert_stmt.excluded.avg_max_temp,
            "avg_min_temp": upsert_stmt.excluded.avg_min_temp,
            "cumulative_precip": upsert_stmt.excluded.cumulative_precip,
        },
    )

    # execute
    with Session(engine) as session:
        result = session.execute(upsert_stmt)
        session.commit()

    return result.rowcount


if __name__ == "__main__":
    print(f"Starting summarizing at {datetime.now(UTC)}")
    rowcount = summarize_stations(base_engine)
    print(f"Finished summarizing at {datetime.now(UTC)}")
    print(f"Touched {rowcount} rows.")
