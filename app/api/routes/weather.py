"""Weather API routes."""

from datetime import datetime

from fastapi import APIRouter, Query
from sqlalchemy import text

from app.api.deps import SessionDep

router = APIRouter(prefix="/weather", tags=["weather"])


@router.get("/")
async def weather_router(
    session: SessionDep,
    station_id: str | None = Query(
        default=None,
        description="Station ID to select",
        openapi_examples={
            "example": {"summary": "Station", "value": "USC00110072"},
            "example2": {"summary": "Station 2", "value": "USC00111436"},
            "null": {"summary": "Null", "value": None},
        },
    ),
    date: datetime | None = Query(
        default=None,
        description="Date of records",
        openapi_examples={
            "example": {"summary": "1/1/1985", "value": "1985-01-01"},
            "example2": {"summary": "10/20/1985", "value": "1985-10-20"},
            "null": {"summary": "null", "value": None},
        },
    ),
    limit: int = Query(default=20, description="Records return limit"),
    offset: int = Query(default=0, description="Records returned offset from start"),
):
    """_summary_

    Parameters
    ----------
    session : SessionDep
        _description_
    station_id : _type_, optional
        _description_, by default Query( default=None, description="Station ID to select", openapi_examples={ "example": {"summary": "Station", "value": "USC00110072"}, "example2": {"summary": "Station 2", "value": "USC00111436"}, "null": {"summary": "Null", "value": None}, }, )
    date : _type_, optional
        _description_, by default Query( default=None, description="Date of records", openapi_examples={ "example": {"summary": "1/1/1985", "value": "1985-01-01"}, "example2": {"summary": "10/20/1985", "value": "1985-10-20"}, "null": {"summary": "null", "value": None}, }, )
    limit : int, optional
        _description_, by default Query(default=20, description="Records return limit")
    offset : int, optional
        _description_, by default Query(default=0, description="Records returned offset from start")

    Returns
    -------
    _type_
        _description_

    """
    if station_id and date:
        t = text(
            f"SELECT * FROM station_data WHERE station_id = '{station_id}' and date = date('{date}') limit {limit} offset {offset};"
        )
    elif station_id and not date:
        t = text(
            f"SELECT * FROM station_data WHERE station_id = '{station_id}' limit {limit} offset {offset};"
        )
    elif not station_id and date:
        t = text(
            f"SELECT * FROM station_data WHERE date = date('{date}') limit {limit} offset {offset};"
        )
    else:
        t = text(f"SELECT * FROM station_data limit {limit} offset {offset};")

    result = session.execute(t)
    return [row._asdict() for row in result]


@router.get("/summary")
async def weather_stats_router(
    session: SessionDep,
    station_id: str | None = Query(
        default=None,
        description="Station ID to select",
        openapi_examples={
            "example": {"summary": "Station 1", "value": "USC00110072"},
            "example2": {"summary": "Station 2", "value": "USC00111436"},
            "null": {"summary": "Null", "value": None},
        },
    ),
    year: int | None = Query(
        default=None,
        description="Year to select",
        openapi_examples={
            "example": {"summary": "1985", "value": 1985},
            "example2": {"summary": "1985", "value": 2000},
            "null": {"summary": "null", "value": None},
        },
    ),
    limit: int = Query(default=20),
    offset: int = Query(default=0),
):
    """_summary_

    Parameters
    ----------
    session : SessionDep
        _description_
    station_id : _type_, optional
        _description_, by default Query( default=None, description="Station ID to select", openapi_examples={ "example": {"summary": "Station 1", "value": "USC00110072"}, "example2": {"summary": "Station 2", "value": "USC00111436"}, "null": {"summary": "Null", "value": None}, }, )
    year : _type_, optional
        _description_, by default Query( default=None, description="Year to select", openapi_examples={ "example": {"summary": "1985", "value": 1985}, "example2": {"summary": "1985", "value": 2000}, "null": {"summary": "null", "value": None}, }, )
    limit : int, optional
        _description_, by default Query(default=20)
    offset : int, optional
        _description_, by default Query(default=0)

    Returns
    -------
    _type_
        _description_

    """
    if station_id and year:
        t = text(
            f"SELECT * FROM station_summary WHERE station_id = '{station_id}' and year = '{year}' limit {limit} offset {offset};"
        )
    elif station_id and not year:
        t = text(
            f"SELECT * FROM station_summary WHERE station_id = '{station_id}' limit {limit} offset {offset};"
        )
    elif not station_id and year:
        t = text(
            f"SELECT * FROM station_summary WHERE year = '{year}' limit {limit} offset {offset};"
        )
    else:
        t = text(f"SELECT * FROM station_summary limit {limit} offset {offset};")

    result = session.execute(t)
    return [row._asdict() for row in result]
