from datetime import date

from pydantic import BaseModel


class WeatherReturn(BaseModel):
    """Return output model for weather route."""

    id: int
    station_id: str
    date: date
    max_temp: float | None
    min_temp: float | None
    total_precip: float | None


class SummaryReturn(BaseModel):
    """Return output model for summary route."""

    id: int
    station_id: str
    year: int
    avg_max_temp: float | None
    avg_min_temp: float | None
    cumulative_precip: float | None
