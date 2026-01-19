"""SQLAlchemy models."""

from sqlalchemy import Column, Date, Float, Integer, String, UniqueConstraint

from app.core.db import Base


class StationData(Base):
    """Class for Weather Station data.

    Includes unique constraint for station / date combination
    """

    __tablename__ = "station_data"
    id = Column(Integer, primary_key=True, index=True)
    station_id = Column(String(50), nullable=False)
    date = Column(Date, nullable=False)
    max_temp = Column(Float, default=None, nullable=True)
    min_temp = Column(Float, default=None, nullable=True)
    total_precip = Column(Float(precision=1), default=None, nullable=True)

    __table_args__ = (UniqueConstraint("station_id", "date", name="station_data_constraint"),)


class StationSummary(Base):
    """Class for Weather Station summaries.

    Includes unique constraint for station / year combination.
    """

    __tablename__ = "station_summary"
    id = Column(Integer, primary_key=True, index=True)
    station_id = Column(String(50), nullable=False)
    year = Column(Integer, nullable=False)
    avg_max_temp = Column(Float, default=None, nullable=True)
    avg_min_temp = Column(Float, default=None, nullable=True)
    cumulative_precip = Column(Float, default=None, nullable=True)

    __table_args__ = (UniqueConstraint("station_id", "year", name="station_year_constraint"),)
