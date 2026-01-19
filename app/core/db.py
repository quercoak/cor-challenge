"""Database creation function. Creates from all models."""

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base

from app.core.config import settings

Base = declarative_base()
engine = create_engine(f"sqlite:///{settings.SQLALCHEMY_DATABASE_URI}")


def init_db():
    """Initializes database with all SQLAlchemy models."""
    import app.models  # noqa

    Base.metadata.create_all(engine)
