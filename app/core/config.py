from pathlib import Path

from pydantic import computed_field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """API settings.

    When delpoyed, this would be modified to load from environmental variables
    """

    INSTANCE_DIR: Path = Path("./db")
    DB: str = "weather.db"

    @computed_field
    @property
    def SQLALCHEMY_DATABASE_URI(self) -> Path:
        """Generate full URI. Create directory if it doesn't exist."""
        self.INSTANCE_DIR.mkdir(exist_ok=True)
        return str(self.INSTANCE_DIR / self.DB)


settings = Settings()
