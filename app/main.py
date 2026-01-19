import logging
from pathlib import Path

import uvicorn
from fastapi import FastAPI, status
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.api.main import api_router

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(asctime)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Create a logger instance
main_logger = logging.getLogger(__name__)

# Tags for OpenAPI
tags_metadata = [
    {
        "name": "Weather",
        "description": "API for weather stations data",
    },
]

app = FastAPI(
    title="Weather Station API",
    description="API for accessing weather station data",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=tags_metadata,
)


class HealthCheck(BaseModel):
    """Response model to validate and return when performing a health check."""

    status: str = "OK"


app.include_router(api_router)


# Health checks
@app.get(
    "/health",
    tags=["Health"],
    summary="Perform a Health Check",
    response_description="Return HTTP Status Code 200 (OK)",
    status_code=status.HTTP_200_OK,
    response_model=HealthCheck,
)
def get_health() -> HealthCheck:
    """Returns a HealthCheck for the server."""
    return HealthCheck(status="OK")


# Mount static files for mkdocs at the root
# This tells FastAPI to serve the static documentation files at the '/' URL
# We only mount the directory if it exists (only after 'mkdocs build' has run)
# This prevents the app from crashing during tests or local development.
docs_dir = Path("static/docs")
if docs_dir.is_dir():
    app.mount("/", StaticFiles(directory=docs_dir, html=True), name="static")
else:
    print("INFO: Documentation directory 'static/docs' not found. Docs will not be served.")

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
