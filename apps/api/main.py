import uvicorn
from fastapi import FastAPI

from apps.api.db_creator import init_db_schema
from common.config import Config
from common.logger import get_logger


logger = get_logger("apps.api")


app = FastAPI(title="PriceLens API")


@app.get("/")
def root() -> dict[str, str]:
    return {
        "name": "PriceLens API",
        "health_live": "/health",
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "api"}


def main() -> None:
    uvicorn.run(
        "apps.api.main:app",
        host=Config.api_host,
        port=Config.api_port,
        reload=False,
    )


if __name__ == "__main__":
    try:
        init_db_schema()
    except Exception as e:
        logger.error(f"Failed to initialize database schema")
    main()
