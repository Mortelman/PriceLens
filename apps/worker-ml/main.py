import asyncio
from datetime import date, datetime, timedelta, timezone

from common.config import Config
from common.logger import full_log, get_logger
from db.database import AsyncDatabase

logger = get_logger("apps.worker_ml")


async def build_predictions(db: AsyncDatabase, batch_size: int) -> int:
    if db.pool is None:
        raise RuntimeError("Database pool is not initialized")

    now = datetime.now(datetime.timezone.utc)
    target_date = date.today() + timedelta(days=1)

    return 0


async def run_ml_loop() -> None:
    db = AsyncDatabase(
        dbname=Config.db_name,
        user=Config.db_user,
        password=Config.db_password,
        host=Config.db_host,
        port=Config.db_port,
    )

    await db.connect()

    try:
        while True:
            logger.info("Starting ML prediction loop")
            await asyncio.sleep(Config.ml_interval_sec)
    except asyncio.CancelledError:
        raise
    except Exception:
        full_log(logger=logger, where="/run_ml_loop")
        await asyncio.sleep(30)
        await run_ml_loop()
    finally:
        await db.close()


def main() -> None:
    asyncio.run(run_ml_loop())


if __name__ == "__main__":
    main()
