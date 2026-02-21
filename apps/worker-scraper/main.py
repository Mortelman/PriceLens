import asyncio

import aiohttp

from common.config import Config
from common.logger import full_log, get_logger
from db.database import AsyncDatabase
from parsers.wildberries import WBScraper

logger = get_logger("apps.worker_scraper")


async def run_scrape_loop() -> None:
    db = AsyncDatabase(
        dbname=Config.db_name,
        user=Config.db_user,
        password=Config.db_password,
        host=Config.db_host,
        port=Config.db_port,
    )

    await db.connect()
    scraper = WBScraper(db=db)

    try:
        while True:
            logger.info("Starting scrape cycle")
            await asyncio.sleep(Config.scrape_interval_sec)
    except asyncio.CancelledError:
        raise
    except Exception:
        full_log(logger=logger, where="/run_scrape_loop")
        await asyncio.sleep(30)
        await run_scrape_loop()
    finally:
        await db.close()


def main() -> None:
    asyncio.run(run_scrape_loop())


if __name__ == "__main__":
    main()
