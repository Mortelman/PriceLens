import asyncio

from common.config import Config
from common.logger import get_logger

logger = get_logger("apps.scheduler")


async def run_scheduler_loop() -> None:
    logger.info("Scheduler started")

    while True:
        # TODO:
        # - pick active subscriptions/products
        # - publish scrape.requested events to RabbitMQ
        await asyncio.sleep(10)
        logger.info("Scheduler heartbeat")


def main() -> None:
    asyncio.run(run_scheduler_loop())


if __name__ == "__main__":
    main()
