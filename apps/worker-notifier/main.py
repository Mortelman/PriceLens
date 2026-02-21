import asyncio

from common.config import Config
from common.logger import get_logger

logger = get_logger("apps.worker_notifier")


async def run_notification_loop() -> None:
    logger.info("Notifier worker started")

    while True:
        # TODO:
        # - consume price.updated events
        # - compare with subscriptions.threshold_price
        # - send telegram/email notification
        await asyncio.sleep(10)
        logger.info("Notifier heartbeat")


def main() -> None:
    asyncio.run(run_notification_loop())


if __name__ == "__main__":
    main()
