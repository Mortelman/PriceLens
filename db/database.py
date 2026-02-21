import asyncpg

from datetime import datetime
from typing import Any, Dict, List, Optional

from common.logger import get_logger

logger = get_logger("db.database")


class AsyncDatabase:
    def __init__(
        self,
        dbname: str,
        user: str,
        password: str,
        host: str = "localhost",
        port: int = 5432,
    ) -> None:
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(
            database=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            min_size=2,
            max_size=10,
        )
        logger.info(f"Connected to database '{self.dbname}' at {self.host}:{self.port}")

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")

    async def get_or_create_product(self):
        pass

    async def insert_price(self):
        pass

    async def save_parsed_product(self):
        pass

    async def save_parsed_products(self):
        pass
