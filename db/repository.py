import asyncpg
from typing import Optional, List, Dict, Any
from datetime import datetime
from logger import get_logger

logger = get_logger('db.repository')


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
        try:
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
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")
    
    async def get_or_create_product(
        self,
        internal_id: int,
        marketplace: str,
        name: str,
        brand: Optional[str] = None,
        brand_id: Optional[int] = None,
        image_url: Optional[str] = None,
        size: Optional[str] = None,
        quantity: Optional[int] = None,
        pics: Optional[int] = None,
    ) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id FROM products
                WHERE internal_id = $1 AND marketplace = $2 AND size = $3
                """,
                internal_id, marketplace, size
            )
            
            if row:
                product_id = row['id']
                await conn.execute(
                    """
                    UPDATE products
                    SET name = $1,
                        brand = $2,
                        brand_id = $3,
                        image_url = $4,
                        quantity = $5,
                        pics = $6,
                        last_scraped_at = $7
                    WHERE id = $8
                    """,
                    name, brand, brand_id, image_url, quantity, pics,
                    datetime.now(), product_id
                )
                logger.debug(f"Updated product {product_id} ({marketplace}:{internal_id}:{size})")
            else:
                row = await conn.fetchrow(
                    """
                    INSERT INTO products (
                        internal_id, name, marketplace, brand, brand_id,
                        image_url, size, quantity, pics, last_scraped_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    RETURNING id
                    """,
                    internal_id, name, marketplace, brand, brand_id,
                    image_url, size, quantity, pics, datetime.now()
                )
                product_id = row['id']
                logger.info(f"Created new product {product_id} ({marketplace}:{internal_id}:{size})")
            
            return product_id
    
    async def insert_price(
        self,
        product_id: int,
        price: float,
        timestamp: Optional[datetime] = None
    ) -> None:
        if timestamp is None:
            timestamp = datetime.now()
        
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO prices (product_id, timestamp, price)
                VALUES ($1, $2, $3)
                ON CONFLICT (product_id, timestamp) DO UPDATE
                SET price = EXCLUDED.price
                """,
                product_id, timestamp, price
            )
            logger.debug(f"Inserted price {price} for product {product_id} at {timestamp}")
    
    async def save_parsed_product(self, product_data: Dict[str, Any]) -> int:
        product_id = await self.get_or_create_product(
            internal_id=product_data['internal_id'],
            marketplace=product_data['marketplace'],
            name=product_data['name'],
            brand=product_data.get('brand'),
            brand_id=product_data.get('brand_id'),
            image_url=product_data.get('image_url'),
            size=product_data.get('size'),
            quantity=product_data.get('quantity'),
            pics=product_data.get('pics'),
        )
        
        price = product_data.get('price', 0.0)
        if price > 0:
            await self.insert_price(product_id, price)
        
        return product_id
    
    async def save_parsed_products(self, products: List[Dict[str, Any]]) -> List[int]:
        product_ids = []
        for product_data in products:
            try:
                product_id = await self.save_parsed_product(product_data)
                product_ids.append(product_id)
            except Exception as e:
                logger.error(f"Failed to save product {product_data.get('internal_id')}: {e}")
        
        logger.info(f"Saved {len(product_ids)} products to database")
        return product_ids
    
    async def get_product_price_history(
        self,
        product_id: int,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            query = """
                SELECT timestamp, price
                FROM prices
                WHERE product_id = $1
                ORDER BY timestamp DESC
            """
            if limit:
                query += f" LIMIT {limit}"
            
            rows = await conn.fetch(query, product_id)
            return [{'timestamp': row['timestamp'], 'price': float(row['price'])} for row in rows]
    
    async def get_product_by_internal_id(
        self,
        internal_id: int,
        marketplace: str,
        size: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            if size:
                row = await conn.fetchrow(
                    """
                    SELECT * FROM products
                    WHERE internal_id = $1 AND marketplace = $2 AND size = $3
                    """,
                    internal_id, marketplace, size
                )
            else:
                row = await conn.fetchrow(
                    """
                    SELECT * FROM products
                    WHERE internal_id = $1 AND marketplace = $2
                    LIMIT 1
                    """,
                    internal_id, marketplace
                )
            
            if row:
                return dict(row)
            return None
    
    async def get_latest_price(self, product_id: int) -> Optional[float]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT price FROM prices
                WHERE product_id = $1
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                product_id
            )
            if row:
                return float(row['price'])
            return None
