import psycopg2

from common.logger import get_logger

logger = get_logger("db.creator")


class Database:
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
        self.conn: psycopg2.extensions.connection | None = None

    def ensure_database(self) -> None:
        conn = psycopg2.connect(
            dbname="postgres",
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    (self.dbname,),
                )
                if cur.fetchone() is None:
                    cur.execute(f"CREATE DATABASE {self.dbname}")
                    logger.info("Database %s created", self.dbname)
        finally:
            conn.close()

    def connect_to_db(self) -> None:
        self.ensure_database()
        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )
        self._init_schema()

    def close_connection(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def _init_schema(self) -> None:
        if self.conn is None:
            raise RuntimeError("Database connection is not initialized")

        timescaledb_available = False
        with self.conn.cursor() as cur:
            try:
                cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
                timescaledb_available = True
            except Exception as exc:
                logger.warning("TimescaleDB extension not available: %s", exc)
                self.conn.rollback()

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id                BIGSERIAL PRIMARY KEY,
                    email             TEXT UNIQUE,
                    telegram_username TEXT UNIQUE
                )
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS products (
                    id              BIGSERIAL PRIMARY KEY,
                    internal_id     BIGINT NOT NULL,
                    name            TEXT NOT NULL,
                    marketplace     TEXT NOT NULL,
                    brand           TEXT,
                    brand_id        INTEGER,
                    image_url       TEXT,
                    size            TEXT,
                    quantity        INTEGER,
                    pics            INTEGER,
                    last_scraped_at TIMESTAMPTZ
                )
                """
            )

            cur.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conname = 'products_marketplace_id_key'
                    ) THEN
                        ALTER TABLE products
                        ADD CONSTRAINT products_marketplace_id_key
                        UNIQUE (marketplace, internal_id, size);
                    END IF;
                END $$;
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS subscriptions (
                    id               BIGSERIAL PRIMARY KEY,
                    user_id          BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    product_id       BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
                    threshold_price  NUMERIC(12,2) NOT NULL,
                    last_notified_at TIMESTAMPTZ
                )
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS predictions (
                    id               BIGSERIAL PRIMARY KEY,
                    product_id       BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
                    price_prediction NUMERIC(12,2) NOT NULL,
                    predicted_at     TIMESTAMPTZ NOT NULL,
                    target_date      DATE NOT NULL
                )
                """
            )

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_predictions_product_target
                ON predictions (product_id, target_date DESC)
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS prices (
                    product_id  BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
                    timestamp   TIMESTAMPTZ NOT NULL,
                    price       NUMERIC(12,2) NOT NULL,
                    PRIMARY KEY (product_id, timestamp)
                )
                """
            )

            if timescaledb_available:
                try:
                    cur.execute(
                        """
                        SELECT create_hypertable(
                            'prices',
                            'timestamp',
                            if_not_exists => TRUE
                        )
                        """
                    )
                except Exception as exc:
                    logger.warning("Could not create hypertable: %s", exc)

        self.conn.commit()
