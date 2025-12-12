import psycopg2


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
        """
        Подключается к служебной БД 'postgres' и создает нашу БД, если ее еще нет.
        """
        conn = psycopg2.connect(
            dbname="postgres",
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    (self.dbname,),
                )

                if cur.fetchone() is None:
                    cur.execute(f'CREATE DATABASE {self.dbname}')
        finally:
            conn.close()

    def connect_to_db(self) -> None:
        """
        Гарантирует существование БД, подключается и создает таблицы при первом запуске.
        """
        self.ensure_database()

        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

        self._init_schema()

    def close_connection(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def _init_schema(self) -> None:
        with self.conn.cursor() as cur:
            # TimescaleDB extension
            cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")

            # Таблица users
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id                BIGSERIAL PRIMARY KEY,
                    email             TEXT UNIQUE,
                    telegram_username TEXT UNIQUE
                );
                """
            )

            # Таблица products
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
                        FROM  pg_constraint
                        WHERE conname = 'products_marketplace_id_key'
                    ) THEN
                        ALTER TABLE products
                        ADD CONSTRAINT products_marketplace_id_key
                        UNIQUE (marketplace, id);
                    END IF
                END
                $$
                """
            )

            # Таблица subscriptions
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

            # Таблица predictions
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

            # Индекс для быстрых запросов "последний прогноз по товару"
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_predictions_product_target
                ON predictions (product_id, target_date DESC)
                """
            )

            # Таблица prices (hypertable)
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

            # Превращаем prices в hypertable
            cur.execute(
                """
                SELECT create_hypertable(
                    'prices',
                    'timestamp',
                    if_not_exists => TRUE
                )
                """
            )

        self.conn.commit()
