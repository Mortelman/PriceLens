from common.config import Config
from common.logger import get_logger
from db.creator import Database

logger = get_logger("apps.db_creator")


def init_db_schema() -> None:
    db = Database(
        dbname=Config.db_name,
        user=Config.db_user,
        password=Config.db_password,
        host=Config.db_host,
        port=Config.db_port,
    )

    try:
        logger.info("Initializing database schema")
        db.connect_to_db()
        logger.info("Database schema initialized")
    finally:
        db.close_connection()
