import os

class Config:
    db_name = os.getenv("DB_NAME", "pricelens")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "postgres")
    db_host = os.getenv("DB_HOST", "postgres")
    db_port = int(os.getenv("DB_PORT", "5432"))

    rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    rabbitmq_port = int(os.getenv("RABBITMQ_PORT", "5672"))
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))

    api_host = os.getenv("API_HOST", "0.0.0.0")
    api_port = int(os.getenv("API_PORT", "8080"))

    scrape_interval_sec = int(os.getenv("SCRAPE_INTERVAL_SEC", "1800"))
    ml_interval_sec = int(os.getenv("ML_INTERVAL_SEC", "3600"))
    ml_batch_size = int(os.getenv("ML_BATCH_SIZE", "100"))
