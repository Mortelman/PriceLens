#!/usr/bin/env python3

import sys
from db.database import Database
from logger import get_logger

logger = get_logger('init_db')


def main():
    print("ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ PRICELENS\n")
    
    db = Database(
        dbname="pricelens",
        user="postgres",
        password="postgres",
        host="localhost",
        port=5432
    )
    
    try:
        db.ensure_database()
        print("База данных готова")
        
        db.connect_to_db()
        print("Подключение к БД установлено")
        
    except Exception as e:
        logger.error(f"Ошибка при инициализации БД: {e}")
        print(f"\nОШИБКА: {e}")
        sys.exit(1)
    finally:
        db.close_connection()


if __name__ == "__main__":
    main()