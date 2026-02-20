#!/usr/bin/env python3

import asyncio
from datetime import datetime, timedelta
from db.repository import AsyncDatabase
from logger import get_logger

logger = get_logger('check_products')


async def get_database_stats(db: AsyncDatabase) -> dict:
    async with db.pool.acquire() as conn:
        total_products = await conn.fetchval("SELECT COUNT(*) FROM products")
        
        marketplace_stats = await conn.fetch(
            """
            SELECT marketplace, COUNT(*) as count
            FROM products
            GROUP BY marketplace
            ORDER BY count DESC
            """
        )
        
        total_prices = await conn.fetchval("SELECT COUNT(*) FROM prices")
        
        products_with_prices = await conn.fetchval(
            """
            SELECT COUNT(DISTINCT product_id) FROM prices
            """
        )
        
        return {
            'total_products': total_products,
            'marketplace_stats': [dict(row) for row in marketplace_stats],
            'total_prices': total_prices,
            'products_with_prices': products_with_prices
        }


async def get_recent_products(db: AsyncDatabase, limit: int = 10) -> list:
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT 
                id,
                internal_id,
                name,
                marketplace,
                brand,
                size,
                quantity,
                last_scraped_at
            FROM products
            ORDER BY last_scraped_at DESC
            LIMIT $1
            """,
            limit
        )
        return [dict(row) for row in rows]


async def get_products_with_price_history(db: AsyncDatabase, limit: int = 5) -> list:
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT 
                p.id,
                p.name,
                p.brand,
                p.marketplace,
                COUNT(pr.timestamp) as price_count,
                MIN(pr.price) as min_price,
                MAX(pr.price) as max_price,
                MAX(pr.timestamp) as last_price_update
            FROM products p
            JOIN prices pr ON p.id = pr.product_id
            GROUP BY p.id, p.name, p.brand, p.marketplace
            HAVING COUNT(pr.timestamp) > 0
            ORDER BY price_count DESC, last_price_update DESC
            LIMIT $1
            """,
            limit
        )
        return [dict(row) for row in rows]


async def get_products_updated_recently(db: AsyncDatabase, hours: int = 24) -> list:
    async with db.pool.acquire() as conn:
        cutoff_time = datetime.now() - timedelta(hours=hours)
        rows = await conn.fetch(
            """
            SELECT 
                id,
                internal_id,
                name,
                marketplace,
                brand,
                size,
                last_scraped_at
            FROM products
            WHERE last_scraped_at >= $1
            ORDER BY last_scraped_at DESC
            """,
            cutoff_time
        )
        return [dict(row) for row in rows]


def print_separator(char='=', length=70):
    print(char * length)


def format_datetime(dt):
    if dt is None:
        return "N/A"
    return dt.strftime('%Y-%m-%d %H:%M:%S')


async def main():
    print_separator()
    print("ПРОВЕРКА БАЗЫ ДАННЫХ PRICELENS")
    print_separator()
    
    db = AsyncDatabase(
        dbname="pricelens",
        user="postgres",
        password="postgres",
        host="localhost",
        port=5432
    )
    
    try:
        await db.connect()
        
        print("ОБЩАЯ СТАТИСТИКА")
        print_separator('-')
        stats = await get_database_stats(db)
        
        print(f"Всего товаров: {stats['total_products']}")
        print(f"Товаров с ценами: {stats['products_with_prices']}")
        print(f"Всего записей цен: {stats['total_prices']}")
        print()
        
        if stats['marketplace_stats']:
            print("По маркетплейсам:")
            for mp_stat in stats['marketplace_stats']:
                print(f"  • {mp_stat['marketplace']}: {mp_stat['count']} товаров")
        print()
        
        print("ПОСЛЕДНИЕ ТОВАРЫ (ТОП 10)")
        print_separator('-')
        recent = await get_recent_products(db, limit=10)
        
        if recent:
            for i, product in enumerate(recent, 1):
                print(f"{i}. {product['name']}")
                print(f"   ID: {product['id']} | Маркетплейс: {product['marketplace']}")
                print(f"   Бренд: {product['brand']} | Размер: {product['size']}")
                print(f"   Количество: {product['quantity']}")
                print(f"   Обновлено: {format_datetime(product['last_scraped_at'])}")
                print()
        else:
            print("Товары не найдены")
            print()
        
        print("ТОВАРЫ С ИСТОРИЕЙ ЦЕН (ТОП-5)")
        print_separator('-')
        with_history = await get_products_with_price_history(db, limit=5)
        
        if with_history:
            for i, product in enumerate(with_history, 1):
                print(f"{i}. {product['name']} ({product['brand']})")
                print(f"   Маркетплейс: {product['marketplace']}")
                print(f"   Записей цен: {product['price_count']}")
                print(f"   Диапазон цен: {float(product['min_price']):.2f} - {float(product['max_price']):.2f} ₽")
                print(f"   Последнее обновление: {format_datetime(product['last_price_update'])}")
                print()
        else:
            print("Товары с историей цен не найдены")
            print()
        
        print("ОБНОВЛЕНО ЗА ПОСЛЕДНИЕ 24 ЧАСА")
        print_separator('-')
        recent_updates = await get_products_updated_recently(db, hours=24)
        
        if recent_updates:
            print(f"Найдено товаров: {len(recent_updates)}")
            print()
            for i, product in enumerate(recent_updates[:5], 1):
                print(f"{i}. {product['name']} ({product['size']})")
                print(f"   Маркетплейс: {product['marketplace']}")
                print(f"   Обновлено: {format_datetime(product['last_scraped_at'])}")
                print()
            
            if len(recent_updates) > 5:
                print(f"... и еще {len(recent_updates) - 5} товаров")
                print()
        else:
            print("Нет товаров, обновленных за последние 24 часа")
            print()
        
        print_separator()
        print("ПРОВЕРКА ЗАВЕРШЕНА")
        print_separator()
        print()
        
    except Exception as e:
        logger.error(f"Ошибка при проверке БД: {e}")
        print(f"\nОШИБКА: {e}\n")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())