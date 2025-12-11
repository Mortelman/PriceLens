import aiohttp
import requests
from abc import ABC, abstractmethod
from typing import Dict

WB_PRODUCT_URL = "https://card.wb.ru/cards/v4/detail?appType=1&curr=rub&dest=1259570207&spp=30&hide_vflags=4294967296&hide_dtype=9%3B11&ab_testing=false&lang=ru&nm=" # + article

class BaseScraper(ABC):
    marketplace: str

    @abstractmethod
    async def fetch_product(self, session: aiohttp.ClientSession, url: str) -> Dict:
        """
        Получает данные о товаре по URL:
        {
            'marketplace': str,
            'id': str | None,
            'name': str,
            'brand': str,
            'brand_id': int
            'price_basic': int | float,
            'price': int | float,
            'total_quantity': int,
            'url': str | None,
            'image_url': str | None,
        }
        """
        raise NotImplementedError


class WBScraper(BaseScraper):
    marketplace = 'wildberries'

    def __init__(self) -> None:
        self.api_url = WB_PRODUCT_URL
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': 'https://www.wildberries.ru',
            'Referer': 'https://www.wildberries.ru/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site',
        }

    async def fetch_product(self, session: aiohttp.ClientSession, article: int) -> Dict:
        url = self.api_url + str(article)
        async with session.get(url=url, headers=self.headers) as response:
            status = response.status
            text = await response.text()

            if status == 200:
                data = await response.json()
                return await self.parse_product(data)

            snippet = text.replace('\n', ' ')
            raise RuntimeError(f'Unexpected status {status} for {article}: {snippet}')
        
    async def parse_product(self, data: Dict):
        product = data['products'][0]
        id = product['id']
        name = product['name']
        brand = product['brand']
        brand_id = product['brandId']
        price_basic = product['sizes'][0]['price']['basic'] / 100
        price = product['sizes'][0]['price']['product'] / 100
        total_quantity = product['totalQuantity']

        return {
            'marketplace': self.marketplace,
            'id': id,
            'name': name,
            'brand': brand,
            'brand_id': brand_id,
            'price_basic': price_basic,
            'price': price,
            'total_quantity': total_quantity,
        }

# if __name__ == '__main__':
#     async def main():
#         article = 288392979
#         async with aiohttp.ClientSession() as session:
#             scraper = WBScraper()
#             try:
#                 result = await scraper.fetch_product(session, article)
#                 print(result)
#             except Exception as e:
#                 print('Ошибка:', e)

#     asyncio.run(main())

def get_vol_and_part(article: int) -> tuple[str, str]:
    if article >= 100_000_000:
        return str(article)[:4], str(article)[:6]
    else:
        return str(article)[:3], str(article)[:5]

def get_product_image(article: int):
    vol, part = get_vol_and_part(article)
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://www.wildberries.ru",
        "Referer": "https://www.wildberries.ru/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
    }
    for number in range(1, 100):
        if number < 10:
            basket = f"0{number}"
        else:
            basket = str(number)

        api_url = f"https://basket-{basket}.wbbasket.ru/vol{vol}/part{part}/{article}/images/big/1.webp"
        response = requests.get(api_url, headers=headers)
        if response.status_code == 200:
            content_type = response.headers.get("Content-Type", "")
            if content_type.startswith("image/"):
                print(f"✅ Изображение валидно: {content_type}")
                return True
            else:
                print(f"❌ Контент не является изображением: {content_type}")
        else:
            print(f"Failed: {response.status_code}")
    return False

# article = 251898297
article = 85999881
result = get_product_image(article)
print(result)
