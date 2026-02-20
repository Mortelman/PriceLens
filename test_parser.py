import aiohttp
import requests
import asyncio
from abc import ABC, abstractmethod
from typing import Dict
import fake_useragent
from typing import Dict, Optional
from curl_cffi import requests as curl_requests

WB_PRODUCT_URL = "https://card.wb.ru/cards/v4/detail?appType=1&curr=rub&dest=1259570207&spp=30&hide_vflags=4294967296&hide_dtype=9%3B11&ab_testing=false&lang=ru&nm=" # + article
ALI_PRODUCT_URL = "https://aliexpress.ru/aer-jsonapi/v1/bx/pdp/web/productData?productId=1005005756534649&sourceId=0&sku_id=12000034243212878"

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
            'User-Agent': fake_useragent.UserAgent().random,
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


class AliScraper(BaseScraper):
    marketplace = 'aliexpress'

    def __init__(self, cookies: Optional[str] = None) -> None:
        self.api_url = "https://aliexpress.ru/aer-jsonapi/v1/bx/pdp/web/productData"
        self.cookies = cookies

    def _get_headers(self, product_id: str) -> dict:
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'accept': '*/*',
            'accept-language': 'ru-RU,ru;q=0.9',
            'bx-v': '2.5.36',
            'referer': f'https://aliexpress.ru/item/{product_id}.html',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
        }
        if self.cookies:
            headers['cookie'] = self.cookies
        return headers

    async def fetch_product(self, product_id: str, sku_id: Optional[str] = None) -> Dict:
        params = {'productId': product_id, 'sourceId': '0', 'locale': 'ru_RU', 'currency': 'RUB'}
        if sku_id:
            params['sku_id'] = sku_id

        response = await asyncio.to_thread(
            curl_requests.get,
            self.api_url,
            headers=self._get_headers(product_id),
            params=params,
            timeout=15,
            impersonate='chrome120'
        )

        if response.status_code != 200:
            raise RuntimeError(f'Unexpected status {response.status_code}: {response.text[:200]}')

        data = response.json()
        
        # Проверка на ошибку внутри JSON
        if data.get('ret') and any('FAIL' in str(r) for r in data.get('ret', [])):
            raise RuntimeError(f'API Error: {data.get("ret")}')

        return self._parse_product(data, product_id)

    def _parse_product(self, data: Dict, product_id: str) -> Dict:
        # ⭐ данные лежат в data['data']
        product = data.get('data', {})
        
        price_info = product.get('price', {})
        price = price_info.get('minActivityAmount', {}).get('value', 0)
        price_basic = price_info.get('maxAmount', {}).get('value', price)

        product_info = product.get('productInfo', {})
        brand = product_info.get('brand', 'Нет')
        
        # Количество (суммируем из всех SKU)
        total_quantity = 0
        sku_info = product.get('skuInfo', {})
        price_list = sku_info.get('priceList', [])
        for sku in price_list:
            total_quantity += sku.get('availQuantity', 0)

        if total_quantity == 0:
            quantity_info = product.get('quantity', {})
            total_count = quantity_info.get('totalCount', '0')
            try:
                total_quantity = int(total_count)
            except:
                total_quantity = 0
        
        gallery = product.get('gallery', [])
        image_url = gallery[0].get('imageUrl') if gallery else None
        
        rating_info = product.get('rating', {})
        rating = rating_info.get('middle', 0)
        
        trade_info = product.get('tradeInfo', {})
        reviews = trade_info.get('tradeCount', product.get('reviews', '0'))
        
        return {
            'marketplace': self.marketplace,
            'id': int(product_id),
            'name': product.get('name', 'Unknown'),
            'brand': brand if brand != 'Нет' else '',
            'brand_id': 0,
            'price_basic': float(price_basic),
            'price': float(price),
            'total_quantity': total_quantity,
            'rating': float(rating) if rating else 0,
            'reviews': int(reviews) if isinstance(reviews, str) and reviews.isdigit() else 0,
            'image_url': image_url,
        }


if __name__ == '__main__':
    async def main():
        async with aiohttp.ClientSession() as session:
            scraper = AliScraper()
            try:
                # result = await scraper.fetch_product(session, 288392979)
                result = await scraper.fetch_product('1005010437284537')
                print(result)
            except Exception as e:
                print('Ошибка:', e)

asyncio.run(main())

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
#article = 85999881
#result = get_product_image(article)
#print(result)


