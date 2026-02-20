import asyncio
import aiohttp
from abc import ABC, abstractmethod
from limiter import RateLimiter

from logger import get_logger, full_log
logger = get_logger('test_parser.py')


class BaseScraper(ABC):
    marketplace: str
    product_url: str
    image_url: str
    headers: dict

    @abstractmethod
    async def fetch_product(self, session: aiohttp.ClientSession, **kwargs) -> dict:
        """
        Получает данные о товаре по URL/артикулу:
        {
            'internal_id': str,
            'name': str,
            'marketplace': str,
            'brand': str,
            'brand_id': int
            'price_basic': float,
            'price': float,
            'image_url': str,
            'size': str
            'quantity': int,
            'pics': int,
        }
        """
        raise NotImplementedError


class WBScraper(BaseScraper):
    marketplace = 'wildberries'
    product_url = 'https://card.wb.ru/cards/v4/detail?appType=1&curr=rub&dest=1259570207&spp=30&hide_vflags=4294967296&hide_dtype=9%3B11&ab_testing=false&lang=ru&nm=article'
    image_url = 'https://basket-article_basket.wbbasket.ru/vol_article_vol/part_article_part/article/images/big/1.webp'
    headers = {
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
    
    def __init__(self):
        self._limiter = RateLimiter(
            period=60, limit=300,
            interval=0.2, burst=20
        )
    
    @staticmethod
    def _get_vol_and_part(article: str) -> tuple[str, str]:
        if int(article) >= 100_000_000:
            return article[:4], article[:6]
        else:
            return article[:3], article[:5]
    
    @staticmethod
    def _get_url(
        base_url: str,
        article: str,
        basket: str = '',
        vol: str = '',
        part: str = ''
    ) -> str:
        url = base_url
        url = url.replace('article_basket', basket).replace('_article_vol', vol).replace('_article_part', part).replace('article', article)
        return url
    
    @staticmethod
    def _get_article(**kwargs) -> str:
        article = kwargs.get('article', None)
        if article is None:
            url = kwargs.get('url', None)
            if url is None:
                raise RuntimeError('invalid arguments')
            article = url.lstrip('https://www.wildberries.ru/catalog/').rstrip('/detail.aspx')
        return str(article)

    async def fetch_product(self, session: aiohttp.ClientSession, **kwargs) -> list[dict]:
        article = self._get_article(**kwargs)
        url = self._get_url(self.product_url, article)
        print(url)
        async with session.get(url=url, headers=self.headers) as response:
            status = response.status

            if status == 200:
                data = await response.json()
                image_url = await self._get_product_image(session, article)
                product_info = self._parse_product(data, image_url)
                return product_info

            text = await response.text()
            snippet = text.replace('\n', ' ')
            full_log(logger=logger, where="/fetch_product")
            raise RuntimeError(f'Unexpected status {status} for {article}: {snippet}')
        
    def _parse_product(self, data: dict, image_url: str) -> list[dict]:
        product = data['products'][0]
        internal_id = product['id']
        name = product['name']
        brand = product['brand']
        brand_id = product['brandId']
        pics = product['pics']

        parsed_data = []
        for size in product['sizes']:
            size_name = size['name']
            price_basic, price = 0.0, 0.0
            quantity = 0

            price_dict = size.get('price', None)
            if price_dict is not None:
                price_basic = price_dict['basic'] / 100
                price = price_dict['product'] / 100
                quantity = size['stocks'][0]['qty']

            parsed_data.append({
                'marketplace': self.marketplace,
                'internal_id': internal_id,
                'name': name,
                'brand': brand,
                'brand_id': brand_id,
                'price_basic': price_basic,
                'price': price,
                'size': size_name,
                'quantity': quantity,
                'image_url': image_url,
                'pics': pics
            })

        return parsed_data

    async def _get_product_image(self, session: aiohttp.ClientSession, article: str) -> str:
        vol, part = self._get_vol_and_part(article)
        image_url = ''
        try:
            for num in range(1, 100):
                basket = f'{num:02d}'
                url = self._get_url(self.image_url, article, basket, vol, part)

                if self._limiter:
                    await self._limiter.acquire()

                async with session.get(url, headers=self.headers) as response:
                    if self._limiter:
                        await self._limiter.record_response(response.status)

                    if response.status == 200:
                        content_type = response.headers.get('Content-Type', '')
                        if content_type.startswith('image/'):
                            image_url = url
                            break
                    #     else:
                    #         print(f'Content is not an image: {content_type}')
                    # else:
                    #     print(f'Failed: {response.status}')
        except Exception as e:
            full_log(logger=logger, where="/_get_product_image")
            raise e
        finally:
            return image_url


async def main() -> None:
    # article = 85999881
    url = 'https://www.wildberries.ru/catalog/15728047/detail.aspx'
    scraper = WBScraper()
    async with aiohttp.ClientSession() as session:
        product_info = await scraper.fetch_product(session, url=url)
    for product in product_info:
        print(product, 2*'\n')

if __name__ == "__main__":
    asyncio.run(main())
