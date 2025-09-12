import aiohttp
from lxml import etree
from typing import Iterable
from .feed_parser import ProductUpdate, _first_text, _first_float, _first_int, _infer_quantity_from_availability, _extract_price_with_currency


class FeedParser:
    def __init__(self, timeout_seconds: int = 60) -> None:
        self.timeout_seconds = timeout_seconds

    async def fetch_and_parse(self, session: aiohttp.ClientSession, feed_url: str) -> Iterable[ProductUpdate]:
        async with session.get(feed_url, timeout=self.timeout_seconds) as response:
            if response.status != 200:
                print(f"❌ {feed_url} — HTTP {response.status}")
                return []

            content = await response.read()
            root = etree.fromstring(content)

            # Try common structures: yml_catalog/shop/offers/offer or rss/channel/item
            offers = root.xpath(".//offer")
            if not offers:
                offers = root.xpath(".//item")

            updates: list[ProductUpdate] = []
            for node in offers:
                yt_id = _first_text(node, ["@id", "g:id", "id", "offer_id", "vendorCode"]) or ""
                if not yt_id:
                    continue

                name = _first_text(node, ["name", "title", "model", "g:title"])
                vendor_code = _first_text(node, ["vendorCode", "article", "sku"])

                price = _first_float(node, ["price", "g:price", "current_price"]) \
                    or _extract_price_with_currency(node)

                stock_quantity = _first_int(node, ["stock_quantity", "quantity", "g:quantity"]) \
                    or _infer_quantity_from_availability(node)

                availability_text = _first_text(node, ["availability", "instock", "in_stock", "g:availability"]) or ""
                in_stock = None
                if availability_text:
                    in_stock = availability_text.strip().lower() in {
                        "true", "1", "in stock", "available", "yes", "instock"
                    }

                updates.append(
                    ProductUpdate(
                        external_id=str(yt_id),
                        name=name,
                        price=price,
                        stock_quantity=stock_quantity,
                        in_stock=in_stock,
                        vendor_code=vendor_code,
                    )
                )

            return updates
