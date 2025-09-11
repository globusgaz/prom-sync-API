from __future__ import annotations

import asyncio
import hashlib
import re
from datetime import datetime
from io import BytesIO
from typing import AsyncIterator, Dict, List, Optional, Tuple, Iterator
from dataclasses import dataclass

import aiohttp
from lxml import etree

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Safari/537.36"
    )
}


@dataclass(frozen=True)
class ProductUpdate:
	external_id: str
	name: str | None
	price: float | None
	stock_quantity: int | None
	in_stock: bool | None
	vendor_code: str | None


class FeedParser:
	def __init__(self, timeout_seconds: int = 60) -> None:
		self.timeout_seconds = timeout_seconds

	def fetch_and_parse(self, feed_url: str) -> Iterable[ProductUpdate]:
		response = requests.get(feed_url, timeout=self.timeout_seconds)
		response.raise_for_status()
		root = etree.fromstring(response.content)

		# Try common structures: yml_catalog/shop/offers/offer or rss/channel/item
		offers = root.xpath(".//offer")
		if not offers:
			offers = root.xpath(".//item")

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
				in_stock = availability_text.strip().lower() in {"true", "1", "in stock", "available", "yes", "instock"}

			yield ProductUpdate(
				external_id=str(yt_id),
				name=name,
				price=price,
				stock_quantity=stock_quantity,
				in_stock=in_stock,
				vendor_code=vendor_code,
			)


def load_urls(feeds_file: str) -> List[str]:
    urls: List[str] = []
    try:
        with open(feeds_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("http"):
                    urls.append(line)
    except FileNotFoundError:
        print(f"❌ Файл {feeds_file} не знайдено")
    return urls


def sanitize_text(text: Optional[str]) -> str:
    if not text:
        return ""
    text = re.sub(r'&(?![a-zA-Z]+;|#\d+;)', '&amp;', text)
    text = text.replace('<', '&lt;').replace('>', '&gt;')
    return text


def sanitize_offer(elem: etree._Element) -> etree._Element:
    for child in elem.iter():
        if child.text:
            child.text = sanitize_text(child.text)
        if child.tail:
            child.tail = sanitize_text(child.tail)
    return elem


def parse_offer_fields(elem: etree._Element) -> Tuple[Optional[str], Optional[str], Optional[float], Optional[int]]:
    offer_id = elem.get("id") or None
    vendor_code = elem.findtext("vendorCode") or None

    price_val: Optional[float] = None
    stock_qty: Optional[int] = None

    # Common YML fields
    price_text = elem.findtext("price")
    if price_text:
        try:
            price_val = float(price_text.strip().replace(",", "."))
        except ValueError:
            price_val = None

    # Try parse stock from 'available' attr or quantity fields
    available_attr = elem.get("available")
    if available_attr is not None:
        stock_qty = 1 if available_attr.lower() in ("true", "1", "yes", "available", "in_stock") else 0

    qty_text = None
    qty_node = elem.find("quantity") or elem.find("stock_quantity") or elem.find("count")
    if qty_node is not None and qty_node.text:
        qty_text = qty_node.text.strip()
    if qty_text:
        try:
            stock_qty = int(float(qty_text))
        except ValueError:
            pass

    return offer_id, vendor_code, price_val, stock_qty


def make_unique_code(prefix: str, offer_id: Optional[str], vendor_code: Optional[str], elem: etree._Element) -> str:
    base = (vendor_code or offer_id or hashlib.md5(etree.tostring(elem)).hexdigest()).strip()
    return f"{prefix}_{base}"


def iter_offers(xml_bytes: bytes, feed_prefix: str) -> Iterator[str]:
    # Typing note: using Iterator, but not importing to keep dependencies minimal here
    try:
        context = etree.iterparse(BytesIO(xml_bytes), tag="offer", recover=True)
        for _, elem in context:
            elem = sanitize_offer(elem)
            offer_id, vendor_code, price_val, stock_qty = parse_offer_fields(elem)

            unique_code = make_unique_code(feed_prefix, offer_id, vendor_code, elem)
            elem.set("id", unique_code)

            vc_elem = elem.find("vendorCode")
            if vc_elem is not None:
                vc_elem.text = unique_code
            else:
                new_vc = etree.Element("vendorCode")
                new_vc.text = unique_code
                elem.insert(0, new_vc)

            url_elem = elem.find("url")
            if url_elem is not None and url_elem.text:
                clean_url = url_elem.text.strip()
                if "?" in clean_url:
                    clean_url = clean_url.split("?")[0]
                url_elem.text = f"{clean_url}?id={unique_code}"

            yield etree.tostring(elem, encoding="utf-8").decode("utf-8")
            elem.clear()
    except Exception as e:
        print(f"❌ Помилка парсингу XML: {e}")


async def fetch_offers_from_url(session: aiohttp.ClientSession, url: str, feed_index: int) -> List[str]:
    try:
        async with session.get(url, headers=HEADERS, timeout=120) as response:
            if response.status != 200:
                print(f"❌ {url} — HTTP {response.status}")
                return []
            content = await response.read()
            feed_prefix = f"f{feed_index}"
            offers = list(iter_offers(content, feed_prefix))
            print(f"✅ {url} — {len(offers)} товарів")
            return offers
    except Exception as e:
        print(f"❌ {url}: {e}")
        return []


async def fetch_all_offers(urls: List[str]) -> Tuple[List[str], List[List[str]]]:
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_offers_from_url(session, url, i + 1) for i, url in enumerate(urls)]
        results = await asyncio.gather(*tasks)
        all_offers = [offer for sublist in results for offer in sublist]
        return all_offers, results


def _first_text(node: etree._Element, paths: list[str]) -> str | None:
	for p in paths:
		if p.startswith("@"):
			val = node.get(p[1:])
			if val:
				return val
		res = node.find(p)
		if res is not None and res.text is not None and res.text.strip() != "":
			return res.text.strip()
	return None


def _first_float(node: etree._Element, tags: list[str]) -> float | None:
	for t in tags:
		el = node.find(t)
		if el is not None and el.text:
			try:
				return float(el.text.replace(",", ".").strip())
			except Exception:
				pass
	return None


def _first_int(node: etree._Element, tags: list[str]) -> int | None:
	for t in tags:
		el = node.find(t)
		if el is not None and el.text:
			try:
				return int(float(el.text.strip()))
			except Exception:
				pass
	return None


def _infer_quantity_from_availability(node: etree._Element) -> int | None:
	text = _first_text(node, ["availability", "instock", "in_stock"]) or ""
	text_lower = text.strip().lower()
	if text_lower in {"true", "1", "instock", "in stock", "available", "yes"}:
		return 1000000  # effectively unlimited
	if text_lower in {"false", "0", "out of stock", "unavailable", "no"}:
		return 0
	return None


def _extract_price_with_currency(node: etree._Element) -> float | None:
	# some feeds use <price currency="UAH">123</price>
	price_el = node.find("price")
	if price_el is not None and price_el.text:
		try:
			return float(price_el.text.replace(",", ".").strip())
		except Exception:
			return None
	return None
