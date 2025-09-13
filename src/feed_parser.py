# src/feed_parser.py
from __future__ import annotations

import asyncio
import hashlib
import re
from dataclasses import dataclass
from io import BytesIO
from typing import List, Optional, Tuple, Iterator

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
    name: Optional[str]
    price: Optional[float]
    stock_quantity: Optional[int]
    in_stock: Optional[bool]
    vendor_code: Optional[str]


# -------------------- HELPER FUNCTIONS --------------------
def _first_text(node: etree._Element, paths: list[str]) -> Optional[str]:
    for p in paths:
        if p.startswith("@"):
            val = node.get(p[1:])
            if val:
                return val.strip()
        res = node.find(p)
        if res is not None and res.text and res.text.strip():
            return res.text.strip()
    return None


def _first_float(node: etree._Element, tags: list[str]) -> Optional[float]:
    for t in tags:
        el = node.find(t)
        if el is not None and el.text:
            try:
                return float(el.text.replace(",", ".").strip())
            except Exception:
                pass
    return None


def _first_int(node: etree._Element, tags: list[str]) -> Optional[int]:
    for t in tags:
        el = node.find(t)
        if el is not None and el.text:
            try:
                return int(float(el.text.strip()))
            except Exception:
                pass
    return None


def _infer_quantity_from_availability(node: etree._Element) -> Optional[int]:
    text = _first_text(node, ["availability", "instock", "in_stock"]) or ""
    text_lower = text.strip().lower()
    if text_lower in {"true", "1", "instock", "in stock", "available", "yes"}:
        return 1000000
    if text_lower in {"false", "0", "out of stock", "unavailable", "no"}:
        return 0
    return None


def _extract_price_with_currency(node: etree._Element) -> Optional[float]:
    price_el = node.find("price")
    if price_el is not None and price_el.text:
        try:
            return float(price_el.text.replace(",", ".").strip())
        except Exception:
            return None
    return None


def sanitize_text(text: Optional[str]) -> str:
    if not text:
        return ""
    text = re.sub(r'&(?![a-zA-Z]+;|#\d+;)', '&amp;', text)
    return text.replace("<", "&lt;").replace(">", "&gt;")


def sanitize_offer(elem: etree._Element) -> etree._Element:
    for child in elem.iter():
        if child.text:
            child.text = sanitize_text(child.text)
        if child.tail:
            child.tail = sanitize_text(child.tail)
    return elem


# -------------------- PARSING --------------------
def parse_offer_fields(elem: etree._Element) -> Tuple[Optional[str], Optional[str], Optional[float], Optional[int]]:
    offer_id = elem.get("id") or None
    vendor_code = elem.findtext("vendorCode") or None

    price_val: Optional[float] = None
    stock_qty: Optional[int] = None

    price_text = elem.findtext("price")
    if price_text:
        try:
            price_val = float(price_text.strip().replace(",", "."))
        except ValueError:
            price_val = None

    available_attr = elem.get("available")
    if available_attr is not None:
        stock_qty = 1 if available_attr.lower() in ("true", "1", "yes", "available", "in_stock") else 0

    qty_node = elem.find("quantity") or elem.find("stock_quantity") or elem.find("count")
    if qty_node is not None and qty_node.text:
        try:
            stock_qty = int(float(qty_node.text.strip()))
        except ValueError:
            pass

    return offer_id, vendor_code, price_val, stock_qty


def make_unique_code(prefix: str, offer_id: Optional[str], vendor_code: Optional[str], elem: etree._Element) -> str:
    base = (vendor_code or offer_id or hashlib.md5(etree.tostring(elem)).hexdigest()).strip()
    return f"{prefix}_{base}"


def iter_offers(xml_bytes: bytes, feed_prefix: str) -> Iterator[ProductUpdate]:
    try:
        context = etree.iterparse(BytesIO(xml_bytes), tag="offer", recover=True)
        for _, elem in context:
            elem = sanitize_offer(elem)
            offer_id, vendor_code, price_val, stock_qty = parse_offer_fields(elem)

            unique_code = make_unique_code(feed_prefix, offer_id, vendor_code, elem)

            name = elem.findtext("name") or elem.findtext("title") or None

            in_stock = None
            if stock_qty is not None:
                in_stock = stock_qty > 0

            yield ProductUpdate(
                external_id=unique_code,
                name=name,
                price=price_val,
                stock_quantity=stock_qty,
                in_stock=in_stock,
                vendor_code=vendor_code,
            )
            elem.clear()
    except Exception as e:
        print(f"❌ Помилка парсингу XML: {e}")


# -------------------- FETCHING --------------------
async def fetch_offers_from_url(session: aiohttp.ClientSession, url: str, feed_index: int) -> List[ProductUpdate]:
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


async def fetch_all_offers(urls: List[str]) -> List[ProductUpdate]:
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_offers_from_url(session, url, i + 1) for i, url in enumerate(urls)]
        results = await asyncio.gather(*tasks)
        return [offer for sublist in results for offer in sublist]
