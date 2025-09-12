from __future__ import annotations

import hashlib
from io import BytesIO
from typing import Iterable, Optional, Tuple, Iterator
from dataclasses import dataclass

import requests
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

    def fetch_and_parse(self, feed_url: str, prefix: str = "f") -> Iterable[ProductUpdate]:
        response = requests.get(feed_url, headers=HEADERS, timeout=self.timeout_seconds)
        response.raise_for_status()
        root = etree.fromstring(response.content)

        # Try common structures: yml_catalog/shop/offers/offer or rss/channel/item
        offers = root.xpath(".//offer")
        if not offers:
            offers = root.xpath(".//item")

        for node in offers:
            external_id = _make_safe_id(node, prefix)

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

            yield ProductUpdate(
                external_id=external_id,
                name=name,
                price=price,
                stock_quantity=stock_quantity,
                in_stock=in_stock,
                vendor_code=external_id,  # Прив'язуємо до унікального коду
            )


# ---------------- helpers ---------------- #

def _make_safe_id(node: etree._Element, prefix: str) -> str:
    yt_id = _first_text(node, ["@id", "g:id", "id", "offer_id", "vendorCode"])
    if yt_id and yt_id.strip():
        return f"{prefix}_{yt_id.strip()}"
    node_hash = hashlib.md5(etree.tostring(node)).hexdigest()
    return f"{prefix}_{node_hash}"


def _first_text(node: etree._Element, paths: list[str]) -> Optional[str]:
    for p in paths:
        if p.startswith("@"):
            val = node.get(p[1:])
            if val:
                return val
        res = node.find(p)
        if res is not None and res.text is not None and res.text.strip() != "":
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
        return 1000000  # effectively unlimited
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
