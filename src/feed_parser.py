from __future__ import annotations

import asyncio
import hashlib
import re
from datetime import datetime
from io import BytesIO
from typing import List, Optional, Tuple, Iterator
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


# ... (весь код як у тебе був: sanitize, iter_offers, fetch_all_offers і т.д.)

# -------------------- Читання feeds.txt --------------------
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
