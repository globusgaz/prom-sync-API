import asyncio
import json
import os
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import hashlib
import xml.etree.ElementTree as ET

import aiohttp
import random
from urllib.parse import urlsplit, urlunsplit

from src.config import get_settings
from src.prom_client import PromClient

# Константи
REQUEST_TIMEOUT_FEED = aiohttp.ClientTimeout(total=120)
REQUEST_TIMEOUT_API = aiohttp.ClientTimeout(total=30)
BATCH_SIZE = 100  # Великі батчі
CONCURRENT_BATCHES = 5  # 5 паралельних запитів
API_DELAY = 0.05  # Мінімальна затримка

# Заголовки для запитів до фідів
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Accept": "application/xml, text/xml, */*",
    "Accept-Language": "uk-UA,uk;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache"
}

# Витяг облікових даних з URL (user:pass@host)
def _extract_basic_auth(url: str) -> Tuple[str, Optional[aiohttp.BasicAuth]]:
    parts = urlsplit(url)
    if parts.username or parts.password:
        clean_netloc = parts.hostname
        if parts.port:
            clean_netloc = f"{clean_netloc}:{parts.port}"
        clean_url = urlunsplit((parts.scheme, clean_netloc, parts.path, parts.query, parts.fragment))
        auth = aiohttp.BasicAuth(parts.username or "", parts.password or "")
        return clean_url, auth
    return url, None

# Функції для роботи з external_id
def _starts_with_fpref(x: str) -> bool:
    """Перевіряє чи починається рядок з префіксу fN_"""
    return len(x) >= 3 and x[0] == "f" and x[1].isdigit() and x[2] == "_"

def _text_of(elem: Optional[ET.Element]) -> str:
    """Безпечно отримує текст з елемента"""
    return (elem.text or "").strip() if elem is not None else ""

def _build_external_id(offer: ET.Element, feed_index: int) -> Optional[str]:
    """Побудова external_id згідно з логікою YML генератора"""
    offer_id = offer.get("id") or ""
    vendor_code = _text_of(offer.find("vendorCode")) or ""
    
    # Перевіряємо чи вже є префікс fN_
    if vendor_code and _starts_with_fpref(vendor_code):
        return vendor_code
    if offer_id and _starts_with_fpref(offer_id):
        return offer_id
    
    # Якщо немає префіксу, додаємо f{feed_index}_
    base = vendor_code or offer_id or hashlib.md5(ET.tostring(offer)).hexdigest()
    return f"f{feed_index}_{base}" if base else None

def _extract_price(offer: ET.Element) -> Optional[float]:
    """Витягує ціну з offer"""
    price_elem = offer.find("price")
    if price_elem is not None and price_elem.text:
        try:
            return float(price_elem.text.strip().replace(",", "."))
        except (ValueError, AttributeError):
            pass
    return None

def _infer_availability(offer: ET.Element) -> Tuple[bool, int, bool]:
    """
    Визначає наявність товару з offer
    Повертає: (presence, quantity, sure)
    sure = True означає що є чіткий сигнал наявності
    """
    # Перевіряємо атрибут available
    available = offer.get("available")
    if available is not None:
        is_available = available.lower() in ("true", "1", "yes", "available", "in_stock")
        return is_available, 1 if is_available else 0, True
    
    # Перевіряємо теги кількості
    quantity_tags = ["quantity", "stock_quantity", "count", "quantity_in_stock"]
    for tag in quantity_tags:
        qty_elem = offer.find(tag)
        if qty_elem is not None and qty_elem.text:
            try:
                qty = int(float(qty_elem.text.strip()))
                return qty > 0, qty, True
            except (ValueError, AttributeError):
                continue
    
    # Перевіряємо теги наявності
    presence_tags = ["presence", "in_stock", "available"]
    for tag in presence_tags:
        pres_elem = offer.find(tag)
        if pres_elem is not None and pres_elem.text:
            text = pres_elem.text.strip().lower()
            if text in ("true", "1", "yes", "available", "in_stock"):
                return True, 1, True
            elif text in ("false", "0", "no", "out_of_stock", "not_available"):
                return False, 0, True
    
    # Якщо немає чітких сигналів, вважаємо що товар в наявності
    return True, 1, False

async def parse_feed(session: aiohttp.ClientSession, url: str, feed_index: int, auth: Optional[aiohttp.BasicAuth]) -> Tuple[bool, List[Dict[str, Any]]]:
    """Парсить фід і повертає список товарів. Підтримує Basic Auth та ретраї."""
    backoffs = [0, 1, 2, 4, 8, 16]
    clean_url, embedded_auth = _extract_basic_auth(url)
    if embedded_auth is not None:
        auth = embedded_auth
    for attempt, delay in enumerate(backoffs):
        if delay:
            await asyncio.sleep(delay)
        try:
            ua = HEADERS["User-Agent"]
            if attempt:
                ua = f"{ua} rv/{random.randint(60,120)}.0"
            headers = {**HEADERS, "User-Agent": ua, "Referer": clean_url}
            async with session.get(clean_url, headers=headers, timeout=REQUEST_TIMEOUT_FEED, auth=auth) as resp:
                if not (200 <= resp.status < 300):
                    if resp.status in (403, 429) or 500 <= resp.status <= 599:
                        print(f"⚠️ {clean_url} — HTTP {resp.status}, ретрай #{attempt}")
                        continue
                    print(f"❌ {clean_url} — HTTP {resp.status}")
                    return False, []
                content = await resp.read()
        except Exception as e:
            print(f"⚠️ {clean_url}: {e}, ретрай #{attempt}")
            continue
        # Парсимо при успішній відповіді
        try:
            root = ET.fromstring(content)
            offers = root.findall(".//offer")
        except ET.ParseError as e:
            print(f"❌ XML parse error {clean_url}: {e}")
            return False, []

        products: List[Dict[str, Any]] = []
        for offer in offers:
            external_id = _build_external_id(offer, feed_index)
            if not external_id:
                continue
            price = _extract_price(offer)
            presence, qty, sure = _infer_availability(offer)
            item: Dict[str, Any] = {"id": external_id}
            if price is not None:
                item["price"] = price
            if sure:
                item["presence"] = presence
                item["quantity_in_stock"] = qty
                item["_presence_sure"] = True
            else:
                item["_presence_sure"] = False
            products.append(item)
        return True, products
    print(f"❌ {clean_url}: вичерпано спроби")
    return False, []

async def send_single_batch(session: aiohttp.ClientSession, client: PromClient, batch: List[Dict[str, Any]], batch_idx: int) -> Tuple[int, int]:
    """Відправляє один батч і повертає (успішні, помилки)"""
    # Формуємо payload
    payload = []
    for product in batch:
        item = {"id": product["id"]}
        if "price" in product:
            item["price"] = product["price"]
        if product.get("_presence_sure", False):
            item["presence"] = product["presence"]
            item["quantity_in_stock"] = product["quantity_in_stock"]
            item["presence_sure"] = True
        payload.append(item)
    
    # Відправка з 1 ретраєм
    for attempt in range(2):
        try:
            status, response_text = await client.update_products(session, "/api/v1/products/edit_by_external_id", payload)
            if 200 <= status < 300:
                return len(batch), 0
            elif status in (403, 429) or 500 <= status <= 599:
                if attempt == 0:
                    await asyncio.sleep(1)
                    continue
            # Інші помилки - не ретраїмо
            return 0, len(batch)
        except Exception as e:
            if attempt == 0:
                await asyncio.sleep(1)
                continue
            return 0, len(batch)
    
    return 0, len(batch)

async def send_updates(session: aiohttp.ClientSession, client: PromClient, products: List[Dict[str, Any]], batch_size: int = BATCH_SIZE) -> None:
    """Відправляє оновлення на Prom.ua з паралельними батчами."""
    batches = [products[i:i + batch_size] for i in range(0, len(products), batch_size)]
    total_batches = len(batches)
    successful = 0
    failed = 0
    
    print(f"🚀 Відправляємо {total_batches} батчів по {batch_size} товарів (паралельно {CONCURRENT_BATCHES})")
    
    # Семафор для обмеження кількості паралельних запитів
    semaphore = asyncio.Semaphore(CONCURRENT_BATCHES)
    
    async def process_batch(batch_idx: int, batch: List[Dict[str, Any]]) -> Tuple[int, int]:
        async with semaphore:
            print(f"🔄 Партія {batch_idx}/{total_batches} ({len(batch)} товарів)")
            return await send_single_batch(session, client, batch, batch_idx)
    
    # Запускаємо всі батчі паралельно
    tasks = [process_batch(i, batch) for i, batch in enumerate(batches, 1)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Підрахунок результатів
    for result in results:
        if isinstance(result, Exception):
            failed += 1
            print(f"❌ Помилка батча: {result}")
        else:
            succ, fail = result
            successful += succ
            failed += fail
    
    print(f"\n📊 Підсумок оновлення:")
    print(f"✅ Успішно: {successful} товарів")
    print(f"❌ Помилок: {failed} товарів")

async def main_async() -> int:
    """Основна функція"""
    settings = get_settings()
    
    if not settings.prom_api_token:
        print("❌ PROM_API_TOKEN не встановлено")
        return 1
    
    # Завантаження URL фідів
    feeds_file = os.path.join(os.getcwd(), "feeds.txt")
    if not os.path.exists(feeds_file):
        print(f"❌ Файл {feeds_file} не знайдено")
        return 1
    
    with open(feeds_file, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip().startswith("http")]
    
    print(f"🔗 Знайдено {len(urls)} фідів")
    
    # Завантаження попереднього стану
    state_file = "product_state.json"
    previous_state = {}
    if os.path.exists(state_file):
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                previous_state = json.load(f)
            print(f"📂 Завантажено попередній стан: {len(previous_state)} товарів")
        except Exception:
            print("📂 Попередній стан не знайдено")
    
    # Збір даних з фідів
    print("🔄 Збір даних з фідів...")
    all_products = []
    successful_feeds = 0
    
    async with aiohttp.ClientSession() as session:
        for i, url in enumerate(urls, 1):
            clean_url, auth = _extract_basic_auth(url)
            print(f"🔄 Обробка фіду: {clean_url}")
            success, products = await parse_feed(session, clean_url, i, auth)
            if success:
                all_products.extend(products)
                successful_feeds += 1
                print(f"✅ Фід {clean_url}: {len(products)} товарів")
            else:
                print(f"❌ Фід {clean_url}: помилка")
    
    print(f"\n📊 Підсумок збору:")
    print(f"✅ Успішних фідів: {successful_feeds}/{len(urls)}")
    print(f"📦 Загальна кількість товарів: {len(all_products)}")
    
    if not all_products:
        print("❌ Немає товарів для оновлення")
        return 1
    
    # Аналіз змін
    print("\n🔍 Аналіз змін:")
    current_state = {p["id"]: p for p in all_products}
    changed_products = []
    
    for product in all_products:
        product_id = product["id"]
        if product_id not in previous_state:
            changed_products.append(product)
        else:
            prev = previous_state[product_id]
            # Порівнюємо ціну та наявність
            if (product.get("price") != prev.get("price") or 
                product.get("presence") != prev.get("presence") or
                product.get("quantity_in_stock") != prev.get("quantity_in_stock")):
                changed_products.append(product)
    
    print(f"📦 Всього товарів: {len(all_products)}")
    print(f"🔄 Змінилось: {len(changed_products)}")
    print(f"✅ Без змін: {len(all_products) - len(changed_products)}")
    
    if not changed_products:
        print("⏭️ Змін не виявлено — пропускаємо оновлення")
        return 0
    
    # Відправка оновлень
    print(f"\n🚀 Оновлення {len(changed_products)} товарів...")
    
    client = PromClient(
        base_url=settings.prom_base_url,
        token=settings.prom_api_token,
        auth_header=settings.prom_auth_header,
        auth_scheme=settings.prom_auth_scheme,
        timeout_seconds=settings.http_timeout_seconds,
    )
    
    async with aiohttp.ClientSession() as session:
        await send_updates(session, client, changed_products, BATCH_SIZE)
    
    # Збереження стану
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(current_state, f, ensure_ascii=False, indent=2)
    
    print("✅ Оновлення завершено")
    return 0

if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async()))
