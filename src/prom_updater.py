import asyncio
import json
import os
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import hashlib
import xml.etree.ElementTree as ET

import aiohttp

from src.config import get_settings
from src.prom_client import PromClient

# Константи
REQUEST_TIMEOUT_FEED = aiohttp.ClientTimeout(total=120)
REQUEST_TIMEOUT_API = aiohttp.ClientTimeout(total=30)
BATCH_SIZE = 10  # Менші батчі для зменшення навантаження
CONCURRENT_BATCHES = 1  # Тільки 1 паралельний запит
API_DELAY = 2.0  # Більша затримка між запитами

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

# Функції для роботи з external_id
def _starts_with_fpref(x: str) -> bool:
    """Перевіряє чи починається рядок з префіксу fN_"""
    return len(x) >= 3 and x[0] == "f" and x[1].isdigit() and x[2] == "_"

def _build_external_id(offer: ET.Element, feed_index: int) -> Optional[str]:
    """Будує external_id згідно з логікою yml_generator"""
    # Спочатку перевіряємо vendorCode
    vendor_code = offer.findtext("vendorCode")
    if vendor_code and vendor_code.strip():
        vc = vendor_code.strip()
        # Якщо вже має префікс fN_, залишаємо як є
        if _starts_with_fpref(vc):
            return vc
        # Інакше додаємо префікс f{feed_index}_
        return f"f{feed_index}_{vc}"
    
    # Якщо немає vendorCode, беремо offer/@id
    offer_id = offer.get("id")
    if offer_id and offer_id.strip():
        oid = offer_id.strip()
        # Якщо вже має префікс fN_, залишаємо як є
        if _starts_with_fpref(oid):
            return oid
        # Інакше додаємо префікс f{feed_index}_
        return f"f{feed_index}_{oid}"
    
    # Якщо немає ні vendorCode, ні id, генеруємо MD5
    try:
        content = ET.tostring(offer, encoding="unicode")
        md5_hash = hashlib.md5(content.encode("utf-8")).hexdigest()[:8]
        return f"f{feed_index}_{md5_hash}"
    except Exception:
        return None

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

def _parse_xml_content(content: bytes, feed_index: int) -> List[Dict[str, Any]]:
    """Парсить XML контент і повертає список товарів."""
    try:
        root = ET.fromstring(content)
        offers = root.findall(".//offer")
    except ET.ParseError as e:
        print(f"❌ XML parse error: {e}")
        return []

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
    return products

async def parse_feed(session: aiohttp.ClientSession, url: str, feed_index: int) -> Tuple[bool, List[Dict[str, Any]]]:
    """Парсить фід і повертає список товарів. Підтримує Basic Auth для api.dropshipping.ua."""
    try:
        # Для api.dropshipping.ua додаємо Basic Auth
        auth = None
        if "api.dropshipping.ua" in url:
            # Додайте ваші логін/пароль для api.dropshipping.ua
            # Замініть "your_login" та "your_password" на реальні дані
            auth = aiohttp.BasicAuth("your_login", "your_password")
        
        async with session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT_FEED, auth=auth) as response:
            if response.status == 200:
                content = await response.read()
                products = _parse_xml_content(content, feed_index)
                return True, products
            else:
                print(f"❌ {url}: HTTP {response.status}")
                return False, []
    except Exception as e:
        print(f"❌ {url}: {e}")
        return False, []

async def send_single_batch(session: aiohttp.ClientSession, client: PromClient, batch: List[Dict[str, Any]], batch_idx: int) -> Tuple[int, int]:
    """Відправляє один батч і повертає (успішні, помилки)"""
    # Формуємо payload згідно з документацією Prom.ua
    # API Prom.ua очікує поле 'id' замість 'external_id'
    payload = []
    for product in batch:
        item = {
            "id": product["id"],  # Використовуємо 'id' замість 'external_id'
        }
        
        # Додаємо ціну тільки якщо вона є
        if product.get("price") is not None:
            item["price"] = product["price"]
        
        # Додаємо наявність тільки якщо є чіткий сигнал
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
                # Детальне логування успішних оновлень
                print(f"✅ Партія {batch_idx}: HTTP {status}")
                try:
                    response_data = json.loads(response_text)
                    if "processed_ids" in response_data:
                        processed = len(response_data["processed_ids"])
                        print(f"📊 Оброблено: {processed}/{len(batch)} товарів")
                        if "errors" in response_data and response_data["errors"]:
                            error_count = len(response_data["errors"])
                            print(f"⚠️ Помилок: {error_count}")
                            # Показуємо перші помилки
                            for i, (pid, error) in enumerate(list(response_data["errors"].items())[:3]):
                                print(f"  ❌ {pid}: {error}")
                            if error_count > 3:
                                print(f"  ... та ще {error_count - 3} помилок")
                    else:
                        print(f"📋 Відповідь: {response_text[:200]}")
                except json.JSONDecodeError:
                    print(f"📋 Відповідь: {response_text[:200]}")
                return len(batch), 0
            else:
                # Детальне логування помилок
                print(f"❌ Партія {batch_idx}: HTTP {status}")
                if response_text:
                    print(f"📋 Відповідь: {response_text[:300]}")
                if status in (403, 429) or 500 <= status <= 599:
                    if attempt == 0:
                        print(f"⚠️ Партія {batch_idx}: ретрай через {status}")
                        await asyncio.sleep(5)  # Більша затримка для серверних помилок
                        continue
                # Інші помилки - не ретраїмо
                return 0, len(batch)
        except Exception as e:
            print(f"❌ Партія {batch_idx}: Exception {e}")
            if attempt == 0:
                await asyncio.sleep(1)
                continue
            return 0, len(batch)
    
    return 0, len(batch)

async def send_updates(session: aiohttp.ClientSession, client: PromClient, products: List[Dict[str, Any]], batch_size: int) -> None:
    """Відправляє оновлення товарів батчами"""
    total_products = len(products)
    total_batches = (total_products + batch_size - 1) // batch_size
    
    print(f"🚀 Починаємо оновлення {total_products} товарів у {total_batches} партіях...")
    
    # Створюємо семафор для обмеження паралельних запитів
    semaphore = asyncio.Semaphore(CONCURRENT_BATCHES)
    
    async def process_batch(batch_idx: int, batch: List[Dict[str, Any]]) -> Tuple[int, int]:
        async with semaphore:
            await asyncio.sleep(API_DELAY)  # Затримка між запитами
            return await send_single_batch(session, client, batch, batch_idx)
    
    # Розбиваємо на батчі
    batches = []
    for i in range(0, total_products, batch_size):
        batch = products[i:i + batch_size]
        batch_idx = i // batch_size + 1
        batches.append((batch_idx, batch))
    
    # Обробляємо батчі паралельно
    tasks = [process_batch(batch_idx, batch) for batch_idx, batch in batches]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Підрахунок результатів
    total_success = 0
    total_errors = 0
    
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"❌ Партія {i+1}: Exception {result}")
            total_errors += len(batches[i][1])
        else:
            success, errors = result
            total_success += success
            total_errors += errors
    
    print(f"\n📊 Підсумок оновлення:")
    print(f"✅ Успішно оновлено: {total_success}")
    print(f"❌ Помилок: {total_errors}")

def load_urls() -> List[str]:
    """Завантажує список URL фідів з файлу feeds.txt"""
    feeds_file = "feeds.txt"
    if not os.path.exists(feeds_file):
        print(f"❌ Файл {feeds_file} не знайдено")
        return []
    
    urls = []
    with open(feeds_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and line.startswith("http"):
                urls.append(line)
    
    return urls

async def main_async() -> int:
    """Основна асинхронна функція"""
    settings = get_settings()
    
    # Завантаження URL фідів
    urls = load_urls()
    if not urls:
        print("❌ Немає URL фідів для обробки")
        return 1
    
    print(f"🔗 Знайдено {len(urls)} фідів")
    
    # Завантаження попереднього стану
    state_file = "product_state.json"
    previous_state = {}
    if os.path.exists(state_file):
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                previous_state = data.get("products", {})
            print(f"📂 Завантажено попередній стан: {len(previous_state)} товарів")
        except Exception:
            print("📂 Попередній стан не знайдено")
    
    # Збір даних з фідів
    print("🔄 Збір даних з фідів...")
    all_products = []
    successful_feeds = 0
    
    async with aiohttp.ClientSession() as session:
        for i, url in enumerate(urls, 1):
            print(f"🔄 Обробка фіду: {url}")
            success, products = await parse_feed(session, url, i)
            if success:
                all_products.extend(products)
                successful_feeds += 1
                print(f"✅ Фід {url}: {len(products)} товарів")
            else:
                print(f"❌ Фід {url}: помилка")
    
    print(f"\n📊 Підсумок збору:")
    print(f"✅ Успішних фідів: {successful_feeds}/{len(urls)}")
    print(f"📦 Загальна кількість товарів: {len(all_products)}")
    
    if not all_products:
        print("❌ Немає товарів для оновлення")
        return 1
    
    # Аналіз змін
    print("\n🔍 Аналіз змін:")
    print(f"📦 Всього товарів: {len(all_products)}")
    
    # ЗАВЖДИ оновлюємо всі товари
    products_to_update = all_products
    print(f"🔄 Оновлюємо ВСІ товари: {len(products_to_update)}")
    
    if not products_to_update:
        print("✅ Немає товарів для оновлення")
        return 0

    # Відправка оновлень
    print(f"\n🚀 Оновлення {len(products_to_update)} товарів...")

    client = PromClient(
        base_url=settings.prom_base_url,
        token=settings.prom_api_token,
        auth_header=settings.prom_auth_header,
        auth_scheme=settings.prom_auth_scheme,
        timeout_seconds=settings.http_timeout_seconds,
    )

    async with aiohttp.ClientSession() as session:
        await send_updates(session, client, products_to_update, BATCH_SIZE)
    
    # Збереження стану
    state = {
        "timestamp": datetime.now().isoformat(),
        "products": {p["id"]: p for p in all_products}
    }
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    
    print("✅ Оновлення завершено")
    return 0

if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async()))
