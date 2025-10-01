import os
import json
import xml.etree.ElementTree as ET
import time
import asyncio
import aiohttp
from hashlib import md5
from typing import Tuple, Optional, Dict, Any, List

API_URL = "https://my.prom.ua/api/v1/products/edit_by_external_id"
API_TOKEN = os.getenv("PROM_API_TOKEN")

FEEDS_FILE = "feeds.txt"
STATE_FILE = "product_state.json"

# Продуктивність
BATCH_SIZE = 100
CONCURRENT_REQUESTS = 3
REQUEST_TIMEOUT_FEED = 180
REQUEST_TIMEOUT_API = 120
DELAY_BETWEEN_WAVES = 0.3

# Вимога цілісності
REQUIRE_ALL_FEEDS = True  # якщо хоч один фід недоступний — зупиняємось, щоб не зіпсувати наявність

# HTTP заголовки (ніжні до блокувань)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Safari/537.36"
    )
}

def load_previous_state() -> Dict[str, Dict[str, Any]]:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_current_state(products: List[Dict[str, Any]]) -> None:
    state = {}
    for p in products:
        state[p["id"]] = {
            "price": p.get("price"),
            "presence": p.get("presence"),
            "quantity_in_stock": p.get("quantity_in_stock")
        }
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

def has_changed(product: Dict[str, Any], old_state: Dict[str, Dict[str, Any]]) -> bool:
    pid = product["id"]
    if pid not in old_state:
        return True
    old = old_state[pid]
    return (
        old.get("price") != product.get("price") or
        old.get("presence") != product.get("presence") or
        old.get("quantity_in_stock") != product.get("quantity_in_stock")
    )

def _get_text(el: ET.Element, tag: str) -> Optional[str]:
    node = el.find(tag)
    return node.text.strip() if node is not None and node.text else None

def _safe_float(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    try:
        return float(text.replace(",", ".").strip())
    except Exception:
        return None

def _infer_availability(offer: ET.Element) -> Tuple[Optional[str], Optional[int], bool]:
    """
    Повертає (presence, quantity_in_stock, sure_flag).
    - presence ∈ {"available","not_available"} або None
    - quantity_in_stock ∈ int або None
    - sure_flag: чи впевнено можемо оновлювати наявність (presence_sure)
    Логіка обережна: якщо немає чітких сигналів - повертаємо (None, None, False).
    """
    # 1) Сильний сигнал: атрибут available
    avail_attr = offer.get("available")
    if avail_attr is not None:
        v = avail_attr.strip().lower()
        if v in {"true", "1", "yes", "available", "in_stock"}:
            return "available", 1, True
        if v in {"false", "0", "no", "not_available", "out_of_stock"}:
            return "not_available", 0, True

    # 2) Сильний сигнал: кількісний тег
    for tag in ["quantity", "stock_quantity", "count", "quantity_in_stock", "g:quantity"]:
        node = offer.find(tag)
        if node is not None and node.text:
            q = _safe_float(node.text)
            if q is not None:
                q_int = max(0, int(q))
                return ("available" if q_int > 0 else "not_available", q_int, True)

    # 3) Середній сигнал: текстові поля
    for tag in ["availability", "instock", "in_stock", "g:availability"]:
        node = offer.find(tag)
        if node is not None and node.text:
            txt = node.text.strip().lower()
            if txt in {"true", "1", "in stock", "available", "yes", "instock"}:
                return "available", 1, True
            if txt in {"false", "0", "out of stock", "unavailable", "no"}:
                return "not_available", 0, True

    # 4) Немає впевненості — не чіпаємо наявність
    return None, None, False

def _build_external_id(offer: ET.Element, feed_index: int) -> str:
    offer_id = offer.get("id") or ""
    vendor_code = _get_text(offer, "vendorCode")
    base = (vendor_code or offer_id or md5(ET.tostring(offer)).hexdigest()).strip()
    return f"f{feed_index}_{base}"

def _extract_price(offer: ET.Element) -> Optional[float]:
    # Найчастіші варіанти
    price_text = None
    price_node = offer.find("price")
    if price_node is not None and price_node.text:
        price_text = price_node.text
    if not price_text:
        # інші кастомні поля (доповнюйте за потреби)
        for tag in ["g:price", "current_price"]:
            node = offer.find(tag)
            if node is not None and node.text:
                price_text = node.text
                break
    return _safe_float(price_text) if price_text else None

async def parse_feed(session: aiohttp.ClientSession, url: str, feed_index: int) -> Tuple[bool, List[Dict[str, Any]]]:
    try:
        async with session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT_FEED) as response:
            if response.status != 200:
                print(f"❌ {url} — HTTP {response.status}")
                return False, []
            content = await response.read()
            root = ET.fromstring(content)
            offers = root.findall(".//offer")
            products: List[Dict[str, Any]] = []

            for offer in offers:
                external_id = _build_external_id(offer, feed_index)
                price = _extract_price(offer)

                presence, qty, sure = _infer_availability(offer)

                item: Dict[str, Any] = {"id": external_id}
                if price is not None:
                    item["price"] = price

                # Критично: оновлюємо наявність тільки якщо ми впевнені
                if sure:
                    item["presence"] = presence
                    item["quantity_in_stock"] = qty
                    item["_presence_sure"] = True
                else:
                    # НІЯКИХ presence/quantity — не чіпаємо наявність
                    item["_presence_sure"] = False

                products.append(item)

            return True, products
    except Exception as e:
        print(f"❌ {url}: {e}")
        return False, []

async def send_updates(session: aiohttp.ClientSession, batch: List[Dict[str, Any]], batch_num: int, total_batches: int) -> None:
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
        "X-LANGUAGE": "uk"
    }

    payload: List[Dict[str, Any]] = []
    for item in batch:
        obj = {"id": item["id"]}
        # Ціна завжди, якщо є
        if item.get("price") is not None:
            obj["price"] = item["price"]
        # Наявність — тільки якщо впевнені
        if item.get("_presence_sure"):
            obj["presence"] = item.get("presence")
            obj["presence_sure"] = True
            obj["quantity_in_stock"] = item.get("quantity_in_stock")
        payload.append(obj)

    print(f"🔄 Партія {batch_num}/{total_batches} ({len(payload)} товарів)")

    try:
        async with session.post(API_URL, headers=headers, json=payload, timeout=REQUEST_TIMEOUT_API) as response:
            response_text = await response.text()
            if response.status == 200:
                try:
                    result = json.loads(response_text)
                    processed = len(result.get("processed_ids", []))
                    errors = result.get("errors", {})
                    if errors:
                        print(f"⚠️ Партія {batch_num}: оброблено {processed}/{len(payload)}, помилок: {len(errors)}")
                        for i, (ext_id, error) in enumerate(list(errors.items())[:5]):
                            print(f"  ❌ {ext_id}: {error}")
                    else:
                        print(f"✅ Партія {batch_num}: {processed}/{len(payload)}")
                    if batch_num == 1:
                        print(f"📋 Відповідь API: {json.dumps(result, ensure_ascii=False, indent=2)[:500]}")
                except json.JSONDecodeError:
                    print(f"❌ Партія {batch_num} - не JSON відповідь: {response_text[:200]}")
            else:
                print(f"❌ Партія {batch_num} - HTTP {response.status}: {response_text[:200]}")
    except Exception as e:
        print(f"❌ Партія {batch_num}: {e}")

async def main_async() -> None:
    if not API_TOKEN:
        print("❌ Токен PROM_API_TOKEN не знайдено!")
        return
    if not os.path.exists(FEEDS_FILE):
        print(f"❌ Файл {FEEDS_FILE} не знайдено!")
        return

    with open(FEEDS_FILE, "r", encoding="utf-8") as f:
        feed_urls = [line.strip() for line in f if line.strip()]

    old_state = load_previous_state()
    print(f"📂 Завантажено попередній стан: {len(old_state)} товарів\n")

    print("🔄 Збір даних з фідів...")
    all_products: List[Dict[str, Any]] = []
    failed_feeds: List[str] = []

    async with aiohttp.ClientSession() as session:
        tasks = [parse_feed(session, url, i + 1) for i, url in enumerate(feed_urls)]
        results = await asyncio.gather(*tasks)
        for url, (success, products) in zip(feed_urls, results):
            if success:
                print(f"✅ {url}: {len(products)} товарів")
                all_products.extend(products)
            else:
                failed_feeds.append(url)

        print("\n📊 Підсумок збору:")
        print(f"✅ Успішних фідів: {len(feed_urls) - len(failed_feeds)}/{len(feed_urls)}")
        print(f"📦 Загальна кількість товарів: {len(all_products)}\n")

        if failed_feeds:
            print("⚠️ Недоступні фіди:")
            for u in failed_feeds:
                print(f"  - {u}")
            if REQUIRE_ALL_FEEDS:
                print("\n🛑 ЗУПИНКА: Не всі фіди доступні! Оновлення не виконується.")
                return

        if not all_products:
            print("❌ Немає товарів!")
            return

        changed_products = [p for p in all_products if has_changed(p, old_state)]

        print("🔍 Аналіз змін:")
        print(f"📦 Всього товарів: {len(all_products)}")
        print(f"🔄 Змінилось: {len(changed_products)}\n")

        if not changed_products:
            print("✅ Немає змін — нічого відправляти")
            save_current_state(all_products)
            return

        total_batches = (len(changed_products) - 1) // BATCH_SIZE + 1
        print(f"🚀 Оновлення {len(changed_products)} товарів у {total_batches} партіях...")

        start = time.time()
        async with aiohttp.ClientSession() as api_session:
            # надсилаємо хвилями, по CONCURRENT_REQUESTS партій одночасно
            for i in range(0, len(changed_products), BATCH_SIZE * CONCURRENT_REQUESTS):
                jobs = []
                for j in range(CONCURRENT_REQUESTS):
                    idx = i + j * BATCH_SIZE
                    if idx >= len(changed_products):
                        break
                    batch = changed_products[idx: idx + BATCH_SIZE]
                    batch_num = idx // BATCH_SIZE + 1
                    jobs.append(send_updates(api_session, batch, batch_num, total_batches))
                await asyncio.gather(*jobs)
                await asyncio.sleep(DELAY_BETWEEN_WAVES)

        duration = time.time() - start
        save_current_state(all_products)
        print(f"\n💾 Стан збережено: {len(all_products)} товарів")
        print(f"✅ Завершено за {duration:.1f}с ({duration/60:.1f}хв)")
        if changed_products:
            print(f"📊 Швидкість: {len(changed_products)/duration:.1f} товарів/сек")

def main() -> None:
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
