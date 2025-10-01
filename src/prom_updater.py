import os
import json
import xml.etree.ElementTree as ET
import time
import asyncio
import aiohttp
from hashlib import md5
from typing import List, Dict, Any, Tuple, Optional

# ---------------- Конфіг ----------------

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

# Вимога цілісності: якщо хоч один фід впав — не оновлюємо взагалі
REQUIRE_ALL_FEEDS = True

# Заголовки для фідів (щоб менше блокували)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Safari/537.36"
    )
}

# ---------------- Допоміжні ----------------

def load_previous_state() -> Dict[str, Dict[str, Any]]:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
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
            "quantity_in_stock": p.get("quantity_in_stock"),
        }
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

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

def _safe_float(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    try:
        return float(text.replace(",", ".").strip())
    except Exception:
        return None

def _text_of(node: Optional[ET.Element]) -> Optional[str]:
    return node.text.strip() if node is not None and node.text else None

def _starts_with_fpref(x: str) -> bool:
    return len(x) >= 3 and x[0] == "f" and x[1].isdigit() and x[2] == "_"

# ---------------- Парсинг фідів ----------------
# external_id для API має співпасти з тим, що в YML:
# - якщо у vendorCode вже fN_... → беремо як є
# - інакше якщо offer/@id вже fN_... → беремо як є
# - інакше додаємо префікс f{feed_index}_ до (vendorCode або offer_id або md5(offer))

def _infer_availability(offer: ET.Element) -> Tuple[Optional[str], Optional[int], bool]:
    """
    Повертає (presence, quantity_in_stock, sure_flag).
    sure_flag=True — можна безпечно оновлювати наявність (presence_sure).
    """
    avail_attr = offer.get("available")
    if avail_attr is not None:
        v = avail_attr.strip().lower()
        if v in {"true", "1", "yes", "available", "in_stock"}:
            return "available", 1, True
        if v in {"false", "0", "no", "not_available", "out_of_stock"}:
            return "not_available", 0, True

    for tag in ("quantity", "stock_quantity", "count", "quantity_in_stock", "g:quantity"):
        node = offer.find(tag)
        if node is not None and node.text:
            q = _safe_float(node.text)
            if q is not None:
                qi = max(0, int(q))
                return ("available" if qi > 0 else "not_available"), qi, True

    for tag in ("availability", "instock", "in_stock", "g:availability"):
        node = offer.find(tag)
        if node is not None and node.text:
            t = node.text.strip().lower()
            if t in {"true", "1", "in stock", "available", "yes", "instock"}:
                return "available", 1, True
            if t in {"false", "0", "out of stock", "unavailable", "no"}:
                return "not_available", 0, True

    return None, None, False

def _extract_price(offer: ET.Element) -> Optional[float]:
    node = offer.find("price")
    if node is not None and node.text:
        return _safe_float(node.text)
    for tag in ("g:price", "current_price"):
        node = offer.find(tag)
        if node is not None and node.text:
            v = _safe_float(node.text)
            if v is not None:
                return v
    return None

def _build_external_id(offer: ET.Element, feed_index: int) -> Optional[str]:
    offer_id = offer.get("id") or ""
    vendor_code = _text_of(offer.find("vendorCode")) or ""

    if vendor_code and _starts_with_fpref(vendor_code):
        return vendor_code
    if offer_id and _starts_with_fpref(offer_id):
        return offer_id

    base = vendor_code or offer_id or md5(ET.tostring(offer)).hexdigest()
    return f"f{feed_index}_{base}" if base else None

async def parse_feed(session: aiohttp.ClientSession, url: str, feed_index: int) -> Tuple[bool, List[Dict[str, Any]]]:
    try:
        async with session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT_FEED) as resp:
            if resp.status != 200:
                print(f"❌ {url} — HTTP {resp.status}")
                return False, []
            content = await resp.read()
            root = ET.fromstring(content)
            offers = root.findall(".//offer")

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
    except Exception as e:
        print(f"❌ {url}: {e}")
        return False, []

# ---------------- Відправка у Prom ----------------

async def send_updates(session: aiohttp.ClientSession, batch: List[Dict[str, Any]], batch_num: int, total_batches: int) -> None:
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
        "X-LANGUAGE": "uk",
    }

    payload: List[Dict[str, Any]] = []
    for item in batch:
        obj = {"id": item["id"]}
        if item.get("price") is not None:
            obj["price"] = item["price"]
        if item.get("_presence_sure"):
            obj["presence"] = item.get("presence")
            obj["presence_sure"] = True
            obj["quantity_in_stock"] = item.get("quantity_in_stock")
        payload.append(obj)

    print(f"🔄 Партія {batch_num}/{total_batches} ({len(payload)} товарів)")
    try:
        async with session.post(API_URL, headers=headers, json=payload, timeout=REQUEST_TIMEOUT_API) as r:
            txt = await r.text()
            if r.status == 200:
                try:
                    result = json.loads(txt)
                    processed = len(result.get("processed_ids", []))
                    errors = result.get("errors", {})
                    if errors:
                        print(f"⚠️ Партія {batch_num}: оброблено {processed}/{len(payload)}, помилок: {len(errors)}")
                        for k, v in list(errors.items())[:5]:
                            print(f"  ❌ {k}: {v}")
                    else:
                        print(f"✅ Партія {batch_num}: {processed}/{len(payload)}")
                    if batch_num == 1:
                        print(f"📋 Відповідь API: {json.dumps(result, ensure_ascii=False, indent=2)[:500]}")
                except json.JSONDecodeError:
                    print(f"❌ Партія {batch_num}: не JSON відповідь: {txt[:200]}")
            else:
                print(f"❌ Партія {batch_num}: HTTP {r.status}: {txt[:200]}")
    except Exception as e:
        print(f"❌ Партія {batch_num}: {e}")

# ---------------- Головна логіка ----------------

async def main_async() -> None:
    if not API_TOKEN:
        print("❌ PROM_API_TOKEN не знайдено!")
        return
    if not os.path.exists(FEEDS_FILE):
        print(f"❌ {FEEDS_FILE} не знайдено!")
        return

    with open(FEEDS_FILE, "r", encoding="utf-8") as f:
        feed_urls = [line.strip() for line in f if line.strip()]

    old_state = load_previous_state()
    print(f"📂 Завантажено попередній стан: {len(old_state)} товарів\n")

    print("🔄 Збір даних з фідів...")
    all_products: List[Dict[str, Any]] = []
    failed: List[str] = []

    async with aiohttp.ClientSession() as session:
        tasks = [parse_feed(session, url, i + 1) for i, url in enumerate(feed_urls)]
        results = await asyncio.gather(*tasks)
        for url, (ok, products) in zip(feed_urls, results):
            if ok:
                print(f"✅ {url}: {len(products)} товарів")
                all_products.extend(products)
            else:
                failed.append(url)

    print("\n📊 Підсумок збору:")
    print(f"✅ Успішних фідів: {len(feed_urls) - len(failed)}/{len(feed_urls)}")
    print(f"📦 Загальна кількість товарів: {len(all_products)}\n")

    if failed:
        print("⚠️ Недоступні фіди:")
        for u in failed:
            print(f"  - {u}")
        if REQUIRE_ALL_FEEDS:
            print("\n🛑 ЗУПИНКА: Не всі фіди доступні! Оновлення не виконується.")
            return

    if not all_products:
        print("❌ Немає товарів!")
        return

    changed = [p for p in all_products if has_changed(p, old_state)]
    print("🔍 Аналіз змін:")
    print(f"📦 Всього товарів: {len(all_products)}")
    print(f"🔄 Змінилось: {len(changed)}\n")

    if not changed:
        print("✅ Немає змін — нічого відправляти")
        save_current_state(all_products)
        return

    total_batches = (len(changed) - 1) // BATCH_SIZE + 1
    print(f"🚀 Оновлення {len(changed)} товарів у {total_batches} партіях...")

    start = time.time()
    async with aiohttp.ClientSession() as api_sess:
        for i in range(0, len(changed), BATCH_SIZE * CONCURRENT_REQUESTS):
            jobs = []
            for j in range(CONCURRENT_REQUESTS):
                idx = i + j * BATCH_SIZE
                if idx >= len(changed):
                    break
                batch = changed[idx: idx + BATCH_SIZE]
                batch_num = idx // BATCH_SIZE + 1
                jobs.append(send_updates(api_sess, batch, batch_num, total_batches))
            await asyncio.gather(*jobs)
            await asyncio.sleep(DELAY_BETWEEN_WAVES)

    dur = time.time() - start
    save_current_state(all_products)
    print(f"\n💾 Стан збережено: {len(all_products)} товарів")
    print(f"✅ Завершено за {dur:.1f}с ({dur/60:.1f}хв)")
    if changed:
        print(f"📊 Швидкість: {len(changed)/dur:.1f} товарів/сек")

def main() -> None:
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
