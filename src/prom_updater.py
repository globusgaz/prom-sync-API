import asyncio
import json
import os
import time
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

import aiohttp
import xml.etree.ElementTree as ET

from src.config import get_settings
from src.prom_client import PromClient

# -------------------------------
# Налаштування та константи
# -------------------------------

REQUEST_TIMEOUT_FEED = aiohttp.ClientTimeout(total=120)
API_DELAY_BETWEEN_BATCHES_SEC = 0.15
STATE_FILE = "product_state.json"

# Заголовки для фідів (імітуємо браузер)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Safari/537.36"
    ),
    "Accept": "application/xml, text/xml, */*",
    "Accept-Language": "uk-UA,uk;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


# -------------------------------
# Допоміжні функції
# -------------------------------

def _starts_with_fpref(x: str) -> bool:
    """Перевіряє префікс формату fN_ (наприклад, f1_, f4_ ...)"""
    return len(x) >= 3 and x[0] == "f" and x[1].isdigit() and x[2] == "_"


def _text_of(elem: Optional[ET.Element]) -> str:
    """Безпечне отримання тексту з XML-елемента"""
    return (elem.text or "").strip() if elem is not None else ""


def _build_external_id(offer: ET.Element, feed_index: int) -> Optional[str]:
    """
    Будує external_id ідентично YML-генератору:
    - Якщо vendorCode або offer/@id вже має префікс fN_ — використовуємо як є
    - Інакше додаємо f{feed_index}_ до (vendorCode | offer/@id | md5(offer))
    """
    offer_id = offer.get("id") or ""
    vendor_code = _text_of(offer.find("vendorCode")) or ""

    if vendor_code and _starts_with_fpref(vendor_code):
        return vendor_code
    if offer_id and _starts_with_fpref(offer_id):
        return offer_id

    base = vendor_code or offer_id or hashlib.md5(ET.tostring(offer)).hexdigest()
    return f"f{feed_index}_{base}" if base else None


def _extract_price(offer: ET.Element) -> Optional[float]:
    """Витягує ціну з тегу <price>"""
    node = offer.find("price")
    if node is None or node.text is None:
        return None
    raw = node.text.strip().replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return None


def _infer_availability(offer: ET.Element) -> Tuple[bool, int, bool]:
    """
    Визначає наявність.
    Повертає кортеж: (presence, quantity_in_stock, sure)

    Логіка:
    - Якщо атрибут offer/@available заданий — використовуємо його (sure=True)
    - Або якщо є числові кількісні теги — використовуємо їх (sure=True)
    - Або якщо є явні теги presence/in_stock/available — інтерпретуємо (sure=True)
    - Інакше не шлемо нічого про наявність (sure=False), щоб не зіпсувати стан
    """
    # Атрибут available
    available_attr = offer.get("available")
    if available_attr is not None:
        val = available_attr.strip().lower()
        is_on = val in ("true", "1", "yes", "available", "in_stock")
        return is_on, (1 if is_on else 0), True

    # Явні теги кількості
    for tag in ("quantity", "stock_quantity", "count", "quantity_in_stock"):
        node = offer.find(tag)
        if node is not None and node.text:
            try:
                qty = int(float(node.text.strip()))
                return (qty > 0), qty, True
            except ValueError:
                pass

    # Явні теги стану
    for tag in ("presence", "in_stock", "available"):
        node = offer.find(tag)
        if node is not None and node.text:
            val = node.text.strip().lower()
            if val in ("true", "1", "yes", "available", "in_stock"):
                return True, 1, True
            if val in ("false", "0", "no", "out_of_stock", "not_available"):
                return False, 0, True

    # Немає чітких сигналів
    return True, 1, False


# -------------------------------
# Робота з фідами
# -------------------------------

async def _fetch_content(session: aiohttp.ClientSession, url: str) -> Optional[bytes]:
    """Завантажує вміст фіда, повертає None при помилці."""
    try:
        async with session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT_FEED) as resp:
            if resp.status != 200:
                print(f"❌ {url} — HTTP {resp.status}")
                return None
            return await resp.read()
    except Exception as e:
        print(f"❌ {url}: {e}")
        return None


def _parse_offers(xml_content: bytes) -> List[ET.Element]:
    """Парсить XML і повертає список <offer>."""
    try:
        root = ET.fromstring(xml_content)
        return root.findall(".//offer")
    except ET.ParseError:
        return []


async def parse_feed(session: aiohttp.ClientSession, url: str, feed_index: int) -> Tuple[bool, List[Dict[str, Any]]]:
    """
    Парсить один фід і повертає (ok, products):
    product = { id, price?, presence?, quantity_in_stock?, _presence_sure }
    """
    content = await _fetch_content(session, url)
    if content is None:
        return False, []

    offers = _parse_offers(content)
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
            # Не шлемо наявність, якщо сигнал нечіткий
            item["_presence_sure"] = False

        products.append(item)

    return True, products


# -------------------------------
# Відправка оновлень у Prom
# -------------------------------

def _build_payload(batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Готує payload для /api/v1/products/edit_by_external_id.
    Включаємо тільки підтверджені поля.
    """
    payload: List[Dict[str, Any]] = []
    for p in batch:
        row: Dict[str, Any] = {"id": p["id"]}
        if "price" in p:
            row["price"] = p["price"]
        if p.get("_presence_sure"):
            row["presence"] = p["presence"]
            row["quantity_in_stock"] = p["quantity_in_stock"]
            row["presence_sure"] = True
        payload.append(row)
    return payload


async def _send_batches(session: aiohttp.ClientSession, client: PromClient, products: List[Dict[str, Any]], batch_size: int) -> None:
    """Надсилає оновлення батчами з невеликою паузою для обмеження RPS."""
    batches = [products[i:i + batch_size] for i in range(0, len(products), batch_size)]
    total = len(batches)
    for idx, batch in enumerate(batches, start=1):
        payload = _build_payload(batch)
        print(f"🔄 Партія {idx}/{total} ({len(batch)} товарів)")
        try:
            status, text = await client.update_products(session, "/api/v1/products/edit_by_external_id", payload)
        except Exception as e:
            print(f"❌ Партія {idx}: помилка запиту — {e}")
            await asyncio.sleep(API_DELAY_BETWEEN_BATCHES_SEC)
            continue

        if 200 <= status < 300:
            print(f"✅ Партія {idx}: OK")
        else:
            print(f"❌ Партія {idx}: HTTP {status}")
            try:
                data = json.loads(text)
                errors = data.get("errors") or {}
                err_items = list(errors.items())
                if err_items:
                    print("  🔎 Перші помилки:")
                    for j, (pid, err) in enumerate(err_items[:10], start=1):
                        print(f"   {j:>2}. {pid}: {err}")
                    rest = max(0, len(err_items) - 10)
                    if rest:
                        print(f"   ... та ще {rest} помилок")
            except json.JSONDecodeError:
                print(f"📋 Відповідь: {text[:300]}")

        if idx < total:
            await asyncio.sleep(API_DELAY_BETWEEN_BATCHES_SEC)


# -------------------------------
# Збереження та порівняння стану
# -------------------------------

def _load_prev_state(path: str) -> Dict[str, Dict[str, Any]]:
    """Завантажує попередній стан external_id -> дані."""
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(path: str, state: Dict[str, Dict[str, Any]]) -> None:
    """Зберігає поточний стан external_id -> дані."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _calc_changes(current: Dict[str, Dict[str, Any]], prev: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Повертає лише змінені/нові товари для скорочення API-викликів."""
    changed: List[Dict[str, Any]] = []
    for eid, cur in current.items():
        old = prev.get(eid)
        if not old:
            changed.append(cur)
            continue
        if cur.get("price") != old.get("price"):
            changed.append(cur)
            continue
        # Перевірка наявності тільки якщо ми її відправляємо
        if cur.get("_presence_sure"):
            if (cur.get("presence") != old.get("presence")
                or cur.get("quantity_in_stock") != old.get("quantity_in_stock")):
                changed.append(cur)
    return changed


# -------------------------------
# Основний сценарій
# -------------------------------

async def main_async() -> int:
    settings = get_settings()
    if not settings.prom_api_token:
        print("❌ PROM_API_TOKEN не встановлено")
        return 1

    feeds_file = os.path.join(os.getcwd(), "feeds.txt")
    if not os.path.exists(feeds_file):
        print(f"❌ Файл {feeds_file} не знайдено")
        return 1

    with open(feeds_file, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip().startswith("http")]

    print(f"🔗 Знайдено {len(urls)} фідів")

    # Збір з фідів
    print("🔄 Збір даних з фідів...")
    all_products: List[Dict[str, Any]] = []
    ok_count = 0

    async with aiohttp.ClientSession() as session:
        for i, url in enumerate(urls, start=1):
            ok, products = await parse_feed(session, url, i)
            if ok:
                ok_count += 1
                all_products.extend(products)
                print(f"✅ {url}: {len(products)} товарів")
            else:
                print(f"❌ {url}: помилка")

    print("\n📊 Підсумок збору:")
    print(f"✅ Успішних фідів: {ok_count}/{len(urls)}")
    print(f"📦 Загальна кількість товарів: {len(all_products)}")

    if not all_products:
        print("⚠️ Немає товарів для оновлення")
        return 0

    # Обчислення змін
    prev_state = _load_prev_state(STATE_FILE)
    current_state: Dict[str, Dict[str, Any]] = {p["id"]: p for p in all_products}
    changed = _calc_changes(current_state, prev_state)

    print("\n🔍 Аналіз змін:")
    print(f"📦 Всього товарів: {len(all_products)}")
    print(f"🔄 Змінилось: {len(changed)}")
    print(f"✅ Без змін: {len(all_products) - len(changed)}")

    if not changed:
        print("⏭️ Змін не виявлено — пропускаємо оновлення")
        return 0

    # Відправка змін
    client = PromClient(
        base_url=settings.prom_base_url,
        token=settings.prom_api_token,
        auth_header=settings.prom_auth_header,
        auth_scheme=settings.prom_auth_scheme,
        timeout_seconds=settings.http_timeout_seconds,
    )

    async with aiohttp.ClientSession() as session:
        await _send_batches(session, client, changed, getattr(settings, "batch_size", 100))

    # Збереження стану
    _save_state(STATE_FILE, current_state)
    print("✅ Оновлення завершено")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async()))
