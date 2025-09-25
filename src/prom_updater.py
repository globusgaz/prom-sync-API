# src/prom_updater.py
import os
import time
import json
import asyncio
import aiohttp
import requests
import orjson
import lxml.etree as ET
from dotenv import load_dotenv
from typing import Dict, List, Any, Optional

load_dotenv()

# --- Конфіг ---
PROM_API_TOKEN = os.getenv("PROM_API_TOKEN")  # MUST be set
PROM_LIST_URL = os.getenv("PROM_LIST_URL", "https://my.prom.ua/api/v1/products/list")
PROM_EDIT_URL = os.getenv("PROM_EDIT_URL", "https://my.prom.ua/api/v1/products/edit")

HEADERS = {
    "Authorization": f"Bearer {PROM_API_TOKEN}" if PROM_API_TOKEN else "",
    "Content-Type": "application/json",
}

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "5"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "60"))  # seconds per HTTP call
RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", "3"))
CACHE_FILE = os.getenv("PROM_CACHE_FILE", "prom_cache.json")
CACHE_TTL = int(os.getenv("PROM_CACHE_TTL", "3600"))  # seconds
DEBUG_PROM = os.getenv("DEBUG_PROM", "0") == "1"
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"


# -------------------- Утиліти кешу --------------------
def load_prom_cache(path: str, ttl: int) -> Optional[Dict[str, Any]]:
    try:
        if not os.path.exists(path):
            return None
        if (time.time() - os.path.getmtime(path)) > ttl:
            return None
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as e:
        print(f"⚠️ Помилка читання кешу {path}: {e}")
        return None


def save_prom_cache(path: str, data: Dict[str, Any]) -> None:
    try:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False)
    except Exception as e:
        print(f"⚠️ Помилка запису кешу {path}: {e}")


# -------------------- Завантаження фідів --------------------
async def fetch_feed(session: aiohttp.ClientSession, url: str):
    try:
        async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
            resp.raise_for_status()
            text = await resp.text()
            root = ET.fromstring(text.encode("utf-8"))
            offers = root.findall(".//offer")
            print(f"✅ {url} — {len(offers)} товарів")
            return offers
    except Exception as e:
        print(f"❌ Помилка парсингу {url}: {e}")
        return []


async def load_all_feeds(file_path="feeds.txt") -> List[ET._Element]:
    urls: List[str] = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                v = line.strip()
                if v:
                    urls.append(v)
    except FileNotFoundError:
        print(f"❌ Файл {file_path} не знайдено")
        return []

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_feed(session, url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        offers: List[ET._Element] = []
        for r in results:
            offers.extend(r)
        return offers


# -------------------- Отримати товари з Prom (мапа) --------------------
def get_prom_products() -> Dict[str, Dict[str, Any]]:
    """
    Повертає dict: sku -> {"id": int, "price": float|None, "quantity_in_stock": int|None, "presence": str|None}
    Використовує кеш 'prom_cache.json' якщо він свіжий.
    """
    # Спроба з кешу
    cached = load_prom_cache(CACHE_FILE, CACHE_TTL)
    if cached:
        if DEBUG_PROM:
            print(f"DEBUG: завантажено кеш Prom ({len(cached)} записів) з {CACHE_FILE}")
        return cached

    vendor_to_data: Dict[str, Dict[str, Any]] = {}
    page = 1
    while True:
        # retry loop для сторінки
        resp_json = None
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                r = requests.get(PROM_LIST_URL, headers=HEADERS, params={"page": page, "limit": 100}, timeout=REQUEST_TIMEOUT)
                if r.status_code == 429:
                    backoff = 2 ** (attempt - 1)
                    print(f"⚠️ Prom LIST 429, сторінка {page}, спроба {attempt}/{RETRY_ATTEMPTS}, чекаю {backoff}s")
                    time.sleep(backoff)
                    continue
                if r.status_code != 200:
                    print(f"⚠️ Prom list error {r.status_code}: {r.text[:200]}")
                    resp_json = {}  # зупинимося
                    break
                resp_json = r.json()
                break
            except requests.RequestException as e:
                backoff = 2 ** (attempt - 1)
                print(f"⚠️ Помилка GET products.list (сторінка {page}) attempt {attempt}: {e} — чекаю {backoff}s")
                time.sleep(backoff)
        if not resp_json:
            break

        # Нормалізація відповіді
        products = []
        if isinstance(resp_json, dict):
            # типова структура: {"products": [...], "total_count": N}
            if "products" in resp_json and isinstance(resp_json["products"], list):
                products = resp_json["products"]
            else:
                # пошук першого списку
                for v in resp_json.values():
                    if isinstance(v, list):
                        products = v
                        break
        elif isinstance(resp_json, list):
            products = resp_json

        if not products:
            break

        for p in products:
            sku = p.get("sku") or p.get("external_id") or p.get("vendor_code") or p.get("article")
            if not sku:
                continue
            vendor_to_data[str(sku).strip()] = {
                "id": p.get("id"),
                "price": p.get("price"),
                "quantity_in_stock": p.get("quantity_in_stock"),
                "presence": p.get("presence"),
            }

        page += 1

    # збережемо кеш
    try:
        save_prom_cache(CACHE_FILE, vendor_to_data)
        if DEBUG_PROM:
            print(f"DEBUG: збережено кеш Prom ({len(vendor_to_data)}) -> {CACHE_FILE}")
    except Exception:
        pass

    print(f"DEBUG: мапа sku->id розмір = {len(vendor_to_data)}")
    return vendor_to_data


# -------------------- Надіслати батч (з retry + timeout) --------------------
async def send_batch(session: aiohttp.ClientSession, batch: List[Dict[str, Any]], sem: asyncio.Semaphore, batch_idx: int) -> bool:
    """Повертає True якщо OK, False інакше."""
    async with sem:
        # Тільки debug: покажемо невелику вибірку payload (щоб не друкувати 40k)
        if DEBUG_PROM:
            try:
                sample = orjson.dumps({"products_sample": batch[:10]}, option=orjson.OPT_INDENT_2).decode()
                print(f"[batch {batch_idx}] DEBUG sample payload:\n{sample}")
            except Exception:
                pass
        else:
            print(f"[batch {batch_idx}] Надсилаю {len(batch)} товарів...")

        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                # виконуємо POST з таймаутом для кожного виклику
                async with session.post(PROM_EDIT_URL, headers=HEADERS, json={"products": batch}, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                    text = await resp.text()
                    status = resp.status
                    if 200 <= status < 300:
                        print(f"[batch {batch_idx}] ✅ OK ({len(batch)})")
                        return True
                    # ретраїмо на 429/5xx
                    if status == 429 or 500 <= status < 600:
                        backoff = 2 ** (attempt - 1)
                        print(f"[batch {batch_idx}] transient HTTP {status}, attempt {attempt}/{RETRY_ATTEMPTS}, backoff {backoff}s")
                        await asyncio.sleep(backoff)
                        continue
                    # інші статуси — не ретраїмо
                    print(f"[batch {batch_idx}] HTTP {status} — {text[:400]}")
                    return False
            except asyncio.TimeoutError:
                backoff = 2 ** (attempt - 1)
                print(f"[batch {batch_idx}] timeout attempt {attempt}/{RETRY_ATTEMPTS}, backoff {backoff}s")
                await asyncio.sleep(backoff)
            except Exception as e:
                backoff = 2 ** (attempt - 1)
                print(f"[batch {batch_idx}] exception attempt {attempt}: {e}, backoff {backoff}s")
                await asyncio.sleep(backoff)
        print(f"[batch {batch_idx}] FAILED after {RETRY_ATTEMPTS} attempts")
        return False


async def update_products(updates: List[Dict[str, Any]]) -> None:
    if not updates:
        print("🚫 Немає оновлень")
        return

    batches = [updates[i : i + BATCH_SIZE] for i in range(0, len(updates), BATCH_SIZE)]
    sem = asyncio.Semaphore(MAX_CONCURRENT)

    conn = aiohttp.TCPConnector(limit_per_host=MAX_CONCURRENT)
    timeout_total = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT * 2 + 10)
    async with aiohttp.ClientSession(connector=conn, timeout=timeout_total) as session:
        tasks = [asyncio.create_task(send_batch(session, batch, sem, idx + 1)) for idx, batch in enumerate(batches)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    ok = sum(1 for r in results if r is True)
    failed = len(batches) - ok
    print(f"Відправлено батчів: {len(batches)}; успішно: {ok}; неуспішно: {failed}")


# -------------------- Головна логіка --------------------
async def main():
    if not PROM_API_TOKEN:
        print("❌ PROM_API_TOKEN не встановлено в оточенні")
        return

    offers = await load_all_feeds()
    print(f"📦 Загальна кількість товарів (offers): {len(offers)}")

    vendor_to_data = get_prom_products()
    updates: List[Dict[str, Any]] = []

    # Формуємо оновлення тільки для товарів, де є розбіжність
    missing_vc = 0
    for offer in offers:
        # vendor_code може бути в атрибуті id або в тегу vendorCode
        vendor_code = (offer.get("id") or offer.findtext("vendorCode") or "").strip()
        price_text = (offer.findtext("price") or "").strip()
        qty_text = (offer.findtext("quantity") or "").strip()

        if not vendor_code or not price_text:
            continue

        try:
            new_price = float(price_text.replace(",", "."))
        except Exception:
            continue
        try:
            new_qty = int(float(qty_text)) if qty_text else 0
        except Exception:
            new_qty = 0

        prom_item = vendor_to_data.get(vendor_code)
        if not prom_item:
            missing_vc += 1
            continue

        prom_price = prom_item.get("price")
        prom_qty = prom_item.get("quantity_in_stock")
        prom_presence = prom_item.get("presence")
        new_presence = "available" if new_qty > 0 else "not_available"

        price_changed = (prom_price is None) or (float(prom_price) != float(new_price))
        qty_changed = (prom_qty is None) or (int(prom_qty) != int(new_qty))
        presence_changed = (prom_presence != new_presence)

        if price_changed or qty_changed or presence_changed:
            updates.append({
                "id": prom_item["id"],
                "price": new_price,
                "quantity_in_stock": new_qty,
                "presence": new_presence,
            })

    print(f"🛠️ Готово {len(updates)} оновлень (не знайдено vendor_code: {missing_vc})")

    if DRY_RUN:
        print("⚙️ DRY_RUN=1 — зразок перших оновлень:")
        print(orjson.dumps(updates[:10], option=orjson.OPT_INDENT_2).decode() if updates else "[]")
        return

    await update_products(updates)


if __name__ == "__main__":
    asyncio.run(main())
