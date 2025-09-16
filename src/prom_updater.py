# src/prom_updater.py
import os
import asyncio
import aiohttp
import lxml.etree as ET
import json
import time
from io import BytesIO
from math import ceil
from dotenv import load_dotenv

load_dotenv()

# --------- Налаштування через ENV ----------
PROM_API_TOKEN = os.getenv("PROM_API_TOKEN", "").strip()
PROM_BASE = os.getenv("PROM_BASE_URL", "https://my.prom.ua")
PROM_LIST_PATH = os.getenv("PROM_LIST_PATH", "/api/v1/products/list")
PROM_EDIT_PATH = os.getenv("PROM_EDIT_PATH", "/api/v1/products/edit")
PROM_LIST_URL = PROM_BASE.rstrip("/") + PROM_LIST_PATH
PROM_EDIT_URL = PROM_BASE.rstrip("/") + PROM_EDIT_PATH

FEEDS_FILE = os.getenv("FEEDS_FILE", "feeds.txt")
CACHE_FILE = os.getenv("CACHE_FILE", "prom_products_cache.json")
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", str(60 * 60)))  # 1 hour default
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "6"))
PER_PAGE = int(os.getenv("PER_PAGE", "200"))  # page size for Prom list
DRY_RUN = os.getenv("DRY_RUN", "1") == "1"
TEST_MODE = os.getenv("TEST_MODE", "0") == "1"  # if set, send only first TEST_LIMIT updates
TEST_LIMIT = int(os.getenv("TEST_LIMIT", "5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

HEADERS = {
    "Authorization": f"Bearer {PROM_API_TOKEN}",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

if not PROM_API_TOKEN:
    print("❌ PROM_API_TOKEN not set (env or secrets). Exiting.")
    raise SystemExit(1)


# --------- Утиліти для XML санітайзу ----------
import re
_amp_re = re.compile(r'&(?![a-zA-Z]+;|#\d+;)')

def sanitize_text(t: str) -> str:
    if not t:
        return ""
    t = _amp_re.sub("&amp;", t)
    t = t.replace("<", "&lt;").replace(">", "&gt;")
    return t


# --------- Парсинг offers (стрімово) ----------
def parse_offer_elem(elem: ET._Element):
    """
    Приймає елемент <offer> і повертає (vendor_code, price:float|None, quantity:int|None)
    vendor_code: шукаємо vendorCode, article, sku або атрибут id
    """
    vendor_code = None
    for tag in ("vendorCode", "article", "sku"):
        v = elem.findtext(tag)
        if v and v.strip():
            vendor_code = v.strip()
            break
    if not vendor_code:
        # спроба взяти атрибут id
        vendor_code = elem.get("id") or None
    # price
    price = None
    ptext = elem.findtext("price")
    if ptext:
        try:
            price = float(ptext.strip().replace(",", "."))
        except Exception:
            price = None
    # quantity - кілька можливих полів
    qty = None
    for qtag in ("quantity", "stock_quantity", "count", "quantity_in_stock"):
        qn = elem.findtext(qtag)
        if qn and qn.strip():
            try:
                qty = int(float(qn.strip()))
            except Exception:
                qty = None
            break
    if qty is None:
        # перевіряємо атрибут available
        av = elem.get("available")
        if av is not None:
            qty = 1 if av.strip().lower() in ("true", "1", "yes", "available", "in_stock") else 0
    return vendor_code, price, qty


async def fetch_feed(session: aiohttp.ClientSession, url: str):
    """
    Асинхронно звантажує і стрімово парсить <offer> з фіду.
    Повертає list(dict) з ключами: vendor_code, price, quantity
    """
    try:
        async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=120) as resp:
            resp.raise_for_status()
            data = await resp.read()
    except Exception as e:
        print(f"❌ Помилка завантаження {url}: {e}")
        return []

    offers = []
    try:
        context = ET.iterparse(BytesIO(data), tag="offer", recover=True)
        for _, elem in context:
            vc, price, qty = parse_offer_elem(elem)
            if vc:
                offers.append({"vendor_code": vc, "price": price, "quantity": qty if qty is not None else 0})
            elem.clear()
        return offers
    except Exception as e:
        print(f"❌ Помилка парсингу {url}: {e}")
        return []


async def load_all_feeds(urls: list):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_feed(session, u) for u in urls]
        results = await asyncio.gather(*tasks)
    # Flatten
    all_offers = [item for sub in results for item in sub]
    return all_offers


# --------- Кеш: збереження/завантаження мапи products ---------
def load_cache_if_fresh(path: str, ttl: int):
    if not os.path.exists(path):
        return None
    try:
        mtime = os.path.getmtime(path)
        if time.time() - mtime > ttl:
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def save_cache(path: str, data: dict):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception as e:
        print(f"⚠️ Не вдалось зберегти кеш {path}: {e}")


# --------- Завантажуємо продукти з Prom (async, з можливістю паралелити сторінки) ----------
async def fetch_prom_page(session: aiohttp.ClientSession, page: int, per_page: int):
    params = {"page": page, "per_page": per_page}
    try:
        async with session.get(PROM_LIST_URL, headers=HEADERS, params=params, timeout=60) as resp:
            text = await resp.text()
            if resp.status != 200:
                return resp.status, None
            data = await resp.json()
            return resp.status, data
    except Exception as e:
        return 0, None


async def get_prom_products_map(use_cache=True):
    # try cache
    if use_cache:
        cached = load_cache_if_fresh(CACHE_FILE, CACHE_TTL)
        if cached:
            print(f"DEBUG: Використано кеш Prom ({len(cached)} записів)")
            return cached

    # else fetch
    async with aiohttp.ClientSession() as session:
        # first page to get total
        st, data = await fetch_prom_page(session, 1, PER_PAGE)
        if st != 200 or not data:
            print(f"⚠️ Помилка при отриманні першої сторінки Prom: status={st}")
            return {}

        products = []
        products.extend(data.get("products") or data.get("items") or [])
        # determine total pages
        total = data.get("total_count") or data.get("total") or data.get("count") or None
        if total:
            total_pages = ceil(int(total) / PER_PAGE)
        else:
            # try to fetch pages until empty (not ideal, but fallback)
            total_pages = 1
            # naive: attempt up to 200 pages
            # but better to try to get next pages until empty
            page = 2
            while True:
                st2, d2 = await fetch_prom_page(session, page, PER_PAGE)
                if st2 != 200 or not d2:
                    break
                p2 = d2.get("products") or d2.get("items") or []
                if not p2:
                    break
                products.extend(p2)
                page += 1
            total_pages = page - 1

        # if we know many pages, fetch them concurrently (pages 2..total_pages)
        if total and total_pages > 1:
            sem = asyncio.Semaphore(MAX_CONCURRENT)
            async def page_worker(pg):
                async with sem:
                    s, d = await fetch_prom_page(session, pg, PER_PAGE)
                    if s == 200 and d:
                        return d.get("products") or d.get("items") or []
                    return []

            tasks = [page_worker(p) for p in range(2, total_pages + 1)]
            pages = await asyncio.gather(*tasks)
            for p in pages:
                products.extend(p or [])

        # build map
        prom_map = {}
        for p in products:
            if not isinstance(p, dict):
                continue
            # try different keys for vendor code
            ext = None
            for k in ("sku", "external_id", "vendor_code", "vendorCode", "article"):
                v = p.get(k)
                if v:
                    ext = str(v).strip()
                    break
            if not ext:
                # maybe in identifiers
                exs = p.get("external_ids") or p.get("identifiers") or None
                if isinstance(exs, dict):
                    for k in ("external_id", "vendor_code", "article", "sku"):
                        if exs.get(k):
                            ext = str(exs.get(k)).strip()
                            break
                elif isinstance(exs, list) and exs:
                    first = exs[0]
                    if isinstance(first, dict):
                        for k in ("external_id", "code", "value"):
                            if first.get(k):
                                ext = str(first.get(k)).strip()
                                break
            if not ext:
                continue

            pid = p.get("id")
            # price extraction
            price = p.get("price")
            if price is None:
                pr_list = p.get("prices") or []
                if isinstance(pr_list, list) and pr_list:
                    try:
                        price = float(pr_list[0].get("price"))
                    except Exception:
                        price = None
            # quantity extraction (best effort)
            qty = None
            if p.get("quantity_in_stock") is not None:
                try:
                    qty = int(p.get("quantity_in_stock") or 0)
                except Exception:
                    qty = 1 if str(p.get("presence", "")).lower() != "not_available" else 0
            else:
                # presence based
                pres = p.get("presence") or p.get("in_stock")
                if isinstance(pres, bool):
                    qty = 1 if pres else 0
                elif isinstance(pres, str):
                    qty = 1 if pres.lower() in ("available", "true", "1", "in_stock") else 0
                else:
                    qty = 0

            try:
                prom_map[ext] = {
                    "id": int(pid) if pid is not None else None,
                    "price": float(price) if price is not None else 0.0,
                    "quantity": int(qty) if qty is not None else 0,
                }
            except Exception:
                continue

        # save cache
        try:
            save_cache(CACHE_FILE, prom_map)
            print(f"DEBUG: кеш Prom збережено ({len(prom_map)} записів)")
        except Exception:
            pass

        return prom_map


# --------- Побудова оновлень: тільки різниця ----------
def needs_update(prom_item, feed_price, feed_qty):
    # normalize floats for price up to 2 decimals
    try:
        prom_price = round(float(prom_item.get("price", 0.0)), 2)
    except Exception:
        prom_price = 0.0
    try:
        prom_qty = int(prom_item.get("quantity", 0) or 0)
    except Exception:
        prom_qty = 0

    if feed_price is None:
        feed_price = 0.0
    try:
        feed_price_r = round(float(feed_price), 2)
    except Exception:
        feed_price_r = 0.0
    try:
        feed_qty_i = int(feed_qty or 0)
    except Exception:
        feed_qty_i = 0

    return (feed_price_r != prom_price) or (feed_qty_i != prom_qty)


# --------- Відправка батчів асинхронно з retry ----------
async def send_batch(session: aiohttp.ClientSession, batch, batch_index):
    retries = 0
    while retries <= MAX_RETRIES:
        try:
            async with session.post(PROM_EDIT_URL, headers=HEADERS, json={"products": batch}, timeout=120) as resp:
                text = await resp.text()
                if resp.status == 200:
                    print(f"✅ Batch {batch_index} OK (items={len(batch)})")
                    return True, text
                # retry on rate-limit or server errors
                if resp.status in (429, 500, 502, 503, 504):
                    wait = 2 ** retries
                    print(f"⚠️ Batch {batch_index} status {resp.status}, retry in {wait}s")
                    await asyncio.sleep(wait)
                    retries += 1
                    continue
                # otherwise log and stop retrying
                print(f"❌ Batch {batch_index} failed status {resp.status}: {text}")
                return False, text
        except Exception as e:
            wait = 2 ** retries
            print(f"⚠️ Exception sending batch {batch_index}: {e} — retry in {wait}s")
            await asyncio.sleep(wait)
            retries += 1
    print(f"❌ Batch {batch_index} failed after {MAX_RETRIES} retries")
    return False, None


async def send_updates_async(updates):
    if not updates:
        print("🚫 Немає оновлень для відправки")
        return 0

    # apply TEST_MODE
    if TEST_MODE:
        updates = updates[:TEST_LIMIT]
        print(f"⚠️ TEST_MODE active — sending first {len(updates)} updates only")

    # chunk into batches
    batches = [updates[i:i + BATCH_SIZE] for i in range(0, len(updates), BATCH_SIZE)]
    total_sent = 0
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    async with aiohttp.ClientSession() as session:
        async def worker(idx, batch):
            async with sem:
                ok, _ = await send_batch(session, batch, idx)
                return ok, len(batch) if ok else 0

        tasks = [worker(i + 1, batch) for i, batch in enumerate(batches)]
        results = await asyncio.gather(*tasks)
        for ok, count in results:
            if ok:
                total_sent += count
    return total_sent


# --------- MAIN ----------
async def main():
    # read feeds file
    if not os.path.exists(FEEDS_FILE):
        print(f"❌ feeds file not found: {FEEDS_FILE}")
        return

    with open(FEEDS_FILE, "r", encoding="utf-8") as f:
        urls = [ln.strip() for ln in f if ln.strip() and ln.strip().startswith("http")]

    print(f"🔗 Знайдено {len(urls)} feed URL(s) in {FEEDS_FILE}")

    # load feeds concurrently
    t0 = time.time()
    offers = await load_all_feeds(urls)
    print(f"📦 Читання фідів завершено: {len(offers)} offers in {time.time()-t0:.1f}s")

    # get prom products map (use cache if fresh)
    t0 = time.time()
    prom_map = await get_prom_products_map(use_cache=True)
    print(f"📊 Prom map loaded: {len(prom_map)} items in {time.time()-t0:.1f}s")

    # build updates only for changed items
    updates = []
    sample_out = []
    for o in offers:
        vc = o.get("vendor_code")
        if not vc:
            continue
        prom_item = prom_map.get(vc)
        if not prom_item:
            # not found in prom — skip (or optionally create)
            continue
        if needs_update(prom_item, o.get("price"), o.get("quantity")):
            # form payload expected by Prom: id, price, quantity_in_stock/presence
            q = int(o.get("quantity") or 0)
            p = o.get("price")
            entry = {"id": prom_item["id"]}
            if p is not None:
                entry["price"] = round(float(p), 2)
            # include both quantity_in_stock and presence for safety
            entry["quantity_in_stock"] = q
            entry["presence"] = "available" if q > 0 else "not_available"
            entry["presence_sure"] = True
            updates.append(entry)
            if len(sample_out) < 5:
                sample_out.append({"vc": vc, "prom": prom_item, "feed_price": p, "feed_qty": q})

    print(f"🛠️ Підготовлено оновлень: {len(updates)}")
    if sample_out:
        print("🔎 Перші приклади оновлень:")
        for s in sample_out:
            print(" -", s)

    if DRY_RUN:
        print("⚙️ DRY_RUN=1 — не відправляю оновлення на Prom")
        return

    # send updates async
    t0 = time.time()
    sent = await send_updates_async(updates)
    print(f"✅ Завершено. Відправлено записів: {sent} (время {time.time()-t0:.1f}s)")


if __name__ == "__main__":
    asyncio.run(main())
