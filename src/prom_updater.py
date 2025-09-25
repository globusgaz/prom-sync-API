# src/prom_updater.py
import os
import asyncio
import aiohttp
import json
import re
from io import BytesIO
from typing import Dict, Any, List
from lxml import etree
from dotenv import load_dotenv

load_dotenv()

# ---------- Налаштування (через env або дефолти) ----------
PROM_API_TOKEN = os.getenv("PROM_API_TOKEN", "").strip()
PROM_BASE_URL = os.getenv("PROM_BASE_URL", "https://my.prom.ua").rstrip("/")
PROM_LIST_PATH = os.getenv("PROM_LIST_PATH", "/api/v1/products/list")
PROM_EDIT_PATH = os.getenv("PROM_EDIT_PATH", "/api/v1/products/edit")  # редагування по internal id
PROM_ID_CSV = os.getenv("PROM_ID_CSV")  # optional fallback CSV: vendor_code,id

FEEDS_FILE = os.getenv("FEEDS_FILE", "feeds.txt")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
MAX_CONCURRENT_REQUESTS = int(os.getenv("MAX_CONCURRENT_REQUESTS", "6"))
PER_PAGE = int(os.getenv("PROM_PER_PAGE", "100"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "60"))  # seconds

TEST_VENDOR_CODE = os.getenv("TEST_VENDOR_CODE")  # e.g. f5_40134 -> set price=1.0 for test

if not PROM_API_TOKEN:
    raise SystemExit("❌ PROM_API_TOKEN is not set in environment")

HEADERS = {
    "Authorization": f"Bearer {PROM_API_TOKEN}",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "PromUpdater/1.0",
}

# ---------- Утиліти парсингу ----------
def parse_price(text: str | None) -> float | None:
    if not text:
        return None
    try:
        t = text.strip()
        # remove currency words if any
        t = re.sub(r"[^\d,.\-]", "", t)
        t = t.replace(",", ".")
        return float(t)
    except Exception:
        return None


def parse_quantity(elem: etree._Element) -> int | None:
    # common tags
    for tag in ("quantity", "stock_quantity", "count", "quantity_in_stock"):
        node = elem.find(tag)
        if node is not None and node.text:
            try:
                return int(float(node.text.strip()))
            except Exception:
                pass
    # available attribute
    avail = elem.get("available")
    if avail is not None:
        if avail.lower() in ("true", "1", "yes", "available", "in_stock"):
            return 1
        else:
            return 0
    # fallback: none
    return None


def extract_vendor_code(elem: etree._Element) -> str | None:
    # priority: <vendorCode>, offer @id, <g:id>, <id>
    vc = elem.findtext("vendorCode")
    if vc and vc.strip():
        return vc.strip()
    attr_id = elem.get("id")
    if attr_id and attr_id.strip():
        return attr_id.strip()
    g_id = elem.findtext("{http://base.google.com/ns/1.0}id")  # g:id namespace common
    if g_id and g_id.strip():
        return g_id.strip()
    other = elem.findtext("id")
    if other and other.strip():
        return other.strip()
    return None


# ---------- Зчитування CSV fallback ----------
def try_load_csv_map(path: str) -> Dict[str, int]:
    mapping: Dict[str, int] = {}
    try:
        import csv
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            for row in reader:
                if not row:
                    continue
                # skip header heuristics
                if row[0].lower() in ("vendor_code", "external_id", "vendorcode", "article", "sku"):
                    continue
                vc = row[0].strip()
                if not vc:
                    continue
                try:
                    pid = int(row[1])
                except Exception:
                    continue
                mapping[vc] = pid
    except Exception as e:
        print(f"⚠️ Could not load CSV mapping {path}: {e}")
    return mapping


# ---------- Prom API: отримати список продуктів (пагінація) ----------
async def build_vendor_to_id_map(session: aiohttp.ClientSession) -> Dict[str, int]:
    """
    Return mapping of vendor_code-like strings -> internal Prom id (int).
    For each product we try to map multiple possible keys (external_id, sku, article, ...).
    """
    mapping: Dict[str, int] = {}
    page = 1
    per_page = PER_PAGE
    base_url = PROM_BASE_URL.rstrip("/")

    while True:
        params = {"page": page, "per_page": per_page}  # note some instances use per_page or limit; adjust if needed
        url = f"{base_url}{PROM_LIST_PATH}"
        try:
            async with session.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT) as resp:
                text = await resp.text()
                if resp.status == 401:
                    print(f"⚠️ Prom list error 401: Not authenticated. Check PROM_API_TOKEN.")
                    break
                if resp.status == 429:
                    print(f"⚠️ Prom list rate limited (429) on page {page}. Waiting 5s then retry.")
                    await asyncio.sleep(5)
                    continue
                if resp.status >= 400:
                    print(f"⚠️ Prom list error {resp.status}: {text[:300]}")
                    break

                try:
                    data = await resp.json()
                except Exception:
                    print(f"⚠️ Prom list: JSON decode failed on page {page}")
                    break

        except asyncio.TimeoutError:
            print(f"⚠️ Timeout when fetching Prom list page {page}")
            break
        except Exception as e:
            print(f"⚠️ Error requesting Prom list page {page}: {e}")
            break

        # normalize products list
        products = []
        if isinstance(data, dict):
            # try common keys
            for candidate in ("products", "items", "data", "result"):
                if candidate in data and isinstance(data[candidate], list):
                    products = data[candidate]
                    break
            if not products:
                # maybe data itself contains list somewhere
                for v in data.values():
                    if isinstance(v, list):
                        products = v
                        break
        elif isinstance(data, list):
            products = data

        if not products:
            print(f"📭 Prom: page {page} returned 0 products, stopping.")
            break

        print(f"📥 Prom: page {page} -> got {len(products)} products")

        for p in products:
            if not isinstance(p, dict):
                continue
            pid = p.get("id") or p.get("product_id") or p.get("internal_id")
            if not pid:
                continue
            # collect candidate external identifiers
            candidates = []
            for k in ("external_id", "sku", "vendor_code", "vendorCode", "article", "code"):
                v = p.get(k)
                if v:
                    candidates.append(str(v).strip())
            # also nested external identifiers
            ei = p.get("external_ids") or p.get("externalId") or p.get("identifiers")
            if isinstance(ei, dict):
                for v in ei.values():
                    if v:
                        candidates.append(str(v).strip())
            elif isinstance(ei, list):
                for x in ei:
                    if isinstance(x, dict):
                        for k in ("external_id", "code", "value"):
                            vv = x.get(k)
                            if vv:
                                candidates.append(str(vv).strip())
                    elif x:
                        candidates.append(str(x).strip())

            # map all found candidate keys to internal id
            for c in candidates:
                if c:
                    try:
                        mapping[c] = int(pid)
                    except Exception:
                        pass

        # stop if products less than per_page -> last page likely
        if len(products) < per_page:
            break
        page += 1

    # fallback CSV
    if not mapping and PROM_ID_CSV:
        print("🔁 vendor->id map empty — trying CSV fallback")
        csv_map = try_load_csv_map(PROM_ID_CSV)
        mapping.update(csv_map)
        print(f"📊 Loaded {len(csv_map)} entries from CSV")

    print(f"DEBUG: built vendor->id map size = {len(mapping)}")
    return mapping


# ---------- Відправка батчів (тут відправляємо масив JSON, не 'products':... ) ----------
async def send_batch(session: aiohttp.ClientSession, batch: List[Dict[str, Any]]):
    if not batch:
        return
    url = f"{PROM_BASE_URL}{PROM_EDIT_PATH}"
    # Prom (based on docs) expects array of objects — send JSON array
    payload = batch  # list
    # debug small sample:
    try:
        print(f"DEBUG: sending batch size={len(batch)}; example id(s): {[b.get('id') for b in batch[:5]]}")
        async with session.post(url, headers=HEADERS, json=payload, timeout=REQUEST_TIMEOUT) as resp:
            text = await resp.text()
            status = resp.status
            if 200 <= status < 300:
                # try parse json for processed_ids/errors
                try:
                    data = await resp.json()
                    # log short summary
                    if isinstance(data, dict) and ("processed_ids" in data or "errors" in data):
                        print(f"✅ Prom response summary: processed={len(data.get('processed_ids', []))}, errors keys={list(data.get('errors', {}).keys())}")
                    else:
                        print(f"✅ Batch OK (HTTP {status})")
                except Exception:
                    print(f"✅ Batch OK (HTTP {status}) - non-JSON response")
            else:
                print(f"⚠️ Prom returned HTTP {status}: {text[:1000]}")
    except asyncio.TimeoutError:
        print("⚠️ Timeout when sending batch to Prom")
    except Exception as e:
        print(f"⚠️ Exception sending batch to Prom: {e}")


# ---------- Парсинг фіду потоково і відправка оновлень по batch ----------
async def process_feed_and_update(session: aiohttp.ClientSession, feed_url: str, vendor_map: Dict[str, int]):
    """
    Парсимо feed stream-подібно, порівнюємо з vendor_map і надсилаємо оновлення батчами.
    """
    try:
        async with session.get(feed_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status >= 400:
                print(f"❌ Помилка при завантаженні {feed_url} — HTTP {resp.status}")
                return 0
            content = await resp.read()
    except Exception as e:
        print(f"❌ Помилка завантаження {feed_url}: {e}")
        return 0

    updates_batch: List[Dict[str, Any]] = []
    sent_count = 0

    # iterparse на байтах
    try:
        context = etree.iterparse(BytesIO(content), tag="offer", recover=True, encoding="utf-8")
    except Exception as e:
        print(f"❌ iterparse failed for {feed_url}: {e}")
        return 0

    for _, elem in context:
        try:
            vc = extract_vendor_code(elem)
            if not vc:
                elem.clear()
                continue

            # vendor_code might be prefixed in feed (f1_...) — mapping must match that
            prom_id = vendor_map.get(vc)
            if not prom_id:
                elem.clear()
                continue

            price_text = elem.findtext("price")
            price_val = parse_price(price_text)
            qty_val = parse_quantity(elem)
            if qty_val is None:
                # if no explicit qty, try presence attribute or availability nodes
                qty_val = 1 if (elem.get("available", "").lower() in ("true", "1", "yes", "available", "in_stock")) else 0

            new_presence = "available" if qty_val and qty_val > 0 else "not_available"

            # optional test injection
            if TEST_VENDOR_CODE and vc == TEST_VENDOR_CODE:
                print(f"⚠️ TEST injection: setting price=1.0 for {vc}")
                price_val = 1.0

            # fetch prom existing values is not stored per-id here; we rely on vendor_map only to map to id.
            # To avoid extra GET per item, we will always send price+quantity for equal comparison at Prom side.
            # If you want to skip unchanged items, you'd need to fetch and keep prom values too (costly).
            # We'll implement minimal check: only send when price present OR quantity present.
            if price_val is not None or qty_val is not None:
                obj: Dict[str, Any] = {"id": int(prom_id)}
                if price_val is not None:
                    obj["price"] = round(float(price_val), 2)
                if qty_val is not None:
                    obj["quantity_in_stock"] = int(qty_val)
                    obj["presence"] = new_presence
                    obj["presence_sure"] = True
                updates_batch.append(obj)

            # send batch when full
            if len(updates_batch) >= BATCH_SIZE:
                await send_batch(session, updates_batch)
                sent_count += len(updates_batch)
                updates_batch = []

        except Exception as e:
            print(f"⚠️ Error processing offer in {feed_url}: {e}")
        finally:
            # clear element to free memory
            try:
                elem.clear()
            except Exception:
                pass

    # send remaining
    if updates_batch:
        await send_batch(session, updates_batch)
        sent_count += len(updates_batch)

    print(f"📤 Finished feed {feed_url} — sent {sent_count} updates")
    return sent_count


# ---------- MAIN ----------
async def main():
    # read feeds list
    if not os.path.exists(FEEDS_FILE):
        print(f"❌ Feeds file {FEEDS_FILE} not found")
        return

    with open(FEEDS_FILE, "r", encoding="utf-8") as f:
        urls = [ln.strip() for ln in f if ln.strip() and ln.strip().startswith("http")]
    print(f"🔗 Found {len(urls)} feed URLs in {FEEDS_FILE}")

    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # build vendor->id map
        vendor_map = await build_vendor_to_id_map(session)
        if not vendor_map:
            print("⚠️ vendor_map is empty — nothing to update. Provide PROM_ID_CSV or check auth.")
            return

        # process feeds sequentially (to reduce parallel pressure). If you want faster, you may run them concurrently.
        total_sent = 0
        for url in urls:
            sent = await process_feed_and_update(session, url, vendor_map)
            total_sent += sent

    print(f"✅ Done. Total updates sent: {total_sent}")


if __name__ == "__main__":
    asyncio.run(main())
