# src/prom_updater.py
import asyncio
import json
import os
import random
import csv
from typing import Dict, List, Any

import aiohttp

from src.config import get_settings
from src.feed_parser import load_urls, fetch_all_offers
from src.prom_client import PromClient
from src.change_detector import detect_changes, persist_state


def chunked(items: List[Dict], size: int) -> List[List[Dict]]:
    """Розбиває список на чанки по size."""
    return [items[i:i + size] for i in range(0, len(items), size)]


def extract_updates_from_offers(offers_xml: List[str]) -> List[Dict]:
    """Парсить <offer> XML у список оновлень (vendor_code, ціна, кількість)."""
    import re

    vc_re = re.compile(r"<vendorCode>([^<]+)</vendorCode>")
    price_re = re.compile(r"<price>([^<]+)</price>")
    qty_re = re.compile(
        r"<(?:quantity|stock_quantity|count|quantity_in_stock)>([^<]+)</(?:quantity|stock_quantity|count|quantity_in_stock)>"
    )
    avail_re = re.compile(
        r"<offer[^>]*?available=\"(true|1|yes|available|in_stock|false|0|no|out_of_stock)\"",
        re.IGNORECASE,
    )

    updates: List[Dict] = []
    for xml in offers_xml:
        vendor_code = None
        m = vc_re.search(xml)
        if m:
            vendor_code = m.group(1).strip()
        if not vendor_code:
            continue

        price = None
        mp = price_re.search(xml)
        if mp:
            try:
                price = float(mp.group(1).strip().replace(",", "."))
            except Exception:
                price = None

        quantity = None
        mq = qty_re.search(xml)
        if mq:
            try:
                quantity = int(float(mq.group(1).strip()))
            except Exception:
                quantity = None
        else:
            ma = avail_re.search(xml)
            if ma:
                quantity = 1 if ma.group(1).lower() in (
                    "true", "1", "yes", "available", "in_stock"
                ) else 0

        update: Dict = {"vendor_code": vendor_code}
        if price is not None:
            update["price"] = price
        if quantity is not None:
            update["quantity"] = quantity
        updates.append(update)
    return updates


async def try_load_map_from_csv(csv_path: str) -> Dict[str, int]:
    """Fallback: завантажити мапу vendor_code -> prom_id з CSV."""
    mapping: Dict[str, int] = {}
    try:
        with open(csv_path, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            for row in reader:
                if not row:
                    continue
                if row[0].lower() in ("vendor_code", "external_id", "vendorcode", "article"):
                    continue
                vc = row[0].strip()
                try:
                    pid = int(row[1])
                except Exception:
                    continue
                if vc:
                    mapping[vc] = pid
    except Exception as e:
        print(f"⚠️ Не вдалося прочитати CSV {csv_path}: {e}")
    return mapping


def extract_products_from_response(data: Any) -> List[Dict]:
    """Нормалізує Prom-відповідь до списку продуктів."""
    if not data:
        return []
    for candidate in ("products", "items", "data", "result", "products_list"):
        if isinstance(data, dict) and candidate in data and isinstance(data[candidate], list):
            return data[candidate]
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list):
                return v
    return []


async def build_vendor_to_id_map(
    client: PromClient,
    session: aiohttp.ClientSession,
    max_pages: int = 500,
    per_page: int = 100,
    debug: bool = False,
) -> Dict[str, int]:
    """Будує мапу vendor_code → id з Prom."""
    vendor_to_id: Dict[str, int] = {}
    page = 1

    while page <= max_pages:
        status, data = await client.get_products(session, page=page, per_page=per_page)
        if status != 200:
            print(f"⚠️ GET products.list вернув {status} (сторінка {page})")
            break

        products = extract_products_from_response(data)
        if page == 1 and debug:
            print("DEBUG: /products/list raw keys:", list(data.keys()) if isinstance(data, dict) else "not dict")
            if products:
                print("DEBUG: перший продукт:", json.dumps(products[0], ensure_ascii=False)[:500])

        if not products:
            break

        for p in products:
            ext = None
            for field in ("external_id", "vendor_code", "article", "sku"):
                if isinstance(p, dict) and p.get(field):
                    ext = p.get(field)
                    break
            pid = p.get("id") if isinstance(p, dict) else None
            if ext and pid:
                vendor_to_id[str(ext).strip()] = int(pid)

        total = data.get("total_count") or data.get("total") if isinstance(data, dict) else None
        if total and page * per_page >= int(total):
            break

        page += 1

    if debug:
        print(f"DEBUG: мапа vendor->id розмір = {len(vendor_to_id)}")

    csv_path = os.getenv("PROM_ID_CSV")
    if not vendor_to_id and csv_path:
        print(f"🔁 Використовую CSV fallback: {csv_path}")
        vendor_to_id = await try_load_map_from_csv(csv_path)
        print(f"📊 Завантажено {len(vendor_to_id)} записів з CSV")

    return vendor_to_id


async def verify_some_products(
    client: PromClient,
    session: aiohttp.ClientSession,
    sample_ids: List[int],
    wanted_by_id: Dict[int, Dict],
):
    """Перевіряє випадкові товари після оновлення."""
    for pid in sample_ids:
        got = None
        for path in (f"/api/v1/products/{pid}", f"/products/{pid}", f"/api/v1/products/get/{pid}"):
            url = f"{client.base_url}{path}"
            try:
                async with session.get(url, headers=client._headers(), timeout=client.timeout_seconds) as resp:
                    if resp.status == 200:
                        try:
                            got = await resp.json()
                        except Exception:
                            got = None
                        break
            except Exception:
                got = None
        if not got:
            print(f"❌ Verify {pid}: неможливо отримати товар")
            continue
        remote_price = got.get("price")
        remote_qty = got.get("quantity_in_stock") or got.get("quantity")
        expected = wanted_by_id.get(pid)
        print(f"🔎 Verify id={pid}: remote_price={remote_price}, remote_qty={remote_qty} | expected={expected}")


async def main_async() -> int:
    settings = get_settings()

    if not settings.prom_api_token:
        print("❌ PROM_API_TOKEN не встановлено")
        return 1

    urls = load_urls(os.path.join(os.getcwd(), "feeds.txt"))
    print(f"🔗 Знайдено {len(urls)} посилань у feeds.txt")
    if not urls:
        return 0

    changed, new_state = await detect_changes(urls)
    print(f"🧭 Зміни у фідах: {'так' if changed else 'ні'}")
    if not changed and not settings.dry_run:
        print("⏭️ Змін не виявлено — пропускаю")
        return 0

    all_offers, _ = await fetch_all_offers(urls)
    print(f"📦 Загальна кількість товарів: {len(all_offers)}")

    updates = extract_updates_from_offers(all_offers)

    client = PromClient(
        base_url=settings.prom_base_url,
        token=settings.prom_api_token,
        auth_header=settings.prom_auth_header,
        auth_scheme=settings.prom_auth_scheme,
        timeout_seconds=settings.http_timeout_seconds,
    )

    async with aiohttp.ClientSession() as session:
        debug = os.getenv("DEBUG_PROM", "0") == "1"
        vendor_to_id = await build_vendor_to_id_map(client, session, debug=debug)

        print(f"📊 Завантажено {len(vendor_to_id)} відповідностей vendorCode → id")

        converted = []
        wanted_by_id = {}
        missing = 0
        for u in updates:
            vc = u["vendor_code"]
            pid = vendor_to_id.get(vc)
            if not pid:
                missing += 1
                continue
            obj = {"id": pid}
            if "price" in u:
                obj["price"] = u["price"]
            if "quantity" in u:
                obj["quantity_in_stock"] = u["quantity"]
                obj["presence"] = "available" if u["quantity"] > 0 else "not_available"
            converted.append(obj)
            wanted_by_id[pid] = {
                "price": obj.get("price"),
                "quantity_in_stock": obj.get("quantity_in_stock"),
            }

        print(f"🛠️ Готово {len(converted)} оновлень (не знайдено vendor_code: {missing})")

        if not converted:
            print("🚫 Немає оновлень для відправки")
            return 0

        batches = chunked(converted, settings.batch_size)
        print(f"🚚 Відправляю {len(batches)} партій")
        sent = 0
        for idx, batch in enumerate(batches, start=1):
            status, text = await client.update_products(session, "/products/edit", batch)
            ok = 200 <= status < 300
            print(f"[{idx}/{len(batches)}] HTTP {status} — {'OK' if ok else 'ERROR'}; items={len(batch)}")
            if debug or not ok:
                print("Відповідь Prom:", (text[:500] if text else "<empty>"))
            if ok:
                sent += len(batch)

        persist_state(new_state)
        print(f"✅ Завершено. Успішно оновлено {sent} товарів")

        sample_size = int(os.getenv("VERIFY_SAMPLE", "5"))
        sample_ids = random.sample(list(wanted_by_id.keys()), min(sample_size, len(wanted_by_id)))
        print(f"🔁 Перевірка випадкових {len(sample_ids)} товарів: {sample_ids}")
        await verify_some_products(client, session, sample_ids, wanted_by_id)

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async()))
