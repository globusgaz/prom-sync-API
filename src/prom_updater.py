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
    return [items[i:i + size] for i in range(0, len(items), size)]


def extract_updates_from_offers(offers_xml: List[str]) -> List[Dict]:
    """Витягуємо з XML офферів vendorCode, price, quantity"""
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


def extract_products_from_response(data: Any) -> List[Dict]:
    """Нормалізація відповіді Prom до списку товарів"""
    if not data:
        return []
    if isinstance(data, dict):
        # найчастіше Prom API повертає {"products": [...], "total_count": N}
        for k in ("products", "items", "data", "result"):
            if k in data and isinstance(data[k], list):
                return data[k]
    if isinstance(data, list):
        return data
    return []


async def build_vendor_to_id_map(client: PromClient, session: aiohttp.ClientSession, debug: bool = False) -> Dict[str, int]:
    vendor_to_id: Dict[str, int] = {}
    page = 1
    per_page = 100

    while True:
        status, data = await client.get_products(session, page=page, per_page=per_page)
        if status != 200:
            print(f"⚠️ Помилка {status} на сторінці {page}")
            break

        products = extract_products_from_response(data)

        if page == 1 and debug:
            print("DEBUG: /products/list raw keys:", list(data.keys()) if isinstance(data, dict) else type(data))
            if products:
                try:
                    print("DEBUG: приклад продукту:", json.dumps(products[0], ensure_ascii=False)[:1000])
                except Exception:
                    pass

        if not products:
            break

        for p in products:
            ext = p.get("external_id") or p.get("vendor_code") or p.get("sku")
            pid = p.get("id")
            if ext and pid:
                vendor_to_id[str(ext).strip()] = int(pid)

        total = data.get("total_count") if isinstance(data, dict) else None
        if total and page * per_page >= int(total):
            break
        page += 1

    if debug:
        print(f"DEBUG: мапа vendor->id розмір = {len(vendor_to_id)}")

    return vendor_to_id


async def main_async() -> int:
    settings = get_settings()

    urls = load_urls(os.path.join(os.getcwd(), "feeds.txt"))
    print(f"🔗 Знайдено {len(urls)} посилань у feeds.txt")
    if not urls:
        return 0

    changed, new_state = await detect_changes(urls)
    print(f"🧭 Зміни у фідах: {'так' if changed else 'ні'}")
    if not changed and not settings.dry_run:
        print("⏭️ Змін не виявлено — пропускаємо оновлення")
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
        vendor_to_id = await build_vendor_to_id_map(client, session, debug=True)
        print(f"📊 Завантажено {len(vendor_to_id)} відповідностей vendorCode → id")

        converted = []
        for u in updates:
            vc = u["vendor_code"]
            pid = vendor_to_id.get(vc)
            if not pid:
                continue
            obj = {"id": pid}
            if "price" in u:
                obj["price"] = u["price"]
            if "quantity" in u:
                obj["quantity_in_stock"] = u["quantity"]
                obj["presence"] = "available" if u["quantity"] > 0 else "not_available"
            converted.append(obj)

        print(f"🛠️ Готово {len(converted)} оновлень (не знайдено vendor_code: {len(updates) - len(converted)})")

        if not converted:
            print("🚫 Немає оновлень для відправки")
            return 0

        batches = chunked(converted, settings.batch_size)
        print(f"🚚 Відправляємо {len(batches)} партій")
        sent = 0
        for idx, batch in enumerate(batches, start=1):
            status, text = await client.update_products(session, "/api/v1/products/edit_by_external_id", batch)
            ok = 200 <= status < 300
            print(f"[{idx}/{len(batches)}] HTTP {status} — {'OK' if ok else 'ERROR'}; items={len(batch)}")
            if not ok or os.getenv("DEBUG_PROM") == "1":
                print("Відповідь Prom:", text[:1000])
            if ok:
                sent += len(batch)

        persist_state(new_state)
        print(f"✅ Завершено. Оновлено записів: {sent}")

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async()))
