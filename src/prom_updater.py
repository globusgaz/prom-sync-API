import asyncio
import json
import os
from typing import Dict, List

import aiohttp

from src.config import get_settings
from src.feed_parser import load_urls, fetch_all_offers
from src.prom_client import PromClient
from src.change_detector import detect_changes, persist_state


def chunked(items: List[Dict], size: int) -> List[List[Dict]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def extract_updates_from_offers(offers_xml: List[str]) -> List[Dict]:
    import re

    vc_re = re.compile(r"<vendorCode>([^<]+)</vendorCode>")
    price_re = re.compile(r"<price>([^<]+)</price>")
    qty_re = re.compile(r"<(?:quantity|stock_quantity|count|quantity_in_stock)>([^<]+)</(?:quantity|stock_quantity|count|quantity_in_stock)>")
    avail_re = re.compile(r"<offer[^>]*?available=\"(true|1|yes|available|in_stock|false|0|no|out_of_stock)\"", re.IGNORECASE)

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
                quantity = 1 if ma.group(1).lower() in ("true", "1", "yes", "available", "in_stock") else 0

        update: Dict = {"external_id": vendor_code}
        if price is not None:
            update["price"] = price
        if quantity is not None:
            update["quantity"] = quantity
        updates.append(update)
    return updates


async def main_async() -> int:
    settings = get_settings()

    if not settings.prom_api_token:
        print("❌ PROM_API_TOKEN не встановлено")
        return 1

    urls = load_urls(os.path.join(os.getcwd(), "feeds.txt"))
    print(f"🔗 Знайдено {len(urls)} посилань у feeds.txt")
    if not urls:
        return 0

    # Change detection
    changed, new_state = await detect_changes(urls)
    print(f"🧭 Зміни у фідах: {'так' if changed else 'ні'}")
    if not changed and not settings.dry_run:
        print("⏭️ Змін не виявлено — пропускаємо оновлення")
        return 0

    # Отримуємо товари
    all_offers, results = await fetch_all_offers(urls)
    print(f"📦 Загальна кількість товарів (offers): {len(all_offers)}")

    updates = extract_updates_from_offers(all_offers)

    # ⚠️ Тест: змінюємо ціну для f5_40134 на 1 грн
    for upd in updates:
        if upd.get("external_id") == "f5_40134":
            print("⚠️ Тест: змінюю ціну для f5_40134 на 1.0 грн")
            upd["price"] = 1.0

    # Фільтруємо: тільки ціна та кількість
    filtered_updates = []
    for u in updates:
        item = {"external_id": u["external_id"]}
        if "price" in u:
            item["price"] = u["price"]
        if "quantity" in u:
            item["quantity"] = u["quantity"]
        filtered_updates.append(item)

    print(f"🛠️ Готуємо оновлення для Prom: {len(filtered_updates)}")
    if filtered_updates:
        print("🔎 External IDs (перші): " + ", ".join([u["external_id"] for u in filtered_updates[:20]]))

    if settings.dry_run:
        print("⚙️ DRY_RUN=1 — друк першої партії оновлень без відправки")
        print(json.dumps(filtered_updates[:10], ensure_ascii=False, indent=2))
        return 0

    client = PromClient(
        base_url=settings.prom_base_url,
        token=settings.prom_api_token,
        auth_header=settings.prom_auth_header,
        auth_scheme=settings.prom_auth_scheme,
        timeout_seconds=settings.http_timeout_seconds,
    )

    batches = chunked(filtered_updates, settings.batch_size)
    print(f"🚚 Відправляємо {len(batches)} партій")

    async with aiohttp.ClientSession() as session:
        sent = 0
        for idx, batch in enumerate(batches, start=1):
            payload = client.build_update_payload(batch)
            status, text = await client.update_products(session, settings.prom_update_endpoint, payload)
            ok = 200 <= status < 300
            print(f"[{idx}/{len(batches)}] HTTP {status} — {'OK' if ok else 'ERROR'}; items={len(batch)}")
            if not ok:
                print(text[:500])
            sent += len(batch)

    persist_state(new_state)
    print(f"✅ Завершено. Оновлено (надіслано) записів: {sent}")

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async()))
