import asyncio
import json
import os
import time
from datetime import datetime
from typing import Dict, List

import aiohttp

from src.config import get_settings
from src.feed_parser import load_urls, fetch_all_offers
from src.prom_client import PromClient
from src.change_detector import detect_changes, persist_state


LOG_DIR = os.path.join(os.getcwd(), "logs")
LOG_FILE = os.path.join(LOG_DIR, "latest.log")
LOG_HISTORY_DIR = os.path.join(LOG_DIR, "history")


def write_log(text: str) -> None:
	os.makedirs(LOG_DIR, exist_ok=True)
	# overwrite latest
	with open(LOG_FILE, "w", encoding="utf-8") as f:
		f.write(text)
	# write history file and rotate
	os.makedirs(LOG_HISTORY_DIR, exist_ok=True)
	ts = datetime.now().strftime("%Y%m%d-%H%M%S")
	hist_path = os.path.join(LOG_HISTORY_DIR, f"run-{ts}.log")
	with open(hist_path, "w", encoding="utf-8") as hf:
		hf.write(text)
	# rotate
	keep = int(os.getenv("LOG_HISTORY_KEEP", "10"))
	files = sorted([p for p in os.listdir(LOG_HISTORY_DIR) if p.endswith(".log")])
	to_delete = files[:-keep] if len(files) > keep else []
	for name in to_delete:
		try:
			os.remove(os.path.join(LOG_HISTORY_DIR, name))
		except Exception:
			pass


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


def filter_updates_by_mode(updates: List[Dict], mode: str) -> List[Dict]:
    if mode == "stocks":
        return [{"external_id": u["external_id"], **({"quantity": u["quantity"]} if "quantity" in u else {})} for u in updates]
    if mode == "prices":
        return [{"external_id": u["external_id"], **({"price": u["price"]} if "price" in u else {})} for u in updates]
    return updates


async def maybe_import_new_products(settings, client: PromClient) -> None:
    if not settings.import_url:
        return
    async with aiohttp.ClientSession() as session:
        status, text = await client.trigger_import_url(session, settings.import_url)
        ok = 200 <= status < 300
        print(f"📥 Import URL: HTTP {status} — {'OK' if ok else 'ERROR'}")
        if not ok:
            print(text[:500])
            return
        try:
            data = json.loads(text)
            import_id = data.get("import_id") or data.get("id") or None
        except Exception:
            import_id = None
        wait_seconds = settings.import_wait_seconds
        print(f"⏳ Чекаємо {wait_seconds}s щоб імпорт обробився...")
        time.sleep(wait_seconds)
        if import_id:
            st, st_text = await client.get_import_status(session, str(import_id))
            print(f"📊 Import status HTTP {st}: {st_text[:200]}")


async def main_async() -> int:
    settings = get_settings()

    log_buffer: List[str] = []
    def log(msg: str) -> None:
        print(msg)
        log_buffer.append(msg)

    if not settings.prom_api_token:
        log("❌ PROM_API_TOKEN не встановлено")
        write_log("\n".join(log_buffer))
        return 1

    urls = load_urls(os.path.join(os.getcwd(), "feeds.txt"))
    log(f"🔗 Знайдено {len(urls)} посилань у feeds.txt")
    if not urls:
        write_log("\n".join(log_buffer))
        return 0

    # Change detection: only proceed if any feed changed
    changed, new_state = await detect_changes(urls)
    log(f"🧭 Зміни у фідах: {'так' if changed else 'ні'}")
    if not changed and not settings.dry_run:
        log("⏭️ Змін не виявлено — пропускаємо імпорт та оновлення")
        write_log("\n".join(log_buffer))
        return 0

    # Pre-step: import new products if configured
    await maybe_import_new_products(settings, PromClient(
        base_url=settings.prom_base_url,
        token=settings.prom_api_token,
        auth_header=settings.prom_auth_header,
        auth_scheme=settings.prom_auth_scheme,
        timeout_seconds=settings.http_timeout_seconds,
    ))

    all_offers, results = await fetch_all_offers(urls)
    log(f"📦 Загальна кількість товарів (offers): {len(all_offers)}")

    updates = extract_updates_from_offers(all_offers)

    max_items_env = os.getenv("MAX_ITEMS")
    if max_items_env:
        try:
            max_items = int(max_items_env)
            updates = updates[:max_items]
        except Exception:
            pass

    updates = filter_updates_by_mode(updates, settings.update_mode)

    log(f"🛠️ Готуємо оновлення для Prom: {len(updates)}")
    tracked = [u.get("external_id") for u in updates[:50]]
    if tracked:
        log("🔎 External IDs (перші): " + ", ".join(tracked))

    if settings.dry_run:
        log("⚙️ DRY_RUN=1 — друк першої партії оновлень без відправки")
        log(json.dumps(updates[:10], ensure_ascii=False, indent=2))
        write_log("\n".join(log_buffer))
        return 0

    client = PromClient(
        base_url=settings.prom_base_url,
        token=settings.prom_api_token,
        auth_header=settings.prom_auth_header,
        auth_scheme=settings.prom_auth_scheme,
        timeout_seconds=settings.http_timeout_seconds,
    )

    batches = chunked(updates, settings.batch_size)
    log(f"🚚 Відправляємо {len(batches)} партій")

    async with aiohttp.ClientSession() as session:
        sent = 0
        for idx, batch in enumerate(batches, start=1):
            payload = client.build_update_payload(batch)
            status, text = await client.update_products(session, settings.prom_update_endpoint, payload)
            ok = 200 <= status < 300
            log(f"[{idx}/{len(batches)}] HTTP {status} — {'OK' if ok else 'ERROR'}; items={len(batch)}")
            if not ok:
                log(text[:500])
            sent += len(batch)

    persist_state(new_state)
    log(f"✅ Завершено. Оновлено (надіслано) записів: {sent}")

    write_log("\n".join(log_buffer))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async()))
