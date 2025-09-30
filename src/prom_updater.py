import os
import asyncio
import aiohttp
import lxml.etree as ET
import orjson
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

PROM_API_TOKEN = os.getenv("PROM_API_TOKEN")
PROM_EDIT_URL = "https://my.prom.ua/api/v1/products/edit_by_external_id"

HEADERS = {
    "Authorization": f"Bearer {PROM_API_TOKEN}",
    "Content-Type": "application/json",
    "Accept-Language": "uk",
}

BATCH_SIZE = 100
MAX_CONCURRENT = 5
LOG_FILE = "prom_update.log"


def log_to_file(message: str):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()} {message}\n")


async def fetch_feed(session, url: str):
    try:
        async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as resp:
            resp.raise_for_status()
            text = await resp.text()
            root = ET.fromstring(text.encode("utf-8"))
            offers = root.findall(".//offer")
            print(f"✅ {url} — {len(offers)} товарів")
            return offers
    except Exception as e:
        print(f"❌ Помилка парсингу {url}: {e}")
        return []


async def load_all_feeds(file_path="feeds.txt"):
    with open(file_path, "r") as f:
        urls = [line.strip() for line in f if line.strip()]

    print(f"🔗 Found {len(urls)} feed URLs in {file_path}")
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_feed(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        return [offer for sublist in results for offer in sublist]


async def send_batch(session, batch, stats):
    try:
        async with session.post(
            PROM_EDIT_URL,
            headers=HEADERS,
            data=orjson.dumps(batch)
        ) as resp:
            text = await resp.text()

            if resp.status != 200:
                print(f"⚠️ Помилка Prom {resp.status}: {text}")
                stats["errors"].append({"status": resp.status, "text": text[:200]})
                return

            try:
                data = orjson.loads(text)
                processed = data.get("processed_ids", [])
                errors = data.get("errors", {})

                stats["updated"] += len(processed)
                if errors:
                    stats["errors"].append(errors)

            except Exception:
                print(f"⚠️ Не вдалося розпарсити відповідь: {text[:200]}")
                stats["errors"].append({"parse_error": text[:200]})

    except Exception as e:
        print(f"⚠️ Виняток при відправці batch: {e}")
        stats["errors"].append({"exception": str(e)})


async def update_products(updates):
    if not updates:
        print("🚫 Немає оновлень")
        return {"checked": 0, "updated": 0, "errors": []}

    batches = [updates[i:i + BATCH_SIZE] for i in range(0, len(updates), BATCH_SIZE)]

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT)
    stats = {"checked": len(updates), "updated": 0, "errors": []}

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [send_batch(session, batch, stats) for batch in batches]
        await asyncio.gather(*tasks)

    return stats


async def main():
    offers = await load_all_feeds()
    print(f"📦 Загальна кількість товарів у фідах: {len(offers)}")

    updates = []
    for offer in offers:
        external_id = offer.get("id")
        price = offer.findtext("price")
        available = offer.get("available", "false").lower() == "true"

        if not external_id:
            continue

        update_item = {"id": external_id}

        if available:
            if price:
                try:
                    update_item["price"] = float(price)
                except ValueError:
                    pass
            update_item["presence"] = "available"
            update_item["status"] = "on_display"
        else:
            update_item["presence"] = "not_available"
            update_item["status"] = "draft"
            update_item["quantity_in_stock"] = 0

        updates.append(update_item)

    print(f"🛠️ Підготовлено {len(updates)} оновлень для Prom")

    stats = await update_products(updates)

    report = [
        "===== ЗВІТ =====",
        f"Перевірено товарів: {stats['checked']}",
        f"Оновлено товарів: {stats['updated']}",
    ]

    if stats["errors"]:
        report.append(f"⚠️ Помилки: {len(stats['errors'])}")
        for err in stats["errors"][:10]:
            report.append(f"  - {err}")

    report.append("================")
    report_text = "\n".join(report)

    print(report_text)
    log_to_file(report_text)


if __name__ == "__main__":
    asyncio.run(main())
