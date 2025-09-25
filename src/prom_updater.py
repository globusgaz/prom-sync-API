# src/prom_updater.py
import os
import asyncio
import aiohttp
import lxml.etree as ET
import orjson
from dotenv import load_dotenv

load_dotenv()

PROM_API_TOKEN = os.getenv("PROM_API_TOKEN")
PROM_EDIT_URL = "https://my.prom.ua/api/v1/products/edit"

HEADERS = {
    "Authorization": f"Bearer {PROM_API_TOKEN}",
    "Content-Type": "application/json",
}

BATCH_SIZE = 100
MAX_CONCURRENT = 5


# ==== Завантаження фідів ====
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


# ==== Відправка батчів ====
payload_logged = False  # глобальний прапорець для логування лише 1-го payload


async def send_batch(session, batch, batch_num):
    global payload_logged
    try:
        payload = {"products": batch}

        if not payload_logged:  # показати тільки перший payload
            print("DEBUG: приклад payload до Prom:")
            print(orjson.dumps(payload, option=orjson.OPT_INDENT_2).decode())
            payload_logged = True

        async with session.post(PROM_EDIT_URL, headers=HEADERS, data=orjson.dumps(payload)) as resp:
            text = await resp.text()
            if resp.status != 200:
                print(f"⚠️ Помилка Prom {resp.status}: {text}")
            else:
                try:
                    data = orjson.loads(text)
                    updated = len(data.get("processed_ids", []))
                except Exception:
                    updated = len(batch)
                print(f"✅ Batch {batch_num} — оновлено {updated} товарів")
    except Exception as e:
        print(f"⚠️ Виняток при відправці batch {batch_num}: {e}")


async def update_products(updates):
    if not updates:
        print("🚫 Немає оновлень")
        return

    batches = [updates[i:i + BATCH_SIZE] for i in range(0, len(updates), BATCH_SIZE)]

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [send_batch(session, batch, idx + 1) for idx, batch in enumerate(batches)]
        await asyncio.gather(*tasks)


# ==== Головна логіка ====
async def main():
    offers = await load_all_feeds()
    print(f"📦 Загальна кількість товарів: {len(offers)}")

    updates = []
    for offer in offers:
        external_id = offer.get("id")
        price = offer.findtext("price")
        quantity = offer.findtext("quantity")

        if external_id and price:
            updates.append(
                {
                    "external_id": external_id,
                    "price": float(price),
                    "quantity_in_stock": int(quantity) if quantity else 0,
                    "presence": "available" if quantity and int(quantity) > 0 else "not_available",
                }
            )

    print(f"🛠️ Готово {len(updates)} оновлень")
    await update_products(updates)


if __name__ == "__main__":
    asyncio.run(main())
