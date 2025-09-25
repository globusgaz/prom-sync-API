# src/prom_updater.py
import os
import asyncio
import aiohttp
import lxml.etree as ET
from dotenv import load_dotenv
import orjson

load_dotenv()

PROM_API_TOKEN = os.getenv("PROM_API_TOKEN")
PROM_EDIT_URL = "https://my.prom.ua/api/v1/products/edit"

HEADERS = {
    "Authorization": f"Bearer {PROM_API_TOKEN}",
    "Content-Type": "application/json",
}

BATCH_SIZE = 100      # скільки товарів у запиті
MAX_CONCURRENT = 5    # паралельні з'єднання


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

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_feed(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        return [offer for sublist in results for offer in sublist]


# ==== Відправка батчів ====
async def send_batch(session, batch):
    try:
        payload = {"products": batch}
        async with session.post(PROM_EDIT_URL, headers=HEADERS, data=orjson.dumps(payload)) as resp:
            text = await resp.text()
            if resp.status != 200:
                print(f"⚠️ Помилка Prom {resp.status}: {text}")
            else:
                print(f"✅ Batch {len(batch)} — OK")
    except Exception as e:
        print(f"⚠️ Виняток при відправці batch: {e}")


async def update_products(updates):
    if not updates:
        print("🚫 Немає оновлень")
        return

    batches = [updates[i:i + BATCH_SIZE] for i in range(0, len(updates), BATCH_SIZE)]

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [send_batch(session, batch) for batch in batches]
        await asyncio.gather(*tasks)


# ==== Головна логіка ====
async def main():
    offers = await load_all_feeds()
    print(f"📦 Загальна кількість товарів: {len(offers)}")

    updates = []
    for offer in offers:
        external_id = offer.get("id") or offer.findtext("vendorCode")
        price = offer.findtext("price")
        quantity = offer.findtext("quantity")

        if external_id and price:
            qty = int(quantity) if quantity else 0
            presence = "available" if qty > 0 else "not_available"

            updates.append(
                {
                    "external_id": external_id,
                    "price": float(price),
                    "quantity_in_stock": qty,
                    "presence": presence,
                }
            )

    print(f"🛠️ Готово {len(updates)} оновлень")
    await update_products(updates)


if __name__ == "__main__":
    asyncio.run(main())
