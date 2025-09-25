import os
import asyncio
import aiohttp
import lxml.etree as ET
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

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_feed(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        return [offer for sublist in results for offer in sublist]


# ==== Відправка батчів ====
async def send_batch(session, batch):
    try:
        async with session.post(PROM_EDIT_URL, headers=HEADERS, json={"products": batch}) as resp:
            text = await resp.text()
            if resp.status != 200:
                print(f"⚠️ Помилка Prom {resp.status}: {text}")
                return []
            return batch
    except Exception as e:
        print(f"⚠️ Виняток при відправці batch: {e}")
        return []


async def update_products(updates):
    if not updates:
        print("🚫 Немає оновлень")
        return []

    batches = [updates[i:i + BATCH_SIZE] for i in range(0, len(updates), BATCH_SIZE)]
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT)

    updated = []
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [send_batch(session, batch) for batch in batches]
        results = await asyncio.gather(*tasks)
        for r in results:
            if r:
                updated.extend(r)
    return updated


# ==== Головна логіка ====
async def main():
    offers = await load_all_feeds()
    print(f"📦 Загальна кількість товарів у фідах: {len(offers)}")

    updates = []
    for offer in offers:
        external_id = offer.get("id") or offer.findtext("vendorCode")
        price = offer.findtext("price")
        quantity = offer.findtext("quantity")

        if not external_id or not price:
            continue

        new_price = float(price)
        new_quantity = int(quantity) if quantity else 0
        new_presence = "available" if new_quantity > 0 else "not_available"

        updates.append(
            {
                "external_id": external_id,
                "price": new_price,
                "quantity_in_stock": new_quantity,
                "presence": new_presence,
            }
        )

    print(f"🛠️ Підготовлено {len(updates)} оновлень для Prom")

    updated = await update_products(updates)

    # ==== Звіт ====
    print("\n===== ЗВІТ =====")
    print(f"Перевірено товарів: {len(offers)}")
    print(f"Оновлено товарів: {len(updated)}")
    if updated:
        print("Список оновлених:")
        for item in updated[:20]:  # показати максимум 20 для компактності
            print(f"- {item['external_id']} | ціна={item['price']} | кількість={item['quantity_in_stock']} | {item['presence']}")
        if len(updated) > 20:
            print(f"... ще {len(updated) - 20} оновлень")
    else:
        print("Жодних змін від постачальників не виявлено.")
    print("================\n")


if __name__ == "__main__":
    asyncio.run(main())
