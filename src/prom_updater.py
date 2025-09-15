# src/prom_updater.py
import os
import asyncio
import aiohttp
import requests
import orjson
import lxml.etree as ET
from dotenv import load_dotenv

load_dotenv()

PROM_API_TOKEN = os.getenv("PROM_API_TOKEN")
PROM_BASE_URL = "https://my.prom.ua/api/v1/products/edit_by_external_id"

HEADERS = {
    "Authorization": f"Bearer {PROM_API_TOKEN}",
    "Content-Type": "application/json",
}

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

# ==== Prom API ====
def send_updates(updates):
    if not updates:
        print("🚫 Немає оновлень для відправки")
        return

    payload = {"products": updates}
    print("DEBUG: payload до Prom:")
    print(orjson.dumps(payload, option=orjson.OPT_INDENT_2).decode())

    resp = requests.post(PROM_BASE_URL, headers=HEADERS, data=orjson.dumps(payload))
    print(f"HTTP {resp.status_code} — {resp.text}")

# ==== Головна логіка ====
async def main():
    offers = await load_all_feeds()
    print(f"📦 Загальна кількість товарів: {len(offers)}")

    updates = []
    for offer in offers:
        external_id = offer.get("id") or offer.findtext("vendorCode")
        price = offer.findtext("price")
        quantity = offer.findtext("quantity")

        if external_id:
            updates.append(
                {
                    "external_id": external_id,
                    "price": float(price) if price else None,
                    "quantity": int(quantity) if quantity else 0,
                }
            )

    print(f"🛠️ Готово {len(updates)} оновлень")
    send_updates(updates)

if __name__ == "__main__":
    asyncio.run(main())
