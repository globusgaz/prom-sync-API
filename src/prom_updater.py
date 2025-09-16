# src/prom_updater.py
import os
import asyncio
import aiohttp
import requests
import orjson
import lxml.etree as ET
from dotenv import load_dotenv
from itertools import islice

load_dotenv()

PROM_API_TOKEN = os.getenv("PROM_API_TOKEN")
PROM_BASE_URL = "https://my.prom.ua/api/v1/products/edit"

HEADERS = {
    "Authorization": f"Bearer {PROM_API_TOKEN}",
    "Content-Type": "application/json",
}

# ==== ЗАВАНТАЖЕННЯ ФІДІВ ====
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


# ==== ПРОМ API ====
def get_prom_products():
    """Отримати всі продукти з Prom у форматі sku → {id, price, quantity}."""
    url = "https://my.prom.ua/api/v1/products/list"
    page = 1
    products_map = {}

    while True:
        resp = requests.get(url, headers=HEADERS, params={"page": page, "limit": 100})
        if resp.status_code != 200:
            print(f"⚠️ Prom list error {resp.status_code}: {resp.text}")
            break

        data = resp.json()
        products = data.get("products", [])
        if not products:
            break

        for p in products:
            sku = p.get("sku")
            if sku:
                products_map[sku] = {
                    "id": p.get("id"),
                    "price": float(p.get("price", 0)),
                    "quantity": int(p.get("presence", 0) != "not_available"),
                }

        page += 1

    print(f"DEBUG: завантажено {len(products_map)} товарів з Prom")
    return products_map


def chunked(iterable, size=100):
    it = iter(iterable)
    for first in it:
        yield [first, *list(islice(it, size - 1))]


def send_updates(updates):
    for i, batch in enumerate(chunked(updates, 100), start=1):
        payload = {"products": batch}
        try:
            resp = requests.post(PROM_BASE_URL, headers=HEADERS, data=orjson.dumps(payload))
            if resp.status_code == 200:
                print(f"✅ Batch {i} — OK")
            else:
                print(f"⚠️ Batch {i} error {resp.status_code}: {resp.text}")
        except Exception as e:
            print(f"❌ Batch {i} failed: {e}")


# ==== ГОЛОВНА ЛОГІКА ====
async def main():
    offers = await load_all_feeds()
    print(f"📦 Загальна кількість товарів у фідах: {len(offers)}")

    prom_products = get_prom_products()
    updates = []

    for offer in offers:
        vendor_code = offer.get("id") or offer.findtext("vendorCode")
        price = float(offer.findtext("price") or 0)
        quantity = int(offer.findtext("quantity") or 0)

        if vendor_code in prom_products:
            prom_data = prom_products[vendor_code]

            if price != prom_data["price"] or quantity != prom_data["quantity"]:
                updates.append(
                    {
                        "id": prom_data["id"],
                        "price": price,
                        "quantity": quantity,
                    }
                )

    print(f"🛠️ Готово {len(updates)} оновлень")
    if updates:
        send_updates(updates)
    else:
        print("🚫 Немає змін для відправки")


if __name__ == "__main__":
    asyncio.run(main())
