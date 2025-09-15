# src/prom_updater.py
import os
import asyncio
import aiohttp
import requests
import orjson
import lxml.etree as ET
from dotenv import load_dotenv

load_dotenv()

PROM_API_KEY = os.getenv("PROM_API_KEY")
PROM_BASE_URL = "https://my.prom.ua/api/v1/products/edit"

HEADERS = {
    "Authorization": f"Bearer {PROM_API_KEY}",
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
        all_offers = [offer for sublist in results for offer in sublist]
        return all_offers


# ==== ПРОМ API ====
def get_prom_products():
    """Забрати всі продукти з Prom (для побудови мапи vendorCode → id)."""
    url = "https://my.prom.ua/api/v1/products/list"
    page = 1
    vendor_to_id = {}

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
            vendor_to_id[p.get("sku")] = p.get("id")

        page += 1

    print(f"DEBUG: мапа vendor->id розмір = {len(vendor_to_id)}")
    return vendor_to_id


def send_updates(updates):
    if not updates:
        print("🚫 Немає оновлень для відправки")
        return

    payload = {"products": updates}

    # Логування того, що реально шлемо
    print("DEBUG: payload до Prom:")
    print(orjson.dumps(payload, option=orjson.OPT_INDENT_2).decode())

    resp = requests.post(PROM_BASE_URL, headers=HEADERS, data=orjson.dumps(payload))
    print(f"HTTP {resp.status_code} — {resp.text}")


# ==== ГОЛОВНА ЛОГІКА ====
async def main():
    offers = await load_all_feeds()
    print(f"📦 Загальна кількість товарів: {len(offers)}")

    vendor_to_id = get_prom_products()
    updates = []

    for offer in offers:
        vendor_code = offer.get("id") or offer.findtext("vendorCode")
        price = offer.findtext("price")
        quantity = offer.findtext("quantity")

        if vendor_code in vendor_to_id:
            updates.append(
                {
                    "id": vendor_to_id[vendor_code],
                    "price": float(price) if price else None,
                    "quantity": int(quantity) if quantity else 0,
                }
            )

    print(f"🛠️ Готово {len(updates)} оновлень")
    send_updates(updates)


if __name__ == "__main__":
    asyncio.run(main())
