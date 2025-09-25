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
PROM_LIST_URL = "https://my.prom.ua/api/v1/products/list"
PROM_EDIT_URL = "https://my.prom.ua/api/v1/products/edit"

HEADERS = {
    "Authorization": f"Bearer {PROM_API_TOKEN}",
    "Content-Type": "application/json",
}

BATCH_SIZE = 100
MAX_CONCURRENT = 5
MAX_PAGES = 200  # safeguard для Prom API


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

    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [fetch_feed(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        return [offer for sublist in results for offer in sublist]


# ==== Товари Prom ====
def get_prom_products():
    """Будуємо мапу sku → {id, price, quantity_in_stock, presence}"""
    page = 1
    vendor_to_data = {}

    while page <= MAX_PAGES:
        try:
            resp = requests.get(
                PROM_LIST_URL,
                headers=HEADERS,
                params={"page": page, "limit": 100},
                timeout=30,  # safeguard
            )
        except Exception as e:
            print(f"⚠️ Запит до Prom завис/помилка: {e}")
            break

        if resp.status_code != 200:
            print(f"⚠️ Prom list error {resp.status_code}: {resp.text}")
            break

        data = resp.json()
        products = data.get("products", [])
        if not products:
            break

        for p in products:
            sku = p.get("sku")
            if not sku:
                continue
            vendor_to_data[sku] = {
                "id": p.get("id"),
                "price": p.get("price"),
                "quantity_in_stock": p.get("quantity_in_stock"),
                "presence": p.get("presence"),
            }

        page += 1

    print(f"DEBUG: мапа sku->id розмір = {len(vendor_to_data)}")
    return vendor_to_data


# ==== Відправка батчів ====
async def send_batch(session, batch):
    try:
        payload = {"products": batch}
        print("DEBUG payload до Prom:")
        print(orjson.dumps(payload, option=orjson.OPT_INDENT_2).decode())

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

    timeout = aiohttp.ClientTimeout(total=60)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [send_batch(session, batch) for batch in batches]
        await asyncio.gather(*tasks)


# ==== Головна логіка ====
async def main():
    offers = await load_all_feeds()
    print(f"📦 Загальна кількість товарів: {len(offers)}")

    vendor_to_data = get_prom_products()
    updates = []

    for offer in offers:
        vendor_code = offer.get("id") or offer.findtext("vendorCode")
        price = offer.findtext("price")
        quantity = offer.findtext("quantity")

        if not vendor_code or not price:
            continue

        if vendor_code in vendor_to_data:
            prom_item = vendor_to_data[vendor_code]

            new_price = float(price)
            new_quantity = int(quantity) if quantity else 0
            new_presence = "available" if new_quantity > 0 else "not_available"

            if (
                prom_item["price"] != new_price
                or prom_item["quantity_in_stock"] != new_quantity
                or prom_item["presence"] != new_presence
            ):
                updates.append(
                    {
                        "id": prom_item["id"],
                        "price": new_price,
                        "quantity_in_stock": new_quantity,
                        "presence": new_presence,
                    }
                )

    print(f"🛠️ Готово {len(updates)} оновлень")
    await update_products(updates)


if __name__ == "__main__":
    asyncio.run(main())
