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


# ==== Товари Prom ====
def get_prom_products():
    """Будуємо мапу external_id → {id, price, quantity_in_stock, presence}"""
    page = 1
    vendor_to_data = {}

    while True:
        resp = requests.get(PROM_LIST_URL, headers=HEADERS, params={"page": page, "limit": 100})
        if resp.status_code != 200:
            print(f"⚠️ Prom list error {resp.status_code}: {resp.text}")
            break

        data = resp.json()
        products = data.get("products", [])
        if not products:
            break

        for p in products:
            ext_id = str(p.get("external_id") or p.get("id"))
            if not ext_id:
                continue
            vendor_to_data[ext_id] = {
                "id": p.get("id"),
                "price": p.get("price"),
                "quantity_in_stock": p.get("quantity_in_stock"),
                "presence": p.get("presence"),
            }

        page += 1

    print(f"📥 Prom: отримано {len(vendor_to_data)} товарів")
    return vendor_to_data


# ==== Відправка батчів ====
async def send_batch(session, batch, batch_num):
    try:
        async with session.post(PROM_EDIT_URL, headers=HEADERS, data=orjson.dumps({"products": batch})) as resp:
            text = await resp.text()
            if resp.status != 200:
                print(f"⚠️ Помилка Prom у batch {batch_num}: {resp.status} {text}")
            else:
                print(f"✅ Batch {batch_num} ({len(batch)} товарів) — OK")
    except Exception as e:
        print(f"⚠️ Виняток у batch {batch_num}: {e}")


async def update_products(updates):
    if not updates:
        print("🚫 Нових оновлень від постачальників не знайдено.")
        return

    batches = [updates[i:i + BATCH_SIZE] for i in range(0, len(updates), BATCH_SIZE)]

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [send_batch(session, batch, i + 1) for i, batch in enumerate(batches)]
        await asyncio.gather(*tasks)


# ==== Головна логіка ====
async def main():
    offers = await load_all_feeds()
    print(f"📦 Загальна кількість товарів у фідах: {len(offers)}")

    vendor_to_data = get_prom_products()
    updates = []
    changes_log = []

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

            changed_fields = {}
            if prom_item["price"] != new_price:
                changed_fields["price"] = f"{prom_item['price']} → {new_price}"
            if prom_item["quantity_in_stock"] != new_quantity:
                changed_fields["quantity"] = f"{prom_item['quantity_in_stock']} → {new_quantity}"
            if prom_item["presence"] != new_presence:
                changed_fields["presence"] = f"{prom_item['presence']} → {new_presence}"

            if changed_fields:
                updates.append(
                    {
                        "external_id": vendor_code,
                        "price": new_price,
                        "quantity_in_stock": new_quantity,
                        "presence": new_presence,
                    }
                )
                changes_log.append((vendor_code, changed_fields))

    print(f"🛠️ Готово {len(updates)} оновлень")

    if changes_log:
        print("🔄 Список змінених товарів:")
        for ext_id, changes in changes_log[:20]:  # показуємо тільки перші 20 для стислості
            changes_str = ", ".join([f"{k}: {v}" for k, v in changes.items()])
            print(f"   • {ext_id}: {changes_str}")
        if len(changes_log) > 20:
            print(f"   … та ще {len(changes_log) - 20} товарів")

    await update_products(updates)


if __name__ == "__main__":
    asyncio.run(main())
