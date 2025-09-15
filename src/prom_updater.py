import os
import sys
import time
import json
import requests
import xml.etree.ElementTree as ET
from pathlib import Path

PROM_API_TOKEN = os.getenv("PROM_API_TOKEN")
PROM_BASE_URL = os.getenv("PROM_BASE_URL", "https://my.prom.ua")
PROM_UPDATE_ENDPOINT = os.getenv("PROM_UPDATE_ENDPOINT", "/api/v1/products/edit_by_external_id")

IMPORT_WAIT_SECONDS = int(os.getenv("IMPORT_WAIT_SECONDS", "600"))
BATCH_SIZE = 20  # Prom рекомендує не відправляти більше 20 товарів за раз

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {PROM_API_TOKEN}"
}


def load_feeds(feed_file="feeds.txt"):
    urls = []
    with open(feed_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                urls.append(line)
    print(f"🔗 Знайдено {len(urls)} посилань у {feed_file}")
    return urls


def parse_feed(url):
    try:
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        offers = []
        for offer in root.findall(".//offer"):
            ext_id = offer.get("id") or offer.findtext("vendorCode") or offer.findtext("sku")
            if not ext_id:
                continue

            price = offer.findtext("price")
            stock = offer.findtext("stock_quantity") or offer.findtext("quantity") or "0"
            available = offer.get("available", "true") in ["true", "1", "yes"]

            offers.append({
                "external_id": str(ext_id),
                "price": float(price) if price else None,
                "presence": "available" if available else "not_available",
                "in_stock": available,
                "quantity_in_stock": int(stock) if stock.isdigit() else None
            })
        return offers
    except Exception as e:
        print(f"❌ Помилка парсингу {url}: {e}")
        return []


def send_updates(products):
    updated = 0
    for i in range(0, len(products), BATCH_SIZE):
        batch = products[i:i + BATCH_SIZE]
        payload = {"products": []}
        for p in batch:
            data = {
                "external_id": p["external_id"]
            }
            if p.get("price") is not None:
                data["price"] = p["price"]
            if p.get("presence"):
                data["presence"] = p["presence"]
            if "in_stock" in p:
                data["in_stock"] = p["in_stock"]
            payload["products"].append(data)

        try:
            r = requests.post(
                PROM_BASE_URL + PROM_UPDATE_ENDPOINT,
                headers=HEADERS,
                data=json.dumps(payload).encode("utf-8"),
                timeout=120
            )
            if r.status_code == 200:
                resp_json = r.json()
                if "error" in resp_json:
                    print(f"⚠️ Помилка Prom: {resp_json}")
                else:
                    print(f"✅ Успішно відправлено {len(batch)} товарів")
                    updated += len(batch)
            else:
                print(f"❌ HTTP {r.status_code}: {r.text}")
        except Exception as e:
            print(f"❌ Запит не вдався: {e}")
        time.sleep(1)  # щоб уникнути 429
    return updated


def main():
    feeds = load_feeds()
    all_products = []
    for url in feeds:
        products = parse_feed(url)
        print(f"✅ {url} — {len(products)} товарів")
        all_products.extend(products)

    print(f"📦 Загальна кількість товарів: {len(all_products)}")

    if not all_products:
        print("🚫 Немає товарів для оновлення")
        sys.exit(0)

    updated = send_updates(all_products)
    print(f"🎯 Завершено. Оновлено товарів: {updated}")


if __name__ == "__main__":
    main()
