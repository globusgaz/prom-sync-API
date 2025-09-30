import os
import json
import requests
import xml.etree.ElementTree as ET

API_URL = "https://my.prom.ua/api/v1/products/edit_by_external_id"
API_TOKEN = os.getenv("PROM_API_TOKEN")

FEEDS_FILE = "feeds.txt"
BATCH_SIZE = 100  # скільки товарів відправляти за раз

def parse_feed(url):
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        root = ET.fromstring(response.content)

        for offer in root.findall(".//offer"):
            product_id = offer.get("id")
            available = offer.get("available", "false").lower()
            price_el = offer.find("price")

            # Ціна (якщо є)
            price = None
            if price_el is not None and price_el.text:
                try:
                    price = float(price_el.text.strip())
                except ValueError:
                    price = None

            # Статус наявності
            if available == "true":
                presence = "available"
                quantity_in_stock = 1
            else:
                presence = "not_available"
                quantity_in_stock = 0

            yield {
                "id": product_id,
                "price": price,
                "presence": presence,
                "quantity_in_stock": quantity_in_stock
            }
    except Exception as e:
        print(f"❌ Помилка при обробці фіду {url}: {e}")

def send_updates(batch):
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
        "X-LANGUAGE": "uk"
    }

    # Формуємо об'єкти тільки з потрібними полями
    payload = []
    for item in batch:
        obj = {"id": item["id"]}

        if item["price"] is not None:
            obj["price"] = item["price"]

        obj["presence"] = item["presence"]
        obj["quantity_in_stock"] = item["quantity_in_stock"]

        payload.append(obj)

    print(f"\n➡️ Відправляю {len(payload)} товарів:")
    print(json.dumps(payload[:3], indent=2, ensure_ascii=False), "...")

    response = requests.post(API_URL, headers=headers, json=payload)

    print(f"📥 Статус: {response.status_code}")
    try:
        print("📥 Відповідь:", response.json())
    except:
        print("📥 Відповідь (text):", response.text)

def main():
    if not API_TOKEN:
        print("❌ Токен PROM_API_TOKEN не знайдено!")
        return

    if not os.path.exists(FEEDS_FILE):
        print(f"❌ Файл {FEEDS_FILE} не знайдено!")
        return

    with open(FEEDS_FILE, "r") as f:
        feed_urls = [line.strip() for line in f if line.strip()]

    all_updates = []

    for url in feed_urls:
        print(f"🔄 Обробка фіда: {url}")
        for product in parse_feed(url):
            all_updates.append(product)

    print(f"\n✅ Зібрано {len(all_updates)} товарів для оновлення")

    for i in range(0, len(all_updates), BATCH_SIZE):
        batch = all_updates[i:i+BATCH_SIZE]
        send_updates(batch)

    print("\n✅ Оновлення завершено.")

if __name__ == "__main__":
    main()
