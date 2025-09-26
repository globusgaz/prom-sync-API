import sys
import os
import json
import requests

API_URL = "https://my.prom.ua/api/v1/products/edit"  # правильний endpoint
TOKEN = os.getenv("PROM_API_TOKEN")

def main():
    if len(sys.argv) < 3:
        print("❌ Використання: python test_update.py <SKU> <PRICE>")
        sys.exit(1)

    sku = sys.argv[1]
    new_price = sys.argv[2]

    payload = [
        {
            "id": sku,
            "price": float(new_price),
            "quantity_in_stock": 99,
            "presence": "available",
            "presence_sure": True,
            "status": "on_display"
        }
    ]

    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
        "Accept-Language": "uk"
    }

    print("➡️ Відправляю на Prom:", json.dumps(payload, ensure_ascii=False, indent=2))

    response = requests.post(API_URL, headers=headers, json=payload)

    print(f"📥 Статус: {response.status_code}")
    try:
        print("📥 Відповідь:", response.json())
    except Exception:
        print("📥 Відповідь (raw):", response.text)


if __name__ == "__main__":
    main()
