# test_update.py
import sys
import os
import requests
import json

def main():
    if len(sys.argv) < 3:
        print("❌ Використання: python test_update.py <sku> <price>")
        sys.exit(1)

    sku = sys.argv[1]
    price = float(sys.argv[2])
    token = os.getenv("PROM_API_TOKEN")

    if not token:
        print("❌ Немає токена PROM_API_TOKEN")
        sys.exit(1)

    url = "https://my.prom.ua/api/v1/products/edit"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept-Language": "uk"
    }

    payload = [{
        "id": sku,
        "price": price,
        "quantity_in_stock": 99,
        "presence": "available"
    }]

    print("➡️ Відправляю на Prom:", json.dumps(payload, ensure_ascii=False, indent=2))

    r = requests.post(url, headers=headers, json=payload)

    print("📥 Статус:", r.status_code)
    print("📥 Відповідь:", r.text)

if __name__ == "__main__":
    main()
