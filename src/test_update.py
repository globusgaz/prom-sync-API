import os
import requests
import json
import sys

API_TOKEN = os.getenv("PROM_API_TOKEN")

if len(sys.argv) != 3:
    print("Вкажи: python src/test_update.py <external_id> <price>")
    sys.exit(1)

external_id = sys.argv[1]
price = float(sys.argv[2])

API_URL = "https://my.prom.ua/api/v1/products/set_prices"

headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "prices": [
        {
            "external_id": external_id,
            "price": price
        }
    ]
}

print("➡️ Відправляю як JSON:")
print(json.dumps(payload, indent=2, ensure_ascii=False))

response = requests.post(API_URL, headers=headers, json=payload)

print("📥 Статус:", response.status_code)
print("📥 Відповідь (text):", response.text)
