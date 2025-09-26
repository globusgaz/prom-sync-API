import requests
import os
import json
import sys

API_TOKEN = os.getenv("PROM_API_TOKEN")
API_URL = "https://my.prom.ua/api/v1/products/edit"

if len(sys.argv) < 3:
    print("❌ Використання: python test_update.py <SKU> <ЦІНА>")
    sys.exit(1)

sku = sys.argv[1]
new_price = float(sys.argv[2])

payload = {
    "products": [
        {
            "external_id": sku,
            "price": new_price,
            "quantity_in_stock": 5,
            "presence": "available"
        }
    ]
}

headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

resp = requests.post(API_URL, headers=headers, json=payload)

print("Status code:", resp.status_code)
try:
    print("Response JSON:", json.dumps(resp.json(), indent=2, ensure_ascii=False))
except Exception:
    print("Raw response:", resp.text)
