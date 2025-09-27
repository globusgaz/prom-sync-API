import os
import sys
import requests
import json

PROM_API_TOKEN = os.getenv("PROM_API_TOKEN")
PROM_BASE_URL = "https://my.prom.ua/api/v1/products/edit"

if not PROM_API_TOKEN:
    print("❌ PROM_API_TOKEN не заданий")
    sys.exit(1)

if len(sys.argv) < 3:
    print("❌ Використання: python test_update.py <SKU> <PRICE>")
    sys.exit(1)

SKU = sys.argv[1]
try:
    PRICE = float(sys.argv[2])
except ValueError:
    print("❌ Ціна має бути числом")
    sys.exit(1)

headers = {
    "Authorization": f"Bearer {PROM_API_TOKEN}",
    "Content-Type": "application/json",
    "Accept-Language": "uk",
}

payload = {
    "products": [
        {
            "external_id": SKU,
            "price": PRICE,
            "presence": "available",
            "quantity_in_stock": 99,
            "presence_sure": True,
            "status": "on_display"
        }
    ]
}

print("➡️ Відправляю на Prom (v1):")
print(json.dumps(payload, ensure_ascii=False, indent=2))

resp = requests.post(PROM_BASE_URL, headers=headers, json=payload)

print("📥 Статус:", resp.status_code)
try:
    print("📥 Відповідь:", resp.json())
except:
    print("📥 Відповідь (text):", resp.text)
