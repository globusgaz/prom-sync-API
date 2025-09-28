import os
import sys
import requests
import json

API_TOKEN = os.getenv("PROM_API_TOKEN")
URL = "https://my.prom.ua/api/v1/products/edit"

if not API_TOKEN:
    print("❌ Не знайдено PROM_API_TOKEN")
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

payload = [
    {
        "id": SKU,
        "price": PRICE,
        "presence": "available",
        "quantity_in_stock": 99,
        "presence_sure": True,
        "status": "on_display"
    }
]

headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Accept-Language": "uk",
    "Content-Type": "application/json"
}

print("➡️ Відправляю як JSON:")
print(json.dumps(payload, ensure_ascii=False, indent=2))

response = requests.post(URL, headers=headers, json=payload)

print("📥 Статус:", response.status_code)
try:
    print("📥 Відповідь:", response.json())
except:
    print("📥 Відповідь (text):", response.text)
