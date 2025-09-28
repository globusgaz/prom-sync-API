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

headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Accept-Language": "uk",
    "Content-Type": "application/x-www-form-urlencoded"
}

payload = {
    "products[0][id]": SKU,
    "products[0][price]": PRICE,
    "products[0][presence]": "available",
    "products[0][quantity_in_stock]": 99,
    "products[0][presence_sure]": "true",
    "products[0][status]": "on_display"
}

print("➡️ Відправляю (form-urlencoded):")
print(json.dumps(payload, ensure_ascii=False, indent=2))

response = requests.post(URL, headers=headers, data=payload)

print("📥 Статус:", response.status_code)
try:
    print("📥 Відповідь:", response.json())
except:
    print("📥 Відповідь (text):", response.text)
