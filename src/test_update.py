import os
import sys
import json
import requests

if len(sys.argv) < 3:
    print("Використання: python test_update.py <external_id> <price>")
    sys.exit(1)

SKU = sys.argv[1]
PRICE = float(sys.argv[2])

API_TOKEN = os.getenv("PROM_API_TOKEN")
if not API_TOKEN:
    print("❌ Помилка: змінна середовища PROM_API_TOKEN не задана")
    sys.exit(1)

# ✅ Правильний URL для Prom API v1:
API_URL = "https://my.prom.ua/api/v1/products/edit"

payload = {
    "products": [
        {
            "id": SKU,
            "price": PRICE
        }
    ]
}

headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

print("➡️ Відправляю на Prom (v1):")
print(json.dumps(payload, indent=2, ensure_ascii=False))

response = requests.post(API_URL, headers=headers, json=payload)

print(f"📥 Статус: {response.status_code}")
try:
    print("📥 Відповідь:", response.json())
except Exception:
    print("📥 Відповідь (raw):", response.text)
