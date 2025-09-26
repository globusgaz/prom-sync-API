import requests
import os
import json

API_TOKEN = os.getenv("PROM_API_TOKEN")  # токен з GitHub Secrets
API_URL = "https://my.prom.ua/api/v1/products/edit"

payload = {
    "products": [
        {
            "external_id": "f4_2736731",  # тестовий SKU
            "price": 1000000,             # тестова ціна
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
