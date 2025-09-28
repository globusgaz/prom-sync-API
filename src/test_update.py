import os
import requests

API_TOKEN = os.getenv("PROM_API_TOKEN")
API_URL = "https://my.prom.ua/api/v1/products/list?limit=3"

headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

print("➡️ Запит на products/list")

response = requests.get(API_URL, headers=headers)

print("📥 Статус:", response.status_code)
print("📥 Відповідь (text):", response.text)
