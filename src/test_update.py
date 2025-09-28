import json
import os
import requests
import sys

# ✅ Читаємо токен з GitHub Secrets
API_TOKEN = os.getenv("PROM_API_TOKEN")

# ✅ ТВОЯ СТОРІНКА: detalua.prom.ua
API_URL = "https://my.prom.ua/api/v1/products/edit"


def main():
    if len(sys.argv) != 3:
        print("❌ Використання: python test_update.py <external_id> <price>")
        sys.exit(1)

    external_id = sys.argv[1]
    price = float(sys.argv[2])

    if not API_TOKEN:
        print("❌ Помилка: токен не знайдено (PROM_API_TOKEN порожній).")
        sys.exit(1)

    payload = {
        "products": [
            {
                "external_id": external_id,
                "price": price
            }
        ]
    }

    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json"
    }

    print("➡️ Відправляю як JSON:")
    print(json.dumps(payload, indent=2, ensure_ascii=False))

    response = requests.post(API_URL, headers=headers, json=payload)

    print(f"📥 Статус: {response.status_code}")
    try:
        print("📥 Відповідь:", response.json())
    except:
        print("📥 Відповідь (text):", response.text)


if __name__ == "__main__":
    main()
