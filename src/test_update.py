import os
import sys
import requests
import json

API_URL = "https://detalua.prom.ua/api/v1/products/update"  # ✅ правильний формат

def main():
    if len(sys.argv) < 3:
        print("Використання: python test_update.py <external_id> <price>")
        sys.exit(1)

    external_id = sys.argv[1]
    price = float(sys.argv[2])

    token = os.getenv("PROM_API_TOKEN")
    if not token:
        print("❌ Не знайдено токен PROM_API_TOKEN у середовищі")
        sys.exit(1)

    payload = {
        "products": [
            {
                "external_id": external_id,
                "price": price
            }
        ]
    }

    print("➡️ Відправляю як JSON:")
    print(json.dumps(payload, ensure_ascii=False, indent=2))

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response = requests.post(API_URL, json=payload, headers=headers)
    print(f"📥 Статус: {response.status_code}")

    try:
        print("📥 Відповідь:", response.json())
    except:
        print("📥 Відповідь (text):", response.text)


if __name__ == "__main__":
    main()
