import json
import os
import requests
import sys

# ✅ Токен із GitHub Secrets
API_TOKEN = os.getenv("PROM_API_TOKEN")

# ✅ Правильний endpoint згідно документації
API_URL = "https://my.prom.ua/api/v1/products/edit_by_external_id"

def main():
    if len(sys.argv) != 3:
        print("❌ Використання: python src/test_update.py <external_id> <price>")
        sys.exit(1)

    external_id = sys.argv[1]
    price = float(sys.argv[2])

    if not API_TOKEN:
        print("❌ Помилка: токен не знайдено (PROM_API_TOKEN порожній).")
        sys.exit(1)

    # ✅ Масив об'єктів, а не "products"
    payload = [
        {
            "id": external_id,   # ⚠️ Саме "id", згідно документації
            "price": price
        }
    ]

    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
        "X-LANGUAGE": "uk"
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
