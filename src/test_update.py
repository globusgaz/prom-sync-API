import sys
import json
import requests

# ▶️ ТВОЙ ТОКЕН
API_TOKEN = "cf92d55fedc41652112cea7c26c405fb********"

# ▶️ API URL
API_URL = "https://my.prom.ua/api/v1/products/edit"

def main():
    if len(sys.argv) != 3:
        print("❌ Використання: python test_update.py <external_id> <price>")
        sys.exit(1)

    product_id = sys.argv[1]
    new_price = float(sys.argv[2])

    payload = {
        "products": [
            {
                "external_id": product_id,
                "price": new_price
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
