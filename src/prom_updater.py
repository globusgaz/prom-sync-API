import os
import json
import requests
import xml.etree.ElementTree as ET
import time
import hashlib

API_URL = "https://my.prom.ua/api/v1/products/edit_by_external_id"
API_TOKEN = os.getenv("PROM_API_TOKEN")

FEEDS_FILE = "feeds.txt"
STATE_FILE = "product_state.json"  # зберігаємо попередній стан
BATCH_SIZE = 50  # збільшено, бо товарів менше
REQUEST_TIMEOUT = 30
DELAY_BETWEEN_BATCHES = 1.0

def load_previous_state():
    """Завантажити попередній стан товарів"""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_current_state(products):
    """Зберегти поточний стан товарів"""
    state = {}
    for p in products:
        state[p["id"]] = {
            "price": p["price"],
            "presence": p["presence"],
            "quantity_in_stock": p["quantity_in_stock"]
        }
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

def has_changed(product, old_state):
    """Перевірити чи змінився товар"""
    product_id = product["id"]
    if product_id not in old_state:
        return True  # новий товар
    
    old = old_state[product_id]
    # Порівнюємо ціну, наявність та кількість
    return (
        old.get("price") != product["price"] or
        old.get("presence") != product["presence"] or
        old.get("quantity_in_stock") != product["quantity_in_stock"]
    )

def parse_feed(url):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
        }
        response = requests.get(url, timeout=REQUEST_TIMEOUT, headers=headers)
        response.raise_for_status()
        root = ET.fromstring(response.content)

        for offer in root.findall(".//offer"):
            product_id = offer.get("id")
            available = offer.get("available", "false").lower()
            price_el = offer.find("price")

            # Ціна (якщо є)
            price = None
            if price_el is not None and price_el.text:
                try:
                    price = float(price_el.text.strip())
                except ValueError:
                    price = None

            # Статус наявності
            if available == "true":
                presence = "available"
                quantity_in_stock = 1
            else:
                presence = "not_available"
                quantity_in_stock = 0

            yield {
                "id": product_id,
                "price": price,
                "presence": presence,
                "quantity_in_stock": quantity_in_stock
            }
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print(f"⚠️ Фід {url} заблокований (403) - пропускаємо")
        elif e.response.status_code == 404:
            print(f"⚠️ Фід {url} не знайдено (404) - пропускаємо")
        else:
            print(f"❌ Помилка HTTP {e.response.status_code} для {url}")
        return
    except requests.exceptions.Timeout:
        print(f"⚠️ Таймаут для {url} - пропускаємо")
        return
    except requests.exceptions.ConnectionError:
        print(f"⚠️ Помилка з'єднання для {url} - пропускаємо")
        return
    except ET.ParseError as e:
        print(f"❌ Помилка парсингу XML для {url}: {e}")
        return
    except Exception as e:
        print(f"❌ Помилка при обробці фіду {url}: {e}")
        return

def send_updates(batch, batch_num, total_batches):
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
        "X-LANGUAGE": "uk"
    }

    # Формуємо об'єкти тільки з потрібними полями
    payload = []
    for item in batch:
        obj = {"id": item["id"]}

        if item["price"] is not None:
            obj["price"] = item["price"]

        obj["presence"] = item["presence"]
        obj["quantity_in_stock"] = item["quantity_in_stock"]

        payload.append(obj)

    print(f"🔄 Партія {batch_num}/{total_batches} ({len(payload)} товарів)")

    try:
        response = requests.post(API_URL, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
        
        if response.status_code == 200:
            print(f"✅ Партія {batch_num} успішно оновлена")
        else:
            print(f"❌ Партія {batch_num} - помилка {response.status_code}")
            try:
                error_data = response.json()
                print(f"Деталі: {error_data}")
            except:
                print(f"Відповідь: {response.text[:100]}")
                
    except requests.exceptions.Timeout:
        print(f"⚠️ Таймаут для партії {batch_num}")
    except Exception as e:
        print(f"❌ Помилка для партії {batch_num}: {e}")

def main():
    if not API_TOKEN:
        print("❌ Токен PROM_API_TOKEN не знайдено!")
        return

    if not os.path.exists(FEEDS_FILE):
        print(f"❌ Файл {FEEDS_FILE} не знайдено!")
        return

    with open(FEEDS_FILE, "r") as f:
        feed_urls = [line.strip() for line in f if line.strip()]

    # Завантажуємо попередній стан
    old_state = load_previous_state()
    print(f"📂 Завантажено попередній стан: {len(old_state)} товарів")

    all_products = []
    successful_feeds = 0

    print("\n🔄 Збір даних з фідів...")
    for url in feed_urls:
        print(f"🔄 Обробка фіду: {url}")
        feed_count = 0
        for product in parse_feed(url):
            all_products.append(product)
            feed_count += 1
        
        if feed_count > 0:
            successful_feeds += 1
            print(f"✅ Фід {url}: {feed_count} товарів")

    print(f"\n📊 Підсумок збору:")
    print(f"✅ Успішних фідів: {successful_feeds}/{len(feed_urls)}")
    print(f"📦 Загальна кількість товарів: {len(all_products)}")

    if not all_products:
        print("❌ Немає товарів для обробки!")
        return

    # Фільтруємо тільки змінені товари
    changed_products = [p for p in all_products if has_changed(p, old_state)]
    
    print(f"\n🔍 Аналіз змін:")
    print(f"📦 Всього товарів: {len(all_products)}")
    print(f"🔄 Змінилось: {len(changed_products)}")
    print(f"✅ Без змін: {len(all_products) - len(changed_products)}")

    if not changed_products:
        print("\n✅ Немає змін для оновлення!")
        # Все одно зберігаємо стан
        save_current_state(all_products)
        return

    # Розрахунок партій
    total_batches = (len(changed_products) - 1) // BATCH_SIZE + 1
    print(f"\n🚀 Починаємо оновлення {len(changed_products)} товарів у {total_batches} партіях...")
    
    start_time = time.time()
    
    for i in range(0, len(changed_products), BATCH_SIZE):
        batch = changed_products[i:i+BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        
        send_updates(batch, batch_num, total_batches)
        
        # Затримка між партіями (крім останньої)
        if batch_num < total_batches:
            time.sleep(DELAY_BETWEEN_BATCHES)
    
    end_time = time.time()
    duration = end_time - start_time
    
    # Зберігаємо новий стан
    save_current_state(all_products)
    print(f"\n💾 Стан збережено: {len(all_products)} товарів")
    
    print(f"\n✅ Оновлення завершено за {duration:.1f} секунд ({duration/60:.1f} хвилин)")
    if changed_products:
        print(f"📊 Середня швидкість: {len(changed_products)/duration:.1f} товарів/сек")

if __name__ == "__main__":
    main()
