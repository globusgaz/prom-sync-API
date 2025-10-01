import os
import json
import requests
import xml.etree.ElementTree as ET
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_URL = "https://my.prom.ua/api/v1/products/edit_by_external_id"
API_TOKEN = os.getenv("PROM_API_TOKEN")

FEEDS_FILE = "feeds.txt"
STATE_FILE = "product_state.json"
BATCH_SIZE = 50
REQUEST_TIMEOUT = 30
DELAY_BETWEEN_BATCHES = 1.0

def get_session():
    """Створити сесію з retry та повними заголовками"""
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def get_headers():
    """Повні заголовки для обходу блокування"""
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Cache-Control": "max-age=0"
    }

def load_previous_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_current_state(products):
    state = {}
    for p in products:
        state[p["id"]] = {
            "price": p.get("price"),
            "presence": p.get("presence"),
            "quantity_in_stock": p.get("quantity_in_stock")
        }
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

def has_changed(product, old_state):
    product_id = product["id"]
    if product_id not in old_state:
        return True
    old = old_state[product_id]
    return (
        old.get("price") != product.get("price") or
        old.get("presence") != product.get("presence") or
        old.get("quantity_in_stock") != product.get("quantity_in_stock")
    )

def parse_feed(session, url):
    try:
        response = session.get(url, headers=get_headers(), timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        root = ET.fromstring(response.content)

        products = []
        for offer in root.findall(".//offer"):
            product_id = offer.get("id")
            if not product_id:
                continue
                
            available = offer.get("available", "false").lower()
            price_el = offer.find("price")

            price = None
            if price_el is not None and price_el.text:
                try:
                    price = float(price_el.text.strip())
                except ValueError:
                    price = None

            if available == "true":
                presence = "available"
                quantity_in_stock = 1
            else:
                presence = "not_available"
                quantity_in_stock = 0

            products.append({
                "id": product_id,
                "price": price,
                "presence": presence,
                "quantity_in_stock": quantity_in_stock
            })
        
        return True, products
        
    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP {e.response.status_code} для {url} - ПРОПУСКАЄМО")
        return False, []
    except requests.exceptions.Timeout:
        print(f"❌ Таймаут для {url} - ПРОПУСКАЄМО")
        return False, []
    except requests.exceptions.ConnectionError:
        print(f"❌ Помилка з'єднання для {url} - ПРОПУСКАЄМО")
        return False, []
    except ET.ParseError as e:
        print(f"❌ Помилка XML для {url} - ПРОПУСКАЄМО")
        return False, []
    except Exception as e:
        print(f"❌ {url}: {e} - ПРОПУСКАЄМО")
        return False, []

def send_updates(batch, batch_num, total_batches):
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
        "X-LANGUAGE": "uk"
    }

    payload = []
    for item in batch:
        obj = {"id": item["id"]}
        if item.get("price") is not None:
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
                print(f"Деталі: {response.json()}")
            except:
                print(f"Відповідь: {response.text[:100]}")
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

    old_state = load_previous_state()
    print(f"📂 Завантажено попередній стан: {len(old_state)} товарів")

    session = get_session()
    all_products = []
    successful_feeds = 0
    failed_feeds = []

    print("\n🔄 Збір даних з фідів...")
    for url in feed_urls:
        print(f"🔄 Обробка фіду: {url}")
        success, products = parse_feed(session, url)
        
        if success:
            successful_feeds += 1
            all_products.extend(products)
            print(f"✅ Фід {url}: {len(products)} товарів")
        else:
            failed_feeds.append(url)

    print(f"\n📊 Підсумок збору:")
    print(f"✅ Успішних фідів: {successful_feeds}/{len(feed_urls)}")
    print(f"❌ Недоступних фідів: {len(failed_feeds)}")
    print(f"📦 Загальна кількість товарів: {len(all_products)}")

    if failed_feeds:
        print(f"\n⚠️ УВАГА: {len(failed_feeds)} фідів недоступні:")
        for url in failed_feeds:
            print(f"  - {url}")

    if not all_products:
        print("\n❌ Немає товарів для оновлення!")
        return

    changed_products = [p for p in all_products if has_changed(p, old_state)]
    
    print(f"\n🔍 Аналіз змін:")
    print(f"📦 Всього товарів: {len(all_products)}")
    print(f"🔄 Змінилось: {len(changed_products)}")
    print(f"✅ Без змін: {len(all_products) - len(changed_products)}")

    if not changed_products:
        print("\n✅ Немає змін для оновлення!")
        save_current_state(all_products)
        return

    total_batches = (len(changed_products) - 1) // BATCH_SIZE + 1
    print(f"\n🚀 Починаємо оновлення {len(changed_products)} товарів у {total_batches} партіях...")
    
    start_time = time.time()
    
    for i in range(0, len(changed_products), BATCH_SIZE):
        batch = changed_products[i:i+BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        send_updates(batch, batch_num, total_batches)
        if batch_num < total_batches:
            time.sleep(DELAY_BETWEEN_BATCHES)
    
    end_time = time.time()
    duration = end_time - start_time
    
    save_current_state(all_products)
    print(f"\n💾 Стан збережено: {len(all_products)} товарів")
    print(f"\n✅ Оновлення завершено за {duration:.1f} секунд ({duration/60:.1f} хвилин)")
    if changed_products:
        print(f"📊 Середня швидкість: {len(changed_products)/duration:.1f} товарів/сек")

if __name__ == "__main__":
    main()
