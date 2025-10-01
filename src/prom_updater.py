import os
import json
import xml.etree.ElementTree as ET
import time
import asyncio
import aiohttp

API_URL = "https://my.prom.ua/api/v1/products/edit_by_external_id"
API_TOKEN = os.getenv("PROM_API_TOKEN")

FEEDS_FILE = "feeds.txt"
STATE_FILE = "product_state.json"
BATCH_SIZE = 100
DELAY_BETWEEN_BATCHES = 0.3
CONCURRENT_REQUESTS = 3

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Safari/537.36"
    )
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

async def parse_feed(session, url):
    """Точна копія логіки з yml-generator"""
    try:
        async with session.get(url, headers=HEADERS, timeout=180) as response:
            if response.status != 200:
                print(f"❌ {url} — HTTP {response.status}")
                return False, []
            
            content = await response.read()
            root = ET.fromstring(content)

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
            
    except Exception as e:
        print(f"❌ {url}: {e}")
        return False, []

async def send_updates(session, batch, batch_num, total_batches):
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
        async with session.post(API_URL, headers=headers, json=payload, timeout=120) as response:
            if response.status == 200:
                print(f"✅ Партія {batch_num}")
            else:
                text = await response.text()
                print(f"❌ Партія {batch_num} - помилка {response.status}")
    except Exception as e:
        print(f"❌ Партія {batch_num}: {e}")

async def main_async():
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

    all_products = []
    successful_feeds = 0
    failed_feeds = []

    print("\n🔄 Збір даних з фідів...")
    
    # Створюємо сесію БЕЗ ClientTimeout - використовуємо числові таймаути
    async with aiohttp.ClientSession() as session:
        # Паралельний збір фідів
        tasks = [parse_feed(session, url) for url in feed_urls]
        results = await asyncio.gather(*tasks)
        
        for url, (success, products) in zip(feed_urls, results):
            if success:
                successful_feeds += 1
                all_products.extend(products)
                print(f"✅ {url}: {len(products)} товарів")
            else:
                failed_feeds.append(url)

        print(f"\n📊 Підсумок збору:")
        print(f"✅ Успішних фідів: {successful_feeds}/{len(feed_urls)}")
        
        if failed_feeds:
            print(f"❌ Недоступних фідів: {len(failed_feeds)}")
            for url in failed_feeds:
                print(f"  - {url}")
            print(f"\n🛑 ЗУПИНКА: Не всі фіди доступні! Оновлення не виконується.")
            return
        
        print(f"📦 Загальна кількість товарів: {len(all_products)}")

        if not all_products:
            print("\n❌ Немає товарів!")
            return

        changed_products = [p for p in all_products if has_changed(p, old_state)]
        
        print(f"\n🔍 Аналіз змін:")
        print(f"📦 Всього товарів: {len(all_products)}")
        print(f"🔄 Змінилось: {len(changed_products)}")

        if not changed_products:
            print("\n✅ Немає змін!")
            save_current_state(all_products)
            return

        total_batches = (len(changed_products) - 1) // BATCH_SIZE + 1
        print(f"\n🚀 Оновлення {len(changed_products)} товарів у {total_batches} партіях...")
        
        start_time = time.time()
        
        # Паралельні запити до API
        for i in range(0, len(changed_products), BATCH_SIZE * CONCURRENT_REQUESTS):
            batch_tasks = []
            for j in range(CONCURRENT_REQUESTS):
                batch_i = i + j * BATCH_SIZE
                if batch_i >= len(changed_products):
                    break
                batch = changed_products[batch_i:batch_i+BATCH_SIZE]
                batch_num = batch_i // BATCH_SIZE + 1
                batch_tasks.append(send_updates(session, batch, batch_num, total_batches))
            
            await asyncio.gather(*batch_tasks)
            await asyncio.sleep(DELAY_BETWEEN_BATCHES)
        
        end_time = time.time()
        duration = end_time - start_time
        
        save_current_state(all_products)
        print(f"\n💾 Стан збережено: {len(all_products)} товарів")
        print(f"\n✅ Завершено за {duration:.1f}с ({duration/60:.1f}хв)")
        if changed_products:
            print(f"📊 Швидкість: {len(changed_products)/duration:.1f} товарів/сек")

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
