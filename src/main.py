import os
import sys
import random
import concurrent.futures
from typing import Iterable

from .config import AppConfig
from .feed_parser import FeedParser, ProductUpdate
from .prom_client import PromClient


def read_feeds_list(feeds_path: str) -> list[str]:
    if not os.path.exists(feeds_path):
        return []
    urls: list[str] = []
    with open(feeds_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            urls.append(line)
    return urls


def gather_updates(feed_urls: list[str], max_workers: int) -> Iterable[ProductUpdate]:
    parser = FeedParser()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(list, parser.fetch_and_parse(u)) for u in feed_urls]
        for fut in concurrent.futures.as_completed(futures):
            for upd in fut.result():
                yield upd


def verify_updates(client: PromClient, updates: list[ProductUpdate], sample_size: int = 5) -> None:
    """Перевіряє кілька випадкових товарів у Prom після оновлення."""
    sample = random.sample(updates, min(sample_size, len(updates)))
    print(f"🔎 Перевірка {len(sample)} випадкових товарів у Prom:")

    for upd in sample:
        try:
            product = client.get_product_by_external_id(upd.external_id)
            if not product:
                print(f"❌ {upd.external_id}: не знайдено в Prom")
                continue

            price = product.get("price")
            quantity = product.get("quantity_in_stock")

            print(
                f"✅ {upd.external_id} — Prom: ціна={price}, залишок={quantity} "
                f"(оновлено: ціна={upd.price}, залишок={upd.stock_quantity})"
            )
        except Exception as e:
            print(f"⚠️ Помилка перевірки {upd.external_id}: {e}")


def main() -> int:
    config = AppConfig.load()
    if not config.prom_api_token:
        print("PROM_API_TOKEN is not set", file=sys.stderr)
        return 2

    feeds_file = os.getenv(
        "FEEDS_FILE",
        os.path.join(os.path.dirname(__file__), "..", "feeds.txt"),
    )
    feeds_file = os.path.abspath(feeds_file)
    feed_urls = read_feeds_list(feeds_file)
    if not feed_urls:
        print("No feed URLs found in feeds.txt", file=sys.stderr)
        return 1

    client = PromClient(config)
    updates = list(gather_updates(feed_urls, max_workers=config.max_workers))

    if not updates:
        print("⚠️ Немає оновлень для відправки")
        return 0

    client.update_stream(updates)
    print(f"✅ Оновлено {len(updates)} товарів у Prom")

    # Перевірка кількох випадкових товарів
    verify_updates(client, updates)

    return 0


if __name__ == "__main__":
    sys.exit(main())
