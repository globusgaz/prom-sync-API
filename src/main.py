import os
import sys
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


def main() -> int:
	config = AppConfig.load()
	if not config.prom_api_token:
		print("PROM_API_TOKEN is not set", file=sys.stderr)
		return 2
	feeds_file = os.getenv("FEEDS_FILE", os.path.join(os.path.dirname(__file__), "..", "feeds.txt"))
	feeds_file = os.path.abspath(feeds_file)
	feed_urls = read_feeds_list(feeds_file)
	if not feed_urls:
		print("No feed URLs found in feeds.txt", file=sys.stderr)
		return 1

	client = PromClient(config)
	updates = gather_updates(feed_urls, max_workers=config.max_workers)
	client.update_stream(updates)
	print("Updates sent successfully")
	return 0


if __name__ == "__main__":
	sys.exit(main())
