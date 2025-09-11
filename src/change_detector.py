import os
import json
import hashlib
from typing import Dict, List, Tuple

import aiohttp

STATE_DIR = os.path.join(os.getcwd(), ".state")
STATE_FILE = os.path.join(STATE_DIR, "feeds_state.json")


async def _head_metadata(session: aiohttp.ClientSession, url: str) -> Tuple[str, str]:
	etag = ""
	last_modified = ""
	try:
		async with session.head(url, timeout=30) as resp:
			etag = resp.headers.get("ETag", "")
			last_modified = resp.headers.get("Last-Modified", "")
	except Exception:
		pass
	return etag, last_modified


async def _hash_body(session: aiohttp.ClientSession, url: str) -> str:
	try:
		async with session.get(url, timeout=60) as resp:
			content = await resp.read()
			return hashlib.md5(content).hexdigest()
	except Exception:
		return ""


def _load_state() -> Dict[str, Dict[str, str]]:
	if not os.path.exists(STATE_FILE):
		return {}
	try:
		with open(STATE_FILE, "r", encoding="utf-8") as f:
			return json.load(f)
	except Exception:
		return {}


def _save_state(state: Dict[str, Dict[str, str]]) -> None:
	os.makedirs(STATE_DIR, exist_ok=True)
	with open(STATE_FILE, "w", encoding="utf-8") as f:
		json.dump(state, f, ensure_ascii=False, indent=2)


async def detect_changes(feed_urls: List[str]) -> Tuple[bool, Dict[str, Dict[str, str]]]:
	"""
	Returns (has_any_changes, new_state)
	"""
	old_state = _load_state()
	new_state: Dict[str, Dict[str, str]] = {}
	changed = False
	async with aiohttp.ClientSession() as session:
		for url in feed_urls:
			etag, last_modified = await _head_metadata(session, url)
			fingerprint = etag or last_modified
			if not fingerprint:
				fingerprint = await _hash_body(session, url)
			new_state[url] = {"fingerprint": fingerprint}
			if old_state.get(url, {}).get("fingerprint", "") != fingerprint:
				changed = True
	return changed, new_state


def persist_state(state: Dict[str, Dict[str, str]]) -> None:
	_save_state(state)
