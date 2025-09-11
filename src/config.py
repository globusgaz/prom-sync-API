import os
from dataclasses import dataclass
from typing import Optional

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


@dataclass
class Settings:
    prom_api_token: str
    vendor_prefix: str
    http_timeout_seconds: int
    max_concurrent_requests: int
    update_mode: str
    prom_base_url: str
    prom_update_endpoint: str
    prom_auth_header: str
    prom_auth_scheme: str
    dry_run: bool
    batch_size: int
    import_url: Optional[str]
    import_wait_seconds: int


def get_settings() -> Settings:
    return Settings(
        prom_api_token=os.getenv("PROM_API_TOKEN", ""),
        vendor_prefix=os.getenv("VENDOR_PREFIX", "SHOP"),
        http_timeout_seconds=int(os.getenv("HTTP_TIMEOUT_SECONDS", "120")),
        max_concurrent_requests=int(os.getenv("MAX_CONCURRENT_REQUESTS", "8")),
        update_mode=os.getenv("UPDATE_MODE", "both"),
        prom_base_url=os.getenv("PROM_BASE_URL", "https://my.prom.ua"),
        prom_update_endpoint=os.getenv("PROM_UPDATE_ENDPOINT", "/api/v1/products/edit_by_external_id"),
        prom_auth_header=os.getenv("PROM_AUTH_HEADER", "Authorization"),
        prom_auth_scheme=os.getenv("PROM_AUTH_SCHEME", "Bearer"),
        dry_run=os.getenv("DRY_RUN", "0") == "1",
        batch_size=int(os.getenv("BATCH_SIZE", "500")),
        import_url=os.getenv("IMPORT_URL"),
        import_wait_seconds=int(os.getenv("IMPORT_WAIT_SECONDS", "600")),
    )
