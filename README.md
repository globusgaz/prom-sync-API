## Prom.ua Feed Updater

- **Мета**: зчитувати XML-фіди з `feeds.txt`, генерувати унікальні `vendorCode` з префіксом, та оновлювати ціну і наявність товарів через API Prom.ua.

### Налаштування
1. Додайте фіди у `feeds.txt` (по одному в рядок).
2. Локально: створіть `.env` на основі `.env.example` і заповніть змінні.
3. У GitHub → Settings → Secrets and variables → Actions додайте секрет `PROM_API_TOKEN`.

### Запуск локально
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/prom_updater.py
```

### GitHub Actions
- Workflow `Update Prom` запускається по розкладу (cron) та при пуші в main. Він використовує секрет `PROM_API_TOKEN` та змінні оточення.

### Префіксація артикула
- Унікальний код формується як `<VENDOR_PREFIX>_<vendorCode або offer id або md5>`.

### Налаштування через змінні оточення
- `PROM_API_TOKEN`: API ключ Prom.ua
- `VENDOR_PREFIX`: префікс для унікальності артикулів
- `HTTP_TIMEOUT_SECONDS`, `MAX_CONCURRENT_REQUESTS`
- `UPDATE_MODE`: `both` | `prices` | `stocks`
- `PROM_BASE_URL`, `PROM_UPDATE_ENDPOINT`, `PROM_AUTH_HEADER`, `PROM_AUTH_SCHEME`
- `DRY_RUN`: `1` для тестового запуску без відправки
