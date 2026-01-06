BASE_URL = "https://ktsr.sfr.gov.ru"
CATALOG_LIST_URL = f"{BASE_URL}/ru-RU/product/default/order86n-list"
SUBSECTION_URL = f"{BASE_URL}/ru-RU/product/product/order86n"
PRODUCT_URL = f"{BASE_URL}/ru-RU/product/view"

# Google Sheets
SPREADSHEET_ID = "17xbK-V5R0LfGtxIlK2Q43rv7vzurRMjj"

# Параметры парсинга
PRODUCTS_PER_PAGE = 12
REQUEST_DELAY = 1.0  # секунда между запросами
MAX_RETRIES = 3
RETRY_DELAY = 5  # секунд между ретраями

# Файлы
PROGRESS_FILE = "progress.json"
OUTPUT_CSV = "tru_data.csv"
SPREADSHEET_CSV = "spreadsheet.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
}
