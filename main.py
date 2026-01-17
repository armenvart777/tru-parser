import asyncio
import argparse
import logging
import urllib.request
import sys

from config import SPREADSHEET_ID, SPREADSHEET_CSV

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("parser.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def download_spreadsheet():
    """Скачать Google Sheets как CSV."""
    url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/export?format=csv&gid=631153221"
    logger.info("Скачиваю таблицу...")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = resp.read()
    with open(SPREADSHEET_CSV, "wb") as f:
        f.write(data)
    lines = data.decode("utf-8").count("\n")
    logger.info("Таблица скачана: %s (%d строк)", SPREADSHEET_CSV, lines)


async def main():
    ap = argparse.ArgumentParser(description="Парсер каталога ТРУ")
    ap.add_argument("--test", type=str,
                    help="Тест-парсинг одного товара по коду")
    ap.add_argument("--collect", action="store_true",
                    help="Только собрать каталог (коды + названия)")
    ap.add_argument("--match", action="store_true",
                    help="Только матчинг (каталог должен быть в кэше)")
    ap.add_argument("--run", action="store_true",
                    help="Полный цикл: каталог → матчинг → парсинг → CSV")
    ap.add_argument("--no-download", action="store_true",
                    help="Не скачивать таблицу заново")
    args = ap.parse_args()

    if args.test:
        import aiohttp
        from parser import fetch, parse_product_page
        from config import PRODUCT_URL
        async with aiohttp.ClientSession() as session:
            url = f"{PRODUCT_URL}/{args.test}"
            html = await fetch(session, url)
            if html:
                result = parse_product_page(html, args.test)
                for k, v in result.items():
                    print(f"  {k}: {v}")
            else:
                print(f"  Не удалось загрузить {args.test}")
        return

    if args.collect:
        import aiohttp, json
        from parser import collect_catalog
        async with aiohttp.ClientSession() as session:
            catalog = await collect_catalog(session)
        with open("catalog.json", "w") as f:
            json.dump(catalog, f, ensure_ascii=False, indent=2)
        print(f"Каталог: {len(catalog)} товаров → catalog.json")
        return

    if args.match:
        from parser import load_spreadsheet, match_products, load_progress
        if not args.no_download:
            download_spreadsheet()
        progress = load_progress()
        catalog = progress.get("catalog", [])
        if not catalog:
            print("Каталог не найден. Сначала --collect или --run")
            return
        spreadsheet = load_spreadsheet()
        matches = match_products(spreadsheet, catalog)
        found = sum(1 for m in matches if m["matched_code"])
        not_found = sum(1 for m in matches if not m["matched_code"])
        print(f"Найдено: {found}, не найдено: {not_found}")
        # Show some not found
        nf = [m for m in matches if not m["matched_code"]][:10]
        if nf:
            print("Примеры ненайденных:")
            for m in nf:
                print(f"  row {m['row_num']}: {m['name'][:60]}")
        return

    if args.run:
        if not args.no_download:
            download_spreadsheet()
        from parser import run_full
        matches, details = await run_full()
        found = sum(1 for m in matches if m["matched_code"])
        print(f"\nГотово! {found}/{len(matches)} товаров заполнено → {SPREADSHEET_CSV}")
        return

    ap.print_help()


if __name__ == "__main__":
    asyncio.run(main())
