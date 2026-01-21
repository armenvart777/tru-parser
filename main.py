import asyncio
import argparse
import logging
import urllib.request
import json
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
    ap.add_argument("--validate", action="store_true",
                    help="Валидация РУ через API Росздравнадзора")
    ap.add_argument("--roszdrav", action="store_true",
                    help="Поиск РУ для ненайденных через Росздрав API")
    ap.add_argument("--build", action="store_true",
                    help="Сборка финального XLSX")
    ap.add_argument("--full", action="store_true",
                    help="Полный пайплайн: матчинг → парсинг → валидация → сборка")
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
        import aiohttp
        from parser import collect_catalog, save_progress, load_progress
        async with aiohttp.ClientSession() as session:
            catalog = await collect_catalog(session)
        progress = load_progress()
        progress["catalog"] = catalog
        save_progress(progress)
        print(f"Каталог: {len(catalog)} товаров → progress.json")
        return

    if args.match:
        from parser import load_spreadsheet, match_products, load_progress, save_results_csv
        if not args.no_download:
            download_spreadsheet()
        progress = load_progress()
        catalog = progress.get("catalog", [])
        if not catalog:
            print("Каталог не найден. Сначала --collect или --run")
            return
        spreadsheet = load_spreadsheet()
        matches = match_products(spreadsheet, catalog)
        details = progress.get("details", {})
        save_results_csv(matches, details)
        found = sum(1 for m in matches if m["matched_code"])
        not_found = sum(1 for m in matches if not m["matched_code"])
        print(f"Найдено: {found}, не найдено: {not_found}")
        return

    if args.validate:
        from validate_ru import main as validate_main
        await validate_main()
        return

    if args.roszdrav:
        from roszdrav import run_roszdrav_stage
        results = await run_roszdrav_stage()
        with open("roszdrav_extra.json", "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"Найдено доп. РУ: {len(results)} → roszdrav_extra.json")
        return

    if args.build:
        from build_final import main as build_main
        build_main()
        return

    if args.run:
        if not args.no_download:
            download_spreadsheet()
        from parser import run_full
        matches, details = await run_full()
        found = sum(1 for m in matches if m["matched_code"])
        print(f"\nГотово! {found}/{len(matches)} товаров заполнено")
        return

    if args.full:
        # Полный пайплайн
        logger.info("=== ПОЛНЫЙ ПАЙПЛАЙН ===")

        # 1. Скачать таблицу
        if not args.no_download:
            download_spreadsheet()

        # 2. Матчинг + парсинг страниц
        from parser import run_full
        matches, details = await run_full()
        found = sum(1 for m in matches if m["matched_code"])
        logger.info("Этап 1: матчинг + парсинг: %d/%d", found, len(matches))

        # 3. Валидация РУ
        logger.info("Этап 2: валидация РУ...")
        from validate_ru import main as validate_main
        await validate_main()

        # 4. Поиск доп. РУ через Росздрав
        logger.info("Этап 3: поиск доп. РУ...")
        from roszdrav import run_roszdrav_stage
        extra = await run_roszdrav_stage()
        with open("roszdrav_extra.json", "w", encoding="utf-8") as f:
            json.dump(extra, f, ensure_ascii=False, indent=2)
        logger.info("Найдено доп. РУ: %d", len(extra))

        # 5. Сборка XLSX
        logger.info("Этап 4: сборка XLSX...")
        from build_final import main as build_main
        build_main()

        logger.info("=== ГОТОВО ===")
        return

    ap.print_help()


if __name__ == "__main__":
    asyncio.run(main())
