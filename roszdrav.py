"""
Поиск РУ через API Росздравнадзора.

Для каждого ненайденного товара:
1. Ищем РУ по производителю + название/модель через API
2. Проверяем что найденное РУ соответствует типу изделия
3. Извлекаем noRu
"""

import asyncio
import aiohttp
import ssl
import json
import csv
import re
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)

ROSZDRAV_API = "https://elk.roszdravnadzor.gov.ru/public-gateway/registered-med-product/api/v1/med-product/filter-public"

# Категории медицинских изделий для валидации совпадения
PRODUCT_CATEGORIES = {
    "коляска": ["коляска", "кресло-коляска", "wheelchair", "кресло коляска"],
    "электроприставка": ["электроприставка", "приставка", "привод"],
    "ортез": ["ортез", "orthosis", "бандаж", "корсет", "тутор"],
    "протез": ["протез", "prosth"],
    "ходунки": ["ходунки", "walker", "роллатор"],
    "опора": ["опора", "трость", "костыль"],
    "кровать": ["кровать", "bed", "матрас"],
    "подъёмник": ["подъёмник", "подъемник", "lift", "hoist"],
    "вертикализатор": ["вертикализатор", "stander"],
    "подушка": ["подушка", "cushion", "сиденье"],
    "велотренажёр": ["велотренаж", "тренажер", "тренажёр"],
    "слуховой": ["слуховой", "слух", "hearing"],
    "кресло": ["кресло", "стул"],
}


def get_ssl_context():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def detect_category(text: str) -> set[str]:
    """Определить категории товара по тексту."""
    text_lower = text.lower()
    found = set()
    for cat, keywords in PRODUCT_CATEGORIES.items():
        if any(kw in text_lower for kw in keywords):
            found.add(cat)
    return found


def categories_compatible(our_product: str, registry_name: str) -> bool:
    """Проверить что категории товара и РУ совместимы."""
    our_cats = detect_category(our_product)
    reg_cats = detect_category(registry_name)

    # Если не можем определить хотя бы одну - НЕ доверяем (возвращаем False)
    if not our_cats or not reg_cats:
        return False

    # Есть пересечение - совместимо
    return bool(our_cats & reg_cats)


async def search_ru(
    session: aiohttp.ClientSession,
    query: str,
    producer: str = "",
) -> list[dict]:
    """Поиск РУ через API Росздравнадзора."""
    payload = {
        "page": 0,
        "size": 20,
    }

    if producer:
        payload["producer"] = producer
    if query:
        payload["name"] = query

    try:
        async with session.post(
            ROSZDRAV_API,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            ssl=get_ssl_context(),
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
            return data.get("content", [])
    except Exception as e:
        logger.debug("Ошибка поиска РУ: %s", e)
        return []


async def search_ru_by_number(
    session: aiohttp.ClientSession,
    ru_number: str,
) -> dict | None:
    """Найти конкретное РУ по номеру."""
    ru_clean = re.sub(r'\s+', ' ', ru_number.strip())
    variants = [ru_clean, ru_clean.replace(" ", "")]

    for variant in variants:
        payload = {"page": 0, "size": 5, "noRu": variant}
        try:
            async with session.post(
                ROSZDRAV_API,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                ssl=get_ssl_context(),
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    continue
                data = await resp.json()
                items = data.get("content", [])
                if items:
                    return items[0]
        except Exception as e:
            logger.debug("Ошибка поиска РУ %s: %s", ru_number, e)
    return None


async def find_ru_for_product(
    session: aiohttp.ClientSession,
    product_name: str,
    manufacturer: str,
    model: str,
) -> dict | None:
    """
    Найти подходящее РУ для товара.
    Возвращает dict с noRu и name, или None.
    """
    # Стратегия 1: поиск по производителю + модели
    if manufacturer and model:
        results = await search_ru(session, model, producer=manufacturer)
        for item in results:
            reg_name = item.get("name", "")
            if categories_compatible(product_name, reg_name):
                # Дополнительно: модель должна быть в имени РУ
                if model.lower() in reg_name.lower():
                    return {
                        "noRu": item.get("noRu", ""),
                        "name": reg_name,
                        "status": item.get("status", {}).get("name", ""),
                    }

    # Стратегия 2: поиск по производителю + ключевым словам из названия
    if manufacturer:
        # Берём ключевые слова из названия (не общие)
        keywords = extract_keywords(product_name)
        if keywords:
            results = await search_ru(session, keywords, producer=manufacturer)
            for item in results:
                reg_name = item.get("name", "")
                if categories_compatible(product_name, reg_name):
                    return {
                        "noRu": item.get("noRu", ""),
                        "name": reg_name,
                        "status": item.get("status", {}).get("name", ""),
                    }

    return None


def extract_keywords(product_name: str) -> str:
    """Извлечь ключевые слова из названия для поиска."""
    # Убираем общие слова
    stop = {
        "для", "с", "и", "в", "на", "от", "по", "из", "без", "при",
        "инвалидная", "инвалидов", "детей", "детская", "взрослых",
        "медицинская", "медицинский", "ручным", "приводом",
        "электроприводом", "размер", "комплект",
    }
    words = product_name.lower().split()
    keywords = [w for w in words if w not in stop and len(w) > 3]
    # Берём первые 3 значимых слова
    return " ".join(keywords[:3])


async def run_roszdrav_stage(
    tru_data_csv: str = "tru_data.csv",
    spreadsheet_csv: str = "spreadsheet.csv",
) -> dict[str, dict]:
    """
    Второй этап: поиск РУ через Росздравнадзор для ненайденных товаров.
    Возвращает row_num → {noRu, name, status}
    """
    # Загружаем результаты первого этапа
    with open(tru_data_csv, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    # Загружаем таблицу для производителей
    with open(spreadsheet_csv, encoding="utf-8") as f:
        spreadsheet = {str(i + 2): row for i, row in enumerate(csv.DictReader(f))}

    # Товары без РУ
    to_search = []
    for row in rows:
        ru = row.get("ru_number", "").strip()
        if not ru:
            row_num = row.get("row_num", "")
            spr = spreadsheet.get(row_num, {})
            to_search.append({
                "row_num": row_num,
                "name": row.get("name", ""),
                "manufacturer": spr.get("Производитель", ""),
                "model": spr.get("Модель", ""),
            })

    logger.info("Товаров без РУ для поиска: %d", len(to_search))

    results = {}
    found = 0

    async with aiohttp.ClientSession() as session:
        for i, item in enumerate(to_search):
            if i % 50 == 0:
                logger.info("Росздрав: %d/%d (найдено: %d)...", i, len(to_search), found)

            result = await find_ru_for_product(
                session,
                item["name"],
                item["manufacturer"],
                item["model"],
            )

            if result:
                results[item["row_num"]] = result
                found += 1

            await asyncio.sleep(0.5)  # Rate limiting

    logger.info("Росздрав: найдено РУ для %d/%d товаров", found, len(to_search))
    return results
