"""
Валидация номеров РУ в финальной таблице.

Для каждой строки с РУ: ищем в реестре Росздравнадзора по номеру,
сравниваем тип изделия с нашим товаром.
Если тип не совпадает - очищаем РУ, ОКПД2 и PDF.

СТРОГАЯ валидация:
- МЗ РФ / ФС (старые форматы) - убираем сразу
- Не найден в реестре - убираем (нет подтверждения)
- Категория не определена - убираем (нельзя подтвердить)
- Категория не совпадает - убираем

Запуск: python3 validate_ru.py [input_csv]
"""

import asyncio
import aiohttp
import ssl
import csv
import re
import sys
import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ROSZDRAV_API = "https://elk.roszdravnadzor.gov.ru/public-gateway/registered-med-product/api/v1/med-product/filter-public"

# Категории медизделий - ключевые слова
CATEGORIES = {
    "коляска": ["коляска", "кресло-коляска", "wheelchair", "кресло коляска"],
    "электроприставка": ["электроприставка", "приставка", "электропривод"],
    "ортез": ["ортез", "orthosis", "голеностоп", "лучезапястн", "коленн", "тутор", "бандаж", "корсет"],
    "протез": ["протез", "prosth", "конечност"],
    "ходунки": ["ходунки", "walker", "роллатор"],
    "опора": ["опора", "трость", "костыль", "cane"],
    "кровать": ["кровать", "bed", "матрас"],
    "подъёмник": ["подъёмник", "подъемник", "лифт", "lift", "hoist"],
    "слух": ["слух", "аудио", "hearing", "cochlear"],
    "зрение": ["зрение", "очки", "лупа", "braille", "брайл"],
    "дыхание": ["дыхан", "вентилятор", "ивл", "кислород"],
    "экг": ["экг", "кардио", "электрокардио"],
    "вертикализатор": ["вертикализатор", "stander", "вертикал"],
    "подушка": ["подушка", "cushion", "противопролежн"],
    "велотренажёр": ["велотренаж", "тренажер", "тренажёр"],
    "кресло": ["кресло", "стул", "сиденье"],
}


def get_ssl_context():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def get_category(text: str) -> set:
    """Определить категории товара по тексту."""
    text_lower = text.lower()
    found = set()
    for cat, keywords in CATEGORIES.items():
        if any(kw in text_lower for kw in keywords):
            found.add(cat)
    return found


def categories_compatible(our_product: str, registry_name: str) -> tuple[bool, str]:
    """
    СТРОГАЯ проверка совместимости категорий.
    Если не можем определить - НЕ доверяем.
    """
    our_cats = get_category(our_product)
    reg_cats = get_category(registry_name)

    if not our_cats:
        return False, "наш товар: категория не определена"

    if not reg_cats:
        return False, f"в реестре: категория не определена ({registry_name[:50]})"

    if our_cats & reg_cats:
        return True, f"совпадение: {our_cats & reg_cats}"

    return False, f"наш: {our_cats}, реестр: {reg_cats}"


async def search_by_ru_number(session: aiohttp.ClientSession, ru_number: str) -> dict | None:
    """Найти изделие по номеру РУ через API Росздравнадзора."""
    ru_clean = re.sub(r'\s+', ' ', ru_number.strip())
    variants = [ru_clean, ru_clean.replace(" ", "")]

    for variant in variants:
        payload = {"page": 0, "size": 10, "noRu": variant}
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


async def validate_all(rows: list[dict]) -> tuple[list[dict], dict]:
    """
    Валидировать все строки с РУ.
    Возвращает (rows, validation_results).
    """
    validation = {}
    to_check = [(i, r) for i, r in enumerate(rows) if r.get("Номер РУ", "").strip()]

    logger.info("Строк с РУ для проверки: %d из %d", len(to_check), len(rows))

    stats = {"ok": 0, "mismatch": 0, "not_found": 0, "old_format": 0, "no_cat": 0}

    async with aiohttp.ClientSession() as session:
        for idx, (i, row) in enumerate(to_check):
            ru = row["Номер РУ"].strip()
            our_name = row.get("Товар", row.get("name", ""))
            row_num = row.get("№ строки", row.get("row_num", str(i)))

            if idx % 50 == 0:
                logger.info(
                    "Проверяю %d/%d... (ok=%d, mismatch=%d, not_found=%d, old=%d)",
                    idx, len(to_check),
                    stats["ok"], stats["mismatch"], stats["not_found"], stats["old_format"],
                )

            # 1. Старые форматы - убираем сразу
            if re.match(r'^(МЗ\s*РФ|ФС\s+\d{2})', ru):
                validation[row_num] = {"ok": False, "reason": "old_format", "registry_name": ""}
                stats["old_format"] += 1
                continue

            # 2. Ищем в реестре
            registry_item = await search_by_ru_number(session, ru)
            await asyncio.sleep(0.3)

            if not registry_item:
                # Не найден - убираем (нет подтверждения)
                validation[row_num] = {"ok": False, "reason": "not_found", "registry_name": ""}
                stats["not_found"] += 1
                continue

            registry_name = registry_item.get("name", "")

            # 3. Проверяем совместимость категорий (СТРОГО)
            compatible, reason = categories_compatible(our_name, registry_name)
            if compatible:
                validation[row_num] = {"ok": True, "reason": reason, "registry_name": registry_name}
                stats["ok"] += 1
            else:
                validation[row_num] = {"ok": False, "reason": reason, "registry_name": registry_name}
                stats["mismatch"] += 1

    logger.info(
        "Итого: ok=%d, несовпадений=%d, не найдено=%d, старый формат=%d",
        stats["ok"], stats["mismatch"], stats["not_found"], stats["old_format"],
    )
    return rows, validation


async def main():
    input_csv = sys.argv[1] if len(sys.argv) > 1 else "tru_data.csv"
    output_json = "ru_validation.json"

    if not Path(input_csv).exists():
        logger.error("Файл не найден: %s", input_csv)
        return

    with open(input_csv, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    logger.info("Загружено строк: %d", len(rows))

    # Маппинг колонок (разные форматы CSV)
    name_key = "Товар" if "Товар" in rows[0] else "name"
    ru_key = "Номер РУ" if "Номер РУ" in rows[0] else "ru_number"
    num_key = "№ строки" if "№ строки" in rows[0] else "row_num"

    # Нормализуем ключи
    for row in rows:
        if name_key != "Товар" and name_key in row:
            row["Товар"] = row[name_key]
        if ru_key != "Номер РУ" and ru_key in row:
            row["Номер РУ"] = row[ru_key]
        if num_key != "№ строки" and num_key in row:
            row["№ строки"] = row[num_key]

    rows, validation = await validate_all(rows)

    # Сохраняем результаты валидации
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(validation, f, ensure_ascii=False, indent=2)

    logger.info("Валидация сохранена: %s (%d записей)", output_json, len(validation))

    # Статистика
    total_ru = sum(1 for r in rows if r.get("Номер РУ", "").strip())
    ok_count = sum(1 for v in validation.values() if v.get("ok"))
    bad_count = sum(1 for v in validation.values() if not v.get("ok"))
    logger.info("РУ всего: %d, подтверждено: %d, убрано: %d", total_ru, ok_count, bad_count)


if __name__ == "__main__":
    asyncio.run(main())
