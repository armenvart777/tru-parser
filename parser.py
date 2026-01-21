"""
Парсер каталога ТРУ.

Алгоритм:
1. Скачиваем таблицу (CSV export из Google Sheets)
2. Собираем ВСЕ товары из каталога ТСР (код + название) через страницы подразделов
3. Матчим модели/бренды из таблицы с названиями в каталоге
4. Для сматченных товаров скачиваем страницу → извлекаем ОКПД2, РУ, PDF
5. Сохраняем результат в CSV
"""

import re
import json
import csv
import logging
import aiohttp
import asyncio
from pathlib import Path
from difflib import SequenceMatcher
from transliterate import translit, get_available_language_codes

from config import (
    CATALOG_LIST_URL, SUBSECTION_URL, PRODUCT_URL,
    PRODUCTS_PER_PAGE, REQUEST_DELAY, MAX_RETRIES, RETRY_DELAY,
    HEADERS, PROGRESS_FILE, OUTPUT_CSV, SPREADSHEET_CSV,
)

logger = logging.getLogger(__name__)

# Маппинг производителей: латиница → варианты написания в каталоге ТСР
MANUFACTURER_ALIASES = {
    "otto bock": ["отто бокк", "otto bock", "ottobock"],
    "vermeiren": ["vermeiren", "вермейрен"],
    "akces-med": ["akces-med", "akces med", "akcesmed", "акцес"],
    "ortonica": ["ortonica", "ортоника"],
    "patron": ["patron", "патрон"],
    "invacare": ["invacare", "инвакаре"],
    "hoggi": ["hoggi", "хогги"],
    "permobil": ["permobil", "пермобил"],
    "convaid": ["convaid", "конвейд"],
    "thomashilfen": ["thomashilfen", "томасхилфен"],
    "sunrise medical": ["sunrise", "санрайз"],
    "karma medical": ["karma"],
    "met": ["met"],
    "meyra": ["meyra", "мейра"],
    "sorg": ["sorg", "зорг"],
    "r82": ["r82"],
    "excel mobility": ["excel"],
    "quickie": ["quickie", "квики"],
    "ki mobility": ["ki mobility"],
    "roho": ["roho", "рохо"],
    "stabilo": ["stabilo", "стабило"],
    "медицинофф": ["медицинофф", "medicinoff"],
    "армед": ["армед", "armed"],
    "мега-оптим": ["мега-оптим", "mega-optim", "мега оптим"],
    "симс-2": ["симс-2", "симс", "sims-2"],
    "titan": ["titan", "титан"],
    "vitea care": ["vitea care", "vitea"],
    "fumagalli": ["fumagalli", "фумагалли"],
    "rebotec": ["rebotec", "реботек"],
    "nova": ["nova"],
    "nuova blandino": ["nuova blandino", "бландино"],
    "barry": ["barry", "барри"],
    "рехатехник": ["рехатехник", "reha technik", "rehatechnik"],
}


# ─── HTTP ───

async def fetch(session: aiohttp.ClientSession, url: str) -> str | None:
    """Загрузить страницу с ретраями."""
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    return await resp.text()
                logger.warning("HTTP %d: %s", resp.status, url)
        except Exception as e:
            logger.warning("Attempt %d failed for %s: %s", attempt + 1, url, e)
        if attempt < MAX_RETRIES - 1:
            await asyncio.sleep(RETRY_DELAY)
    return None


# ─── Проверка совместимости категорий ───

PRODUCT_TYPE_KEYWORDS = {
    "коляска": ["коляска", "кресло-коляска", "кресло коляска", "wheelchair"],
    "электроприставка": ["электроприставка", "приставка"],
    "кровать": ["кровать", "bed"],
    "ходунки": ["ходунки", "роллатор", "walker"],
    "опора": ["опора", "трость", "костыль"],
    "подъёмник": ["подъёмник", "подъемник", "lift"],
    "вертикализатор": ["вертикализатор", "stander"],
    "скутер": ["скутер", "scooter"],
    "тренажёр": ["тренажер", "тренажёр", "велоэргометр"],
    "обувь": ["обувь", "ботинки", "полусапоги", "сандалии", "туфли", "стельки", "вкладные"],
    "ортез": ["ортез", "бандаж", "корсет", "тутор"],
    "протез": ["протез"],
    "подушка": ["подушка", "cushion"],
    "кресло-стул": ["кресло-стул", "стул с санитарным"],
    "слуховой": ["слуховой", "hearing"],
}


def get_product_type(text: str) -> set[str]:
    """Определить тип изделия по тексту."""
    # Нормализация: заменяем латинские буквы-двойники на кириллические
    text_lower = text.lower()
    latin_to_cyrillic = str.maketrans('abcehkmoptxy', 'абсенкмортху')
    text_lower = text_lower.translate(latin_to_cyrillic)
    found = set()
    for ptype, keywords in PRODUCT_TYPE_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            found.add(ptype)
    return found


def types_compatible(our_name: str, catalog_name: str) -> bool:
    """Проверить что типы товаров совместимы."""
    our_types = get_product_type(our_name)
    cat_types = get_product_type(catalog_name)

    # Если оба определены и нет пересечения - несовместимо
    if our_types and cat_types and not (our_types & cat_types):
        return False
    return True


# ─── Шаг 1: Сбор каталога (код + название) ───

async def get_subsection_ids(session: aiohttp.ClientSession) -> list[int]:
    """Получить все ID подразделов."""
    html = await fetch(session, CATALOG_LIST_URL)
    if not html:
        return []
    # Ищем ссылки вида /ru-RU/product/product/order86n?subsection=123
    ids = re.findall(r'subsection=(\d+)', html)
    unique = list(dict.fromkeys(int(i) for i in ids))
    logger.info("Найдено подразделов: %d", len(unique))
    return unique


async def get_products_from_subsection(
    session: aiohttp.ClientSession, subsection_id: int
) -> list[dict]:
    """Собрать товары (код + название) из подраздела с пагинацией."""
    products = []
    page = 1
    while True:
        url = f"{SUBSECTION_URL}?subsection={subsection_id}&page={page}"
        html = await fetch(session, url)
        if not html:
            break

        # Парсим карточки товаров
        # Формат: <a href="/ru-RU/product/view/CODE">NAME</a>
        items = re.findall(
            r'href="/ru-RU/product/view/([\d\-\.]+)"[^>]*>\s*(.+?)\s*</a>',
            html, re.DOTALL
        )
        if not items:
            break

        for code, name in items:
            name_clean = re.sub(r'<[^>]+>', '', name).strip()
            name_clean = re.sub(r'\s+', ' ', name_clean)
            if name_clean and code not in [p["code"] for p in products]:
                products.append({"code": code, "name": name_clean})

        # Проверяем наличие следующей страницы
        if f'page={page + 1}' not in html:
            break
        page += 1
        await asyncio.sleep(REQUEST_DELAY)

    return products


async def collect_catalog(session: aiohttp.ClientSession) -> list[dict]:
    """Собрать весь каталог: код + название для каждого товара."""
    subsection_ids = await get_subsection_ids(session)
    all_products = []
    seen_codes = set()

    for i, sub_id in enumerate(subsection_ids):
        logger.info("Подраздел %d/%d (id=%d)...", i + 1, len(subsection_ids), sub_id)
        products = await get_products_from_subsection(session, sub_id)
        for p in products:
            if p["code"] not in seen_codes:
                all_products.append(p)
                seen_codes.add(p["code"])
        await asyncio.sleep(REQUEST_DELAY)

    logger.info("Каталог собран: %d товаров", len(all_products))
    return all_products


# ─── Шаг 2: Матчинг (СТРОГИЙ) ───

def normalize(text: str) -> str:
    """Нормализовать текст для сравнения."""
    text = text.lower().strip()
    text = re.sub(r'[«»"\'(){}[\]]', '', text)
    text = re.sub(r'[,;:!?]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text


def get_manufacturer_variants(manufacturer: str) -> list[str]:
    """Получить все варианты написания производителя."""
    mfr_lower = manufacturer.lower().strip()

    # Поиск в маппинге
    for key, aliases in MANUFACTURER_ALIASES.items():
        if key in mfr_lower or mfr_lower in key:
            return aliases
        for alias in aliases:
            if alias in mfr_lower or mfr_lower in alias:
                return aliases

    # Если нет в маппинге - используем как есть
    return [mfr_lower]


def transliterate_to_russian(text: str) -> str:
    """Транслитерировать латиницу в кириллицу."""
    try:
        return translit(text, 'ru')
    except Exception:
        return text


# Слова-стоп: если модель состоит только из них - не матчим
MODEL_STOPWORDS = {
    "mini", "maxi", "max", "pro", "plus", "lite", "basic", "standard",
    "classic", "comfort", "active", "sport", "junior", "senior",
    "small", "medium", "large", "xl", "xxl", "s", "m", "l",
    "new", "neo",  # neo без контекста слишком общий
    "размер", "комплект", "цвет",
}


def model_matches(model: str, catalog_name: str) -> bool:
    """
    Проверить, что модель ТОЧНО присутствует в названии каталога.
    Пробует и оригинальное написание, и транслитерацию.
    Модель должна быть целым словом/фразой, не подстрокой другого слова.
    """
    model_clean = normalize(model)
    name_clean = normalize(catalog_name)

    if not model_clean or len(model_clean) < 2:
        return False

    model_words = model_clean.split()

    # Если модель из одного общего слова (maxi, lite, pro) - не матчим
    if len(model_words) == 1 and model_words[0] in MODEL_STOPWORDS:
        return False

    # Пробуем оригинал и транслитерацию
    model_variants = [model_clean]
    translit_model = normalize(transliterate_to_russian(model))
    if translit_model != model_clean:
        model_variants.append(translit_model)

    for variant in model_variants:
        if _check_model_in_name(variant, name_clean):
            return True
    return False


def _check_model_in_name(model_clean: str, name_clean: str) -> bool:
    """Проверить одну вариацию модели в названии."""
    model_words = model_clean.split()

    if len(model_words) == 1:
        word = model_words[0]
        if len(word) <= 2:
            return False
        # Проверяем как целое слово (не часть "SuperMaxi42" или "размер MAXI")
        pattern = r'(?<!\w)' + re.escape(word) + r'(?!\w)'
        match = re.search(pattern, name_clean)
        if not match:
            return False
        # Доп. проверка: слово перед моделью не должно быть "размер", "комплект" и т.д.
        before = name_clean[:match.start()].strip()
        before_word = before.split()[-1] if before.split() else ""
        if before_word in ("размер", "размера", "р.", "р", "комплект", "цвет"):
            return False
        return True
    else:
        # Многословная модель - все слова в правильном порядке
        all_present = all(w in name_clean for w in model_words)
        if not all_present:
            return False
        last_pos = -1
        for w in model_words:
            pos = name_clean.find(w, last_pos + 1)
            if pos == -1:
                return False
            last_pos = pos
        return True


def manufacturer_in_catalog(manufacturer: str, catalog_name: str) -> bool:
    """Проверить, что производитель упомянут в названии каталога."""
    variants = get_manufacturer_variants(manufacturer)
    name_lower = normalize(catalog_name)

    for variant in variants:
        if variant in name_lower:
            return True

    # Попробуем транслитерацию производителя
    mfr_translit = normalize(transliterate_to_russian(manufacturer))
    if mfr_translit and mfr_translit in name_lower:
        return True

    return False


def match_products(spreadsheet_rows: list[dict], catalog: list[dict]) -> list[dict]:
    """
    Сматчить товары из таблицы с каталогом.

    СТРОГИЕ правила:
    1. Производитель должен присутствовать в названии каталога
    2. Модель должна присутствовать как целое слово/фраза
    3. Если несколько совпадений - берём лучшее по similarity
    """
    results = []

    # Предрассчитываем нормализованные имена каталога
    catalog_normalized = [(c, normalize(c["name"])) for c in catalog]

    # Кэш транслитераций
    translit_cache = {}

    def get_model_variants(model: str) -> list[str]:
        model_clean = normalize(model)
        if model_clean not in translit_cache:
            translit_model = normalize(transliterate_to_russian(model))
            variants = [model_clean]
            if translit_model != model_clean:
                variants.append(translit_model)
            translit_cache[model_clean] = variants
        return translit_cache[model_clean]

    def get_mfr_variants_cached(manufacturer: str) -> list[str]:
        key = f"mfr_{manufacturer.lower()}"
        if key not in translit_cache:
            base = get_manufacturer_variants(manufacturer)
            mfr_translit = normalize(transliterate_to_russian(manufacturer))
            all_v = list(base)
            if mfr_translit and mfr_translit not in all_v:
                all_v.append(mfr_translit)
            translit_cache[key] = all_v
        return translit_cache[key]

    for idx, row in enumerate(spreadsheet_rows):
        if idx % 200 == 0:
            logger.info("Матчинг: %d/%d...", idx, len(spreadsheet_rows))
        row_num = row.get("row_num", "")
        name = row.get("name", "")
        manufacturer = row.get("manufacturer", "")
        model = row.get("model", "")
        existing_tsr = row.get("existing_tsr", "").strip()

        result = {
            "row_num": row_num,
            "name": name,
            "manufacturer": manufacturer,
            "model": model,
            "matched_code": "",
            "match_type": "not_found",
            "match_confidence": 0.0,
            "catalog_name": "",
        }

        # Если уже есть код ТСР в таблице - используем его
        if existing_tsr:
            # Проверяем что он есть в каталоге
            cat_item = next((c for c in catalog if c["code"] == existing_tsr), None)
            if cat_item:
                result["matched_code"] = existing_tsr
                result["match_type"] = "existing"
                result["match_confidence"] = 1.0
                result["catalog_name"] = cat_item["name"]
                results.append(result)
                continue
            # Код не найден в каталоге - пробуем матчинг как обычно

        if not model:
            results.append(result)
            continue

        # Предрассчитываем варианты модели и производителя
        model_vars = get_model_variants(model) if model else []
        mfr_vars = get_mfr_variants_cached(manufacturer) if manufacturer else []

        # Проверяем стоп-слова
        model_clean = normalize(model) if model else ""
        model_words = model_clean.split()
        is_stopword = len(model_words) == 1 and model_words[0] in MODEL_STOPWORDS if model_words else False

        if is_stopword or not model:
            results.append(result)
            continue

        # Ищем в каталоге: производитель + модель
        candidates = []

        for cat_item, cat_name_norm in catalog_normalized:
            # Шаг 1: производитель
            mfr_match = any(v in cat_name_norm for v in mfr_vars) if mfr_vars else True

            # Шаг 2: модель
            mdl_match = False
            for variant in model_vars:
                if _check_model_in_name(variant, cat_name_norm):
                    mdl_match = True
                    break

            if mfr_match and mdl_match:
                # Шаг 3: проверка совместимости типов
                if types_compatible(name, cat_item["name"]):
                    candidates.append({
                        "code": cat_item["code"],
                        "name": cat_item["name"],
                        "match_type": "strict",
                    })

        # Если нет с производителем - пробуем только по модели (если модель уникальная)
        if not candidates and model and len(model) >= 4:
            model_only_matches = []
            for cat_item, cat_name_norm in catalog_normalized:
                mdl_match = False
                for variant in model_vars:
                    if _check_model_in_name(variant, cat_name_norm):
                        mdl_match = True
                        break
                if mdl_match and types_compatible(name, cat_item["name"]):
                    model_only_matches.append({
                        "code": cat_item["code"],
                        "name": cat_item["name"],
                        "match_type": "model_only",
                    })

            # Только если нашлось мало (уникальная модель)
            if 1 <= len(model_only_matches) <= 5:
                candidates = model_only_matches

        if candidates:
            if len(candidates) == 1:
                best = candidates[0]
                best["similarity"] = 1.0
            else:
                # Считаем similarity только для кандидатов (не для всего каталога)
                name_norm = normalize(name)
                for c in candidates:
                    c["similarity"] = SequenceMatcher(None, name_norm, normalize(c["name"])).ratio()
                best = max(candidates, key=lambda c: c["similarity"])
            result["matched_code"] = best["code"]
            result["match_type"] = best["match_type"]
            result["match_confidence"] = best.get("similarity", 1.0)
            result["catalog_name"] = best["name"]

        results.append(result)

    matched = sum(1 for r in results if r["matched_code"])
    not_matched = sum(1 for r in results if not r["matched_code"])
    logger.info("Матчинг: найдено %d, не найдено %d", matched, not_matched)

    return results


# ─── Шаг 3: Парсинг страниц товаров ───

def parse_product_page(html: str, product_code: str) -> dict:
    """Извлечь данные из HTML страницы товара."""
    result = {
        "product_code": product_code,
        "tsr_code": "",
        "okpd2": "",
        "ru_number": "",
        "ru_pdf_url": "",
    }

    # Код ТСР (первые 3 части кода)
    parts = product_code.split(".")
    if len(parts) >= 1:
        # Код ТСР = всё до последнего компонента (4 цифры)
        tsr = re.match(r'(\d{2}-\d{2}-\d{2})', product_code)
        if tsr:
            result["tsr_code"] = tsr.group(1)

    # ОКПД2 - ищем паттерн XX.XX.XX.XXX
    okpd2_patterns = [
        r'ОКПД\s*2?\s*[:\-]?\s*(\d{2}\.\d{2}\.\d{2}\.\d{3})',
        r'(\d{2}\.\d{2}\.\d{2}\.\d{3})',
    ]
    for pat in okpd2_patterns:
        m = re.search(pat, html)
        if m:
            result["okpd2"] = m.group(1)
            break

    # Номер РУ - ищем РЗН YYYY/XXXXX или ФСЗ YYYY/XXXXX
    ru_patterns = [
        r'(РЗН\s*\d{4}/\d+)',
        r'(ФСЗ\s*\d{4}/\d+)',
    ]
    for pat in ru_patterns:
        m = re.search(pat, html)
        if m:
            result["ru_number"] = m.group(1)
            break

    # PDF ссылка на скан РУ
    pdf_patterns = [
        r'(https?://static-tsr\.fss\.ru/product/[a-f0-9/]+\.pdf)',
        r'href="([^"]*static-tsr[^"]*\.pdf)"',
    ]
    for pat in pdf_patterns:
        m = re.search(pat, html)
        if m:
            result["ru_pdf_url"] = m.group(1)
            break

    return result


async def fetch_product_details(
    session: aiohttp.ClientSession, product_codes: list[str]
) -> dict[str, dict]:
    """Загрузить и спарсить страницы товаров. Возвращает code → data."""
    details = {}
    total = len(product_codes)

    for i, code in enumerate(product_codes):
        if i % 50 == 0:
            logger.info("Загрузка страниц: %d/%d...", i, total)

        url = f"{PRODUCT_URL}/{code}"
        html = await fetch(session, url)
        if html:
            details[code] = parse_product_page(html, code)
        await asyncio.sleep(REQUEST_DELAY)

    logger.info("Загружено страниц: %d/%d", len(details), total)
    return details


# ─── Шаг 4: Сборка результатов ───

def save_results_csv(matches: list[dict], details: dict[str, dict], output_path: str = OUTPUT_CSV):
    """Сохранить финальный CSV с результатами."""
    fieldnames = [
        "row_num", "name", "matched_code", "match_type", "match_confidence",
        "catalog_name", "tsr_code", "okpd2", "ru_number", "ru_pdf_url",
    ]

    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for match in matches:
            code = match.get("matched_code", "")
            detail = details.get(code, {})

            row = {
                "row_num": match["row_num"],
                "name": match["name"],
                "matched_code": code,
                "match_type": match["match_type"],
                "match_confidence": f"{match.get('match_confidence', 0):.2f}",
                "catalog_name": match.get("catalog_name", ""),
                "tsr_code": detail.get("tsr_code", ""),
                "okpd2": detail.get("okpd2", ""),
                "ru_number": detail.get("ru_number", ""),
                "ru_pdf_url": detail.get("ru_pdf_url", ""),
            }
            writer.writerow(row)

    logger.info("Результаты сохранены: %s", output_path)


# ─── Сохранение/загрузка прогресса ───

def save_progress(data: dict):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


def load_progress() -> dict:
    if Path(PROGRESS_FILE).exists():
        with open(PROGRESS_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


# ─── Загрузка таблицы ───

def load_spreadsheet(csv_path: str = SPREADSHEET_CSV) -> list[dict]:
    """Загрузить таблицу из CSV и вернуть нормализованные строки."""
    rows = []
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, raw in enumerate(reader, start=2):  # строка 1 = заголовок
            rows.append({
                "row_num": str(i),
                "name": raw.get("Товар", "").strip(),
                "manufacturer": raw.get("Производитель", "").strip(),
                "model": raw.get("Модель", "").strip(),
                "existing_tsr": raw.get("Код ТСР", "").strip(),
                "existing_ru": raw.get("Номер РУ", "").strip(),
            })
    return rows


# ─── Основной пайплайн ───

async def run_full():
    """Полный цикл: каталог → матчинг → парсинг → CSV."""
    progress = load_progress()

    # Шаг 1: Каталог
    catalog = progress.get("catalog")
    if catalog:
        logger.info("Каталог загружен из кэша: %d товаров", len(catalog))
    else:
        async with aiohttp.ClientSession() as session:
            catalog = await collect_catalog(session)
        progress["catalog"] = catalog
        save_progress(progress)

    # Шаг 2: Загрузка таблицы + матчинг
    spreadsheet = load_spreadsheet()
    matches = match_products(spreadsheet, catalog)

    # Шаг 3: Загрузка страниц товаров
    codes_to_fetch = [
        m["matched_code"] for m in matches
        if m["matched_code"] and m["matched_code"] not in progress.get("details", {})
    ]
    existing_details = progress.get("details", {})

    if codes_to_fetch:
        logger.info("Нужно загрузить %d новых страниц", len(codes_to_fetch))
        async with aiohttp.ClientSession() as session:
            new_details = await fetch_product_details(session, codes_to_fetch)
        existing_details.update(new_details)
        progress["details"] = existing_details
        save_progress(progress)
    else:
        logger.info("Все страницы уже в кэше")

    # Шаг 4: Сборка CSV
    save_results_csv(matches, existing_details)

    return matches, existing_details
