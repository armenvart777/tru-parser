"""
Второй этап: поиск РУ через API Росздравнадзора.

Для каждого ненайденного товара:
1. Ищем все РУ по производителю через API
2. Матчим модель из таблицы с modelsDescription/name из РУ
3. Извлекаем noRu и okpd2CodeId
"""

import asyncio
import aiohttp
import ssl
import json
import csv
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)

ROSZDRAV_API = "https://elk.roszdravnadzor.gov.ru/public-gateway/registered-med-product/api/v1/med-product/filter-public"
OKPD2_API = "https://elk.roszdravnadzor.gov.ru/public-gateway/registered-med-product/api/v1/dictionaries/all"


def get_ssl_context():
    ...


async def fetch_ru_by_producer(session: aiohttp.ClientSession, producer_name: str) -> list[dict]:
    """Получить все РУ для производителя."""
    ...


def match_model_to_ru(model: str, product_name: str, ru_items: list[dict]) -> dict | None:
    """Сматчить модель с РУ по modelsDescription и name."""
    ...


async def run_stage2(spreadsheet_csv: str = "spreadsheet.csv", stage1_csv: str = "tru_data.csv"):
    """Второй этап: поиск через Росздравнадзор API."""
    ...
