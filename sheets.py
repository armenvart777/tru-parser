import csv
import logging
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from config import SPREADSHEET_ID

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SERVICE_ACCOUNT_FILE = "credentials.json"


def get_sheets_service():
    """Создать сервис Google Sheets API."""
    ...


def upload_to_sheets(results: list[dict], sheet_name: str = "Парсинг ТРУ"):
    """Выгрузить результаты в Google Sheets."""
    ...


def upload_from_csv(csv_file: str = "tru_data.csv", sheet_name: str = "Парсинг ТРУ"):
    """Выгрузить данные из CSV в Google Sheets."""
    ...
