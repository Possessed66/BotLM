import os
from dotenv import load_dotenv
import json

# Загружаем переменные из .env
load_dotenv('secret.env')

# Конфигурация Google API - из secrets.env
GOOGLE_CREDS_JSON = os.environ.get('GOOGLE_CREDENTIALS')
if not GOOGLE_CREDS_JSON:
    raise ValueError("GOOGLE_CREDENTIALS не найден в secret.env")

try:
    SERVICE_ACCOUNT_INFO = json.loads(GOOGLE_CREDS_JSON)
except json.JSONDecodeError as e:
    raise ValueError(f"Неверный формат JSON в GOOGLE_CREDENTIALS: {e}")

# SPREADSHEET_ID задаем вручную
SPREADSHEET_ID = '1wgSC64AtdF3n2qk58n0hxO3nEgaLQ6e1qJdVKcKqStI'

if not SPREADSHEET_ID:
    raise ValueError("SPREADSHEET_ID должен быть задан")
