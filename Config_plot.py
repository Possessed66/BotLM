import os
from dotenv import load_dotenv

# Загружаем переменные из .env (только для сервисного аккаунта)
load_dotenv('secret.env')

# Конфигурация Google API - из secrets.env
SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_CREDENTIALS')

# SPREADSHEET_ID задаем вручную
SPREADSHEET_ID = '1wgSC64AtdF3n2qk58n0hxO3nEgaLQ6e1qJdVKcKqStI'  # <-- Замените на ваш ID

# Проверка наличия обязательных переменных
if not SERVICE_ACCOUNT_FILE:
    raise ValueError("GOOGLE_SERVICE_ACCOUNT_FILE не найден в secrets.env")
if not SPREADSHEET_ID:
    raise ValueError("SPREADSHEET_ID должен быть задан в config.py")
