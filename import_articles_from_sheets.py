import json
import gspread
import os
from main import SPREADSHEET_NAME, WORKSHEET_NAME
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

load_dotenv("secrets.env")  # грузим GOOGLE_CREDS_JSON

def get_sheet_data():
    service_json_raw = os.getenv("GOOGLE_CREDS_JSON")

    if not service_json_raw:
        raise RuntimeError("❌ GOOGLE_CREDS_JSON не найден в secrets.env")

    try:
        service_json = json.loads(service_json_raw)
    except json.JSONDecodeError:
        raise RuntimeError("❌ GOOGLE_CREDS_JSON содержит неверный JSON")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_dict(service_json, scope)
    client = gspread.authorize(creds)

    sheet = client.open(SPREADSHEET_NAME).worksheet(WORKSHEET_NAME)
    return sheet.get_all_records()
