import os
import json
import sqlite3
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials


load_dotenv("secrets.env")

DB_PATH = "articles.db"
TABLE_NAME = "articles"


def prepare_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            full_key TEXT PRIMARY KEY,
            article_code TEXT,
            store_number TEXT,
            department TEXT,
            name TEXT,
            gamma TEXT,
            supplier_code TEXT,
            supplier_name TEXT,
            is_top_store INTEGER
        )
    """)
    conn.commit()
    return conn


def get_sheet_data():
    service_json_raw = os.getenv("GOOGLE_SERVICE_JSON")
    spreadsheet_name = os.getenv("SPREADSHEET_NAME")
    worksheet_name = os.getenv("WORKSHEET_NAME")

    if not service_json_raw or not spreadsheet_name or not worksheet_name:
        raise RuntimeError("❌ Отсутствуют переменные окружения!")

    try:
        service_json = json.loads(service_json_raw)
    except json.JSONDecodeError:
        raise RuntimeError("❌ GOOGLE_SERVICE_JSON содержит неверный формат JSON")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_dict(service_json, scope)
    client = gspread.authorize(creds)

    sheet = client.open(spreadsheet_name).worksheet(worksheet_name)
    return sheet.get_all_records()


def import_data(records, conn):
    conn.execute(f"DELETE FROM {TABLE_NAME}")
    prepared = []

    for row in records:
        full_key = str(row["Ключ"]).strip()
        article_code = str(row["Артикул"]).strip()
        store_number = str(row["Магазин"]).strip()
        department = str(row["Отдел"]).strip()
        name = str(row["Название"]).strip()
        gamma = str(row["Гамма"]).strip()
        supplier_code = str(row["Номер осн. пост."]).strip()
        supplier_name = str(row["Название осн. пост."]).strip()
        is_top = 1 if str(row.get("Топ в магазине", "")).strip() == "1" else 0

        prepared.append((
            full_key, article_code, store_number, department,
            name, gamma, supplier_code, supplier_name, is_top
        ))

    conn.executemany(f"""
        INSERT INTO {TABLE_NAME} (
            full_key, article_code, store_number, department,
            name, gamma, supplier_code, supplier_name, is_top_store
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, prepared)
    conn.commit()
    print(f"✅ Импортировано записей: {len(prepared)}")


if __name__ == "__main__":
    try:
        conn = prepare_db()
        data = get_sheet_data()
        import_data(data, conn)
        conn.close()
        print("✅ Импорт завершён.")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
