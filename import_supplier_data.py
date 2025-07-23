# import_supplier_data.py

import os
import json
import logging
import sqlite3
from google.oauth2.service_account import Credentials
import gspread
from contextlib import contextmanager

# --- Конфигурация ---
# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Загрузка переменных окружения (предполагается, что secret.env находится в той же папке)
from dotenv import load_dotenv
load_dotenv('secret.env')

try:
    GOOGLE_CREDS_JSON = os.environ['GOOGLE_CREDENTIALS']
    ORDERS_SPREADSHEET_NAME = "Копия Заказы МЗ 0.2" # Имя таблицы с данными поставщиков
except KeyError as e:
    raise RuntimeError(f"Отсутствует обязательная переменная окружения: {e}")

# Путь к файлу базы данных SQLite (должен быть в той же папке)
DB_PATH = os.path.join(os.path.dirname(__file__), 'products.db')

# Имя листа с гамма-кластером (нужно для определения уникальных магазинов)
GAMMA_CLUSTER_SHEET_NAME = "Гамма кластер"

# --- Инициализация Google Sheets ---
credentials = Credentials.from_service_account_info(
    json.loads(GOOGLE_CREDS_JSON),
    scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
)
gc = gspread.authorize(credentials)

# --- Контекстный менеджер для SQLite ---
@contextmanager
def get_db_connection():
    """Контекстный менеджер для безопасного подключения к SQLite."""
    conn = None
    try:
        if not os.path.exists(DB_PATH):
            logging.critical(f"❌ Файл базы данных не найден: {DB_PATH}")
            raise FileNotFoundError(f"Файл базы данных не найден: {DB_PATH}")
        conn = sqlite3.connect(DB_PATH)
        # conn.row_factory = sqlite3.Row # Не обязательно для этого скрипта
        yield conn
        conn.commit() # Явно коммитим изменения
    except sqlite3.Error as e:
        logging.error(f"Ошибка подключения/работы с БД: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

# --- Функция для получения уникальных номеров магазинов ---
def get_unique_shops():
    """Получает список уникальных номеров магазинов из gamma_cluster."""
    try:
        orders_spreadsheet = gc.open(ORDERS_SPREADSHEET_NAME)
        gamma_cluster_sheet = orders_spreadsheet.worksheet(GAMMA_CLUSTER_SHEET_NAME)
        
        # Получаем все записи из gamma_cluster
        gamma_records = gamma_cluster_sheet.get_all_records()
        
        # Извлекаем уникальные номера магазинов
        unique_shops = list(set(str(record.get("Магазин", "")).strip() for record in gamma_records if record.get("Магазин")))
        logging.info(f"Найдены магазины: {unique_shops}")
        return unique_shops
    except Exception as e:
        logging.error(f"Ошибка получения списка магазинов: {e}")
        raise

# --- Функция для импорта данных поставщиков ---
def import_supplier_data_for_shop(shop_number: str):
    """
    Импортирует данные поставщиков для конкретного магазина из Google Sheets в SQLite.
    Создает таблицу, если она не существует, и очищает её перед импортом.
    """
    try:
        orders_spreadsheet = gc.open(ORDERS_SPREADSHEET_NAME)
        sheet_name = f"Даты выходов заказов {shop_number}"
        supplier_sheet = orders_spreadsheet.worksheet(sheet_name)
        logging.info(f"Обрабатываю лист: {sheet_name}")
    except gspread.exceptions.WorksheetNotFound:
        logging.warning(f"Лист '{sheet_name}' не найден в таблице '{ORDERS_SPREADSHEET_NAME}'. Пропускаю.")
        return # Просто выходим, если лист не найден
    except Exception as e:
        logging.error(f"Ошибка открытия листа '{sheet_name}': {e}")
        raise

    try:
        # Получаем все данные из листа
        supplier_records = supplier_sheet.get_all_records()
        logging.info(f"Получено {len(supplier_records)} записей для магазина {shop_number}")

        if not supplier_records:
            logging.info(f"Нет данных для импорта в магазин {shop_number}")
            return

        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 1. Создаем таблицу для магазина (если не существует)
            # Используем двойные кавычки для названий столбцов с пробелами
            table_name = f"Даты выходов заказов {shop_number}"
            # Экранируем имя таблицы с помощью двойных кавычек, если оно содержит пробелы
            create_table_query = f'''
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                "Номер осн. пост." TEXT,
                "Название осн. пост." TEXT,
                "Срок доставки в магазин" INTEGER,
                "День выхода заказа" INTEGER,
                "День выхода заказа 2" INTEGER,
                "День выхода заказа 3" INTEGER
            );
            '''
            cursor.execute(create_table_query)
            logging.info(f"Таблица '{table_name}' проверена/создана.")

            # 2. Очищаем таблицу перед импортом (TRUNCATE в SQLite это DELETE без WHERE)
            cursor.execute(f'DELETE FROM "{table_name}";')
            logging.info(f"Таблица '{table_name}' очищена.")

            # 3. Подготавливаем данные для вставки
            # Фильтруем и нормализуем данные
            data_to_insert = []
            for record in supplier_records:
                # Пропускаем строки, где нет номера поставщика
                supplier_id = str(record.get("Номер осн. пост.", "")).strip()
                if not supplier_id:
                    continue

                # Нормализуем числовые поля
                def to_int_or_zero(val):
                    try:
                        return int(str(val).strip())
                    except (ValueError, TypeError):
                        return 0 # Или None, если предпочитаете NULL в БД

                data_to_insert.append((
                    supplier_id,
                    str(record.get("Название осн. пост.", "")).strip(),
                    to_int_or_zero(record.get("Срок доставки в магазин", 0)),
                    to_int_or_zero(record.get("День выхода заказа", 0)),
                    to_int_or_zero(record.get("День выхода заказа 2", 0)),
                    to_int_or_zero(record.get("День выхода заказа 3", 0))
                ))

            if not data_to_insert:
                 logging.info(f"Нет корректных данных для вставки в магазин {shop_number}")
                 return

            # 4. Вставляем данные
            insert_query = f'''
            INSERT INTO "{table_name}" (
                "Номер осн. пост.", "Название осн. пост.", "Срок доставки в магазин",
                "День выхода заказа", "День выхода заказа 2", "День выхода заказа 3"
            ) VALUES (?, ?, ?, ?, ?, ?);
            '''
            cursor.executemany(insert_query, data_to_insert)
            logging.info(f"Вставлено {len(data_to_insert)} записей в таблицу '{table_name}'.")

    except sqlite3.Error as e:
        logging.error(f"Ошибка SQLite при импорте данных для магазина {shop_number}: {e}")
        raise
    except Exception as e:
        logging.error(f"Неожиданная ошибка при импорте данных для магазина {shop_number}: {e}")
        raise

# --- Основная функция ---
def main():
    """Основная функция скрипта."""
    logging.info("Начало импорта данных о поставщиках...")
    
    try:
        shops = get_unique_shops()
        logging.info(f"Начинаю импорт для {len(shops)} магазинов.")
        
        for shop in shops:
            try:
                import_supplier_data_for_shop(shop)
            except Exception as e:
                # Логируем ошибку, но продолжаем импорт для других магазинов
                logging.error(f"Не удалось импортировать данные для магазина {shop}: {e}")
        
        logging.info("Импорт данных о поставщиках завершен.")
        
    except Exception as e:
        logging.critical(f"Критическая ошибка в процессе импорта: {e}")
        raise

if __name__ == "__main__":
    main()
