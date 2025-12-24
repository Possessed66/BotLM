

import csv
import sqlite3
import os
from datetime import datetime, timedelta
import logging

# --- Убедитесь, что DB_PATH указывает на нужный файл ---
DB_PATH = "articles.db"  # <-- Путь к вашей базе

def parse_date(date_str):
    """Парсинг даты из строки в формате DD.MM.YYYY"""
    if not date_str or not date_str.strip():
        return None
    try:
        return datetime.strptime(date_str.strip(), "%d.%m.%Y").date()
    except ValueError:
        logging.warning(f"⚠️ Некорректный формат даты: {date_str}")
        return None

def generate_holiday_dates(start_date, end_date):
    """Генерирует список всех дат между start_date и end_date (включительно)"""
    if not start_date or not end_date:
        return []
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates

def import_holidays_from_csv(csv_path):
    """Импорт данных о каникулах из CSV в SQLite"""
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"❌ Файл базы данных не найден: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # --- Проверим, существуют ли нужные колонки, и добавим при необходимости ---
    tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    for (table_name,) in tables:
        if table_name.startswith("Даты выходов заказов "):
            # Проверяем и добавляем колонки
            try:
                cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "Каникулы список" TEXT DEFAULT "";')
                logging.info(f"✅ Добавлена колонка 'Каникулы список' в таблицу '{table_name}'")
            except sqlite3.OperationalError:
                # Уже есть
                pass

            try:
                cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "Исключения список" TEXT DEFAULT "";')
                logging.info(f"✅ Добавлена колонка 'Исключения список' в таблицу '{table_name}'")
            except sqlite3.OperationalError:
                # Уже есть
                pass

    with open(csv_path, 'r', encoding='utf-8-sig') as f:  # utf-8-sig — чтобы обработать BOM
        reader = csv.DictReader(f, delimiter=';')  

        updated_count = 0
        for row in reader:
            shop_number = str(row.get('Номер магазина', '')).strip()
            supplier_id = str(row.get('Код поставщика', '')).strip()

            if not shop_number or not supplier_id:
                logging.warning(f"⚠️ Пропущена строка с пустым магазином или поставщиком: {row}")
                continue

            # Парсим даты
            last_order_before = parse_date(row.get('Дата последнего заказа перед каникулами', ''))
            first_order_after = parse_date(row.get('Дата первого заказа после каникул', ''))

            # --- Обработка исключений ---
            order_exception_date_str = row.get('Дата заказа с поставкой в период каникул', '').strip()
            delivery_exception_date_str = row.get('Дата поставки в период каникул', '').strip()

            order_exception_date = parse_date(order_exception_date_str)
            delivery_exception_date = parse_date(delivery_exception_date_str)

            holidays = set()
            exceptions = set()

            # --- Генерация каникул ---
            if last_order_before and first_order_after:
                # Генерируем все даты между последним заказом и первым после — это и есть "каникулы"
                holiday_dates = generate_holiday_dates(last_order_before + timedelta(days=1), first_order_after - timedelta(days=1))
                holidays.update(holiday_dates)

            # --- Генерация исключений ---
            if order_exception_date:
                exceptions.add(order_exception_date)
            if delivery_exception_date:
                exceptions.add(delivery_exception_date)

            # --- Преобразование в строки ---
            holidays_str = ", ".join(sorted(d.strftime("%d.%m.%Y") for d in holidays))
            exceptions_str = ", ".join(sorted(d.strftime("%d.%m.%Y") for d in exceptions))

            table_name = f"Даты выходов заказов {shop_number}"

            # Проверяем, существует ли таблица
            cursor.execute(f'''
                SELECT name FROM sqlite_master WHERE type='table' AND name=?
            ''', (table_name,))
            if not cursor.fetchone():
                logging.warning(f"⚠️ Таблица '{table_name}' не существует. Пропускаю поставщика {supplier_id}.")
                continue

            # Обновляем запись поставщика
            cursor.execute(f'''
                UPDATE "{table_name}"
                SET "Каникулы список" = ?, "Исключения список" = ?
                WHERE "Номер осн. пост." = ?
            ''', (holidays_str, exceptions_str, supplier_id))

            if cursor.rowcount > 0:
                updated_count += 1
                logging.info(f"✅ Обновлено: Магазин {shop_number}, Поставщик {supplier_id}. Каникулы: {holidays_str or 'нет'}. Исключения: {exceptions_str or 'нет'}")
            else:
                logging.warning(f"⚠️ Не найдена запись: Магазин {shop_number}, Поставщик {supplier_id}")

    conn.commit()
    conn.close()
    logging.info(f"✅ Импорт каникул завершён. Обновлено {updated_count} записей.")
    return updated_count
