import pandas as pd
from sqlalchemy import create_engine, text
import os
import logging
from datetime import datetime, timedelta

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Подключение к БД ---
# Используем тот же путь, что и в rating_module.py
DB_PATH = os.path.join(os.path.dirname(__file__), 'rating_system.db')
DATABASE_URL = f'sqlite:///{DB_PATH}'
engine = create_engine(DATABASE_URL)

# --- Определение ожидаемых столбцов ---
# Убедитесь, что порядок и названия соответствуют новой структуре CSV
EXPECTED_CSV_COLUMNS = [
    'year', 'week_number', 'store_id', 'department_id',
    'uto_value', 'bests_value', 'tc_percent_value', 'twenty_eighty_percent_value',
    'turnover_value', 'gold_value',
    'uto_rating', 'bests_rating', 'tc_percent_rating', 'twenty_eighty_percent_rating'
]

def iso_year_start(y):
    """Возвращает дату понедельника первой недели ISO-календаря для года y."""
    fourth_jan = datetime(y, 1, 4)
    delta = timedelta(fourth_jan.isoweekday() - 1)
    return fourth_jan - delta

def iso_to_gregorian(year, week, day):
    """Преобразует ISO-номер недели в григорианскую дату."""
    year_start = iso_year_start(year)
    return year_start + timedelta(weeks=week - 1, days=day - 1)

def validate_csv(df: pd.DataFrame) -> bool:
    """Проверяет структуру и типы данных DataFrame."""
    logger.info("Начинаю валидацию CSV...")
    
    if not set(EXPECTED_CSV_COLUMNS).issubset(set(df.columns)):
        missing_cols = set(EXPECTED_CSV_COLUMNS) - set(df.columns)
        logger.error(f"Отсутствуют ожидаемые столбцы: {missing_cols}")
        return False

    # Проверка типов (грубо)
    try:
        df[['year', 'week_number', 'store_id', 'department_id']].apply(pd.to_numeric, downcast='integer') # Проверяем, можно ли в int
        # Проверяем числовые значения (включая рейтинги)
        numeric_cols = [col for col in EXPECTED_CSV_COLUMNS if col not in ['year', 'week_number', 'store_id', 'department_id']]
        df[numeric_cols].apply(pd.to_numeric, errors='raise') # errors='raise' вызовет исключение, если не получится
    except (ValueError, TypeError) as e:
        logger.error(f"Ошибка типа данных: {e}")
        return False

    # Проверка уникальности комбинации year, week_number, store_id, department_id
    # (если в weekly_data есть UNIQUE constraint)
    duplicates = df.duplicated(subset=['year', 'week_number', 'store_id', 'department_id'])
    if duplicates.any():
        logger.error(f"Найдены дубликаты строк для комбинаций (year, week_number, store_id, department_id) в CSV.")
        print(df[duplicates]) # Печатаем дубликаты для проверки
        return False

    logger.info("Валидация CSV прошла успешно.")
    return True

def load_historical_data(csv_file_path: str):
    """Загружает исторические данные из CSV в таблицу weekly_data."""
    logger.info(f"Начинаю загрузку исторических данных из: {csv_file_path}")
    
    try:
        df = pd.read_csv(csv_file_path, sep=';') # Укажите правильный разделитель
    except FileNotFoundError:
        logger.error(f"Файл не найден: {csv_file_path}")
        return
    except Exception as e:
        logger.error(f"Ошибка при чтении CSV: {e}")
        return

    if not validate_csv(df):
        logger.error("CSV не прошёл валидацию. Загрузка отменена.")
        return

    # --- Код для вставки данных ---

    # 1. Преобразование year, week_number в week_start_date (понедельник недели)
    logger.info("Вычисление week_start_date из year и week_number...")
    df['week_start_date'] = df.apply(lambda row: iso_to_gregorian(int(row['year']), int(row['week_number']), 1), axis=1)
    df['week_start_date'] = df['week_start_date'].dt.date # Преобразуем в формат date

    # 2. Загрузка соответствия week_start_date -> week_id (из таблицы weeks)
    logger.info("Загрузка соответствия week_start_date -> week_id...")
    try:
        with engine.connect() as conn:
            df_weeks_from_db = pd.read_sql("SELECT week_id, week_start_date FROM weeks;", conn)
    except Exception as e:
        logger.error(f"Ошибка при чтении таблицы weeks: {e}")
        return

    df_weeks_from_db['week_start_date'] = pd.to_datetime(df_weeks_from_db['week_start_date']).dt.date

    # 3. Объединение для получения week_id на основе вычисленной week_start_date
    df_with_week_id = df.merge(df_weeks_from_db, on='week_start_date', how='left')

    # Проверка, нашлись ли все даты недель в таблице weeks
    # Если даты нет в weeks, её нужно добавить.
    missing_week_dates = df_with_week_id[df_with_week_id['week_id'].isna()]
    if not missing_week_dates.empty:
        logger.info("Некоторые даты начала недель из CSV отсутствуют в таблице weeks. Добавляю их...")
        unique_missing_dates = missing_week_dates[['week_start_date']].drop_duplicates()
        try:
            unique_missing_dates.to_sql('weeks', engine, if_exists='append', index=False, method='multi')
            logger.info(f"Добавлены {len(unique_missing_dates)} новых дат недель в таблицу weeks.")
        except Exception as e:
            logger.error(f"Ошибка при вставке новых дат недель: {e}")
            return # Прерываем, если не можем добавить даты

        # После вставки, нужно заново получить week_id для этих дат
        # Перезагружаем weeks и повторяем объединение
        logger.info("Повторная загрузка соответствия week_start_date -> week_id после вставки новых дат...")
        try:
            with engine.connect() as conn:
                df_weeks_from_db = pd.read_sql("SELECT week_id, week_start_date FROM weeks;", conn)
        except Exception as e:
            logger.error(f"Ошибка при повторном чтении таблицы weeks: {e}")
            return
        df_weeks_from_db['week_start_date'] = pd.to_datetime(df_weeks_from_db['week_start_date']).dt.date
        df_with_week_id = df.merge(df_weeks_from_db, on='week_start_date', how='left') # Объединяем снова

    # Проверка, нашлись ли теперь все даты
    if df_with_week_id['week_id'].isna().any():
         logger.error("Некоторые вычисленные даты начала недели из CSV всё ещё не найдены в таблице weeks после попытки вставки.")
         print(df_with_week_id[df_with_week_id['week_id'].isna()]) # Покажем проблемные строки
         return

    # 4. Подготовка DataFrame для вставки
    # Включаем week_id (вычисленный из year/week_number) и все остальные столбцы из CSV
    # Столбцы *_rating также включаем
    columns_to_insert = [
        'week_id', 'store_id', 'department_id',
        'uto_value', 'bests_value', 'tc_percent_value', 'twenty_eighty_percent_value',
        'turnover_value', 'gold_value',
        'uto_rating', 'bests_rating', 'tc_percent_rating', 'twenty_eighty_percent_rating'
    ]

    df_for_db = df_with_week_id[columns_to_insert].copy()

    # 5. Вставка в базу данных
    logger.info(f"Загрузка {len(df_for_db)} строк в weekly_data...")
    try:
        # Используем if_exists='append', чтобы добавить к существующим данным
        # method='multi' для более эффективной вставки
        df_for_db.to_sql('weekly_data', engine, if_exists='append', index=False, method='multi')
        logger.info(f"✅ Исторические данные успешно загружены ({len(df_for_db)} строк).")
    except Exception as e:
        logger.error(f"❌ Ошибка при загрузке исторических данных: {e}")
        # Если возникла ошибка уникальности (UNIQUE constraint), это может быть проблемой дубликатов
        # с уже существующими данными в weekly_data. В таком случае, нужно обдумать стратегию
        # (например, пропустить дубликаты или обновить существующие строки).
        # Пока оставим как есть, но имейте в виду.


if __name__ == "__main__":
    historical_csv_path = "historical_ratings_week_num.csv" # Укажите путь к вашему файлу
    load_historical_data(historical_csv_path)
