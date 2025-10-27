import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
import logging
from typing import Dict, Any
import os
from datetime import datetime, timedelta

# --- Настройки подключения к БД SQLite ---
DB_PATH = os.path.join(os.path.dirname(__file__), 'rating_system.db')
DATABASE_URL = f'sqlite:///{DB_PATH}'

engine = create_engine(DATABASE_URL)

# --- Настройки логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Определение структуры ожидаемого CSV (обновлённая) ---
EXPECTED_CSV_COLUMNS = {
    'year': 'int', # Новый столбец
    'week_number': 'int', # Новый столбец
    'store_id': 'int',
    'department_id': 'int',
    'uto_value': 'float',
    'bests_value': 'float',
    'tc_percent_value': 'float',
    'twenty_eighty_percent_value': 'float',
    'turnover_value': 'float',
    'gold_value': 'float'
}

# --- Словарь для расчёта рейтинга ---
RATING_LOGIC = {
    'uto_value': {'rating_col': 'uto_rating', 'direction': 'ASC'},  # Меньше - лучше
    'bests_value': {'rating_col': 'bests_rating', 'direction': 'ASC'},
    'tc_percent_value': {'rating_col': 'tc_percent_rating', 'direction': 'ASC'},
    'twenty_eighty_percent_value': {'rating_col': 'twenty_eighty_percent_rating', 'direction': 'DESC'} # Больше - лучше
}

def validate_csv(df: pd.DataFrame) -> bool:
    """Проверяет структуру и типы данных DataFrame."""
    logger.info("Начинаю валидацию CSV...")
    
    if not set(EXPECTED_CSV_COLUMNS.keys()).issubset(set(df.columns)):
        missing_cols = set(EXPECTED_CSV_COLUMNS.keys()) - set(df.columns)
        logger.error(f"Отсутствуют ожидаемые столбцы: {missing_cols}")
        return False

    for col, expected_type in EXPECTED_CSV_COLUMNS.items():
        try:
            if expected_type == 'date':
                pd.to_datetime(df[col])
            elif expected_type == 'int':
                pd.to_numeric(df[col], downcast='integer')
            elif expected_type == 'float':
                pd.to_numeric(df[col], downcast='float')
        except (ValueError, TypeError):
            logger.error(f"Ошибка типа данных в столбце '{col}'. Ожидался тип {expected_type}.")
            return False

    # Проверка уникальности комбинации year, week_number, store_id, department_id
    duplicates = df.duplicated(subset=['year', 'week_number', 'store_id', 'department_id'])
    if duplicates.any():
        logger.error(f"Найдены дубликаты строк для комбинаций (year, week_number, store_id, department_id).")
        print(df[duplicates])
        return False

    logger.info("Валидация CSV прошла успешно.")
    return True

def load_csv_to_db(csv_file_path: str):
    """Загружает валидный CSV в таблицу weekly_data."""
    logger.info(f"Начинаю загрузку CSV: {csv_file_path}")
    
    try:
        df = pd.read_csv(csv_file_path)
    except FileNotFoundError:
        logger.error(f"Файл не найден: {csv_file_path}")
        return
    except Exception as e:
        logger.error(f"Ошибка при чтении CSV: {e}")
        return

    if not validate_csv(df):
        logger.error("CSV не прошёл валидацию. Загрузка отменена.")
        return

    # --- Ключевое изменение: вычисление week_start_date из year и week_number ---
    def iso_year_start(y):
        """Возвращает дату понедельника первой недели ISO-календаря для года y."""
        fourth_jan = datetime(y, 1, 4)
        delta = timedelta(fourth_jan.isoweekday() - 1)
        return fourth_jan - delta

    def iso_to_gregorian(year, week, day):
        """Преобразует ISO-номер недели в григорианскую дату."""
        year_start = iso_year_start(year)
        return year_start + timedelta(weeks=week - 1, days=day - 1)

    # Применяем функцию к каждой строке для вычисления даты начала недели (понедельника)
    df['week_start_date'] = df.apply(lambda row: iso_to_gregorian(int(row['year']), int(row['week_number']), 1), axis=1)
    df['week_start_date'] = df['week_start_date'].dt.date # Преобразуем в формат date

    # Теперь у нас есть 'week_start_date' в формате date
    # Дальнейшая логика аналогична предыдущей версии, но использует 'week_start_date'

    # Получение week_id для каждой даты недели
    unique_week_dates = df[['week_start_date']].drop_duplicates()
    try:
        unique_week_dates.to_sql('weeks', engine, if_exists='append', index=False, method='multi')
        logger.info("Вставлены новые даты недель в таблицу weeks (если были).")
    except sqlalchemy.exc.IntegrityError:
        logger.info("Данные о дате недели уже существуют в таблице weeks.")
    except Exception as e:
        logger.error(f"Ошибка при вставке дат недель: {e}")
        return

    # Используем pandas merge для получения week_id
    try:
        with engine.connect() as conn:
            df_weeks_from_db = pd.read_sql("SELECT week_id, week_start_date FROM weeks;", conn)
    except Exception as e:
        logger.error(f"Ошибка при чтении таблицы weeks: {e}")
        return

    df_weeks_from_db['week_start_date'] = pd.to_datetime(df_weeks_from_db['week_start_date']).dt.date

    df_with_week_id = df.merge(df_weeks_from_db, on='week_start_date', how='left')
    
    if df_with_week_id['week_id'].isna().any():
         logger.error("Некоторые вычисленные даты начала недели из CSV не найдены в таблице weeks после попытки вставки.")
         return

    df_for_db = df_with_week_id[['week_id', 'store_id', 'department_id', 'uto_value', 'bests_value', 'tc_percent_value', 'twenty_eighty_percent_value', 'turnover_value', 'gold_value']].copy()
    
    try:
        df_for_db.to_sql('weekly_data', engine, if_exists='append', index=False, method='multi')
        logger.info(f"Данные из CSV успешно загружены в таблицу weekly_data.")
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных в БД: {e}")
        return

def calculate_ratings_for_new_data():
    """
    Вычисляет рейтинги для новых записей в weekly_data.
    Новые - это те, у которых хотя бы один из столбцов _rating равен NULL.
    Использует UPDATE ... WHERE ... IN (SELECT ...) для совместимости с SQLite.
    """
    logger.info("Начинаю расчёт рейтингов для новых данных...")

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # Получаем список уникальных week_id, department_id, для которых есть NULL в рейтинге
            query_get_weeks_deps = text("""
                SELECT DISTINCT week_id, department_id
                FROM weekly_data
                WHERE uto_rating IS NULL OR bests_rating IS NULL OR tc_percent_rating IS NULL OR twenty_eighty_percent_rating IS NULL
            """)
            result = conn.execute(query_get_weeks_deps)
            weeks_deps_to_update = result.fetchall()

            for week_id, dept_id in weeks_deps_to_update:
                logger.info(f"Пересчитываю рейтинги для недели {week_id}, отдела {dept_id}")

                for value_col, rating_info in RATING_LOGIC.items():
                    rating_col = rating_info['rating_col']
                    direction = rating_info['direction']

                    # Подзапрос для вычисления новых рейтингов
                    subquery = f"""
                        SELECT id,
                               ROW_NUMBER() OVER (
                                   PARTITION BY week_id, department_id
                                   ORDER BY {value_col} {direction}
                               ) AS new_rating
                        FROM weekly_data
                        WHERE week_id = ? AND department_id = ? AND {value_col} IS NOT NULL
                    """

                    # Основной UPDATE запрос
                    update_query = text(f"""
                        UPDATE weekly_data
                        SET {rating_col} = (
                            SELECT new_rating
                            FROM (
                                {subquery}
                            ) AS ranked_subquery
                            WHERE weekly_data.id = ranked_subquery.id
                        )
                        WHERE (week_id, department_id) = (?, ?)
                          AND {value_col} IS NOT NULL;
                    """)
                    # Выполняем запрос с параметрами
                    conn.execute(update_query, (week_id, dept_id, week_id, dept_id))

            trans.commit()
            logger.info("Расчёт рейтингов завершён успешно.")
        except Exception as e:
            trans.rollback()
            logger.error(f"Ошибка при расчёте рейтингов: {e}")
            raise

def process_csv_and_update_ratings(csv_file_path: str):
    """
    Основная функция: загружает CSV и пересчитывает рейтинги.
    """
    logger.info(f"Запуск процесса обработки CSV: {csv_file_path}")
    load_csv_to_db(csv_file_path)
    calculate_ratings_for_new_data()
    logger.info("Процесс обработки CSV завершён.")
