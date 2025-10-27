from sqlalchemy import create_engine, text
import logging

# --- Настройки подключения к БД SQLite ---
DB_PATH = 'your_database.db' # Укажите путь к файлу вашей БД
DATABASE_URL = f'sqlite:///{DB_PATH}'

engine = create_engine(DATABASE_URL)

# --- Настройки логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_tables():
    """Создаёт таблицы в БД, если они не существуют."""
    logger.info("Начинаю создание таблиц...")

    # SQL-запросы для создания таблиц
    create_weeks = """
    CREATE TABLE IF NOT EXISTS weeks (
        week_id INTEGER PRIMARY KEY, -- AUTO_INCREMENT в SQLite
        week_start_date DATE NOT NULL
    );
    """

    create_stores = """
    CREATE TABLE IF NOT EXISTS stores (
        store_id INTEGER PRIMARY KEY, -- Не AUTO_INCREMENT, вы вручную указываете ID
        store_name VARCHAR(255)
    );
    """

    create_departments = """
    CREATE TABLE IF NOT EXISTS departments (
        department_id INTEGER PRIMARY KEY, -- Не AUTO_INCREMENT, вы вручную указываете ID
        department_name VARCHAR(255)
    );
    """

    create_weekly_data = """
    CREATE TABLE IF NOT EXISTS weekly_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        week_id INTEGER NOT NULL,
        store_id INTEGER NOT NULL,
        department_id INTEGER NOT NULL,
        uto_value REAL,
        bests_value REAL,
        tc_percent_value REAL,
        twenty_eighty_percent_value REAL,
        turnover_value REAL,
        gold_value REAL,
        uto_rating INTEGER,
        bests_rating INTEGER,
        tc_percent_rating INTEGER,
        twenty_eighty_percent_rating INTEGER,
        FOREIGN KEY (week_id) REFERENCES weeks(week_id),
        FOREIGN KEY (store_id) REFERENCES stores(store_id),
        FOREIGN KEY (department_id) REFERENCES departments(department_id),
        UNIQUE (week_id, store_id, department_id) -- Уникальный индекс
    );
    """

    create_index_week_dept = """
    CREATE INDEX IF NOT EXISTS idx_week_dept ON weekly_data (week_id, department_id);
    """

    create_index_store = """
    CREATE INDEX IF NOT EXISTS idx_store ON weekly_data (store_id);
    """

    create_index_dept = """
    CREATE INDEX IF NOT EXISTS idx_dept ON weekly_data (department_id);
    """

    # ИСПРАВЛЕНО: Обычная строка присвоена переменной
    create_index_week_start = """
    CREATE INDEX IF NOT EXISTS idx_week_start_date ON weeks (week_start_date); -- Индекс на дату недели
    """

    # Выполнение запросов
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # Все строки запросов передаются через переменные
            conn.execute(text(create_weeks))
            conn.execute(text(create_stores))
            conn.execute(text(create_departments))
            conn.execute(text(create_weekly_data))
            conn.execute(text(create_index_week_dept))
            conn.execute(text(create_index_store))
            conn.execute(text(create_index_dept))
            # ИСПРАВЛЕНО: Теперь правильно передаётся переменная
            conn.execute(text(create_index_week_start))
            trans.commit()
            logger.info("Таблицы успешно созданы (или уже существовали).")
        except Exception as e:
            trans.rollback()
            logger.error(f"Ошибка при создании таблиц: {e}")
            raise
