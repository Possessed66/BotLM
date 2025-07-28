import os
import json
import pickle
import io 
import re
import gc
import asyncio
import logging
import traceback
import time
import threading
import tracemalloc
import objgraph
import psutil
import sqlite3
import gspread.utils
from contextlib import contextmanager
from aiogram.exceptions import TelegramBadRequest
from aiogram.utils.markdown import markdown_decoration
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from aiogram.types import ReplyKeyboardRemove, File, BufferedInputFile
from aiogram.exceptions import TelegramForbiddenError
from aiogram.filters import Command
from contextlib import suppress
from google.oauth2.service_account import Credentials
import gspread
from gspread.exceptions import APIError, SpreadsheetNotFound
from cachetools import LRUCache



# ===================== ГЛОБАЛЬНАЯ ОБРАБОТКА ОШИБОК =====================
from aiogram.fsm.storage.base import StorageKey

@contextmanager
def get_db_connection():
    """Контекстный менеджер для безопасного подключения к SQLite."""
    conn = None
    try:
        # Проверка существования файла БД
        if not os.path.exists(DB_PATH):
             # Можно добавить логирование ошибки, если файл обязателен
             logging.critical(f"❌ Файл базы данных не найден: {DB_PATH}")
             raise FileNotFoundError(f"Файл базы данных не найден: {DB_PATH}")
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row  # Позволяет обращаться к колонкам по имени
        yield conn
    except sqlite3.Error as e:
        logging.error(f"Ошибка подключения к БД: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

async def global_error_handler(event: types.ErrorEvent, bot: Bot):
    """Централизованный обработчик всех необработанных исключений"""
    exception = event.exception
    update = event.update
    
    # Получаем идентификатор пользователя
    user_id = None
    if update.message:
        user_id = update.message.from_user.id
    elif update.callback_query:
        user_id = update.callback_query.from_user.id
    
    # Формируем сообщение об ошибке
    error_type = type(exception).__name__
    error_message = str(exception) or "Без описания"
    traceback_str = "".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
    
    # Логируем в консоль
    logging.critical(
        f"ГЛОБАЛЬНАЯ ОШИБКА [user:{user_id}]\n"
        f"Type: {error_type}\n"
        f"Message: {error_message}\n"
        f"Traceback:\n{traceback_str}"
    )
    
    # Логируем в Google Sheets
    if user_id:
        try:
            logs_sheet.append_row([
                datetime.now().strftime("%d.%m.%Y %H:%M"),
                str(user_id),
                "CRITICAL_ERROR",
                f"{error_type}: {error_message[:200]}"
            ])
        except Exception as log_ex:
            logging.error(f"Ошибка при логировании: {str(log_ex)}")
    
    # Уведомляем администраторов
    for admin_id in ADMINS:
        try:
            await bot.send_message(
                admin_id,
                f"🚨 <b>Критическая ошибка</b>\n"
                f"• Пользователь: {user_id}\n"
                f"• Тип: {error_type}\n"
                f"• Сообщение: {error_message}\n\n"
                f"<code>{traceback_str[:3500]}</code>",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass
    
    # Отправляем сообщение пользователю
    if user_id:
        try:
            await bot.send_message(
                user_id,
                "⚠️ Произошла непредвиденная ошибка. Администратор уведомлен.\n"
                "Попробуйте позже или начните заново с команды /start",
                reply_markup=ReplyKeyboardRemove()
            )
        except Exception:
            pass
    
    # Очищаем состояние пользователя
    if user_id:
        try:
            state = FSMContext(
                storage=dp.storage,
                key=StorageKey(
                    bot_id=bot.id,
                    chat_id=user_id,
                    user_id=user_id
                )
            )
            await state.clear()
        except Exception:
            pass
    
    return True



# ===================== ПРОФИЛИРОВАНИЕ ПАМЯТИ =====================

def init_tracemalloc():
    """Безопасная инициализация трассировки памяти"""
    if not tracemalloc.is_tracing():
        tracemalloc.start()
        logging.info("Tracemalloc initialized")




async def memory_monitor():
    """Мониторинг использования памяти с расширенной диагностикой"""
    if not tracemalloc.is_tracing():
        tracemalloc.start(10)  # Ограничиваем глубину трассировки
    
    cycle_count = 0
    prev_snapshot = None
    
    while True:
        try:
            process = psutil.Process()
            mem_info = process.memory_info()
            
            # Логируем общее использование
            logging.info(
                f"Memory: RSS={mem_info.rss / 1024 / 1024:.2f}MB, "
                f"VMS={mem_info.vms / 1024 / 1024:.2f}MB"
            )
            
            # Создаем снимок памяти
            snapshot = tracemalloc.take_snapshot()
            
            # Анализируем распределение памяти
            top_stats = snapshot.statistics('lineno')[:5]
            for i, stat in enumerate(top_stats):
                logging.info(
                    f"Alloc {i+1}: {stat.size / 1024:.2f}KB in {stat.count} blocks "
                    f"at {stat.traceback.format()[-1]}"
                )
            
            # Периодический углубленный анализ
            cycle_count += 1
            if cycle_count >= 10:  # Каждые 60 минут
                # Анализ типов объектов
                logging.info("Most common object types:")
                common_types = objgraph.most_common_types(limit=10)
                for obj_type, count in common_types:
                    logging.info(f"  {obj_type}: {count}")
                
                # Анализ роста памяти
                if prev_snapshot:
                    diff_stats = snapshot.compare_to(prev_snapshot, 'lineno')
                    growth_stats = [stat for stat in diff_stats if stat.size_diff > 0][:5]
                    
                    if growth_stats:
                        logging.info("Top memory growth:")
                        for stat in growth_stats:
                            logging.info(
                                f"  +{stat.size_diff / 1024:.2f}KB: "
                                f"{stat.traceback.format()[-1]}"
                            )
                    else:
                        logging.info("No significant memory growth detected")
                
                # Сброс счетчика
                cycle_count = 0
                prev_snapshot = snapshot
                gc.collect()
            
            await asyncio.sleep(1200)  # 20 минут
            
        except Exception as e:
            logging.error(f"Memory monitor error: {str(e)}")
            await asyncio.sleep(60)


def profile_memory(func):
    """Декоратор для профилирования памяти функции"""
    def wrapper(*args, **kwargs):
        # Проверяем и инициализируем tracemalloc при необходимости
        if not tracemalloc.is_tracing():
            tracemalloc.start()
        
        # Запоминаем текущее распределение памяти
        start_snapshot = tracemalloc.take_snapshot()
        
        # Выполняем функцию
        result = func(*args, **kwargs)
        
        # Анализируем использование памяти
        end_snapshot = tracemalloc.take_snapshot()
        top_stats = end_snapshot.compare_to(start_snapshot, 'lineno')
        
        # Логируем результаты
        logging.info(f"Memory profile for {func.__name__}:")
        for stat in top_stats[:5]:
            logging.info(
                f"  {stat.size_diff / 1024:.2f}KB difference, "
                f"Total: {stat.size / 1024:.2f}KB, "
                f"File: {stat.traceback.format()[-1]}"
            )
        
        return result
    return wrapper

# ===================== КОНФИГУРАЦИЯ =====================

DB_PATH = os.path.join(os.path.dirname(__file__), 'articles.db')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

# Сервисный режим
SERVICE_MODE = False
ADMINS = [122086799, 5183727015]

# Кэширование
CACHE_TTL = 43200  # 12 часов
cache = LRUCache(maxsize=500)

# Загрузка переменных окружения
from dotenv import load_dotenv
load_dotenv('secret.env')

# Проверка обязательных переменных
try:
    BOT_TOKEN = os.environ['BOT_TOKEN']
    GOOGLE_CREDS_JSON = os.environ['GOOGLE_CREDENTIALS']
except KeyError as e:
    raise RuntimeError(f"Отсутствует обязательная переменная: {e}")

# Конфигурация Google Sheets
GOOGLE_CREDS = json.loads(GOOGLE_CREDS_JSON)
SPREADSHEET_NAME = "ShopBotData"
STATSS_SHEET_NAME = "Статистика_Пользователей"
ORDERS_SPREADSHEET_NAME = "Копия Заказы МЗ 0.2"
USERS_SHEET_NAME = "Пользователи"
GAMMA_CLUSTER_SHEET = "Гамма кластер"
TASKS_SHEET_NAME = "Задачи"
LOGS_SHEET = "Логи"


# ===================== ИНИЦИАЛИЗАЦИЯ =====================
credentials = Credentials.from_service_account_info(
    GOOGLE_CREDS,
    scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
)
client = gspread.authorize(credentials)

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()
dp.errors.register(global_error_handler)

# Инициализация таблиц
try:
    main_spreadsheet = client.open(SPREADSHEET_NAME)
    users_sheet = main_spreadsheet.worksheet(USERS_SHEET_NAME)
    logs_sheet = main_spreadsheet.worksheet(LOGS_SHEET)
    orders_spreadsheet = client.open(ORDERS_SPREADSHEET_NAME)
    gamma_cluster_sheet = orders_spreadsheet.worksheet(GAMMA_CLUSTER_SHEET)
    print("✅ Google Sheets успешно инициализированы")
except Exception as e:
    print(f"❌ Ошибка инициализации Google Sheets: {str(e)}")
    exit()


# ===================== СОСТОЯНИЯ FSM =====================
class Registration(StatesGroup):
    name = State()
    surname = State()
    position = State()
    shop = State()

class OrderStates(StatesGroup):
    article_input = State()
    shop_selection = State()
    shop_input = State()
    quantity_input = State()
    order_reason_input = State()
    confirmation = State()

class InfoRequest(StatesGroup):
    article_input = State()

class AdminBroadcast(StatesGroup):
    message_input = State()
    target_selection = State()
    manual_ids = State()
    confirmation = State()

class TaskStates(StatesGroup):
    # Состояния для добавления задач
    add_text = State()
    add_link = State()
    add_deadline = State()
    # Состояния для удаления задач
    delete_task = State()
    confirm_delete = State()
    # Состояния для отправки задач
    select_action = State()  # Выбор действия (отправить/статистика)
    select_tasks = State()   # Выбор задач для отправки
    input_task_ids = State() # Ввод ID задач вручную
    select_audience = State() # Выбор аудитории
    input_position = State()
    input_manual_ids = State() # Ввод ID пользователей
    confirmation = State()
    
    # Состояния для статистики
    view_stats = State()     # Просмотр статистики
    input_task_id_for_details = State() # Ввод ID для детализации
    review_selection = State()

# ===================== КЛАВИАТУРЫ =====================
def create_keyboard(buttons: List[str], sizes: tuple, resize=True, one_time=False) -> types.ReplyKeyboardMarkup:
    """Универсальный конструктор клавиатур"""
    builder = ReplyKeyboardBuilder()
    for button in buttons:
        builder.button(text=button)
    builder.adjust(*sizes)
    return builder.as_markup(
        resize_keyboard=resize,
        one_time_keyboard=one_time
    )

def main_menu_keyboard(user_id: int = None) -> types.ReplyKeyboardMarkup:
    """Главное меню с учетом прав"""
    buttons = ["📋 Запрос информации", "📦 Проверка стока", "🛒 Заказ под клиента"]
    if user_id and user_id in ADMINS:
        buttons.append("🛠 Админ-панель")
    return create_keyboard(buttons, (2, 1, 1))

def article_input_keyboard() -> types.ReplyKeyboardMarkup:
    return create_keyboard(
        [ "❌ Отмена"],
        (1)
    )

def shop_selection_keyboard() -> types.ReplyKeyboardMarkup:
    return create_keyboard(
        ["Использовать мой магазин", "Выбрать другой", "❌ Отмена"],
        (2, 1)
    )

def confirm_keyboard() -> types.ReplyKeyboardMarkup:
    return create_keyboard(
        ["✅ Подтвердить", "✏️ Исправить количество", "❌ Отмена"],
        (2, 1)
    )

def admin_panel_keyboard() -> types.ReplyKeyboardMarkup:
    return create_keyboard(
        ["📊 Статистика", "📢 Рассылка", "🔄 Обновить кэш", "🔧 Сервисный режим", "📊 Дамп памяти", "📝 Управление задачами", "🔙 Главное меню"],
        (3, 2, 2)
    )

def service_mode_keyboard() -> types.ReplyKeyboardMarkup:
    return create_keyboard(
        ["🟢 Включить сервисный режим", "🔴 Выключить сервисный режим", "🔙 Назад"],
        (2, 1)
    )

def cancel_keyboard() -> types.ReplyKeyboardMarkup:
    return create_keyboard(["❌ Отмена"], (1,))


def broadcast_target_keyboard():
    return create_keyboard(
        ["Всем пользователям", "По магазинам", "По отделам", "Вручную", "❌ Отмена"],
        (2, 2, 1)
    )

def broadcast_confirmation_keyboard():
    return create_keyboard(
        ["✅ Подтвердить рассылку", "❌ Отмена"],
        (2,)
    )

def tasks_admin_keyboard() -> types.ReplyKeyboardMarkup:
    return create_keyboard(
        ["➕ Добавить задачу", "🗑️ Удалить задачу", "📤 Отправить список", "📊 Статистика выполнения", "🔙 Назад"],
        (2, 2, 1)
    )

def get_task_keyboard(task_id: str) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(
        text="✅ Выполнено", 
        callback_data=f"task_done:{task_id}"
    )
    return builder.as_markup()

# ===================== СЕРВИСНЫЕ ФУНКЦИИ =====================
async def notify_admins(message: str) -> None:
    """Уведомление администраторов"""
    for admin_id in ADMINS:
        with suppress(TelegramForbiddenError):
            await bot.send_message(admin_id, message)

async def toggle_service_mode(enable: bool) -> None:
    """Включение/выключение сервисного режима"""
    global SERVICE_MODE
    SERVICE_MODE = enable
    status = "ВКЛЮЧЕН" if enable else "ВЫКЛЮЧЕН"
    await notify_admins(f"🛠 Сервисный режим {status}")

async def get_user_data(user_id: str) -> Optional[Dict[str, Any]]:
    """Получение данных пользователя с улучшенной обработкой ошибок"""
    try:
        cache_key = f"user_{user_id}"
        if cache_key in cache:
            user_data = cache[cache_key]
            # Проверяем наличие обязательных полей
            if all(key in user_data for key in ['shop', 'name', 'position']):
                return user_data
            else:
                # Если данные неполные, запрашиваем заново
                cache.pop(cache_key, None)
        
        # Загрузка данных из Google Sheets
        users_records = pickle.loads(cache.get("users_data", b""))
        if not users_records:
            users_records = users_sheet.get_all_records()
            cache["users_data"] = pickle.dumps(users_records)
        
        for user in users_records:
            if str(user.get("ID пользователя", "")).strip() == str(user_id).strip():
                user_data = {
                    'shop': user.get("Номер магазина", "") or "Не указан",
                    'name': user.get("Имя", "") or "Не указано",
                    'surname': user.get("Фамилия", "") or "Не указано",
                    'position': user.get("Должность", "") or "Не указана"
                }
                cache[cache_key] = user_data
                return user_data
        
        return None
    except Exception as e:
        logging.error(f"Ошибка получения данных пользователя: {str(e)}")
        return None


async def log_error(user_id: str, error: str) -> None:
    """Логирование ошибок"""
    try:
        logs_sheet.append_row([
            datetime.now().strftime("%d.%m.%Y %H:%M"),
            user_id,
            "ERROR",
            error
        ])
    except Exception as e:
        logging.error(f"Ошибка логирования: {str(e)}")

async def log_user_activity(user_id: str, command: str, event_type: str = "command") -> None:
    """Логирование действий пользователя"""
    try:
        user_data = await get_user_data(str(user_id))
        if not user_data:
            return
            
        record = [
            datetime.now().strftime("%d.%m.%Y"),
            datetime.now().strftime("%H:%M:%S"),
            str(user_id),
            user_data.get('name', ''),
            user_data.get('surname', ''),
            user_data.get('position', ''),
            user_data.get('shop', ''),
            command,
            event_type
        ]
        
        stats_sheet = main_spreadsheet.worksheet(STATSS_SHEET_NAME)
        stats_sheet.append_row(record)
    except Exception as e:
        logging.error(f"Ошибка логирования активности: {str(e)}")




##Задачи\\\\\\\\\\\\\\\\\
def normalize_task_row(task_id: str, row: dict) -> dict:
    return {
        "text": row.get("Текст", ""),
        "creator_initials": row.get("Инициалы", ""),
        "deadline": row.get("Дедлайн", ""),
        "link": row.get("Ссылка", ""),
        "statuses": row.get("Статусы", ""),
    }


def get_tasks_sheet():
    """Возвращает лист с задачами"""
    return main_spreadsheet.worksheet(TASKS_SHEET_NAME)

async def get_user_initials(user_id: int) -> str:
    """Возвращает инициалы пользователя (например, 'И.Иванов')"""
    user_data = await get_user_data(str(user_id))
    if not user_data:
        return "Аноним"
    name = user_data.get("name", "")
    surname = user_data.get("surname", "")
    return f"{name}.{surname}" if name else surname


async def save_task(
    task_id: str,
    text: str,
    creator_id: int,
    creator_initials: str,
    link: str = None,
    deadline: str = None
):
    """Сохранение задачи в Google Sheets"""
    sheet = get_tasks_sheet()
    sheet.append_row([
        task_id,
        text,
        link,
        deadline,
        creator_id,
        creator_initials,
        datetime.now().strftime("%d.%m.%Y %H:%M"),
        "",
        json.dumps({"user_ids": []})  # Пустой список для статусов
    ])


# --- Исправленный фрагмент load_tasks ---
async def load_tasks() -> Dict[str, Dict[str, Any]]:
    """
    Загрузка задач из Google Sheets.
    Возвращает словарь: {task_id: {task_data}}
    task_data включает: text, link, deadline, creator_initials, creator_id, assigned_to, completed_by
    """
    sheet = get_tasks_sheet()
    tasks = {}
    try:
        records = sheet.get_all_records()
        logging.info(f"Загружено {len(records)} строк из Google Sheets для задач.")
        for row in records:
            task_id = str(row.get("ID задачи", "")).strip()
            if not task_id:
                continue # Пропускаем строки без ID
            
            # Обработка назначенных пользователей
            assigned_raw = str(row.get("Назначена", "")).strip()
            if assigned_raw:
                # Разбиваем строку, очищаем и фильтруем ID
                assigned_user_ids = [
                    uid_str for uid_str in
                    (uid.strip() for uid in assigned_raw.split(","))
                    if uid_str.isdigit()
                ]
            else:
                assigned_user_ids = []

            # --- Исправлено и Улучшено: Обработка выполненных пользователей с поддержкой старого формата ---
            completed_user_ids = []
            statuses_raw = str(row.get("Статусы", "{}")).strip()
            # logging.debug(f"Задача {task_id}: Сырой статус = '{statuses_raw}'")
            if statuses_raw:
                try:
                    statuses_data = json.loads(statuses_raw)
                    # logging.debug(f"Задача {task_id}: Распарсенный статус = {statuses_data} (тип: {type(statuses_data)})")
                    if isinstance(statuses_data, dict):
                        # Новый формат: {"completed_by": [...]}
                        if "completed_by" in statuses_data:
                            completed_user_ids = statuses_data.get("completed_by", [])
                            # logging.debug(f"Задача {task_id}: Найден ключ 'completed_by': {completed_user_ids}")
                        # Старый формат: {"user_ids": [...]}
                        elif "user_ids" in statuses_data:
                            logging.info(f"Задача {task_id} использует устаревший формат 'user_ids'.")
                            completed_user_ids = statuses_data.get("user_ids", [])
                            # logging.debug(f"Задача {task_id}: Найден ключ 'user_ids': {completed_user_ids}")
                        # Если ни один ключ не найден, оставляем пустой список
                        else:
                             completed_user_ids = []
                             # logging.debug(f"Задача {task_id}: Ключи в статусе не найдены.")
                        # Убедимся, что это список строк ID
                        completed_user_ids = [str(uid).strip() for uid in completed_user_ids if str(uid).strip()]
                        # logging.debug(f"Задача {task_id}: Финальный список completed_by = {completed_user_ids}")
                    else:
                        # Если statuses_data не словарь (например, пустой список или что-то еще)
                        logging.warning(f"Неверная структура 'Статусы' для задачи {task_id} (не словарь): {statuses_data}. Считается пустым.")
                        completed_user_ids = []
                except (json.JSONDecodeError, TypeError, ValueError) as e:
                    logging.warning(f"Ошибка парсинга 'Статусы' для задачи {task_id}: {e}. Считается пустым.")
                    completed_user_ids = []
            # else:
            #     logging.debug(f"Задача {task_id}: Статусы пусты.")
            
            tasks[task_id] = {
                "text": str(row.get("Текст", "")).strip(),
                "link": str(row.get("Ссылка", "")).strip(),
                "deadline": str(row.get("Дедлайн", "")).strip(),
                "creator_initials": str(row.get("Инициалы", "")).strip(),
                "creator_id": str(row.get("ID создателя", "")).strip(),
                "assigned_to": assigned_user_ids,
                # --- Исправлено: Теперь всегда используем ключ "completed_by" в памяти ---
                "completed_by": completed_user_ids, 
            }
            # logging.debug(f"Задача {task_id} добавлена в словарь tasks. completed_by: {tasks[task_id]['completed_by']}")
            
        logging.info(f"✅ Загружено {len(tasks)} задач из Google Sheets")
        
    except Exception as e: # <-- Этот except корректно завершает блок try
        logging.error(f"Ошибка загрузки задач из Google Sheets: {e}", exc_info=True)
        # tasks = {} # <-- Не нужно, так как tasks уже инициализирован. Просто вернется то, что есть (возможно, пустой словарь).
        # Возврат произойдет в конце функции
        
    # Возврат результата происходит в любом случае
    return tasks # <-- Эта строка должна быть на уровне функции, вне блока try...except

    

# =============================ПАРСЕР=================================
  
def parse_supplier_data(record: dict) -> Dict[str, Any]:
    """Парсинг данных поставщика"""
    order_days = []
    for key in ['День выхода заказа', 'День выхода заказа 2', 'День выхода заказа 3']:
        value = str(record.get(key, '')).strip()
        if value and value.isdigit():
            order_days.append(int(value))
    
    delivery_days = str(record.get('Срок доставки в магазин', '0')).strip()
    return {
        'supplier_id': str(record.get('Номер осн. пост.', '')),
        'order_days': sorted(list(set(order_days))),
        'delivery_days': int(delivery_days) if delivery_days.isdigit() else 0
    }

def calculate_delivery_date(supplier_data: dict) -> Tuple[str, str]:
    """Расчет даты доставки"""
    today = datetime.now()
    current_weekday = today.isoweekday()

    # Находим ближайший день заказа
    nearest_day = None
    for day in sorted(supplier_data['order_days']):
        if day >= current_weekday:
            nearest_day = day
            break
    if not nearest_day:
        nearest_day = min(supplier_data['order_days'])
    
    delta_days = (nearest_day - current_weekday) % 7
    order_date = today + timedelta(days=delta_days)
    delivery_date = order_date + timedelta(days=supplier_data['delivery_days'])
    
    return (
        order_date.strftime("%d.%m.%Y"),
        delivery_date.strftime("%d.%m.%Y")
    )


@profile_memory
async def get_product_info(article: str, shop: str) -> Optional[Dict[str, Any]]:
    """Получение информации о товаре с расширенным логированием, используя SQLite"""
    try:
        logging.info(f"🔍 Поиск товара: артикул={article}, магазин={shop}")
        
        # === 1. Получение данных товара из SQLite ===
        product_data = await get_product_data_from_db(article, shop)
        
        if not product_data:
            logging.warning(f"Товар не найден в БД: артикул={article}, магазин={shop}")
            return None
            
        logging.info(f"Найден товар: {product_data.get('Название', 'Неизвестно')}")

        # === 2. Получение данных поставщика из SQLite ===
        supplier_id = str(product_data.get("Номер осн. пост.", "")).strip()
        logging.info(f"ID поставщика: {supplier_id}")
        
        if not supplier_id:
             # Если поставщик не указан, возвращаем базовую информацию
             logging.info("Номер поставщика отсутствует, возвращаю базовую информацию")
             return {
                 'Артикул': article,
                 'Название': product_data.get('Название', ''),
                 'Отдел': str(product_data.get('Отдел', '')),
                 'Магазин': shop,
                 'Поставщик': 'Товар РЦ', # Или другое значение по умолчанию
                 'Топ в магазине': product_data.get('Топ в магазине', '0'),
                 'Дата заказа': 'Не определена (поставщик не найден)',      
                 'Дата поставки': 'Не определена (поставщик не найден)',    
             }

        supplier_data = await get_supplier_data_from_db(supplier_id, shop)
        
        # === 3. Обработка случая, если поставщик не найден ===
        if not supplier_data:
            logging.info("Поставщик не найден в БД, используется резервная информация")
            return {
                'Артикул': article,
                'Название': product_data.get('Название', ''),
                'Отдел': str(product_data.get('Отдел', '')),
                'Магазин': shop,
                'Поставщик': 'Товар РЦ', # Или product_data.get('Название осн. пост.', 'Не указано').strip()
                'Топ в магазине': product_data.get('Топ в магазине', '0'),
                'Дата заказа': 'Не определена (поставщик не найден)',      
                'Дата поставки': 'Не определена (поставщик не найден)',
            }

        # === 4. Парсинг данных поставщика и расчет дат ===
        # Парсинг данных поставщика (используем существующую функцию)
        parsed_supplier = parse_supplier_data(supplier_data)
        order_date, delivery_date = calculate_delivery_date(parsed_supplier)
        
        # === 5. Формирование итогового результата ===
        result = {
            'Артикул': article,
            'Название': product_data.get('Название', ''),
            'Отдел': str(product_data.get('Отдел', '')),
            'Магазин': shop,
            'Поставщик': supplier_data.get("Название осн. пост.", "Не указано").strip(),
            'Дата заказа': order_date,
            'Дата поставки': delivery_date,
            'Номер поставщика': supplier_id,
            'Топ в магазине': product_data.get('Топ в магазине', '0')
        }
        
        logging.info(f"Успешно получена информация: {result}")
        return result
        
    except Exception as e:
        logging.exception(f"Критическая ошибка в get_product_info: {str(e)}")
        return None


@profile_memory
async def preload_cache() -> None:
    """Предзагрузка кэша"""
    try:
        # Кэширование пользователей
        users_records = users_sheet.get_all_records()
        cache["users_data"] = pickle.dumps(users_records)
        
        cache_size = sum(len(pickle.dumps(v)) for v in cache.values()) / 1024 / 1024
        logging.info(f"✅ Кэш пользователей загружен. Размер: {cache_size:.2f} MB")
        logging.info("✅ Кэш успешно загружен (без gamma_index)")
    except Exception as e:
        logging.error(f"Ошибка загрузки кэша: {str(e)}")


# === Заменить полностью функцию get_product_data_from_db ===
async def get_product_data_from_db(article: str, shop: str) -> Optional[Dict[str, Any]]:
    """
    Получение данных о товаре из SQLite по составному ключу (full_key).

    Args:
        article (str): Артикул товара.
        shop (str): Номер магазина.

    Returns:
        Optional[Dict[str, Any]]: Словарь с данными товара или None, если не найден.
    """
    try:
        # Формируем составной ключ для точного поиска
        full_key_exact = f"{article}{shop}"
        logging.info(f"🔍 Поиск по full_key: '{full_key_exact}'")

        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 1. Поиск с точным совпадением по full_key
            cursor.execute("""
                SELECT full_key, store_number, department, article_code, name, gamma, 
                       supplier_code, supplier_name, is_top_store
                FROM articles 
                WHERE full_key = ?
            """, (full_key_exact,))
            
            row = cursor.fetchone()
            
            if row:
                # Преобразуем sqlite3.Row в словарь
                logging.info(f"✅ Найден товар по точному full_key '{full_key_exact}': {row['name']}")
                # Отображаем имена столбцов из БД в имена, ожидаемые get_product_info
                return {
                    "Магазин": row['store_number'],
                    "Отдел": row['department'],
                    "Артикул": row['article_code'],
                    "Название": row['name'],
                    "Гамма": row['gamma'],
                    "Номер осн. пост.": row['supplier_code'],
                    "Название осн. пост.": row['supplier_name'],
                    "Топ в магазине": str(row['is_top_store'])
                }
            
            # 2. Если не найден по точному ключу, ищем по артикулу в начале full_key
            # Формируем шаблон поиска: full_key начинается с артикула
            article_prefix = f"{article}%"
            logging.info(f"Товар с full_key '{full_key_exact}' не найден, ищу по артикулу '{article}' в начале full_key...")
            
            cursor.execute("""
                SELECT full_key, store_number, department, article_code, name, gamma, 
                       supplier_code, supplier_name, is_top_store
                FROM articles 
                WHERE full_key LIKE ?
                ORDER BY full_key -- Сортируем для получения какого-либо результата
                LIMIT 1
            """, (article_prefix,))
            
            row = cursor.fetchone()
            
            if row:
                found_key = row['full_key']
                found_shop = row['store_number']
                logging.info(f"✅ Найден товар по артикулу в full_key: full_key='{found_key}', магазин={found_shop}, название={row['name']}")
                # Отображаем имена столбцов из БД в имена, ожидаемые get_product_info
                return {
                    "Магазин": row['store_number'],
                    "Отдел": row['department'],
                    "Артикул": row['article_code'],
                    "Название": row['name'],
                    "Гамма": row['gamma'],
                    "Номер осн. пост.": row['supplier_code'],
                    "Название осн. пост.": row['supplier_name'],
                    "Топ в магазине": str(row['is_top_store'])
                }
            else:
                logging.warning(f"❌ Товар с артикулом '{article}' не найден даже по артикулу в full_key")
                
            return None
            
    except sqlite3.Error as e:
        logging.error(f"Ошибка запроса к БД (get_product_data_from_db): {e}")
        return None
    except Exception as e:
        logging.error(f"Неожиданная ошибка в get_product_data_from_db: {e}")
        return None


async def get_supplier_data_from_db(supplier_id: str, shop: str) -> Optional[Dict[str, Any]]:
    """
    Получение данных о поставщике и сроках поставки из SQLite.

    Args:
        supplier_id (str): Номер основного поставщика.
        shop (str): Номер магазина (для выбора правильной таблицы).

    Returns:
        Optional[Dict[str, Any]]: Словарь с данными поставщика или None, если не найден.
    """
    # Нормализуем supplier_id
    supplier_id = str(supplier_id).strip()
    if not supplier_id:
        return None

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # Формируем имя таблицы поставщиков (как в Google Sheets)
            supplier_table_name = f"Даты выходов заказов {shop}"

            # Определяем части SQL-запроса
            select_clause = '''
                SELECT "Номер осн. пост.", "Название осн. пост.", "Срок доставки в магазин",
                       "День выхода заказа", "День выхода заказа 2", "День выхода заказа 3"
            '''
            from_clause = f'FROM "{supplier_table_name}"'
            where_clause = 'WHERE "Номер осн. пост." = ?'

            # Собираем полный запрос
            query = f"{select_clause} {from_clause} {where_clause}"
            
            cursor.execute(query, (supplier_id,))
            
            row = cursor.fetchone()
            
            if row:
                # Преобразуем sqlite3.Row в словарь
                return dict(row)
            else:
                logging.info(f"Поставщик {supplier_id} не найден в таблице '{supplier_table_name}'")
                return None
                
    except sqlite3.Error as e:
        logging.error(f"Ошибка запроса к БД (get_supplier_data_from_db): {e}")
        return None
    except Exception as e:
        logging.error(f"Неожиданная ошибка в get_supplier_data_from_db: {e}")
        return None


# ===================== MIDDLEWARES =====================
@dp.update.middleware()
async def service_mode_middleware(handler, event, data):
    """Проверка сервисного режима"""
    if SERVICE_MODE and (event.message or event.callback_query):
        user_id = event.message.from_user.id if event.message else event.callback_query.from_user.id
        if user_id not in ADMINS:
            msg = "⏳ Бот в режиме обслуживания. Попробуйте позже."
            if event.message:
                await event.message.answer(msg)
            elif event.callback_query:
                await event.callback_query.answer(msg, show_alert=True)
            return
    return await handler(event, data)

@dp.update.middleware()
async def activity_tracker_middleware(handler, event, data):
    """Улучшенный трекинг активности пользователя с обработкой ошибок"""
    try:
        state = data.get('state')
        if state:
            # Получаем данные состояния
            state_data = await state.get_data()
            
            # Обновляем активность ПОСЛЕ обработки сообщения
            response = await handler(event, data)
            
            # Обновляем время активности
            new_data = await state.get_data()
            new_data['last_activity'] = datetime.now().isoformat()
            await state.set_data(new_data)
            
            return response
        
        return await handler(event, data)
        
    except Exception as e:
        logging.error(f"Ошибка в трекере активности: {str(e)}")
        return await handler(event, data)


# ===================== АВТОМАТИЧЕСКАЯ ОЧИСТКА СОСТОЯНИЙ =====================
async def state_cleanup_task():
    """Фоновая задача для очистки устаревших состояний с логированием"""
    while True:
        try:
            now = datetime.now()
            cleared_count = 0
            
            if hasattr(dp.storage, 'storage'):
                states = dp.storage.storage
                logging.info(f"Проверка состояний: {len(states)} активных сессий")
                
                for key, state_record in list(states.items()):
                    if not hasattr(state_record, 'data') or not isinstance(state_record.data, dict):
                        continue
                    
                    data = state_record.data
                    last_activity_str = data.get('last_activity')
                    
                    if not last_activity_str:
                        continue
                    
                    try:
                        last_activity = datetime.fromisoformat(last_activity_str)
                        inactivity = (now - last_activity).total_seconds() / 60
                        
                        if inactivity > 30:
                            user_id = key.user_id
                            logging.info(f"Очистка состояния: пользователь {user_id}, неактивен {inactivity:.1f} мин")
                            await dp.storage.set_state(key=key, state=None)
                            await dp.storage.set_data(key=key, data={})
                            del states[key]
                            try:
                                await bot.send_message(
                                    user_id,
                                    "🕒 Сессия была автоматически завершена из-за неактивности.",
                                    reply_markup=main_menu_keyboard(user_id) # Отправляем главную клавиатуру
                                    )
                                
                                logging.info(f"Пользователю {user_id} отправлено сообщение о сбросе сессии.")
                            
                            except Exception as e:
        # TelegramForbiddenError (пользователь заблокировал бота), 
        # TelegramRetryAfter, и т.д.
                                logging.warning(f"Не удалось отправить сообщение о сбросе сессии пользователю {user_id}: {e}")    
                        cleared_count += 1
                            
                    except (TypeError, ValueError) as e:
                        logging.error(f"Ошибка формата времени: {str(e)}")
                
                if cleared_count > 0:
                    logging.info(f"Автоочистка: очищено {cleared_count} состояний")
            
            await asyncio.sleep(900)
                
        except Exception as e:
            logging.exception(f"Ошибка в задаче очистки состояний: {str(e)}")
            await asyncio.sleep(300)


# ===================== ОБРАБОТЧИКИ КОМАНД =====================
@dp.message(Command("start"))
async def start_handler(message: types.Message, state: FSMContext):
    """Обработчик команды /start"""
    # Инициализируем активность в состоянии
    await state.update_data(last_activity=datetime.now().isoformat())
    
    user_data = await get_user_data(str(message.from_user.id))
    if user_data:
        await message.answer("ℹ️ Вы в главном меню:", 
                            reply_markup=main_menu_keyboard(message.from_user.id))
        return
    
    await message.answer("👋 Добро пожаловать! Введите ваше имя:", 
                        reply_markup=types.ReplyKeyboardRemove())
    await log_user_activity(message.from_user.id, "/start", "registration")
    await state.set_state(Registration.name)

# Регистрация пользователя
@dp.message(Registration.name)
async def process_name(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat(), name=message.text.strip())
    await message.answer("📝 Введите вашу фамилию:")
    await state.set_state(Registration.surname)

@dp.message(Registration.surname)
async def process_surname(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat(), surname=message.text.strip())
    await message.answer("💼 Введите вашу должность:")
    await state.set_state(Registration.position)

@dp.message(Registration.position)
async def process_position(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat(), position=message.text.strip())
    await message.answer("🏪 Введите номер магазина (только цифры, без нулей):")
    await state.set_state(Registration.shop)

@dp.message(Registration.shop)
async def process_shop(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat())
    shop = message.text.strip()
    
    if not shop.isdigit():
        await message.answer("❌ Номер магазина должен быть числом! Повторите ввод:")
        return
    
    data = await state.get_data()
    users_sheet.append_row([
        str(message.from_user.id),
        data['name'],
        data['surname'],
        data['position'],
        shop,
        datetime.now().strftime("%d.%m.%Y %H:%M")
    ])
    cache.pop(f"user_{message.from_user.id}", None)  # Сброс кэша пользователя
    
    try:
        # Перезагружаем только кэш пользователей
        users_records = users_sheet.get_all_records()
        cache["users_data"] = pickle.dumps(users_records)
        logging.info(f"✅ Кэш пользователей обновлен после регистрации пользователя {message.from_user.id}")
    except Exception as e:
        logging.error(f"Ошибка обновления кэша пользователей после регистрации {message.from_user.id}: {e}")
        
    await message.answer("✅ Регистрация завершена!", 
                            reply_markup=main_menu_keyboard(message.from_user.id))
    await state.clear()


# Навигация
@dp.message(F.text.in_(["↩️ Назад", "🔙 Главное меню"]))
async def handle_back(message: types.Message, state: FSMContext):
    """Обработчик возврата в меню"""
    await state.clear()
    await message.answer("🔙 Возврат в главное меню", 
                        reply_markup=main_menu_keyboard(message.from_user.id))

@dp.message(F.text.casefold() == "отмена")
@dp.message(F.text == "❌ Отмена")
async def cancel_handler(message: types.Message, state: FSMContext):
    """Универсальный обработчик отмены с обновлением активности"""
    # Обновляем активность перед обработкой
    await state.update_data(last_activity=datetime.now().isoformat())
    
    current_state = await state.get_state()
    if current_state:
        await state.clear()
        await message.answer("🔄 Операция отменена", 
                            reply_markup=main_menu_keyboard(message.from_user.id))

# Заказ товара
@dp.message(F.text == "🛒 Заказ под клиента")
async def handle_client_order(message: types.Message, state: FSMContext):
    """Начало оформления заказа"""
    await state.update_data(last_activity=datetime.now().isoformat())
    user_data = await get_user_data(str(message.from_user.id))
    
    if not user_data:
        await message.answer("❌ Сначала пройдите регистрацию через /start")
        return
    
    await state.update_data(
        shop=user_data['shop'],
        user_name=user_data['name'],
        user_position=user_data['position']
    )
    
    await message.answer("🔢 Введите артикул товара:", 
                         reply_markup=cancel_keyboard())
    await log_user_activity(message.from_user.id, "Заказ под клиента", "order")
    await state.set_state(OrderStates.article_input)


@dp.message(OrderStates.article_input)
async def process_article_input(message: types.Message, state: FSMContext):
    """Обработка введенного артикула"""
    if message.photo:
        await message.answer("📸 Распознавание штрих-кодов отключено. Введите артикул вручную.")
        return
    article = message.text.strip()
    
    if not re.match(r'^\d{4,10}$', article):
        await message.answer("❌ Неверный формат артикула. Артикул должен состоять из 4-10 цифр.")
        return
        
    await state.update_data(article=article)
    await message.answer("📌 Выберите магазин для заказа:", 
                        reply_markup=shop_selection_keyboard())
    await state.set_state(OrderStates.shop_selection)


@dp.message(OrderStates.shop_selection)
async def process_shop_selection(message: types.Message, state: FSMContext):
    """Обработка выбора магазина"""
    user_data = await get_user_data(str(message.from_user.id))
    
    if message.text == "Использовать мой магазин":
        selected_shop = user_data['shop']
    elif message.text == "Выбрать другой":
        await message.answer("🏪 Введите номер магазина (только цифры, без ведущих нулей):")
        await state.set_state(OrderStates.shop_input)
        return
    else:
        await message.answer("❌ Неверный выбор. Используйте кнопки меню",
                            reply_markup=shop_selection_keyboard())
        return
    
    await state.update_data(selected_shop=selected_shop)
    await continue_order_process(message, state)

@dp.message(OrderStates.shop_input)
async def process_custom_shop(message: types.Message, state: FSMContext):
    """Обработка ввода номера магазина"""
    shop = message.text.strip()
    if not shop.isdigit() or shop.startswith('0'):
        await message.answer("❗ Номер магазина должен быть целым числом без ведущих нулей. Повторите ввод:")
        return
    await state.update_data(selected_shop=shop)
    await continue_order_process(message, state)

async def continue_order_process(message: types.Message, state: FSMContext):
    """Продолжение обработки заказа после выбора магазина"""
    data = await state.get_data()
    article = data.get('article')
    selected_shop = data.get('selected_shop')
    
    await message.answer("🔄 Загружаю информацию о товаре...")
    product_info = await get_product_info(article, selected_shop)
    
    if not product_info:
        await message.answer("❌ Товар не найден в выбранном магазине", reply_markup=main_menu_keyboard(message.from_user.id))
        await state.clear()
        return
        
    response = (
        f"Магазин: {selected_shop}\n"
        f"📦 Артикул: {product_info['Артикул']}\n"
        f"🏷️ Название: {product_info['Название']}\n"
        f"🏭 Поставщик: {product_info['Поставщик']}\n" 
        f"📅 Дата заказа: {product_info['Дата заказа']}\n"
        f"🚚 Дата поставки: {product_info['Дата поставки']}\n"
    )

    if product_info.get('Топ в магазине', '0') == '0':
        response += "\n⚠️ <b>Внимание, артикул в ТОП 0!</b>\nСвяжись с менеджером для уточнения возможности заказа"

    
    await state.update_data(
        product_name=product_info['Название'],
        department=product_info['Отдел'],
        supplier_name=product_info['Поставщик'],
        order_date=product_info['Дата заказа'],  
        delivery_date=product_info['Дата поставки'],
        top_in_shop=product_info.get('Топ в магазине', '0')
    )
    
    await message.answer(response)
    await message.answer("🔢 Введите количество товара:", 
                        reply_markup=cancel_keyboard())
    await state.set_state(OrderStates.quantity_input)

@dp.message(OrderStates.quantity_input)
async def process_quantity(message: types.Message, state: FSMContext):
    """Обработка ввода количества"""
    quantity = message.text.strip()
    
    if not quantity.isdigit():
        await message.answer("❌ Введите число!", reply_markup=cancel_keyboard())
        return
        
    await state.update_data(quantity=int(quantity))
    await message.answer("Введите номер заказа или причину:", 
                        reply_markup=cancel_keyboard())
    await state.set_state(OrderStates.order_reason_input)

@dp.message(OrderStates.order_reason_input)
async def process_order_reason(message: types.Message, state: FSMContext):
    """Обработка причины заказа"""
    reason = message.text.strip()
    await state.update_data(order_reason=reason)
    
    data = await state.get_data()
    selected_shop = data.get('selected_shop')

    warning = ""
    if data.get('top_in_shop', '0') == '0':
        warning = "\n\n⚠️ <b>Внимание, артикул в ТОП 0!</b>\nСвяжись с менеджером для уточнения возможности заказа"
    
    response = (
        "🔎 Проверьте данные заказа:\n"
        f"Магазин: {selected_shop}\n"
        f"📦 Артикул: {data['article']}\n"
        f"🏷️ Название: {data['product_name']}\n"
        f"🏭 Поставщик: {data['supplier_name']}\n" 
        f"📅 Дата заказа: {data['order_date']}\n"
        f"🚚 Дата поставки: {data['delivery_date']}\n"
        f"Количество: {data['quantity']}\n"
        f"Номер заказа/Причина: {reason}\n"
        f"{warning}"
    )
    
    await message.answer(response, reply_markup=confirm_keyboard())
    await state.set_state(OrderStates.confirmation)


@dp.message(OrderStates.confirmation, F.text == "✅ Подтвердить")
async def final_confirmation(message: types.Message, state: FSMContext):
    """Подтверждение заказа"""
    await state.update_data(last_activity=datetime.now().isoformat())
    data = await state.get_data()
    
    
    # Проверка обязательных полей
    required_fields = ['selected_shop', 'article', 'order_reason', 'quantity', 'department']
    for field in required_fields:
        if field not in data:
            raise ValueError(f"Отсутствует поле: {field}")
        
        # Получаем лист отдела
    department_sheet = orders_spreadsheet.worksheet(data['department'])
    next_row = len(department_sheet.col_values(1)) + 1
        
        # Формируем обновления
    updates = [
        {'range': f'A{next_row}', 'values': [[data['selected_shop']]]},
        {'range': f'B{next_row}', 'values': [[int(data['article'])]]},
        {'range': f'C{next_row}', 'values': [[data['order_reason']]]},
        {'range': f'D{next_row}', 'values': [[datetime.now().strftime("%d.%m.%Y %H:%M")]]},
        {'range': f'E{next_row}', 'values': [[f"{data['user_name']}, {data['user_position']}"]]},
        {'range': f'K{next_row}', 'values': [[int(data['quantity'])]]},
        {'range': f'R{next_row}', 'values': [[int(message.from_user.id)]]}
    ]

    # Записываем данные
    department_sheet.batch_update(updates)
    await message.answer("✅ Заказ успешно сохранен!", 
    reply_markup=main_menu_keyboard(message.from_user.id))
    await log_user_activity(message.from_user.id, "Подтвердить заказ", "confirmation")
    await state.clear()



@dp.message(OrderStates.confirmation, F.text == "✏️ Исправить количество")
async def correct_quantity(message: types.Message, state: FSMContext):
    """Корректировка количества"""
    await message.answer("🔢 Введите новое количество:", 
                        reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(OrderStates.quantity_input)


# Запрос информации о товаре
@dp.message(F.text == "📋 Запрос информации")
async def handle_info_request(message: types.Message, state: FSMContext):
    """Обработчик запроса информации с защитой от потери данных"""
    try:
        await state.update_data(last_activity=datetime.now().isoformat())
        await log_user_activity(message.from_user.id, "Запрос информации", "info")
        
        # Получаем данные пользователя
        user_data = await get_user_data(str(message.from_user.id))
        if not user_data:
            await message.answer("❌ Сначала пройдите регистрацию через /start")
            return
        
        # Проверяем наличие магазина в профиле
        shop = user_data.get('shop', 'Не указан')
        if shop == "Не указан":
            await message.answer("❌ В вашем профиле не указан магазин. Обратитесь к администратору.")
            return
        
        # Сохраняем магазин в состоянии
        await state.set_data({
            'shop': shop,
            'last_activity': datetime.now().isoformat()
        })
        
        await message.answer("🔢 Введите артикул товара:", reply_markup=cancel_keyboard())
        await state.set_state(InfoRequest.article_input)
        
    except Exception as e:
        logging.error(f"Ошибка в начале запроса информации: {str(e)}")
        await message.answer("⚠️ Произошла ошибка. Попробуйте позже.")
        await state.clear()


@dp.message(InfoRequest.article_input)
async def process_info_request(message: types.Message, state: FSMContext):
    """Обработка запроса информации о товаре с дополнительной защитой"""
    try:
        # Получаем данные состояния
        data = await state.get_data()
        user_id = str(message.from_user.id)
        
        # Проверяем наличие магазина в данных состояния
        if 'shop' not in data:
            # Логируем предупреждение
            logging.warning(f"Магазин отсутствует в состоянии для {user_id}")
            
            # Если нет в состоянии, пробуем получить из профиля
            user_data = await get_user_data(user_id)
            if not user_data:
                logging.warning(f"Профиль не найден для {user_id}")
                await message.answer("❌ Ваш профиль не найден. Пройдите регистрацию через /start")
                await state.clear()
                return
                
            shop = user_data.get('shop', 'Не указан')
            if shop == "Не указан":
                logging.warning(f"Магазин не указан в профиле для {user_id}")
                await message.answer("❌ В вашем профиле не указан магазин. Обратитесь к администратору.")
                await state.clear()
                return
            else:
                # Обновляем состояние
                await state.update_data(shop=shop)
                logging.info(f"Магазин {shop} восстановлен из профиля для {user_id}")
        else:
            shop = data['shop']
            logging.info(f"Используется магазин {shop} из состояния для {user_id}")
        
        # Обработка ввода
        article = None
        if message.photo:
            await message.answer("📸 Распознавание штрих-кодов отключено. Введите артикул вручную.")
            return
        else:
            # Логирование ручного ввода
            logging.info(f"Ручной ввод артикула: {message.text} (пользователь: {user_id})")
            
            # Обработка текста
            article = message.text.strip()
            if not re.match(r'^\d{4,10}$', article):
                await message.answer("❌ Неверный формат артикула.")
                return
        
        # Поиск информации о товаре
        logging.info(f"Поиск информации о товаре {article} для магазина {shop} (пользователь: {user_id})")
        await message.answer("🔄 Поиск информации о товаре...")
        product_info = await get_product_info(article, shop)
        
        if not product_info:
            logging.warning(f"Товар {article} не найден для магазина {shop} (пользователь: {user_id})")
            await message.answer("❌ Товар не найден", reply_markup=main_menu_keyboard(message.from_user.id))
            await state.clear()
            return

        # Формирование ответа
        response = (
            f"🔍 Информация о товаре:\n"
            f"Магазин: {shop}\n"
            f"📦 Артикул: {product_info['Артикул']}\n"
            f"🏷️ Название: {product_info['Название']}\n"
            f"🔢 Отдел: {product_info['Отдел']}\n"
            f"📅 Ближайшая дата заказа: {product_info['Дата заказа']}\n"
            f"🚚 Ожидаемая дата поставки: {product_info['Дата поставки']}\n"
            f"🏭 Поставщик: {product_info['Поставщик']}" 
        )
        
        # Добавляем предупреждение для ТОП 0
        if product_info.get('Топ в магазине', '0') == '0':
            response += "\n\n⚠️ <b>ВНИМАНИЕ: Артикул в ТОП 0!</b>\nСвяжитесь с менеджером для уточнения информации"
        
        await message.answer(response, reply_markup=main_menu_keyboard(message.from_user.id))
        await state.clear()
        
        # Логирование успешного завершения
        logging.info(f"Успешно обработан запрос информации для товара {article} (пользователь: {user_id})")
        
    except Exception as e:
        logging.error(f"Ошибка в обработчике информации: {str(e)}")
        await message.answer("⚠️ Произошла ошибка при обработке запроса. Попробуйте позже.")
        await state.clear()



##=============================ОБРАБОТЧИКИ АДМИН ПАНЕЛИ====================================
@dp.message(F.text == "🛠 Админ-панель")
async def handle_admin_panel(message: types.Message):
    """Панель администратора"""
    if message.from_user.id not in ADMINS:
        await message.answer("⛔ У вас нет прав доступа")
        return
    
    await message.answer("🛠 Панель администратора:", 
                        reply_markup=admin_panel_keyboard())


@dp.message(F.text == "📊 Статистика")
async def handle_admin_stats(message: types.Message):
    """Статистика бота"""
    if message.from_user.id not in ADMINS:
        return
    
    try:
        # Получаем данные о пользователях из кэша
        users_data = pickle.loads(cache.get("users_data", b"[]"))
        users_count = len(users_data) if users_data else 0
        
        # ПРЯМОЕ ОБРАЩЕНИЕ К GOOGLE SHEETS ДЛЯ СТАТИСТИКИ
        stats_sheet = main_spreadsheet.worksheet(STATSS_SHEET_NAME)
        stats_records = stats_sheet.get_all_records()
        
        # Считаем количество заказов
        orders_count = sum(1 for r in stats_records if r.get('Тип события') == 'order')
        
        # Получаем системные метрики
        cpu_usage = psutil.cpu_percent()
        memory_usage = psutil.virtual_memory().percent
        
        response = (
            f"📊 Статистика бота:\n\n"
            f"• Пользователей: {users_count}\n"
            f"• Заказов оформлено: {orders_count}\n"
            f"• Логов действий: {len(stats_records)}\n\n"
            f"⚙️ Состояние сервера:\n"
            f"• Загрузка CPU: {cpu_usage}%\n"
            f"• Использование RAM: {memory_usage}%\n"
            f"• Сервисный режим: {'ВКЛ' if SERVICE_MODE else 'ВЫКЛ'}"
        )
        await message.answer(response, reply_markup=admin_panel_keyboard())
        
    except Exception as e:
        logging.error(f"Ошибка при получении статистики: {str(e)}")
        await message.answer(f"❌ Ошибка при получении статистики: {str(e)}", 
                            reply_markup=admin_panel_keyboard())



@dp.message(F.text == "📊 Дамп памяти")
async def handle_memory_dump(message: types.Message):
    """Генерация дампа памяти для анализа (текстовый вариант)"""
    if message.from_user.id not in ADMINS:
        return
    
    wait_msg = await message.answer("🔄 Формирование отчета о памяти...")
    
    try:
        # Формируем текстовый отчет
        report = []
        process = psutil.Process()
        mem_info = process.memory_info()
        
        # Основная информация
        report.append(f"<b>📊 Отчет об использовании памяти</b>")
        report.append(f"• Время: {datetime.now().strftime('%H:%M:%S')}")
        report.append(f"• RSS: {mem_info.rss / 1024 / 1024:.2f} MB")
        report.append(f"• VMS: {mem_info.vms / 1024 / 1024:.2f} MB")
        
        # Информация о процессах
        report.append("\n<b>🔢 Процессная информация:</b>")
        report.append(f"• Потоков: {process.num_threads()}")
        report.append(f"• Дескрипторов: {process.num_fds()}")
        
        # Топ объектов в памяти
        report.append("\n<b>📦 Топ объектов в памяти:</b>")
        common_types = objgraph.most_common_types(limit=15)
        for i, (obj_type, count) in enumerate(common_types, 1):
            report.append(f"{i}. {obj_type}: {count}")
        
        # Собираем полный отчет
        full_report = "\n".join(report)
        
        # Разбиваем отчет на части по 4000 символов
        for i in range(0, len(full_report), 4000):
            part = full_report[i:i+4000]
            await message.answer(
                part,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
        
        await wait_msg.delete()
        
    except Exception as e:
        logging.error(f"Ошибка при создании дампа памяти: {str(e)}")
        await message.answer(f"❌ Ошибка: {str(e)}")
        with suppress(Exception):
            await wait_msg.delete()


##===============РАССЫЛКА=================

@dp.message(F.text == "📢 Рассылка")
async def handle_broadcast_menu(message: types.Message, state: FSMContext):
    """Начало процесса рассылки"""
    if message.from_user.id not in ADMINS:
        return
    
    await message.answer(
        "✉️ Введите сообщение для рассылки (можно с медиа-вложениями):",
        reply_markup=types.ReplyKeyboardRemove()
    )
    await state.set_state(AdminBroadcast.message_input)

@dp.message(AdminBroadcast.message_input)
async def process_broadcast_message(message: types.Message, state: FSMContext):
    """Обработка сообщения для рассылки"""
    # Сохраняем контент в зависимости от типа
    content = {
        'text': message.html_text,
        'media': None,
        'type': 'text'
    }
    
    if message.photo:
        content.update({
            'type': 'photo',
            'media': message.photo[-1].file_id,
            'caption': message.caption
        })
    elif message.document:
        content.update({
            'type': 'document',
            'media': message.document.file_id,
            'caption': message.caption
        })
    
    await state.update_data(content=content)
    
    # Предпросмотр сообщения
    preview_text = "✉️ Предпросмотр сообщения:\n"
    if content['type'] == 'text':
        preview_text += content['text']
    else:
        preview_text += f"[{content['type'].upper()}] {content.get('caption', '')}"
    
    await message.answer(
        preview_text,
        reply_markup=broadcast_target_keyboard()
    )
    await state.set_state(AdminBroadcast.target_selection)

@dp.message(AdminBroadcast.target_selection)
async def handle_target_selection(message: types.Message, state: FSMContext):
    """Обработка выбора целевой аудитории"""
    if message.text == "Всем пользователям":
        await state.update_data(target="all")
        await message.answer("✅ Отправить всем пользователям", 
                            reply_markup=broadcast_confirmation_keyboard())
        await state.set_state(AdminBroadcast.confirmation)
    elif message.text == "Вручную":
        await message.answer("🔢 Введите ID пользователей через запятую:")
        await state.set_state(AdminBroadcast.manual_ids)
    elif message.text == "❌ Отмена":
        await state.clear()
        await message.answer("❌ Рассылка отменена", reply_markup=admin_panel_keyboard())
    else:
        await message.answer("❌ Неверный выбор. Пожалуйста, используйте кнопки.", 
                            reply_markup=broadcast_target_keyboard())

@dp.message(AdminBroadcast.manual_ids)
async def process_manual_ids(message: types.Message, state: FSMContext):
    """Обработка ручного ввода ID"""
    user_ids = [id.strip() for id in message.text.split(",") if id.strip().isdigit()]
    
    if not user_ids:
        await message.answer("❌ Неверный формат ID. Повторите ввод:")
        return
    
    await state.update_data(target="manual", user_ids=user_ids)
    await message.answer(f"✅ ID пользователей введены ({len(user_ids)} шт.)", 
                        reply_markup=broadcast_confirmation_keyboard())
    await state.set_state(AdminBroadcast.confirmation)

@dp.message(AdminBroadcast.confirmation, F.text == "✅ Подтвердить рассылку")
async def confirm_broadcast(message: types.Message, state: FSMContext):
    """Подтверждение и запуск рассылки"""
    data = await state.get_data()
    content = data['content']
    target = data.get('target', 'all')
    user_ids = data.get('user_ids', [])
    
    # Получаем список всех пользователей для рассылки
    if target == "all":
        users_data = pickle.loads(cache.get("users_data", b"[]"))
        user_ids = [str(user['ID пользователя']) for user in users_data if user.get('ID пользователя')]
    elif target == "manual":
        # Уже есть user_ids
        pass
    
    if not user_ids:
        await message.answer("❌ Нет пользователей для рассылки")
        await state.clear()
        return
    
    # Записываем в логи
    try:
        logs_sheet.append_row([
            datetime.now().strftime("%d.%m.%Y %H:%M"),
            message.from_user.id,
            "BROADCAST",
            f"Type: {content['type']}, Users: {len(user_ids)}"
        ])
    except Exception as e:
        logging.error(f"Ошибка логирования рассылки: {str(e)}")
    
    await message.answer(f"🔄 Начинаю рассылку для {len(user_ids)} пользователей...", 
                        reply_markup=admin_panel_keyboard())
    
    # Запускаем асинхронную рассылку
    asyncio.create_task(send_broadcast(content, user_ids))
    
    await state.clear()

async def send_broadcast(content: dict, user_ids: list):
    """Асинхронная отправка рассылки"""
    success = 0
    failed = 0
    errors = []
    
    for user_id in user_ids:
        try:
            if not user_id.strip():
                continue
                
            if content['type'] == 'text':
                await bot.send_message(int(user_id), content['text'])
            elif content['type'] == 'photo':
                await bot.send_photo(
                    int(user_id),
                    photo=content['media'],
                    caption=content.get('caption', '')
                )
            elif content['type'] == 'document':
                await bot.send_document(
                    int(user_id),
                    document=content['media'],
                    caption=content.get('caption', '')
                )
            
            success += 1
            await asyncio.sleep(0.1)  # Защита от ограничений
        except TelegramForbiddenError:
            failed += 1  # Пользователь заблокировал бота
        except Exception as e:
            failed += 1
            logging.error(f"Ошибка рассылки для {user_id}: {str(e)}")
            if not isinstance(e, (TelegramBadRequest, TimeoutError)):
                raise
    
    # Отправляем отчет администратору
    report = (
        f"📊 Результаты рассылки:\n"
        f"• Всего получателей: {len(user_ids)}\n"
        f"• Успешно: {success}\n"
        f"• Не удалось: {failed}"
    )
    
    if errors:
        unique_errors = set(errors)
        report += f"\n\nОсновные ошибки:\n" + "\n".join([f"- {e}" for e in list(unique_errors)[:3]])
    
    try:
        await bot.send_message(ADMINS[0], report)
    except Exception as e:
        logging.error(f"Не удалось отправить отчет: {str(e)}")


##===============ОБРАБОТЧИКИ=================

@dp.message(F.text == "🔄 Обновить кэш")
async def handle_cache_refresh(message: types.Message):
    """Обновление кэша"""
    if message.from_user.id not in ADMINS:
        return
    
    try:
        cache.clear()
        await preload_cache()
        await message.answer("✅ Кэш успешно обновлен!", 
                            reply_markup=admin_panel_keyboard())
    except Exception as e:
        await message.answer(f"❌ Ошибка обновления кэша: {str(e)}", 
                            reply_markup=admin_panel_keyboard())

@dp.message(F.text == "🔧 Сервисный режим")
async def handle_service_mode_menu(message: types.Message):
    """Управление сервисным режимом"""
    if message.from_user.id not in ADMINS:
        return
    
    status = "🟢 ВКЛЮЧЕН" if SERVICE_MODE else "🔴 ВЫКЛЮЧЕН"
    await message.answer(
        f"🛠 Текущий статус сервисного режима: {status}\nВыберите действие:",
        reply_markup=service_mode_keyboard()
    )

@dp.message(F.text == "🟢 Включить сервисный режим")
async def enable_service_mode(message: types.Message):
    """Включение сервисного режима"""
    if message.from_user.id not in ADMINS:
        return
    
    global SERVICE_MODE
    SERVICE_MODE = True
    await message.answer("✅ Сервисный режим включен", 
                        reply_markup=admin_panel_keyboard())

@dp.message(F.text == "🔴 Выключить сервисный режим")
async def disable_service_mode(message: types.Message):
    """Выключение сервисного режима"""
    if message.from_user.id not in ADMINS:
        return
    
    global SERVICE_MODE
    SERVICE_MODE = False
    await message.answer("✅ Сервисный режим выключен", 
                        reply_markup=admin_panel_keyboard())





#============================Задачи========================
def format_task_message(task_id: str, task: dict) -> str:
    """Форматирует сообщение задачи для отправки пользователю."""
    lines = [f"📌 *Задача #{task_id}*"]
    
    lines.append(f"▫️ {task['text']}")
    
    if task.get('creator_initials'):
        lines.append(f"👤 Создал: {task['creator_initials']}")
        
    if task.get('deadline'):
        # Можно добавить проверку и подсветку просроченных
        lines.append(f"⏰ *Дедлайн:* {task['deadline']}")
    else:
        lines.append("⏳ *Дедлайн:* Не установлен")
        
    if task.get('link'):
        # Используем Markdown для ссылки
        lines.append(f"🔗 [Ссылка на документ]({task['link']})")
    # else: # Можно не добавлять, если ссылка не обязательна
    #     lines.append("📎 Ссылка: Нет")
        
    return "\n".join(lines)


# --- Исправленный фрагмент assign_tasks_to_users ---
async def assign_tasks_to_users(task_ids: list[str], user_ids: list[int], sheet=None):
    """
    Назначает задачи пользователям, обновляя столбец "Назначена" (H) в Google Sheets.
    Args:
        task_ids (list[str]): Список ID задач для обновления.
        user_ids (list[int]): Список ID пользователей, которым назначаются задачи.
        sheet (gspread.Worksheet, optional): Лист задач. Если None, будет получен заново.
    """
    if not task_ids or not user_ids:
        logging.info("Нет задач или пользователей для назначения.")
        return
    try:
        if sheet is None:
            sheet = get_tasks_sheet()
        # Преобразуем user_ids в строку, разделенную запятыми
        assigned_users_str = ", ".join(map(str, user_ids))
        # Получаем все значения столбца ID задачи (A)
        task_id_col_values = sheet.col_values(1) # 1 = столбец A
        # Создаем словарь {task_id: row_number}
        task_id_to_row = {str(task_id_col_values[i]).strip(): i + 1 for i in range(len(task_id_col_values))}
        batch_updates = []
        updated_count = 0
        for task_id in task_ids:
            row_number = task_id_to_row.get(str(task_id))
            if row_number:
                # --- Исправлено: Столбец "Назначена" это H (8) ---
                assigned_column_index = 8 # H = 8
                range_label = gspread.utils.rowcol_to_a1(row_number, assigned_column_index)
                batch_updates.append({
                    'range': range_label,
                    'values': [[assigned_users_str]]
                })
                updated_count += 1
            else:
                logging.warning(f"Строка для задачи {task_id} не найдена при назначении.")
        if batch_updates:
            # Выполняем пакетное обновление
            sheet.batch_update(batch_updates)
            logging.info(f"✅ Назначено {updated_count} задач {len(user_ids)} пользователям.")
        else:
            logging.warning("Не найдено строк для обновления при назначении задач.")
    except gspread.exceptions.APIError as e:
        logging.error(f"API ошибка Google Sheets при назначении задач: {e}")
    except Exception as e:
        logging.error(f"Ошибка при назначении задач пользователям: {e}", exc_info=True)


@dp.message(F.text == "📝 Управление задачами")
async def handle_task_menu(message: types.Message):
    if message.from_user.id not in ADMINS:
        return
    await message.answer("📝 Управление задачами:", reply_markup=tasks_admin_keyboard())

@dp.message(F.text == "➕ Добавить задачу")
async def add_task_text(message: types.Message, state: FSMContext):
    await message.answer("📝 Введите текст задачи:", reply_markup=cancel_keyboard())
    await state.set_state(TaskStates.add_text)

@dp.message(TaskStates.add_text)
async def add_task_link(message: types.Message, state: FSMContext):
    await state.update_data(text=message.text)
    await message.answer("🔗 Пришлите ссылку на Google Sheets (или /skip):")
    await state.set_state(TaskStates.add_link)

@dp.message(TaskStates.add_link)
async def add_task_deadline(message: types.Message, state: FSMContext):
    link = message.text if message.text != "/skip" else None
    await state.update_data(link=link)
    await message.answer("📅 Укажите дедлайн (ДД.ММ.ГГГГ или /skip):")
    await state.set_state(TaskStates.add_deadline)

@dp.message(TaskStates.add_deadline)
async def save_task_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    deadline = message.text if message.text != "/skip" else None
    
    if deadline and not re.match(r"^\d{2}\.\d{2}\.\d{4}$", deadline):
        await message.answer("❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ:")
        return
    
    task_id = str(int(time.time()))
    creator_initials = await get_user_initials(message.from_user.id)
    
    await save_task(
        task_id=task_id,
        text=data["text"],
        creator_id=message.from_user.id,
        creator_initials=creator_initials,
        link=data.get("link"),
        deadline=deadline
    )
    
    await message.answer(
        f"✅ Задача добавлена!\n"
        f"ID: `{task_id}`\n"
        f"Дедлайн: {deadline if deadline else 'не установлен'}",
        reply_markup=tasks_admin_keyboard()
    )
    await state.clear()


@dp.message(F.text == "🗑️ Удалить задачу")
async def delete_task_start(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMINS:
        return
    tasks = await load_tasks()
    if not tasks:
        await message.answer("❌ Нет задач для удаления.", reply_markup=tasks_admin_keyboard())
        return
    # Формируем список задач
    tasks_list = "\n".join([f"ID: `{tid}` — {task['text'][:50]}{'...' if len(task['text']) > 50 else ''}" for tid, task in tasks.items()])
    await message.answer(
        f"📝 *Список задач:*\n"
        f"{tasks_list}\n\n"
        f"✏️ *Введите ID задачи для удаления:*",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=cancel_keyboard()
    )
    await state.set_state(TaskStates.delete_task)


@dp.message(TaskStates.delete_task)
async def delete_task_handler(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMINS:
        return
    task_id = message.text.strip()
    tasks = await load_tasks() # Перезагружаем, чтобы убедиться в актуальности
    task = tasks.get(task_id)
    if not task:
        await message.answer("❌ Задача с таким ID не найдена.", reply_markup=tasks_admin_keyboard())
        await state.clear()
        return

    # Сохраняем ID задачи для подтверждения
    await state.update_data(task_id_to_delete=task_id, task_text_to_delete=task['text'])
    
    # Создаем inline-клавиатуру для подтверждения
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да, удалить", callback_data=f"confirm_delete:{task_id}")
    builder.button(text="❌ Нет, отмена", callback_data="cancel_delete")
    builder.adjust(2)
    
    await message.answer(
        f"❓ *Вы уверены, что хотите удалить задачу?*\n"
        f"ID: `{task_id}`\n"
        f"Текст: {task['text'][:100]}{'...' if len(task['text']) > 100 else ''}",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=builder.as_markup()
    )
    await state.set_state(TaskStates.confirm_delete)


@dp.callback_query(TaskStates.confirm_delete, F.data.startswith("confirm_delete:"))
async def confirm_delete_task(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMINS:
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
        
    data = await state.get_data()
    task_id_to_delete = data.get('task_id_to_delete')
    task_id_from_callback = callback.data.split(":")[1]

    if task_id_to_delete != task_id_from_callback:
        await callback.answer("❌ Ошибка данных", show_alert=True)
        await state.clear()
        return

    success = await delete_task(task_id_to_delete, callback.from_user.id)
    if success:
        await callback.message.edit_text(f"✅ Задача `{task_id_to_delete}` успешно удалена.", parse_mode=ParseMode.MARKDOWN)
        # Или отправить новое сообщение и удалить старое, если edit_text не подходит
        # await callback.message.delete()
        # await callback.message.answer(f"✅ Задача `{task_id_to_delete}` успешно удалена.", reply_markup=tasks_admin_keyboard())
    else:
        await callback.message.edit_text("❌ Не удалось удалить задачу. Возможно, она не существует или у вас нет прав.", parse_mode=ParseMode.MARKDOWN)
        # await callback.message.answer("❌ Не удалось удалить задачу...", reply_markup=tasks_admin_keyboard())
    
    await state.clear()
    # Отправляем обновленное меню задач (если не редактировали сообщение выше)
    # await callback.message.answer("📝 Управление задачами:", reply_markup=tasks_admin_keyboard())
    await callback.answer() # Закрываем уведомление о нажатии


@dp.callback_query(TaskStates.confirm_delete, F.data == "cancel_delete")
async def cancel_delete_task(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMINS:
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.clear()
    await callback.message.edit_text("❌ Удаление задачи отменено.", parse_mode=ParseMode.MARKDOWN)
    # await callback.message.answer("❌ Удаление задачи отменено.", reply_markup=tasks_admin_keyboard())
    await callback.answer()


@dp.message(F.text == "📤 Отправить список")
async def send_tasks_menu(message: types.Message, state: FSMContext):
    tasks = await load_tasks()
    if not tasks:
        await message.answer("❌ Нет задач для отправки.", reply_markup=tasks_admin_keyboard())
        return
    
    await state.update_data(tasks=tasks)
    keyboard = create_keyboard(
        ["Отправить все", "Выбрать задачи", "Статистика выполнения", "🔙 Назад"],
        (2, 2)
    )
    await message.answer("Выберите действие:", reply_markup=keyboard)
    await state.set_state(TaskStates.select_action)

@dp.message(TaskStates.select_action, F.text == "Отправить все")
async def send_all_tasks(message: types.Message, state: FSMContext):
    data = await state.get_data()
    tasks = data.get("tasks")

    if not tasks:
        await message.answer("❌ Нет задач для отправки.")
        await state.clear()
        return

    await state.update_data(selected_tasks=tasks)

    await message.answer(
        f"✅ Выбраны все задачи: {len(tasks)} шт.\nВыберите аудиторию:",
        reply_markup=create_keyboard(
            ["Всем пользователям", "По должности", "Вручную", "🔙 Назад"],
            (2, 2)
        )
    )
    await state.set_state(TaskStates.select_audience)


@dp.message(TaskStates.select_action, F.text == "Выбрать задачи")
async def select_action_to_send(message: types.Message, state: FSMContext):
    data = await state.get_data()
    tasks = data['tasks']
    
    tasks_list = "\n".join([f"{task_id}: {task['text']}" for task_id, task in tasks.items()])
    await message.answer(
        f"Введите ID задач через запятую:\n{tasks_list}",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(TaskStates.input_task_ids)


@dp.message(TaskStates.input_task_ids)
async def process_task_ids(message: types.Message, state: FSMContext):
    data = await state.get_data()
    all_tasks = data['tasks']
    
    # Нормализуем ввод: удаляем пробелы и пустые значения
    input_ids = [tid.strip() for tid in message.text.split(",") if tid.strip()]
    
    if not input_ids:
        await message.answer("❌ Не указано ни одного ID задачи.")
        return
    
    # Преобразуем все ID к строковому типу для сравнения
    all_task_ids = {str(k): v for k, v in all_tasks.items()}
    
    # Фильтруем задачи
    valid_tasks = {}
    invalid_ids = []
    
    for input_id in input_ids:
        if input_id in all_task_ids:
            valid_tasks[input_id] = all_task_ids[input_id]
        else:
            invalid_ids.append(input_id)
    
    if not valid_tasks:
        await message.answer("❌ Не найдено ни одной действительной задачи.")
        return
    
    # Сообщаем о невалидных ID (если есть)
    if invalid_ids:
        await message.answer(
            f"⚠️ Не найдены задачи с ID: {', '.join(invalid_ids)}\n"
            f"Будут отправлены только действительные задачи.",
            reply_markup=types.ReplyKeyboardRemove()
        )
        await asyncio.sleep(2)  # Даем время прочитать сообщение
    
    await state.update_data(selected_tasks=valid_tasks)
    
    # Переходим к выбору аудитории
    await message.answer(
        f"✅ Готово к отправке: {len(valid_tasks)} задач\n"
        "Выберите аудиторию:",
        reply_markup=create_keyboard(["Всем пользователям","По должности", "Вручную", "🔙 Назад"], (2, 2))
    )
    await state.set_state(TaskStates.select_audience)


@dp.message(TaskStates.select_audience, F.text == "Всем пользователям")
async def send_to_all_users(message: types.Message, state: FSMContext):
    user_ids = users_sheet.col_values(1)[1:] # Предполагаем, что ID в первом столбце, без заголовка
    await state.update_data(user_ids=user_ids)
    # --- Изменено: Переход в новое состояние ---
    await state.set_state(TaskStates.review_selection)
    await review_selection_summary(message, state)


@dp.message(TaskStates.select_audience, F.text == "По должности")
async def ask_for_position_filter(message: types.Message, state: FSMContext):
    await message.answer("👥 Введите должность:", reply_markup=cancel_keyboard())
    await state.set_state(TaskStates.input_position)


@dp.message(TaskStates.select_audience, F.text == "Вручную")
async def ask_for_manual_ids(message: types.Message, state: FSMContext):
    """Обработчик кнопки 'Вручную' в меню выбора аудитории."""
    logging.info(f"Пользователь {message.from_user.id} выбрал 'Вручную' в состоянии select_audience. Текст сообщения: '{message.text}'")
    # Добавим явную проверку состояния перед ответом
    current_state = await state.get_state()
    logging.info(f"Текущее состояние перед ответом: {current_state}")
    
    try:
        await message.answer("🔢 Введите ID пользователей через запятую (например: 123456789, 987654321):", reply_markup=cancel_keyboard())
        await state.set_state(TaskStates.input_manual_ids)
        logging.info(f"Состояние успешно изменено на input_manual_ids для пользователя {message.from_user.id}")
    except Exception as e:
        logging.error(f"Ошибка в ask_for_manual_ids для пользователя {message.from_user.id}: {e}", exc_info=True)
        # Отправим сообщение об ошибке, если что-то пошло не так
        try:
            await message.answer("❌ Произошла ошибка. Попробуйте снова или выберите другой способ.", reply_markup=tasks_admin_keyboard())
            await state.clear()
        except:
            pass


@dp.message(TaskStates.input_position)
async def process_position_filter(message: types.Message, state: FSMContext):
    position_input = message.text.strip().lower()
    try:
        users_data = pickle.loads(cache.get("users_data", b"[]"))
        matched_user_ids = [
            str(u["ID пользователя"])
            for u in users_data
            if str(u.get("Должность", "")).strip().lower() == position_input
        ]
        if not matched_user_ids:
            await message.answer("❌ Пользователи с такой должностью не найдены.", reply_markup=tasks_admin_keyboard())
            await state.clear() # <-- Важно: очищать состояние при ошибке
            return
        await state.update_data(user_ids=matched_user_ids)
        # --- Изменено: Переход в новое состояние ---
        await state.set_state(TaskStates.review_selection)
        await review_selection_summary(message, state)
    except Exception as e:
        logging.error(f"Ошибка при фильтрации по должности: {str(e)}")
        await message.answer("❌ Ошибка обработки должности", reply_markup=tasks_admin_keyboard())
        await state.clear()


@dp.message(TaskStates.input_manual_ids)
async def handle_manual_user_ids(message: types.Message, state: FSMContext):
    user_ids = [uid.strip() for uid in message.text.split(",") if uid.strip().isdigit()]
    if not user_ids:
        await message.answer("❌ Нет валидных ID. Попробуйте снова.", reply_markup=cancel_keyboard())
        # Не очищаем состояние, позволяем повторный ввод
        return
    await state.update_data(user_ids=user_ids)
    # --- Изменено: Переход в новое состояние ---
    await state.set_state(TaskStates.review_selection)
    await review_selection_summary(message, state)


async def review_selection_summary(message: types.Message, state: FSMContext):
    """
    Отправляет администратору сводку по выбранной аудитории и задачам перед финальным подтверждением.
    """
    data = await state.get_data()
    user_ids = data.get("user_ids", [])
    selected_tasks = data.get("selected_tasks", {})
    
    if not user_ids or not selected_tasks:
        await message.answer("❌ Ошибка: Нет данных для подтверждения (пользователи или задачи).", reply_markup=tasks_admin_keyboard())
        await state.clear()
        return

    # Формируем сводку
    summary_lines = [
        "🔍 *Предварительный просмотр рассылки*:",
        f"• *Задач для отправки:* {len(selected_tasks)}",
        f"• *Пользователей для рассылки:* {len(user_ids)}",
        "",
        "*Выбранные задачи:*"
    ]
    # Ограничиваем список задач, если их много
    task_items = list(selected_tasks.items())
    for task_id, task in task_items[:5]: # Показываем первые 5
        summary_lines.append(f"  • `#{task_id}`: {task['text'][:50]}{'...' if len(task['text']) > 50 else ''}")
    if len(task_items) > 5:
        summary_lines.append(f"  ... и ещё {len(task_items) - 5} задач(и).")

    summary_lines.append("")
    summary_lines.append("Вы уверены, что хотите отправить эти задачи этой аудитории?")

    summary_text = "\n".join(summary_lines)
    
    # Отправляем сводку и клавиатуру подтверждения
    await message.answer(
        summary_text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=create_keyboard(["📤 Подтвердить отправку", "❌ Отмена"], (2,))
    )

async def send_selected_tasks(selected_tasks: dict, user_ids: list):
    results = {"success": 0, "failed": 0}
    
    for user_id in user_ids:
        try:
            for task_id, task in selected_tasks.items():
                await bot.send_message(
                    user_id,
                    format_task_message(task_id, task),
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_task_keyboard(task_id))
            results["success"] += 1
            logging.info(f"Sent tasks to {user_id}")
        except Exception as e:
            results["failed"] += 1
            logging.error(f"Error sending to {user_id}: {str(e)}")
    
    return results


@dp.message(TaskStates.input_manual_ids)
async def handle_manual_user_ids(message: types.Message, state: FSMContext):
    user_ids = [uid.strip() for uid in message.text.split(",") if uid.strip().isdigit()]

    if not user_ids:
        await message.answer("❌ Нет валидных ID. Попробуйте снова.")
        return

    await state.update_data(user_ids=user_ids)

    await message.answer(
        f"✅ Указано ID: {len(user_ids)}\n📤 Подтвердите отправку задач.",
        reply_markup=create_keyboard(["📤 Подтвердить отправку", "❌ Отмена"], (2,))
    )
    await state.set_state(TaskStates.confirmation)


@dp.message(F.text == "🔙 Назад")
async def handle_back_from_tasks(message: types.Message, state: FSMContext):
    """Обработчик кнопки Назад в меню задач"""
    await state.clear()
    await message.answer("🔙 Возврат в админ-панель", 
                        reply_markup=admin_panel_keyboard())


@dp.message(TaskStates.select_audience, F.text == "❌ Отмена")
async def cancel_sending(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Рассылка отменена", reply_markup=tasks_admin_keyboard())


@dp.callback_query(F.data.startswith("task_done:"))
async def mark_task_done(callback: types.CallbackQuery):
    task_id = callback.data.split(":")[1]
    user_id = callback.from_user.id
    sheet = get_tasks_sheet()
    try:
        cell = sheet.find(task_id)
        if not cell:
            await callback.answer("❌ Задача не найдена")
            return
        # --- Исправлено: Работаем с ключом "completed_by" ---
        # Получаем текущие статусы
        statuses_raw = sheet.cell(cell.row, 9).value # Столбец I (9) - "Статусы"
        try:
            # Пытаемся распарсить JSON. Если не удается, создаем пустой.
            statuses_data = json.loads(statuses_raw) if statuses_raw.strip() else {}
        except (json.JSONDecodeError, TypeError):
            logging.warning(f"Неверный формат JSON для задачи {task_id} в строке {cell.row}. Создаю новый.")
            statuses_data = {}

        # Инициализируем список выполненных, если его нет
        if "completed_by" not in statuses_data:
            statuses_data["completed_by"] = []

        # Проверяем, выполнен ли уже
        if str(user_id) in statuses_data["completed_by"]:
             await callback.answer("✅ Уже отмечено")
             return

        # Добавляем пользователя в список выполнивших
        statuses_data["completed_by"].append(str(user_id))

        # Сохраняем обновленный JSON
        sheet.update_cell(cell.row, 9, json.dumps(statuses_data, ensure_ascii=False))
        await callback.answer("✅ Отмечено как выполнено")
        try:
            # Получаем обновленные данные задачи для форматирования
            # Это упрощенный способ, в идеале перезагружать задачу
            # или передавать текст изначально. Для демонстрации сойдет.
            # Альтернатива: хранить task_text в callback_data или в состоянии.
            
            # Простое обновление текста и кнопки
            original_text = callback.message.text
            # Добавляем отметку о выполнении в текст (если нужно)
            # updated_text = f"{original_text}\n\n✅ *Выполнено вами*"
            
            new_markup = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="✔️ Выполнено", callback_data="task_already_done")]
            ])
            # await callback.message.edit_text(text=updated_text, parse_mode=ParseMode.MARKDOWN, reply_markup=new_markup)
            # Или просто обновляем кнопку
            await callback.message.edit_reply_markup(reply_markup=new_markup)
        except Exception as e:
            # Ошибка редактирования сообщения не критична
            logging.warning(f"Не удалось обновить сообщение задачи {task_id} для пользователя {user_id}: {e}")
            
    except Exception as e:
        logging.error(f"Ошибка отметки задачи {task_id} пользователем {user_id}: {str(e)}", exc_info=True)
        await callback.answer("❌ Ошибка выполнения. Попробуйте позже.", show_alert=True)


@dp.callback_query(F.data == "task_already_done")
async def handle_already_done(callback: types.CallbackQuery):
    await callback.answer("✅ Задача уже выполнена вами.", show_alert=True)


# --- Исправленный фрагмент check_deadlines ---
async def check_deadlines():
    """
    Проверка просроченных задач и уведомление пользователей,
    которым задача была назначена, но которые её НЕ ВЫПОЛНИЛИ.
    Проверка происходит раз в сутки.
    """
    while True:
        try:
            logging.info("🔍 Начало проверки просроченных задач...")
            tasks = await load_tasks() # Используем исправленную load_tasks
            if not tasks:
                logging.info("📭 Нет задач для проверки.")
                await asyncio.sleep(86400) # Ждем 24 часа
                continue
            today_date = datetime.now().date()
            notified_count = 0
            for task_id, task in tasks.items():
                deadline_str = task.get("deadline")
                if not deadline_str:
                    continue # Пропускаем задачи без дедлайна
                try:
                    # Преобразуем строку дедлайна в объект date
                    deadline_date = datetime.strptime(deadline_str, "%d.%m.%Y").date()
                except ValueError as e:
                    logging.warning(f"Неверный формат даты для задачи {task_id} ('{deadline_str}'): {e}")
                    continue
                # Проверяем, просрочена ли задача
                if deadline_date < today_date:
                    logging.info(f"⏰ Найдена просроченная задача {task_id}: {task['text']}")
                    # --- Исправлено: Используем ключи из исправленной load_tasks ---
                    # Получаем множества ID
                    assigned_users = set(task.get("assigned_to", []))
                    completed_users = set(task.get("completed_by", [])) # <-- Исправленный ключ
                    # Находим пользователей для уведомления: назначены, но не выполнили
                    users_to_notify = assigned_users - completed_users
                    if not users_to_notify:
                        logging.info(f"📭 По задаче {task_id} нет пользователей для уведомления "
                                   f"(все выполнили ({len(completed_users)}) или никто не назначен ({len(assigned_users)})).")
                        continue
                    # Формируем сообщение
                    # Формируем сообщение
                    notification_text = f"🚨 *Просроченная задача!*\n📌 Задача #{task_id}: {task['text']}\n📅 Дедлайн был: {deadline_str}"

                    # Отправляем уведомления
                    for user_id_str in users_to_notify:
                        try:
                            user_id_int = int(user_id_str)
                            await bot.send_message(
                                user_id_int,
                                notification_text,
                                parse_mode=ParseMode.MARKDOWN
                            )
                            logging.info(f"✉️ Уведомление о просроченной задаче {task_id} отправлено пользователю {user_id_int}")
                            notified_count += 1
                            # Небольшая пауза между сообщениями
                            await asyncio.sleep(0.1)
                        except ValueError:
                            logging.error(f"Неверный формат ID пользователя '{user_id_str}' для задачи {task_id}")
                        except Exception as e: # TelegramForbiddenError, TelegramRetryAfter и др.
                            logging.error(f"❌ Ошибка отправки уведомления пользователю {user_id_str} по задаче {task_id}: {e}")
                    logging.info(f"✅ По задаче {task_id} уведомлено {len(users_to_notify)} пользователей.")
            logging.info(f"🏁 Проверка просроченных задач завершена. Отправлено уведомлений: {notified_count}")
        except Exception as e:
            logging.error(f"🚨 Критическая ошибка в check_deadlines: {e}", exc_info=True)
        # Ждем 24 часа до следующей проверки (86400 секунд)
        logging.info("⏳ check_deadlines уходит в сон на 24 часа.")
        await asyncio.sleep(86400)


@dp.message(TaskStates.review_selection, F.text == "📤 Подтвердить отправку") # <-- Новый фильтр
async def confirm_task_dispatch(message: types.Message, state: FSMContext):
    """
    Подтверждение и выполнение рассылки задач.
    Перед отправкой назначает задачи выбранным пользователям.
    """
    data = await state.get_data()
    user_ids = data.get("user_ids", [])
    selected_tasks = data.get("selected_tasks", {})
    # --- Проверка наличия данных ---
    if not user_ids or not selected_tasks:
        await message.answer("❌ Нет получателей или задач для отправки.", reply_markup=tasks_admin_keyboard())
        await state.clear()
        return
    wait_msg = await message.answer("🔄 Начинаю процесс отправки задач...") # <-- Добавлено: Сообщение о начале
    try:
        task_ids_to_assign = list(selected_tasks.keys())
        # Преобразуем user_ids из строки (как они хранятся в state) в int
        user_ids_int = [int(uid) for uid in user_ids if uid.isdigit()]
        if task_ids_to_assign and user_ids_int:
            # Назначаем задачи пользователям в Google Sheets
            # Передаем sheet, чтобы не переоткрывать соединение
            sheet = get_tasks_sheet() 
            await assign_tasks_to_users(task_ids_to_assign, user_ids_int, sheet=sheet)
            await message.answer("✅ Задачи успешно назначены выбранным пользователям.")
        else:
            logging.warning("Нет корректных ID задач или пользователей для назначения.")
    except Exception as e:
        logging.error(f"Ошибка при назначении задач: {e}")
        # Можно решить, продолжать ли рассылку в случае ошибки назначения
        await message.answer("⚠️ Ошибка при назначении задач, но рассылка продолжится.")
    success = 0
    failed = 0
    total_attempts = len(user_ids) * len(selected_tasks)
    if total_attempts > 100: # <-- Пример: для больших рассылок показываем прогресс
         progress_msg = await message.answer(f"📨 Отправка... (0/{len(user_ids)})")
    for i, uid in enumerate(user_ids):
        for task_id, task in selected_tasks.items():
            try:
                await bot.send_message(
                    int(uid),
                    format_task_message(task_id, task),
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_task_keyboard(task_id)
                )
                success += 1
                await asyncio.sleep(0.05) # немного уменьшена пауза
            except Exception as e:
                logging.warning(f"Ошибка отправки задачи {task_id} пользователю {uid}: {e}")
                failed += 1
        # Обновляем прогресс, если нужно
        if total_attempts > 100 and (i + 1) % 10 == 0: # <-- Обновляем каждые 10 пользователей
            try:
                await progress_msg.edit_text(f"📨 Отправка... ({i+1}/{len(user_ids)})")
            except:
                pass # Игнорируем ошибки редактирования прогресса
    # Финальный отчет
    report = f"📊 Отправка завершена:\n• Пользователей: {len(user_ids)}\n• Задач каждому: {len(selected_tasks)}\n• Успешных отправок: {success}\n• Ошибок: {failed}"
    await message.answer(report, reply_markup=tasks_admin_keyboard())
    await state.clear()
    # Удаляем сообщения о прогрессе, если они были
    if total_attempts > 100:
         try:
             await wait_msg.delete()
             await progress_msg.delete()
         except:
             pass


@dp.message(TaskStates.review_selection, F.text == "❌ Отмена") # <-- Новый фильтр
async def cancel_task_dispatch(message: types.Message, state: FSMContext):
    await message.answer("❌ Отправка отменена", reply_markup=tasks_admin_keyboard())
    await state.clear()


# --- Рефакторинг handle_mytasks ---
@dp.message(Command("mytasks"))
async def handle_mytasks(message: types.Message):
    user_id = str(message.from_user.id) # Преобразуем в строку для сравнения
    try:
        # 1. Загружаем ВСЕ задачи с помощью универсальной функции
        all_tasks = await load_tasks() # <-- Используем load_tasks

        # 2. Фильтруем задачи: оставляем только НЕВЫПОЛНЕННЫЕ пользователем
        pending_tasks = []
        # all_tasks это словарь {task_id: task_data}
        for task_id, task_data in all_tasks.items():
             # task_data уже содержит корректно распарсенный список completed_by
             completed_users_list = task_data.get("completed_by", [])
             if user_id not in completed_users_list:
                 # task_data уже содержит нужные поля (text, deadline и т.д.)
                 # Можно использовать его напрямую
                 pending_tasks.append((task_id, task_data)) 
                 # Если по какой-то причине нужно использовать normalize_task_row, 
                 # можно, но это менее эффективно, чем использовать task_data напрямую.
                 # pending_tasks.append((task_id, normalize_task_row(task_id, task_data))) 

        if not pending_tasks:
            await message.answer("✅ У вас нет незавершённых задач.")
            return

        total_pending = len(pending_tasks)
        shown_count = min(5, total_pending)
        await message.answer(f"📋 У вас {total_pending} незавершенных задач(и). Показываю первые {shown_count}:")

        # 3. Отправляем отфильтрованные задачи
        for task_id, task in pending_tasks[:5]: # показываем максимум 5
            # Убедитесь, что format_task_message работает с форматом task_data из load_tasks
            msg = format_task_message(task_id, task) 
            try:
                await message.answer(
                    msg,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_task_keyboard(task_id)
                )
            except Exception as e:
                logging.error(f"Ошибка отправки задачи {task_id} пользователю {user_id}: {e}")
                await message.answer(f"⚠️ Ошибка при отображении задачи {task_id}")

        if total_pending > 5:
            await message.answer(
                f"ℹ️ Показаны первые 5 задач. Осталось ещё {total_pending - 5}. "
                f"Проверяйте регулярно или обратитесь к администратору за полным списком."
            )

    except Exception as e:
        logging.error(f"Ошибка в /mytasks для пользователя {message.from_user.id}: {str(e)}", exc_info=True)
        await message.answer("❌ Не удалось загрузить ваши задачи. Попробуйте позже.")


@dp.message(F.text == "📊 Статистика выполнения") # <-- Слушает из любого состояния
async def handle_stats_from_main_menu(message: types.Message, state: FSMContext):
    """Обработчик кнопки '📊 Статистика выполнения' из основного меню задач."""
    if message.from_user.id not in ADMINS:
        return
    try:
        logging.info(f"Запрос статистики от администратора {message.from_user.id}")
        tasks = await load_tasks() # Загружаем задачи
        logging.info(f"load_tasks вернул {len(tasks) if tasks else 0} задач. Тип: {type(tasks)}")
        # Логируем пример первой задачи для проверки структуры
        if tasks:
            first_task_id, first_task = next(iter(tasks.items()))
            logging.info(f"Пример задачи (ID: {first_task_id}): {first_task}")
            logging.info(f"  completed_by: {first_task.get('completed_by', 'N/A')} (тип: {type(first_task.get('completed_by', 'N/A'))})")
            logging.info(f"  assigned_to: {first_task.get('assigned_to', 'N/A')} (тип: {type(first_task.get('assigned_to', 'N/A'))})")
            
        if not tasks:
            await message.answer("📭 Нет задач для отображения статистики.", reply_markup=tasks_admin_keyboard())
            return
        # Сохраняем задачи в состоянии
        await state.update_data(tasks=tasks)
        
        # --- Логика из show_stats_menu ---
        stats_lines = ["📊 *Статистика выполнения задач:*"]
        for task_id, task in tasks.items():
            completed_count = len(task.get('completed_by', []))
            assigned_count = len(task.get('assigned_to', []))
            # Логируем подсчет для каждой задачи
            logging.info(f"Задача {task_id}: completed={completed_count}, assigned={assigned_count}")
            # Более информативная строка
            stats_line = f"🔹 `#{task_id}`: {task['text'][:30]}{'...' if len(task['text']) > 30 else ''} - ✅ {completed_count}/{assigned_count if assigned_count > 0 else '?'}"
            stats_lines.append(stats_line)
        
        stats_text = "\n".join(stats_lines) # <-- Исправлено: используем \n
        logging.info(f"Сформированный текст статистики (длина: {len(stats_text)}): {stats_text[:200]}...")
        # Проверяем длину, если слишком длинная, можно разбить на части или предложить выбор задачи
        if len(stats_text) > 4000: # Примерный лимит
             stats_text = stats_text[:3900] + "\n... (список обрезан)"
             logging.warning("Текст статистики был обрезан из-за превышения лимита длины.")
             
        await message.answer(
            stats_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=create_keyboard(["Детали по задаче", "🔙 Назад"], (1,))
        )
        await state.set_state(TaskStates.view_stats)
        logging.info("Статистика успешно отправлена.")
        
    except Exception as e:
        logging.error(f"Ошибка при запросе статистики из главного меню для пользователя {message.from_user.id}: {e}", exc_info=True)
        await message.answer("❌ Ошибка загрузки статистики.", reply_markup=tasks_admin_keyboard())
    

@dp.message(TaskStates.view_stats, F.text == "Детали по задаче")
async def ask_for_task_details(message: types.Message, state: FSMContext):
    data = await state.get_data()
    tasks = data['tasks']
    if not tasks:
         await message.answer("📭 Нет задач для детализации.", reply_markup=tasks_admin_keyboard())
         await state.set_state(TaskStates.select_action)
         return
    # Предлагаем список ID для удобства
    task_ids = list(tasks.keys())
    await message.answer(
        f"✏️ Введите ID задачи для детализации:\n"
        f"Доступные ID: {', '.join(task_ids) if len(task_ids) <= 10 else ', '.join(task_ids[:10]) + '...'}",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(TaskStates.input_task_id_for_details)




# --- Исправленный фрагмент show_task_details с markdown_decoration ---
@dp.message(TaskStates.input_task_id_for_details)
async def show_task_details(message: types.Message, state: FSMContext):
    input_task_id = str(message.text.strip())
    data = await state.get_data()
    tasks = data['tasks']
    string_keyed_tasks = {str(k): v for k, v in tasks.items()}

    if input_task_id not in string_keyed_tasks:
        similar_ids = [tid for tid in string_keyed_tasks.keys() if input_task_id in tid or tid in input_task_id]
        if similar_ids:
            # Используем markdown_decoration.quote для безопасного отображения ID в сообщении
            escaped_input_id = markdown_decoration.quote(input_task_id)
            escaped_similar_ids = [markdown_decoration.quote(sid) for sid in similar_ids[:3]]
            await message.answer(
                f"❌ Задача с ID `{escaped_input_id}` не найдена.\n"
                f"Возможно, вы имели в виду: {', '.join(escaped_similar_ids)}?",
                parse_mode=ParseMode.MARKDOWN, # parse_mode можно оставить, так как мы экранировали
                reply_markup=cancel_keyboard()
            )
            # Не очищаем state, позволяем повторный ввод
            return
        else:
            await message.answer("❌ Задача не найдена.", reply_markup=tasks_admin_keyboard())
            await state.clear()
            return

    task = string_keyed_tasks[input_task_id]
    
    # --- Улучшено и Исправлено: Получаем имена назначенных и выполнивших с экранированием ---
    assigned_user_names = []
    for user_id_str in task.get('assigned_to', []):
        try:
            initials = await get_user_initials(int(user_id_str))
            # Экранируем данные, полученные из внешних источников
            escaped_initials = markdown_decoration.quote(initials)
            escaped_user_id = markdown_decoration.quote(user_id_str)
            assigned_user_names.append(f"{escaped_initials} (ID: {escaped_user_id})")
        except (ValueError, TypeError) as e:
            logging.warning(f"Неверный формат ID пользователя '{user_id_str}' для задачи {input_task_id} (назначенные): {e}")
            # Экранируем ID даже в случае ошибки
            escaped_user_id = markdown_decoration.quote(user_id_str)
            assigned_user_names.append(f"ID: {escaped_user_id} (Ошибка)")
        except Exception as e:
            logging.error(f"Ошибка получения инициалов для ID {user_id_str} (назначенные): {e}")
            # Экранируем ID даже в случае ошибки
            escaped_user_id = markdown_decoration.quote(user_id_str)
            assigned_user_names.append(f"ID: {escaped_user_id} (Ошибка загрузки)")

    completed_user_names = []
    for user_id_str in task.get('completed_by', []): # Используем исправленный ключ
        try:
            initials = await get_user_initials(int(user_id_str))
            # Экранируем данные, полученные из внешних источников
            escaped_initials = markdown_decoration.quote(initials)
            escaped_user_id = markdown_decoration.quote(user_id_str)
            completed_user_names.append(f"{escaped_initials} (ID: {escaped_user_id})")
        except (ValueError, TypeError) as e:
            logging.warning(f"Неверный формат ID пользователя '{user_id_str}' для задачи {input_task_id} (выполнившие): {e}")
            # Экранируем ID даже в случае ошибки
            escaped_user_id = markdown_decoration.quote(user_id_str)
            completed_user_names.append(f"ID: {escaped_user_id} (Ошибка)")
        except Exception as e:
            logging.error(f"Ошибка получения инициалов для ID {user_id_str} (выполнившие): {e}")
            # Экранируем ID даже в случае ошибки
            escaped_user_id = markdown_decoration.quote(user_id_str)
            completed_user_names.append(f"ID: {escaped_user_id} (Ошибка загрузки)")

    # --- Исправлено: Экранируем данные из задачи ---
    escaped_task_id = markdown_decoration.quote(input_task_id)
    escaped_task_text = markdown_decoration.quote(task['text'])
    # Для link, deadline, creator_initials также желательно экранировать, если они могут содержать спецсимволы
    escaped_task_link = markdown_decoration.quote(task.get('link', 'Нет') if task.get('link') else 'Нет')
    escaped_task_deadline = markdown_decoration.quote(task.get('deadline', 'Не установлен'))
    escaped_creator_initials = markdown_decoration.quote(task.get('creator_initials', 'Неизвестно'))

    # --- Исправлено: Используем \n для переносов строк ---
    response_lines = [
        f"📋 *Детали задачи #{escaped_task_id}*:", # ID уже экранирован
        f"📌 *Текст:* {escaped_task_text}", # Текст экранирован
        f"👤 *Создал:* {escaped_creator_initials}", # Инициалы экранированы
        f"🔗 *Ссылка:* {escaped_task_link}", # Ссылка экранирована
        f"📅 *Дедлайн:* {escaped_task_deadline}", # Дедлайн экранирован
        f"📬 *Назначена ({len(assigned_user_names)}):*",
        ("\n".join(assigned_user_names) if assigned_user_names else "Никто не назначен"), # \n между именами
        f"✅ *Выполнили ({len(completed_user_names)}):*",
        ("\n".join(completed_user_names) if completed_user_names else "Никто не выполнил") # \n между именами
    ]
    
    # --- Исправлено: Используем \n для объединения строк ---
    response = "\n".join(response_lines) 
    
    # Проверка длины сообщения (по-прежнему актуальна)
    if len(response) > 4096:
        # Можно разбить на несколько сообщений или обрезать
        response = response[:4000] + "\n... (сообщение обрезано)"
        
    await message.answer(response, parse_mode=ParseMode.MARKDOWN, reply_markup=tasks_admin_keyboard())
    await state.clear()



# ===================== ЗАПУСК ПРИЛОЖЕНИЯ =====================
async def scheduled_cache_update():
    """Плановое обновление кэша"""
    while True:
        await asyncio.sleep(3600 * 12)  # Каждые 12 часов
        try:
            await preload_cache()
            logging.info("✅ Кэш успешно обновлен по расписанию")
        except Exception as e:
            logging.error(f"Ошибка обновления кэша: {str(e)}")

async def startup():
    """Инициализация при запуске"""
    init_tracemalloc()
    logging.info("🟢 Бот запускается...")
    try:  
        asyncio.create_task(memory_monitor())
        await preload_cache()
        asyncio.create_task(scheduled_cache_update())
        asyncio.create_task(state_cleanup_task())
        asyncio.create_task(check_deadlines())
        logging.info("✅ Кэш загружен, задачи запущены")
    except Exception as e:
        logging.critical(f"🚨 Критическая ошибка запуска: {str(e)}")
        raise

async def shutdown():
    """Завершение работы"""
    try:
        await bot.session.close()
        await dp.storage.close()
        logging.info("✅ Ресурсы успешно освобождены")
    except Exception as e:
        logging.error(f"Ошибка при завершении: {str(e)}")

async def main():
    """Главная функция запуска"""
    try:
        await startup()
        logging.info("✅ Бот запущен в режиме поллинга")
        await dp.start_polling(bot, skip_updates=True)
    except KeyboardInterrupt:
        logging.info("🛑 Бот остановлен пользователем")
    except Exception as e:
        # Ловим только критические ошибки запуска (не из обработчиков)
        logging.critical(f"🚨 Критическая ошибка запуска: {str(e)}\n{traceback.format_exc()}")
        # Уведомляем администраторов
        for admin_id in ADMINS:
            try:
                await bot.send_message(
                    admin_id,
                    f"🚨 Бот упал при запуске:\n{str(e)}\n\n{traceback.format_exc()[:3000]}"
                )
            except Exception:
                pass
    finally:
        await shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("🛑 Бот остановлен пользователем")
    except Exception as e:
        logging.critical(f"🚨 Критическая ошибка: {str(e)}")
    finally:
        asyncio.run(shutdown())
