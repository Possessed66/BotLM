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
from pyzbar.pyzbar import decode
from io import BytesIO
from aiogram.exceptions import TelegramBadRequest
from PIL import Image, ImageEnhance
from concurrent.futures import ThreadPoolExecutor
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
BARCODES_SHEET_NAME = "Штрих-коды"
MAX_IMAGE_SIZE = 2_000_000
MAX_WORKERS = 4

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

image_processor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

# ===================== СОСТОЯНИЯ FSM =====================
class Registration(StatesGroup):
    name = State()
    surname = State()
    position = State()
    shop = State()

class OrderStates(StatesGroup):
    article_input = State()
    barcode_scan = State()
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
    
    # Состояния для отправки задач
    select_action = State()  # Выбор действия (отправить/статистика)
    select_tasks = State()   # Выбор задач для отправки
    input_task_ids = State() # Ввод ID задач вручную
    select_audience = State() # Выбор аудитории
    input_manual_ids = State() # Ввод ID пользователей
    
    # Состояния для статистики
    view_stats = State()     # Просмотр статистики
    input_task_id_for_details = State() # Ввод ID для детализации


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
        ["🔍 Сканировать штрих-код", "⌨️ Ввести вручную", "❌ Отмена", "↩️ Назад"],
        (2, 2)
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
        ["➕ Добавить задачу", "🗑️ Удалить задачу", "📤 Отправить список", "📊 Статистика", "🔙 Назад"],
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


def get_tasks_sheet():
    """Возвращает лист с задачами"""
    return main_spreadsheet.worksheet(TASKS_SHEET_NAME)

async def get_user_initials(user_id: int) -> str:
    """Возвращает инициалы пользователя (например, 'И.Иванов')"""
    user_data = await get_user_data(str(user_id))
    if not user_data:
        return "Аноним"
    name = user_data.get("Имя", "")
    surname = user_data.get("Фамилия", "")
    return f"{name[0]}.{surname}" if name else surname

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
        json.dumps({"user_ids": []})  # Пустой список для статусов
    ])

async def load_tasks() -> dict:
    """Загрузка задач из Google Sheets"""
    sheet = get_tasks_sheet()
    tasks = {}
    for row in sheet.get_all_records():
        tasks[row["ID задачи"]] = {
            "text": row["Текст"],
            "link": row["Ссылка"],
            "deadline": row["Дедлайн"],
            "creator_initials": row["Инициалы"],
            "completed_by": json.loads(row["Статусы"]).get("user_ids", [])
        }
    return tasks

async def delete_task(task_id: str, user_id: int) -> bool:
    """Удаление задачи с проверкой прав"""
    sheet = get_tasks_sheet()
    cell = sheet.find(task_id)
    if not cell:
        return False
    
    # Проверяем, что удаляет автор или админ
    task_creator_id = int(sheet.cell(cell.row, 5).value)
    if user_id != task_creator_id and user_id not in ADMINS:
        return False
    
    sheet.delete_rows(cell.row)
    return True

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


IMAGE_PROCESSING_SEMAPHORE = asyncio.Semaphore(MAX_WORKERS)


async def process_barcode_image(photo: types.PhotoSize) -> Tuple[Optional[str], Optional[str]]:
    """Улучшенная обработка изображений со штрих-кодом"""
    start_time = time.monotonic()
    # Проверка кэша штрих-кодов
    if "barcodes_index" not in cache or not cache["barcodes_index"]:
        return None, "Система штрих-кодов не настроена"

    # Выбираем фото наилучшего качества
    best_photo = max(photo, key=lambda p: p.width * p.height)
    
    try:
        # Проверяем размер файла перед скачиванием
        file_info = await bot.get_file(best_photo.file_id)
        if file_info.file_size > MAX_IMAGE_SIZE:
            return None, f"Изображение слишком большое ({file_info.file_size//1024}KB > {MAX_IMAGE_SIZE//1024}KB)"

        # Скачиваем изображение частями с проверкой размера
        image_bytes = b""
        async with IMAGE_PROCESSING_SEMAPHORE:
            file = await bot.get_file(photo.file_id)
            async for chunk in file.download(destination=None):
                if len(image_bytes) + len(chunk) > MAX_IMAGE_SIZE:
                    return None, "Превышен максимальный размер изображения"
                image_bytes += chunk

        # Обработка в отдельном потоке
        loop = asyncio.get_running_loop()
        decoded_objects = None
    
        def process_image():
            """Синхронная обработка изображения"""
            nonlocal decoded_objects
            try:
                # Оптимизация изображения перед распознаванием
                with io.BytesIO(image_bytes) as buffer:
                    with Image.open(buffer) as img:
                        # Конвертируем в grayscale для улучшения распознавания
                        img = img.convert('L')
                        
                        # Увеличиваем контраст
                        enhancer = ImageEnhance.Contrast(img)
                        img = enhancer.enhance(2.0)
                        
                        # Масштабируем, сохраняя пропорции
                        scale_factor = 800 / max(img.width, img.height)
                        new_size = (int(img.width * scale_factor), int(img.height * scale_factor))
                        img = img.resize(new_size, Image.LANCZOS)
                        
                        # Распознаем штрих-коды
                        return decode(img)
            except Exception as e:
                logging.error(f"Ошибка обработки изображения: {str(e)}")
                return None
    
        # Выполняем в thread pool с таймаутом
        decoded_objects = await asyncio.wait_for(
            loop.run_in_executor(image_processor, process_image),
            timeout=15.0
        )
        
        if not decoded_objects:
            return None, "Штрих-код не распознан"

        # Обрабатываем все найденные штрих-коды
        barcodes_index = pickle.loads(cache["barcodes_index"])
        for obj in decoded_objects:
            try:
                barcode_data = obj.data.decode("utf-8").strip()
                if article := barcodes_index.get(barcode_data):
                    return article, None
            except UnicodeDecodeError:
                # Пробуем альтернативные кодировки
                for encoding in ['latin-1', 'cp1251']:
                    try:
                        barcode_data = obj.data.decode(encoding).strip()
                        if article := barcodes_index.get(barcode_data):
                            return article, None
                    except:
                        continue
        
        return None, "Распознанный штрих-код не найден в базе"
    
    except asyncio.TimeoutError:
        return None, "Превышено время обработки изображения"
    except TelegramBadRequest as e:
        logging.error(f"Ошибка Telegram: {str(e)}")
        return None, "Ошибка загрузки файла"
    except Exception as e:
        logging.exception(f"Критическая ошибка обработки: {str(e)}")
        return None, "Ошибка обработки изображения"
    finally:
        duration = time.monotonic() - start_time
        logging.info(f"Barcode processing took {duration:.2f} seconds")
        # Принудительная очистка памяти
        del image_bytes
        gc.collect()


@profile_memory
async def get_product_info(article: str, shop: str) -> Optional[Dict[str, Any]]:
    """Получение информации о товаре с расширенным логированием"""
    try:
        logging.info(f"🔍 Поиск товара: артикул={article}, магазин={shop}")
        
        # Проверяем кэш gamma_index
        if "gamma_index" not in cache:
            logging.error("❌ gamma_index отсутствует в кэше!")
            return None
        
        try:
            gamma_index = pickle.loads(cache["gamma_index"])
            logging.info(f"Размер gamma_index: {len(gamma_index)} записей")
        except Exception as e:
            logging.error(f"Ошибка загрузки gamma_index: {str(e)}")
            return None
        
        # Формируем ключ для поиска
        lookup_key = (str(article).strip(), str(shop).strip())
        logging.info(f"Ключ поиска: {lookup_key}")
        
        # Поиск в индексе
        product_data = gamma_index.get(lookup_key)
        
        if not product_data:
            logging.warning(f"Товар не найден по ключу: {lookup_key}")
            # Попробуем найти без магазина (только по артикулу)
            found = False
            for key, data in gamma_index.items():
                if key[0] == lookup_key[0]:
                    logging.info(f"Найден альтернативный магазин: {key[1]} - {data['Название']}")
                    found = True
            if not found:
                logging.warning("Товар не найден даже без привязки к магазину")
            return None
        
        logging.info(f"Найден товар: {product_data['Название']}")
        
        # Получаем данные поставщика
        supplier_id = str(product_data.get("Номер осн. пост.", "")).strip()
        logging.info(f"ID поставщика: {supplier_id}")
        
        supplier_list = get_supplier_dates_sheet(shop)
        logging.info(f"Получено {len(supplier_list)} поставщиков для магазина {shop}")
        
        # Поиск поставщика
        supplier_data = None
        for item in supplier_list:
            if str(item.get("Номер осн. пост.", "")).strip() == supplier_id:
                supplier_data = item
                break
        
        if not supplier_data:
            logging.info("Поставщик не найден, используется резервная информация")
            return {
                'Артикул': article,
                'Название': product_data.get('Название', ''),
                'Отдел': str(product_data.get('Отдел', '')),
                'Магазин': shop,
                'Поставщик': 'Товар РЦ',
                'Топ в магазине': product_data.get('Топ в магазине', '0')
            }
        
        # Парсинг данных поставщика
        parsed_supplier = parse_supplier_data(supplier_data)
        order_date, delivery_date = calculate_delivery_date(parsed_supplier)
        
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

def get_supplier_dates_sheet(shop_number: str) -> list:
    """Получение данных поставщика с кэшированием"""
    cache_key = f"supplier_{shop_number}"
    if cache_key in cache:
        return pickle.loads(cache[cache_key])
    
    try:
        sheet = orders_spreadsheet.worksheet(f"Даты выходов заказов {shop_number}")
        data = sheet.get_all_records()
        cache[cache_key] = pickle.dumps(data)
        return data
    except Exception as e:
        logging.error(f"Ошибка получения данных поставщика: {str(e)}")
        return []


@profile_memory
async def preload_cache() -> None:
    """Предзагрузка кэша"""
    try:
        # Кэширование пользователей
        users_records = users_sheet.get_all_records()
        cache["users_data"] = pickle.dumps(users_records)
        
        # Кэширование товаров
        gamma_records = gamma_cluster_sheet.get_all_records()
        gamma_index = {}
        
        for item in gamma_records:
            article = str(item.get("Артикул", "")).strip()
            shop = str(item.get("Магазин", "")).strip()
            if article and shop:
                key = (article, shop)
                gamma_index[key] = {
                    "Название": item.get("Название", ""),
                    "Отдел": item.get("Отдел", ""),
                    "Номер осн. пост.": item.get("Номер осн. пост.", ""),
                    "Топ в магазине": str(item.get("Топ в магазине", "0"))
                }
        
        cache["gamma_index"] = pickle.dumps(gamma_index)
        
        # Кэширование штрих-кодов
        barcodes_sheet = main_spreadsheet.worksheet(BARCODES_SHEET_NAME)
        barcodes_data = barcodes_sheet.get_all_records()
        barcodes_index = {}
        
        for record in barcodes_data:
            barcode = str(record.get("Штрих-код", "")).strip()
            article = str(record.get("Артикул", "")).strip()
            if barcode and article:
                barcodes_index[barcode] = article
        
        cache["barcodes_index"] = pickle.dumps(barcodes_index)

        cache_size = sum(len(pickle.dumps(v)) for v in cache.values()) / 1024 / 1024
        logging.info(f"✅ Кэш загружен. Размер: {cache_size:.2f} MB")
        
        logging.info("✅ Кэш успешно загружен")
    except Exception as e:
        logging.error(f"Ошибка загрузки кэша: {str(e)}")

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
    
    await message.answer("📦 Выберите способ ввода артикула:", 
                        reply_markup=article_input_keyboard())
    await log_user_activity(message.from_user.id, "Заказ под клиента", "order")
    await state.set_state(OrderStates.article_input)

@dp.message(OrderStates.article_input, F.text == "🔍 Сканировать штрих-код")
async def handle_scan_choice(message: types.Message, state: FSMContext):
    """Обработка выбора сканирования"""
    await message.answer(
        "📸 Отправьте фото штрих-кода товара\n\n"
        "Советы для лучшего распознавания:\n"
        "- Убедитесь, что штрих-код хорошо освещен\n"
        "- Держите камеру прямо напротив штрих-кода\n"
        "- Избегайте бликов и теней",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(OrderStates.barcode_scan)

@dp.message(OrderStates.article_input, F.text == "⌨️ Ввести вручную")
async def handle_manual_choice(message: types.Message, state: FSMContext):
    """Обработка ручного ввода"""
    await message.answer("🔢 Введите артикул товара вручную:", 
                        reply_markup=article_input_keyboard())

@dp.message(OrderStates.article_input)
async def process_article_manual(message: types.Message, state: FSMContext):
    """Обработка введенного артикула"""
    article = message.text.strip()
    
    if not re.match(r'^\d{4,10}$', article):
        await message.answer("❌ Неверный формат артикула. Артикул должен состоять из 4-10 цифр.")
        return
        
    await state.update_data(article=article)
    await message.answer("📌 Выберите магазин для заказа:", 
                        reply_markup=shop_selection_keyboard())
    await state.set_state(OrderStates.shop_selection)


@profile_memory
@dp.message(OrderStates.barcode_scan, F.photo)
async def handle_barcode_scan(message: types.Message, state: FSMContext):
    """Обработка фото штрих-кода с улучшенной логикой"""
    try:
        # Показываем статус обработки
        processing_msg = await message.answer("🔍 Обработка изображения...")
        
        # Обрабатываем изображение
        article, error = await process_barcode_image(message.photo)
        
        # Удаляем сообщение о статусе
        await processing_msg.delete()
        
        if error:
            # Предлагаем альтернативные варианты
            await message.answer(
                f"❌ {error}\nПопробуйте:\n"
                "- Сфотографировать код при хорошем освещении\n"
                "- Убедиться, что код не поврежден\n"
                "- Или введите артикул вручную",
                reply_markup=article_input_keyboard()
            )
            await state.set_state(OrderStates.article_input)
            return
            
        # Успешное распознавание
        await state.update_data(article=article)
        await message.answer(f"✅ Штрих-код распознан! Артикул: {article}")
        
        # Переходим к выбору магазина
        await message.answer("📌 Выберите магазин для заказа:", 
                            reply_markup=shop_selection_keyboard())
        await state.set_state(OrderStates.shop_selection)
        
    except Exception as e:
        logging.exception("Barcode scan error")
        await message.answer(
            "⚠️ Произошла критическая ошибка. Попробуйте другой способ ввода.",
            reply_markup=article_input_keyboard()
        )
        await state.set_state(OrderStates.article_input)

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
        await message.answer("❌ Товар не найден в выбранном магазине")
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
            # Логирование попытки обработки фото
            logging.info(f"Обработка фото для запроса информации (пользователь: {user_id})")
            
            # Обработка фото
            photo = message.photo[-1]
            article, error = await process_barcode_image(photo)
            if error:
                await message.answer(error)
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
            await message.answer("❌ Товар не найден")
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
    deadline = task.get('Дедлайн')
    deadline_text = f"⏰ Дедлайн: {deadline}" if deadline else "⏳ Без дедлайна"
    
    return (
        f"📌 *Задача #{task_id}*\n"
        f"▫️ {task['text']}\n"
        f"👤 Создал: {task['creator_initials']}\n"
        f"{deadline_text}\n"
        f"🔗 {'[Ссылка](' + task['link'] + ')' if task.get('link') else 'Нет ссылки'}"
    )


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
    tasks = await load_tasks()
    if not tasks:
        await message.answer("❌ Нет задач для удаления.")
        return
    
    tasks_list = "\n".join(f"ID: `{id}` — {task['text']}" for id, task in tasks.items())
    await message.answer(
        f"Введите ID задачи для удаления:\n{tasks_list}",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(TaskStates.delete_task)

@dp.message(TaskStates.delete_task)
async def delete_task_handler(message: types.Message, state: FSMContext):
    task_id = message.text.strip()
    if not await delete_task(task_id, message.from_user.id):
        await message.answer("❌ Задача не найдена или нет прав!")
    else:
        await message.answer("✅ Задача удалена!", reply_markup=tasks_admin_keyboard())
    await state.clear()



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
    await state.update_data(selected_tasks=data['tasks'])  # Выбираем все задачи
    
    await message.answer(
        "Выберите аудиторию:",
        reply_markup=create_keyboard(["Всем", "По магазинам", "Вручную", "🔙 Назад"], (2, 2))
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
    selected_ids = [tid.strip() for tid in message.text.split(",")]
    
    # Фильтруем только существующие задачи
    tasks_to_send = {tid: all_tasks[tid] for tid in selected_ids if tid in all_tasks}
    
    if not tasks_to_send:
        await message.answer("❌ Не выбрано ни одной действительной задачи.")
        return
    
    await state.update_data(selected_tasks=tasks_to_send)
    
    # Показываем меню выбора аудитории
    await message.answer(
        f"Выбрано задач: {len(tasks_to_send)}\nВыберите аудиторию:",
        reply_markup=create_keyboard(["Всем", "По магазинам", "Вручную", "🔙 Назад"], (2, 2))
    )
    await state.set_state(TaskStates.select_audience)

@dp.message(TaskStates.select_audience, F.text == "Всем пользователям")
async def send_to_all(message: types.Message, state: FSMContext):
    data = await state.get_data()
    tasks = data['tasks']
    users = users_sheet.col_values(1)  # Все ID из Google Sheets
    
    success = 0
    failed = 0
    
    for user_id in users:
        try:
            for task_id, task in tasks.items():
                task_msg = format_task_message(task_id, task)
                await bot.send_message(
                    user_id,
                    task_msg,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_task_keyboard(task_id)
                )
            success += 1
            logging.info(f"Успешно отправлено пользователю {user_id}")
        except Exception as e:
            failed += 1
            logging.error(f"Ошибка отправки {user_id}: {str(e)}")
    
    await message.answer(
        f"📊 Результат рассылки:\n"
        f"• Всего получателей: {len(users)}\n"
        f"• Успешно: {success}\n"
        f"• Не удалось: {failed}",
        reply_markup=tasks_admin_keyboard()
    )
    await state.clear()

@dp.message(TaskStates.select_audience, F.text == "Вручную ввести ID")
async def ask_for_manual_ids(message: types.Message, state: FSMContext):
    await message.answer(
        "Введите ID пользователей через запятую:",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(TaskStates.input_manual_ids)

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
async def send_to_manual_ids(message: types.Message, state: FSMContext):
    data = await state.get_data()
    tasks = data['tasks']
    user_ids = [uid.strip() for uid in message.text.split(",") if uid.strip().isdigit()]
    
    if not user_ids:
        await message.answer("❌ Нет валидных ID. Попробуйте снова.")
        return
    
    results = {"success": 0, "failed": 0}
    
    for user_id in user_ids:
        try:
            for task_id, task in tasks.items():
                task_msg = format_task_message(task_id, task)
                await bot.send_message(
                    user_id,
                    task_msg,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=get_task_keyboard(task_id)
                )
            results["success"] += 1
            logging.info(f"Успешно отправлено пользователю {user_id}")
        except Exception as e:
            results["failed"] += 1
            logging.error(f"Ошибка отправки {user_id}: {str(e)}")
    
    await message.answer(
        f"📊 Результат рассылки:\n"
        f"• Получателей: {len(user_ids)}\n"
        f"• Успешно: {results['success']}\n"
        f"• Не удалось: {results['failed']}",
        reply_markup=tasks_admin_keyboard()
    )
    await state.clear()


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
    
    try:
        sheet = get_tasks_sheet()
        cell = sheet.find(task_id)
        if not cell:
            await callback.answer("❌ Задача не найдена!")
            return
        
        # Получаем текущие статусы
        statuses = json.loads(sheet.cell(cell.row, 8).value)  # Колонка 8 - "Статусы"
        
        # Добавляем пользователя если его еще нет
        if str(user_id) not in statuses["user_ids"]:
            statuses["user_ids"].append(str(user_id))
            sheet.update_cell(cell.row, 8, json.dumps(statuses))
            await callback.answer("✅ Задача отмечена выполненной!")
        else:
            await callback.answer("✔️ Вы уже отмечали эту задачу")
            
    except Exception as e:
        logging.error(f"Ошибка при отметке задачи: {str(e)}")
        await callback.answer("❌ Ошибка сервера")


async def check_deadlines():
    while True:
        tasks = await load_tasks()
        today = datetime.now().strftime("%d.%m.%Y")
        
        for task_id, task in tasks.items():
            if not task.get("deadline"):
                continue
                
            if task["deadline"] < today:  # Просрочено
                for user_id in task["completed_by"]:
                    try:
                        await bot.send_message(
                            user_id,
                            f"🚨 *Просрочено!*\nЗадача: {task['text']}\n"
                            f"Дедлайн был: {task['deadline']}",
                            parse_mode=ParseMode.MARKDOWN
                        )
                    except Exception:
                        continue
        
        await asyncio.sleep(86400)  # Проверка раз в сутки


@dp.message(TaskStates.select_action, F.text == "Статистика выполнения")
async def show_stats_menu(message: types.Message, state: FSMContext):
    data = await state.get_data()
    tasks = data['tasks']
    
    stats = []
    for task_id, task in tasks.items():
        completed = len(task['completed_by'])
        stats.append(f"{task_id}: {task['text']} - ✅ {completed} чел.")
    
    await message.answer(
        "📊 Статистика выполнения:\n\n" + "\n".join(stats),
        reply_markup=create_keyboard(["Детали по задаче", "🔙 Назад"], (1,))
    )
    await state.set_state(TaskStates.view_stats)

@dp.message(TaskStates.view_stats, F.text == "Детали по задаче")
async def ask_for_task_details(message: types.Message, state: FSMContext):
    await message.answer("Введите ID задачи для детализации:", reply_markup=cancel_keyboard())
    await state.set_state(TaskStates.input_task_id_for_details)

@dp.message(TaskStates.input_task_id_for_details)
async def show_task_details(message: types.Message, state: FSMContext):
    task_id = message.text.strip()
    data = await state.get_data()
    tasks = data['tasks']
    
    if task_id not in tasks:
        await message.answer("❌ Задача не найдена.")
        return
    
    task = tasks[task_id]
    completed_users = task['completed_by']
    
    # Получаем имена выполнивших
    user_names = []
    for user_id in completed_users:
        initials = await get_user_initials(user_id)
        user_names.append(f"{initials} (ID: {user_id})")
    
    response = (
        f"📋 Детали задачи {task_id}:\n"
        f"Текст: {task['text']}\n"
        f"Выполнили ({len(completed_users)}):\n"
    )
    response += "\n".join(user_names) if user_names else "Никто не выполнил"
    
    await message.answer(response, reply_markup=tasks_admin_keyboard())
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
