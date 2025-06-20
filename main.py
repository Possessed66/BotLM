import os
import json
import pickle
import io
import re
import gc
import asyncio
import logging
from pyzbar.pyzbar import decode
from PIL import Image
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from aiogram.types import ReplyKeyboardRemove
from aiogram.exceptions import TelegramForbiddenError
from aiogram.filters import Command
from contextlib import suppress
from google.oauth2.service_account import Credentials
import gspread
from gspread.exceptions import APIError, SpreadsheetNotFound
from cachetools import LRUCache
import psutil

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
        ["📊 Статистика", "📢 Рассылка", "🔄 Обновить кэш", "🔧 Сервисный режим", "🔙 Главное меню"],
        (2, 2, 1)
    )

def service_mode_keyboard() -> types.ReplyKeyboardMarkup:
    return create_keyboard(
        ["🟢 Включить сервисный режим", "🔴 Выключить сервисный режим", "🔙 Назад"],
        (2, 1)
    )

def cancel_keyboard() -> types.ReplyKeyboardMarkup:
    return create_keyboard(["❌ Отмена"], (1,))

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
    """Получение данных пользователя с кэшированием"""
    cache_key = f"user_{user_id}"
    if cache_key in cache:
        return cache[cache_key]
    
    try:
        users_data = pickle.loads(cache.get("users_data", b""))
        if not users_data:
            users_data = users_sheet.get_all_records()
            cache["users_data"] = pickle.dumps(users_data)
        
        for user in users_data:
            if str(user.get("ID пользователя", "")).strip() == str(user_id).strip():
                user_data = {
                    'shop': user.get("Номер магазина", ""),
                    'name': user.get("Имя", ""),
                    'surname': user.get("Фамилия", ""),
                    'position': user.get("Должность", "")
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

async def process_barcode_image(photo: types.PhotoSize) -> Tuple[Optional[str], Optional[str]]:
    """Обработка изображения со штрих-кодом"""
    try:
        # Проверка кэша штрих-кодов
        if "barcodes_index" not in cache or not cache["barcodes_index"]:
            return None, "Система штрих-кодов не настроена"
        
        # Скачиваем фото
        file = await bot.get_file(photo.file_id)
        image_bytes = await bot.download(file.file_path)
        
        # Проверка размера
        if len(image_bytes) > MAX_IMAGE_SIZE:
            return None, f"Изображение слишком большое (> {MAX_IMAGE_SIZE//1024}KB)"
        
        # Обработка в отдельном потоке
        loop = asyncio.get_running_loop()
        image = None
        
        try:
            with io.BytesIO(image_bytes) as buffer:
                image = await loop.run_in_executor(image_processor, Image.open, buffer)
                image = image.convert('L')
                image.thumbnail((800, 800))
                
                decoded_objects = await asyncio.wait_for(
                    loop.run_in_executor(image_processor, decode, image),
                    timeout=10.0
                )
        finally:
            if image:
                image.close()
            del image_bytes
            gc.collect()
        
        if not decoded_objects:
            return None, "Штрих-код не распознан"
        
        barcode_data = decoded_objects[0].data.decode("utf-8").strip()
        barcodes_index = pickle.loads(cache["barcodes_index"])
        article = barcodes_index.get(barcode_data)
        
        return article, None if article else f"Штрих-код {barcode_data} не найден"
        
    except Exception as e:
        logging.exception(f"Ошибка обработки штрих-кода: {str(e)}")
        return None, "Ошибка обработки изображения"

async def get_product_info(article: str, shop: str) -> Optional[Dict[str, Any]]:
    """Получение информации о товаре"""
    try:
        gamma_index = pickle.loads(cache.get("gamma_index", b""))
        key = (str(article).strip(), str(shop).strip())
        product_data = gamma_index.get(key)
        
        if not product_data:
            return None
        
        supplier_id = str(product_data.get("Номер осн. пост.", "")).strip()
        supplier_list = get_supplier_dates_sheet(shop)
        
        # Поиск поставщика
        supplier_data = next(
            (item for item in supplier_list 
             if str(item.get("Номер осн. пост.", "")).strip() == supplier_id),
            None
        )
        
        if not supplier_data:
            return {
                'Артикул': article,
                'Название': product_data.get('Название', ''),
                'Отдел': str(product_data.get('Отдел', '')),
                'Магазин': shop,
                'Поставщик': 'Товар РЦ'
            }
        
        # Парсинг данных поставщика
        supplier_name = supplier_data.get("Название осн. пост.", "Не указано").strip()
        parsed_supplier = parse_supplier_data(supplier_data)
        order_date, delivery_date = calculate_delivery_date(parsed_supplier)
        
        return {
            'Артикул': article,
            'Название': product_data.get('Название', ''),
            'Отдел': str(product_data.get('Отдел', '')),
            'Магазин': shop,
            'Поставщик': supplier_name,
            'Дата заказа': order_date,
            'Дата поставки': delivery_date,
            'Номер поставщика': supplier_id
        }
    
    except Exception as e:
        logging.error(f"Ошибка получения информации о товаре: {str(e)}")
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
                    "Номер осн. пост.": item.get("Номер осн. пост.", "")
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
    """Отслеживание активности пользователя"""
    state = data.get('state')
    if state:
        current_state = await state.get_state()
        if current_state:
            state_data = await state.get_data()
            last_activity = state_data.get('last_activity', datetime.min)
            
            if isinstance(last_activity, str):
                try:
                    last_activity = datetime.fromisoformat(last_activity)
                except ValueError:
                    last_activity = datetime.min
            
            if datetime.now() - last_activity > timedelta(minutes=20):
                await state.clear()
                if event.message:
                    await event.message.answer("🕒 Сессия истекла. Начните заново.")
                elif event.callback_query:
                    await event.callback_query.message.answer("🕒 Сессия истекла. Начните заново.")
                return

            await state.update_data(last_activity=datetime.now().isoformat())
    
    return await handler(event, data)

# ===================== ОБРАБОТЧИКИ КОМАНД =====================
@dp.message(Command("start"))
async def start_handler(message: types.Message, state: FSMContext):
    """Обработчик команды /start"""
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
    try:
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
    except Exception as e:
        await message.answer("⚠️ Ошибка сохранения данных!")
        await log_error(str(message.from_user.id), str(e))

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
    """Универсальный обработчик отмены"""
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

@dp.message(OrderStates.barcode_scan, F.photo)
async def handle_barcode_scan(message: types.Message, state: FSMContext):
    """Обработка фото штрих-кода"""
    try:
        processing_msg = await message.answer("🔍 Обработка изображения...")
        photo = message.photo[-1]
        article, error = await process_barcode_image(photo)
        await processing_msg.delete()
        
        if error:
            await message.answer(f"❌ {error}\nПопробуйте еще раз или введите артикул вручную.",
                                reply_markup=article_input_keyboard())
            await state.set_state(OrderStates.article_input)
            return
            
        await state.update_data(article=article)
        await message.answer(f"✅ Штрих-код распознан!\nАртикул: {article}")
        await message.answer("📌 Выберите магазин для заказа:", 
                            reply_markup=shop_selection_keyboard())
        await state.set_state(OrderStates.shop_selection)
        
    except Exception as e:
        logging.exception("Barcode scan error")
        await message.answer("⚠️ Произошла ошибка при обработке изображения.", 
                            reply_markup=article_input_keyboard())
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
    
    await state.update_data(
        product_name=product_info['Название'],
        department=product_info['Отдел'],
        supplier_name=product_info['Поставщик']
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
    )
    
    await message.answer(response, reply_markup=confirm_keyboard())
    await state.set_state(OrderStates.confirmation)

@dp.message(OrderStates.confirmation, F.text == "✅ Подтвердить")
async def final_confirmation(message: types.Message, state: FSMContext):
    """Подтверждение заказа"""
    await state.update_data(last_activity=datetime.now().isoformat())
    data = await state.get_data()
    
    try:
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
            {'range': f'A{next_row}', 'values': [[data['selected_shop']]},
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

    except Exception as e:
        await log_error(message.from_user.id, f"Ошибка сохранения заказа: {str(e)}")
        await message.answer(f"⚠️ Ошибка сохранения: {str(e)}")

@dp.message(OrderStates.confirmation, F.text == "✏️ Исправить количество")
async def correct_quantity(message: types.Message, state: FSMContext):
    """Корректировка количества"""
    await message.answer("🔢 Введите новое количество:", 
                        reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(OrderStates.quantity_input)

# Запрос информации о товаре
@dp.message(F.text == "📋 Запрос информации")
async def handle_info_request(message: types.Message, state: FSMContext):
    """Обработчик запроса информации"""
    await state.update_data(last_activity=datetime.now().isoformat())
    await log_user_activity(message.from_user.id, "Запрос информации", "info")
    
    user_data = await get_user_data(str(message.from_user.id))
    if not user_data:
        await message.answer("❌ Сначала пройдите регистрацию через /start")
        return
        
    await state.update_data(shop=user_data['shop'])
    await message.answer("🔢 Введите артикул товара:", 
                        reply_markup=article_input_keyboard())
    await state.set_state(InfoRequest.article_input)

@dp.message(InfoRequest.article_input)
async def process_info_request(message: types.Message, state: FSMContext):
    """Обработка запроса информации"""
    # Обработка фото
    if message.photo:
        photo = message.photo[-1]
        article, error = await process_barcode_image(photo)
        
        if error:
            await message.answer(error)
            return
    else:
        # Обработка текста
        article = message.text.strip()
        if not re.match(r'^\d{4,10}$', article):
            await message.answer("❌ Неверный формат артикула.")
            return
    
    data = await state.get_data()
    user_shop = data['shop']
    product_info = await get_product_info(article, user_shop)
    
    if not product_info:
        await message.answer("❌ Товар не найден")
        await state.clear()
        return

    response = (
        f"🔍 Информация о товаре:\n"
        f"Магазин: {user_shop}\n"
        f"📦 Артикул: {product_info['Артикул']}\n"
        f"🏷️ Название: {product_info['Название']}\n"
        f"🔢 Отдел: {product_info['Отдел']}\n"
        f"📅 Ближайшая дата заказа: {product_info['Дата заказа']}\n"
        f"🚚 Ожидаемая дата поставки: {product_info['Дата поставки']}\n"
        f"🏭 Поставщик: {product_info['Поставщик']}" 
    )
    
    await message.answer(response, reply_markup=main_menu_keyboard(message.from_user.id))
    await state.clear()

# Администрирование
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
        # Основная статистика
        users_data = pickle.loads(cache.get("users_data", b"[]"))
        users_count = len(users_data) if users_data else 0
        
        stats_data = pickle.loads(cache.get("stats_data", b"[]"))
        orders_count = sum(1 for r in stats_data if len(r) > 8 and r[8] == 'order')
        
        cpu_usage = psutil.cpu_percent()
        memory_usage = psutil.virtual_memory().percent
        
        response = (
            f"📊 Статистика бота:\n\n"
            f"• Пользователей: {users_count}\n"
            f"• Заказов оформлено: {orders_count}\n"
            f"• Логов действий: {len(stats_data)}\n\n"
            f"⚙️ Состояние сервера:\n"
            f"• Загрузка CPU: {cpu_usage}%\n"
            f"• Использование RAM: {memory_usage}%\n"
            f"• Сервисный режим: {'ВКЛ' if SERVICE_MODE else 'ВЫКЛ'}"
        )
        await message.answer(response, reply_markup=admin_panel_keyboard())
        
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}", reply_markup=admin_panel_keyboard())

@dp.message(F.text == "📢 Рассылка")
async def handle_broadcast_menu(message: types.Message, state: FSMContext):
    """Начало рассылки"""
    if message.from_user.id not in ADMINS:
        return
    
    await message.answer("✉️ Введите сообщение для рассылки:", 
                        reply_markup=cancel_keyboard())
    await state.set_state(AdminBroadcast.message_input)

@dp.message(AdminBroadcast.message_input)
async def process_broadcast_message(message: types.Message, state: FSMContext):
    """Обработка сообщения для рассылки"""
    await state.update_data(message=message.html_text)
    await message.answer("✅ Сообщение сохранено\nВыберите целевую аудиторию:", 
                        reply_markup=create_keyboard(
                            ["Всем пользователям", "По магазинам", "По отделам", "Вручную", "❌ Отмена"],
                            (2, 2, 1)
                        ))
    await state.set_state(AdminBroadcast.target_selection)

@dp.message(AdminBroadcast.target_selection, F.text == "Всем пользователям")
async def handle_broadcast_all(message: types.Message, state: FSMContext):
    """Рассылка всем пользователям"""
    data = await state.get_data()
    users = [str(user['ID пользователя']) for user in pickle.loads(cache.get("users_data", b"[]"))]
    await send_broadcast(data['message'], users)
    await message.answer(f"✅ Рассылка запущена для {len(users)} пользователей", 
                        reply_markup=admin_panel_keyboard())
    await state.clear()

async def send_broadcast(message_text: str, user_ids: list) -> None:
    """Асинхронная рассылка сообщений"""
    success, failed = 0, 0
    
    for user_id in user_ids:
        try:
            if not user_id.strip():
                continue
                
            await bot.send_message(int(user_id), message_text)
            success += 1
            await asyncio.sleep(0.1)
        except Exception as e:
            failed += 1
            logging.error(f"Ошибка рассылки для {user_id}: {str(e)}")
    
    await bot.send_message(
        ADMINS[0],
        f"📊 Результаты рассылки:\n✅ Успешно: {success}\n❌ Не удалось: {failed}"
    )

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
    logging.info("🟢 Бот запускается...")
    try:  
        await preload_cache()
        asyncio.create_task(scheduled_cache_update())
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
    await startup()
    logging.info("✅ Бот запущен в режиме поллинга")
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("🛑 Бот остановлен пользователем")
    except Exception as e:
        logging.critical(f"🚨 Критическая ошибка: {str(e)}")
    finally:
        asyncio.run(shutdown())
