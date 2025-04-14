import os
import json
from typing import Dict, Any, List
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
from aiohttp import web
import asyncio
from cachetools import cached, TTLCache
import logging
from tenacity import retry, stop_after_attempt, wait_exponential


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

# ===================== КОНФИГУРАЦИЯ СЕРВИСНОГО РЕЖИМА =====================
SERVICE_MODE = False
ADMINS = [122086799]  # ID администраторов

# ===================== КОНФИГУРАЦИЯ КЭША =====================
CACHE_TTL = 43200  # 12 часов в секундах
cache = TTLCache(maxsize=1000, ttl=CACHE_TTL)

# ===================== КОНФИГУРАЦИЯ =====================
from dotenv import load_dotenv

load_dotenv('secret.env')  # Загружаем переменные из .env

# Проверка наличия обязательных переменных
try:
    BOT_TOKEN = os.environ['BOT_TOKEN']
    GOOGLE_CREDS_JSON = os.environ['GOOGLE_CREDENTIALS']
except KeyError as e:
    raise RuntimeError(f"Отсутствует обязательная переменная окружения: {e}")

# Преобразуем GOOGLE_CREDENTIALS из строки в объект
GOOGLE_CREDS = json.loads(GOOGLE_CREDS_JSON)
SPREADSHEET_NAME = "ShopBotData"
ORDERS_SPREADSHEET_NAME = "Копия Заказы МЗ 0.2"
USERS_SHEET_NAME = "Пользователи"
GAMMA_CLUSTER_SHEET = "Гамма кластер"
LOGS_SHEET = "Логи"

# Конфигурация для веб-хуков
# В секции конфигурации
USE_WEBHOOKS = os.getenv('USE_WEBHOOKS', 'false').lower() == 'true'

# Добавить проверку для вебхук-режима
WEBHOOK_HOST = os.getenv('WEBHOOK_HOST')  # Например: https://your-bot.render.com
WEBHOOK_PATH = "/webhook"  # Путь для веб-хука
WEBHOOK_PORT = 8443
WEBHOOK_URL = f"{WEBHOOK_HOST}:{WEBHOOK_PORT}{WEBHOOK_PATH}" if USE_WEBHOOKS else None
if USE_WEBHOOKS and not WEBHOOK_HOST.startswith("https://"):
    raise ValueError("WEBHOOK_HOST must be HTTPS URL in webhook mode")


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
except Exception as e:
    print(f"Ошибка инициализации: {str(e)}")
    exit()


# ===================== СОСТОЯНИЯ FSM =====================
class Registration(StatesGroup):
    name = State()
    surname = State()
    position = State()
    shop = State()


class OrderStates(StatesGroup):
    article_input = State()
    quantity_input = State()
    confirmation = State()
    order_reason_input = State()


class InfoRequest(StatesGroup):
    article_input = State()


class AdminBroadcast(StatesGroup):
    message_input = State()
    confirmation = State()


# ===================== ВСПОМОГАТЕЛЬНЫЙ КЛАСС =====================
class FakeSheet:
    """Имитация объекта листа для работы с кэшированными данными"""
    def __init__(self, data):
        self.data = data
        self.headers = list(data[0].keys()) if data else []
    
    def find(self, value):
        for idx, row in enumerate(self.data):
            if str(value) in row.values():
                return type('Cell', (), {'row': idx + 2})  # Эмулируем объект ячейки
        raise gspread.exceptions.CellNotFound(value)
    
    def row_values(self, row):
        return list(self.data[row-2].values()) if row-2 < len(self.data) else []
    
    def get_all_records(self):
        return self.data


# ===================== КЛАВИАТУРЫ =====================
def main_menu_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.button(text="📋 Запрос информации")
    builder.button(text="📦 Проверка стока")
    builder.button(text="🛒 Заказ под клиента")
    builder.adjust(2, 1)
    return builder.as_markup(resize_keyboard=True)


def article_input_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.button(text="❌ Отмена")
    builder.button(text="↩️ Назад")
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=True)


def confirm_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.button(text="✅ Подтвердить")
    builder.button(text="✏️ Исправить количество")
    builder.button(text="❌ Отмена")
    builder.adjust(2, 1)
    return builder.as_markup(resize_keyboard=True)


def broadcast_confirmation_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.button(text="✅ Подтвердить рассылку")
    builder.button(text="✏️ Редактировать сообщение")
    builder.button(text="❌ Отменить рассылку")
    builder.adjust(1, 2)
    return builder.as_markup(resize_keyboard=True)


# ===================== СЕРВИСНЫЙ РЕЖИМ =====================
async def notify_admins(message: str):
    """Уведомление администраторов"""
    for admin_id in ADMINS:
        with suppress(TelegramForbiddenError):
            await bot.send_message(admin_id, message)

async def broadcast(message: str):
    """Рассылка сообщения всем пользователям"""
    users = users_sheet.col_values(1)[1:]  # ID пользователей из колонки A
    for user_id in users:
        with suppress(TelegramForbiddenError, ValueError):
            await bot.send_message(int(user_id), message)

async def toggle_service_mode(enable: bool):
    """Включение/выключение сервисного режима"""
    global SERVICE_MODE
    SERVICE_MODE = enable
    status = "ВКЛЮЧЕН" if enable else "ВЫКЛЮЧЕН"
    await notify_admins(f"🛠 Сервисный режим {status}")




# ===================== СИСТЕМА КЭШИРОВАНИЯ =====================
async def cache_sheet_data(sheet, cache_key: str):
    """Кэширование данных из листа"""
    try:
        print(f"⌛ Начало загрузки кэша для ключа: {cache_key}")
        data = sheet.get_all_records()
        print(f"✅ Данные из Google Sheets ({cache_key}): {data[:1]}...")  # Первая запись для примера
        cache[cache_key] = data
        print(f"📥 Успешно загружено в кэш: {cache_key} ({len(data)} записей)")
    except Exception as e:
        print(f"🔥 Ошибка загрузки {cache_key}: {str(e)}")
        raise



async def cache_supplier_data(shop: str):
    """Кэширование данных поставщиков для магазина"""
    cache_key = f"supplier_{shop}"
    try:
        sheet = get_supplier_dates_sheet(shop)
        data = sheet.get_all_records()
        cache[cache_key] = data
        print(f"📦 Загружено поставщиков для магазина {shop}: {len(data)}")
    except Exception as e:
        print(f"⚠️ Ошибка загрузки поставщиков для магазина {shop}: {str(e)}")




async def preload_cache(_=None):  # Добавляем неиспользуемый параметр
    """Предзагрузка данных при старте бота"""
    try:
        print("♻️ Начало предзагрузки кэша...")
        
        # Основная загрузка данных
        await cache_sheet_data(users_sheet, "users")
        await cache_sheet_data(gamma_cluster_sheet, "gamma_cluster")
        
        # Загрузка данных поставщиков
        shops = users_sheet.col_values(5)[1:]  # Колонка E
        for shop in set(shops):
            await cache_supplier_data(shop)
            
        print(f"✅ Кэш загружен. Всего элементов: {len(cache)}")
        validate_cache_keys()
    
    except Exception as e:
        print(f"⚠️ Ошибка загрузки кэша: {str(e)}")
        raise

def validate_cache_keys():
    required_keys = ['users', 'gamma_cluster']
    for key in required_keys:
        if key not in cache:
            raise KeyError(f"Отсутствует обязательный ключ кэша: {key}")


# ===================== КОМАНДЫ ДЛЯ АДМИНОВ =====================
@dp.message(Command("stats"))
async def get_stats(message: types.Message):
    """Получение краткой статистики"""
    if message.from_user.id not in ADMINS:
        return
    
    try:
        spreadsheet = client.open(ORDERS_SPREADSHEET_NAME)
        stats_sheet = spreadsheet.worksheet(STATS_SHEET_NAME)
        records = stats_sheet.get_all_records()
        
        total = len(records)
        success = len([r for r in records if '✅' in r['Status']])
        failed = total - success
        
        response = (
            f"📊 Статистика уведомлений:\n\n"
            f"• Всего отправок: {total}\n"
            f"• Успешных: {success}\n"
            f"• Неудачных: {failed}\n"
            f"• Последние 5 ошибок:\n"
        )
        
        for r in records[-5:]:
            if '❌' in r['Status']:
                response += f"\n- {r['Date']}: {r['Status']}"
                
        await message.answer(response)
        
    except Exception as e:
        await message.answer(f"⚠️ Ошибка получения статистики: {str(e)}")

@dp.message(Command("full_stats"))
async def get_full_stats(message: types.Message):
    """Экспорт полной статистики"""
    if message.from_user.id not in ADMINS:
        return
    
    try:
        spreadsheet = client.open(ORDERS_SPREADSHEET_NAME)
        stats_sheet = spreadsheet.worksheet(STATS_SHEET_NAME)
        stats_sheet.export('csv')
        
        with open("stats.csv", "rb") as file:
            await message.answer_document(file, caption="📊 Полная статистика")
            
    except Exception as e:
        await message.answer(f"⚠️ Ошибка экспорта: {str(e)}")

@dp.message(F.text == "/maintenance_on")
async def maintenance_on(message: types.Message):
    if message.from_user.id not in ADMINS:
        return
    
    await toggle_service_mode(True)
    await broadcast("🔧 Бот временно недоступен. Идет обновление системы...")
    await message.answer("Сервисный режим активирован")

@dp.message(F.text == "/maintenance_off")
async def maintenance_off(message: types.Message):
    if message.from_user.id not in ADMINS:
        return
    
    await toggle_service_mode(False)
    await broadcast("✅ Обновление завершено! Бот снова в работе.")
    await message.answer("Сервисный режим деактивирован")


# ===================== ОБНОВЛЕННЫЙ МИДЛВАР =====================
@dp.update.middleware()
async def service_mode_middleware(handler, event, data):
    if SERVICE_MODE and event.message:
        # Проверяем, является ли пользователь админом
        user_id = event.message.from_user.id
        if user_id not in ADMINS:
            with suppress(TelegramForbiddenError):
                await event.message.answer("⏳ Бот в режиме обслуживания. Попробуйте позже.")
            return
    return await handler(event, data)



@dp.message(F.text.lower().in_(["отмена", "❌ отмена", "/cancel"]))
async def cancel_handler(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("🔄 Операция отменена", 
                        reply_markup=main_menu_keyboard())



@dp.update.middleware()
async def timeout_middleware(handler, event, data):
    state = data.get('state')
    if state:
        current_state = await state.get_state()
        if current_state:
            # Получаем время последней активности из хранилища
            state_data = await state.get_data()
            last_activity_str = state_data.get('last_activity')
            
            if last_activity_str:
                last_activity = datetime.fromisoformat(last_activity_str)
                if datetime.now() - last_activity > timedelta(minutes=15):
                    await state.clear()
                    await event.answer("🕒 Сессия истекла. Начните заново.")
                    return
            
            # Обновляем время активности
            await state.update_data(last_activity=datetime.now().isoformat())
    
    return await handler(event, data)

# ===================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====================
async def get_user_data(user_id: str) -> Dict[str, Any]:
    cache_key = f"user_{user_id}"
    if cache_key in cache:
        return cache[cache_key]
    
    try:
        cell = users_sheet.find(user_id)
        user_data = {
            'shop': users_sheet.cell(cell.row, 5).value,
            'name': users_sheet.cell(cell.row, 2).value,
            'surname': users_sheet.cell(cell.row, 3).value,
            'position': users_sheet.cell(cell.row, 4).value
        }
        cache[cache_key] = user_data
        return user_data
    except:
        return None


async def log_error(user_id: str, error: str):
    logs_sheet.append_row([
        datetime.now().strftime("%d.%m.%Y %H:%M"),
        user_id,
        "ERROR",
        error
    ])


def get_supplier_dates_sheet(shop_number: str):
    cache_key = f"supplier_{shop_number}"
    if cache_key in cache:
        return FakeSheet(cache[cache_key])
    
    sheet = orders_spreadsheet.worksheet(f"Даты выходов заказов {shop_number}")
    data = sheet.get_all_records()
    cache[cache_key] = data
    return FakeSheet(data)


def calculate_delivery_date(supplier_data: dict) -> tuple:
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

    # Расчет даты поставки
    delivery_date = order_date + timedelta(days=supplier_data['delivery_days'])
    return (
        order_date.strftime("%d.%m.%Y"),
        delivery_date.strftime("%d.%m.%Y")
    )


# ========================== ПАРСЕР ===========================
async def get_product_info(article: str, user_shop: str) -> dict:
    """Получение информации о товаре по артикулу"""
    try:
        gamma_data = cache.get("gamma_cluster", [])
        product_data = next(
            (item for item in gamma_data
             if str(item.get("Артикул", "")).strip() == str(article).strip()
             and str(item.get("Магазин", "")).strip() == str(user_shop).strip()),
            None
        )
        
        if not product_data:
            return None

        supplier_id = str(product_data.get("Номер осн. пост.", "")).strip()
        supplier_sheet = get_supplier_dates_sheet(user_shop)
        supplier_data = next(
            (item for item in supplier_sheet.data 
             if str(item.get("Номер осн. пост.", "")).strip() == supplier_id),
            None
        )
        
        if not supplier_data:
            return None

        # Получаем название поставщика (следующий столбец после ID)
        headers = supplier_sheet.headers
        supplier_id_index = headers.index("Номер осн. пост.")
        supplier_name = list(supplier_data.values())[supplier_id_index + 1]

        parsed_supplier = parse_supplier_data(supplier_data)
        order_date, delivery_date = calculate_delivery_date(parsed_supplier)

        supplier_name = supplier_data.get("Название осн. пост.", "Не указано").strip()

        
        return {
            'article': article,
            'product_name': product_data.get('Название', ''),
            'department': str(product_data.get('Отдел', '')),
            'order_date': order_date,
            'delivery_date': delivery_date,
            'supplier_id': supplier_id,
            'supplier_name': supplier_name,  # Новое поле
            'shop': user_shop,
            'parsed_supplier': parsed_supplier
        }
        
    except (ValueError, IndexError) as e:
        logging.error(f"Supplier name error: {str(e)}")
        return None
    except Exception as e:
        logging.error(f"Product info error: {str(e)}")
        return None


# ===================== ОБРАБОТЧИКИ КОМАНД =====================
@dp.message(Command("start"))
async def start_handler(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat())
    user_data = await get_user_data(str(message.from_user.id))
    if user_data:
        await message.answer("ℹ️ Вы уже зарегистрированы!", reply_markup=main_menu_keyboard())
        return
    await message.answer(
        "👋 Добро пожаловать! Введите ваше имя:", reply_markup=types.ReplyKeyboardRemove()
    )
    await state.set_state(Registration.name)


@dp.message(Registration.name)
async def process_name(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat())
    await state.update_data(name=message.text.strip())
    await message.answer("📝 Введите вашу фамилию:")
    await state.set_state(Registration.surname)


@dp.message(Registration.surname)
async def process_surname(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat())
    await state.update_data(surname=message.text.strip())
    await message.answer("💼 Введите вашу должность:")
    await state.set_state(Registration.position)


@dp.message(Registration.position)
async def process_position(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat())
    await state.update_data(position=message.text.strip())
    await message.answer("🏪 Введите номер магазина (только цифры, без нулей):")
    await state.set_state(Registration.shop)


@dp.message(Registration.shop)
async def process_shop(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat())
    if not message.text.strip().isdigit():
        await message.answer("❌ Номер магазина должен быть числом! Повторите ввод:")
        return
    data = await state.get_data()
    try:
        users_sheet.append_row([
            str(message.from_user.id),
            data['name'],
            data['surname'],
            data['position'],
            message.text.strip(),
            datetime.now().strftime("%d.%m.%Y %H:%M")
        ])
        await message.answer("✅ Регистрация завершена!", reply_markup=main_menu_keyboard())
        await state.clear()
    except Exception as e:
        await message.answer("⚠️ Ошибка сохранения данных!")
        await log_error(str(message.from_user.id), str(e))


@dp.message(F.text == "🛒 Заказ под клиента")
async def handle_client_order(message: types.Message, state: FSMContext):
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
    await message.answer("🔢 Введите артикул товара:", reply_markup=article_input_keyboard())
    await state.set_state(OrderStates.article_input)


@dp.message(OrderStates.article_input)
async def process_article(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat())
    article = message.text.strip()
    data = await state.get_data()
    user_shop = data['shop']
    
    product_info = await get_product_info(article, user_shop)
    if not product_info:
        await message.answer("❌ Товар не найден")
        return

    response = (
        f"Магазин: {user_shop}\n"
        f"📦 Артикул: {product_info['article']}\n"
        f"🏷️ Название: {product_info['product_name']}\n"
        f"🏭 Поставщик: {product_info['supplier_name']}\n" 
        f"📅 Дата заказа: {product_info['order_date']}\n"
        f"🚚 Дата поставки: {product_info['delivery_date']}\n"
        
    )
    
    await state.update_data(
        article=product_info['article'],
        product_name=product_info['product_name'],
        department=product_info['department'],
        order_date=product_info['order_date'],
        delivery_date=product_info['delivery_date'],
        supplier_id=product_info['supplier_id'],
        supplier_name=product_info['supplier_name']
    )
    
    await message.answer(response)
    await message.answer("🔢 Введите количество товара:")
    await state.set_state(OrderStates.quantity_input)

def parse_supplier_data(record):
    order_days = []
    for key in ['День выхода заказа', 'День выхода заказа 2', 'День выхода заказа 3']:
        value = str(record.get(key, '')).strip()  # Преобразуем в строку перед strip()
        if value and value.isdigit():
            order_days.append(int(value))
    
    delivery_days = str(record.get('Срок доставки в магазин', '0')).strip()
    return {
        'supplier_id': str(record.get('Номер осн. пост.', '')),
        'order_days': sorted(list(set(order_days))),
        'delivery_days': int(delivery_days) if delivery_days.isdigit() else 0
    }


@dp.message(OrderStates.quantity_input)
async def process_quantity(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat())
    if not message.text.strip().isdigit():
        await message.answer("❌ Введите число!")
        return
    data = await state.get_data()
    await state.update_data(quantity=int(message.text.strip()))
    # Запрос номера заказа или причины
    await message.answer("Введите номер заказа или причину:")
    await state.set_state(OrderStates.order_reason_input)


@dp.message(OrderStates.order_reason_input)
async def process_order_reason(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat())
    data = await state.get_data()
    order_reason = message.text.strip()
    user_shop = data['shop']
    # Обновляем состояние
    await state.update_data(order_reason=order_reason)
    # Вывод информации для подтверждения
    await message.answer(
        f"Магазин: {user_shop}\n"
        f"📦 Артикул: {data['article']}\n"
        f"🏷️ Название: {data['product_name']}\n"
        f"🏭 Поставщик: {data['supplier_name']}\n" 
        f"📅 Дата заказа: {data['order_date']}\n"
        f"🚚 Дата поставки: {data['delivery_date']}\n"
        f"Количество: {data['quantity']}\n"
        f"Номер заказа/Причина: {order_reason}\n",
        reply_markup=confirm_keyboard()
    )
    await state.set_state(OrderStates.confirmation)


@dp.message(OrderStates.confirmation, F.text == "✅ Подтвердить")
async def final_confirmation(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat())
    data = await state.get_data()
    try:
        # Проверка обязательных полей
        required_fields = ['shop', 'article', 'order_reason', 'quantity', 'department']
        for field in required_fields:
            if not data.get(field):
                raise ValueError(f"Отсутствует обязательное поле: {field}")

        # Получаем лист отдела
        department_sheet = orders_spreadsheet.worksheet(data['department'])
        
        # Рассчитываем next_row безопасным способом
        try:
            next_row = len(department_sheet.col_values(1)) + 1
        except APIError as e:
            logging.error(f"Ошибка Google Sheets: {str(e)}")
            next_row = 1  # Начнем с первой строки если не удалось получить данные

        # Формируем обновления
        updates = [
            {'range': f'A{next_row}', 'values': [[data['shop']]]},
            {'range': f'B{next_row}', 'values': [[int(data['article'])]]},
            {'range': f'C{next_row}', 'values': [[data['order_reason']]]},
            {'range': f'D{next_row}', 'values': [[datetime.now().strftime("%d.%m.%Y %H:%M")]]},
            {'range': f'E{next_row}', 'values': [[f"{data['user_name']}, {data['user_position']}"]]},
            {'range': f'K{next_row}', 'values': [[int(data['quantity'])]]},
            {'range': f'R{next_row}', 'values': [[int(message.from_user.id)]]}
        ]

        # Записываем данные
        department_sheet.batch_update(updates)
        await message.answer("✅ Заказ успешно сохранен!", reply_markup=main_menu_keyboard())
        await state.clear()

    except Exception as e:
        await log_error(message.from_user.id, f"Save Error: {str(e)}")
        await message.answer(f"⚠️ Ошибка сохранения: {str(e)}")

@dp.message(OrderStates.confirmation, F.text == "✏️ Исправить количество")
async def correct_quantity(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat())
    await message.answer("🔢 Введите новое количество:", reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(OrderStates.quantity_input)


@dp.message(OrderStates.confirmation, F.text == "❌ Отмена")
async def cancel_order(message: types.Message, state: FSMContext):
    # Очищаем состояние
    await state.clear()
    # Показываем главное меню
    await message.answer("❌ Операция отменена.", reply_markup=main_menu_keyboard())


@dp.message(OrderStates.article_input, F.text == "❌ Отмена")
async def cancel_order_process(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Создание заказа отменено.", reply_markup=main_menu_keyboard())
@dp.message(OrderStates.quantity_input, F.text == "❌ Отмена")
async def cancel_order_process(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Создание заказа отменено.", reply_markup=main_menu_keyboard())
@dp.message(OrderStates.order_reason_input, F.text == "❌ Отмена")
async def cancel_order_process(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Создание заказа отменено.", reply_markup=main_menu_keyboard())


@dp.message(F.text == "📋 Запрос информации")
async def handle_info_request(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat())
    user_data = await get_user_data(str(message.from_user.id))
    if not user_data:
        await message.answer("❌ Сначала пройдите регистрацию через /start")
        return
        
    await state.update_data(shop=user_data['shop'])
    await message.answer("🔢 Введите артикул товара:", reply_markup=article_input_keyboard())
    await state.set_state(InfoRequest.article_input)


@dp.message(InfoRequest.article_input)
async def process_info_request(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat())
    article = message.text.strip()
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
        f"📦Артикул: {product_info['article']}\n"
        f"🏷️Название: {product_info['product_name']}\n"
        f"🔢Отдел: {product_info['department']}\n"
        f"📅Ближайшая дата заказа: {product_info['order_date']}\n"
        f"🚚Ожидаемая дата поставки: {product_info['delivery_date']}\n"
        f"🏭 Поставщик: {product_info['supplier_name']}" 
    )
    
    await message.answer(response, reply_markup=main_menu_keyboard())
    await state.clear()


@dp.message(F.text == "📦 Проверка стока")
async def handle_stock_check(message: types.Message):
    await message.answer("🛠️ Функция в разработке")


@dp.message(F.text == "/reload_cache")
async def reload_cache_command(message: types.Message):  # Изменено имя функции
    try:
        # Принудительная очистка кэша
        cache.clear()
        
        # Загрузка основных данных
        await cache_sheet_data(users_sheet, "users")
        await cache_sheet_data(gamma_cluster_sheet, "gamma_cluster")
        
        # Дополнительная проверка данных
        gamma_data = cache.get("gamma_cluster", [])
        test_article = gamma_data[0].get("Артикул") if gamma_data else None
        response = (
            f"✅ Кэш перезагружен\n"
            f"• Пользователей: {len(cache['users'])}\n"
            f"• Товаров: {len(gamma_data)}\n"
            f"• Тестовый артикул: {test_article or 'Нет данных'}"
        )
        
        await message.answer(response)
    except Exception as e:
        error_msg = f"Ошибка перезагрузки кэша: {str(e)}"
        print(error_msg)
        await message.answer(error_msg)



@dp.message(F.text == "/debug_article")
async def debug_article(message: types.Message):
    try:
        gamma_data = cache.get("gamma_cluster", [])
        if not gamma_data:
            await message.answer("❌ Кэш товаров пуст")
            return
            
        sample_item = gamma_data[0]
        debug_info = (
            f"🔍 Пример элемента из кэша:\n"
            f"Тип артикула: {type(sample_item.get('Артикул'))}\n"
            f"Значение: {sample_item.get('Артикул')}\n"
            f"Все ключи: {list(sample_item.keys())}"
        )
        await message.answer(debug_info)
    except Exception as e:
        await message.answer(f"⚠️ Ошибка: {str(e)}")



@dp.message(F.text == "/check_cache")
async def check_cache(message: types.Message):
    gamma_data = cache.get("gamma_cluster", [])
    response = (
        f"Кэш gamma_cluster: {len(gamma_data)} записей\n"
        f"Пример: {gamma_data[:1] if gamma_data else 'Нет данных'}"
    )
    await message.answer(response)

#===========================Рассылка==================================


@dp.message(Command("broadcast"))
async def start_broadcast(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat())
    if message.from_user.id not in ADMINS:
        return
    
    await message.answer(
        "📢 Введите сообщение для рассылки (можно с медиа-вложениями):",
        reply_markup=types.ReplyKeyboardRemove()
    )
    await state.set_state(AdminBroadcast.message_input)

@dp.message(AdminBroadcast.message_input)
async def process_broadcast_message(message: types.Message, state: FSMContext):
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
    
    preview_text = "✉️ Предпросмотр сообщения:\n\n"
    if content['type'] == 'text':
        preview_text += content['text']
    else:
        preview_text += f"[{content['type'].upper()}] {content.get('caption', '')}"

    await message.answer(
        preview_text,
        reply_markup=broadcast_confirmation_keyboard()
    )
    await state.set_state(AdminBroadcast.confirmation)

@dp.message(AdminBroadcast.confirmation, F.text == "✅ Подтвердить рассылку")
async def confirm_broadcast(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat())
    data = await state.get_data()
    content = data['content']
    
    # Записываем в логи
    logs_sheet.append_row([
        datetime.now().strftime("%d.%m.%Y %H:%M"),
        message.from_user.id,
        "BROADCAST",
        f"Type: {content['type']}, Chars: {len(content.get('text', '') or content.get('caption', ''))}"
    ])
    
    await message.answer("🔄 Начинаю рассылку...", reply_markup=main_menu_keyboard())
    
    # Асинхронная рассылка
    asyncio.create_task(send_broadcast(content))
    
    await state.clear()

async def send_broadcast(content: dict):
    users = users_sheet.col_values(1)[1:]  # ID из колонки A
    success = 0
    failed = 0
    
    for user_id in users:
        try:
            if content['type'] == 'text':
                await bot.send_message(
                    chat_id=int(user_id),
                    text=content['text'],
                    parse_mode=ParseMode.HTML
                )
            elif content['type'] == 'photo':
                await bot.send_photo(
                    chat_id=int(user_id),
                    photo=content['media'],
                    caption=content.get('caption', ''),
                    parse_mode=ParseMode.HTML
                )
            elif content['type'] == 'document':
                await bot.send_document(
                    chat_id=int(user_id),
                    document=content['media'],
                    caption=content.get('caption', ''),
                    parse_mode=ParseMode.HTML
                )
            success += 1
            await asyncio.sleep(0.1)  # Защита от ограничений Telegram
        except Exception as e:
            failed += 1
            logging.error(f"Broadcast error to {user_id}: {str(e)}")
    
    # Отправляем отчет админу
    await bot.send_message(
        chat_id=ADMINS[0],
        text=f"📊 Результаты рассылки:\n\n✅ Успешно: {success}\n❌ Не удалось: {failed}"
    )

# Добавить в существующий код
@dp.message(AdminBroadcast.confirmation, F.text == "❌ Отменить рассылку")
async def cancel_broadcast(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Рассылка отменена", reply_markup=main_menu_keyboard())

@dp.message(AdminBroadcast.confirmation, F.text == "✏️ Редактировать сообщение")
async def edit_broadcast(message: types.Message, state: FSMContext):
    await message.answer("📝 Введите новое сообщение:", reply_markup=types.ReplyKeyboardRemove())  
    await state.set_state(AdminBroadcast.message_input)





#=============================УВЕДОМЛЕНИЯ=========================



# ===================== ИНИЦИАЛИЗАЦИЯ СТАТИСТИКИ =====================
async def initialize_stats_sheet():
    """Создание листа статистики при первом запуске"""
    try:
        spreadsheet = client.open(ORDERS_SPREADSHEET_NAME)
        spreadsheet.add_worksheet(title=STATS_SHEET_NAME, rows=1000, cols=4)
        stats_sheet = spreadsheet.worksheet(STATS_SHEET_NAME)
        stats_sheet.update('A1:D1', [['Дата', 'НомерЗаказа', 'ChatID', 'Статус']])
    except Exception as e:
        logging.info(f"Статистический лист уже существует: {str(e)}")



# ===================== КОНФИГУРАЦИЯ УВЕДОМЛЕНИЙ =====================
ORDERS_SHEET_NAMES = [str(i) for i in range(1, 16)]
CHECK_INTERVAL = 300  # 5 минут
STATS_SHEET_NAME = "Статистика Уведомлений"

COLUMNS = {
    'order_number': 3,   # B
    'order_date': 16,    # P
    'order_id': 17,      # Q
    'chat_id': 18,       # R 
    'notified': 19       # S
}



# ===================== ЗАПУСК ФОНОВЫХ ЗАДАЧ =====================
async def scheduled_notifications_checker():
    """Периодическая проверка уведомлений"""
    while True:
        await check_orders_notifications()
        await asyncio.sleep(CHECK_INTERVAL)



# ===================== ОСНОВНАЯ ЛОГИКА УВЕДОМЛЕНИЙ =====================
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def check_orders_notifications():
    """Проверка и отправка уведомлений с оптимизацией запросов"""
    try:
        spreadsheet = client.open(ORDERS_SPREADSHEET_NAME)
        stats_sheet = spreadsheet.worksheet(STATS_SHEET_NAME)
        
        for sheet_name in ORDERS_SHEET_NAMES:
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                # Используем формулу для фильтрации данных
                query = (
                    f"SELECT B, P, Q, R WHERE "
                    f"P != '' AND Q != '' AND R != '' AND S = ''"
                )
                records = worksheet.get_all_records(formula=query)
                
                for idx, record in enumerate(records, start=2):
                    await process_order_record(worksheet, stats_sheet, idx, record)
                    
            except SpreadsheetNotFound:
                continue

    except APIError as e:
        logging.error(f"Google API Error: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")


# ===================== КОНФИГУРАЦИЯ ТЕСТОВОГО РЕЖИМА =====================
TEST_MODE = True  # Переключить на False для реальных уведомлений

#===================== ОСНОВНАЯ ЛОГИКА УВЕДОМЛЕНИЙ =====================
async def process_order_record(worksheet, stats_sheet, row_num, record):
    """Обработка одной записи с валидацией"""
    try:
        chat_id = str(record['chat_id']).strip()
        
        # Валидация chat_id
        if not chat_id.isdigit():
            raise ValueError(f"Неверный Chat ID: {chat_id}")

        # Проверка тестового режима
        if TEST_MODE and chat_id not in map(str, ADMINS):
            logging.info(f"Тестовый режим: пропуск chat_id {chat_id}")
            return
        
        # Формирование сообщения
        message = (
            f"📦 Заказ №{record['order_number']}\n"
            f"🗓 Дата: {record['order_date']}\n"
            f"🔢 Номер заказа: {record['order_id']}"
        )
        
        # Добавляем пометку для тестового режима
        if TEST_MODE:
            message = "[ТЕСТ] " + message

        # Отправка сообщения
        try:
            await bot.send_message(chat_id=int(chat_id), text=message)
            status = "✅ Успешно"
        except TelegramForbiddenError:
            status = "❌ Пользователь заблокировал бота"
        except Exception as e:
            status = f"❌ Ошибка: {str(e)}"
            raise
        
        # Логирование статистики
        stats_record = [
            datetime.now().strftime("%d.%m.%Y %H:%M"),
            record['order_number'],
            chat_id,
            status
        ]
        stats_sheet.append_row(stats_record)
        
        # Обновление статуса в основном листе
        worksheet.update_cell(row_num, COLUMNS['notified'], status.split(':')[0])
        
    except Exception as e:
        logging.error(f"Ошибка обработки заказа {record['order_number']}: {str(e)}")
        stats_sheet.append_row([
            datetime.now().strftime("%d.%m.%Y %H:%M"),
            record.get('order_number', 'N/A'),
            chat_id,
            f"❌ Критическая ошибка: {str(e)}"
        ])

# ===================== ОБЩАЯ ЛОГИКА ЗАПУСКА =====================
async def scheduled_cache_update():
    while True:
        await asyncio.sleep(3600 * 12)  # Обновление каждые 12 часов
        try:
            await preload_cache()
        except Exception as e:
            logging.error(f"Ошибка обновления кэша: {str(e)}")

async def startup():
    asyncio.create_task(scheduled_cache_update())
    if TEST_MODE:
        await notify_admins("🔧 Бот запущен в ТЕСТОВОМ РЕЖИМЕ. Уведомления отправляются только администраторам.")
    
    await initialize_stats_sheet()
    asyncio.create_task(scheduled_notifications_checker())
    """Общая инициализация для всех режимов"""
    startup_msg = "🟢 Бот запущен"
    print(startup_msg)
    if TEST_MODE:
        await notify_admins("🔧 Бот запущен в ТЕСТОВОМ РЕЖИМЕ. Уведомления отправляются только администраторам.")
    try:
        print("♻️ Начало загрузки кэша...")
        await preload_cache()
        print(f"✅ Кэш загружен. Ключи: {list(cache.keys())[:5]}...")  # Логируем первые 5 ключей
        await notify_admins(startup_msg)
    except Exception as e:
        error_msg = f"🚨 Критическая ошибка запуска: {str(e)}"
        print(error_msg)
        await notify_admins(error_msg)
        raise

async def shutdown():
    """Завершение работы с гарантированным закрытием ресурсов"""
    try:
        # Закрытие сессий AIOHTTP
        await bot.session.close()
        await dp.storage.close()
        
        # Закрытие вебхука
        if USE_WEBHOOKS:
            await bot.delete_webhook()
            
    except Exception as e:
        print(f"Ошибка при завершении: {str(e)}")
        
    finally:
        # Принудительное завершение всех задач
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()



# ===================== ОБРАБОТЧИК ВЕБХУКОВ =====================
async def handle_webhook(request):
    """Обработчик вебхуков"""
    update_data = await request.json()
    update = types.Update(**update_data)
    await dp.feed_update(bot=bot, update=update)
    return web.Response(text="OK", status=200)

# ===================== ИНИЦИАЛИЗАЦИЯ ПРИЛОЖЕНИЯ =====================
app = web.Application()
if USE_WEBHOOKS:
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    app.on_startup.append(lambda _: startup())
    app.on_shutdown.append(lambda _: shutdown())

# ===================== УНИВЕРСАЛЬНЫЙ ЗАПУСК =====================
async def main():
    """Главная функция запуска"""
    await startup()
    
    if USE_WEBHOOKS:
        # Только в вебхук-режиме
        await bot.set_webhook(
            url=WEBHOOK_URL,
            drop_pending_updates=True
        )
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', WEBHOOK_PORT)
        await site.start()
        print(f"✅ Режим вебхуков: {WEBHOOK_URL}")
        while True:
            await asyncio.sleep(3600)
    else:
        # Режим поллинга
        print("✅ Режим поллинга")
        await dp.start_polling(bot, skip_updates=True)

# ===================== ЗАВЕРШЕНИЕ РАБОТЫ =====================
async def shutdown():
    try:
        if USE_WEBHOOKS:
            await bot.delete_webhook()
        await bot.session.close()
        await dp.storage.close()
    except Exception as e:
        logging.error(f"Shutdown error: {str(e)}")
    finally:
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
if __name__ == "__main__":
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\n🛑 Bot stopped by user")
    except Exception as e:
        logging.critical(f"Critical error: {str(e)}")
    finally:
        loop.run_until_complete(shutdown())
        loop.close()
