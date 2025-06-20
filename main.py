import os
import json
import pickle
import io
import re
import gc
from pyzbar.pyzbar import decode
from PIL import Image
from concurrent.futures import ThreadPoolExecutor
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
SERVICE_MODE = True
ADMINS = [122086799, 5183727015]  # ID администраторов

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
STATSS_SHEET_NAME = "Статистика_Пользователей"
ORDERS_SPREADSHEET_NAME = "Копия Заказы МЗ 0.2"
USERS_SHEET_NAME = "Пользователи"
GAMMA_CLUSTER_SHEET = "Гамма кластер"
LOGS_SHEET = "Логи"
BARCODES_SHEET_NAME = "Штрих-коды"  # Название листа со штрих-кодами
MAX_IMAGE_SIZE = 2_000_000  # 2MB максимальный размер изображения
MAX_WORKERS = 4  # Максимальное количество потоков для обработки изображений

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

image_processor = ThreadPoolExecutor(max_workers=MAX_WORKERS)


# ===================== СОСТОЯНИЯ FSM =====================
class Registration(StatesGroup):
    name = State()
    surname = State()
    position = State()
    shop = State()


class OrderStates(StatesGroup):
    article_input = State()
    shop_selection = State()  # Новое состояние для выбора магазина
    barcode_scan = State()
    shop_input = State() 
    quantity_input = State()
    confirmation = State()
    order_reason_input = State()


class InfoRequest(StatesGroup):
    article_input = State()


class AdminBroadcast(StatesGroup):
    message_input = State()
    target_selection = State()
    manual_ids = State()  # Новое состояние для ввода списка ID
    confirmation = State()


class AdminStates(StatesGroup):
    panel = State()
    broadcast_message = State()
    broadcast_target = State()
    manual_ids_input = State()
    service_mode_manage = State()
    cache_manage = State()
    logs_view = State()


# ===================== КЛАВИАТУРЫ =====================
def create_keyboard(buttons: List[str], sizes: tuple, resize=True, one_time=False):
    """Универсальный конструктор клавиатур"""
    builder = ReplyKeyboardBuilder()
    for button in buttons:
        builder.button(text=button)
    builder.adjust(*sizes)
    return builder.as_markup(
        resize_keyboard=resize,
        one_time_keyboard=one_time
    )

def main_menu_keyboard(user_id: int = None):
    """Главное меню с учетом прав пользователя"""
    buttons = [
        "📋 Запрос информации",
        "📦 Проверка стока",
        "🛒 Заказ под клиента"
    ]
    
    # Добавляем админ-панель для администраторов
    if user_id and user_id in ADMINS:
        buttons.append("🛠 Админ-панель")
    
    return create_keyboard(buttons, (2, 1, 1))

def article_input_keyboard():
    return create_keyboard(
        ["🔍 Сканировать штрих-код", "⌨️ Ввести вручную", "❌ Отмена", "↩️ Назад"],
        (2, 2)
    )

def shop_selection_keyboard():
    return create_keyboard(
        ["Использовать мой магазин", "Выбрать другой", "❌ Отмена"],
        (2, 1)
    )

def confirm_keyboard():
    return create_keyboard(
        ["✅ Подтвердить", "✏️ Исправить количество", "❌ Отмена"],
        (2, 1)
    )

def admin_panel_keyboard():
    return create_keyboard(
        [
            "📊 Статистика", "📢 Рассылка", 
            "🔄 Обновить кэш", "📝 Просмотр логов",
            "🔧 Сервисный режим", "🔙 Главное меню"
        ],
        (2, 2, 2)
    )

def broadcast_target_keyboard():
    return create_keyboard(
        ["Всем пользователям", "По магазинам", "По отделам", "Вручную", "❌ Отмена"],
        (2, 2, 1)
    )

def service_mode_keyboard():
    return create_keyboard(
        ["🟢 Включить сервисный режим", "🔴 Выключить сервисный режим", "🔙 Назад"],
        (2, 1)
    )

def back_to_admin_keyboard():
    return create_keyboard(["🔙 В админ-панель"], (1,))

def cancel_keyboard():
    return create_keyboard(["❌ Отмена"], (1,))
    

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


# ===================== ОБРАБОТЧИКИ АДМИН-ПАНЕЛИ =====================

# ===================== ОБРАБОТЧИКИ АДМИН-ПАНЕЛИ =====================
@dp.message(F.text == "🛠 Админ-панель")
async def handle_admin_panel(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMINS:
        await message.answer("⛔ У вас нет прав доступа к админ-панели")
        return
    
    try:
        await message.answer(
            "🛠 Панель администратора:",
            reply_markup=admin_panel_keyboard()
        )
        await state.set_state(AdminStates.panel)
    except Exception as e:
        logging.error(f"Ошибка открытия админ-панели: {str(e)}")
        await message.answer("⚠️ Ошибка открытия админ-панели")

@dp.message(AdminStates.panel, F.text == "📊 Статистика")
async def handle_admin_stats(message: types.Message):
    try:
        # Статистика пользователей
        users_data = pickle.loads(cache.get("users_data", b"[]"))
        users_count = len(users_data) if users_data else 0
        
        # Статистика использования
        stats_data = pickle.loads(cache.get("stats_data", b"[]"))
        orders_count = sum(1 for r in stats_data if len(r) > 8 and r[8] == 'order')
        
        # Статистика сервера
        import psutil
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
        await message.answer(response, reply_markup=back_to_admin_keyboard())
        
    except Exception as e:
        logging.error(f"Ошибка получения статистики: {str(e)}")
        await message.answer(f"❌ Ошибка: {str(e)}", reply_markup=admin_panel_keyboard())

@dp.message(AdminStates.panel, F.text == "📢 Рассылка")
async def handle_broadcast_start(message: types.Message, state: FSMContext):
    await message.answer(
        "✉️ Введите сообщение для рассылки:",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(AdminStates.broadcast_message)

@dp.message(AdminStates.broadcast_message)
async def handle_broadcast_message(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.set_state(AdminStates.panel)
        await message.answer("❌ Рассылка отменена", reply_markup=admin_panel_keyboard())
        return
        
    await state.update_data(broadcast_message=message.html_text)
    await message.answer(
        "🎯 Выберите аудиторию для рассылки:",
        reply_markup=broadcast_target_keyboard()
    )
    await state.set_state(AdminStates.broadcast_target)

@dp.message(AdminStates.broadcast_target)
async def handle_broadcast_target(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.set_state(AdminStates.panel)
        await message.answer("❌ Рассылка отменена", reply_markup=admin_panel_keyboard())
        return
        
    data = await state.get_data()
    message_text = data['broadcast_message']
    
    # Рассылка всем пользователям
    if message.text == "Всем пользователям":
        users_data = pickle.loads(cache.get("users_data", b"[]"))
        user_ids = [str(user.get("ID пользователя", "")) for user in users_data if user.get("ID пользователя")]
        success, failed = await send_broadcast(message_text, user_ids)
        await message.answer(
            f"✅ Рассылка завершена:\nУспешно: {success}\nНе удалось: {failed}",
            reply_markup=admin_panel_keyboard()
        )
        await state.set_state(AdminStates.panel)
    
    # Рассылка по магазинам (пример)
    elif message.text == "По магазинам":
        await message.answer(
            "🏪 Введите номера магазинов через запятую:",
            reply_markup=cancel_keyboard()
        )
        await state.set_state(AdminStates.manual_ids_input)
    
    # Другие типы рассылки
    else:
        await message.answer(
            "⏳ Эта функция в разработке",
            reply_markup=admin_panel_keyboard()
        )
        await state.set_state(AdminStates.panel)

@dp.message(AdminStates.manual_ids_input)
async def handle_manual_ids_input(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.set_state(AdminStates.panel)
        await message.answer("❌ Рассылка отменена", reply_markup=admin_panel_keyboard())
        return
        
    # Здесь можно реализовать логику фильтрации пользователей
    # Пока просто отправляем сообщение
    await message.answer(
        "✅ Параметры рассылки получены. Начинаю рассылку...",
        reply_markup=admin_panel_keyboard()
    )
    
    # Фиктивная рассылка для примера
    await message.answer(
        "⏳ Рассылка завершена (режим демонстрации)",
        reply_markup=admin_panel_keyboard()
    )
    await state.set_state(AdminStates.panel)

@dp.message(AdminStates.panel, F.text == "🔄 Обновить кэш")
async def handle_cache_refresh(message: types.Message):
    try:
        await preload_cache()
        await message.answer("✅ Кэш успешно обновлен!", reply_markup=admin_panel_keyboard())
    except Exception as e:
        await message.answer(f"❌ Ошибка обновления кэша: {str(e)}", reply_markup=admin_panel_keyboard())

@dp.message(AdminStates.panel, F.text == "📝 Просмотр логов")
async def handle_logs_view(message: types.Message):
    try:
        # Получаем последние 20 записей логов
        stats_data = pickle.loads(cache.get("stats_data", b"[]"))
        if len(stats_data) > 20:
            logs = stats_data[-20:]
        else:
            logs = stats_data
        
        log_text = "📝 Последние действия в системе:\n\n"
        for log in logs:
            # Проверяем, что лог содержит достаточно элементов
            if len(log) >= 9:
                log_text += f"{log[0]} {log[1]} - {log[2]}: {log[7]} ({log[8]})\n"
            else:
                log_text += f"Неполная запись: {log}\n"
        
        await message.answer(log_text[:4000], reply_markup=admin_panel_keyboard())
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}", reply_markup=admin_panel_keyboard())

@dp.message(AdminStates.panel, F.text == "🔧 Сервисный режим")
async def handle_service_mode(message: types.Message, state: FSMContext):
    status = "🟢 ВКЛЮЧЕН" if SERVICE_MODE else "🔴 ВЫКЛЮЧЕН"
    await message.answer(
        f"🛠 Текущий статус сервисного режима: {status}\nВыберите действие:",
        reply_markup=service_mode_keyboard()
    )
    await state.set_state(AdminStates.service_mode_manage)

@dp.message(AdminStates.service_mode_manage, F.text == "🟢 Включить сервисный режим")
async def enable_service_mode(message: types.Message):
    global SERVICE_MODE
    SERVICE_MODE = True
    await broadcast("🔧 Бот переходит в сервисный режим. Некоторые функции временно недоступны.")
    await message.answer("✅ Сервисный режим включен", reply_markup=admin_panel_keyboard())
    await state.set_state(AdminStates.panel)

@dp.message(AdminStates.service_mode_manage, F.text == "🔴 Выключить сервисный режим")
async def disable_service_mode(message: types.Message):
    global SERVICE_MODE
    SERVICE_MODE = False
    await broadcast("✅ Сервисный режим отключен. Все функции доступны.")
    await message.answer("✅ Сервисный режим выключен", reply_markup=admin_panel_keyboard())
    await state.set_state(AdminStates.panel)

@dp.message(AdminStates.service_mode_manage, F.text == "🔙 Назад")
async def back_from_service_mode(message: types.Message):
    await message.answer("🔙 Возврат в админ-панель", reply_markup=admin_panel_keyboard())
    await state.set_state(AdminStates.panel)





# ===================== СИСТЕМА КЭШИРОВАНИЯ =====================
async def get_cached_data(cache_key: str) -> List[Dict]:
    print(f"[DEBUG] Получение данных из кэша: {cache_key}")
    if cache_key in cache:
        data = pickle.loads(cache[cache_key])
        print(f"[DEBUG] Десериализовано: {len(data)} записей")
        return data
    return []


def get_supplier_dates_sheet(shop_number: str) -> list:
    """Получение данных поставщика с простым кэшированием"""
    cache_key = f"supplier_{shop_number}"
    if cache_key in cache:
        return pickle.loads(cache[cache_key])
    
    try:
        sheet = orders_spreadsheet.worksheet(f"Даты выходов заказов {shop_number}")
        data = sheet.get_all_records()
        cache[cache_key] = pickle.dumps(data)
        return data
    except Exception as e:
        print(f"[ERROR] Ошибка получения данных поставщика: {str(e)}")
        return []


async def preload_cache(_=None):
    """Предзагрузка только необходимых данных"""
    try:
        stats_sheet = main_spreadsheet.worksheet(STATSS_SHEET_NAME)
        cache["stats_data"] = pickle.dumps(stats_sheet.get_all_records())
    except:
        pass
    try:
        # Кэшируем пользователей как список словарей
        users_records = users_sheet.get_all_records()
        cache["users_data"] = pickle.dumps(users_records)
        print(f"👥 Кэш пользователей загружен: {len(users_records)} записей")

        # Кэшируем товары в оптимизированном формате
        gamma_records = gamma_cluster_sheet.get_all_records()
        gamma_index = {}
        
        for item in gamma_records:
            article = str(item.get("Артикул", "")).strip()
            shop = str(item.get("Магазин", "")).strip()
            if article and shop:
                # Используем кортеж как ключ для быстрого поиска
                key = (article, shop)
                gamma_index[key] = {
                    "Название": item.get("Название", ""),
                    "Отдел": item.get("Отдел", ""),
                    "Номер осн. пост.": item.get("Номер осн. пост.", "")
                }
        
        cache["gamma_index"] = pickle.dumps(gamma_index)
        print(f"📦 Кэш товаров загружен: {len(gamma_index)} записей")
        
        # ЗАГРУЖАЕМ ТАБЛИЦУ ШТРИХ-КОДОВ
        barcodes_sheet = main_spreadsheet.worksheet(BARCODES_SHEET_NAME)
        barcodes_data = barcodes_sheet.get_all_records()
        barcodes_index = {}
        
        for record in barcodes_data:
            barcode = str(record.get("Штрих-код", "")).strip()
            article = str(record.get("Артикул", "")).strip()
            if barcode and article:
                barcodes_index[barcode] = article
        
        cache["barcodes_index"] = pickle.dumps(barcodes_index)
        print(f"📊 Кэш штрих-кодов загружен: {len(barcodes_index)} записей")
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
        success = len([r for r in records if '✅' in r['Статус']])
        failed = total - success
        
        response = (
            f"📊 Статистика уведомлений:\n\n"
            f"• Всего отправок: {total}\n"
            f"• Успешных: {success}\n"
            f"• Неудачных: {failed}\n"
            f"• Последние 5 ошибок:\n"
        )
        
        for r in records[-5:]:
            if '❌' in r['Статус']:
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
    if SERVICE_MODE and (event.message or event.callback_query):
        user_id = event.message.from_user.id if event.message else event.callback_query.from_user.id
        if user_id not in ADMINS:
            if event.message:
                await event.message.answer("⏳ Бот в режиме обслуживания. Попробуйте позже.")
            elif event.callback_query:
                await event.callback_query.answer("⏳ Бот в режиме обслуживания. Попробуйте позже.", show_alert=True)
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
            state_data = await state.get_data()
            last_activity_str = state_data.get('last_activity')

            try:
                last_activity = datetime.fromisoformat(last_activity_str)
            except (ValueError, TypeError):
                last_activity = datetime.min
                # Исправление здесь:
                user_id = "unknown"
                if event.message:
                    user_id = event.message.from_user.id
                elif event.callback_query:
                    user_id = event.callback_query.from_user.id
                logging.warning(f"Invalid last_activity format for user {user_id}")

            if datetime.now() - last_activity > timedelta(minutes=20):
                await state.clear()
                # Исправление здесь:
                if event.message:
                    await event.message.answer("🕒 Сессия истекла. Начните заново.")
                elif event.callback_query:
                    await event.callback_query.message.answer("🕒 Сессия истекла. Начните заново.")
                return

            await state.update_data(last_activity=datetime.now().isoformat())

    return await handler(event, data)
    await message.answer(reply_markup=main_menu_keyboard())

# ===================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====================
async def get_user_data(user_id: str) -> Dict[str, Any]:
    """Получение данных пользователя с простым кэшированием"""
    cache_key = f"user_{user_id}"
    if cache_key in cache:
        return cache[cache_key]
    
    try:
        # Получаем кэшированные данные всех пользователей
        users_data = pickle.loads(cache.get("users_data", b""))
        if not users_data:
            # Если кэш пуст, загружаем данные
            users_data = users_sheet.get_all_records()
            cache["users_data"] = pickle.dumps(users_data)
            print(f"👥 Загружено пользователей: {len(users_data)}")
        
        # Ищем пользователя в кэшированных данных
        for user in users_data:
            
            if str(user.get("ID пользователя", "")).strip() == str(user_id).strip():
                # Кэшируем найденного пользователя
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
        print(f"Ошибка в get_user_data: {str(e)}")
        return None


async def log_error(user_id: str, error: str):
    logs_sheet.append_row([
        datetime.now().strftime("%d.%m.%Y %H:%M"),
        user_id,
        "ERROR",
        error
    ])




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


async def process_barcode_image(photo: types.PhotoSize) -> (str, str):
    """Обработка изображения со штрих-кодом с улучшенной обработкой ошибок"""
    try:
        # Проверяем кэш штрих-кодов
        if "barcodes_index" not in cache or not cache["barcodes_index"]:
            return None, "Система штрих-кодов не настроена"
        
        # Скачиваем фото
        file = await bot.get_file(photo.file_id)
        image_bytes = await bot.download(file.file_path)
        
        logging.info(f"Processing image: {len(image_bytes)} bytes")
        
        # Проверка размера
        if len(image_bytes) > MAX_IMAGE_SIZE:
            return None, f"Изображение слишком большое (> {MAX_IMAGE_SIZE//1024}KB)"
        
        # Обработка в отдельном потоке
        loop = asyncio.get_running_loop()
        
        # Используем контекстный менеджер для работы с памятью
        with io.BytesIO(image_bytes) as buffer:
            image = await loop.run_in_executor(
                image_processor, 
                Image.open, 
                buffer
            )
            
            # Оптимизация памяти
            image = image.convert('L')  # Градации серого
            image.thumbnail((800, 800))
            
            # Обработка с таймаутом
            try:
                decoded_objects = await asyncio.wait_for(
                    loop.run_in_executor(image_processor, decode, image),
                    timeout=10.0
                )
            except asyncio.TimeoutError:
                return None, "Превышено время обработки изображения"
            
            # Явно освобождаем ресурсы
            del image
        
        if not decoded_objects:
            return None, "Штрих-код не распознан"
        
        barcode_data = decoded_objects[0].data.decode("utf-8").strip()
        
        # Получаем артикул из кэша
        barcodes_index = pickle.loads(cache["barcodes_index"])
        article = barcodes_index.get(barcode_data)
        
        if not article:
            return None, f"Штрих-код {barcode_data} не найден в базе"
        
        return article, None
        
    except Exception as e:
        logging.exception(f"Barcode processing error: {str(e)}")
        return None, "Ошибка обработки изображения"
    finally:
        # Принудительный сбор мусора после обработки изображения
        gc.collect()

@dp.message(Command("test_barcode"))
async def test_barcode(message: types.Message):
    """Тестовая команда для проверки штрих-кодов"""
    if message.from_user.id not in ADMINS:
        return
        
    try:
        barcodes_index = pickle.loads(cache.get("barcodes_index", b""))
        if not barcodes_index:
            await message.answer("❌ Кэш штрих-кодов пуст")
            return
            
        test_barcode = next(iter(barcodes_index.keys()))
        article = barcodes_index[test_barcode]
        await message.answer(f"Тестовый штрих-код: {test_barcode}\nСоответствует артикулу: {article}")
    except Exception as e:
        await message.answer(f"❌ Ошибка тестирования: {str(e)}")



# ========================== ПАРСЕР ===========================
async def get_product_info(article: str, shop: str) -> dict:
    try:
        gamma_index = pickle.loads(cache.get("gamma_index", b""))
        key = (str(article).strip(), str(shop).strip())
        product_data = gamma_index.get(key)
        
        if not product_data:
            print(f"[INFO] Товар не найден: артикул {article}, магазин {shop}")
            return None
        
        print(f"[INFO] Найдены данные о товаре для артикула: {article}, магазин: {shop}")
        
        supplier_id = str(product_data.get("Номер осн. пост.", "")).strip()
        
        # Получаем данные поставщика - это список словарей
        supplier_list = get_supplier_dates_sheet(shop)
        
        # Ищем поставщика в списке
        supplier_data = next(
            (item for item in supplier_list 
             if str(item.get("Номер осн. пост.", "")).strip() == supplier_id),
            None
        )
        
        if not supplier_data:
            print(f"[INFO] Поставщик {supplier_id} не найден для магазина {shop}")
            return {
                'Артикул': article,
                'Название': product_data.get('Название', ''),
                'Отдел': str(product_data.get('Отдел', '')),
                'Магазин': shop,
                'Поставщик': 'Товар РЦ'
            }
        
        # Получаем название поставщика напрямую из словаря
        supplier_name = supplier_data.get("Название осн. пост.", "Не указано").strip()
        
        # Парсим данные поставщика
        parsed_supplier = parse_supplier_data(supplier_data)
        order_date, delivery_date = calculate_delivery_date(parsed_supplier)
        
        print(f"[INFO] Успешно получена информация для артикула: {article}, магазин: {shop}")
        
        return {
            'Артикул': article,
            'Название': product_data.get('Название', ''),
            'Отдел': str(product_data.get('Отдел', '')),
            'Магазин': shop,
            'Поставщик': supplier_name,
            'Дата заказа': order_date,
            'Дата поставки': delivery_date,
            'Номер поставщика': supplier_id,
            'Парсинг поставщика': parsed_supplier
        }
    
    except Exception as e:
        print(f"[ERROR] Ошибка в get_product_info: {str(e)}")
        logging.error(f"Product info error: {str(e)}")
        return None


# ===================== ОБРАБОТЧИКИ КОМАНД =====================
@dp.message(Command("start"))
async def start_handler(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat())
    user_data = await get_user_data(str(message.from_user.id))
    if user_data:
        await message.answer("ℹ️ Вы в главном меню:", reply_markup=main_menu_keyboard(message.from_user.id))
        return
    await message.answer(
        "👋 Добро пожаловать! Введите ваше имя:", reply_markup=types.ReplyKeyboardRemove()
    )
    await log_user_activity(message.from_user.id, "/start", "registration")
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


@dp.message(F.text == "↩️ Назад")
async def handle_back(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "🔙 Возврат в главное меню",
        reply_markup=main_menu_keyboard(message.from_user.id)
    )


@dp.message(F.text == "🔙 Главное меню")
async def handle_back_to_main(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "🔙 Возврат в главное меню",
        reply_markup=main_menu_keyboard(message.from_user.id)
    )


@dp.message(F.text == "🔙 В админ-панель")
async def handle_back_to_admin(message: types.Message, state: FSMContext):
    await handle_admin_panel(message, state)


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
    
    await message.answer(
        "📦 Выберите способ ввода артикула:",
        reply_markup=article_input_keyboard()
    )
    await log_user_activity(message.from_user.id, "Заказ под клиента", "order")
    await state.set_state(OrderStates.article_input)  # Установка состояния


@dp.message(OrderStates.barcode_scan, F.text == "❌ Отмена")
async def cancel_scan(message: types.Message, state: FSMContext):
    await message.answer(
        "❌ Сканирование отменено",
        reply_markup=article_input_keyboard()
    )
    await state.set_state(OrderStates.article_input)


@dp.message(OrderStates.article_input, F.text == "🔍 Сканировать штрих-код(пока не работает)")
async def handle_scan_choice(message: types.Message, state: FSMContext):
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
    await message.answer(
        "🔢 Введите артикул товара вручную:",
        reply_markup=article_input_keyboard()
    )


@dp.message(OrderStates.article_input)
async def process_article_manual(message: types.Message, state: FSMContext):
    # Пропускаем системные команды
    if message.text in ["🔍 Сканировать штрих-код", "⌨️ Ввести вручную", "❌ Отмена", "↩️ Назад"]:
        return
        
    # Обработка отмены
    if message.text.lower() in ["отмена", "❌ отмена"]:
        await state.clear()
        await message.answer("🔄 Операция отменена", reply_markup=main_menu_keyboard())
        return
        
    # Обработка возврата
    if message.text == "↩️ Назад":
        await state.clear()
        await message.answer("🔙 Возврат в главное меню", reply_markup=main_menu_keyboard())
        return
        
    # Проверка наличия текста
    if not message.text:
        await message.answer("❌ Пожалуйста, введите артикул.")
        return
        
    article = message.text.strip()
    
    # Проверка формата артикула
    if not re.match(r'^\d{4,10}$', article):
        await message.answer("❌ Неверный формат артикула. Артикул должен состоять из 4-10 цифр. Попробуйте еще раз:")
        return
        
    await state.update_data(article=article)
    
    await message.answer(
        "📌 Выберите магазин для заказа:",
        reply_markup=shop_selection_keyboard()
    )
    await state.set_state(OrderStates.shop_selection)



@dp.message(OrderStates.barcode_scan, F.photo)
async def handle_barcode_scan(message: types.Message, state: FSMContext):
    try:
        # Показываем индикатор загрузки
        processing_msg = await message.answer("🔍 Обработка изображения...")
        
        # Используем самое качественное изображение
        photo = message.photo[-1]
        article, error = await process_barcode_image(photo)
        
        # Удаляем сообщение о обработке
        await processing_msg.delete()
        
        if error:
            await message.answer(
                f"❌ {error}\n\nПопробуйте еще раз или введите артикул вручную.",
                reply_markup=article_input_keyboard()
            )
            await state.set_state(OrderStates.article_input)
            return
            
        await state.update_data(article=article)
        await message.answer(
            f"✅ Штрих-код распознан!\nАртикул: {article}",
            reply_markup=ReplyKeyboardRemove()
        )
        
        # Переходим к выбору магазина
        await message.answer(
            "📌 Выберите магазин для заказа:",
            reply_markup=shop_selection_keyboard()
        )
        await state.set_state(OrderStates.shop_selection)
        
    except Exception as e:
        logging.exception("Barcode scan error")
        await message.answer(
            "⚠️ Произошла ошибка при обработке изображения. Попробуйте еще раз или введите артикул вручную.",
            reply_markup=article_input_keyboard()
        )
        await state.set_state(OrderStates.article_input)


@dp.message(OrderStates.shop_input)
async def process_custom_shop(message: types.Message, state: FSMContext):
    shop = message.text.strip()
    if not shop.isdigit() or shop.startswith('0'):
        await message.answer("❗ Номер магазина должен быть целым числом без ведущих нулей. Повторите ввод:")
        return
    await state.update_data(selected_shop=shop)
    await message.answer("✅ Магазин выбран\n 🔄 Загружаю", reply_markup=ReplyKeyboardRemove())
    await process_article_continuation(message, state)

@dp.message(OrderStates.shop_selection)
async def process_shop_selection(message: types.Message, state: FSMContext):
    user_data = await get_user_data(str(message.from_user.id))
    
    if message.text == "Использовать мой магазин":
        selected_shop = user_data['shop']
    elif message.text == "Выбрать другой":
        await message.answer(
            "🏪 Введите номер магазина (только цифры, без ведущих нулей):",
            reply_markup=ReplyKeyboardRemove()
        )
        await state.set_state(OrderStates.shop_input)
        return
    elif message.text == "❌ Отмена":
        await message.answer(
            "❌ Выбор отменен",
            reply_markup=main_menu_keyboard()
        )
        await state.clear()
        return
    else:
        await message.answer(
            "❌ Неверный выбор. Используйте кнопки меню",
            reply_markup=shop_selection_keyboard()
        )
        return
    
    # Сохраняем выбранный магазин в FSM
    await state.update_data(selected_shop=selected_shop)
    
    # Продолжаем процесс оформления заказа
    await message.answer("🔄 Загружаю", reply_markup=ReplyKeyboardRemove())
    await process_article_continuation(message, state)
    


async def process_article_continuation(message: types.Message, state: FSMContext):
    data = await state.get_data()
    article = data.get('article')
    selected_shop = data.get('selected_shop')
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
        article=product_info['Артикул'],
        product_name=product_info['Название'],
        department=product_info['Отдел'],
        order_date=product_info['Дата заказа'],
        delivery_date=product_info['Дата поставки'],
        supplier_id=product_info['Номер поставщика'],
        supplier_name=product_info['Поставщик']
    )
    await message.answer(response)
    await message.answer("🔢 Введите количество товара:", reply_markup=cancel_keyboard())
    await state.set_state(OrderStates.quantity_input)

def parse_supplier_data(record: dict):
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


# Модифицируем обработчики
@dp.message(OrderStates.quantity_input)
async def process_quantity(message: types.Message, state: FSMContext):
    # Проверяем команду отмены
    if message.text.strip().lower() in ["отмена", "❌ отмена"]:
        await state.clear()
        await message.answer("🔄 Операция отменена", reply_markup=main_menu_keyboard())
        return
        
    await state.update_data(last_activity=datetime.now().isoformat())
    if not message.text.strip().isdigit():
        await message.answer("❌ Введите число!", reply_markup=cancel_keyboard())
        return
        
    data = await state.get_data()
    await state.update_data(quantity=int(message.text.strip()))
    # Запрос номера заказа или причины с клавиатурой отмены
    await message.answer("Введите номер заказа или причину:", reply_markup=cancel_keyboard())
    await state.set_state(OrderStates.order_reason_input)


@dp.message(OrderStates.order_reason_input)
async def process_order_reason(message: types.Message, state: FSMContext):
    # Проверяем команду отмены
    if message.text.strip().lower() in ["отмена", "❌ отмена"]:
        await state.clear()
        await message.answer("🔄 Операция отменена", reply_markup=main_menu_keyboard())
        return
        
    await state.update_data(last_activity=datetime.now().isoformat())
    data = await state.get_data()
    order_reason = message.text.strip()
    selected_shop = data.get('selected_shop')
    # Обновляем состояние
    await state.update_data(order_reason=order_reason)
    # Вывод информации для подтверждения
    await message.answer(
        "🔎 Проверьте данные заказа:\n"
        f"Магазин: {selected_shop}\n"
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
    selected_shop = data['selected_shop']
    try:
        # Проверка обязательных полей
        required_fields = ['selected_shop', 'article', 'order_reason', 'quantity', 'department']
        for field in required_fields:
            if data.get(field) is None:
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
            {'range': f'A{next_row}', 'values': [[selected_shop]]},
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
        await log_user_activity(message.from_user.id, "Подтвердить","Confirmation")
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

@dp.message(OrderStates.shop_selection, F.text == "❌ Отмена")
@dp.message(OrderStates.shop_input, F.text == "❌ Отмена")
async def cancel_shop_selection(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Выбор магазина отменён", reply_markup=main_menu_keyboard())

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
    await log_user_activity(message.from_user.id, "Запрос информации", "info")
    user_data = await get_user_data(str(message.from_user.id))
    if not user_data:
        await message.answer("❌ Сначала пройдите регистрацию через /start")
        return
        
    await state.update_data(shop=user_data['shop'])
    await message.answer("🔢 Введите артикул товара:", reply_markup=article_input_keyboard())
    await state.set_state(InfoRequest.article_input)



@dp.message(InfoRequest.article_input, F.photo)
async def handle_barcode_info(message: types.Message, state: FSMContext):
    """Обработка фото штрих-кода для запроса информации"""
    await state.update_data(last_activity=datetime.now().isoformat())
    
    # Используем самое качественное изображение
    photo = message.photo[-1]
    article, error = await process_barcode_image(photo)
    
    if error:
        await message.answer(error)
        return
    
    data = await state.get_data()
    user_shop = data['shop']
    
    # Получаем информацию о товаре
    product_info = await get_product_info(article, user_shop)
    if not product_info:
        await message.answer("❌ Товар не найден")
        await state.clear()
        return

    response = (
        f"🔍 Информация о товаре:\n"
        f"Магазин: {user_shop}\n"
        f"📦Артикул: {product_info['Артикул']}\n"
        f"🏷️Название: {product_info['Название']}\n"
        f"🔢Отдел: {product_info['Отдел']}\n"
        f"📅Ближайшая дата заказа: {product_info['Дата заказа']}\n"
        f"🚚Ожидаемая дата поставки: {product_info['Дата поставки']}\n"
        f"🏭 Поставщик: {product_info['Поставщик']}" 
    )
    
    await message.answer(response, reply_markup=main_menu_keyboard())
    await state.clear()


@dp.message(InfoRequest.article_input)
async def process_info_request_text(message: types.Message, state: FSMContext):
    """Обработка текстового запроса информации"""
    # Если это фото - пропускаем (уже есть отдельный обработчик)
    if message.photo:
        return
        
    article = message.text.strip()
    
    # Проверка формата артикула
    if not re.match(r'^\d{4,10}$', article):
        await message.answer("❌ Неверный формат артикула. Артикул должен состоять из 4-10 цифр. Попробуйте еще раз:")
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
        f"📦Артикул: {product_info['Артикул']}\n"
        f"🏷️Название: {product_info['Название']}\n"
        f"🔢Отдел: {product_info['Отдел']}\n"
        f"📅Ближайшая дата заказа: {product_info['Дата заказа']}\n"
        f"🚚Ожидаемая дата поставки: {product_info['Дата поставки']}\n"
        f"🏭 Поставщик: {product_info['Поставщик']}" 
    )
    
    await message.answer(response, reply_markup=main_menu_keyboard())
    await state.clear()


@dp.message(F.text == "📦 Проверка стока")
async def handle_stock_check(message: types.Message):
    await message.answer("🛠️ Функция в разработке")



@dp.message(Command("test_scan"))
async def test_scan_command(message: types.Message):
    """Тест работы сканера штрих-кодов"""
    await message.answer("Отправьте фото штрих-кода для теста")
    
    # Сохраняем состояние для возврата
    await state.set_data({"test_mode": True})
    await state.set_state(OrderStates.barcode_scan)

@dp.message(OrderStates.barcode_scan, F.photo)
async def handle_test_scan(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if not data.get("test_mode"):
        await handle_barcode_scan(message, state)
        return
        
    try:
        processing_msg = await message.answer("🔍 Тестовая обработка...")
        photo = message.photo[-1]
        article, error = await process_barcode_image(photo)
        await processing_msg.delete()
        
        if error:
            await message.answer(f"❌ Ошибка: {error}")
        else:
            await message.answer(f"✅ Тест успешен! Распознан артикул: {article}")
            
    finally:
        await state.clear()
        await message.answer("🔙 Возврат в главное меню", reply_markup=main_menu_keyboard())

# ===================== ОБНОВЛЕНИЕ КЭША =====================
@dp.message(F.text == "/reload_cache")
async def reload_cache_command(message: types.Message):
    """Перезагрузка кэша с очисткой"""
    try:
        cache.clear()
        await cache_sheet_data(users_sheet, "users")
        await cache_sheet_data(gamma_cluster_sheet, "gamma_cluster")
        await message.answer("✅ Кэш перезагружен")
    except Exception as e:
        await message.answer(f"⚠️ Ошибка: {str(e)}")



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




# ===================== РАССЫЛКА =====================
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
    # Предпросмотр сообщения
    preview_text = "✉️ Предпросмотр сообщения:\n"
    if content['type'] == 'text':
        preview_text += content['text']
    else:
        preview_text += f"[{content['type'].upper()}] {content.get('caption', '')}"
    await message.answer(
        preview_text,
        reply_markup=target_selection_keyboard()  # Клавиатура выбора целевой аудитории
    )
    await state.set_state(AdminBroadcast.target_selection)

@dp.message(AdminBroadcast.target_selection)
async def handle_target_selection(message: types.Message, state: FSMContext):
    if message.text == "Всем":
        await state.update_data(target="all")
        await message.answer("✅ Отправить всем пользователям", reply_markup=broadcast_confirmation_keyboard())
        await state.set_state(AdminBroadcast.confirmation)
    elif message.text == "Вручную":
        await message.answer("🔢 Введите ID пользователей через запятую:")
        await state.set_state(AdminBroadcast.manual_ids)
    elif message.text == "❌ Отмена":
        await state.clear()
        await message.answer("❌ Рассылка отменена", reply_markup=main_menu_keyboard())
    else:
        await message.answer("❌ Неверный выбор. Пожалуйста, используйте кнопки.", reply_markup=target_selection_keyboard())

@dp.message(AdminBroadcast.manual_ids)
async def process_manual_ids(message: types.Message, state: FSMContext):
    user_ids = [id.strip() for id in message.text.split(",") if id.strip().isdigit()]
    if not user_ids:
        await message.answer("❌ Неверный формат ID. Повторите ввод:")
        return
    await state.update_data(target="manual", user_ids=user_ids)
    await message.answer("✅ ID пользователей введены", reply_markup=broadcast_confirmation_keyboard())
    await state.set_state(AdminBroadcast.confirmation)

@dp.message(AdminBroadcast.confirmation, F.text == "✅ Подтвердить рассылку")
async def confirm_broadcast(message: types.Message, state: FSMContext):
    await state.update_data(last_activity=datetime.now().isoformat())
    data = await state.get_data()
    content = data['content']
    target = data.get('target', 'all')
    user_ids = data.get('user_ids')
    # Записываем в логи
    logs_sheet.append_row([
        datetime.now().strftime("%d.%m.%Y %H:%M"),
        message.from_user.id,
        "BROADCAST",
        f"Type: {content['type']}, Chars: {len(content.get('text', '') or content.get('caption', ''))}"
    ])
    await message.answer("🔄 Начинаю рассылку...", reply_markup=main_menu_keyboard())
    # Асинхронная рассылка
    asyncio.create_task(send_broadcast(content, target, user_ids))
    await state.clear()

async def send_broadcast(message_text: str, user_ids: list) -> tuple:
    """Улучшенная функция рассылки с обработкой ошибок"""
    success = 0
    failed = 0
    failed_ids = []
    
    for user_id in user_ids:
        try:
            # Пропускаем пустые ID
            if not user_id or not user_id.strip():
                continue
                
            await bot.send_message(int(user_id), message_text)
            success += 1
            await asyncio.sleep(0.1)  # Ограничение частоты отправки
        except Exception as e:
            failed += 1
            failed_ids.append(user_id)
            logging.error(f"Ошибка рассылки: {user_id} - {str(e)}")
    
    # Логирование результатов
    logging.info(f"Рассылка завершена: успешно={success}, неудачно={failed}")
    if failed_ids:
        logging.info(f"Не удалось отправить: {', '.join(failed_ids)}")
    
    return success, failed
    await bot.send_message(
        chat_id=ADMINS[0],
        text=f"📊 Результаты рассылки:\n✅ Успешно: {success}\n❌ Не удалось: {failed}"
    )




#================================Статистика==========================================#


STATS_COLUMNS = [
    "Дата", "Время", "User ID", "Имя", "Фамилия", 
    "Должность", "Магазин", "Команда", "Тип события"
]

async def log_user_activity(user_id: str, command: str, event_type: str = "command"):
    """Запись информации о действии пользователя"""
    try:
        user_data = await get_user_data(str(user_id))
        if not user_data:
            return
            
        # Формируем запись для логирования
        new_record = [
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
        stats_sheet.append_row(new_record)
        
        # Обновляем кэш статистики
        if "stats_data" in cache:
            stats_data = pickle.loads(cache["stats_data"])
            stats_data.append(new_record)
            cache["stats_data"] = pickle.dumps(stats_data[-1000:])  # Храним последние 1000 записей
        
    except Exception as e:
        logging.error(f"Ошибка логирования статистики: {str(e)}")

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
     
    """Общая инициализация для всех режимов"""
    startup_msg = "🟢 Бот запущен"
    print(startup_msg)
    try:  
        print("♻️ Начало загрузки кэша...")
        await preload_cache()
        print(f"✅ Кэш загружен. Ключи: {list(cache.keys())[:5]}...")
    except Exception as e:
        error_msg = f"🚨 Критическая ошибка запуска: {str(e)}"
        print(error_msg)
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
