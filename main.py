import os
import json
import asyncio
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from google.oauth2.service_account import Credentials
import gspread
from gspread.exceptions import APIError, SpreadsheetNotFound
from aiohttp import web
import logging

# =============== ОПТИМИЗИРОВАННЫЙ КЭШ ===============
CACHE = {}  # Кэш для данных таблиц
CACHE_TIMEOUT = 60 * 10  # Обновляем каждые 10 минут

async def get_sheet_data(sheet, force_update=False):
    """Асинхронный метод получения данных таблицы с кэшем"""
    now = datetime.now().timestamp()
    cached_data = CACHE.get(sheet.title, {})
    if not force_update and cached_data.get('expires') > now:
        return cached_data['data']
    
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(
        None, 
        lambda: sheet.get_all_values()
    )
    
    CACHE[sheet.title] = {
        'data': data,
        'expires': now + CACHE_TIMEOUT
    }
    return data

async def preload_sheets():
    await get_sheet_data(gamma_cluster_sheet, force_update=True)
    
    # Получаем все листы из orders_spreadsheet
    all_worksheets = orders_spreadsheet.worksheets()
    
    # Фильтруем листы по шаблону "Даты выходов заказов"
    supplier_sheets = [
        ws.title.replace("Даты выходов заказов ", "")
        for ws in all_worksheets
        if ws.title.startswith("Даты выходов заказов")
    ]
    
    for shop in supplier_sheets:
        await get_sheet_data(
            get_supplier_dates_sheet(shop),
            force_update=True
        )

# =============== ОБНОВЛЕННЫЕ КОНФИГУРАЦИИ ===============
from dotenv import load_dotenv
load_dotenv('secret.env')  

try:
    BOT_TOKEN = os.environ['BOT_TOKEN']
    GOOGLE_CREDS_JSON = os.environ['GOOGLE_CREDENTIALS']
except KeyError as e:
    raise RuntimeError(f"Отсутствует переменная {e}")

GOOGLE_CREDS = json.loads(GOOGLE_CREDS_JSON)
SPREADSHEET_NAME = "ShopBotData"
ORDERS_SPREADSHEET_NAME = "Копия Заказы МЗ 0.2"
USERS_SHEET_NAME = "Пользователи"
GAMMA_CLUSTER_SHEET = "Гамма кластер"
LOGS_SHEET = "Логи"

WEBHOOK_HOST = os.getenv('WEBHOOK_HOST')
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"

# =============== ИНИЦИАЛИЗАЦИЯ ===============
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

# =============== СОСТОЯНИЯ FSM ===============
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

# =============== КЛАВИАТУРЫ ===============
def main_menu_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.button(text="📋 Запрос информации")
    builder.button(text="📦 Проверка стока")
    builder.button(text="🛒 Заказ под клиента")
    builder.adjust(2, 1)
    return builder.as_markup(resize_keyboard=True)

def confirm_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.button(text="✅ Подтвердить")
    builder.button(text="✏️ Исправить количество")
    builder.button(text="❌ Отмена")
    builder.adjust(2, 1)
    return builder.as_markup(resize_keyboard=True)

# =============== ОПТИМИЗИРОВАННЫЕ ФУНКЦИИ ===============
def get_supplier_dates_sheet(shop_number: str):
    """Возвращает лист с датами поставок для указанного магазина"""
    return orders_spreadsheet.worksheet(f"Даты выходов заказов {shop_number}")
    
async def get_supplier_data(shop_number: str, supplier_id: str):
    """Асинхронный поиск данных поставщика с кэшированием"""
    supplier_sheet = get_supplier_dates_sheet(shop_number)
    cached_data = await get_sheet_data(supplier_sheet)
    
    for row in cached_data:
        if row[0] == supplier_id:
            return row
    return None
async def get_user_data(user_id: str):
    try:
        cell = users_sheet.find(user_id)
        return {
            'shop': users_sheet.cell(cell.row, 5).value,
            'name': users_sheet.cell(cell.row, 2).value,
            'surname': users_sheet.cell(cell.row, 3).value,
            'position': users_sheet.cell(cell.row, 4).value
        }
    except:
        return None

async def log_error(user_id: str, error: str):
    await asyncio.sleep(0.1)  # Фоновое логирование
    logs_sheet.append_row([
        datetime.now().strftime("%d.%m.%Y %H:%M"),
        user_id,
        "ERROR",
        error
    ])



# =============== ОБРАБОТЧИКИ КОМАНД ===============
@dp.message(F.text == "/start")
async def start_handler(message: types.Message, state: FSMContext):
    user_data = await get_user_data(str(message.from_user.id))
    if user_data:
        await message.answer("ℹ️ Вы уже зарегистрированы!", reply_markup=main_menu_keyboard())
        return
    await message.answer("👋 Добро пожаловать! Введите ваше имя:", reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(Registration.name)

@dp.message(Registration.name)
async def process_name(message: types.Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await message.answer("📝 Введите вашу фамилию:")
    await state.set_state(Registration.surname)

@dp.message(Registration.surname)
async def process_surname(message: types.Message, state: FSMContext):
    await state.update_data(surname=message.text.strip())
    await message.answer("💼 Введите вашу должность:")
    await state.set_state(Registration.position)

@dp.message(Registration.position)
async def process_position(message: types.Message, state: FSMContext):
    await state.update_data(position=message.text.strip())
    await message.answer("🏪 Введите номер магазина (только цифры):")
    await state.set_state(Registration.shop)

@dp.message(Registration.shop)
async def process_shop(message: types.Message, state: FSMContext):
    if not message.text.strip().isdigit():
        await message.answer("❌ Номер магазина должен быть числом! Повторите ввод:")
        return
    
    data = await state.get_data()
    user_id = str(message.from_user.id)
    
    try:
        # Используем append_row с асинхронной оберткой
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            users_sheet.append_row, [
                user_id,
                data['name'],
                data['surname'],
                data['position'],
                message.text.strip(),
                datetime.now().strftime("%d.%m.%Y %H:%M")
            ]
        )
        
        await message.answer("✅ Регистрация завершена!", reply_markup=main_menu_keyboard())
        await state.clear()
    except Exception as e:
        await message.answer("⚠️ Ошибка сохранения данных!")
        await log_error(user_id, str(e))

@dp.message(F.text == "🛒 Заказ под клиента")
async def handle_client_order(message: types.Message, state: FSMContext):
    user_data = await get_user_data(str(message.from_user.id))
    if not user_data:
        await message.answer("❌ Сначала пройдите регистрацию через /start")
        return
    
    await state.update_data(
        shop=user_data['shop'],
        user_name=user_data['name'],
        user_position=user_data['position']
    )
    await message.answer("🔢 Введите артикул товара:", reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(OrderStates.article_input)

@dp.message(OrderStates.article_input)
async def process_article(message: types.Message, state: FSMContext):
    article = message.text.strip()
    data = await state.get_data()
    
    # Получаем кэшированные данные гамма-кластера
    gamma_data = await get_sheet_data(gamma_cluster_sheet)
    
    unique_key = f"{article}{data['shop']}"
    for row in gamma_data:
        if row[0] == unique_key:
            product_data = dict(zip(gamma_data[0], row))  # Используем заголовки из первой строки
            break
    else:
        await message.answer("❌ Товар не найден.")
        return
    
    try:
        supplier_id = product_data["Номер осн. пост."]
        supplier_data_row = await get_supplier_data(data['shop'], supplier_id)
        
        if not supplier_data_row:
            raise ValueError("Поставщик не найден")
        
        supplier_data = parse_supplier_data(dict(zip(supplier_data_row[0], supplier_data_row)))
        
        order_date, delivery_date = calculate_delivery_date(supplier_data)
        
        await state.update_data(
            article=article,
            product_name=product_data['Название'],
            department=product_data['Отдел'],
            order_date=order_date,
            delivery_date=delivery_date,
            supplier_id=supplier_id
        )
        
        await message.answer(
            f"📦 Артикул: {article}\n"
            f"🏷️ Название: {product_data['Название']}\n"
            f"📅 Дата заказа: {order_date}\n"
            f"🚚 Дата поставки: {delivery_date}",
            disable_web_page_preview=True
        )
        await message.answer("🔢 Введите количество товара:", reply_markup=types.ReplyKeyboardRemove())
        await state.set_state(OrderStates.quantity_input)
    except Exception as e:
        await log_error(str(message.from_user.id), f"Ошибка обработки товара: {str(e)}")
        await message.answer(f"⚠️ Ошибка: {str(e)}")

def parse_supplier_data(record):
    order_days = []
    order_day_1 = record.get('День выхода заказа', '')
    order_day_2 = record.get('День выхода заказа 2', '')
    order_day_3 = record.get('День выхода заказа 3', '')
    
    for day in (order_day_1, order_day_2, order_day_3):
        if day:
            order_days.append(int(day))
    
    return {
        'order_days': order_days,
        'delivery_days': int(record.get('Срок доставки в магазин', 0))
    }

@dp.message(OrderStates.quantity_input)
async def process_quantity(message: types.Message, state: FSMContext):
    if not message.text.strip().isdigit():
        await message.answer("❌ Введите число!")
        return
    
    data = await state.update_data(quantity=int(message.text.strip()))
    await message.answer("Введите номер заказа или причину:")
    await state.set_state(OrderStates.order_reason_input)

@dp.message(OrderStates.order_reason_input)
async def process_order_reason(message: types.Message, state: FSMContext):
    data = await state.update_data(order_reason=message.text.strip())
    await message.answer(
        f"📦 Артикул: {data['article']}\n"
        f"🏷️ Название: {data['product_name']}\n"
        f"📅 Дата заказа: {data['order_date']}\n"
        f"🚚 Дата поставки: {data['delivery_date']}\n"
        f"Количество: {data['quantity']}\n"
        f"Причина: {data['order_reason']}",
        reply_markup=confirm_keyboard()
    )
    await state.set_state(OrderStates.confirmation)

@dp.message(OrderStates.confirmation, F.text == "✅ Подтвердить")
async def final_confirmation(message: types.Message, state: FSMContext):
    data = await state.get_data()
    
    try:
        department_sheet = orders_spreadsheet.worksheet(data['department'])
        cached_department = await get_sheet_data(department_sheet)
        next_row = len(cached_department) + 1
        
        # Атомарное обновление через batch_update
        updates = [
            {'range': f'A{next_row}', 'values': [[data['shop']]]},
            {'range': f'B{next_row}', 'values': [[data['article']]]},
            {'range': f'C{next_row}', 'values': [[data['order_reason']]]},
            {'range': f'D{next_row}', 'values': [[datetime.now().strftime("%d.%m.%Y %H:%M")]]},
            {'range': f'E{next_row}', 'values': [[f"{data['user_name']}, {data['user_position']}"]]},
            {'range': f'K{next_row}', 'values': [[str(data['quantity'])]]},
            {'range': f'R{next_row}', 'values': [[str(message.from_user.id)]]}
        ]
        
        # Проверка обязательных полей
        required = ['shop', 'article', 'order_reason', 'quantity']
        if not all(data.get(field) for field in required):
            raise ValueError("Не все поля заполнены")
        
        # Выполняем batch_update асинхронно
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            department_sheet.batch_update,
            updates
        )
        
        await message.answer("✅ Заказ сохранен!", reply_markup=main_menu_keyboard())
        await state.clear()
    except Exception as e:
        await log_error(str(message.from_user.id), f"Ошибка сохранения: {str(e)}")
        await message.answer("⚠️ Ошибка сохранения заказа")

# =============== ОБРАБОТЧИКИ ВЕБХУКОВ ===============
async def on_startup(app):
    await bot.set_webhook(WEBHOOK_URL)
    await preload_sheets()  # Предзагрузка кэша при старте
    logging.info(f"Webhook активирован: {WEBHOOK_URL}")

async def on_shutdown(app):
    await bot.delete_webhook()
    await bot.session.close()

async def handle_webhook(request):
    try:
        data = await request.json()
        chat_id = data.get('chat_id')
        text = data.get('text')
        
        if chat_id and text:
            await bot.send_message(chat_id=chat_id, text=text)
            return web.Response(text="Ok", status=200)
        else:
            return web.Response(text="Bad request", status=400)
    except Exception as e:
        logging.error(f"Ошибка вебхука: {str(e)}")
        return web.Response(text="Error", status=500)

# =============== ДОПОЛНИТЕЛЬНЫЕ ОПТИМИЗАЦИИ ===============
async def update_cache():
    """Фоновая задача для обновления кэша"""
    while True:
        await get_sheet_data(gamma_cluster_sheet, force_update=True)
        for shop in ["магазин1", "магазин2"]:
            await get_sheet_data(
                get_supplier_dates_sheet(shop),
                force_update=True
            )
        await asyncio.sleep(CACHE_TIMEOUT)

async def main():
    # Запускаем фоновое обновление кэша
    asyncio.create_task(update_cache())
    
    # Инициализация веб-сервера
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    
    # Запуск через polling для тестирования
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
