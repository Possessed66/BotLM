import os
import json
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
WEBHOOK_HOST = os.getenv('WEBHOOK_HOST')  # Например: https://123.45.67.89.sslip.io
WEBHOOK_PATH = "/webhook"  # Путь для веб-хука
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"

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
    article_input_info = State()

# ===================== КЛАВИАТУРЫ =====================
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


def make_order_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.button(text="Сделать заказ")
    builder.button(text="🏠 Главное меню")
    builder.adjust(2, 1)
    return builder.as_markup(resize_keyboard=True)

# ===================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====================
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
    logs_sheet.append_row([
        datetime.now().strftime("%d.%m.%Y %H:%M"),
        user_id,
        "ERROR",
        error
    ])

def get_supplier_dates_sheet(shop_number: str):
    return orders_spreadsheet.worksheet(f"Даты выходов заказов {shop_number}")

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

def parse_supplier_data(record):
    """
    Извлекает необходимые данные из записи поставщика.
    :param record: Словарь с данными поставщика.
    :return: Словарь с извлеченными данными.
    """
    order_days = []
    # Извлекаем дни выхода заказа из трех столбцов
    order_day_1 = record.get('День выхода заказа', '')
    order_day_2 = record.get('День выхода заказа 2', '')
    order_day_3 = record.get('День выхода заказа 3', '')
    # Добавляем дни в список, если они не пустые
    if order_day_1:
        order_days.append(int(order_day_1))
    if order_day_2:
        order_days.append(int(order_day_2))
    if order_day_3:
        order_days.append(int(order_day_3))
    return {
        'supplier_id': record.get('Номер осн. пост.', ''),
        'order_days': order_days,
        'delivery_days': int(record.get('Срок доставки в магазин', 0)),  # Количество дней на доставку
        'supplier_name': record.get('Название осн. пост.', '')
    }

# ===================== ОБРАБОТЧИКИ КОМАНД =====================
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
    try:
        # Поиск строки по артикулу и магазину
        unique_key = f"{article}{data['shop']}"
        cell = gamma_cluster_sheet.find(unique_key)
        if not cell:
            await message.answer("❌ Товар не найден.")
            return
        # Получаем строку с данными товара
        product_row = gamma_cluster_sheet.row_values(cell.row)
        product_data = dict(zip(gamma_cluster_sheet.row_values(1), product_row))
        # Извлекаем данные поставщика
        supplier_id = str(product_data["Номер осн. пост."]).strip()
        supplier_sheet = get_supplier_dates_sheet(data['shop'])
        # Поиск строки по supplier_id
        supplier_cell = supplier_sheet.find(supplier_id)
        if not supplier_cell:
            raise ValueError("Поставщик не найден")
        # Получаем строку с данными поставщика
        supplier_row = supplier_sheet.row_values(supplier_cell.row)
        supplier_data = parse_supplier_data(dict(zip(supplier_sheet.row_values(1), supplier_row)))
        # Рассчитываем даты
        order_date, delivery_date = calculate_delivery_date(supplier_data)
        await state.update_data(
            article=article,
            product_name=product_data['Название'],
            department=product_data['Отдел'],
            order_date=order_date,
            delivery_date=delivery_date,
            supplier_id=supplier_id,
            supplier_name=supplier_data['supplier_name']
        )
        await message.answer(
            f"Магазин: {data['shop']}\n"
            f"📦 Артикул: {article}\n"
            f"🏷️ Название: {product_data['Название']}\n"
            f"📅 Дата заказа: {order_date}\n"
            f"🚚 Дата поставки: {delivery_date}\n"
            f"🏭 Название поставщика: {supplier_data['supplier_name']}"
        )
        await message.answer("🔢 Введите количество товара:")
        await state.set_state(OrderStates.quantity_input)
    except Exception as e:
        await log_error(message.from_user.id, f"Article {article}: {str(e)}")
        await message.answer(f"⚠️ Ошибка: {str(e)}")

@dp.message(OrderStates.quantity_input)
async def process_quantity(message: types.Message, state: FSMContext):
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
    data = await state.get_data()
    order_reason = message.text.strip()
    # Обновляем состояние
    await state.update_data(order_reason=order_reason)
    # Вывод информации для подтверждения
    await message.answer(
        f"📦 Артикул: {data['article']}\n"
        f"🏷️ Название: {data['product_name']}\n"
        f"📅 Дата заказа: {data['order_date']}\n"
        f"🚚 Дата поставки: {data['delivery_date']}\n"
        f"🏭 Название поставщика: {supplier_data['supplier_name']}"
        f"Количество: {data['quantity']}\n"
        f"Номер заказа/Причина: {order_reason}\n",
        reply_markup=confirm_keyboard()
    )
    await state.set_state(OrderStates.confirmation)

@dp.message(OrderStates.confirmation, F.text == "✅ Подтвердить")
async def final_confirmation(message: types.Message, state: FSMContext):
    data = await state.get_data()
    try:
        department_sheet = orders_spreadsheet.worksheet(data['department'])
        next_row = len(department_sheet.col_values(1)) + 1
        # СОЗДАЁМ СПИСОК ОБНОВЛЕНИЙ
        updates = [
            {'range': f'A{next_row}', 'values': [[data['shop']]]},          # Магазин
            {'range': f'B{next_row}', 'values': [[data['article']]]},       # Артикул
            {'range': f'C{next_row}', 'values': [[data['order_reason']]]},  # Причина/Номер
            {'range': f'D{next_row}', 'values': [[datetime.now().strftime("%d.%m.%Y %H:%M")]]},  # Дата заказа
            {'range': f'E{next_row}', 'values': [[f"{data['user_name']}, {data['user_position']}"]]},  # Имя/Должность
            {'range': f'K{next_row}', 'values': [[str(data['quantity'])]]},  # Количество
            {'range': f'R{next_row}', 'values': [[str(message.from_user.id)]]},  # Chat ID
        ]
        # ПРОВЕРКА НАЛИЧИЯ ВСЕХ ОБЯЗАТЕЛЬНЫХ ПОЛЯХ
        required_fields = ['shop', 'article', 'order_reason', 'quantity']
        for field in required_fields:
            if not data.get(field):
                raise ValueError(f"Отсутствует обязательное поле: {field}")
        # ПРОМЕЩАЕМ ВСЕ ОБНОВЛЕНИЯ В ОДИН ЗАПРОС
        department_sheet.batch_update(updates)
        await message.answer("✅ Заказ успешно сохранен!", reply_markup=main_menu_keyboard())
        await state.clear()
    except Exception as e:
        await log_error(message.from_user.id, f"Save Error: {str(e)}")
        await message.answer("⚠️ Ошибка сохранения заказа")

@dp.message(OrderStates.confirmation, F.text == "✏️ Исправить количество")
async def correct_quantity(message: types.Message, state: FSMContext):
    await message.answer("🔢 Введите новое количество:", reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(OrderStates.quantity_input)


@dp.message(OrderStates.confirmation, F.text == "❌ Отмена")
async def cancel_order(message: types.Message, state: FSMContext):
    await message.answer("Отмена заказа. Возврат в главное меню.", reply_markup=main_menu_keyboard())
    await state.clear()


@dp.message(F.text == "📋 Запрос информации")
async def handle_info_request(message: types.Message, state: FSMContext):
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
    await state.set_state(OrderStates.article_input_info)

@dp.message(OrderStates.article_input_info)
async def process_article_info(message: types.Message, state: FSMContext):
    article = message.text.strip()
    data = await state.get_data()
    try:
        # Поиск строки по артикулу и магазину
        unique_key = f"{article}{data['shop']}"
        cell = gamma_cluster_sheet.find(unique_key)
        if not cell:
            await message.answer("❌ Товар не найден.")
            return
        # Получаем строку с данными товара
        product_row = gamma_cluster_sheet.row_values(cell.row)
        product_data = dict(zip(gamma_cluster_sheet.row_values(1), product_row))
        # Извлекаем данные поставщика
        supplier_id = str(product_data["Номер осн. пост."]).strip()
        supplier_sheet = get_supplier_dates_sheet(data['shop'])
        # Поиск строки по supplier_id
        supplier_cell = supplier_sheet.find(supplier_id)
        if not supplier_cell:
            raise ValueError("Поставщик не найден")
        # Получаем строку с данными поставщика
        supplier_row = supplier_sheet.row_values(supplier_cell.row)
        supplier_data = parse_supplier_data(dict(zip(supplier_sheet.row_values(1), supplier_row)))
        # Рассчитываем даты
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
            f"Магазин: {data['shop']}\n"
            f"📦 Артикул: {article}\n"
            f"🏷️ Название: {product_data['Название']}\n"
            f"📅 Дата заказа: {order_date}\n"
            f"🚚 Дата поставки: {delivery_date}\n"
            f"🏭 Название поставщика: {supplier_data['supplier_name']}"
        )
        await message.answer("Выберите действие:", reply_markup=make_order_keyboard())
    except Exception as e:
        await log_error(message.from_user.id, f"Article {article}: {str(e)}")
        await message.answer(f"⚠️ Ошибка: {str(e)}")

@dp.message(F.text == "Сделать заказ", OrderStates.article_input_info)
async def make_order_from_info(message: types.Message, state: FSMContext):
    data = await state.get_data()
    article = data.get('article')
    if not article:
        await message.answer("Артикул не найден. Попробуйте снова.")
        return
    await state.update_data(quantity=None)
    await message.answer("🔢 Введите количество товара:")
    await state.set_state(OrderStates.quantity_input)

@dp.message(F.text == "🏠 Главное меню", OrderStates.article_input_info)
async def go_to_main_menu_from_info(message: types.Message, state: FSMContext):
    await message.answer("Возврат в главное меню.", reply_markup=main_menu_keyboard())
    await state.clear()

@dp.message(OrderStates.order_reason_input)
async def process_order_reason(message: types.Message, state: FSMContext):
    data = await state.get_data()
    order_reason = message.text.strip()
    # Обновляем состояние
    await state.update_data(order_reason=order_reason)
    # Вывод информации для подтверждения
    await message.answer(
        f"📦 Артикул: {data['article']}
"
        f"🏷️ Название: {data['product_name']}
"
        f"📅 Дата заказа: {data['order_date']}
"
        f"🚚 Дата поставки: {data['delivery_date']}
"
        f"🏭 Название поставщика: {data['supplier_name']}
"
        f"Количество: {data['quantity']}
"
        f"Номер заказа/Причина: {order_reason}
",
        reply_markup=confirm_keyboard()
    )
    await state.set_state(OrderStates.confirmation)

@dp.message(F.text == "📦 Проверка стока")
async def handle_stock_check(message: types.Message):
    await message.answer("🛠️ Функция в разработке")

# ===================== ОБРАБОТЧИК ВЕБХУКОВ =====================
async def on_startup(app):
    await bot.set_webhook(url=WEBHOOK_URL)
    logging.info(f"Webhook URL: {WEBHOOK_URL}")

async def on_shutdown(app):
    await bot.delete_webhook()
    await bot.session.close()

async def handle_webhook(request):
    try:
        data = await request.json()
        chat_id = data.get('chat_id')
        text = data.get('text')
        if not chat_id or not text:
            raise ValueError("Недостаточно данных в вебхуке")
        await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
        return web.Response(text="Ok", status=200)
    except Exception as e:
        logging.error(f"Ошибка обработки вебхука: {str(e)}")
        return web.Response(text="Error", status=500)

# ИНИЦИАЛИЗАЦИЯ АППЛИКАЦИИ ОДИН РАЗ
app = web.Application()
app.router.add_post(WEBHOOK_PATH, handle_webhook)
app.on_startup.append(on_startup)
app.on_shutdown.append(on_shutdown)

@dp.message(lambda message: 'order_update' in message.text)
async def send_order_notification(message: types.Message):
    try:
        data = message.text.split('\n')
        chat_id = data[1]
        order_info = '\n'.join(data[2:])
        await bot.send_message(chat_id=chat_id, text=order_info, parse_mode=ParseMode.HTML)
    except Exception as e:
        logging.error(f"Ошибка отправки уведомления: {str(e)}")

if __name__ == "__main__":
    # Запуск бота в режиме polling
    async def main():
        await dp.start_polling(bot, skip_updates=True)
    import asyncio
    asyncio.run(main())
