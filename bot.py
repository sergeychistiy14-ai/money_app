import asyncio
import sqlite3
import json
import logging
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, WebAppInfo
from aiohttp import web

# --- НАСТРОЙКИ ---
API_TOKEN = '8503104964:AAFQjyQlePmmsyo1tXWHdW-IZd6V9utI4pA'
WEB_APP_URL = "https://sergeychistiy14-ai.github.io/money_app/"
DB_PATH = 'finance_pro.db'

logging.basicConfig(level=logging.INFO)
bot = Bot(token=API_TOKEN)
dp = Dispatcher()


# --- РАБОТА С БАЗОЙ ДАННЫХ ---
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS transactions
                        (
                            id
                            INTEGER
                            PRIMARY
                            KEY
                            AUTOINCREMENT,
                            user_id
                            INTEGER,
                            amount
                            REAL,
                            category
                            TEXT,
                            type
                            TEXT,
                            date
                            TEXT
                        )''')
        conn.commit()


def save_transaction(user_id, amount, category, t_type):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO transactions (user_id, amount, category, type, date) VALUES (?, ?, ?, ?, ?)",
            (user_id, float(amount), category, t_type, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )


# --- 1. ОБРАБОТКА ДАННЫХ ИЗ MINI APP (tg.sendData) ---
# Этот метод работает ВСЕГДА, даже если порты закрыты
@dp.message(F.web_app_data)
async def web_app_receive(message: types.Message):
    try:
        data = json.loads(message.web_app_data.data)
        save_transaction(
            message.from_user.id,
            data.get('amount'),
            data.get('category'),
            data.get('type')
        )

        icon = "📉" if data.get('type') == 'expense' else "📈"
        await message.answer(f"✅ **Данные сохранены!**\n{icon} {data.get('amount')} р. ({data.get('category')})",
                             parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Ошибка web_app_data: {e}")
        await message.answer("❌ Ошибка при сохранении данных.")


# --- 2. API ОБРАБОТЧИК (Прямой POST запрос) ---
# Для работы этого метода нужен открытый порт 8080
async def handle_api_save(request):
    try:
        data = await request.json()
        user_id = data.get('user_id')

        save_transaction(user_id, data.get('amount'), data.get('category'), data.get('type'))

        icon = "📉" if data.get('type') == 'expense' else "📈"
        await bot.send_message(
            user_id,
            f"✅ **Запись через API!**\n{icon} {data.get('amount')} р. ({data.get('category')})",
            parse_mode="Markdown"
        )
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"status": "error", "message": str(e)}, status=400)


# --- 3. ОБЫЧНЫЕ КОМАНДЫ БОТА ---
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    init_db()
    kb = [
        [KeyboardButton(text="🚀 Записать доход/расход", web_app=WebAppInfo(url=WEB_APP_URL))],
        [KeyboardButton(text="💰 Баланс"), KeyboardButton(text="📋 История")]
    ]
    await message.answer(
        "Привет! Я твой финансовый помощник. Нажми на кнопку ниже, чтобы открыть приложение.",
        reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    )


@dp.message(F.text == "💰 Баланс")
async def get_balance(message: types.Message):
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("SELECT amount, type FROM transactions WHERE user_id = ?",
                            (message.from_user.id,)).fetchall()

    inc = sum(r[0] for r in rows if r[1] == 'income')
    exp = sum(r[0] for r in rows if r[1] == 'expense')

    await message.answer(
        f"📊 **Ваш баланс:**\n\n🟢 Доходы: {inc:,.0f} р.\n🔴 Расходы: {exp:,.0f} р.\n\n💰 **Итого: {inc - exp:,.0f} р.**",
        parse_mode="Markdown")


@dp.message(F.text == "📋 История")
async def get_history(message: types.Message):
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT date, amount, category, type FROM transactions WHERE user_id = ? ORDER BY id DESC LIMIT 5",
            (message.from_user.id,)).fetchall()

    if not rows:
        return await message.answer("История пуста.")

    text = "📂 **Последние 5 записей:**\n\n"
    for r in rows:
        sign = "+" if r[3] == 'income' else "-"
        text += f"`{r[0][:10]}` | **{sign}{r[1]:.0f} р.** ({r[2]})\n"
    await message.answer(text, parse_mode="Markdown")


# --- ЗАПУСК ---
async def main():
    init_db()

    # Настройка API сервера (aiohttp)
    app = web.Application()
    app.router.add_post('/api/save', handle_api_save)
    runner = web.AppRunner(app)
    await runner.setup()

    # Слушаем на всех интерфейсах (0.0.0.0)
    api_site = web.TCPSite(runner, '0.0.0.0', 8080)
    await api_site.start()

    logging.info("API server started on port 8080")

    # Запуск бота (polling)
    await dp.start_polling(bot)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped")