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
# Ссылка на твой GitHub (проверь, чтобы в конце был слеш)
WEB_APP_URL = "https://sergeychistiy14-ai.github.io/money_app/"
# Путь к базе данных
DB_PATH = 'finance_pro.db'

logging.basicConfig(level=logging.INFO)
bot = Bot(token=API_TOKEN)
dp = Dispatcher()


# --- БАЗА ДАННЫХ ---
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


# --- ОБРАБОТЧИК API (Сюда приходят данные из index.html) ---
async def handle_api_save(request):
    try:
        data = await request.json()
        user_id = data.get('user_id')
        t_type = data.get('type')
        amount = float(data.get('amount'))
        category = data.get('category')

        # Если user_id не пришел из WebApp, это ошибка
        if not user_id:
            return web.json_response({"status": "error", "message": "User ID missing"}, status=400)

        # Сохраняем в базу
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT INTO transactions (user_id, amount, category, type, date) VALUES (?, ?, ?, ?, ?)",
                (user_id, amount, category, t_type, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            )

        # Бот отправляет подтверждение в чат пользователю
        icon = "📉" if t_type == 'expense' else "📈"
        text_type = "Расход" if t_type == 'expense' else "Доход"

        await bot.send_message(
            user_id,
            f"✅ **Данные получены!**\n\n"
            f"{icon} {text_type}: {amount:,.0f} р.\n"
            f"📂 Категория: {category}",
            parse_mode="Markdown"
        )

        return web.json_response({"status": "ok"})
    except Exception as e:
        logging.error(f"Ошибка API: {e}")
        return web.json_response({"status": "error", "message": str(e)}, status=400)


# --- ОБРАБОТЧИКИ БОТА ---
@dp.message(Command("start"))
async def start(message: types.Message):
    init_db()
    kb = [
        [KeyboardButton(text="🚀 Записать (Mini App)", web_app=WebAppInfo(url=WEB_APP_URL))],
        [KeyboardButton(text="📊 Мой Баланс"), KeyboardButton(text="📝 История")]
    ]
    await message.answer(
        "Привет! Нажми кнопку ниже, чтобы открыть финансовый помощник 👇",
        reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    )


@dp.message(F.text == "📊 Мой Баланс")
async def show_balance(message: types.Message):
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("SELECT amount, type FROM transactions WHERE user_id = ?",
                            (message.from_user.id,)).fetchall()

    income = sum(row[0] for row in rows if row[1] == 'income')
    expense = sum(row[0] for row in rows if row[1] == 'expense')
    balance = income - expense

    await message.answer(
        f"📊 **Баланс:**\n\n➕ Доходы: {income:,.0f} р.\n➖ Расходы: {expense:,.0f} р.\n"
        f"⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n💰 **Итого: {balance:,.0f} р.**",
        parse_mode="Markdown"
    )


@dp.message(F.text == "📝 История")
async def show_history(message: types.Message):
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT date, amount, category, type FROM transactions WHERE user_id = ? ORDER BY id DESC LIMIT 5",
            (message.from_user.id,)
        ).fetchall()

    if not rows:
        await message.answer("История пока пуста.")
        return

    text = "📝 **Последние 5 операций:**\n\n"
    for date, amount, category, t_type in rows:
        sign = "+" if t_type == 'income' else "-"
        text += f"`{date[:10]}` | {sign}{amount:.0f} р. ({category})\n"
    await message.answer(text, parse_mode="Markdown")


# --- ЗАПУСК ---
async def main():
    init_db()

    # Настройка веб-сервера aiohttp
    app = web.Application()
    app.router.add_post('/api/save', handle_api_save)
    runner = web.AppRunner(app)
    await runner.setup()

    # Бот будет слушать локальный порт 8080
    site = web.TCPSite(runner, '127.0.0.1', 8080)

    logging.info("Starting API server on port 8080...")
    await site.start()

    logging.info("Starting Bot polling...")
    await dp.start_polling(bot)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Бот выключен")