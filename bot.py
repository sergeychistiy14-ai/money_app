import asyncio
import sqlite3
import json
import logging
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
)

# --- НАСТРОЙКИ ---
API_TOKEN = '8503104964:AAFQjyQlePmmsyo1tXWHdW-IZd6V9utI4pA'
WEB_APP_URL = 'https://fingoal.ru'  # Убедись, что тут твой домен с https

logging.basicConfig(level=logging.INFO)
bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


# --- БАЗА ДАННЫХ ---
def init_db():
    with sqlite3.connect('finance_pro.db') as conn:
        cursor = conn.cursor()
        # Основная таблица транзакций
        cursor.execute('''CREATE TABLE IF NOT EXISTS transactions
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
        # Таблица категорий
        cursor.execute('''CREATE TABLE IF NOT EXISTS categories
                          (
                              id
                              INTEGER
                              PRIMARY
                              KEY
                              AUTOINCREMENT,
                              user_id
                              INTEGER,
                              name
                              TEXT,
                              type
                              TEXT
                          )''')
        # Таблица целей
        cursor.execute('''CREATE TABLE IF NOT EXISTS goals
                          (
                              id
                              INTEGER
                              PRIMARY
                              KEY
                              AUTOINCREMENT,
                              user_id
                              INTEGER,
                              name
                              TEXT,
                              target
                              REAL,
                              current
                              REAL
                          )''')
        conn.commit()


# --- ВСПОМОГАТЕЛЬНЫЕ ДАННЫЕ ---
MONTHS = {
    "01": "Январь", "02": "Февраль", "03": "Март", "04": "Апрель",
    "05": "Май", "06": "Июнь", "07": "Июль", "08": "Август",
    "09": "Сентябрь", "10": "Октябрь", "11": "Ноябрь", "12": "Декабрь"
}


# --- КЛАВИАТУРЫ ---
def main_menu():
    kb = [
        [KeyboardButton(text="🚀 Быстрая запись", web_app=WebAppInfo(url=WEB_APP_URL))],
        [KeyboardButton(text="📊 Баланс"), KeyboardButton(text="📋 Отчет")],
        [KeyboardButton(text="🎯 Цели"), KeyboardButton(text="🗂 Категории")],
        [KeyboardButton(text="📝 Транзакции")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


# --- ОБРАБОТКА ДАННЫХ ИЗ MINI APP ---
@dp.message(F.web_app_data)
async def handle_webapp_data(message: types.Message):
    try:
        web_data = json.loads(message.web_app_data.data)
        action = web_data.get("action")
        amount = float(web_data['amount'])
        category = web_data['category']

        t_type = 'expense' if action == 'add_expense' else 'income'

        with sqlite3.connect('finance_pro.db') as conn:
            conn.execute(
                "INSERT INTO transactions (user_id, amount, category, type, date) VALUES (?, ?, ?, ?, ?)",
                (message.from_user.id, amount, category, t_type, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            )

        icon = "🔻" if t_type == 'expense' else "🟢"
        label = "Расход" if t_type == 'expense' else "Доход"

        await message.answer(
            f"{icon} *Запись через Mini App*\n"
            f"⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n"
            f"💰 Сумма: `{amount:,.2f}` р.\n"
            f"🗂 Категория: {category}\n"
            f"📝 Тип: {label}",
            parse_mode="Markdown"
        )
    except Exception as e:
        logging.error(f"WebAppData Error: {e}")
        await message.answer("❌ Ошибка при сохранении данных из приложения.")


# --- ЛОГИКА ОТЧЕТОВ ---
async def get_report_text(user_id, period):
    with sqlite3.connect('finance_pro.db') as conn:
        rows = conn.execute("SELECT amount, category, type FROM transactions WHERE user_id = ? AND date LIKE ?",
                            (user_id, f"{period}%")).fetchall()

    if not rows:
        return "За этот период данных пока нет."

    inc, exp = 0, 0
    inc_cats, exp_cats = {}, {}

    for amt, cat, t_type in rows:
        if t_type == 'income':
            inc += amt
            inc_cats[cat] = inc_cats.get(cat, 0) + amt
        elif t_type == 'expense':
            exp += amt
            exp_cats[cat] = exp_cats.get(cat, 0) + amt

    report = f"📋 *ОТЧЕТ ({period})*\n⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n"
    report += f"💰 Доходы: `+{inc:,.0f}` р.\n"
    report += f"📉 Расходы: `-{exp:,.0f}` р.\n"
    report += f"⚖️ *Баланс: {(inc - exp):,.0f} р.*\n"
    return report


# --- ОБРАБОТЧИКИ КНОПОК ---
@dp.message(Command("start"))
async def start(message: types.Message):
    init_db()
    await message.answer("Привет! Я твой финансовый бот.", reply_markup=main_menu())


@dp.message(F.text == "📊 Баланс")
async def balance_btn(message: types.Message):
    text = await get_report_text(message.from_user.id, datetime.now().strftime("%Y-%m"))
    await message.answer(text, parse_mode="Markdown")


@dp.message(F.text == "📝 Транзакции")
async def show_transactions(message: types.Message):
    with sqlite3.connect('finance_pro.db') as conn:
        rows = conn.execute(
            "SELECT amount, category, type, date FROM transactions WHERE user_id = ? ORDER BY id DESC LIMIT 10",
            (message.from_user.id,)).fetchall()
    if not rows:
        return await message.answer("История пуста.")

    res = "📝 *ПОСЛЕДНИЕ ОПЕРАЦИИ:*\n\n"
    for a, c, t, d in rows:
        sign = "+" if t == 'income' else "-"
        res += f"`{d[5:16]}` | *{sign}{a:,.0f}* ({c})\n"
    await message.answer(res, parse_mode="Markdown")


@dp.message(F.text == "🎯 Цели")
async def goals_btn(message: types.Message):
    await message.answer("🎯 Секция целей в разработке или используй кнопки категорий.")


@dp.message(F.text == "🗂 Категории")
async def categories_btn(message: types.Message):
    await message.answer("🗂 Используйте Mini App для быстрого выбора категорий.")


# --- ЗАПУСК ---
async def main():
    init_db()
    print("Бот запущен и готов к работе...")
    await dp.start_polling(bot)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Бот остановлен")