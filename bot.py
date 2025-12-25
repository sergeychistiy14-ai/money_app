import asyncio
import sqlite3
import json
import logging
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    WebAppInfo
)

# --- НАСТРОЙКИ ---
API_TOKEN = '8503104964:AAFQjyQlePmmsyo1tXWHdW-IZd6V9utI4pA'

# !!! ВНИМАНИЕ: Если index.html лежит в папке, добавь её в путь !!!
# Пример: "https://fingoal.ru/mypage/index.html"
WEB_APP_URL = "https://fingoal.ru/index.html"

logging.basicConfig(level=logging.INFO)
bot = Bot(token=API_TOKEN)
dp = Dispatcher()


# --- БАЗА ДАННЫХ ---
def init_db():
    with sqlite3.connect('finance_pro.db') as conn:
        cursor = conn.cursor()
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS transactions
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
                       )
                       ''')
        conn.commit()


# --- МЕНЮ ---
def main_menu():
    kb = [
        [KeyboardButton(text="🚀 Записать (Mini App)", web_app=WebAppInfo(url=WEB_APP_URL))],
        [KeyboardButton(text="📊 Мой Баланс"), KeyboardButton(text="📝 История")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


# --- ОБРАБОТЧИК ДАННЫХ ИЗ MINI APP ---
@dp.message(F.web_app_data)
async def handle_webapp_data(message: types.Message):
    try:
        # 1. Получаем данные из JS
        data = json.loads(message.web_app_data.data)

        # 2. Вытаскиваем значения (как назвали в index.html)
        t_type = data['type']  # 'expense' или 'income'
        amount = float(data['amount'])
        category = data['category']

        # 3. Сохраняем в Базу Данных
        with sqlite3.connect('finance_pro.db') as conn:
            conn.execute(
                "INSERT INTO transactions (user_id, amount, category, type, date) VALUES (?, ?, ?, ?, ?)",
                (message.from_user.id, amount, category, t_type, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            )

        # 4. Отвечаем пользователю
        icon = "📉" if t_type == 'expense' else "📈"
        text_type = "Расход" if t_type == 'expense' else "Доход"

        await message.answer(
            f"✅ **Сохранено!**\n\n"
            f"{icon} Тип: {text_type}\n"
            f"💰 Сумма: {amount:,.0f} р.\n"
            f"📂 Категория: {category}",
            parse_mode="Markdown"
        )

    except Exception as e:
        logging.error(f"Ошибка WebApp: {e}")
        await message.answer("❌ Произошла ошибка при чтении данных.")


# --- КОМАНДА СТАРТ ---
@dp.message(Command("start"))
async def start(message: types.Message):
    init_db()
    await message.answer("Привет! Нажми кнопку ниже, чтобы открыть приложение 👇", reply_markup=main_menu())


# --- КНОПКА БАЛАНС ---
@dp.message(F.text == "📊 Мой Баланс")
async def show_balance(message: types.Message):
    with sqlite3.connect('finance_pro.db') as conn:
        rows = conn.execute("SELECT amount, type FROM transactions WHERE user_id = ?",
                            (message.from_user.id,)).fetchall()

    income = sum(row[0] for row in rows if row[1] == 'income')
    expense = sum(row[0] for row in rows if row[1] == 'expense')
    balance = income - expense

    await message.answer(
        f"📊 **Финансовая сводка:**\n\n"
        f"➕ Доходы: {income:,.0f} р.\n"
        f"➖ Расходы: {expense:,.0f} р.\n"
        f"⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n"
        f"💰 **Итого: {balance:,.0f} р.**",
        parse_mode="Markdown"
    )


# --- КНОПКА ИСТОРИЯ ---
@dp.message(F.text == "📝 История")
async def show_history(message: types.Message):
    with sqlite3.connect('finance_pro.db') as conn:
        rows = conn.execute(
            "SELECT date, amount, category, type FROM transactions WHERE user_id = ? ORDER BY id DESC LIMIT 5",
            (message.from_user.id,)
        ).fetchall()

    if not rows:
        await message.answer("История пуста.")
        return

    text = "📝 **Последние 5 операций:**\n\n"
    for date, amount, category, t_type in rows:
        sign = "+" if t_type == 'income' else "-"
        text += f"{date[:10]} | {sign}{amount:.0f} р. ({category})\n"

    await message.answer(text, parse_mode="Markdown")

# --- ЗАПУСК ---
async def main():
    init_db()
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())