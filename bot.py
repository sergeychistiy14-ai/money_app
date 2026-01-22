import asyncio
import sqlite3
import json
import base64
import logging
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, WebAppInfo, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiohttp import web

import difflib

# --- НАСТРОЙКИ ---
API_TOKEN = '8503104964:AAFQjyQlePmmsyo1tXWHdW-IZd6V9utI4pA'
WEB_APP_URL = "https://sergeychistiy14-ai.github.io/money_app/"
DB_PATH = 'finance_pro.db'

# Включаем логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot_debug.log"),
        logging.StreamHandler()
    ]
)
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# Note: Main web_app_data handler is defined at the end of the file (web_app_data_handler)


class GoalStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_target = State()

class CategoryStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_type = State()

class TransactionStates(StatesGroup):
    waiting_for_decision = State() # Ждем решения: создать новую или выбрать

class BudgetStates(StatesGroup):
    waiting_for_category = State()
    waiting_for_amount = State()


# --- РАБОТА С БАЗОЙ ДАННЫХ ---

# Главный администратор (не может быть удалён)
ROOT_ADMIN_ID = 616706758

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        # 1. Транзакции
        conn.execute('''CREATE TABLE IF NOT EXISTS transactions
                        (id INTEGER PRIMARY KEY AUTOINCREMENT,
                         user_id INTEGER,
                         amount REAL,
                         category TEXT,
                         type TEXT,
                         date TEXT,
                         description TEXT)''')
        
        try:
            conn.execute("ALTER TABLE transactions ADD COLUMN description TEXT")
        except sqlite3.OperationalError:
            pass

        # 2. Цели
        conn.execute('''CREATE TABLE IF NOT EXISTS goals
                        (id INTEGER PRIMARY KEY AUTOINCREMENT,
                         user_id INTEGER,
                         name TEXT,
                         target_amount REAL,
                         current_amount REAL DEFAULT 0,
                         status TEXT DEFAULT 'active',
                         created_at TEXT)''')
        
        # 3. Категории
        conn.execute('''CREATE TABLE IF NOT EXISTS categories
                        (id INTEGER PRIMARY KEY AUTOINCREMENT,
                         user_id INTEGER,
                         name TEXT,
                         type TEXT,
                         created_at TEXT)''')

        # 4. Бюджеты
        conn.execute('''CREATE TABLE IF NOT EXISTS budgets
                        (id INTEGER PRIMARY KEY AUTOINCREMENT,
                         user_id INTEGER,
                         category_name TEXT,
                         amount REAL,
                         month_year TEXT)''')
        
        # 5. Администраторы
        conn.execute('''CREATE TABLE IF NOT EXISTS admins
                        (user_id INTEGER PRIMARY KEY,
                         added_by INTEGER,
                         added_at TEXT)''')
        
        # Добавляем root админа если его нет
        conn.execute('''INSERT OR IGNORE INTO admins (user_id, added_by, added_at) 
                        VALUES (?, ?, ?)''', (ROOT_ADMIN_ID, ROOT_ADMIN_ID, datetime.now().strftime("%Y-%m-%d")))
        
        # 6. Пользователи (для отслеживания)
        conn.execute('''CREATE TABLE IF NOT EXISTS users
                        (user_id INTEGER PRIMARY KEY,
                         username TEXT,
                         first_name TEXT,
                         registered_at TEXT,
                         last_active TEXT)''')
        
        # 7. Ограничения пользователей
        conn.execute('''CREATE TABLE IF NOT EXISTS user_limits
                        (user_id INTEGER PRIMARY KEY,
                         is_blocked INTEGER DEFAULT 0,
                         max_transactions INTEGER DEFAULT -1,
                         disabled_features TEXT DEFAULT '')''')
        
        conn.commit()


def is_admin(user_id):
    """Проверка является ли пользователь админом"""
    with sqlite3.connect(DB_PATH) as conn:
        result = conn.execute("SELECT 1 FROM admins WHERE user_id = ?", (user_id,)).fetchone()
        return result is not None


def is_user_blocked(user_id):
    """Проверка заблокирован ли пользователь"""
    with sqlite3.connect(DB_PATH) as conn:
        result = conn.execute("SELECT is_blocked FROM user_limits WHERE user_id = ?", (user_id,)).fetchone()
        return result and result[0] == 1


def get_disabled_features(user_id):
    """Получить отключённые функции пользователя"""
    with sqlite3.connect(DB_PATH) as conn:
        result = conn.execute("SELECT disabled_features FROM user_limits WHERE user_id = ?", (user_id,)).fetchone()
        if result and result[0]:
            return result[0].split(',')
        return []


def register_user(user):
    """Регистрация/обновление пользователя"""
    with sqlite3.connect(DB_PATH) as conn:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute('''INSERT INTO users (user_id, username, first_name, registered_at, last_active)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(user_id) DO UPDATE SET 
                        username = excluded.username,
                        first_name = excluded.first_name,
                        last_active = excluded.last_active''',
                     (user.id, user.username, user.first_name, now, now))
        conn.commit()


def save_transaction(user_id, amount, category, t_type, description=None):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        
        # Проверка на дубликаты (защита от двойного нажатия)
        cursor.execute("""
            SELECT id, date FROM transactions 
            WHERE user_id = ? AND amount = ? AND category = ? AND type = ? 
            ORDER BY id DESC LIMIT 1
        """, (user_id, float(amount), category, t_type))
        
        last_tx = cursor.fetchone()
        if last_tx:
            last_date_str = last_tx[1]
            try:
                last_date = datetime.strptime(last_date_str, "%Y-%m-%d %H:%M:%S")
                if (datetime.now() - last_date).total_seconds() < 5:
                    logging.info("Duplicate transaction prevented")
                    return False 
            except ValueError:
                pass

        conn.execute(
            "INSERT INTO transactions (user_id, amount, category, type, date, description) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, float(amount), category, t_type, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), description)
        )
        return True


# --- 1. ОБРАБОТКА ДАННЫХ ИЗ MINI APP (tg.sendData) ---
# УСТАРЕВШИЙ ОБРАБОТЧИК - ОТКЛЮЧЕН (использует старый формат данных)
# Актуальный обработчик: web_app_data_handler (строка ~1243)
# @dp.message(F.web_app_data)
# async def web_app_receive(message: types.Message):
#     try:
#         data = json.loads(message.web_app_data.data)
#         save_transaction(
#             message.from_user.id,
#             data.get('amount'),
#             data.get('category'),
#             data.get('type')
#         )
#
#         icon = "📉" if data.get('type') == 'expense' else "📈"
#         await message.answer(f"✅ **Данные сохранены!**\n{icon} {data.get('amount')} р. ({data.get('category')})",
#                              parse_mode="Markdown")
#     except Exception as e:
#         logging.error(f"Ошибка web_app_data: {e}")
#         await message.answer("❌ Ошибка при сохранении данных.")

# --- 2. API ОБРАБОТЧИК (Прямой POST запрос) ---
# Универсальный API для MiniApp (работает через MenuButton и KeyboardButton)

async def handle_api_action(request):
    """Обработчик всех действий из MiniApp через HTTP API"""
    # CORS headers
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
    }
    
    # Handle preflight
    if request.method == 'OPTIONS':
        return web.Response(headers=headers)
    
    try:
        data = await request.json()
        action = data.get('action')
        user_id = data.get('user_id')
        
        if not user_id:
            return web.json_response({"status": "error", "message": "user_id required"}, status=400, headers=headers)
        
        resp_text = "✅ Данные обновлены"
        
        with sqlite3.connect(DB_PATH) as conn:
            if action == "add_tx":
                t_type = data.get('t')  # income/expense
                amount = float(data.get('a'))
                cat = data.get('c')
                desc = data.get('d', '')
                date_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                conn.execute("INSERT INTO transactions (user_id, amount, category, type, date, description) VALUES (?, ?, ?, ?, ?, ?)",
                             (user_id, amount, cat, t_type, date_str, desc))
                
                icon = "📉" if t_type == "expense" else "📈"
                resp_text = f"✅ Добавлено: {amount:.0f} р. ({cat})"
                
            elif action == "add_goal":
                name = data.get('n')
                target = float(data.get('t'))
                conn.execute("INSERT INTO goals (user_id, name, target_amount, current_amount, created_at) VALUES (?, ?, ?, 0, ?)",
                             (user_id, name, target, datetime.now().strftime("%Y-%m-%d")))
                resp_text = f"🎯 Цель '{name}' создана!"
                
            elif action == "add_budget":
                cat = data.get('c')
                limit = float(data.get('l'))
                m_key = datetime.now().strftime("%Y-%m")
                conn.execute("DELETE FROM budgets WHERE user_id = ? AND category_name = ? AND month_year = ?", (user_id, cat, m_key))
                conn.execute("INSERT INTO budgets (user_id, category_name, amount, month_year) VALUES (?, ?, ?, ?)", (user_id, cat, limit, m_key))
                resp_text = f"⚖️ Бюджет на '{cat}' установлен!"

            elif action == "top_up_goal":
                gid = data.get('id')
                amount = float(data.get('a'))
                conn.execute("UPDATE goals SET current_amount = current_amount + ? WHERE id = ? AND user_id = ?", (amount, gid, user_id))
                resp_text = f"💰 Копилка пополнена на {amount:.0f} р.!"
            
            else:
                return web.json_response({"status": "error", "message": f"Unknown action: {action}"}, status=400, headers=headers)
            
            conn.commit()
        
        # Отправляем уведомление в бот
        try:
            await bot.send_message(user_id, resp_text, parse_mode="Markdown")
        except Exception as e:
            logging.error(f"Failed to send message: {e}")
        
        # Обновляем Menu Button
        await update_user_menu_button(user_id)
        
        return web.json_response({"status": "ok", "message": resp_text}, headers=headers)
        
    except Exception as e:
        logging.error(f"API Error: {e}")
        return web.json_response({"status": "error", "message": str(e)}, status=400, headers=headers)


# --- 3. ОБЫЧНЫЕ КОМАНДЫ БОТА ---
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    init_db()
    register_user(message.from_user)
    
    # Проверка блокировки
    if is_user_blocked(message.from_user.id):
        await message.answer("🚫 Ваш аккаунт заблокирован. Обратитесь к администратору.")
        return
    # Проверяем, есть ли аргументы (payload)
    # Формат: type|amount|category ИЛИ goal|name|target ИЛИ budget|cat|limit ИЛИ topup|id|amount
    args = message.text.split(maxsplit=1)
    if len(args) > 1:
        payload = args[1]
        try:
            # Декодируем Base64 (стандарт URL-safe)
            # 1. Восстанавливаем паддинг '='
            padding = len(payload) % 4
            if padding:
                payload += '=' * (4 - padding)
            
            # 2. Декодируем Base64
            from urllib.parse import unquote
            decoded_bytes = base64.urlsafe_b64decode(payload)
            decoded_str = decoded_bytes.decode('utf-8')
            
            # Формат строки: action|param1|param2
            parts = decoded_str.split('|')
            
            if len(parts) >= 3:
                action = parts[0]
                
                # --- ТРАНЗАКЦИЯ (income|1000|Salary или expense|500|Food) ---
                if action in ('income', 'expense'):
                    t_type, amount, category = action, parts[1], parts[2]
                    
                    if not save_transaction(message.from_user.id, amount, category, t_type):
                        try:
                            await message.delete()
                        except:
                            pass
                        return

                    try:
                        await message.delete()
                    except:
                        pass

                    icon = "📉" if t_type == 'expense' else "📈"
                    await message.answer(f"✅ **Данные сохранены!**\n{icon} {amount} р. ({category})",
                                         parse_mode="Markdown")
                    await update_user_menu_button(message.from_user.id)
                    return
                
                # --- ЦЕЛЬ (goal|iPhone|100000) ---
                elif action == 'goal':
                    name, target = parts[1], float(parts[2])
                    with sqlite3.connect(DB_PATH) as conn:
                        conn.execute("INSERT INTO goals (user_id, name, target_amount, current_amount, created_at) VALUES (?, ?, ?, 0, ?)",
                                     (message.from_user.id, name, target, datetime.now().strftime("%Y-%m-%d")))
                        conn.commit()
                    
                    try:
                        await message.delete()
                    except:
                        pass
                    
                    await message.answer(f"🎯 **Цель '{name}' создана!**\nНужно накопить: {target:,.0f} р.", parse_mode="Markdown")
                    await update_user_menu_button(message.from_user.id)
                    return
                
                # --- БЮДЖЕТ (budget|Food|10000) ---
                elif action == 'budget':
                    cat, limit = parts[1], float(parts[2])
                    month_key = datetime.now().strftime("%Y-%m")
                    with sqlite3.connect(DB_PATH) as conn:
                        conn.execute("DELETE FROM budgets WHERE user_id = ? AND category_name = ? AND month_year = ?",
                                     (message.from_user.id, cat, month_key))
                        conn.execute("INSERT INTO budgets (user_id, category_name, amount, month_year) VALUES (?, ?, ?, ?)",
                                     (message.from_user.id, cat, limit, month_key))
                        conn.commit()
                    
                    try:
                        await message.delete()
                    except:
                        pass
                    
                    await message.answer(f"⚖️ **Бюджет на '{cat}' установлен!**\nЛимит: {limit:,.0f} р.", parse_mode="Markdown")
                    await update_user_menu_button(message.from_user.id)
                    return
                
                # --- ПОПОЛНЕНИЕ ЦЕЛИ (topup|goal_id|amount) ---
                elif action == 'topup':
                    goal_id, amount = int(parts[1]), float(parts[2])
                    with sqlite3.connect(DB_PATH) as conn:
                        conn.execute("UPDATE goals SET current_amount = current_amount + ? WHERE id = ? AND user_id = ?",
                                     (amount, goal_id, message.from_user.id))
                        conn.commit()
                    
                    try:
                        await message.delete()
                    except:
                        pass
                    
                    await message.answer(f"💰 **Копилка пополнена на {amount:,.0f} р.!**", parse_mode="Markdown")
                    await update_user_menu_button(message.from_user.id)
                    return
                    
        except Exception as e:
            logging.error(f"Error parsing payload: {e}")
            pass

    # Генерируем URL с данными для MiniApp
    payload = await get_miniapp_data(message.from_user.id, limit=10)
    json_str = json.dumps(payload)
    b64_data = base64.urlsafe_b64encode(json_str.encode()).decode()
    webapp_url = f"{WEB_APP_URL}?data={b64_data}"
    
    kb = [
        [KeyboardButton(text="📱 Мои Деньги", web_app=WebAppInfo(url=webapp_url))],
        [KeyboardButton(text="🎯 Цели"), KeyboardButton(text="📂 Категории")],
        [KeyboardButton(text="📊 Бюджеты"), KeyboardButton(text="📈 Отчеты")],
        [KeyboardButton(text="💰 Баланс"), KeyboardButton(text="📋 Транзакции")]
    ]
    # Получаем имя пользователя
    user_name = message.from_user.first_name or "друг"
    
    await message.answer(
        f"👋 **Привет, {user_name}!**\n\n"
        f"Я — **FinGoal**, твой персональный финансовый помощник 💰\n\n"
        f"📱 **Нажми '📱 Мои Деньги'** чтобы открыть MiniApp\n\n"
        f"Или пиши мне текстом:\n"
        f"🔹 `1000 Еда` — записать расход\n"
        f"🔹 `+5000 ЗП` — записать доход\n"
        f"🔹 `!1000 Отпуск` — отложить в копилку\n\n"
        f"Удачного планирования! 🚀",
        reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True),
        parse_mode="Markdown"
    )


# --- АДМИН-ПАНЕЛЬ ---

class AdminStates(StatesGroup):
    waiting_for_admin_id = State()
    waiting_for_limit_value = State()

@dp.message(Command("admin"))
async def admin_cmd(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("🚫 У вас нет доступа к админ-панели.")
        return
    
    # Статистика
    with sqlite3.connect(DB_PATH) as conn:
        users_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        tx_count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        blocked_count = conn.execute("SELECT COUNT(*) FROM user_limits WHERE is_blocked = 1").fetchone()[0]
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Пользователи", callback_data="adm_users")],
        [InlineKeyboardButton(text="🚫 Блокировки", callback_data="adm_blocks")],
        [InlineKeyboardButton(text="⚙️ Ограничения", callback_data="adm_limits")],
        [InlineKeyboardButton(text="👑 Администраторы", callback_data="adm_admins")],
    ])
    
    await message.answer(
        f"👑 **Админ-панель FinGoal**\n\n"
        f"📊 Статистика:\n"
        f"• Пользователей: {users_count}\n"
        f"• Транзакций: {tx_count}\n"
        f"• Заблокировано: {blocked_count}\n",
        reply_markup=kb,
        parse_mode="Markdown"
    )


# --- Список пользователей с пагинацией ---
@dp.callback_query(F.data.startswith("adm_users"))
async def admin_users_list(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    
    # Парсим страницу: adm_users или adm_users_page_2
    parts = callback.data.split("_")
    page = 1
    if len(parts) >= 4 and parts[2] == "page":
        page = int(parts[3])
    
    per_page = 10
    offset = (page - 1) * per_page
    
    with sqlite3.connect(DB_PATH) as conn:
        total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        users = conn.execute("""
            SELECT u.user_id, u.username, u.first_name, u.last_active,
                   COALESCE(ul.is_blocked, 0) as is_blocked
            FROM users u
            LEFT JOIN user_limits ul ON u.user_id = ul.user_id
            ORDER BY u.last_active DESC
            LIMIT ? OFFSET ?
        """, (per_page, offset)).fetchall()
    
    if not users and page == 1:
        await callback.message.edit_text("Пользователей пока нет.")
        return
    
    total_pages = (total_users + per_page - 1) // per_page
    
    buttons = []
    for uid, uname, fname, last_active, blocked in users:
        status = "🚫" if blocked else "✅"
        name = fname or uname or str(uid)
        buttons.append([InlineKeyboardButton(
            text=f"{status} {name[:20]}",
            callback_data=f"adm_user_{uid}"
        )])
    
    # Навигация
    nav_row = []
    if page > 1:
        nav_row.append(InlineKeyboardButton(text="⬅️", callback_data=f"adm_users_page_{page-1}"))
    nav_row.append(InlineKeyboardButton(text=f"📄 {page}/{total_pages}", callback_data="ignore"))
    if page < total_pages:
        nav_row.append(InlineKeyboardButton(text="➡️", callback_data=f"adm_users_page_{page+1}"))
    buttons.append(nav_row)
    
    # Действия
    buttons.append([InlineKeyboardButton(text="🔍 Поиск по username", callback_data="adm_search_user")])
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="adm_back")])
    
    await callback.message.edit_text(
        f"👥 **Пользователи** ({total_users} всего)\n\n"
        f"Страница {page} из {total_pages}\n"
        f"✅ = активен, 🚫 = заблокирован",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="Markdown"
    )
    await callback.answer()


# --- Поиск пользователя по username ---
@dp.callback_query(F.data == "adm_search_user")
async def admin_search_prompt(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    
    await state.set_state("admin_search_user")
    await callback.message.edit_text(
        "🔍 **Поиск пользователя**\n\n"
        "Введите username (без @) или часть имени:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="adm_users")]
        ]),
        parse_mode="Markdown"
    )
    await callback.answer()


@dp.message(StateFilter("admin_search_user"))
async def admin_search_handler(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    
    query = message.text.strip().lower().replace("@", "")
    
    with sqlite3.connect(DB_PATH) as conn:
        users = conn.execute("""
            SELECT u.user_id, u.username, u.first_name,
                   COALESCE(ul.is_blocked, 0) as is_blocked
            FROM users u
            LEFT JOIN user_limits ul ON u.user_id = ul.user_id
            WHERE LOWER(u.username) LIKE ? OR LOWER(u.first_name) LIKE ?
            ORDER BY u.last_active DESC
            LIMIT 20
        """, (f"%{query}%", f"%{query}%")).fetchall()
    
    await state.clear()
    
    if not users:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔍 Искать ещё", callback_data="adm_search_user")],
            [InlineKeyboardButton(text="🔙 К списку", callback_data="adm_users")]
        ])
        await message.answer(f"❌ Пользователи по запросу \"{query}\" не найдены.", reply_markup=kb)
        return
    
    buttons = []
    for uid, uname, fname, blocked in users:
        status = "🚫" if blocked else "✅"
        name = fname or uname or str(uid)
        buttons.append([InlineKeyboardButton(
            text=f"{status} {name[:20]} (@{uname or 'N/A'})",
            callback_data=f"adm_user_{uid}"
        )])
    
    buttons.append([InlineKeyboardButton(text="🔍 Искать ещё", callback_data="adm_search_user")])
    buttons.append([InlineKeyboardButton(text="🔙 К списку", callback_data="adm_users")])
    
    await message.answer(
        f"🔍 Результаты поиска \"{query}\" ({len(users)}):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


# --- Детали пользователя ---
@dp.callback_query(F.data.startswith("adm_user_"))
async def admin_user_details(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    
    uid = int(callback.data.split("_")[2])
    
    with sqlite3.connect(DB_PATH) as conn:
        user = conn.execute("SELECT username, first_name, registered_at, last_active FROM users WHERE user_id = ?", (uid,)).fetchone()
        if not user:
            await callback.answer("Пользователь не найден")
            return
        
        # Статистика
        stats = conn.execute("""
            SELECT type, SUM(amount), COUNT(*) 
            FROM transactions WHERE user_id = ? 
            GROUP BY type
        """, (uid,)).fetchall()
        
        goals_count = conn.execute("SELECT COUNT(*) FROM goals WHERE user_id = ?", (uid,)).fetchone()[0]
        
        limits = conn.execute("SELECT is_blocked, disabled_features FROM user_limits WHERE user_id = ?", (uid,)).fetchone()
    
    uname, fname, reg_at, last_act = user
    is_blocked = limits[0] if limits else 0
    disabled = limits[1] if limits else ""
    
    income = expense = tx_count = 0
    for row in stats:
        if row[0] == 'income':
            income = row[1]
        elif row[0] == 'expense':
            expense = row[1]
        tx_count += row[2]
    
    balance = income - expense
    
    msg = f"👤 **{fname or uname or uid}**\n"
    msg += f"ID: `{uid}`\n"
    if uname: msg += f"Username: @{uname}\n"
    msg += f"\n📊 **Статистика:**\n"
    msg += f"• Баланс: {balance:,.0f} р.\n"
    msg += f"• Доходы: {income:,.0f} р.\n"
    msg += f"• Расходы: {expense:,.0f} р.\n"
    msg += f"• Транзакций: {tx_count}\n"
    msg += f"• Целей: {goals_count}\n"
    msg += f"\n📅 Регистрация: {reg_at[:10] if reg_at else 'N/A'}\n"
    msg += f"🕐 Последняя активность: {last_act[:16] if last_act else 'N/A'}\n"
    
    if is_blocked:
        msg += "\n🚫 **ЗАБЛОКИРОВАН**\n"
    if disabled:
        msg += f"⚠️ Отключено: {disabled}\n"
    
    buttons = []
    buttons.append([InlineKeyboardButton(text="📋 Транзакции", callback_data=f"adm_tx_{uid}_1")])
    if is_blocked:
        buttons.append([InlineKeyboardButton(text="✅ Разблокировать", callback_data=f"adm_unblock_{uid}")])
    else:
        buttons.append([InlineKeyboardButton(text="🚫 Заблокировать", callback_data=f"adm_block_{uid}")])
    
    buttons.append([InlineKeyboardButton(text="⚙️ Ограничения", callback_data=f"adm_userlim_{uid}")])
    buttons.append([InlineKeyboardButton(text="🔙 К списку", callback_data="adm_users")])
    
    await callback.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="Markdown")
    await callback.answer()


# --- Блокировка/Разблокировка ---
@dp.callback_query(F.data.startswith("adm_block_"))
async def admin_block_user(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    
    uid = int(callback.data.split("_")[2])
    
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO user_limits (user_id, is_blocked) VALUES (?, 1)
            ON CONFLICT(user_id) DO UPDATE SET is_blocked = 1
        """, (uid,))
        conn.commit()
    
    await callback.answer("✅ Пользователь заблокирован", show_alert=True)
    # Обновляем экран
    callback.data = f"adm_user_{uid}"
    await admin_user_details(callback)


@dp.callback_query(F.data.startswith("adm_unblock_"))
async def admin_unblock_user(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    
    uid = int(callback.data.split("_")[2])
    
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE user_limits SET is_blocked = 0 WHERE user_id = ?", (uid,))
        conn.commit()
    
    await callback.answer("✅ Пользователь разблокирован", show_alert=True)
    callback.data = f"adm_user_{uid}"
    await admin_user_details(callback)


# --- Просмотр транзакций пользователя ---
@dp.callback_query(F.data.startswith("adm_tx_"))
async def admin_view_user_transactions(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    
    # adm_tx_123456_1 (uid_page)
    parts = callback.data.split("_")
    uid = int(parts[2])
    page = int(parts[3]) if len(parts) > 3 else 1
    
    per_page = 15
    offset = (page - 1) * per_page
    
    with sqlite3.connect(DB_PATH) as conn:
        user = conn.execute("SELECT first_name, username FROM users WHERE user_id = ?", (uid,)).fetchone()
        total_tx = conn.execute("SELECT COUNT(*) FROM transactions WHERE user_id = ?", (uid,)).fetchone()[0]
        txs = conn.execute("""
            SELECT amount, category, type, date, COALESCE(description, '')
            FROM transactions WHERE user_id = ?
            ORDER BY id DESC
            LIMIT ? OFFSET ?
        """, (uid, per_page, offset)).fetchall()
    
    user_name = user[0] or user[1] or str(uid) if user else str(uid)
    total_pages = max(1, (total_tx + per_page - 1) // per_page)
    
    if not txs:
        msg = f"📋 **Транзакции {user_name}**\n\nНет транзакций."
    else:
        msg = f"📋 **Транзакции {user_name}**\n"
        msg += f"Страница {page}/{total_pages} (всего: {total_tx})\n\n"
        
        for amount, cat, t_type, date, desc in txs:
            icon = "📉" if t_type == "expense" else "📈"
            sign = "-" if t_type == "expense" else "+"
            date_short = date[5:10] if date else ""
            msg += f"{icon} {sign}{amount:,.0f} | {cat[:12]}"
            if desc:
                msg += f" | {desc[:15]}"
            msg += f" | {date_short}\n"
    
    buttons = []
    nav_row = []
    if page > 1:
        nav_row.append(InlineKeyboardButton(text="⬅️", callback_data=f"adm_tx_{uid}_{page-1}"))
    nav_row.append(InlineKeyboardButton(text=f"📄 {page}/{total_pages}", callback_data="ignore"))
    if page < total_pages:
        nav_row.append(InlineKeyboardButton(text="➡️", callback_data=f"adm_tx_{uid}_{page+1}"))
    if nav_row:
        buttons.append(nav_row)
    
    buttons.append([InlineKeyboardButton(text="🔙 К профилю", callback_data=f"adm_user_{uid}")])
    
    await callback.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="Markdown")
    await callback.answer()


# --- Управление ограничениями пользователя ---
@dp.callback_query(F.data.startswith("adm_userlim_"))
async def admin_user_limits(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    
    uid = int(callback.data.split("_")[2])
    
    with sqlite3.connect(DB_PATH) as conn:
        limits = conn.execute("SELECT disabled_features FROM user_limits WHERE user_id = ?", (uid,)).fetchone()
    
    disabled = limits[0].split(',') if limits and limits[0] else []
    
    features = [
        ("goals", "🎯 Цели"),
        ("budgets", "📊 Бюджеты"),
        ("reports", "📈 Отчёты"),
    ]
    
    buttons = []
    for feat_id, feat_name in features:
        is_off = feat_id in disabled
        status = "❌" if is_off else "✅"
        action = "enable" if is_off else "disable"
        buttons.append([InlineKeyboardButton(
            text=f"{status} {feat_name}",
            callback_data=f"adm_feat_{action}_{feat_id}_{uid}"
        )])
    
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data=f"adm_user_{uid}")])
    
    await callback.message.edit_text(
        f"⚙️ **Ограничения функций**\n\n✅ = включено, ❌ = отключено",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="Markdown"
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("adm_feat_"))
async def admin_toggle_feature(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    
    parts = callback.data.split("_")
    action = parts[2]  # enable/disable
    feature = parts[3]
    uid = int(parts[4])
    
    with sqlite3.connect(DB_PATH) as conn:
        limits = conn.execute("SELECT disabled_features FROM user_limits WHERE user_id = ?", (uid,)).fetchone()
        current = limits[0].split(',') if limits and limits[0] else []
        current = [f for f in current if f]  # Remove empty strings
        
        if action == "disable" and feature not in current:
            current.append(feature)
        elif action == "enable" and feature in current:
            current.remove(feature)
        
        new_disabled = ','.join(current)
        
        conn.execute("""
            INSERT INTO user_limits (user_id, disabled_features) VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET disabled_features = ?
        """, (uid, new_disabled, new_disabled))
        conn.commit()
    
    await callback.answer("✅ Сохранено")
    callback.data = f"adm_userlim_{uid}"
    await admin_user_limits(callback)


# --- Список блокировок ---
@dp.callback_query(F.data == "adm_blocks")
async def admin_blocks_list(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    
    with sqlite3.connect(DB_PATH) as conn:
        blocked = conn.execute("""
            SELECT u.user_id, u.first_name, u.username
            FROM user_limits ul
            JOIN users u ON ul.user_id = u.user_id
            WHERE ul.is_blocked = 1
        """).fetchall()
    
    if not blocked:
        buttons = [[InlineKeyboardButton(text="🔙 Назад", callback_data="adm_back")]]
        await callback.message.edit_text("Заблокированных пользователей нет.", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
        await callback.answer()
        return
    
    buttons = []
    for uid, fname, uname in blocked:
        name = fname or uname or str(uid)
        buttons.append([InlineKeyboardButton(text=f"🚫 {name}", callback_data=f"adm_user_{uid}")])
    
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="adm_back")])
    
    await callback.message.edit_text(
        f"🚫 **Заблокированные пользователи:** {len(blocked)}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="Markdown"
    )
    await callback.answer()


# --- Управление лимитами (общее) ---
@dp.callback_query(F.data == "adm_limits")
async def admin_limits_menu(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    
    buttons = [
        [InlineKeyboardButton(text="👥 Выбрать пользователя", callback_data="adm_users")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="adm_back")],
    ]
    
    await callback.message.edit_text(
        "⚙️ **Управление ограничениями**\n\nВыберите пользователя для настройки его лимитов.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="Markdown"
    )
    await callback.answer()


# --- Управление администраторами ---
@dp.callback_query(F.data == "adm_admins")
async def admin_admins_list(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    
    with sqlite3.connect(DB_PATH) as conn:
        admins = conn.execute("""
            SELECT a.user_id, u.first_name, u.username, a.added_at
            FROM admins a
            LEFT JOIN users u ON a.user_id = u.user_id
        """).fetchall()
    
    msg = "👑 **Администраторы:**\n\n"
    buttons = []
    
    for uid, fname, uname, added_at in admins:
        name = fname or uname or str(uid)
        is_root = "👑 " if uid == ROOT_ADMIN_ID else ""
        msg += f"{is_root}• {name} (`{uid}`)\n"
        
        if uid != ROOT_ADMIN_ID:
            buttons.append([InlineKeyboardButton(text=f"❌ Удалить {name}", callback_data=f"adm_rmadmin_{uid}")])
    
    buttons.append([InlineKeyboardButton(text="➕ Добавить админа", callback_data="adm_addadmin")])
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="adm_back")])
    
    await callback.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="Markdown")
    await callback.answer()


@dp.callback_query(F.data == "adm_addadmin")
async def admin_add_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    
    await callback.message.edit_text(
        "➕ **Добавление администратора**\n\n"
        "Введите Telegram ID нового админа:\n"
        "(его можно узнать через @userinfobot)",
        parse_mode="Markdown"
    )
    await state.set_state(AdminStates.waiting_for_admin_id)
    await callback.answer()


@dp.message(AdminStates.waiting_for_admin_id)
async def admin_add_finish(message: types.Message, state: FSMContext):
    try:
        new_admin_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Введите корректный ID (число)")
        return
    
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT OR IGNORE INTO admins (user_id, added_by, added_at) VALUES (?, ?, ?)
        """, (new_admin_id, message.from_user.id, datetime.now().strftime("%Y-%m-%d")))
        conn.commit()
    
    await message.answer(f"✅ Администратор {new_admin_id} добавлен!")
    await state.clear()


@dp.callback_query(F.data.startswith("adm_rmadmin_"))
async def admin_remove(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    
    uid = int(callback.data.split("_")[2])
    
    if uid == ROOT_ADMIN_ID:
        await callback.answer("Нельзя удалить главного админа!", show_alert=True)
        return
    
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM admins WHERE user_id = ?", (uid,))
        conn.commit()
    
    await callback.answer("✅ Админ удалён")
    await admin_admins_list(callback)


# --- Кнопка Назад ---
@dp.callback_query(F.data == "adm_back")
async def admin_back(callback: types.CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    
    with sqlite3.connect(DB_PATH) as conn:
        users_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        tx_count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        blocked_count = conn.execute("SELECT COUNT(*) FROM user_limits WHERE is_blocked = 1").fetchone()[0]
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Пользователи", callback_data="adm_users")],
        [InlineKeyboardButton(text="🚫 Блокировки", callback_data="adm_blocks")],
        [InlineKeyboardButton(text="⚙️ Ограничения", callback_data="adm_limits")],
        [InlineKeyboardButton(text="👑 Администраторы", callback_data="adm_admins")],
    ])
    
    await callback.message.edit_text(
        f"👑 **Админ-панель FinGoal**\n\n"
        f"📊 Статистика:\n"
        f"• Пользователей: {users_count}\n"
        f"• Транзакций: {tx_count}\n"
        f"• Заблокировано: {blocked_count}\n",
        reply_markup=kb,
        parse_mode="Markdown"
    )
    await callback.answer()


@dp.message(F.text.in_({"💰 Баланс", "📊 Мой Баланс", "Баланс"}))
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


# --- 4. ФУНКЦИОНАЛ ЦЕЛЕЙ ---

@dp.message(F.text == "🎯 Цели")
async def goals_menu(message: types.Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать цель", callback_data="goal_create")],
        [InlineKeyboardButton(text="📋 Мои цели (Прогресс)", callback_data="goal_list")],
        [InlineKeyboardButton(text="❌ Удалить цель", callback_data="goal_delete")],
    ])
    await message.answer("🎯 **Управление Целями**\nВыберите действие:", reply_markup=kb, parse_mode="Markdown")


# -- Создание цели --
@dp.callback_query(F.data == "goal_create")
async def goal_create_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("Введите название новой цели (например: 'Новый iPhone'):")
    await state.set_state(GoalStates.waiting_for_name)
    await callback.answer()

@dp.message(GoalStates.waiting_for_name)
async def goal_name_entered(message: types.Message, state: FSMContext):
    logging.info(f"DEBUG: Goal name entered: {message.text}")
    await state.update_data(name=message.text)
    await message.answer("Сколько нужно накопить? (Введите число, например: 100000)")
    await state.set_state(GoalStates.waiting_for_target)
    logging.info("DEBUG: State set to waiting_for_target")

@dp.message(GoalStates.waiting_for_target)
async def goal_target_entered(message: types.Message, state: FSMContext):
    logging.info(f"DEBUG: Goal target entered: {message.text}")
    try:
        target = float(message.text.replace(' ', ''))
        data = await state.get_data()
        name = data['name']
        
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("INSERT INTO goals (user_id, name, target_amount, current_amount) VALUES (?, ?, ?, 0)",
                         (message.from_user.id, name, target))
            conn.commit()
            
        await message.answer(f"✅ Цель **'{name}'** создана!\nЦель: {target:,.0f} р.\n\nПополняйте её командой: `!сумма {name}`", parse_mode="Markdown")
        await state.clear()
    except ValueError:
        await message.answer("Пожалуйста, введите корректное число.")


# -- Список целей --
def generate_progress_bar(current, target, length=10):
    percent = current / target if target > 0 else 0
    if percent > 1: percent = 1
    filled = int(length * percent)
    bar = "🟩" * filled + "⬜" * (length - filled)
    return bar, int(percent * 100)

@dp.callback_query(F.data == "goal_list")
async def goal_list_view(callback: types.CallbackQuery):
    with sqlite3.connect(DB_PATH) as conn:
        goals = conn.execute("SELECT name, target_amount, current_amount FROM goals WHERE user_id = ?", 
                             (callback.from_user.id,)).fetchall()
    
    if not goals:
        await callback.message.answer("У вас пока нет целей. Создайте первую!")
        await callback.answer()
        return

    text = "🎯 **Ваши финансовые цели:**\n\n"
    for name, target, current in goals:
        bar, percent = generate_progress_bar(current, target)
        text += f"**{name}**\n{bar} {percent}%\n💰 {current:,.0f} / {target:,.0f} р.\n\n"
    
    await callback.message.answer(text, parse_mode="Markdown")
    await callback.answer()


# -- Удаление цели --
@dp.callback_query(F.data == "goal_delete")
async def goal_delete_select(callback: types.CallbackQuery):
    with sqlite3.connect(DB_PATH) as conn:
        goals = conn.execute("SELECT id, name FROM goals WHERE user_id = ?", 
                             (callback.from_user.id,)).fetchall()
    
    if not goals:
        await callback.message.answer("Нечего удалять 🤷‍♂️")
        await callback.answer()
        return

    # Создаем клавиатуру с кнопками для каждой цели
    buttons = []
    for g_id, g_name in goals:
        buttons.append([InlineKeyboardButton(text=f"❌ {g_name}", callback_data=f"delete_goal_{g_id}")])
    
    buttons.append([InlineKeyboardButton(text="🔙 Отмена", callback_data="goals_menu_back")]) # можно просто закрыть, но добавим
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.answer("Выберите цель для удаления:", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.startswith("delete_goal_"))
async def goal_delete_perform(callback: types.CallbackQuery):
    goal_id = callback.data.split("_")[2]
    
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM goals WHERE id = ?", (goal_id,))
        conn.commit()
    
    await callback.message.edit_text("✅ Цель удалена.")
    await callback.answer()


# --- 5. ФУНКЦИОНАЛ КАТЕГОРИЙ ---

@dp.message(F.text == "📂 Категории")
async def categories_menu(message: types.Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить категорию", callback_data="cat_create")],
        [InlineKeyboardButton(text="📊 Статистика за месяц", callback_data="cat_list")],
        [InlineKeyboardButton(text="❌ Удалить категорию", callback_data="cat_delete")],
    ])
    await message.answer("📂 **Управление Категориями**", reply_markup=kb, parse_mode="Markdown")

# -- Создание --
@dp.callback_query(F.data == "cat_create")
async def cat_create_start(callback: types.CallbackQuery, state: FSMContext):
    # Спросим тип
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📉 Расход", callback_data="type_expense"),
         InlineKeyboardButton(text="📈 Доход", callback_data="type_income")]
    ])
    await callback.message.answer("К чему относится категория?", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.startswith("type_"))
async def cat_type_selected(callback: types.CallbackQuery, state: FSMContext):
    c_type = callback.data.split("_")[1]
    logging.info(f"DEBUG: Category type selected: {c_type}")
    await state.update_data(type=c_type)
    await callback.message.answer("Введите название категории (например: 'Такси'):")
    await state.set_state(CategoryStates.waiting_for_name)
    logging.info("DEBUG: State set to CategoryStates.waiting_for_name")
    await callback.answer()

@dp.message(CategoryStates.waiting_for_name)
async def cat_name_entered(message: types.Message, state: FSMContext):
    logging.info(f"DEBUG: Category name entered: {message.text}")
    name = message.text.strip()
    data = await state.get_data()
    c_type = data['type']
    
    with sqlite3.connect(DB_PATH) as conn:
        # Проверка на дубликат
        exist = conn.execute("SELECT id FROM categories WHERE user_id = ? AND name = ? AND type = ?", 
                             (message.from_user.id, name, c_type)).fetchone()
        if exist:
             await message.answer("Такая категория уже есть!")
        else:
            conn.execute("INSERT INTO categories (user_id, name, type, created_at) VALUES (?, ?, ?, ?)",
                         (message.from_user.id, name, c_type, datetime.now().strftime("%Y-%m-%d")))
            conn.commit()
            await message.answer(f"✅ Категория **{name}** ({'Расход' if c_type == 'expense' else 'Доход'}) создана!", parse_mode="Markdown")
    
    await state.clear()


# -- Список со статистикой --
@dp.callback_query(F.data == "cat_list")
async def cat_list_view(callback: types.CallbackQuery):
    now = datetime.now()
    month_start = now.strftime("%Y-%m-01") # грубо сработает для выборки по строке YYYY-MM-DD
    
    with sqlite3.connect(DB_PATH) as conn:
        # 1. Берем наши созданные категории
        cats = conn.execute("SELECT name, type FROM categories WHERE user_id = ?", 
                            (callback.from_user.id,)).fetchall()
        
        # 2. Считаем суммы по транзакциям за этот месяц
        # (Осторожно: тут могут быть категории, которых нет в списке categories, если запись была текстом.
        #  Но мы покажем всё, что есть в transactions, сгруппировав по имени)
        
        stats = conn.execute("""
            SELECT category, type, SUM(amount) 
            FROM transactions 
            WHERE user_id = ? AND date >= ? 
            GROUP BY category, type
        """, (callback.from_user.id, month_start)).fetchall()
        
    # Преобразуем stats в словарь для быстрого поиска
    stats_dict = {(r[0].lower(), r[1]): r[2] for r in stats} # (name, type) -> amount
    
    # Собираем список для отображения. 
    # Объединим "официальные" категории и те, что просто встречались в транзакциях.
    all_cats = set()
    for c_name, c_type in cats:
        all_cats.add((c_name, c_type))
    for s_name, s_type, s_sum in stats:
        all_cats.add((s_name, s_type))
        
    if not all_cats:
        await callback.message.answer("Пока пусто. Самое время начать вести бюджет!")
        await callback.answer()
        return

    # Формируем текст
    msg = f"📊 **Статистика за {now.strftime('%B')}:**\n\n"
    
    # Доходы
    incomes = sorted([c for c in all_cats if c[1] == 'income'])
    if incomes:
        msg += "📈 **ДОХОДЫ:**\n"
        total_inc = 0
        for name, _ in incomes:
            amount = stats_dict.get((name.lower(), 'income'), 0)
            total_inc += amount
            msg += f"- {name}: {amount:,.0f} р.\n"
        msg += f"**Всего: {total_inc:,.0f} р.**\n\n"
        
    # Расходы
    expenses = sorted([c for c in all_cats if c[1] == 'expense'])
    if expenses:
        msg += "📉 **РАСХОДЫ:**\n"
        total_exp = 0
        for name, _ in expenses:
            amount = stats_dict.get((name.lower(), 'expense'), 0)
            total_exp += amount
            msg += f"- {name}: {amount:,.0f} р.\n"
        msg += f"**Всего: {total_exp:,.0f} р.**\n"

    await callback.message.answer(msg, parse_mode="Markdown")
    await callback.answer()

# -- Удаление --
# -- Удаление --
@dp.callback_query(F.data == "cat_delete")
async def cat_delete_start(callback: types.CallbackQuery, state: FSMContext):
    with sqlite3.connect(DB_PATH) as conn:
        # 1. Официальные категории
        cats_db = conn.execute("SELECT name FROM categories WHERE user_id = ?", 
                               (callback.from_user.id,)).fetchall()
        
        # 2. Категории из транзакций (призраки)
        cats_tx = conn.execute("SELECT DISTINCT category FROM transactions WHERE user_id = ?", 
                               (callback.from_user.id,)).fetchall()
        
    all_names = set()
    for (name,) in cats_db: all_names.add(name)
    for (name,) in cats_tx: all_names.add(name)
    
    if not all_names:
        await callback.message.answer("Вообще нет категорий.")
        await callback.answer()
        return

    # Сортируем и сохраняем в state, чтобы потом достать по индексу
    sorted_cats = sorted(list(all_names))
    await state.update_data(cats_to_delete=sorted_cats)
    
    buttons = []
    for i, name in enumerate(sorted_cats):
        buttons.append([InlineKeyboardButton(text=f"❌ {name}", callback_data=f"del_cat_idx_{i}")])
    buttons.append([InlineKeyboardButton(text="🔙 Отмена", callback_data="remove_kb")])

    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.answer("Выберите категорию для удаления (удалится история и сама категория):", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.startswith("del_cat_idx_"))
async def cat_delete_perform(callback: types.CallbackQuery, state: FSMContext):
    idx = int(callback.data.split("_")[3])
    data = await state.get_data()
    cats = data.get('cats_to_delete', [])
    
    if idx < 0 or idx >= len(cats):
        await callback.message.answer("Ошибка выбора. Попробуйте снова.")
        return

    cat_name = cats[idx]
    
    with sqlite3.connect(DB_PATH) as conn:
        # Удаляем отовсюду
        conn.execute("DELETE FROM categories WHERE name = ? AND user_id = ?", (cat_name, callback.from_user.id))
        conn.execute("DELETE FROM transactions WHERE category = ? AND user_id = ?", (cat_name, callback.from_user.id))
        conn.commit()
    
    await callback.message.edit_text(f"✅ Категория **'{cat_name}'** и все её транзакции удалены.", parse_mode="Markdown")
    await callback.answer()
    
@dp.callback_query(F.data == "remove_kb")
async def remove_keyboard(callback: types.CallbackQuery):
    await callback.message.delete()


# --- 6. ФУНКЦИОНАЛ БЮДЖЕТОВ ---

@dp.message(F.text == "📊 Бюджеты")
async def budgets_menu(message: types.Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Установить лимит", callback_data="budget_set")],
        [InlineKeyboardButton(text="📉 Мои лимиты (Статус)", callback_data="budget_list")],
    ])
    await message.answer("📊 **Управление Бюджетами (на месяц)**", reply_markup=kb, parse_mode="Markdown")

# -- Установка --
@dp.callback_query(F.data == "budget_set")
async def budget_set_start(callback: types.CallbackQuery, state: FSMContext):
    # Предлагаем выбрать категорию из существующих (только расходы)
    with sqlite3.connect(DB_PATH) as conn:
        cats = conn.execute("SELECT name FROM categories WHERE user_id = ? AND type = 'expense'", 
                            (callback.from_user.id,)).fetchall()
        # Также добавим те, что были в транзакциях
        cats_tx = conn.execute("SELECT DISTINCT category FROM transactions WHERE user_id = ? AND type = 'expense'", 
                               (callback.from_user.id,)).fetchall()
    
    all_cats = sorted(list(set([c[0] for c in cats] + [c[0] for c in cats_tx])))
    
    if not all_cats:
        await callback.message.answer("Сначала добавьте расходы, чтобы ставить на них лимиты.")
        await callback.answer()
        return

    buttons = []
    row = []
    for name in all_cats:
        row.append(InlineKeyboardButton(text=name, callback_data=f"bud_cat_{name}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row: buttons.append(row)
    buttons.append([InlineKeyboardButton(text="🔙 Отмена", callback_data="remove_kb")])
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.answer("Выберите категорию для лимита:", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.startswith("bud_cat_"))
async def budget_cat_selected(callback: types.CallbackQuery, state: FSMContext):
    cat_name = callback.data[8:] # "bud_cat_" len 8
    await state.update_data(cat_name=cat_name)
    await callback.message.answer(f"Введите лимит на месяц для категории **'{cat_name}'** (число):", parse_mode="Markdown")
    await state.set_state(BudgetStates.waiting_for_amount)
    await callback.answer()

@dp.message(BudgetStates.waiting_for_amount)
async def budget_amount_entered(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text.replace(' ', ''))
        data = await state.get_data()
        cat_name = data['cat_name']
        month_key = datetime.now().strftime("%Y-%m")
        
        with sqlite3.connect(DB_PATH) as conn:
            # Upsert (удалим старый, добавим новый - проще всего)
            conn.execute("DELETE FROM budgets WHERE user_id = ? AND category_name = ? AND month_year = ?",
                         (message.from_user.id, cat_name, month_key))
            conn.execute("INSERT INTO budgets (user_id, category_name, amount, month_year) VALUES (?, ?, ?, ?)",
                         (message.from_user.id, cat_name, amount, month_key))
            conn.commit()
            
        await message.answer(f"✅ Установлен бюджет **{amount:,.0f} р.** на *{cat_name}*.", parse_mode="Markdown")
        await state.clear()
    except ValueError:
        await message.answer("Введите корректное число.")

# -- Список и проверка --
@dp.callback_query(F.data == "budget_list")
async def budget_list_view(callback: types.CallbackQuery):
    month_key = datetime.now().strftime("%Y-%m")
    month_start = datetime.now().strftime("%Y-%m-01")
    
    with sqlite3.connect(DB_PATH) as conn:
        # Ваши бюджеты
        budgets = conn.execute("SELECT category_name, amount FROM budgets WHERE user_id = ? AND month_year = ?", 
                               (callback.from_user.id, month_key)).fetchall()
        
        # Ваши траты по этим категориям
        stats = conn.execute("""
            SELECT category, SUM(amount) 
            FROM transactions 
            WHERE user_id = ? AND date >= ? AND type = 'expense'
            GROUP BY category
        """, (callback.from_user.id, month_start)).fetchall()
    
    if not budgets:
        await callback.message.answer("Бюджеты на этот месяц не установлены.")
        await callback.answer()
        return
        
    stats_dict = {r[0]: r[1] for r in stats}
    
    msg = f"📊 **Бюджеты на {datetime.now().strftime('%B')}:**\n\n"
    
    for cat, limit in budgets:
        spent = stats_dict.get(cat, 0)
        percent = spent / limit if limit > 0 else 0
        
        # Визуал
        if percent > 1:
            icon = "🔴"
            status = f"ПРЕВЫШЕНИЕ на {spent - limit:,.0f} р.!"
        elif percent > 0.8:
            icon = "🟠"
            status = "Осталось немного"
        else:
            icon = "🟢"
            status = "В рамках"
            
        bar_len = 8
        filled = int(min(percent, 1) * bar_len)
        bar = "█" * filled + "░" * (bar_len - filled)
        
        msg += f"**{cat}** {icon}\n{bar} {int(percent*100)}%\n💸 {spent:,.0f} / {limit:,.0f} р.\n_{status}_\n\n"

    await callback.message.answer(msg, parse_mode="Markdown")
    await callback.answer()

# --- 7. ОТЧЕТЫ (WEB APP) ---

@dp.message(F.text == "📈 Отчеты")
async def reports_menu(message: types.Message):
    # По умолчанию текущий месяц
    now = datetime.now()
    text, markup = await generate_report_response(message.from_user.id, now.year, now.month)
    await message.answer(text, reply_markup=markup, parse_mode="Markdown")

@dp.callback_query(F.data.startswith("report_nav_"))
async def report_navigate(callback: types.CallbackQuery):
    # report_nav_2023_10
    parts = callback.data.split("_")
    year, month = int(parts[2]), int(parts[3])
    
    text, markup = await generate_report_response(callback.from_user.id, year, month)
    
    try:
        await callback.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")
    except Exception:
        pass # Если текст не изменился (редкий кейс)
    await callback.answer()

async def generate_report_response(user_id, year, month):
    # Начало и конец месяца
    month_str = f"{year}-{month:02d}"
    start_date = f"{month_str}-01"
    
    # След месяц для query (чтобы взять < next_start)
    if month == 12:
        next_start = f"{year+1}-01-01"
    else:
        next_start = f"{year}-{month+1:02d}-01"
        
    with sqlite3.connect(DB_PATH) as conn:
        # 1. Общие цифры
        summ = conn.execute("""
            SELECT type, SUM(amount)
            FROM transactions
            WHERE user_id = ? AND date >= ? AND date < ?
            GROUP BY type
        """, (user_id, start_date, next_start)).fetchall()
        
        # 2. По категориям (расходы) - Топ 5
        cats = conn.execute("""
            SELECT category, SUM(amount)
            FROM transactions
            WHERE user_id = ? AND date >= ? AND date < ? AND type = 'expense'
            GROUP BY category
            ORDER BY SUM(amount) DESC
            LIMIT 5
        """, (user_id, start_date, next_start)).fetchall()
        
        # Для JSON берем все
        cats_all = conn.execute("""
            SELECT category, SUM(amount)
            FROM transactions
            WHERE user_id = ? AND date >= ? AND date < ? AND type = 'expense'
            GROUP BY category
        """, (user_id, start_date, next_start)).fetchall()
        
        # Бюджеты и цели не зависят от месяца жестко, но бюджеты привязаны к месяцу.
        # Покажем бюджеты именно этого месяца
        month_key = f"{year}-{month:02d}"
        budgets = conn.execute("SELECT category_name, amount FROM budgets WHERE user_id = ? AND month_year = ?", 
                               (user_id, month_key)).fetchall()
        
        current_goals = conn.execute("SELECT name, current_amount, target_amount FROM goals WHERE user_id = ?", 
                             (user_id,)).fetchall()

    summary = {r[0]: r[1] for r in summ}
    total_income = summary.get('income', 0)
    total_expense = summary.get('expense', 0)
    balance = total_income - total_expense
    
    # Имя месяца
    month_name = datetime(year, month, 1).strftime("%B %Y")
    
    msg = f"📊 **Отчет за {month_name}**\n\n"
    msg += f"💰 **Баланс:** {balance:,.0f} р.\n"
    msg += f"📈 Доход: {total_income:,.0f} р.\n"
    msg += f"📉 Расход: {total_expense:,.0f} р.\n\n"
    
    if cats:
        msg += "**🏆 Топ-5 расходов:**\n"
        for name, amount in cats:
            msg += f"- {name}: {amount:,.0f} р.\n"
        msg += "\n"
        
    if budgets:
        msg += "**⚖️ Бюджеты (в этом месяце):**\n"
        for name, limit in budgets:
             msg += f"- {name}: {limit:,.0f} р.\n"
        msg += "\n"
        
    if current_goals and (year == datetime.now().year and month == datetime.now().month):
        # Цели показываем только если смотрим текущий месяц, т.к. история целей не хранится (только текущее состояние)
        msg += "**🎯 Цели (сейчас):**\n"
        for name, curr, target in current_goals:
             percent = (curr / target * 100) if target > 0 else 0
             msg += f"- {name}: {curr:,.0f} / {target:,.0f} ({percent:.0f}%)\n"

    # JSON for WebApp - передаём полные данные транзакций для выбранного месяца
    # Получаем транзакции за выбранный месяц
    with sqlite3.connect(DB_PATH) as conn:
        tx_rows = conn.execute("""
            SELECT id, amount, category, type, date, COALESCE(description, '') 
            FROM transactions 
            WHERE user_id = ? AND date >= ? AND date < ?
            ORDER BY id DESC
        """, (user_id, start_date, next_start)).fetchall()
        
        tx = [{"i": r[0], "a": int(r[1]), "c": r[2], "t": (1 if r[3] == "expense" else 0), "d": r[4][5:16], "ds": r[5]} for r in tx_rows]
    
    report_data = {
        'uid': user_id,
        'tx': tx,
        'g': [],  # Цели не нужны для отчёта
        'b': [],  # Бюджеты не нужны для отчёта
        'c': {"expense": [], "income": []},
        's': {'i': int(total_income), 'e': int(total_expense)},
        'm': month_name,
        'tab': 'reports'  # Автопереход на вкладку отчётов
    }
    json_str = json.dumps(report_data)
    b64_data = base64.urlsafe_b64encode(json_str.encode()).decode()
    report_url = f"{WEB_APP_URL}?data={b64_data}"
    
    # Кнопки навигации
    # Prev:
    if month == 1:
        prev_y, prev_m = year - 1, 12
    else:
        prev_y, prev_m = year, month - 1
        
    # Next:
    if month == 12:
        next_y, next_m = year + 1, 1
    else:
        next_y, next_m = year, month + 1
        
    now = datetime.now()
    # Не даем уйти в будущее дальше текущего месяца
    # (Хотя можно, но данных не будет)
    
    # Кнопки
    buttons = []
    # Верхний ряд: Навигация
    nav_row = [
        InlineKeyboardButton(text="⬅️", callback_data=f"report_nav_{prev_y}_{prev_m}"),
        InlineKeyboardButton(text=f"🗓 {month}/{year}", callback_data="ignore"),
        InlineKeyboardButton(text="➡️", callback_data=f"report_nav_{next_y}_{next_m}")
    ]
    buttons.append(nav_row)
    
    # Нижний ряд: Графики
    buttons.append([InlineKeyboardButton(text="📊 Открыть диаграммы", web_app=WebAppInfo(url=report_url))])
    
    return msg, InlineKeyboardMarkup(inline_keyboard=buttons)


@dp.message(F.text == "📱 Мои Деньги")
async def open_miniapp_handler(message: types.Message):
    # Генерация ссылки с данными
    payload = await get_miniapp_data(message.from_user.id)
    json_str = json.dumps(payload)
    # Сжатие? JSON может быть большим. Надеемся на 20 txs и base64.
    b64_data = base64.urlsafe_b64encode(json_str.encode()).decode()
    url = f"{WEB_APP_URL}?data={b64_data}"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 Открыть приложение", web_app=WebAppInfo(url=url))]
    ])
    await message.answer("Ваш финансовый пульт готов:", reply_markup=kb)

# --- УМНЫЙ ПАРСИНГ ---
import re
import random

FUNNY_RESPONSES = [
    "Опять траты? Ну ладно...",
    "Записал. Плакали твои денежки.",
    "Интересный выбор! (нет)",
    "Баланс худеет, а ты нет?",
    "Ок, босс. Минус в карму (и в кошелек).",
    "Найс! (но можно было сэкономить)",
]

FUNNY_INCOME_RESPONSES = [
    "О_О Деньги! Срочно тратить!",
    "Богач детектед.",
    "Плюс на счет, минус на совесть (шутка).",
    "Наконец-то пополнение!",
]

async def process_transaction_request(message: types.Message, state: FSMContext, amount, category_input, t_type, desc):
    # 1. Получаем список существующих категорий пользователя (из таблицы categories + из транзакций)
    with sqlite3.connect(DB_PATH) as conn:
        cats_db = conn.execute("SELECT name FROM categories WHERE user_id = ? AND type = ?", 
                               (message.from_user.id, t_type)).fetchall()
        cats_tx = conn.execute("SELECT DISTINCT category FROM transactions WHERE user_id = ? AND type = ?",
                               (message.from_user.id, t_type)).fetchall()
    
    # Объединяем, убираем дубли (с учетом регистра)
    existing_names_raw = list(set([c[0] for c in cats_db] + [c[0] for c in cats_tx]))
    
    # 2. Нечеткий поиск (Fuzzy match) - с нормализацией регистра
    category_input_lower = category_input.lower().strip()
    
    # Создаём маппинг lower -> original для точного совпадения
    name_map = {name.lower(): name for name in existing_names_raw}
    existing_names_lower = list(name_map.keys())
    
    # Точное совпадение (без учёта регистра)
    if category_input_lower in name_map:
        matched_category = name_map[category_input_lower]
    else:
        # Нечеткий поиск с cutoff=0.5 (позволяет 1-2 ошибки)
        matches = difflib.get_close_matches(category_input_lower, existing_names_lower, n=1, cutoff=0.5)
        if matches:
            matched_category = name_map[matches[0]]  # Получаем оригинальное имя с правильным регистром
        else:
            matched_category = None
    
    if matched_category:
        # Нашли совпадение! Используем существующую категорию
        save_transaction(message.from_user.id, amount, matched_category, t_type, desc)
        
        icon = "📉" if t_type == 'expense' else "📈"
        responses = FUNNY_RESPONSES if t_type == 'expense' else FUNNY_INCOME_RESPONSES
        resp = random.choice(responses)
        
        # Если было исправление, скажем об этом
        note = ""
        if matched_category.lower() != category_input.lower():
            note = f"\n(Исправил _'{category_input}'_ на **'{matched_category}'**)"
            
        caption = f"{resp}\n{icon} **{amount} р.**\nКатегория: {matched_category}{note}"
        if desc: caption += f"\nОписание: {desc}"
        
        # Проверка бюджета (шутливая)
        if t_type == 'expense':
            warning = check_budget_exceeded(message.from_user.id, matched_category, amount)
            if warning:
                caption += f"\n\n🚨 {warning}"

        await message.answer(caption, parse_mode="Markdown")
        return

    # 3. Совпадений нет. Спрашиваем пользователя/
    # Сохраняем данные транзакции во временное хранилище (state)
    await state.update_data(pending_tx={
        'amount': amount,
        'category_input': category_input,
        'type': t_type,
        'desc': desc
    })
    
    kb_buttons = [
        [InlineKeyboardButton(text=f"➕ Создать '{category_input}'", callback_data="tx_create_new")],
        [InlineKeyboardButton(text="📂 Выбрать из списка", callback_data="tx_choose_existing")]
    ]
    if existing_names:
        kb_buttons.append([InlineKeyboardButton(text="❌ Отмена", callback_data="tx_cancel")])
        
    kb = InlineKeyboardMarkup(inline_keyboard=kb_buttons)
    await message.answer(f"🤔 Категория **'{category_input}'** не найдена.\nЧто делаем?", reply_markup=kb, parse_mode="Markdown")
    await state.set_state(TransactionStates.waiting_for_decision)


async def parse_and_save(message: types.Message, state: FSMContext):
    text = message.text.strip()
    user_id = message.from_user.id
    
    # 1. Цели (!1000 Отпуск) - оставляем как есть, тут категории не нужны
    match_goal = re.match(r'^!(\d+)\s+(.+)', text)
    if match_goal:
        amount = float(match_goal.group(1))
        goal_name = match_goal.group(2).strip()
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, current_amount FROM goals WHERE user_id = ? AND name LIKE ?", (user_id, f"%{goal_name}%"))
            goal = cursor.fetchone()
            if goal:
                new_amount = goal[1] + amount
                cursor.execute("UPDATE goals SET current_amount = ? WHERE id = ?", (new_amount, goal[0]))
                await message.answer(f"🎯 **Цель '{goal_name}' пополнена!**\nБыло: {goal[1]}\nСтало: {new_amount}\nДобавлено: +{amount}")
            else:
                cursor.execute("INSERT INTO goals (user_id, name, target_amount, current_amount, created_at) VALUES (?, ?, ?, ?, ?)",
                               (user_id, goal_name, 0, amount, datetime.now().strftime("%Y-%m-%d")))
                await message.answer(f"🆕 **Новая цель '{goal_name}' создана!**\nНачало положено: {amount} р.")
            conn.commit()
        await update_user_menu_button(user_id) # UPDATE APP DATA
        return

    # 2. Доход (+1000 Зарплата)
    match_income = re.match(r'^\+(\d+)\s+(.+)', text)
    if match_income:
        amount = float(match_income.group(1))
        parts = match_income.group(2).strip().split(maxsplit=1)
        category_input = parts[0]
        desc = parts[1] if len(parts) > 1 else None
        
        # Запускаем процесс проверки
        await process_transaction_request(message, state, amount, category_input, 'income', desc)
        return

    # 3. Расход (1000 Продукты молоко)
    match_expense = re.match(r'^(\d+)\s+(.+)', text)
    if match_expense:
        amount = float(match_expense.group(1))
        parts = match_expense.group(2).strip().split(maxsplit=1)
        category_input = parts[0]
        desc = parts[1] if len(parts) > 1 else None
        
        # Запускаем процесс проверки
        await process_transaction_request(message, state, amount, category_input, 'expense', desc)
        return


# --- ОБРАБОТЧИКИ ВЫБОРА КАТЕГОРИИ ---

@dp.callback_query(F.data == "tx_create_new")
async def tx_create_new_cat(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    tx = data.get('pending_tx')
    if not tx:
        await callback.message.edit_text("Ошибка: данные устарели.")
        return

    # Создаем категорию
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("INSERT INTO categories (user_id, name, type, created_at) VALUES (?, ?, ?, ?)",
                     (callback.from_user.id, tx['category_input'], tx['type'], datetime.now().strftime("%Y-%m-%d")))
        conn.commit()
        
    # Сохраняем транзакцию
    save_transaction(callback.from_user.id, tx['amount'], tx['category_input'], tx['type'], tx['desc'])
    
    await callback.message.edit_text(f"✅ Создана категория **'{tx['category_input']}'** и добавлена запись:\n{tx['amount']} р.", parse_mode="Markdown")
    await update_user_menu_button(callback.from_user.id) # UPDATE APP DATA
    await state.clear()

@dp.callback_query(F.data == "tx_choose_existing")
async def tx_choose_start(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    tx = data.get('pending_tx')
    
    with sqlite3.connect(DB_PATH) as conn:
        cats = conn.execute("SELECT name FROM categories WHERE user_id = ? AND type = ?", 
                            (callback.from_user.id, tx['type'])).fetchall()
        
    buttons = []
    # Группируем по 2
    row = []
    for (name,) in cats:
        row.append(InlineKeyboardButton(text=name, callback_data=f"tx_sel_idx_{name}")) # Используем имя, риск длины, но просто
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row: buttons.append(row)
    
    buttons.append([InlineKeyboardButton(text="🔙 Отмена", callback_data="tx_cancel")])
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.edit_text("📂 Выберите категорию:", reply_markup=kb)

@dp.callback_query(F.data.startswith("tx_sel_idx_"))
async def tx_select_existing(callback: types.CallbackQuery, state: FSMContext):
    selected_cat = callback.data.split("tx_sel_idx_")[1] # Осторожно, если в имени _ . Но мы сплитим по первому вхождению префикса? Нет.
    # Лучше split("_", 3) если префикс фикс
    selected_cat = callback.data[11:] # "tx_sel_idx_" len is 11

    data = await state.get_data()
    tx = data.get('pending_tx')
    
    save_transaction(callback.from_user.id, tx['amount'], selected_cat, tx['type'], tx['desc'])
    
    await callback.message.edit_text(f"✅ Добавлено в **'{selected_cat}'**:\n{tx['amount']} р.", parse_mode="Markdown")
    await update_user_menu_button(callback.from_user.id) # UPDATE APP DATA
    await state.clear()
    
@dp.callback_query(F.data == "tx_cancel")
async def tx_cancel(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.delete()
    await state.clear()


# Подключаем этот обработчик ко всем текстовым сообщениям (кроме команд)
@dp.message(F.text & ~F.text.startswith('/'))
async def text_handler(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state:
        logging.info(f"DEBUG: Text handler skipped because of active state: {current_state}")
        return

    if message.text in ["💰 Баланс", "📊 Мой Баланс", "Баланс", "📋 История", "🎯 Цели", "📂 Категории", "📊 Бюджеты", "📈 Отчеты", "📋 Транзакции"]:
        return 
    
    await parse_and_save(message, state) # PASS STATE HERE
    # Если ничего не подошло - игнорируем (или можно сказать "не понял", но лучше не бесить)


# Note: Removed duplicate text_handler, the one above is used


def check_budget_exceeded(user_id, category_name, current_amount):
    month_key = datetime.now().strftime("%Y-%m")
    month_start = datetime.now().strftime("%Y-%m-01")
    
    with sqlite3.connect(DB_PATH) as conn:
        # 1. Получаем бюджет
        budget_row = conn.execute("SELECT amount FROM budgets WHERE user_id = ? AND category_name = ? AND month_year = ?", 
                              (user_id, category_name, month_key)).fetchone()
        if not budget_row:
            return None # Нет бюджета - нет проблем
            
        limit = budget_row[0]
        
        # 2. Получаем сумму трат (включая только что добавленную? save_transaction уже сработал)
        spent_row = conn.execute("SELECT SUM(amount) FROM transactions WHERE user_id = ? AND category = ? AND date >= ? AND type = 'expense'",
                                 (user_id, category_name, month_start)).fetchone()
        spent = spent_row[0] if spent_row and spent_row[0] else 0

    if spent > limit:
        # Проверим, было ли превышение ДО этой транзакции?
        # Если (spent - current_amount) <= limit < spent -> значит только что превысили
        prev_spent = spent - current_amount
        if prev_spent <= limit:
            return random.choice([
                "АЛАРМ! Бюджет пробит! 😱",
                "Кто-то слишком много кушает... 🍞",
                "Бюджет: 'Я устал, я ухожу...'",
                "Остановись, безумец! Лимит исчерпан!",
                "Поздравляю, вы банкрот в этой категории! 🎉"
            ])
        else:
             return None # Уже было превышено, не спамим каждый раз
    return None

# --- 8. FULL MINI APP SUPPORT (DYNAMIC MENU BUTTON) ---

async def update_user_menu_button(user_id):
    """
    Updates the native Menu Button for the user with a dynamic URL containing their latest data.
    """
    try:
        # Generate Payload
        # Limit to 10 transactions to keep URL short (< 2KB safety)
        payload = await get_miniapp_data(user_id, limit=10)
        json_str = json.dumps(payload)
        b64_data = base64.urlsafe_b64encode(json_str.encode()).decode()
        # Add timestamp to force cache invalidation
        ts = int(datetime.now().timestamp())
        url = f"{WEB_APP_URL}?data={b64_data}&v={ts}"
        
        # Update Button
        await bot.set_chat_menu_button(
            chat_id=user_id,
            menu_button=types.MenuButtonWebApp(text="📱 Мои Деньги", web_app=WebAppInfo(url=url))
        )
    except Exception as e:
        logging.error(f"Failed to update menu button for {user_id}: {e}")

async def get_miniapp_data(user_id, limit=15):
    month_start = datetime.now().strftime("%Y-%m-01")
    month_key = datetime.now().strftime("%Y-%m")
    
    with sqlite3.connect(DB_PATH) as conn:
        # 1. Transactions
        tx_rows = conn.execute("""
            SELECT id, amount, category, type, date, description 
            FROM transactions 
            WHERE user_id = ? 
            ORDER BY id DESC LIMIT ?
        """, (user_id, limit)).fetchall()
        
        # Short keys: i=id, a=amount, c=cat, t=type(0=inc,1=exp), d=date
        # Optimize size: "2023-10-15 12:00:00" -> "15 Oct" handling in JS? 
        # For now keep full date but maybe truncated?
        tx = [{"i": r[0], "a": int(r[1]), "c": r[2], "t": (1 if r[3] == "expense" else 0), "d": r[4][5:16], "ds": r[5]} for r in tx_rows]
        
        # 2. Goals
        goals_rows = conn.execute("SELECT id, name, current_amount, target_amount FROM goals WHERE user_id = ?", (user_id,)).fetchall()
        goals = [{"i": r[0], "n": r[1], "c": int(r[2]), "t": int(r[3])} for r in goals_rows]
        
        # 3. Budgets
        bud_rows = conn.execute("SELECT category_name, amount FROM budgets WHERE user_id = ? AND month_year = ?", (user_id, month_key)).fetchall()
        buds = {r[0]: r[1] for r in bud_rows}
        
        # 4. Categories - включаем и из таблицы categories, и уникальные из транзакций
        # Разделяем по типу для удобства фильтрации в MiniApp
        cat_rows = conn.execute("SELECT DISTINCT name, type FROM categories WHERE user_id = ?", (user_id,)).fetchall()
        tx_cats = conn.execute("SELECT DISTINCT category, type FROM transactions WHERE user_id = ?", (user_id,)).fetchall()
        
        # Объединяем и разделяем по типу
        expense_cats = set()
        income_cats = set()
        for name, ctype in cat_rows:
            if ctype == 'expense':
                expense_cats.add(name)
            else:
                income_cats.add(name)
        for name, ctype in tx_cats:
            if ctype == 'expense':
                expense_cats.add(name)
            else:
                income_cats.add(name)
        
        cats = {
            "expense": sorted(list(expense_cats)),
            "income": sorted(list(income_cats))
        }
        
        # 5. Stats
        summ = conn.execute("SELECT type, SUM(amount) FROM transactions WHERE user_id = ? AND date >= ? GROUP BY type", (user_id, month_start)).fetchall()
        
        # Calc spent for budgets
        cat_spent_rows = conn.execute("SELECT category, SUM(amount) FROM transactions WHERE user_id = ? AND date >= ? AND type = 'expense' GROUP BY category", (user_id, month_start)).fetchall()
        cat_spent = {r[0]: r[1] for r in cat_spent_rows}
        
    summary = {r[0]: r[1] for r in summ}
    inc = summary.get('income', 0)
    exp = summary.get('expense', 0)
    
    budgets_list = []
    # Merge budget info
    all_bud_cats = set(buds.keys()) | set(cat_spent.keys())
    for c in all_bud_cats:
        l = int(buds.get(c, 0))
        s = int(cat_spent.get(c, 0))
        if l > 0 or s > 0:
             budgets_list.append({"n": c, "l": l, "s": s})
    
    payload = {
        "uid": user_id,  # User ID для API запросов
        "tx": tx,
        "g": goals,
        "b": budgets_list,
        "c": cats,
        "s": {"i": int(inc), "e": int(exp)}, # bal calculated on client
        "m": datetime.now().strftime("%B")
    }
    return payload

@dp.message(F.web_app_data)
async def web_app_data_handler(message: types.Message):
    try:
        logging.info(f"DEBUG: WEB APP DATA RECEIVED: {message.web_app_data.data}")
        data = json.loads(message.web_app_data.data)
        action = data.get('action')
        uid = message.from_user.id
        
        resp_text = "✅ Данные обновлены"
        
        with sqlite3.connect(DB_PATH) as conn:
            if action == "add_tx":
                t_type = data.get('t')   # income/expense
                amount = float(data.get('a'))
                cat = data.get('c')
                desc = data.get('d', '')
                date_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                conn.execute("INSERT INTO transactions (user_id, amount, category, type, date, description) VALUES (?, ?, ?, ?, ?, ?)",
                             (uid, amount, cat, t_type, date_str, desc))
                
                resp_text = f"✅ Добавлено: {amount} р. ({cat})"
                if t_type == "expense":
                    w = check_budget_exceeded(uid, cat, amount)
                    if w: resp_text += f"\n\n🚨 {w}"
                    
            elif action == "add_goal":
                name = data.get('n')
                target = float(data.get('t'))
                conn.execute("INSERT INTO goals (user_id, name, target_amount, current_amount, created_at) VALUES (?, ?, ?, 0, ?)",
                             (uid, name, target, datetime.now().strftime("%Y-%m-%d")))
                resp_text = f"🎯 Цель '{name}' создана!"
                
            elif action == "add_budget":
                cat = data.get('c')
                limit = float(data.get('l'))
                m_key = datetime.now().strftime("%Y-%m")
                conn.execute("DELETE FROM budgets WHERE user_id = ? AND category_name = ? AND month_year = ?", (uid, cat, m_key))
                conn.execute("INSERT INTO budgets (user_id, category_name, amount, month_year) VALUES (?, ?, ?, ?)", (uid, cat, limit, m_key))
                resp_text = f"⚖️ Бюджет на '{cat}' установлен!"

            elif action == "top_up_goal":
                gid = data.get('id')
                amount = float(data.get('a'))
                conn.execute("UPDATE goals SET current_amount = current_amount + ? WHERE id = ? AND user_id = ?", (amount, gid, uid))
                resp_text = f"💰 Копилка пополнена на {amount} р.!"

            conn.commit()
            logging.info(f"Transaction committed successfully for user {uid}, action: {action}")
        
        # Update Menu Button (Critical!)
        await update_user_menu_button(uid)
        
        # Just notify user
        await message.answer(resp_text)
        
    except Exception as e:
        logging.error(f"WebApp Error: {e}")
        await message.answer("Ошибка обработки данных приложения.")

# --- ЗАПУСК ---

@dp.message(Command("reset_all_data_secret"))
async def secret_reset_data(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM transactions WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM goals WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM categories WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM budgets WHERE user_id = ?", (user_id,))
        conn.commit()
    
    await state.clear()
    await update_user_menu_button(user_id) # Reset app state too
    await message.answer("💥 **ПОЛНЫЙ СБРОС ВЫПОЛНЕН**\nВсе ваши категории, транзакции, цели и бюджеты удалены.\n\nЖмите /start для начала новой жизни.", parse_mode="Markdown")


async def main():
    init_db()

    # Настройка API сервера (aiohttp)
    app = web.Application()
    app.router.add_route('*', '/api/action', handle_api_action)  # Новый универсальный эндпоинт
    runner = web.AppRunner(app)
    await runner.setup()

    # Слушаем на всех интерфейсах (0.0.0.0)
    api_site = web.TCPSite(runner, '0.0.0.0', 8080)
    await api_site.start()

    logging.info("API server started on port 8080")

    # Сброс вебхука перед запуском polling
    await bot.delete_webhook(drop_pending_updates=True)

    # Запуск бота (polling)
    await dp.start_polling(bot)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped")