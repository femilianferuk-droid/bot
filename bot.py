"""
Telegram Bot для массовых рассылок с использованием aiogram 3.x и Telethon
Структура:
- Главное меню с тремя разделами
- Менеджер аккаунтов (добавление, список)
- Система рассылок с выбором чатов, настройкой параметров и выполнением
- Остановка рассылок в реальном времени
"""

import asyncio
import os
import logging
from typing import List, Dict, Optional
from datetime import datetime
import sqlite3
from contextlib import contextmanager

from aiogram import Bot, Dispatcher, types, F
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, 
    InlineKeyboardButton, Message, CallbackQuery
)
from aiogram.filters import Command, StateFilter
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from aiogram.enums import ParseMode

from telethon import TelegramClient, errors
from telethon.sessions import StringSession
from telethon.tl.types import User, Chat, Channel

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация
API_ID = 32480523
API_HASH = "147839735c9fa4e83451209e9b55cfc5"
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise ValueError("Не установлена переменная окружения BOT_TOKEN")

# ID премиум эмодзи
EMOJI = {
    "settings": "5870982283724328568",
    "profile": "5870994129244131212",
    "people": "5870772616305839506",
    "person_check": "5891207662678317861",
    "person_cross": "5893192487324880883",
    "file": "5870528606328852614",
    "smile": "5870764288364252592",
    "chart_growth": "5870930636742595124",
    "chart_stats": "5870921681735781843",
    "home": "5873147866364514353",
    "lock_closed": "6037249452824072506",
    "lock_open": "6037496202990194718",
    "megaphone": "6039422865189638057",
    "check": "5870633910337015697",
    "cross": "5870657884844462243",
    "pencil": "5870676941614354370",
    "trash": "5870875489362513438",
    "down": "5893057118545646106",
    "paperclip": "6039451237743595514",
    "link": "5769289093221454192",
    "info": "6028435952299413210",
    "bot": "6030400221232501136",
    "eye": "6037397706505195857",
    "eye_hidden": "6037243349675544634",
    "send": "5963103826075456248",
    "download": "6039802767931871481",
    "notification": "6039486778597970865",
    "gift": "6032644646587338669",
    "clock": "5983150113483134607",
    "celebration": "6041731551845159060",
    "font": "5870801517140775623",
    "write": "5870753782874246579",
    "media": "6035128606563241721",
    "geo": "6042011682497106307",
    "wallet": "5769126056262898415",
    "box": "5884479287171485878",
    "crypto_bot": "5260752406890711732",
    "calendar": "5890937706803894250",
    "tag": "5886285355279193209",
    "time_past": "5775896410780079073",
    "apps": "5778672437122045013",
    "brush": "6050679691004612757",
    "add_text": "5771851822897566479",
    "format": "5778479949572738874",
    "money": "5904462880941545555",
    "send_money": "5890848474563352982",
    "accept_money": "5879814368572478751",
    "code": "5940433880585605708",
    "loading": "5345906554510012647",
    "back": "5345906554510012647",
    "subscribe": "6039450962865688331",
    "check_sub": "5774022692642492953",
    "broadcast": "5370599459661045441",
    "star": "5870633910337015697",
    "plus": "5870633910337015697",
    "list": "5870772616305839506",
    "rocket": "5963103826075456248",
    "stop": "5870657884844462243",
    "stats": "5870921681735781843",
    "forward": "5893057118545646106"
}

# Инициализация БД
DB_NAME = "bot_database.db"

# Глобальный словарь для хранения активных задач рассылок
active_mailing_tasks: Dict[int, asyncio.Task] = {}

# Глобальный словарь для хранения Telethon клиентов в процессе авторизации
auth_clients: Dict[int, TelegramClient] = {}

# Инициализация клиентов
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Глобальный Telethon клиент
telethon_client: Optional[TelegramClient] = None
current_account_id: Optional[int] = None

# ========== Работа с БД ==========

class Database:
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.init_database()

    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_name)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def init_database(self):
        """Инициализация таблиц БД"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executescript("""
                CREATE TABLE IF NOT EXISTS accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    phone TEXT NOT NULL,
                    session_string TEXT NOT NULL,
                    has_2fa BOOLEAN DEFAULT 0,
                    is_active BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS mailings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id INTEGER,
                    chats TEXT NOT NULL,
                    delay INTEGER DEFAULT 30,
                    cycles INTEGER DEFAULT 10,
                    message_html TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    current_cycle INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (account_id) REFERENCES accounts(id)
                );
            """)
            conn.commit()

    def add_account(self, phone: str, session_string: str, has_2fa: bool = False):
        """Добавление нового аккаунта"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO accounts (phone, session_string, has_2fa) VALUES (?, ?, ?)",
                (phone, session_string, has_2fa)
            )
            conn.commit()
            return cursor.lastrowid

    def get_accounts(self) -> List[Dict]:
        """Получение списка всех аккаунтов"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM accounts ORDER BY created_at DESC")
            return [dict(row) for row in cursor.fetchall()]

    def get_account_by_id(self, account_id: int) -> Optional[Dict]:
        """Получение аккаунта по ID"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM accounts WHERE id = ?", (account_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def set_active_account(self, account_id: int):
        """Установка активного аккаунта"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE accounts SET is_active = 0")
            cursor.execute("UPDATE accounts SET is_active = 1 WHERE id = ?", (account_id,))
            conn.commit()

    def get_active_account(self) -> Optional[Dict]:
        """Получение активного аккаунта"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM accounts WHERE is_active = 1")
            row = cursor.fetchone()
            return dict(row) if row else None

    def add_mailing(self, account_id: int, chats: List[str], delay: int, 
                   cycles: int, message_html: str) -> int:
        """Добавление новой рассылки"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO mailings (account_id, chats, delay, cycles, message_html, status) "
                "VALUES (?, ?, ?, ?, ?, 'running')",
                (account_id, ",".join(chats), delay, cycles, message_html)
            )
            conn.commit()
            return cursor.lastrowid

    def update_mailing_status(self, mailing_id: int, status: str, current_cycle: int = None):
        """Обновление статуса рассылки"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if current_cycle is not None:
                cursor.execute(
                    "UPDATE mailings SET status = ?, current_cycle = ? WHERE id = ?",
                    (status, current_cycle, mailing_id)
                )
            else:
                cursor.execute(
                    "UPDATE mailings SET status = ? WHERE id = ?",
                    (status, mailing_id)
                )
            conn.commit()

    def get_mailing_by_id(self, mailing_id: int) -> Optional[Dict]:
        """Получение рассылки по ID"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM mailings WHERE id = ?", (mailing_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

db = Database(DB_NAME)

# ========== Состояния FSM ==========

class AddAccount(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_2fa = State()

class MailingSetup(StatesGroup):
    selecting_chats = State()
    waiting_for_delay = State()
    waiting_for_cycles = State()
    waiting_for_message = State()
    confirming = State()

# ========== Клавиатуры ==========

def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Главное меню"""
    return {
        "keyboard": [
            [
                {
                    "text": "Менеджер аккаунтов",
                    "icon_custom_emoji_id": EMOJI["profile"]
                },
                {
                    "text": "Функции",
                    "icon_custom_emoji_id": EMOJI["settings"]
                }
            ],
            [
                {
                    "text": "Рассылка",
                    "icon_custom_emoji_id": EMOJI["send"]
                },
                {
                    "text": "Остановить рассылку",
                    "icon_custom_emoji_id": EMOJI["stop"]
                }
            ]
        ],
        "resize_keyboard": True
    }

def get_account_manager_keyboard() -> ReplyKeyboardMarkup:
    """Меню менеджера аккаунтов"""
    return {
        "keyboard": [
            [
                {
                    "text": "Добавить аккаунт",
                    "icon_custom_emoji_id": EMOJI["plus"]
                },
                {
                    "text": "Список аккаунтов",
                    "icon_custom_emoji_id": EMOJI["list"]
                }
            ],
            [
                {
                    "text": "Назад",
                    "icon_custom_emoji_id": EMOJI["back"]
                }
            ]
        ],
        "resize_keyboard": True
    }

def get_mailing_keyboard() -> ReplyKeyboardMarkup:
    """Меню рассылки"""
    return {
        "keyboard": [
            [
                {
                    "text": "Запустить рассылку",
                    "icon_custom_emoji_id": EMOJI["rocket"]
                },
                {
                    "text": "Статус рассылки",
                    "icon_custom_emoji_id": EMOJI["stats"]
                }
            ],
            [
                {
                    "text": "Назад",
                    "icon_custom_emoji_id": EMOJI["back"]
                }
            ]
        ],
        "resize_keyboard": True
    }

def get_back_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура с кнопкой назад"""
    return {
        "keyboard": [
            [
                {
                    "text": "Назад",
                    "icon_custom_emoji_id": EMOJI["back"]
                }
            ]
        ],
        "resize_keyboard": True
    }

# ========== Telethon функции ==========

async def create_telethon_client(session_string: str = None) -> TelegramClient:
    """Создание или восстановление Telethon клиента"""
    client = TelegramClient(
        StringSession(session_string) if session_string else StringSession(),
        API_ID,
        API_HASH,
        system_version="4.16.30-vxCUSTOM",
        device_model="Python Telethon",
        app_version="1.0.0"
    )
    await client.connect()
    return client

async def get_dialogs_from_client(client: TelegramClient) -> List[Dict]:
    """Получение списка диалогов из клиента"""
    dialogs = await client.get_dialogs()
    
    result = []
    for dialog in dialogs:
        entity = dialog.entity
        
        if isinstance(entity, User):
            name = f"{entity.first_name or ''} {entity.last_name or ''}".strip()
            if not name:
                name = f"User {entity.id}"
            chat_id = str(entity.id)
            chat_type = "user"
        elif isinstance(entity, Chat):
            name = entity.title or f"Chat {entity.id}"
            chat_id = str(entity.id)
            chat_type = "group"
        elif isinstance(entity, Channel):
            name = entity.title or f"Channel {entity.id}"
            chat_id = str(entity.id)
            chat_type = "channel"
        else:
            continue
        
        if hasattr(entity, 'username') and entity.username:
            name += f" (@{entity.username})"
        
        result.append({
            'id': chat_id,
            'name': name,
            'type': chat_type
        })
    
    return result

async def send_message_to_chat(client: TelegramClient, chat_id: str, message: str):
    """Отправка сообщения в чат"""
    try:
        entity = await client.get_entity(int(chat_id))
        await client.send_message(entity, message, parse_mode='HTML')
        return True
    except Exception as e:
        logger.error(f"Ошибка отправки в чат {chat_id}: {e}")
        return False

# ========== Обработчики команд ==========

@dp.message(Command("start"))
async def cmd_start(message: Message):
    """Обработчик команды /start"""
    await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["bot"]}">🤖</tg-emoji> Добро пожаловать в бот для массовых рассылок!</b>\n'
        'Выберите нужный раздел:',
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )

@dp.message(F.text == "Назад")
async def go_back(message: Message, state: FSMContext):
    """Возврат в главное меню"""
    current_state = await state.get_state()
    if current_state:
        # Очищаем клиент авторизации если есть
        user_id = message.from_user.id
        if user_id in auth_clients:
            try:
                await auth_clients[user_id].disconnect()
            except:
                pass
            del auth_clients[user_id]
        await state.clear()
    
    await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["home"]}">🏘</tg-emoji> Главное меню:</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )

@dp.message(F.text == "Менеджер аккаунтов")
async def account_manager(message: Message):
    """Менеджер аккаунтов"""
    await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["profile"]}">👤</tg-emoji> Менеджер аккаунтов</b>\n'
        'Выберите действие:',
        parse_mode=ParseMode.HTML,
        reply_markup=get_account_manager_keyboard()
    )

@dp.message(F.text == "Функции")
async def functions(message: Message):
    """Заглушка для функций"""
    await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["settings"]}">⚙</tg-emoji> Раздел в разработке</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )

@dp.message(F.text == "Рассылка")
async def mailing_menu(message: Message):
    """Меню рассылки"""
    active_account = db.get_active_account()
    if not active_account:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Нет активного аккаунта!</b> '
            'Сначала выберите аккаунт в менеджере.',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        return
    
    await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["send"]}">📨</tg-emoji> Меню рассылки</b>\n'
        f'Активный аккаунт: {active_account["phone"]}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_mailing_keyboard()
    )

# ========== Менеджер аккаунтов ==========

@dp.message(F.text == "Добавить аккаунт")
async def add_account_start(message: Message, state: FSMContext):
    """Начало добавления аккаунта"""
    await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["profile"]}">👤</tg-emoji> Введите номер телефона в международном формате</b>\n'
        'Например: +79991234567',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(AddAccount.waiting_for_phone)

@dp.message(AddAccount.waiting_for_phone)
async def process_phone(message: Message, state: FSMContext):
    """Обработка номера телефона"""
    phone = message.text.strip()
    user_id = message.from_user.id
    
    if not phone.startswith('+') or len(phone) < 10:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Неверный формат номера.</b> '
            'Введите номер в формате +79991234567',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        return
    
    try:
        # Создаем и сохраняем клиент для этого пользователя
        client = await create_telethon_client()
        auth_clients[user_id] = client
        
        # Отправляем запрос кода
        send_code_result = await client.send_code_request(phone)
        
        # Сохраняем данные в состоянии
        await state.update_data(
            phone=phone,
            phone_code_hash=send_code_result.phone_code_hash
        )
        
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["send"]}">📩</tg-emoji> Код подтверждения отправлен!</b>\n'
            'Введите код из сообщения Telegram:',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        await state.set_state(AddAccount.waiting_for_code)
        
    except errors.FloodWaitError as e:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["clock"]}">⏰</tg-emoji> Слишком много попыток!</b>\n'
            f'Подождите {e.seconds} секунд перед повторной попыткой.',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        await cleanup_auth_client(user_id)
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка при отправке кода: {e}")
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибка: {str(e)}</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        await cleanup_auth_client(user_id)
        await state.clear()

async def cleanup_auth_client(user_id: int):
    """Очистка клиента авторизации"""
    if user_id in auth_clients:
        try:
            await auth_clients[user_id].disconnect()
        except:
            pass
        del auth_clients[user_id]

@dp.message(AddAccount.waiting_for_code)
async def process_code(message: Message, state: FSMContext):
    """Обработка кода подтверждения"""
    code = message.text.strip()
    data = await state.get_data()
    user_id = message.from_user.id
    
    # Получаем сохраненный клиент
    client = auth_clients.get(user_id)
    if not client:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Сессия авторизации истекла.</b> '
            'Начните процесс заново.',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        await state.clear()
        return
    
    try:
        # Пытаемся войти с кодом
        try:
            await client.sign_in(
                phone=data['phone'],
                code=code,
                phone_code_hash=data['phone_code_hash']
            )
        except errors.SessionPasswordNeededError:
            # Нужен пароль 2FA
            await state.update_data(session_string=client.session.save())
            await message.answer(
                f'<b><tg-emoji emoji-id="{EMOJI["lock_closed"]}">🔒</tg-emoji> Требуется двухфакторная аутентификация!</b>\n'
                'Введите пароль 2FA:',
                parse_mode=ParseMode.HTML,
                reply_markup=get_back_keyboard()
            )
            await state.set_state(AddAccount.waiting_for_2fa)
            return
        
        # Успешный вход без 2FA
        session_string = client.session.save()
        account_id = db.add_account(data['phone'], session_string)
        
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["check"]}">✅</tg-emoji> Аккаунт успешно добавлен!</b>\n'
            f'Номер: {data["phone"]}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_account_manager_keyboard()
        )
        
        await cleanup_auth_client(user_id)
        await state.clear()
        
    except errors.PhoneCodeExpiredError:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Код подтверждения истек!</b>\n'
            'Запросите новый код, начав процесс заново.',
            parse_mode=ParseMode.HTML,
            reply_markup=get_account_manager_keyboard()
        )
        await cleanup_auth_client(user_id)
        await state.clear()
    except errors.PhoneCodeInvalidError:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Неверный код подтверждения!</b>\n'
            'Проверьте код и попробуйте снова.',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
    except Exception as e:
        logger.error(f"Ошибка при входе: {e}")
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибка: {str(e)}</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        await cleanup_auth_client(user_id)
        await state.clear()

@dp.message(AddAccount.waiting_for_2fa)
async def process_2fa(message: Message, state: FSMContext):
    """Обработка пароля 2FA"""
    password = message.text.strip()
    data = await state.get_data()
    user_id = message.from_user.id
    
    # Получаем сохраненный клиент
    client = auth_clients.get(user_id)
    if not client:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Сессия авторизации истекла.</b> '
            'Начните процесс заново.',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        await state.clear()
        return
    
    try:
        # Входим с паролем 2FA
        await client.sign_in(password=password)
        
        # Сохраняем сессию
        session_string = client.session.save()
        account_id = db.add_account(data['phone'], session_string, has_2fa=True)
        
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["check"]}">✅</tg-emoji> Аккаунт успешно добавлен!</b>\n'
            f'Номер: {data["phone"]} (с 2FA защитой)',
            parse_mode=ParseMode.HTML,
            reply_markup=get_account_manager_keyboard()
        )
        
        await cleanup_auth_client(user_id)
        await state.clear()
        
    except errors.PasswordHashInvalidError:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Неверный пароль 2FA!</b>\n'
            'Попробуйте снова.',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
    except Exception as e:
        logger.error(f"Ошибка при входе с 2FA: {e}")
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибка: {str(e)}</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        await cleanup_auth_client(user_id)
        await state.clear()

@dp.message(F.text == "Список аккаунтов")
async def list_accounts(message: Message):
    """Показ списка аккаунтов"""
    accounts = db.get_accounts()
    
    if not accounts:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["info"]}">ℹ</tg-emoji> Нет сохраненных аккаунтов</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_account_manager_keyboard()
        )
        return
    
    builder = InlineKeyboardBuilder()
    
    for account in accounts:
        status_text = "✅ " if account['is_active'] else ""
        button_text = f"{status_text}{account['phone']} {'(2FA)' if account['has_2fa'] else ''}"
        
        builder.row(InlineKeyboardButton(
            text=button_text,
            callback_data=f"select_account_{account['id']}"
        ))
    
    await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["list"]}">👥</tg-emoji> Список аккаунтов:</b>\n'
        'Нажмите на аккаунт, чтобы выбрать его:',
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )

@dp.callback_query(F.data.startswith("select_account_"))
async def select_account(callback: CallbackQuery):
    """Выбор аккаунта"""
    account_id = int(callback.data.split("_")[2])
    
    db.set_active_account(account_id)
    account = db.get_account_by_id(account_id)
    
    await callback.message.delete()
    await callback.message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["check"]}">✅</tg-emoji> Выбран аккаунт: {account["phone"]}</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_account_manager_keyboard()
    )
    await callback.answer()

# ========== Система рассылок ==========

@dp.message(F.text == "Запустить рассылку")
async def start_mailing(message: Message, state: FSMContext):
    """Начало настройки рассылки"""
    global telethon_client, current_account_id
    
    active_account = db.get_active_account()
    if not active_account:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Сначала выберите активный аккаунт в менеджере!</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        return
    
    try:
        telethon_client = await create_telethon_client(active_account['session_string'])
        
        if not await telethon_client.is_user_authorized():
            await message.answer(
                f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибка авторизации аккаунта.</b> '
                'Возможно, сессия устарела.',
                parse_mode=ParseMode.HTML,
                reply_markup=get_main_keyboard()
            )
            return
        
        current_account_id = active_account['id']
        
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["download"]}">📥</tg-emoji> Загружаем список чатов...</b>',
            parse_mode=ParseMode.HTML
        )
        
        dialogs = await get_dialogs_from_client(telethon_client)
        
        if not dialogs:
            await message.answer(
                f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Не удалось получить список чатов</b>',
                parse_mode=ParseMode.HTML,
                reply_markup=get_mailing_keyboard()
            )
            return
        
        await state.update_data(
            dialogs=dialogs,
            selected_chats=[],
            current_page=0
        )
        
        await state.set_state(MailingSetup.selecting_chats)
        await show_chats_page(message, state)
        
    except Exception as e:
        logger.error(f"Ошибка при инициализации рассылки: {e}")
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибка: {str(e)}</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_mailing_keyboard()
        )

async def show_chats_page(message: Message, state: FSMContext):
    """Показ страницы с чатами"""
    data = await state.get_data()
    dialogs = data['dialogs']
    selected_chats = data.get('selected_chats', [])
    current_page = data.get('current_page', 0)
    
    items_per_page = 10
    total_pages = max(1, (len(dialogs) + items_per_page - 1) // items_per_page)
    start_idx = current_page * items_per_page
    end_idx = min(start_idx + items_per_page, len(dialogs))
    
    builder = InlineKeyboardBuilder()
    
    for i in range(start_idx, end_idx):
        dialog = dialogs[i]
        is_selected = dialog['id'] in selected_chats
        prefix = "✅ " if is_selected else "⬜ "
        
        name = dialog['name'][:40] + "..." if len(dialog['name']) > 40 else dialog['name']
        
        builder.row(InlineKeyboardButton(
            text=f"{prefix}{name}",
            callback_data=f"toggle_chat_{dialog['id']}"
        ))
    
    nav_buttons = []
    if current_page > 0:
        nav_buttons.append(InlineKeyboardButton(
            text="◀️ Назад",
            callback_data=f"page_{current_page - 1}"
        ))
    
    nav_buttons.append(InlineKeyboardButton(
        text=f"📄 {current_page + 1}/{total_pages}",
        callback_data="ignore"
    ))
    
    if current_page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(
            text="Вперед ▶️",
            callback_data=f"page_{current_page + 1}"
        ))
    
    if nav_buttons:
        builder.row(*nav_buttons)
    
    if len(selected_chats) > 0:
        builder.row(InlineKeyboardButton(
            text=f"✅ Подтвердить выбор ({len(selected_chats)} чатов)",
            callback_data="confirm_chats"
        ))
    
    builder.row(InlineKeyboardButton(
        text="❌ Отмена",
        callback_data="cancel_mailing"
    ))
    
    text = (
        f'<b><tg-emoji emoji-id="{EMOJI["list"]}">👥</tg-emoji> Выберите чаты для рассылки:</b>\n'
        f'Выбрано: {len(selected_chats)} из 50 макс.\n'
        f'Страница {current_page + 1} из {total_pages}'
    )
    
    if 'chat_message_id' in data:
        try:
            await message.bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=data['chat_message_id'],
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=builder.as_markup()
            )
        except:
            msg = await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=builder.as_markup())
            await state.update_data(chat_message_id=msg.message_id)
    else:
        msg = await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=builder.as_markup())
        await state.update_data(chat_message_id=msg.message_id)

@dp.callback_query(StateFilter(MailingSetup.selecting_chats), F.data.startswith("toggle_chat_"))
async def toggle_chat_selection(callback: CallbackQuery, state: FSMContext):
    """Переключение выбора чата"""
    chat_id = callback.data.split("_")[2]
    data = await state.get_data()
    selected_chats = data.get('selected_chats', [])
    
    if chat_id in selected_chats:
        selected_chats.remove(chat_id)
    else:
        if len(selected_chats) < 50:
            selected_chats.append(chat_id)
        else:
            await callback.answer("Максимум 50 чатов!", show_alert=True)
            return
    
    await state.update_data(selected_chats=selected_chats)
    await show_chats_page(callback.message, state)
    await callback.answer()

@dp.callback_query(StateFilter(MailingSetup.selecting_chats), F.data.startswith("page_"))
async def change_page(callback: CallbackQuery, state: FSMContext):
    """Смена страницы с чатами"""
    page = int(callback.data.split("_")[1])
    await state.update_data(current_page=page)
    await show_chats_page(callback.message, state)
    await callback.answer()

@dp.callback_query(StateFilter(MailingSetup.selecting_chats), F.data == "confirm_chats")
async def confirm_chat_selection(callback: CallbackQuery, state: FSMContext):
    """Подтверждение выбора чатов"""
    data = await state.get_data()
    selected_chats = data['selected_chats']
    
    if not selected_chats:
        await callback.answer("Выберите хотя бы один чат!", show_alert=True)
        return
    
    await callback.message.delete()
    await callback.message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["check"]}">✅</tg-emoji> Выбрано чатов: {len(selected_chats)}</b>\n'
        'Введите задержку между циклами (в секундах, по умолчанию 30):',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(MailingSetup.waiting_for_delay)
    await callback.answer()

@dp.callback_query(StateFilter(MailingSetup.selecting_chats), F.data == "cancel_mailing")
async def cancel_mailing_setup(callback: CallbackQuery, state: FSMContext):
    """Отмена настройки рассылки"""
    await callback.message.delete()
    await callback.message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Рассылка отменена</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_mailing_keyboard()
    )
    await state.clear()
    await callback.answer()

@dp.message(MailingSetup.waiting_for_delay)
async def process_delay(message: Message, state: FSMContext):
    """Обработка задержки"""
    delay_text = message.text.strip()
    
    try:
        delay = int(delay_text) if delay_text else 30
        if delay < 0:
            raise ValueError
    except ValueError:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Введите целое число (секунды)</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        return
    
    await state.update_data(delay=delay)
    await message.answer(
        '<b>Введите количество циклов (по умолчанию 10):</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(MailingSetup.waiting_for_cycles)

@dp.message(MailingSetup.waiting_for_cycles)
async def process_cycles(message: Message, state: FSMContext):
    """Обработка количества циклов"""
    cycles_text = message.text.strip()
    
    try:
        cycles = int(cycles_text) if cycles_text else 10
        if cycles < 1:
            raise ValueError
    except ValueError:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Введите целое положительное число</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        return
    
    await state.update_data(cycles=cycles)
    await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["write"]}">✍</tg-emoji> Введите текст сообщения в формате HTML:</b>\n'
        'Поддерживаются теги: <b>жирный</b>, <i>курсив</i>, <a href="...">ссылка</a>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(MailingSetup.waiting_for_message)

@dp.message(MailingSetup.waiting_for_message)
async def process_message_text(message: Message, state: FSMContext):
    """Обработка текста сообщения"""
    message_text = message.text.strip()
    
    if not message_text:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Сообщение не может быть пустым</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        return
    
    await state.update_data(message_html=message_text)
    
    data = await state.get_data()
    
    confirmation_text = (
        f'<b><tg-emoji emoji-id="{EMOJI["info"]}">ℹ</tg-emoji> Подтвердите рассылку:</b>\n'
        f'• Чатов: {len(data["selected_chats"])}\n'
        f'• Задержка: {data["delay"]} сек\n'
        f'• Циклов: {data["cycles"]}\n'
        f'• Сообщение:\n{data["message_html"]}'
    )
    
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="✅ Запустить",
            callback_data="start_mailing"
        ),
        InlineKeyboardButton(
            text="❌ Отмена",
            callback_data="cancel_mailing"
        )
    )
    
    await message.answer(confirmation_text, parse_mode=ParseMode.HTML, reply_markup=builder.as_markup())
    await state.set_state(MailingSetup.confirming)

@dp.callback_query(StateFilter(MailingSetup.confirming), F.data == "start_mailing")
async def start_mailing_process(callback: CallbackQuery, state: FSMContext):
    """Запуск процесса рассылки"""
    global telethon_client, current_account_id
    
    data = await state.get_data()
    await callback.message.delete()
    
    mailing_id = db.add_mailing(
        account_id=current_account_id,
        chats=data['selected_chats'],
        delay=data['delay'],
        cycles=data['cycles'],
        message_html=data['message_html']
    )
    
    task = asyncio.create_task(
        execute_mailing(
            bot=callback.bot,
            chat_id=callback.message.chat.id,
            mailing_id=mailing_id,
            client=telethon_client,
            chats=data['selected_chats'],
            delay=data['delay'],
            cycles=data['cycles'],
            message_html=data['message_html']
        )
    )
    
    active_mailing_tasks[mailing_id] = task
    
    await callback.message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["rocket"]}">🚀</tg-emoji> Рассылка #{mailing_id} запущена!</b>\n'
        f'Чатов: {len(data["selected_chats"])}\n'
        f'Циклов: {data["cycles"]}\n'
        f'Задержка: {data["delay"]} сек',
        parse_mode=ParseMode.HTML,
        reply_markup=get_mailing_keyboard()
    )
    
    await state.clear()
    await callback.answer()

@dp.callback_query(StateFilter(MailingSetup.confirming), F.data == "cancel_mailing")
async def cancel_mailing_confirm(callback: CallbackQuery, state: FSMContext):
    """Отмена рассылки на этапе подтверждения"""
    await callback.message.delete()
    await callback.message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Рассылка отменена</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_mailing_keyboard()
    )
    await state.clear()
    await callback.answer()

# ========== Выполнение рассылки ==========

async def execute_mailing(
    bot: Bot,
    chat_id: int,
    mailing_id: int,
    client: TelegramClient,
    chats: List[str],
    delay: int,
    cycles: int,
    message_html: str
):
    """Выполнение рассылки в фоновом режиме"""
    try:
        for cycle in range(1, cycles + 1):
            if mailing_id not in active_mailing_tasks:
                await bot.send_message(
                    chat_id,
                    f'<b><tg-emoji emoji-id="{EMOJI["stop"]}">🛑</tg-emoji> Рассылка #{mailing_id} остановлена пользователем</b>',
                    parse_mode=ParseMode.HTML
                )
                db.update_mailing_status(mailing_id, 'stopped')
                return
            
            db.update_mailing_status(mailing_id, 'running', current_cycle=cycle)
            
            await bot.send_message(
                chat_id,
                f'<b><tg-emoji emoji-id="{EMOJI["send"]}">📤</tg-emoji> Рассылка #{mailing_id}: '
                f'Цикл {cycle} из {cycles}</b>\n'
                f'Отправка в {len(chats)} чатов...',
                parse_mode=ParseMode.HTML
            )
            
            success_count = 0
            fail_count = 0
            
            for i, chat_id_to_send in enumerate(chats, 1):
                if mailing_id not in active_mailing_tasks:
                    break
                
                try:
                    success = await send_message_to_chat(client, chat_id_to_send, message_html)
                    if success:
                        success_count += 1
                    else:
                        fail_count += 1
                    
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"Ошибка отправки в чат {chat_id_to_send}: {e}")
                    fail_count += 1
            
            await bot.send_message(
                chat_id,
                f'<b><tg-emoji emoji-id="{EMOJI["stats"]}">📊</tg-emoji> Цикл {cycle} завершен:</b>\n'
                f'<tg-emoji emoji-id="{EMOJI["check"]}">✅</tg-emoji> Успешно: {success_count}\n'
                f'<tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибок: {fail_count}',
                parse_mode=ParseMode.HTML
            )
            
            if cycle < cycles and mailing_id in active_mailing_tasks:
                await bot.send_message(
                    chat_id,
                    f'<b><tg-emoji emoji-id="{EMOJI["clock"]}">⏰</tg-emoji> Ожидание {delay} секунд до следующего цикла...</b>',
                    parse_mode=ParseMode.HTML
                )
                
                for _ in range(delay):
                    if mailing_id not in active_mailing_tasks:
                        break
                    await asyncio.sleep(1)
        
        if mailing_id in active_mailing_tasks:
            await bot.send_message(
                chat_id,
                f'<b><tg-emoji emoji-id="{EMOJI["check"]}">✅</tg-emoji> Рассылка #{mailing_id} успешно завершена!</b>\n'
                f'Всего циклов: {cycles}',
                parse_mode=ParseMode.HTML
            )
            db.update_mailing_status(mailing_id, 'completed')
        
    except Exception as e:
        logger.error(f"Ошибка в рассылке #{mailing_id}: {e}")
        await bot.send_message(
            chat_id,
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибка в рассылке #{mailing_id}: {str(e)}</b>',
            parse_mode=ParseMode.HTML
        )
        db.update_mailing_status(mailing_id, 'error')
    finally:
        if mailing_id in active_mailing_tasks:
            del active_mailing_tasks[mailing_id]

# ========== Остановка рассылки ==========

@dp.message(F.text == "Остановить рассылку")
@dp.message(Command("stop_ml"))
async def stop_mailing(message: Message):
    """Остановка активной рассылки"""
    if not active_mailing_tasks:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["info"]}">ℹ</tg-emoji> Нет активных рассылок для остановки</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        return
    
    builder = InlineKeyboardBuilder()
    
    for mailing_id in list(active_mailing_tasks.keys()):
        mailing = db.get_mailing_by_id(mailing_id)
        if mailing:
            builder.row(InlineKeyboardButton(
                text=f"Остановить рассылку #{mailing_id} (цикл {mailing['current_cycle']})",
                callback_data=f"stop_mailing_{mailing_id}"
            ))
    
    builder.row(InlineKeyboardButton(
        text="❌ Отмена",
        callback_data="cancel_stop"
    ))
    
    await message.answer(
        '<b>Выберите рассылку для остановки:</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )

@dp.callback_query(F.data.startswith("stop_mailing_"))
async def confirm_stop_mailing(callback: CallbackQuery):
    """Подтверждение остановки рассылки"""
    mailing_id = int(callback.data.split("_")[2])
    
    if mailing_id in active_mailing_tasks:
        task = active_mailing_tasks[mailing_id]
        task.cancel()
        del active_mailing_tasks[mailing_id]
        
        await callback.message.delete()
        await callback.message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["stop"]}">🛑</tg-emoji> Рассылка #{mailing_id} остановлена</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
    else:
        await callback.answer("Рассылка уже завершена", show_alert=True)
    
    await callback.answer()

@dp.callback_query(F.data == "cancel_stop")
async def cancel_stop_mailing(callback: CallbackQuery):
    """Отмена остановки"""
    await callback.message.delete()
    await callback.answer()

# ========== Статус рассылки ==========

@dp.message(F.text == "Статус рассылки")
async def show_mailing_status(message: Message):
    """Показ статуса активной рассылки"""
    if not active_mailing_tasks:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["info"]}">ℹ</tg-emoji> Нет активных рассылок</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_mailing_keyboard()
        )
        return
    
    status_text = f'<b><tg-emoji emoji-id="{EMOJI["stats"]}">📊</tg-emoji> Статус активных рассылок:</b>\n\n'
    
    for mailing_id, task in active_mailing_tasks.items():
        mailing = db.get_mailing_by_id(mailing_id)
        if mailing:
            status_text += (
                f'📨 Рассылка #{mailing_id}\n'
                f'• Статус: {mailing["status"]}\n'
                f'• Цикл: {mailing["current_cycle"]} из {mailing["cycles"]}\n'
                f'• Чатов: {len(mailing["chats"].split(","))}\n'
                f'• Задержка: {mailing["delay"]} сек\n\n'
            )
    
    await message.answer(status_text, parse_mode=ParseMode.HTML, reply_markup=get_mailing_keyboard())

# ========== Обработчик для игнорирования ==========

@dp.callback_query(F.data == "ignore")
async def ignore_callback(callback: CallbackQuery):
    """Игнорирование некоторых callback-запросов"""
    await callback.answer()

# ========== Главная функция ==========

async def main():
    """Главная функция запуска бота"""
    try:
        logger.info("Запуск бота...")
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Ошибка при запуске: {e}")
        raise
    finally:
        if telethon_client:
            await telethon_client.disconnect()
        
        # Очищаем все клиенты авторизации
        for user_id, client in auth_clients.items():
            try:
                await client.disconnect()
            except:
                pass
        auth_clients.clear()

if __name__ == "__main__":
    print("=" * 50)
    print("Telegram Bot для массовых рассылок")
    print("=" * 50)
    print(f"API ID: {API_ID}")
    print(f"Токен бота: {'*' * 10}")
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nБот остановлен пользователем")
    except Exception as e:
        print(f"Критическая ошибка: {e}")
