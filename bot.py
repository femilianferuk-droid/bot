"""
Telegram Bot для массовых рассылок с PostgreSQL - Полная версия v5.4
- 2500+ строк оптимизированного кода
- Все функции с подробными комментариями
- Премиум эмодзи во всех элементах интерфейса
- Жирный шрифт во всех сообщениях бота
- Полная поддержка HTML форматирования
"""

import asyncio
import os
import logging
import re
import json
import tempfile
import base64
from typing import List, Dict, Optional, Any, Tuple, Union
from datetime import datetime, timedelta
import asyncpg

from aiogram import Bot, Dispatcher, types, F
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardButton, 
    Message, 
    CallbackQuery,
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardBuilder
)
from aiogram.filters import Command, StateFilter
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.enums import ParseMode, ChatAction
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

from telethon import TelegramClient, events, errors
from telethon.sessions import StringSession
from telethon.tl.types import (
    User, 
    Chat, 
    Channel, 
    Message as TelethonMessage,
    PeerUser,
    PeerChat,
    PeerChannel
)
from telethon.tl.functions.messages import (
    ImportChatInviteRequest,
    CheckChatInviteRequest,
    SendMessageRequest
)
from telethon.tl.functions.channels import (
    JoinChannelRequest,
    GetParticipantsRequest
)
from telethon.tl.functions.account import UpdateStatusRequest
from telethon.errors import (
    FloodWaitError,
    SessionPasswordNeededError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PasswordHashInvalidError,
    UserAlreadyParticipantError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    ChatAdminRequiredError,
    ChannelPrivateError
)

# ============================================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# КОНФИГУРАЦИЯ
# ============================================================

API_ID = 32480523
API_HASH = "147839735c9fa4e83451209e9b55cfc5"
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/telegram_bot")

if not BOT_TOKEN:
    raise ValueError("Не установлена переменная окружения BOT_TOKEN")

# ============================================================
# ID ПРЕМИУМ ЭМОДЗИ
# ============================================================

EMOJI = {
    # Основные
    "bot": "6030400221232501136",
    "home": "5873147866364514353",
    "profile": "5870994129244131212",
    "people": "5870772616305839506",
    "settings": "5870982283724328568",
    
    # Действия
    "send": "5963103826075456248",
    "download": "6039802767931871481",
    "join": "6039450962865688331",
    "reply": "6039422865189638057",
    "stop": "5870657884844462243",
    "rocket": "5963103826075456248",
    "back": "5345906554510012647",
    
    # Статусы
    "check": "5870633910337015697",
    "cross": "5870657884844462243",
    "info": "6028435952299413210",
    "warning": "6037249452824072506",
    "clock": "5983150113483134607",
    "celebration": "6041731551845159060",
    "stats": "5870921681735781843",
    
    # Интерфейс
    "plus": "5870633910337015697",
    "list": "5870772616305839506",
    "file": "5870528606328852614",
    "trash": "5870875489362513438",
    "write": "5870753782874246579",
    "lock": "6037249452824072506",
    "spam": "6037249452824072506",
    "notification": "6039486778597970865",
    
    # Дополнительные
    "folder": "5884479287171485878",
    "link": "5769289093221454192",
    "money": "5904462880941545555",
    "gift": "6032644646587338669",
    "calendar": "5890937706803894250",
    "tag": "5886285355279193209",
    "brush": "6050679691004612757",
    "code": "5940433880585605708",
    "loading": "5345906554510012647",
    "eye": "6037397706505195857",
    "eye_hidden": "6037243349675544634",
    "megaphone": "6039422865189638057",
    "pencil": "5870676941614354370",
    "paperclip": "6039451237743595514",
    "font": "5870801517140775623",
    "media": "6035128606563241721",
    "geo": "6042011682497106307",
    "wallet": "5769126056262898415",
    "box": "5884479287171485878",
    "crypto_bot": "5260752406890711732",
    "apps": "5778672437122045013",
    "add_text": "5771851822897566479",
    "format": "5778479949572738874",
    "send_money": "5890848474563352982",
    "accept_money": "5879814368572478751",
    "subscribe": "6039450962865688331",
    "check_sub": "5774022692642492953",
    "broadcast": "5370599459661045441",
    "star": "5870633910337015697",
    "forward": "5893057118545646106",
    "person_check": "5891207662678317861",
    "person_cross": "5893192487324880883",
    "lock_closed": "6037249452824072506",
    "lock_open": "6037496202990194718",
    "chart_growth": "5870930636742595124",
    "chart_stats": "5870921681735781843",
    "smile": "5870764288364252592",
}

# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ ЭМОДЗИ
# ============================================================

def em(key: str) -> str:
    """
    Возвращает HTML-тег премиум эмодзи по ключу.
    
    Args:
        key: Ключ эмодзи из словаря EMOJI
        
    Returns:
        str: HTML-тег с премиум эмодзи
        
    Example:
        >>> em("bot")
        '<tg-emoji emoji-id="6030400221232501136">...</tg-emoji>'
    """
    emoji_id = EMOJI.get(key, EMOJI["info"])
    return f'<tg-emoji emoji-id="{emoji_id}">...</tg-emoji>'

def bem(key: str, text: str) -> str:
    """
    Возвращает жирный текст с премиум эмодзи.
    
    Args:
        key: Ключ эмодзи
        text: Текст для отображения
        
    Returns:
        str: Жирный текст с эмодзи в HTML формате
    """
    return f'<b>{em(key)} {text}</b>'

def bi(text: str) -> str:
    """
    Возвращает курсивный текст.
    
    Args:
        text: Текст
        
    Returns:
        str: Курсивный текст в HTML формате
    """
    return f'<i>{text}</i>'

def bcode(text: str) -> str:
    """
    Возвращает текст в формате кода.
    
    Args:
        text: Текст
        
    Returns:
        str: Текст в теге code
    """
    return f'<code>{text}</code>'

# ============================================================
# ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ
# ============================================================

# Активные задачи рассылок
active_mailing_tasks: Dict[int, asyncio.Task] = {}

# Клиенты авторизации (временные)
auth_clients: Dict[int, TelegramClient] = {}

# Активные клиенты для рассылок (кеш)
active_clients: Dict[int, TelegramClient] = {}

# Клиенты автоответчиков
auto_reply_clients: Dict[int, TelegramClient] = {}

# Настройки автоответчиков
auto_reply_settings: Dict[int, Dict] = {}

# Пул подключений к БД
db_pool: Optional[asyncpg.Pool] = None

# ============================================================
# ИНИЦИАЛИЗАЦИЯ БОТА
# ============================================================

bot = Bot(
    token=BOT_TOKEN,
    parse_mode=ParseMode.HTML
)

storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ============================================================
# РАБОТА С БАЗОЙ ДАННЫХ POSTGRESQL
# ============================================================

class Database:
    """
    Класс для работы с PostgreSQL базой данных.
    Использует asyncpg для асинхронных операций.
    """
    
    # ========== Инициализация ==========
    
    @staticmethod
    async def init_pool() -> None:
        """
        Инициализация пула подключений к PostgreSQL.
        Создает пул и инициализирует таблицы.
        """
        global db_pool
        try:
            db_pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=2,
                max_size=10,
                command_timeout=60
            )
            await Database.init_tables()
            logger.info("Пул подключений к PostgreSQL создан")
        except Exception as e:
            logger.error(f"Ошибка подключения к PostgreSQL: {e}")
            raise

    @staticmethod
    async def init_tables() -> None:
        """
        Создание всех необходимых таблиц в базе данных.
        Использует IF NOT EXISTS для безопасного создания.
        """
        async with db_pool.acquire() as conn:
            # Таблица аккаунтов
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    id SERIAL PRIMARY KEY,
                    phone TEXT NOT NULL,
                    session_string TEXT NOT NULL,
                    has_2fa BOOLEAN DEFAULT FALSE,
                    is_active BOOLEAN DEFAULT FALSE,
                    last_used TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Таблица рассылок
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mailings (
                    id SERIAL PRIMARY KEY,
                    account_id INTEGER REFERENCES accounts(id) ON DELETE CASCADE,
                    chats TEXT NOT NULL,
                    delay INTEGER DEFAULT 30,
                    cycles INTEGER DEFAULT 10,
                    message_html TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    current_cycle INTEGER DEFAULT 0,
                    success_count INTEGER DEFAULT 0,
                    fail_count INTEGER DEFAULT 0,
                    notifications_enabled BOOLEAN DEFAULT TRUE,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    finished_at TIMESTAMP
                );
            """)
            
            # Таблица настроек пользователей
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS user_settings (
                    user_id BIGINT PRIMARY KEY,
                    notifications_enabled BOOLEAN DEFAULT TRUE,
                    language TEXT DEFAULT 'ru',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Таблица автоответчиков
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS auto_replies (
                    id SERIAL PRIMARY KEY,
                    account_id INTEGER REFERENCES accounts(id) ON DELETE CASCADE UNIQUE,
                    reply_text TEXT NOT NULL,
                    is_active BOOLEAN DEFAULT FALSE,
                    reply_count INTEGER DEFAULT 0,
                    last_reply_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Таблица логов рассылок
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS mailing_logs (
                    id SERIAL PRIMARY KEY,
                    mailing_id INTEGER REFERENCES mailings(id) ON DELETE CASCADE,
                    chat_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            logger.info("Таблицы базы данных инициализированы")

    # ========== Методы для аккаунтов ==========
    
    @staticmethod
    async def add_account(phone: str, session_string: str, has_2fa: bool = False) -> int:
        """
        Добавление нового аккаунта в базу данных.
        
        Args:
            phone: Номер телефона
            session_string: Строка сессии Telethon
            has_2fa: Флаг наличия двухфакторной аутентификации
            
        Returns:
            int: ID созданного аккаунта
        """
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "INSERT INTO accounts (phone, session_string, has_2fa) VALUES ($1, $2, $3) RETURNING id",
                phone, session_string, has_2fa
            )
            logger.info(f"Добавлен аккаунт: {phone}")
            return row['id']

    @staticmethod
    async def add_account_from_file(phone: str, session_string: str) -> int:
        """
        Добавление аккаунта из файла сессии.
        
        Args:
            phone: Номер телефона
            session_string: Строка сессии
            
        Returns:
            int: ID созданного аккаунта
        """
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "INSERT INTO accounts (phone, session_string, has_2fa) VALUES ($1, $2, FALSE) RETURNING id",
                phone, session_string
            )
            logger.info(f"Добавлен аккаунт из файла: {phone}")
            return row['id']

    @staticmethod
    async def get_accounts() -> List[Dict[str, Any]]:
        """
        Получение списка всех аккаунтов.
        
        Returns:
            List[Dict]: Список аккаунтов
        """
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM accounts ORDER BY created_at DESC"
            )
            return [dict(row) for row in rows]

    @staticmethod
    async def get_account_by_id(account_id: int) -> Optional[Dict[str, Any]]:
        """
        Получение аккаунта по ID.
        
        Args:
            account_id: ID аккаунта
            
        Returns:
            Optional[Dict]: Данные аккаунта или None
        """
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM accounts WHERE id = $1", account_id
            )
            return dict(row) if row else None

    @staticmethod
    async def delete_account(account_id: int) -> None:
        """
        Удаление аккаунта и связанных данных.
        
        Args:
            account_id: ID аккаунта
        """
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM auto_replies WHERE account_id = $1", account_id)
            await conn.execute("DELETE FROM accounts WHERE id = $1", account_id)
            logger.info(f"Удален аккаунт ID: {account_id}")

    @staticmethod
    async def update_account_activity(account_id: int) -> None:
        """
        Обновление времени последней активности аккаунта.
        
        Args:
            account_id: ID аккаунта
        """
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE accounts SET last_used = CURRENT_TIMESTAMP WHERE id = $1",
                account_id
            )

    # ========== Методы для автоответчиков ==========
    
    @staticmethod
    async def set_auto_reply(account_id: int, reply_text: str, is_active: bool = True) -> None:
        """
        Установка или обновление автоответчика для аккаунта.
        
        Args:
            account_id: ID аккаунта
            reply_text: Текст ответа
            is_active: Флаг активности
        """
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO auto_replies (account_id, reply_text, is_active) VALUES ($1, $2, $3) "
                "ON CONFLICT (account_id) DO UPDATE SET reply_text = $2, is_active = $3",
                account_id, reply_text, is_active
            )
            logger.info(f"Автоответчик установлен для аккаунта {account_id}")

    @staticmethod
    async def get_auto_reply(account_id: int) -> Optional[Dict[str, Any]]:
        """
        Получение активного автоответчика для аккаунта.
        
        Args:
            account_id: ID аккаунта
            
        Returns:
            Optional[Dict]: Данные автоответчика или None
        """
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM auto_replies WHERE account_id = $1 AND is_active = TRUE",
                account_id
            )
            return dict(row) if row else None

    @staticmethod
    async def toggle_auto_reply(account_id: int, is_active: bool) -> None:
        """
        Включение/выключение автоответчика.
        
        Args:
            account_id: ID аккаунта
            is_active: Новый статус
        """
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE auto_replies SET is_active = $1 WHERE account_id = $2",
                is_active, account_id
            )

    @staticmethod
    async def get_all_active_auto_replies() -> List[Dict[str, Any]]:
        """
        Получение всех активных автоответчиков.
        
        Returns:
            List[Dict]: Список активных автоответчиков с данными аккаунтов
        """
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT ar.*, a.phone, a.session_string FROM auto_replies ar "
                "JOIN accounts a ON ar.account_id = a.id WHERE ar.is_active = TRUE"
            )
            return [dict(row) for row in rows]

    @staticmethod
    async def increment_reply_count(account_id: int) -> None:
        """
        Увеличение счетчика ответов автоответчика.
        
        Args:
            account_id: ID аккаунта
        """
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE auto_replies SET reply_count = reply_count + 1, "
                "last_reply_at = CURRENT_TIMESTAMP WHERE account_id = $1",
                account_id
            )

    # ========== Методы для рассылок ==========
    
    @staticmethod
    async def add_mailing(
        account_id: int,
        chats: List[str],
        delay: int = 30,
        cycles: int = 10,
        message_html: str = "",
        notifications_enabled: bool = True
    ) -> int:
        """
        Создание новой рассылки.
        
        Args:
            account_id: ID аккаунта
            chats: Список ID чатов
            delay: Задержка между циклами
            cycles: Количество циклов
            message_html: Текст сообщения в HTML
            notifications_enabled: Включены ли уведомления
            
        Returns:
            int: ID созданной рассылки
        """
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "INSERT INTO mailings (account_id, chats, delay, cycles, message_html, status, notifications_enabled) "
                "VALUES ($1, $2, $3, $4, $5, 'running', $6) RETURNING id",
                account_id, ",".join(chats), delay, cycles, message_html, notifications_enabled
            )
            logger.info(f"Создана рассылка ID: {row['id']}")
            return row['id']

    @staticmethod
    async def update_mailing_status(
        mailing_id: int,
        status: str,
        current_cycle: Optional[int] = None,
        success_count: Optional[int] = None,
        fail_count: Optional[int] = None
    ) -> None:
        """
        Обновление статуса рассылки.
        
        Args:
            mailing_id: ID рассылки
            status: Новый статус
            current_cycle: Текущий цикл
            success_count: Количество успешных отправок
            fail_count: Количество ошибок
        """
        async with db_pool.acquire() as conn:
            query = "UPDATE mailings SET status = $1"
            params: List[Any] = [status]
            param_count = 2
            
            if current_cycle is not None:
                query += f", current_cycle = ${param_count}"
                params.append(current_cycle)
                param_count += 1
            
            if success_count is not None:
                query += f", success_count = ${param_count}"
                params.append(success_count)
                param_count += 1
            
            if fail_count is not None:
                query += f", fail_count = ${param_count}"
                params.append(fail_count)
                param_count += 1
            
            if status in ('completed', 'stopped', 'error'):
                query += ", finished_at = CURRENT_TIMESTAMP"
            
            query += f" WHERE id = ${param_count}"
            params.append(mailing_id)
            
            await conn.execute(query, *params)

    @staticmethod
    async def get_mailing_by_id(mailing_id: int) -> Optional[Dict[str, Any]]:
        """
        Получение рассылки по ID.
        
        Args:
            mailing_id: ID рассылки
            
        Returns:
            Optional[Dict]: Данные рассылки или None
        """
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM mailings WHERE id = $1", mailing_id
            )
            return dict(row) if row else None

    @staticmethod
    async def add_mailing_log(
        mailing_id: int,
        chat_id: str,
        status: str,
        error_message: Optional[str] = None
    ) -> None:
        """
        Добавление записи в лог рассылки.
        
        Args:
            mailing_id: ID рассылки
            chat_id: ID чата
            status: Статус отправки
            error_message: Сообщение об ошибке
        """
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO mailing_logs (mailing_id, chat_id, status, error_message) "
                "VALUES ($1, $2, $3, $4)",
                mailing_id, chat_id, status, error_message
            )

    # ========== Методы для настроек пользователей ==========
    
    @staticmethod
    async def get_user_settings(user_id: int) -> Dict[str, Any]:
        """
        Получение настроек пользователя.
        
        Args:
            user_id: ID пользователя
            
        Returns:
            Dict: Настройки пользователя
        """
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM user_settings WHERE user_id = $1", user_id
            )
            if not row:
                await conn.execute(
                    "INSERT INTO user_settings (user_id, notifications_enabled) VALUES ($1, TRUE)",
                    user_id
                )
                return {"user_id": user_id, "notifications_enabled": True}
            return dict(row)

    @staticmethod
    async def update_user_settings(user_id: int, notifications_enabled: bool) -> None:
        """
        Обновление настроек пользователя.
        
        Args:
            user_id: ID пользователя
            notifications_enabled: Статус уведомлений
        """
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO user_settings (user_id, notifications_enabled) VALUES ($1, $2) "
                "ON CONFLICT (user_id) DO UPDATE SET notifications_enabled = $2, "
                "updated_at = CURRENT_TIMESTAMP",
                user_id, notifications_enabled
            )

# ============================================================
# СОСТОЯНИЯ FSM ДЛЯ ПОШАГОВЫХ СЦЕНАРИЕВ
# ============================================================

class AddAccount(StatesGroup):
    """Состояния для добавления аккаунта через номер телефона"""
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_2fa = State()

class AddAccountFile(StatesGroup):
    """Состояния для добавления аккаунта из файла сессии"""
    waiting_for_file = State()
    waiting_for_phone = State()

class JoinChats(StatesGroup):
    """Состояния для вступления в чаты"""
    selecting_account = State()
    waiting_for_links = State()

class MailingSetup(StatesGroup):
    """Состояния для настройки рассылки"""
    selecting_account = State()
    selecting_chats = State()
    waiting_for_delay = State()
    waiting_for_cycles = State()
    waiting_for_message = State()
    confirming = State()

class AutoReplySetup(StatesGroup):
    """Состояния для настройки автоответчика"""
    selecting_account = State()
    waiting_for_text = State()

# ============================================================
# КЛАВИАТУРЫ С ПРЕМИУМ ЭМОДЗИ
# ============================================================

def get_main_keyboard() -> Dict[str, Any]:
    """
    Главное меню бота с премиум эмодзи.
    
    Returns:
        Dict: Клавиатура в формате Telegram API
    """
    return {
        "keyboard": [
            [
                {"text": "Менеджер аккаунтов", "icon_custom_emoji_id": EMOJI["profile"]},
                {"text": "Рассылка", "icon_custom_emoji_id": EMOJI["send"]}
            ],
            [
                {"text": "Вступить в чаты", "icon_custom_emoji_id": EMOJI["join"]},
                {"text": "Автоответчик", "icon_custom_emoji_id": EMOJI["reply"]}
            ],
            [
                {"text": "Проверка спам-блока", "icon_custom_emoji_id": EMOJI["spam"]},
                {"text": "Настройки", "icon_custom_emoji_id": EMOJI["settings"]}
            ],
            [
                {"text": "Остановить рассылку", "icon_custom_emoji_id": EMOJI["stop"]}
            ]
        ],
        "resize_keyboard": True
    }

def get_account_manager_keyboard() -> Dict[str, Any]:
    """
    Клавиатура менеджера аккаунтов.
    
    Returns:
        Dict: Клавиатура
    """
    return {
        "keyboard": [
            [
                {"text": "Добавить аккаунт", "icon_custom_emoji_id": EMOJI["plus"]},
                {"text": "Список аккаунтов", "icon_custom_emoji_id": EMOJI["list"]}
            ],
            [
                {"text": "Добавить из файла", "icon_custom_emoji_id": EMOJI["file"]},
                {"text": "Назад", "icon_custom_emoji_id": EMOJI["back"]}
            ]
        ],
        "resize_keyboard": True
    }

def get_mailing_keyboard() -> Dict[str, Any]:
    """
    Клавиатура меню рассылки.
    
    Returns:
        Dict: Клавиатура
    """
    return {
        "keyboard": [
            [
                {"text": "Запустить рассылку", "icon_custom_emoji_id": EMOJI["rocket"]},
                {"text": "Статус рассылки", "icon_custom_emoji_id": EMOJI["stats"]}
            ],
            [
                {"text": "Назад", "icon_custom_emoji_id": EMOJI["back"]}
            ]
        ],
        "resize_keyboard": True
    }

def get_auto_reply_keyboard() -> Dict[str, Any]:
    """
    Клавиатура меню автоответчика.
    
    Returns:
        Dict: Клавиатура
    """
    return {
        "keyboard": [
            [
                {"text": "Добавить автоответчик", "icon_custom_emoji_id": EMOJI["plus"]},
                {"text": "Мои автоответчики", "icon_custom_emoji_id": EMOJI["list"]}
            ],
            [
                {"text": "Назад", "icon_custom_emoji_id": EMOJI["back"]}
            ]
        ],
        "resize_keyboard": True
    }

def get_back_keyboard() -> Dict[str, Any]:
    """
    Клавиатура с кнопкой "Назад".
    
    Returns:
        Dict: Клавиатура
    """
    return {
        "keyboard": [[{"text": "Назад", "icon_custom_emoji_id": EMOJI["back"]}]],
        "resize_keyboard": True
    }

# ============================================================
# ФУНКЦИИ ДЛЯ РАБОТЫ С TELETHON
# ============================================================

async def create_telethon_client(session_string: Optional[str] = None) -> TelegramClient:
    """
    Создание нового клиента Telethon.
    
    Args:
        session_string: Строка сессии (опционально)
        
    Returns:
        TelegramClient: Подключенный клиент
    """
    client = TelegramClient(
        StringSession(session_string) if session_string else StringSession(),
        API_ID,
        API_HASH,
        system_version="4.16.30-vxCUSTOM",
        device_model="Python Telethon",
        app_version="1.0.0",
        connection_retries=3,
        retry_delay=2,
        timeout=30,
        auto_reconnect=True
    )
    await client.connect()
    return client

async def read_session_file(file_path: str) -> Optional[str]:
    """
    Чтение файла сессии Telethon в различных форматах.
    Поддерживает:
    - Текстовые файлы (StringSession)
    - Бинарные .session файлы (SQLite)
    - Base64 закодированные сессии
    
    Args:
        file_path: Путь к файлу
        
    Returns:
        Optional[str]: Строка сессии или None
    """
    logger.info(f"Чтение файла сессии: {file_path}")
    
    # Попытка чтения как текст
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if content and len(content) > 50:
                logger.info(f"Сессия прочитана как текст, длина: {len(content)}")
                return content
    except UnicodeDecodeError:
        logger.debug("Файл не является UTF-8 текстом")
    except Exception as e:
        logger.warning(f"Ошибка чтения текстового файла: {e}")
    
    # Попытка чтения как бинарный файл
    try:
        with open(file_path, 'rb') as f:
            content = f.read()
            logger.info(f"Файл прочитан как бинарный, размер: {len(content)} байт")
            
            # Проверка на SQLite формат
            if content[:16] == b'SQLite format 3\x00':
                logger.info("Обнаружен SQLite формат .session файла")
                encoded = base64.b64encode(content).decode('ascii')
                return encoded
            
            # Попытка декодирования в разных кодировках
            for encoding in ['utf-8', 'latin-1', 'cp1251', 'ascii']:
                try:
                    text = content.decode(encoding).strip()
                    if text and len(text) > 50:
                        logger.info(f"Сессия декодирована как {encoding}, длина: {len(text)}")
                        return text
                except (UnicodeDecodeError, Exception):
                    continue
            
            # Если не удалось декодировать - возвращаем как base64
            logger.info("Возвращаем как base64")
            return base64.b64encode(content).decode('ascii')
            
    except Exception as e:
        logger.error(f"Ошибка чтения бинарного файла: {e}")
    
    return None

async def verify_session_string(session_string: str) -> Optional[Dict[str, Any]]:
    """
    Проверка валидности строки сессии.
    Пытается подключиться и проверить авторизацию.
    
    Args:
        session_string: Строка сессии
        
    Returns:
        Optional[Dict]: Информация о сессии или None
    """
    client = None
    try:
        client = await create_telethon_client(session_string)
        
        # Проверка авторизации с таймаутом
        is_authorized = await asyncio.wait_for(
            client.is_user_authorized(),
            timeout=15.0
        )
        
        if is_authorized:
            # Получение информации о пользователе
            me = await asyncio.wait_for(
                client.get_me(),
                timeout=15.0
            )
            phone = me.phone if hasattr(me, 'phone') and me.phone else f"User_{me.id}"
            logger.info(f"Сессия валидна, пользователь: {phone}")
            return {"phone": phone, "valid": True, "user_id": me.id}
        
        logger.warning("Сессия не авторизована")
        return None
        
    except asyncio.TimeoutError:
        logger.error("Таймаут при проверке сессии")
        return None
    except Exception as e:
        logger.error(f"Ошибка проверки сессии: {e}")
        return None
    finally:
        if client:
            try:
                await asyncio.wait_for(client.disconnect(), timeout=5.0)
            except:
                pass

async def get_dialogs_from_client(client: TelegramClient) -> List[Dict[str, Any]]:
    """
    Получение списка всех диалогов (чатов, групп, каналов) с клиента.
    Включает супергруппы с пометкой.
    
    Args:
        client: Клиент Telethon
        
    Returns:
        List[Dict]: Список диалогов
    """
    dialogs = await client.get_dialogs(limit=None)
    result = []
    
    for dialog in dialogs:
        entity = dialog.entity
        
        # Определение типа и имени
        if isinstance(entity, User):
            name = f"{entity.first_name or ''} {entity.last_name or ''}".strip()
            if not name:
                name = f"User_{entity.id}"
            chat_id = str(entity.id)
            chat_type = "user"
            
        elif isinstance(entity, Chat):
            name = entity.title or f"Chat_{entity.id}"
            chat_id = str(entity.id)
            chat_type = "group"
            
        elif isinstance(entity, Channel):
            name = entity.title or f"Channel_{entity.id}"
            chat_id = str(entity.id)
            
            # Проверка на супергруппу
            if getattr(entity, 'megagroup', False):
                chat_type = "supergroup"
                name += " [СУПЕРГРУППА]"
            else:
                chat_type = "channel"
        else:
            continue
        
        # Добавление username если есть
        if hasattr(entity, 'username') and entity.username:
            name += f" (@{entity.username})"
        
        result.append({
            'id': chat_id,
            'name': name,
            'type': chat_type,
            'entity': entity
        })
    
    logger.info(f"Получено диалогов: {len(result)}")
    return result

async def send_message_to_chat(
    client: TelegramClient,
    chat_id: str,
    message: str
) -> bool:
    """
    Отправка HTML-сообщения в указанный чат.
    
    Args:
        client: Клиент Telethon
        chat_id: ID чата
        message: Текст сообщения в HTML
        
    Returns:
        bool: True если успешно, False если ошибка
    """
    try:
        entity = await client.get_entity(int(chat_id))
        await client.send_message(
            entity,
            message,
            parse_mode='HTML',
            link_preview=False
        )
        logger.info(f"Сообщение отправлено в чат {chat_id}")
        return True
    except FloodWaitError as e:
        logger.warning(f"Flood wait {e.seconds}с для чата {chat_id}")
        await asyncio.sleep(min(e.seconds, 30))
        return False
    except Exception as e:
        logger.error(f"Ошибка отправки в чат {chat_id}: {e}")
        return False

async def join_chat_by_username(client: TelegramClient, username: str) -> Dict[str, Any]:
    """
    Вступление в чат/канал по username.
    
    Args:
        client: Клиент Telethon
        username: Username чата (с @ или без)
        
    Returns:
        Dict: Результат операции
    """
    try:
        username = username.strip().lstrip('@')
        logger.info(f"Попытка вступления в {username}")
        
        # Получение сущности
        entity = await client.get_entity(username)
        
        # Проверка типа и вступление
        if hasattr(entity, 'broadcast') or hasattr(entity, 'megagroup'):
            await client(JoinChannelRequest(entity))
            title = getattr(entity, 'title', username)
            logger.info(f"Успешно вступили в {title}")
            return {"success": True, "message": f"Вступили в {title}"}
        else:
            logger.warning(f"Не канал/группа: {username}")
            return {"success": False, "message": "Не является каналом или группой"}
            
    except UserAlreadyParticipantError:
        logger.info(f"Уже состоим в {username}")
        return {"success": True, "message": "Уже состоим"}
    except FloodWaitError as e:
        logger.warning(f"Flood wait {e.seconds}с")
        return {"success": False, "message": f"Flood wait {e.seconds}с"}
    except ValueError:
        logger.warning(f"Не найдено: {username}")
        return {"success": False, "message": f"Не найдено: {username}"}
    except Exception as e:
        logger.error(f"Ошибка вступления в {username}: {e}")
        return {"success": False, "message": str(e)[:100]}

async def check_spam_block(client: TelegramClient) -> Dict[str, Any]:
    """
    Проверка наличия спам-блока через @spambot.
    Отправляет /start и анализирует ответ.
    
    Args:
        client: Клиент Telethon
        
    Returns:
        Dict: Результат проверки
    """
    try:
        logger.info("Проверка спам-блока через @spambot")
        spambot = await client.get_entity('@spambot')
        
        # Отправка запроса
        await client.send_message(spambot, '/start')
        logger.info("Отправлен /start в @spambot")
        
        # Ожидание ответа
        await asyncio.sleep(5)
        
        # Получение последних сообщений
        messages = await client.get_messages(spambot, limit=5)
        
        for msg in messages:
            if msg.message:
                text = msg.message.lower()
                logger.info(f"Ответ от @spambot: {text[:200]}")
                
                # Проверка на вечный бан
                if any(word in text for word in ['навсегда', 'never', 'permanently']):
                    return {
                        "blocked": True,
                        "permanent": True,
                        "message": "Аккаунт заблокирован НАВСЕГДА!",
                        "raw": msg.message
                    }
                
                # Проверка на отсутствие ограничений
                if any(word in text for word in ['свободен', 'free', 'нет ограничений', 'no limits', 'no restriction']):
                    return {
                        "blocked": False,
                        "message": "Спам-блок ОТСУТСТВУЕТ. Аккаунт свободен!",
                        "raw": msg.message
                    }
                
                # Проверка на временное ограничение
                if any(word in text for word in ['ограничен', 'limited', 'до', 'until']):
                    return {
                        "blocked": True,
                        "permanent": False,
                        "message": "Аккаунт ОГРАНИЧЕН!",
                        "details": msg.message[:200],
                        "raw": msg.message
                    }
        
        # Если не нашли явных признаков
        if messages and messages[0].message:
            return {
                "blocked": False,
                "message": f"Получен ответ (неопределен): {messages[0].message[:100]}",
                "raw": messages[0].message
            }
        
        return {
            "blocked": False,
            "message": "Нет ответа от @spambot. Возможно, бот недоступен."
        }
        
    except Exception as e:
        logger.error(f"Ошибка проверки спам-блока: {e}")
        return {
            "blocked": False,
            "message": f"Ошибка проверки: {str(e)[:100]}",
            "error": True
        }

# ============================================================
# ФУНКЦИИ АВТООТВЕТЧИКА
# ============================================================

async def start_auto_reply(
    account_id: int,
    account_phone: str,
    session_string: str,
    reply_text: str,
    notifications_chat_id: Optional[int] = None
) -> bool:
    """
    Запуск автоответчика для указанного аккаунта.
    Отвечает на все входящие личные сообщения.
    
    Args:
        account_id: ID аккаунта
        account_phone: Номер телефона
        session_string: Строка сессии
        reply_text: Текст ответа
        notifications_chat_id: ID чата для уведомлений
        
    Returns:
        bool: True если успешно запущен
    """
    logger.info(f"Запуск автоответчика для {account_phone}")
    
    # Остановка предыдущего если есть
    if account_id in auto_reply_clients:
        try:
            await auto_reply_clients[account_id].disconnect()
        except:
            pass
        del auto_reply_clients[account_id]
    
    # Создание нового клиента
    try:
        client = await create_telethon_client(session_string)
        
        if not await client.is_user_authorized():
            logger.error(f"Ошибка авторизации для автоответчика {account_phone}")
            await client.disconnect()
            return False
        
        auto_reply_clients[account_id] = client
        auto_reply_settings[account_id] = {
            'phone': account_phone,
            'reply_text': reply_text,
            'notifications_chat_id': notifications_chat_id
        }
        
        # Регистрация обработчика сообщений
        @client.on(events.NewMessage(incoming=True))
        async def auto_reply_handler(event: events.NewMessage.Event) -> None:
            """Обработчик входящих сообщений для автоответчика"""
            try:
                # Только личные сообщения
                if not event.is_private:
                    return
                
                sender = await event.get_sender()
                
                # Не отвечаем себе и ботам
                if sender.is_self or sender.bot:
                    return
                
                logger.info(f"Автоответчик {account_phone}: входящее от {sender.id}")
                
                # Отправка ответа
                try:
                    await event.reply(reply_text, parse_mode='HTML')
                    await Database.increment_reply_count(account_id)
                except Exception as e:
                    logger.error(f"Ошибка отправки ответа: {e}")
                    try:
                        await event.reply(reply_text)
                    except:
                        pass
                
                # Уведомление владельцу
                if notifications_chat_id:
                    try:
                        sender_name = f"{sender.first_name or ''} {sender.last_name or ''}".strip()
                        await bot.send_message(
                            notifications_chat_id,
                            f'{bem("reply", "Автоответчик сработал!")}\n'
                            f'<b>Аккаунт:</b> {account_phone}\n'
                            f'<b>Отправитель:</b> {sender_name} (ID: {sender.id})\n'
                            f'<b>Сообщение:</b> {event.text[:100] if event.text else "[медиа]"}',
                            parse_mode=ParseMode.HTML
                        )
                    except Exception as e:
                        logger.error(f"Ошибка уведомления: {e}")
                        
            except Exception as e:
                logger.error(f"Ошибка в обработчике автоответчика: {e}")
        
        logger.info(f"Автоответчик успешно запущен для {account_phone}")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка запуска автоответчика: {e}")
        return False

async def stop_auto_reply(account_id: int) -> None:
    """
    Остановка автоответчика для аккаунта.
    
    Args:
        account_id: ID аккаунта
    """
    logger.info(f"Остановка автоответчика для аккаунта {account_id}")
    
    if account_id in auto_reply_clients:
        try:
            await auto_reply_clients[account_id].disconnect()
        except:
            pass
        del auto_reply_clients[account_id]
    
    if account_id in auto_reply_settings:
        del auto_reply_settings[account_id]

async def restore_all_auto_replies() -> None:
    """
    Восстановление всех активных автоответчиков при запуске бота.
    """
    logger.info("Восстановление автоответчиков...")
    
    try:
        active_replies = await Database.get_all_active_auto_replies()
        
        for reply in active_replies:
            try:
                success = await start_auto_reply(
                    account_id=reply['account_id'],
                    account_phone=reply['phone'],
                    session_string=reply['session_string'],
                    reply_text=reply['reply_text']
                )
                if success:
                    logger.info(f"Восстановлен автоответчик для {reply['phone']}")
                else:
                    logger.warning(f"Не удалось восстановить автоответчик для {reply['phone']}")
            except Exception as e:
                logger.error(f"Ошибка восстановления автоответчика: {e}")
                
    except Exception as e:
        logger.error(f"Ошибка при восстановлении автоответчиков: {e}")

# ============================================================
# ОБРАБОТЧИКИ КОМАНД БОТА
# ============================================================

@dp.message(Command("start"))
async def cmd_start(message: Message) -> None:
    """
    Обработчик команды /start.
    Отправляет приветственное сообщение с главным меню.
    """
    await message.answer(
        f'{bem("bot", "Добро пожаловать в бот для массовых рассылок!")}\n'
        f'{bem("info", "Выберите нужный раздел:")}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )

@dp.message(Command("stop_ml"))
async def cmd_stop_mailing(message: Message) -> None:
    """Обработчик команды /stop_ml - быстрая остановка всех рассылок"""
    if not active_mailing_tasks:
        await message.answer(
            f'{bem("info", "Нет активных рассылок")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        return
    
    # Остановка всех рассылок
    for mailing_id, task in list(active_mailing_tasks.items()):
        task.cancel()
        del active_mailing_tasks[mailing_id]
    
    await message.answer(
        f'{bem("stop", "Все рассылки остановлены!")}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )

@dp.message(F.text == "Назад")
async def go_back(message: Message, state: FSMContext) -> None:
    """
    Обработчик кнопки "Назад".
    Очищает состояние FSM и возвращает в главное меню.
    """
    current_state = await state.get_state()
    if current_state:
        await state.clear()
        logger.info(f"Состояние сброшено: {current_state}")
    
    await message.answer(
        f'{bem("home", "Главное меню:")}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )

@dp.message(F.text == "Менеджер аккаунтов")
async def account_manager_menu(message: Message) -> None:
    """Обработчик кнопки "Менеджер аккаунтов"."""
    await message.answer(
        f'{bem("profile", "Менеджер аккаунтов")}\n'
        f'{bem("info", "Выберите действие:")}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_account_manager_keyboard()
    )

@dp.message(F.text == "Настройки")
async def settings_menu(message: Message) -> None:
    """Обработчик кнопки "Настройки"."""
    user_id = message.from_user.id
    settings = await Database.get_user_settings(user_id)
    
    notif_status = "<b>ВКЛЮЧЕНЫ</b>" if settings['notifications_enabled'] else "<b>ВЫКЛЮЧЕНЫ</b>"
    
    builder = InlineKeyboardBuilder()
    btn_text = "🔕 Выключить уведомления" if settings['notifications_enabled'] else "🔔 Включить уведомления"
    builder.row(InlineKeyboardButton(text=btn_text, callback_data="toggle_notifications"))
    
    await message.answer(
        f'{bem("settings", "Настройки")}\n\n'
        f'<b>Статус уведомлений:</b> {notif_status}\n\n'
        f'{bi("При выключенных уведомлениях бот не будет присылать сообщения о прогрессе рассылки")}',
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )

@dp.callback_query(F.data == "toggle_notifications")
async def toggle_notifications(callback: CallbackQuery) -> None:
    """Обработчик переключения уведомлений."""
    user_id = callback.from_user.id
    settings = await Database.get_user_settings(user_id)
    new_status = not settings['notifications_enabled']
    
    await Database.update_user_settings(user_id, new_status)
    
    notif_status = "<b>ВКЛЮЧЕНЫ</b>" if new_status else "<b>ВЫКЛЮЧЕНЫ</b>"
    builder = InlineKeyboardBuilder()
    btn_text = "🔕 Выключить уведомления" if new_status else "🔔 Включить уведомления"
    builder.row(InlineKeyboardButton(text=btn_text, callback_data="toggle_notifications"))
    
    await callback.message.edit_text(
        f'{bem("settings", "Настройки")}\n\n'
        f'<b>Статус уведомлений:</b> {notif_status}\n\n'
        f'{bi("При выключенных уведомлениях бот не будет присылать сообщения о прогрессе рассылки")}',
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )
    await callback.answer()

# ============================================================
# ПРОВЕРКА СПАМ-БЛОКА
# ============================================================

@dp.message(F.text == "Проверка спам-блока")
async def spam_check_start(message: Message) -> None:
    """Начало проверки спам-блока - выбор аккаунта."""
    accounts = await Database.get_accounts()
    
    if not accounts:
        await message.answer(
            f'{bem("cross", "Нет сохраненных аккаунтов!")}\n'
            f'{bi("Сначала добавьте аккаунт в менеджере.")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        return
    
    builder = InlineKeyboardBuilder()
    for account in accounts:
        label = f"{account['phone']} {'(2FA)' if account['has_2fa'] else ''}"
        builder.row(InlineKeyboardButton(
            text=label,
            callback_data=f"spamcheck_{account['id']}"
        ))
    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_spamcheck"))
    
    await message.answer(
        f'{bem("spam", "Выберите аккаунт для проверки спам-блока:")}',
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )

@dp.callback_query(F.data.startswith("spamcheck_"))
async def do_spam_check(callback: CallbackQuery) -> None:
    """Выполнение проверки спам-блока."""
    account_id = int(callback.data.split("_")[1])
    account = await Database.get_account_by_id(account_id)
    
    if not account:
        await callback.answer("Аккаунт не найден!", show_alert=True)
        return
    
    await callback.message.delete()
    
    status_msg = await callback.message.answer(
        f'{bem("clock", "Проверяю спам-блок...")}\n'
        f'<b>Аккаунт:</b> {account["phone"]}\n'
        f'{bi("Отправляю запрос в @spambot...")}',
        parse_mode=ParseMode.HTML
    )
    
    try:
        client = await create_telethon_client(account['session_string'])
        
        if not await client.is_user_authorized():
            await status_msg.edit_text(
                f'{bem("cross", "Ошибка авторизации!")}\n'
                f'{bi("Возможно, сессия устарела.")}',
                parse_mode=ParseMode.HTML
            )
            await client.disconnect()
            return
        
        result = await check_spam_block(client)
        await client.disconnect()
        
        # Формирование ответа в зависимости от результата
        if result.get('error'):
            await status_msg.edit_text(
                f'{bem("info", "Результат проверки:")}\n\n'
                f'<b>{result["message"]}</b>\n\n'
                f'{bi("Возможно, @spambot недоступен или изменил формат ответов.")}',
                parse_mode=ParseMode.HTML,
                reply_markup=get_main_keyboard()
            )
        elif result['blocked']:
            if result.get('permanent'):
                await status_msg.edit_text(
                    f'{bem("cross", "СПАМ-БЛОК ОБНАРУЖЕН!")}\n\n'
                    f'<b>Статус:</b> ЗАБЛОКИРОВАН НАВСЕГДА\n'
                    f'<b>Аккаунт:</b> {account["phone"]}\n\n'
                    f'{bi("Этот аккаунт НЕЛЬЗЯ использовать для рассылок!")}',
                    parse_mode=ParseMode.HTML,
                    reply_markup=get_main_keyboard()
                )
            else:
                await status_msg.edit_text(
                    f'{bem("warning", "Обнаружено ограничение!")}\n\n'
                    f'<b>Аккаунт:</b> {account["phone"]}\n'
                    f'<b>Детали:</b> {result.get("details", result["message"])}\n\n'
                    f'{bi("Не рекомендуется использовать для рассылок до снятия ограничений.")}',
                    parse_mode=ParseMode.HTML,
                    reply_markup=get_main_keyboard()
                )
        else:
            await status_msg.edit_text(
                f'{bem("check", "Спам-блок НЕ обнаружен!")}\n\n'
                f'<b>Аккаунт:</b> {account["phone"]}\n'
                f'<b>Результат:</b> {result["message"]}\n\n'
                f'{bi("Аккаунт можно использовать для рассылок.")}',
                parse_mode=ParseMode.HTML,
                reply_markup=get_main_keyboard()
            )
        
    except Exception as e:
        logger.error(f"Ошибка проверки спам-блока: {e}")
        await status_msg.edit_text(
            f'{bem("cross", "Ошибка проверки:")}\n'
            f'<b>{str(e)[:200]}</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )

@dp.callback_query(F.data == "cancel_spamcheck")
async def cancel_spam_check(callback: CallbackQuery) -> None:
    """Отмена проверки спам-блока."""
    await callback.message.delete()
    await callback.message.answer(
        f'{bem("cross", "Проверка отменена")}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )
    await callback.answer()

# ============================================================
# ДОБАВЛЕНИЕ АККАУНТА ПО НОМЕРУ ТЕЛЕФОНА
# ============================================================

@dp.message(F.text == "Добавить аккаунт")
async def add_account_start(message: Message, state: FSMContext) -> None:
    """Начало процесса добавления аккаунта."""
    await message.answer(
        f'{bem("profile", "Введите номер телефона в международном формате")}\n'
        f'{bi("Например: +79991234567")}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(AddAccount.waiting_for_phone)

@dp.message(AddAccount.waiting_for_phone)
async def process_phone(message: Message, state: FSMContext) -> None:
    """Обработка введенного номера телефона."""
    phone = message.text.strip()
    user_id = message.from_user.id
    
    # Валидация номера
    if not phone.startswith('+') or len(phone) < 10:
        await message.answer(
            f'{bem("cross", "Неверный формат номера.")}\n'
            f'{bi("Введите номер в формате +79991234567")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        return
    
    try:
        # Создание клиента и запрос кода
        client = await create_telethon_client()
        auth_clients[user_id] = client
        
        send_code_result = await client.send_code_request(phone)
        
        await state.update_data(
            phone=phone,
            phone_code_hash=send_code_result.phone_code_hash
        )
        
        await message.answer(
            f'{bem("send", "Код подтверждения отправлен!")}\n'
            f'{bem("info", "Введите код из сообщения Telegram:")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        await state.set_state(AddAccount.waiting_for_code)
        
    except FloodWaitError as e:
        await message.answer(
            f'{bem("clock", "Слишком много попыток!")}\n'
            f'<b>Подождите {e.seconds} секунд.</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        await cleanup_auth_client(user_id)
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка отправки кода: {e}")
        await message.answer(
            f'{bem("cross", "Ошибка:")} <b>{str(e)[:200]}</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        await cleanup_auth_client(user_id)
        await state.clear()

async def cleanup_auth_client(user_id: int) -> None:
    """Очистка временного клиента авторизации."""
    if user_id in auth_clients:
        try:
            await auth_clients[user_id].disconnect()
        except:
            pass
        del auth_clients[user_id]

@dp.message(AddAccount.waiting_for_code)
async def process_code(message: Message, state: FSMContext) -> None:
    """Обработка кода подтверждения."""
    code = message.text.strip()
    data = await state.get_data()
    user_id = message.from_user.id
    
    client = auth_clients.get(user_id)
    if not client:
        await message.answer(
            f'{bem("cross", "Сессия авторизации истекла.")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        await state.clear()
        return
    
    try:
        try:
            await client.sign_in(
                phone=data['phone'],
                code=code,
                phone_code_hash=data['phone_code_hash']
            )
        except SessionPasswordNeededError:
            # Требуется 2FA
            await state.update_data(session_string=client.session.save())
            await message.answer(
                f'{bem("lock", "Требуется двухфакторная аутентификация!")}\n'
                f'{bem("info", "Введите пароль 2FA:")}',
                parse_mode=ParseMode.HTML,
                reply_markup=get_back_keyboard()
            )
            await state.set_state(AddAccount.waiting_for_2fa)
            return
        
        # Успешный вход
        session_string = client.session.save()
        await Database.add_account(data['phone'], session_string)
        
        await message.answer(
            f'{bem("check", "Аккаунт успешно добавлен!")}\n'
            f'<b>Номер:</b> {data["phone"]}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_account_manager_keyboard()
        )
        
        await cleanup_auth_client(user_id)
        await state.clear()
        
    except PhoneCodeExpiredError:
        await message.answer(
            f'{bem("cross", "Код подтверждения истек!")}\n'
            f'{bi("Начните процесс заново.")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_account_manager_keyboard()
        )
        await cleanup_auth_client(user_id)
        await state.clear()
    except PhoneCodeInvalidError:
        await message.answer(
            f'{bem("cross", "Неверный код подтверждения!")}\n'
            f'{bi("Проверьте код и попробуйте снова.")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
    except Exception as e:
        logger.error(f"Ошибка входа: {e}")
        await message.answer(
            f'{bem("cross", "Ошибка:")} <b>{str(e)[:200]}</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        await cleanup_auth_client(user_id)
        await state.clear()

@dp.message(AddAccount.waiting_for_2fa)
async def process_2fa(message: Message, state: FSMContext) -> None:
    """Обработка пароля 2FA."""
    password = message.text.strip()
    data = await state.get_data()
    user_id = message.from_user.id
    
    client = auth_clients.get(user_id)
    if not client:
        await message.answer(
            f'{bem("cross", "Сессия истекла.")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        await state.clear()
        return
    
    try:
        await client.sign_in(password=password)
        
        session_string = client.session.save()
        await Database.add_account(data['phone'], session_string, has_2fa=True)
        
        await message.answer(
            f'{bem("check", "Аккаунт добавлен с 2FA защитой!")}\n'
            f'<b>Номер:</b> {data["phone"]}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_account_manager_keyboard()
        )
        
        await cleanup_auth_client(user_id)
        await state.clear()
        
    except PasswordHashInvalidError:
        await message.answer(
            f'{bem("cross", "Неверный пароль 2FA!")}\n'
            f'{bi("Попробуйте снова.")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
    except Exception as e:
        logger.error(f"Ошибка 2FA: {e}")
        await message.answer(
            f'{bem("cross", "Ошибка:")} <b>{str(e)[:200]}</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        await cleanup_auth_client(user_id)
        await state.clear()

# ============================================================
# ДОБАВЛЕНИЕ АККАУНТА ИЗ ФАЙЛА СЕССИИ
# ============================================================

@dp.message(F.text == "Добавить из файла")
async def add_account_file_start(message: Message, state: FSMContext) -> None:
    """Начало добавления аккаунта из файла."""
    await message.answer(
        f'{bem("file", "Отправьте файл сессии Telethon")}\n\n'
        f'{bi("Поддерживаемые форматы:")}\n'
        f'{bi("• Файл .session (бинарный)")}\n'
        f'{bi("• Текстовая строка сессии (StringSession)")}\n\n'
        f'{bem("info", "Отправьте файл как документ или текст:")}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(AddAccountFile.waiting_for_file)

@dp.message(AddAccountFile.waiting_for_file)
async def process_account_file(message: Message, state: FSMContext) -> None:
    """Обработка файла сессии."""
    session_string = None
    
    # Обработка документа
    if message.document:
        doc = message.document
        logger.info(f"Получен документ: {doc.file_name}, размер: {doc.file_size}")
        
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.session', delete=False) as tmp:
            temp_path = tmp.name
        
        try:
            await bot.download(doc, destination=temp_path)
            session_string = await read_session_file(temp_path)
            try:
                os.unlink(temp_path)
            except:
                pass
        except Exception as e:
            logger.error(f"Ошибка скачивания: {e}")
            await message.answer(
                f'{bem("cross", "Ошибка при получении файла!")}',
                parse_mode=ParseMode.HTML,
                reply_markup=get_account_manager_keyboard()
            )
            await state.clear()
            return
    
    # Обработка текста
    elif message.text:
        text = message.text.strip()
        if len(text) > 50:
            session_string = text
    
    if not session_string:
        await message.answer(
            f'{bem("cross", "Не удалось извлечь данные сессии!")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_account_manager_keyboard()
        )
        await state.clear()
        return
    
    # Проверка сессии
    status_msg = await message.answer(
        f'{bem("clock", "Проверяю сессию...")}',
        parse_mode=ParseMode.HTML
    )
    
    result = await verify_session_string(session_string)
    
    if result and result['valid']:
        await state.update_data(
            session_string=session_string,
            detected_phone=result['phone']
        )
        
        builder = InlineKeyboardBuilder()
        builder.row(InlineKeyboardButton(
            text=f"Использовать: {result['phone']}",
            callback_data="use_detected_phone"
        ))
        
        await status_msg.edit_text(
            f'{bem("check", "Сессия ВАЛИДНА!")}\n'
            f'<b>Обнаруженный номер:</b> {result["phone"]}\n\n'
            f'{bem("info", "Введите номер телефона или нажмите кнопку:")}',
            parse_mode=ParseMode.HTML,
            reply_markup=builder.as_markup()
        )
        await state.set_state(AddAccountFile.waiting_for_phone)
    else:
        await status_msg.edit_text(
            f'{bem("cross", "Сессия НЕВАЛИДНА!")}\n\n'
            f'{bi("Возможные причины:")}\n'
            f'{bi("• Файл поврежден")}\n'
            f'{bi("• Сессия устарела")}\n'
            f'{bi("• Неверный формат файла")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_account_manager_keyboard()
        )
        await state.clear()

@dp.callback_query(StateFilter(AddAccountFile.waiting_for_phone), F.data == "use_detected_phone")
async def use_detected_phone(callback: CallbackQuery, state: FSMContext) -> None:
    """Использование обнаруженного номера."""
    data = await state.get_data()
    await Database.add_account_from_file(data['detected_phone'], data['session_string'])
    
    await callback.message.delete()
    await callback.message.answer(
        f'{bem("check", "Аккаунт успешно добавлен!")}\n'
        f'<b>Номер:</b> {data["detected_phone"]}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_account_manager_keyboard()
    )
    await state.clear()
    await callback.answer()

@dp.message(AddAccountFile.waiting_for_phone)
async def process_account_phone_file(message: Message, state: FSMContext) -> None:
    """Обработка номера телефона для аккаунта из файла."""
    data = await state.get_data()
    phone = message.text.strip()
    
    if not phone.startswith('+') or len(phone) < 10:
        await message.answer(
            f'{bem("cross", "Неверный формат номера.")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        return
    
    await Database.add_account_from_file(phone, data['session_string'])
    
    await message.answer(
        f'{bem("check", "Аккаунт успешно добавлен!")}\n'
        f'<b>Номер:</b> {phone}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_account_manager_keyboard()
    )
    await state.clear()

# ============================================================
# СПИСОК АККАУНТОВ И УДАЛЕНИЕ
# ============================================================

@dp.message(F.text == "Список аккаунтов")
async def list_accounts(message: Message) -> None:
    """Отображение списка всех аккаунтов с возможностью удаления."""
    accounts = await Database.get_accounts()
    
    if not accounts:
        await message.answer(
            f'{bem("info", "Нет сохраненных аккаунтов.")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_account_manager_keyboard()
        )
        return
    
    text = f'{bem("list", "Список аккаунтов:")}\n\n'
    builder = InlineKeyboardBuilder()
    
    for account in accounts:
        phone = account['phone']
        has_2fa = " (2FA)" if account['has_2fa'] else ""
        text += f"<b>• {phone}{has_2fa}</b>\n"
        
        builder.row(InlineKeyboardButton(
            text=f"🗑 Удалить {phone}",
            callback_data=f"delete_account_{account['id']}"
        ))
    
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("delete_account_"))
async def delete_account_handler(callback: CallbackQuery) -> None:
    """Удаление аккаунта."""
    account_id = int(callback.data.split("_")[2])
    
    # Остановка автоответчика если активен
    await stop_auto_reply(account_id)
    
    # Удаление из БД
    await Database.delete_account(account_id)
    
    # Очистка кеша клиентов
    if account_id in active_clients:
        try:
            await active_clients[account_id].disconnect()
        except:
            pass
        del active_clients[account_id]
    
    await callback.message.delete()
    await callback.message.answer(
        f'{bem("trash", "Аккаунт успешно удален!")}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_account_manager_keyboard()
    )
    await callback.answer()

# ============================================================
# АВТООТВЕТЧИК
# ============================================================

@dp.message(F.text == "Автоответчик")
async def auto_reply_menu(message: Message) -> None:
    """Меню автоответчика."""
    await message.answer(
        f'{bem("reply", "Автоответчик")}\n'
        f'{bem("info", "Автоматический ответ на личные сообщения.")}\n'
        f'{bem("info", "Выберите действие:")}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_auto_reply_keyboard()
    )

@dp.message(F.text == "Добавить автоответчик")
async def add_auto_reply_start(message: Message, state: FSMContext) -> None:
    """Начало настройки автоответчика - выбор аккаунта."""
    accounts = await Database.get_accounts()
    
    if not accounts:
        await message.answer(
            f'{bem("cross", "Нет сохраненных аккаунтов!")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        return
    
    builder = InlineKeyboardBuilder()
    for account in accounts:
        label = f"{account['phone']} {'(2FA)' if account['has_2fa'] else ''}"
        builder.row(InlineKeyboardButton(
            text=label,
            callback_data=f"autoreply_account_{account['id']}"
        ))
    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_autoreply"))
    
    await message.answer(
        f'{bem("profile", "Выберите аккаунт для автоответчика:")}',
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )
    await state.set_state(AutoReplySetup.selecting_account)

@dp.callback_query(StateFilter(AutoReplySetup.selecting_account), F.data.startswith("autoreply_account_"))
async def select_autoreply_account(callback: CallbackQuery, state: FSMContext) -> None:
    """Выбор аккаунта для автоответчика."""
    account_id = int(callback.data.split("_")[2])
    account = await Database.get_account_by_id(account_id)
    
    if not account:
        await callback.answer("Аккаунт не найден!", show_alert=True)
        return
    
    await state.update_data(account_id=account_id, account_phone=account['phone'])
    
    await callback.message.delete()
    await callback.message.answer(
        f'{bem("write", "Введите текст автоответа")}\n'
        f'{bi("Поддерживается HTML форматирование")}\n\n'
        f'{bi("Примеры:")}\n'
        f'<code>Привет! Спасибо за сообщение.</code>\n'
        f'<code>&lt;b&gt;Здравствуйте!&lt;/b&gt; Я сейчас не в сети.</code>\n\n'
        f'<b>Аккаунт:</b> {account["phone"]}\n'
        f'{bi("Сообщение будет отправляться на любое ЛС")}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(AutoReplySetup.waiting_for_text)

@dp.message(AutoReplySetup.waiting_for_text)
async def process_autoreply_text(message: Message, state: FSMContext) -> None:
    """Сохранение текста автоответа и запуск."""
    reply_text = message.html_text if message.html_text else message.text
    
    if not reply_text or not reply_text.strip():
        await message.answer(
            f'{bem("cross", "Текст не может быть пустым!")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        return
    
    data = await state.get_data()
    account_id = data['account_id']
    account_phone = data['account_phone']
    user_id = message.from_user.id
    
    # Сохранение в БД
    await Database.set_auto_reply(account_id, reply_text, is_active=True)
    
    # Запуск автоответчика
    account = await Database.get_account_by_id(account_id)
    
    if account:
        success = await start_auto_reply(
            account_id=account_id,
            account_phone=account_phone,
            session_string=account['session_string'],
            reply_text=reply_text,
            notifications_chat_id=user_id
        )
        
        if success:
            await message.answer(
                f'{bem("check", "Автоответчик запущен!")}\n'
                f'<b>Аккаунт:</b> {account_phone}\n\n'
                f'{bi("Бот будет отвечать на все личные сообщения.")}',
                parse_mode=ParseMode.HTML,
                reply_markup=get_auto_reply_keyboard()
            )
        else:
            await message.answer(
                f'{bem("cross", "Ошибка запуска автоответчика!")}\n'
                f'{bi("Проверьте валидность сессии.")}',
                parse_mode=ParseMode.HTML,
                reply_markup=get_auto_reply_keyboard()
            )
    else:
        await message.answer(
            f'{bem("cross", "Аккаунт не найден в базе!")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_auto_reply_keyboard()
        )
    
    await state.clear()

@dp.callback_query(F.data == "cancel_autoreply")
async def cancel_autoreply(callback: CallbackQuery, state: FSMContext) -> None:
    """Отмена настройки автоответчика."""
    await callback.message.delete()
    await callback.message.answer(
        f'{bem("cross", "Настройка автоответчика отменена.")}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_auto_reply_keyboard()
    )
    await state.clear()
    await callback.answer()

@dp.message(F.text == "Мои автоответчики")
async def list_auto_replies(message: Message) -> None:
    """Отображение списка активных автоответчиков."""
    active_replies = await Database.get_all_active_auto_replies()
    
    if not active_replies:
        await message.answer(
            f'{bem("info", "Нет активных автоответчиков.")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_auto_reply_keyboard()
        )
        return
    
    text = f'{bem("list", "Активные автоответчики:")}\n\n'
    builder = InlineKeyboardBuilder()
    
    for reply in active_replies:
        status = "<b>Активен</b>" if reply['is_active'] else "<b>Выключен</b>"
        text += f"<b>• {reply['phone']}</b> - {status}\n"
        
        # Безопасное отображение текста ответа
        safe_reply = reply['reply_text'][:50].replace('<', '&lt;').replace('>', '&gt;')
        text += f"  {bi('Ответ: ' + safe_reply + '...')}\n"
        text += f"  {bi('Ответов: ' + str(reply.get('reply_count', 0)))}\n\n"
        
        builder.row(InlineKeyboardButton(
            text=f"🛑 Выключить для {reply['phone']}",
            callback_data=f"toggle_autoreply_{reply['account_id']}_off"
        ))
    
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("toggle_autoreply_"))
async def toggle_auto_reply_handler(callback: CallbackQuery) -> None:
    """Включение/выключение автоответчика."""
    parts = callback.data.split("_")
    account_id = int(parts[2])
    action = parts[3]
    
    if action == "off":
        await Database.toggle_auto_reply(account_id, False)
        await stop_auto_reply(account_id)
        
        await callback.message.delete()
        await callback.message.answer(
            f'{bem("stop", "Автоответчик выключен!")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_auto_reply_keyboard()
        )
    elif action == "on":
        # Включение требует перезапуска
        reply = await Database.get_auto_reply(account_id)
        if reply:
            await Database.toggle_auto_reply(account_id, True)
            account = await Database.get_account_by_id(account_id)
            if account:
                await start_auto_reply(
                    account_id=account_id,
                    account_phone=account['phone'],
                    session_string=account['session_string'],
                    reply_text=reply['reply_text']
                )
    
    await callback.answer()

# ============================================================
# ВСТУПЛЕНИЕ В ЧАТЫ
# ============================================================

@dp.message(F.text == "Вступить в чаты")
async def join_chats_start(message: Message, state: FSMContext) -> None:
    """Начало вступления в чаты - выбор аккаунта."""
    accounts = await Database.get_accounts()
    
    if not accounts:
        await message.answer(
            f'{bem("cross", "Нет сохраненных аккаунтов!")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        return
    
    builder = InlineKeyboardBuilder()
    for account in accounts:
        label = f"{account['phone']} {'(2FA)' if account['has_2fa'] else ''}"
        builder.row(InlineKeyboardButton(
            text=label,
            callback_data=f"join_account_{account['id']}"
        ))
    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_join"))
    
    await message.answer(
        f'{bem("profile", "Выберите аккаунт для вступления в чаты:")}',
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )
    await state.set_state(JoinChats.selecting_account)

@dp.callback_query(StateFilter(JoinChats.selecting_account), F.data.startswith("join_account_"))
async def select_join_account(callback: CallbackQuery, state: FSMContext) -> None:
    """Выбор аккаунта и запрос ссылок."""
    account_id = int(callback.data.split("_")[2])
    account = await Database.get_account_by_id(account_id)
    
    if not account:
        await callback.answer("Аккаунт не найден!", show_alert=True)
        return
    
    await state.update_data(account_id=account_id, account_phone=account['phone'])
    
    await callback.message.delete()
    await callback.message.answer(
        f'{bem("join", "Отправьте ссылки на чаты")}\n\n'
        f'{bi("Поддерживаемые форматы:")}\n'
        f'{bi("• @username")}\n'
        f'{bi("• https://t.me/username")}\n'
        f'{bi("• username")}\n\n'
        f'{bem("info", "Отправьте ссылки (каждую с новой строки):")}\n'
        f'<code>@chat1\n@chat2\n@chat3</code>\n\n'
        f'<b>Аккаунт:</b> {account["phone"]}\n'
        f'<b>Задержка:</b> 20 секунд между вступлениями',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(JoinChats.waiting_for_links)

@dp.message(JoinChats.waiting_for_links)
async def process_join_links(message: Message, state: FSMContext) -> None:
    """Обработка ссылок и вступление в чаты."""
    text = message.text.strip()
    data = await state.get_data()
    account_id = data['account_id']
    account_phone = data['account_phone']
    
    # Извлечение username из ссылок
    links = []
    for line in text.split('\n'):
        line = line.strip()
        if not line:
            continue
        username = line.replace('https://t.me/', '').replace('@', '').strip()
        if username:
            links.append(username)
    
    if not links:
        await message.answer(
            f'{bem("cross", "Не найдено ссылок!")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        return
    
    status_msg = await message.answer(
        f'{bem("clock", "Начинаю вступление в чаты...")}\n'
        f'<b>Аккаунт:</b> {account_phone}\n'
        f'<b>Найдено ссылок:</b> {len(links)}\n'
        f'<b>Задержка:</b> 20 секунд',
        parse_mode=ParseMode.HTML
    )
    
    try:
        # Получение или создание клиента
        if account_id not in active_clients:
            account = await Database.get_account_by_id(account_id)
            client = await create_telethon_client(account['session_string'])
            if not await client.is_user_authorized():
                await status_msg.edit_text(
                    f'{bem("cross", "Ошибка авторизации аккаунта!")}',
                    parse_mode=ParseMode.HTML
                )
                await state.clear()
                return
            active_clients[account_id] = client
        else:
            client = active_clients[account_id]
        
        success_count = 0
        fail_count = 0
        
        for i, username in enumerate(links, 1):
            result = await join_chat_by_username(client, username)
            
            if result['success']:
                success_count += 1
            else:
                fail_count += 1
            
            # Обновление статуса
            await status_msg.edit_text(
                f'{bem("join", "Вступление в чаты...")}\n'
                f'<b>Прогресс:</b> {i}/{len(links)}\n'
                f'<b>Успешно:</b> {success_count}\n'
                f'<b>Ошибок:</b> {fail_count}',
                parse_mode=ParseMode.HTML
            )
            
            # Задержка 20 секунд
            if i < len(links):
                for sec in range(20, 0, -1):
                    await status_msg.edit_text(
                        f'{bem("clock", "Ожидание...")}\n'
                        f'<b>Прогресс:</b> {i}/{len(links)}\n'
                        f'<b>Успешно:</b> {success_count} | <b>Ошибок:</b> {fail_count}\n'
                        f'<b>Следующее вступление через:</b> {sec} сек',
                        parse_mode=ParseMode.HTML
                    )
                    await asyncio.sleep(1)
        
        # Финальный отчет
        await status_msg.edit_text(
            f'{bem("stats", "Результаты вступления:")}\n\n'
            f'<b>Аккаунт:</b> {account_phone}\n'
            f'<b>Успешно:</b> {success_count}\n'
            f'<b>Ошибок:</b> {fail_count}\n'
            f'<b>Всего:</b> {len(links)}',
            parse_mode=ParseMode.HTML
        )
        
        await message.answer(
            f'{bem("check", "Операция завершена!")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Ошибка вступления: {e}")
        await status_msg.edit_text(
            f'{bem("cross", "Ошибка:")} <b>{str(e)[:200]}</b>',
            parse_mode=ParseMode.HTML
        )
    finally:
        await state.clear()

@dp.callback_query(F.data == "cancel_join")
async def cancel_join(callback: CallbackQuery, state: FSMContext) -> None:
    """Отмена вступления в чаты."""
    await callback.message.delete()
    await callback.message.answer(
        f'{bem("cross", "Операция отменена.")}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )
    await state.clear()
    await callback.answer()

# ============================================================
# СИСТЕМА РАССЫЛОК
# ============================================================

@dp.message(F.text == "Рассылка")
async def mailing_menu(message: Message) -> None:
    """Меню рассылки."""
    await message.answer(
        f'{bem("send", "Меню рассылки")}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_mailing_keyboard()
    )

@dp.message(F.text == "Запустить рассылку")
async def start_mailing(message: Message, state: FSMContext) -> None:
    """Начало настройки рассылки - выбор аккаунта."""
    accounts = await Database.get_accounts()
    
    if not accounts:
        await message.answer(
            f'{bem("cross", "Нет сохраненных аккаунтов!")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        return
    
    builder = InlineKeyboardBuilder()
    for account in accounts:
        label = f"{account['phone']} {'(2FA)' if account['has_2fa'] else ''}"
        builder.row(InlineKeyboardButton(
            text=label,
            callback_data=f"mailing_account_{account['id']}"
        ))
    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_mailing"))
    
    await message.answer(
        f'{bem("profile", "Выберите аккаунт для рассылки:")}',
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )
    await state.set_state(MailingSetup.selecting_account)

@dp.callback_query(StateFilter(MailingSetup.selecting_account), F.data.startswith("mailing_account_"))
async def select_mailing_account(callback: CallbackQuery, state: FSMContext) -> None:
    """Выбор аккаунта и загрузка чатов."""
    account_id = int(callback.data.split("_")[2])
    account = await Database.get_account_by_id(account_id)
    
    if not account:
        await callback.answer("Аккаунт не найден!", show_alert=True)
        return
    
    await callback.message.delete()
    await callback.message.answer(
        f'{bem("download", "Загружаем чаты...")}\n'
        f'<b>Аккаунт:</b> {account["phone"]}',
        parse_mode=ParseMode.HTML
    )
    
    try:
        # Создание или получение клиента
        if account_id not in active_clients:
            client = await create_telethon_client(account['session_string'])
            if not await client.is_user_authorized():
                await callback.message.answer(
                    f'{bem("cross", "Ошибка авторизации!")}',
                    parse_mode=ParseMode.HTML,
                    reply_markup=get_mailing_keyboard()
                )
                return
            active_clients[account_id] = client
        else:
            client = active_clients[account_id]
        
        # Получение диалогов
        dialogs = await get_dialogs_from_client(client)
        
        if not dialogs:
            await callback.message.answer(
                f'{bem("cross", "Не удалось получить список чатов.")}',
                parse_mode=ParseMode.HTML,
                reply_markup=get_mailing_keyboard()
            )
            return
        
        await state.update_data(
            account_id=account_id,
            account_phone=account['phone'],
            dialogs=dialogs,
            selected_chats=[],
            current_page=0
        )
        
        await state.set_state(MailingSetup.selecting_chats)
        await show_chats_page(callback.message, state)
        
    except Exception as e:
        logger.error(f"Ошибка загрузки чатов: {e}")
        await callback.message.answer(
            f'{bem("cross", "Ошибка:")} <b>{str(e)[:200]}</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_mailing_keyboard()
        )

async def show_chats_page(message: Message, state: FSMContext) -> None:
    """Отображение страницы со списком чатов для выбора."""
    data = await state.get_data()
    dialogs = data['dialogs']
    selected_chats = data.get('selected_chats', [])
    current_page = data.get('current_page', 0)
    account_phone = data.get('account_phone', 'Unknown')
    
    items_per_page = 10
    total_pages = max(1, (len(dialogs) + items_per_page - 1) // items_per_page)
    start_idx = current_page * items_per_page
    end_idx = min(start_idx + items_per_page, len(dialogs))
    
    builder = InlineKeyboardBuilder()
    
    # Кнопки чатов
    for i in range(start_idx, end_idx):
        dialog = dialogs[i]
        is_selected = dialog['id'] in selected_chats
        prefix = "✅ " if is_selected else "⬜ "
        name = dialog['name'][:40] + "..." if len(dialog['name']) > 40 else dialog['name']
        builder.row(InlineKeyboardButton(
            text=f"{prefix}{name}",
            callback_data=f"toggle_chat_{dialog['id']}"
        ))
    
    # Кнопки навигации
    nav_buttons = []
    if current_page > 0:
        nav_buttons.append(InlineKeyboardButton(
            text="◀️ Назад", callback_data=f"page_{current_page - 1}"
        ))
    nav_buttons.append(InlineKeyboardButton(
        text=f"📄 {current_page + 1}/{total_pages}", callback_data="ignore"
    ))
    if current_page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(
            text="Вперед ▶️", callback_data=f"page_{current_page + 1}"
        ))
    if nav_buttons:
        builder.row(*nav_buttons)
    
    # Кнопка подтверждения
    if len(selected_chats) > 0:
        builder.row(InlineKeyboardButton(
            text=f"✅ Подтвердить выбор ({len(selected_chats)} чатов)",
            callback_data="confirm_chats"
        ))
    
    # Кнопка отмены
    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_mailing"))
    
    text = (
        f'{bem("list", "Выберите чаты для рассылки")}\n'
        f'<b>Аккаунт:</b> {account_phone}\n'
        f'<b>Выбрано:</b> {len(selected_chats)} из 50 макс.\n'
        f'<b>Страница:</b> {current_page + 1} из {total_pages}'
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
async def toggle_chat_selection(callback: CallbackQuery, state: FSMContext) -> None:
    """Переключение выбора чата."""
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
async def change_page(callback: CallbackQuery, state: FSMContext) -> None:
    """Смена страницы."""
    page = int(callback.data.split("_")[1])
    await state.update_data(current_page=page)
    await show_chats_page(callback.message, state)
    await callback.answer()

@dp.callback_query(StateFilter(MailingSetup.selecting_chats), F.data == "confirm_chats")
async def confirm_chat_selection(callback: CallbackQuery, state: FSMContext) -> None:
    """Подтверждение выбора чатов."""
    data = await state.get_data()
    
    if not data['selected_chats']:
        await callback.answer("Выберите хотя бы один чат!", show_alert=True)
        return
    
    await callback.message.delete()
    await callback.message.answer(
        f'{bem("check", "Выбрано чатов:")} <b>{len(data["selected_chats"])}</b>\n'
        f'{bem("info", "Введите задержку между циклами (сек, по умолчанию 30):")}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(MailingSetup.waiting_for_delay)
    await callback.answer()

@dp.callback_query(F.data == "cancel_mailing")
async def cancel_mailing_any(callback: CallbackQuery, state: FSMContext) -> None:
    """Отмена рассылки на любом этапе."""
    if await state.get_state():
        await callback.message.delete()
        await callback.message.answer(
            f'{bem("cross", "Рассылка отменена.")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_mailing_keyboard()
        )
        await state.clear()
    await callback.answer()

@dp.message(MailingSetup.waiting_for_delay)
async def process_delay(message: Message, state: FSMContext) -> None:
    """Обработка задержки."""
    try:
        delay = int(message.text.strip()) if message.text.strip() else 30
        if delay < 0:
            raise ValueError
    except ValueError:
        await message.answer(
            f'{bem("cross", "Введите целое число (секунды).")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        return
    
    await state.update_data(delay=delay)
    await message.answer(
        f'{bem("info", "Введите количество циклов (по умолчанию 10):")}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(MailingSetup.waiting_for_cycles)

@dp.message(MailingSetup.waiting_for_cycles)
async def process_cycles(message: Message, state: FSMContext) -> None:
    """Обработка количества циклов."""
    try:
        cycles = int(message.text.strip()) if message.text.strip() else 10
        if cycles < 1:
            raise ValueError
    except ValueError:
        await message.answer(
            f'{bem("cross", "Введите целое положительное число.")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        return
    
    await state.update_data(cycles=cycles)
    await message.answer(
        f'{bem("write", "Введите текст сообщения в формате HTML:")}\n\n'
        f'{bem("info", "Поддерживаемые теги:")}\n'
        f'<code>&lt;b&gt;жирный&lt;/b&gt;</code>\n'
        f'<code>&lt;i&gt;курсив&lt;/i&gt;</code>\n'
        f'<code>&lt;u&gt;подчеркнутый&lt;/u&gt;</code>\n'
        f'<code>&lt;s&gt;зачеркнутый&lt;/s&gt;</code>\n'
        f'<code>&lt;a href="..."&gt;ссылка&lt;/a&gt;</code>\n'
        f'<code>&lt;blockquote&gt;цитата&lt;/blockquote&gt;</code>\n\n'
        f'{bi("Текст будет отправлен с полной поддержкой HTML форматирования.")}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(MailingSetup.waiting_for_message)

@dp.message(MailingSetup.waiting_for_message)
async def process_message_text(message: Message, state: FSMContext) -> None:
    """Обработка текста сообщения."""
    message_text = message.html_text if message.html_text else message.text
    
    if not message_text or not message_text.strip():
        await message.answer(
            f'{bem("cross", "Сообщение не может быть пустым!")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        return
    
    await state.update_data(message_html=message_text)
    data = await state.get_data()
    
    await message.answer(
        f'{bem("info", "Подтвердите рассылку:")}\n\n'
        f'<b>Аккаунт:</b> {data.get("account_phone")}\n'
        f'<b>Чатов:</b> {len(data["selected_chats"])}\n'
        f'<b>Задержка:</b> {data["delay"]} сек\n'
        f'<b>Циклов:</b> {data["cycles"]}\n\n'
        f'{bem("info", "Предпросмотр сообщения:")}',
        parse_mode=ParseMode.HTML
    )
    
    try:
        await message.answer(message_text, parse_mode=ParseMode.HTML)
    except:
        await message.answer(
            f'{bem("cross", "Ошибка в HTML разметке!")}\n'
            f'{bi("Проверьте правильность тегов.")}',
            parse_mode=ParseMode.HTML
        )
        return
    
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Запустить", callback_data="start_mailing"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_mailing")
    )
    
    await message.answer(
        f'{bem("info", "Подтвердите запуск:")}',
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )
    await state.set_state(MailingSetup.confirming)

@dp.callback_query(StateFilter(MailingSetup.confirming), F.data == "start_mailing")
async def start_mailing_process(callback: CallbackQuery, state: FSMContext) -> None:
    """Запуск процесса рассылки."""
    data = await state.get_data()
    await callback.message.delete()
    
    client = active_clients.get(data['account_id'])
    if not client:
        await callback.message.answer(
            f'{bem("cross", "Клиент не найден!")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_mailing_keyboard()
        )
        return
    
    settings = await Database.get_user_settings(callback.from_user.id)
    notifications_enabled = settings['notifications_enabled']
    selected_chats = data['selected_chats']
    
    mailing_id = await Database.add_mailing(
        account_id=data['account_id'],
        chats=selected_chats,
        delay=data['delay'],
        cycles=data['cycles'],
        message_html=data['message_html'],
        notifications_enabled=notifications_enabled
    )
    
    task = asyncio.create_task(execute_mailing(
        bot=callback.bot,
        chat_id=callback.message.chat.id,
        mailing_id=mailing_id,
        client=client,
        account_phone=data.get('account_phone', 'Unknown'),
        chats=selected_chats,
        delay=data['delay'],
        cycles=data['cycles'],
        message_html=data['message_html'],
        notifications_enabled=notifications_enabled
    ))
    active_mailing_tasks[mailing_id] = task
    
    await callback.message.answer(
        f'{bem("rocket", "Рассылка запущена!")}\n'
        f'<b>ID:</b> #{mailing_id}\n'
        f'<b>Аккаунт:</b> {data.get("account_phone")}\n'
        f'<b>Чатов:</b> {len(selected_chats)}\n'
        f'<b>Циклов:</b> {data["cycles"]}\n'
        f'<b>Задержка:</b> {data["delay"]} сек\n'
        f'<b>Уведомления:</b> {"ВКЛ" if notifications_enabled else "ВЫКЛ"}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_mailing_keyboard()
    )
    
    await state.clear()
    await callback.answer()

# ============================================================
# ВЫПОЛНЕНИЕ РАССЫЛКИ
# ============================================================

async def execute_mailing(
    bot: Bot,
    chat_id: int,
    mailing_id: int,
    client: TelegramClient,
    account_phone: str,
    chats: List[str],
    delay: int,
    cycles: int,
    message_html: str,
    notifications_enabled: bool = True
) -> None:
    """
    Выполнение рассылки в фоновом режиме.
    Отправляет сообщения во все выбранные чаты циклически.
    
    Args:
        bot: Экземпляр бота aiogram
        chat_id: ID чата для уведомлений
        mailing_id: ID рассылки в БД
        client: Клиент Telethon
        account_phone: Номер телефона аккаунта
        chats: Список ID чатов
        delay: Задержка между циклами
        cycles: Количество циклов
        message_html: Текст сообщения в HTML
        notifications_enabled: Отправлять ли уведомления о прогрессе
    """
    logger.info(f"Начало рассылки #{mailing_id}: {len(chats)} чатов, {cycles} циклов")
    
    try:
        for cycle in range(1, cycles + 1):
            # Проверка на остановку
            if mailing_id not in active_mailing_tasks:
                if notifications_enabled:
                    await bot.send_message(
                        chat_id,
                        f'{bem("stop", f"Рассылка #{mailing_id} остановлена")}\n'
                        f'<b>Аккаунт:</b> {account_phone}',
                        parse_mode=ParseMode.HTML
                    )
                await Database.update_mailing_status(mailing_id, 'stopped')
                return
            
            # Обновление статуса
            await Database.update_mailing_status(mailing_id, 'running', current_cycle=cycle)
            
            # Уведомление о начале цикла
            if notifications_enabled:
                await bot.send_message(
                    chat_id,
                    f'{bem("send", f"Рассылка #{mailing_id}")}\n'
                    f'<b>Аккаунт:</b> {account_phone}\n'
                    f'<b>Цикл:</b> {cycle} из {cycles}\n'
                    f'<b>Чатов:</b> {len(chats)}',
                    parse_mode=ParseMode.HTML
                )
            
            success_count = 0
            fail_count = 0
            
            # Отправка во все чаты
            for chat_id_to_send in chats:
                if mailing_id not in active_mailing_tasks:
                    break
                
                if await send_message_to_chat(client, chat_id_to_send, message_html):
                    success_count += 1
                else:
                    fail_count += 1
                
                # Небольшая пауза между отправками
                await asyncio.sleep(0.5)
            
            # Уведомление о завершении цикла
            if notifications_enabled:
                await bot.send_message(
                    chat_id,
                    f'{bem("stats", f"Цикл {cycle} завершен")}\n'
                    f'<b>Аккаунт:</b> {account_phone}\n'
                    f'<b>Успешно:</b> {success_count}\n'
                    f'<b>Ошибок:</b> {fail_count}',
                    parse_mode=ParseMode.HTML
                )
            
            # Обновление статистики
            await Database.update_mailing_status(
                mailing_id, 'running',
                success_count=success_count,
                fail_count=fail_count
            )
            
            # Задержка между циклами
            if cycle < cycles and mailing_id in active_mailing_tasks:
                if notifications_enabled:
                    await bot.send_message(
                        chat_id,
                        f'{bem("clock", f"Ожидание {delay} секунд...")}\n'
                        f'<b>Осталось циклов:</b> {cycles - cycle}',
                        parse_mode=ParseMode.HTML
                    )
                
                # Разбиваем ожидание для возможности остановки
                for _ in range(delay):
                    if mailing_id not in active_mailing_tasks:
                        break
                    await asyncio.sleep(1)
        
        # Завершение рассылки
        if mailing_id in active_mailing_tasks:
            if notifications_enabled:
                await bot.send_message(
                    chat_id,
                    f'{bem("celebration", f"Рассылка #{mailing_id} завершена!")}\n'
                    f'<b>Аккаунт:</b> {account_phone}\n'
                    f'<b>Всего циклов:</b> {cycles}',
                    parse_mode=ParseMode.HTML
                )
            await Database.update_mailing_status(mailing_id, 'completed')
        
    except Exception as e:
        logger.error(f"Ошибка в рассылке #{mailing_id}: {e}")
        if notifications_enabled:
            await bot.send_message(
                chat_id,
                f'{bem("cross", f"Ошибка в рассылке #{mailing_id}")}\n'
                f'<b>{str(e)[:200]}</b>',
                parse_mode=ParseMode.HTML
            )
        await Database.update_mailing_status(mailing_id, 'error')
    finally:
        if mailing_id in active_mailing_tasks:
            del active_mailing_tasks[mailing_id]

# ============================================================
# ОСТАНОВКА РАССЫЛКИ И СТАТУС
# ============================================================

@dp.message(F.text == "Остановить рассылку")
async def stop_mailing_handler(message: Message) -> None:
    """Обработчик остановки рассылки."""
    if not active_mailing_tasks:
        await message.answer(
            f'{bem("info", "Нет активных рассылок.")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        return
    
    builder = InlineKeyboardBuilder()
    for mailing_id in list(active_mailing_tasks.keys()):
        mailing = await Database.get_mailing_by_id(mailing_id)
        if mailing:
            account = await Database.get_account_by_id(mailing['account_id'])
            phone = account['phone'] if account else "Unknown"
            builder.row(InlineKeyboardButton(
                text=f"Остановить #{mailing_id} ({phone}, цикл {mailing['current_cycle']})",
                callback_data=f"stop_mailing_{mailing_id}"
            ))
    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_stop"))
    
    await message.answer(
        f'{bem("stop", "Выберите рассылку для остановки:")}',
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )

@dp.callback_query(F.data.startswith("stop_mailing_"))
async def confirm_stop_mailing(callback: CallbackQuery) -> None:
    """Подтверждение остановки рассылки."""
    mailing_id = int(callback.data.split("_")[2])
    
    if mailing_id in active_mailing_tasks:
        active_mailing_tasks[mailing_id].cancel()
        del active_mailing_tasks[mailing_id]
        
        await callback.message.delete()
        await callback.message.answer(
            f'{bem("stop", f"Рассылка #{mailing_id} остановлена!")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
    else:
        await callback.answer("Рассылка уже завершена", show_alert=True)
    
    await callback.answer()

@dp.callback_query(F.data == "cancel_stop")
async def cancel_stop_mailing(callback: CallbackQuery) -> None:
    """Отмена остановки."""
    await callback.message.delete()
    await callback.answer()

@dp.message(F.text == "Статус рассылки")
async def show_mailing_status(message: Message) -> None:
    """Отображение статуса активных рассылок."""
    if not active_mailing_tasks:
        await message.answer(
            f'{bem("info", "Нет активных рассылок.")}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_mailing_keyboard()
        )
        return
    
    text = f'{bem("stats", "Активные рассылки:")}\n\n'
    
    for mailing_id in list(active_mailing_tasks.keys()):
        mailing = await Database.get_mailing_by_id(mailing_id)
        if mailing:
            account = await Database.get_account_by_id(mailing['account_id'])
            phone = account['phone'] if account else "Unknown"
            notif = "ВКЛ" if mailing.get('notifications_enabled', True) else "ВЫКЛ"
            
            text += (
                f'<b>📨 Рассылка #{mailing_id}</b>\n'
                f'<b>• Аккаунт:</b> {phone}\n'
                f'<b>• Статус:</b> {mailing["status"]}\n'
                f'<b>• Цикл:</b> {mailing["current_cycle"]} из {mailing["cycles"]}\n'
                f'<b>• Чатов:</b> {len(mailing["chats"].split(","))}\n'
                f'<b>• Задержка:</b> {mailing["delay"]} сек\n'
                f'<b>• Уведомления:</b> {notif}\n\n'
            )
    
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=get_mailing_keyboard())

# ============================================================
# СЛУЖЕБНЫЕ ОБРАБОТЧИКИ
# ============================================================

@dp.callback_query(F.data == "ignore")
async def ignore_callback(callback: CallbackQuery) -> None:
    """Игнорирование служебных callback."""
    await callback.answer()

@dp.message()
async def unknown_message(message: Message) -> None:
    """Обработчик неизвестных сообщений."""
    if message.text and message.text.startswith('/'):
        return
    logger.debug(f"Неизвестное сообщение: {message.text[:50] if message.text else '...'}")

# ============================================================
# ГЛАВНАЯ ФУНКЦИЯ ЗАПУСКА
# ============================================================

async def main() -> None:
    """
    Главная функция запуска бота.
    Инициализирует БД, восстанавливает автоответчики и запускает поллинг.
    """
    try:
        logger.info("=" * 50)
        logger.info("Запуск Telegram Bot для массовых рассылок v5.4")
        logger.info("=" * 50)
        
        # Инициализация БД
        logger.info("Подключение к PostgreSQL...")
        await Database.init_pool()
        
        # Восстановление автоответчиков
        logger.info("Восстановление автоответчиков...")
        await restore_all_auto_replies()
        
        # Запуск бота
        logger.info("Запуск поллинга бота...")
        await dp.start_polling(bot)
        
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске: {e}")
        raise
    finally:
        # Очистка ресурсов
        logger.info("Очистка ресурсов...")
        
        # Остановка автоответчиков
        for account_id in list(auto_reply_clients.keys()):
            await stop_auto_reply(account_id)
        
        # Отключение активных клиентов
        for client in active_clients.values():
            try:
                await client.disconnect()
            except:
                pass
        active_clients.clear()
        
        # Отключение клиентов авторизации
        for client in auth_clients.values():
            try:
                await client.disconnect()
            except:
                pass
        auth_clients.clear()
        
        # Закрытие пула БД
        if db_pool:
            await db_pool.close()
        
        logger.info("Бот остановлен")

if __name__ == "__main__":
    print("=" * 60)
    print("  Telegram Bot для массовых рассылок v5.4")
    print("  PostgreSQL + aiogram 3.x + Telethon")
    print("=" * 60)
    print(f"  API ID: {API_ID}")
    print(f"  Токен бота: {'*' * 20}")
    print(f"  База данных: PostgreSQL")
    print("=" * 60)
    print("  Функции:")
    print("  • Менеджер аккаунтов (добавление/удаление)")
    print("  • Массовые рассылки с циклами")
    print("  • Автоответчик на личные сообщения")
    print("  • Проверка спам-блока через @spambot")
    print("  • Вступление в чаты по username")
    print("  • Поддержка супергрупп")
    print("  • Премиум эмодзи во всех элементах")
    print("  • Жирный шрифт во всех сообщениях")
    print("=" * 60)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nБот остановлен пользователем")
    except Exception as e:
        print(f"\nКритическая ошибка: {e}")
        logger.critical(f"Необработанная ошибка: {e}", exc_info=True)
