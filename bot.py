"""
Telegram Bot для массовых рассылок с PostgreSQL
- Выбор аккаунта при запуске рассылки
- Полная поддержка HTML форматирования
- Добавление аккаунтов через файл сессии Telethon
- Вступление в чаты по username
- Проверка спам-блока через @spambot
- Автоответчик на личные сообщения
- Настройка уведомлений
"""

import asyncio
import os
import logging
import re
import json
import tempfile
from typing import List, Dict, Optional
from datetime import datetime
import asyncpg

from aiogram import Bot, Dispatcher, types, F
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardButton, Message, CallbackQuery
)
from aiogram.filters import Command, StateFilter
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ParseMode

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import User, Chat, Channel
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.functions.channels import JoinChannelRequest

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
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/telegram_bot")

if not BOT_TOKEN:
    raise ValueError("Не установлена переменная окружения BOT_TOKEN")

# ID премиум эмодзи
EMOJI = {
    "settings": "5870982283724328568",
    "profile": "5870994129244131212",
    "people": "5870772616305839506",
    "file": "5870528606328852614",
    "home": "5873147866364514353",
    "lock_closed": "6037249452824072506",
    "check": "5870633910337015697",
    "cross": "5870657884844462243",
    "trash": "5870875489362513438",
    "info": "6028435952299413210",
    "bot": "6030400221232501136",
    "send": "5963103826075456248",
    "download": "6039802767931871481",
    "clock": "5983150113483134607",
    "celebration": "6041731551845159060",
    "write": "5870753782874246579",
    "back": "5345906554510012647",
    "plus": "5870633910337015697",
    "list": "5870772616305839506",
    "rocket": "5963103826075456248",
    "stop": "5870657884844462243",
    "stats": "5870921681735781843",
    "join": "6039450962865688331",
    "spam": "6037249452824072506",
    "reply": "6039422865189638057",
    "robot": "6030400221232501136"
}

# Глобальные переменные
active_mailing_tasks: Dict[int, asyncio.Task] = {}
auth_clients: Dict[int, TelegramClient] = {}
active_clients: Dict[int, TelegramClient] = {}
auto_reply_clients: Dict[int, TelegramClient] = {}
auto_reply_tasks: Dict[int, asyncio.Task] = {}
auto_reply_settings: Dict[int, Dict] = {}
db_pool: asyncpg.Pool = None

# Инициализация клиентов
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ========== Работа с PostgreSQL ==========

class Database:
    @staticmethod
    async def init_pool():
        global db_pool
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        await Database.init_tables()

    @staticmethod
    async def init_tables():
        async with db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    id SERIAL PRIMARY KEY,
                    phone TEXT NOT NULL,
                    session_string TEXT NOT NULL,
                    has_2fa BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS mailings (
                    id SERIAL PRIMARY KEY,
                    account_id INTEGER REFERENCES accounts(id),
                    chats TEXT NOT NULL,
                    delay INTEGER DEFAULT 30,
                    cycles INTEGER DEFAULT 10,
                    message_html TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    current_cycle INTEGER DEFAULT 0,
                    notifications_enabled BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS user_settings (
                    user_id BIGINT PRIMARY KEY,
                    notifications_enabled BOOLEAN DEFAULT TRUE
                );

                CREATE TABLE IF NOT EXISTS auto_replies (
                    id SERIAL PRIMARY KEY,
                    account_id INTEGER REFERENCES accounts(id) UNIQUE,
                    reply_text TEXT NOT NULL,
                    is_active BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

    @staticmethod
    async def add_account(phone: str, session_string: str, has_2fa: bool = False) -> int:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "INSERT INTO accounts (phone, session_string, has_2fa) VALUES ($1, $2, $3) RETURNING id",
                phone, session_string, has_2fa
            )
            return row['id']

    @staticmethod
    async def add_account_from_file(phone: str, session_string: str) -> int:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "INSERT INTO accounts (phone, session_string, has_2fa) VALUES ($1, $2, FALSE) RETURNING id",
                phone, session_string
            )
            return row['id']

    @staticmethod
    async def get_accounts() -> List[Dict]:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM accounts ORDER BY created_at DESC")
            return [dict(row) for row in rows]

    @staticmethod
    async def get_account_by_id(account_id: int) -> Optional[Dict]:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM accounts WHERE id = $1", account_id)
            return dict(row) if row else None

    @staticmethod
    async def delete_account(account_id: int):
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM auto_replies WHERE account_id = $1", account_id)
            await conn.execute("DELETE FROM accounts WHERE id = $1", account_id)

    @staticmethod
    async def set_auto_reply(account_id: int, reply_text: str, is_active: bool = True):
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO auto_replies (account_id, reply_text, is_active) VALUES ($1, $2, $3) "
                "ON CONFLICT (account_id) DO UPDATE SET reply_text = $2, is_active = $3",
                account_id, reply_text, is_active
            )

    @staticmethod
    async def get_auto_reply(account_id: int) -> Optional[Dict]:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM auto_replies WHERE account_id = $1 AND is_active = TRUE", account_id
            )
            return dict(row) if row else None

    @staticmethod
    async def toggle_auto_reply(account_id: int, is_active: bool):
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE auto_replies SET is_active = $1 WHERE account_id = $2",
                is_active, account_id
            )

    @staticmethod
    async def get_all_active_auto_replies() -> List[Dict]:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT ar.*, a.phone, a.session_string FROM auto_replies ar "
                "JOIN accounts a ON ar.account_id = a.id WHERE ar.is_active = TRUE"
            )
            return [dict(row) for row in rows]

    @staticmethod
    async def add_mailing(account_id: int, chats: List[str], delay: int = 30, 
                         cycles: int = 10, message_html: str = "",
                         notifications_enabled: bool = True) -> int:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "INSERT INTO mailings (account_id, chats, delay, cycles, message_html, status, notifications_enabled) "
                "VALUES ($1, $2, $3, $4, $5, 'running', $6) RETURNING id",
                account_id, ",".join(chats), delay, cycles, message_html, notifications_enabled
            )
            return row['id']

    @staticmethod
    async def update_mailing_status(mailing_id: int, status: str, current_cycle: int = None):
        async with db_pool.acquire() as conn:
            if current_cycle is not None:
                await conn.execute(
                    "UPDATE mailings SET status = $1, current_cycle = $2 WHERE id = $3",
                    status, current_cycle, mailing_id
                )
            else:
                await conn.execute(
                    "UPDATE mailings SET status = $1 WHERE id = $2",
                    status, mailing_id
                )

    @staticmethod
    async def get_mailing_by_id(mailing_id: int) -> Optional[Dict]:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM mailings WHERE id = $1", mailing_id)
            return dict(row) if row else None

    @staticmethod
    async def get_user_settings(user_id: int) -> Dict:
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
    async def update_user_settings(user_id: int, notifications_enabled: bool):
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO user_settings (user_id, notifications_enabled) VALUES ($1, $2) "
                "ON CONFLICT (user_id) DO UPDATE SET notifications_enabled = $2",
                user_id, notifications_enabled
            )

# ========== Состояния FSM ==========

class AddAccount(StatesGroup):
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_2fa = State()

class AddAccountFile(StatesGroup):
    waiting_for_file = State()
    waiting_for_phone = State()

class JoinChats(StatesGroup):
    selecting_account = State()
    waiting_for_links = State()

class MailingSetup(StatesGroup):
    selecting_account = State()
    selecting_chats = State()
    waiting_for_delay = State()
    waiting_for_cycles = State()
    waiting_for_message = State()
    confirming = State()

class AutoReplySetup(StatesGroup):
    selecting_account = State()
    waiting_for_text = State()

# ========== Клавиатуры ==========

def get_main_keyboard():
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

def get_account_manager_keyboard():
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

def get_mailing_keyboard():
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

def get_auto_reply_keyboard():
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

def get_back_keyboard():
    return {
        "keyboard": [
            [{"text": "Назад", "icon_custom_emoji_id": EMOJI["back"]}]
        ],
        "resize_keyboard": True
    }

# ========== Telethon функции ==========

async def create_telethon_client(session_string: str = None) -> TelegramClient:
    client = TelegramClient(
        StringSession(session_string) if session_string else StringSession(),
        API_ID,
        API_HASH,
        system_version="4.16.30-vxCUSTOM",
        device_model="Python Telethon",
        app_version="1.0.0",
        connection_retries=3,
        retry_delay=2,
        timeout=30
    )
    await client.connect()
    return client

async def read_session_file(file_path: str) -> Optional[str]:
    logger.info(f"Попытка чтения файла: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if content and len(content) > 50:
                logger.info(f"Файл прочитан как текст UTF-8, длина: {len(content)}")
                return content
    except UnicodeDecodeError:
        logger.info("Файл не UTF-8 текст")
    except Exception as e:
        logger.warning(f"Ошибка чтения как текст: {e}")
    
    try:
        import base64
        with open(file_path, 'rb') as f:
            content = f.read()
            logger.info(f"Файл прочитан как бинарный, размер: {len(content)} байт")
            
            if content[:16] == b'SQLite format 3\x00':
                logger.info("Обнаружен SQLite формат .session")
                encoded = base64.b64encode(content).decode('ascii')
                return encoded
            
            for encoding in ['utf-8', 'latin-1', 'cp1251']:
                try:
                    text = content.decode(encoding).strip()
                    if text and len(text) > 50:
                        logger.info(f"Файл прочитан как {encoding}, длина: {len(text)}")
                        return text
                except:
                    continue
    except Exception as e:
        logger.error(f"Ошибка чтения бинарного файла: {e}")
    
    return None

async def verify_session_string(session_string: str) -> Optional[Dict]:
    client = None
    try:
        client = await create_telethon_client(session_string)
        
        try:
            is_authorized = await asyncio.wait_for(
                client.is_user_authorized(),
                timeout=10.0
            )
            
            if is_authorized:
                me = await asyncio.wait_for(
                    client.get_me(),
                    timeout=10.0
                )
                phone = me.phone if hasattr(me, 'phone') and me.phone else f"User_{me.id}"
                logger.info(f"Сессия валидна, пользователь: {phone}")
                return {"phone": phone, "valid": True}
            else:
                logger.warning("Сессия не авторизована")
                return None
                
        except asyncio.TimeoutError:
            logger.error("Таймаут при проверке сессии")
            return None
        except Exception as e:
            logger.error(f"Ошибка при проверке сессии: {e}")
            return None
            
    except Exception as e:
        logger.error(f"Ошибка создания клиента: {e}")
        return None
    finally:
        if client:
            try:
                await asyncio.wait_for(client.disconnect(), timeout=5.0)
            except:
                pass

async def get_dialogs_from_client(client: TelegramClient) -> List[Dict]:
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
    try:
        entity = await client.get_entity(int(chat_id))
        await client.send_message(entity, message, parse_mode='HTML')
        return True
    except Exception as e:
        logger.error(f"Ошибка отправки в чат {chat_id}: {e}")
        return False

async def join_chat_by_username(client: TelegramClient, username: str) -> Dict:
    try:
        username = username.strip().lstrip('@')
        
        try:
            entity = await client.get_entity(username)
        except Exception as e:
            return {"success": False, "message": f"Не найден: {username}"}
        
        try:
            if hasattr(entity, 'broadcast') or hasattr(entity, 'megagroup'):
                await client(JoinChannelRequest(entity))
                return {"success": True, "message": f"Вступили в {getattr(entity, 'title', username)}"}
            else:
                return {"success": False, "message": f"Не канал/группа: {username}"}
        except Exception as e:
            if "already participant" in str(e).lower() or "user_already_participant" in str(e).lower():
                return {"success": True, "message": f"Уже состоим в {username}"}
            return {"success": False, "message": str(e)}
            
    except Exception as e:
        return {"success": False, "message": str(e)}

async def check_spam_block(client: TelegramClient) -> Dict:
    try:
        spambot = await client.get_entity('@spambot')
        await client.send_message(spambot, '/start')
        await asyncio.sleep(5)
        
        messages = await client.get_messages(spambot, limit=3)
        
        for msg in messages:
            if msg.message:
                text = msg.message.lower()
                logger.info(f"Ответ от @spambot: {text[:100]}")
                
                if 'навсегда' in text or 'never' in text or 'permanently' in text:
                    return {"blocked": True, "permanent": True, "message": "Аккаунт заблокирован навсегда!"}
                elif 'свободен' in text or 'free' in text or 'нет ограничений' in text or 'no limits' in text:
                    return {"blocked": False, "message": "Спам-блок отсутствует"}
        
        return {"blocked": False, "message": "Спам-блок не обнаружен"}
        
    except Exception as e:
        logger.error(f"Ошибка проверки спам-блока: {e}")
        return {"blocked": False, "message": f"Ошибка проверки: {str(e)}", "error": True}

# ========== Автоответчик ==========

async def start_auto_reply(account_id: int, account_phone: str, session_string: str, reply_text: str, notifications_chat_id: int = None):
    """Запуск автоответчика для аккаунта"""
    
    if account_id in auto_reply_tasks:
        auto_reply_tasks[account_id].cancel()
        try:
            await auto_reply_tasks[account_id]
        except:
            pass
    
    if account_id in auto_reply_clients:
        try:
            await auto_reply_clients[account_id].disconnect()
        except:
            pass
    
    client = await create_telethon_client(session_string)
    
    if not await client.is_user_authorized():
        logger.error(f"Ошибка авторизации для автоответчика {account_phone}")
        return False
    
    auto_reply_clients[account_id] = client
    auto_reply_settings[account_id] = {
        'phone': account_phone,
        'reply_text': reply_text,
        'notifications_chat_id': notifications_chat_id
    }
    
    @client.on(events.NewMessage(incoming=True))
    async def auto_reply_handler(event):
        try:
            if event.is_private:
                sender = await event.get_sender()
                
                if sender.is_self or sender.bot:
                    return
                
                logger.info(f"Автоответчик {account_phone}: ответ на сообщение от {sender.id}")
                
                await event.reply(reply_text, parse_mode='HTML')
                
                if notifications_chat_id:
                    try:
                        sender_name = f"{sender.first_name or ''} {sender.last_name or ''}".strip()
                        await bot.send_message(
                            notifications_chat_id,
                            f'<b><tg-emoji emoji-id="{EMOJI["reply"]}">📣</tg-emoji> Автоответчик сработал!</b>\n'
                            f'Аккаунт: {account_phone}\n'
                            f'Отправитель: {sender_name} (ID: {sender.id})\n'
                            f'Сообщение: {event.text[:100] if event.text else "[медиа]"}',
                            parse_mode=ParseMode.HTML
                        )
                    except Exception as e:
                        logger.error(f"Ошибка отправки уведомления: {e}")
                        
        except Exception as e:
            logger.error(f"Ошибка в автоответчике: {e}")
    
    logger.info(f"Автоответчик запущен для {account_phone}")
    return True

async def stop_auto_reply(account_id: int):
    """Остановка автоответчика"""
    if account_id in auto_reply_tasks:
        auto_reply_tasks[account_id].cancel()
        try:
            await auto_reply_tasks[account_id]
        except:
            pass
        del auto_reply_tasks[account_id]
    
    if account_id in auto_reply_clients:
        try:
            await auto_reply_clients[account_id].disconnect()
        except:
            pass
        del auto_reply_clients[account_id]
    
    if account_id in auto_reply_settings:
        del auto_reply_settings[account_id]
    
    logger.info(f"Автоответчик остановлен для аккаунта {account_id}")

async def restore_all_auto_replies():
    """Восстановление всех активных автоответчиков при запуске"""
    active_replies = await Database.get_all_active_auto_replies()
    
    for reply in active_replies:
        try:
            await start_auto_reply(
                account_id=reply['account_id'],
                account_phone=reply['phone'],
                session_string=reply['session_string'],
                reply_text=reply['reply_text']
            )
            logger.info(f"Восстановлен автоответчик для {reply['phone']}")
        except Exception as e:
            logger.error(f"Ошибка восстановления автоответчика для {reply['phone']}: {e}")

# ========== Обработчики команд ==========

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["bot"]}">🤖</tg-emoji> Добро пожаловать в бот для массовых рассылок!</b>\n'
        'Выберите нужный раздел:',
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )

@dp.message(F.text == "Назад")
async def go_back(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state:
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
    await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["profile"]}">👤</tg-emoji> Менеджер аккаунтов</b>\n'
        'Выберите действие:',
        parse_mode=ParseMode.HTML,
        reply_markup=get_account_manager_keyboard()
    )

@dp.message(F.text == "Автоответчик")
async def auto_reply_menu(message: Message):
    await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["reply"]}">📣</tg-emoji> Автоответчик</b>\n'
        'Автоматический ответ на личные сообщения.\n'
        'Выберите действие:',
        parse_mode=ParseMode.HTML,
        reply_markup=get_auto_reply_keyboard()
    )

@dp.message(F.text == "Добавить автоответчик")
async def add_auto_reply_start(message: Message, state: FSMContext):
    accounts = await Database.get_accounts()
    
    if not accounts:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Нет сохраненных аккаунтов!</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        return
    
    builder = InlineKeyboardBuilder()
    
    for account in accounts:
        button_text = f"{account['phone']} {'(2FA)' if account['has_2fa'] else ''}"
        builder.row(InlineKeyboardButton(
            text=button_text,
            callback_data=f"autoreply_account_{account['id']}"
        ))
    
    builder.row(InlineKeyboardButton(
        text="❌ Отмена",
        callback_data="cancel_autoreply"
    ))
    
    await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["profile"]}">👤</tg-emoji> Выберите аккаунт для автоответчика:</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )
    await state.set_state(AutoReplySetup.selecting_account)

@dp.callback_query(StateFilter(AutoReplySetup.selecting_account), F.data.startswith("autoreply_account_"))
async def select_autoreply_account(callback: CallbackQuery, state: FSMContext):
    account_id = int(callback.data.split("_")[2])
    account = await Database.get_account_by_id(account_id)
    
    if not account:
        await callback.answer("Аккаунт не найден!", show_alert=True)
        return
    
    await state.update_data(account_id=account_id, account_phone=account['phone'])
    
    await callback.message.delete()
    await callback.message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["write"]}">✍</tg-emoji> Введите текст автоответа (HTML поддерживается)</b>\n\n'
        '<i>Примеры:</i>\n'
        '<code>Привет! Спасибо за сообщение.</code>\n'
        '<code>&lt;b&gt;Здравствуйте!&lt;/b&gt; Я сейчас не в сети.</code>\n\n'
        f'<b>Аккаунт:</b> {account["phone"]}\n\n'
        '<i>Это сообщение будет отправляться на любое личное сообщение</i>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(AutoReplySetup.waiting_for_text)

@dp.message(AutoReplySetup.waiting_for_text)
async def process_autoreply_text(message: Message, state: FSMContext):
    reply_text = message.html_text if message.html_text else message.text
    
    if not reply_text or not reply_text.strip():
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Текст не может быть пустым</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        return
    
    data = await state.get_data()
    account_id = data['account_id']
    account_phone = data['account_phone']
    user_id = message.from_user.id
    
    await Database.set_auto_reply(account_id, reply_text, is_active=True)
    
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
                f'<b><tg-emoji emoji-id="{EMOJI["check"]}">✅</tg-emoji> Автоответчик запущен!</b>\n'
                f'Аккаунт: {account_phone}\n\n'
                '<i>Бот будет отвечать на все личные сообщения</i>',
                parse_mode=ParseMode.HTML,
                reply_markup=get_auto_reply_keyboard()
            )
        else:
            await message.answer(
                f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибка запуска автоответчика!</b>',
                parse_mode=ParseMode.HTML,
                reply_markup=get_auto_reply_keyboard()
            )
    else:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Аккаунт не найден!</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_auto_reply_keyboard()
        )
    
    await state.clear()

@dp.callback_query(F.data == "cancel_autoreply")
async def cancel_autoreply(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    await callback.message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Настройка автоответчика отменена</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_auto_reply_keyboard()
    )
    await state.clear()
    await callback.answer()

@dp.message(F.text == "Мои автоответчики")
async def list_auto_replies(message: Message):
    active_replies = await Database.get_all_active_auto_replies()
    
    if not active_replies:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["info"]}">ℹ</tg-emoji> Нет активных автоответчиков</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_auto_reply_keyboard()
        )
        return
    
    text = f'<b><tg-emoji emoji-id="{EMOJI["list"]}">👥</tg-emoji> Активные автоответчики:</b>\n\n'
    
    builder = InlineKeyboardBuilder()
    
    for reply in active_replies:
        status = "✅ Активен" if reply['is_active'] else "❌ Выключен"
        text += f"• {reply['phone']} - {status}\n"
        text += f"  Ответ: {reply['reply_text'][:50]}...\n\n"
        
        builder.row(InlineKeyboardButton(
            text=f"🛑 Выключить для {reply['phone']}",
            callback_data=f"toggle_autoreply_{reply['account_id']}_off"
        ))
    
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("toggle_autoreply_"))
async def toggle_auto_reply(callback: CallbackQuery):
    parts = callback.data.split("_")
    account_id = int(parts[2])
    action = parts[3]
    
    if action == "off":
        await Database.toggle_auto_reply(account_id, False)
        await stop_auto_reply(account_id)
        
        await callback.message.delete()
        await callback.message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["stop"]}">🛑</tg-emoji> Автоответчик выключен!</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_auto_reply_keyboard()
        )
    
    await callback.answer()

# ========== Настройки ==========

@dp.message(F.text == "Настройки")
async def settings_menu(message: Message):
    user_id = message.from_user.id
    settings = await Database.get_user_settings(user_id)
    
    notif_status = "ВКЛЮЧЕНЫ ✅" if settings['notifications_enabled'] else "ВЫКЛЮЧЕНЫ ❌"
    
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="🔔 Включить уведомления" if not settings['notifications_enabled'] else "🔕 Выключить уведомления",
        callback_data="toggle_notifications"
    ))
    
    await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["settings"]}">⚙</tg-emoji> Настройки</b>\n\n'
        f'Статус уведомлений: {notif_status}\n\n'
        '<i>При выключенных уведомлениях бот НЕ будет присылать сообщения о прогрессе рассылки</i>',
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )

@dp.callback_query(F.data == "toggle_notifications")
async def toggle_notifications(callback: CallbackQuery):
    user_id = callback.from_user.id
    settings = await Database.get_user_settings(user_id)
    new_status = not settings['notifications_enabled']
    
    await Database.update_user_settings(user_id, new_status)
    
    notif_status = "ВКЛЮЧЕНЫ ✅" if new_status else "ВЫКЛЮЧЕНЫ ❌"
    
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="🔔 Включить уведомления" if not new_status else "🔕 Выключить уведомления",
        callback_data="toggle_notifications"
    ))
    
    await callback.message.edit_text(
        f'<b><tg-emoji emoji-id="{EMOJI["settings"]}">⚙</tg-emoji> Настройки</b>\n\n'
        f'Статус уведомлений: {notif_status}\n\n'
        '<i>При выключенных уведомлениях бот НЕ будет присылать сообщения о прогрессе рассылки</i>',
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )
    await callback.answer()

# ========== Проверка спам-блока ==========

@dp.message(F.text == "Проверка спам-блока")
async def spam_check_start(message: Message):
    accounts = await Database.get_accounts()
    
    if not accounts:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Нет сохраненных аккаунтов!</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        return
    
    builder = InlineKeyboardBuilder()
    
    for account in accounts:
        builder.row(InlineKeyboardButton(
            text=f"{account['phone']} {'(2FA)' if account['has_2fa'] else ''}",
            callback_data=f"spamcheck_{account['id']}"
        ))
    
    builder.row(InlineKeyboardButton(
        text="❌ Отмена",
        callback_data="cancel_spamcheck"
    ))
    
    await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["spam"]}">🔒</tg-emoji> Выберите аккаунт для проверки спам-блока:</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )

@dp.callback_query(F.data.startswith("spamcheck_"))
async def do_spam_check(callback: CallbackQuery):
    account_id = int(callback.data.split("_")[1])
    account = await Database.get_account_by_id(account_id)
    
    if not account:
        await callback.answer("Аккаунт не найден!", show_alert=True)
        return
    
    await callback.message.delete()
    status_msg = await callback.message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["loading"]}">🔄</tg-emoji> Проверяю спам-блок для {account["phone"]}...</b>',
        parse_mode=ParseMode.HTML
    )
    
    try:
        client = await create_telethon_client(account['session_string'])
        
        if not await client.is_user_authorized():
            await status_msg.edit_text(
                f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибка авторизации!</b>',
                parse_mode=ParseMode.HTML
            )
            await client.disconnect()
            return
        
        result = await check_spam_block(client)
        await client.disconnect()
        
        if result.get('error'):
            await status_msg.edit_text(
                f'<b><tg-emoji emoji-id="{EMOJI["info"]}">ℹ</tg-emoji> Результат проверки:</b>\n\n'
                f'{result["message"]}\n\n'
                '<i>Возможно, @spambot недоступен или изменил формат ответов</i>',
                parse_mode=ParseMode.HTML,
                reply_markup=get_main_keyboard()
            )
        elif result['blocked']:
            if result.get('permanent'):
                await status_msg.edit_text(
                    f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">🚫</tg-emoji> Спам-блок обнаружен!</b>\n\n'
                    f'<b>Статус:</b> ЗАБЛОКИРОВАН НАВСЕГДА\n'
                    f'<b>Аккаунт:</b> {account["phone"]}\n\n'
                    '<i>Этот аккаунт нельзя использовать для рассылок</i>',
                    parse_mode=ParseMode.HTML,
                    reply_markup=get_main_keyboard()
                )
            else:
                await status_msg.edit_text(
                    f'<b><tg-emoji emoji-id="{EMOJI["clock"]}">⚠️</tg-emoji> Обнаружено ограничение!</b>\n\n'
                    f'<b>Аккаунт:</b> {account["phone"]}\n'
                    f'<b>Детали:</b> {result.get("details", result["message"])}\n\n'
                    '<i>Рекомендуется не использовать для рассылок до снятия ограничений</i>',
                    parse_mode=ParseMode.HTML,
                    reply_markup=get_main_keyboard()
                )
        else:
            await status_msg.edit_text(
                f'<b><tg-emoji emoji-id="{EMOJI["check"]}">✅</tg-emoji> Спам-блок не обнаружен!</b>\n\n'
                f'<b>Аккаунт:</b> {account["phone"]}\n'
                '<i>Аккаунт можно использовать для рассылок</i>',
                parse_mode=ParseMode.HTML,
                reply_markup=get_main_keyboard()
            )
        
    except Exception as e:
        logger.error(f"Ошибка проверки спам-блока: {e}")
        await status_msg.edit_text(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибка: {str(e)}</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )

@dp.callback_query(F.data == "cancel_spamcheck")
async def cancel_spam_check(callback: CallbackQuery):
    await callback.message.delete()
    await callback.message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Проверка отменена</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )
    await callback.answer()

# ========== Менеджер аккаунтов ==========

@dp.message(F.text == "Добавить аккаунт")
async def add_account_start(message: Message, state: FSMContext):
    await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["profile"]}">👤</tg-emoji> Введите номер телефона в международном формате</b>\n'
        'Например: +79991234567',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(AddAccount.waiting_for_phone)

@dp.message(AddAccount.waiting_for_phone)
async def process_phone(message: Message, state: FSMContext):
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
        client = await create_telethon_client()
        auth_clients[user_id] = client
        
        send_code_result = await client.send_code_request(phone)
        
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
    if user_id in auth_clients:
        try:
            await auth_clients[user_id].disconnect()
        except:
            pass
        del auth_clients[user_id]

@dp.message(AddAccount.waiting_for_code)
async def process_code(message: Message, state: FSMContext):
    code = message.text.strip()
    data = await state.get_data()
    user_id = message.from_user.id
    
    client = auth_clients.get(user_id)
    if not client:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Сессия авторизации истекла.</b>',
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
        except Exception as e:
            if "password" in str(e).lower() or "2fa" in str(e).lower():
                await state.update_data(session_string=client.session.save())
                await message.answer(
                    f'<b><tg-emoji emoji-id="{EMOJI["lock_closed"]}">🔒</tg-emoji> Требуется 2FA!</b>\n'
                    'Введите пароль:',
                    parse_mode=ParseMode.HTML,
                    reply_markup=get_back_keyboard()
                )
                await state.set_state(AddAccount.waiting_for_2fa)
                return
            raise
        
        session_string = client.session.save()
        await Database.add_account(data['phone'], session_string)
        
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["check"]}">✅</tg-emoji> Аккаунт добавлен!</b>\n'
            f'Номер: {data["phone"]}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_account_manager_keyboard()
        )
        
        await cleanup_auth_client(user_id)
        await state.clear()
        
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
    password = message.text.strip()
    data = await state.get_data()
    user_id = message.from_user.id
    
    client = auth_clients.get(user_id)
    if not client:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Сессия истекла.</b>',
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
            f'<b><tg-emoji emoji-id="{EMOJI["check"]}">✅</tg-emoji> Аккаунт добавлен с 2FA!</b>\n'
            f'Номер: {data["phone"]}',
            parse_mode=ParseMode.HTML,
            reply_markup=get_account_manager_keyboard()
        )
        
        await cleanup_auth_client(user_id)
        await state.clear()
        
    except Exception as e:
        logger.error(f"Ошибка при входе с 2FA: {e}")
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Неверный пароль!</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )

# ========== Добавление из файла ==========

@dp.message(F.text == "Добавить из файла")
async def add_account_file_start(message: Message, state: FSMContext):
    await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["file"]}">📁</tg-emoji> Отправьте файл сессии Telethon</b>\n\n'
        '<i>Поддерживаемые форматы:</i>\n'
        '• Файл .session (бинарный формат)\n'
        '• Текстовая строка сессии\n\n'
        '<b>Отправьте файл как документ</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(AddAccountFile.waiting_for_file)

@dp.message(AddAccountFile.waiting_for_file)
async def process_account_file(message: Message, state: FSMContext):
    user_id = message.from_user.id
    session_string = None
    
    if message.document:
        document = message.document
        file_name = document.file_name or "session_file"
        logger.info(f"Получен документ: {file_name}, размер: {document.file_size}")
        
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.session', delete=False) as tmp_file:
            temp_path = tmp_file.name
        
        try:
            await bot.download(document, destination=temp_path)
            logger.info(f"Файл сохранен: {temp_path}")
            
            session_string = await read_session_file(temp_path)
            
            try:
                os.unlink(temp_path)
            except:
                pass
            
        except Exception as e:
            logger.error(f"Ошибка скачивания файла: {e}")
            await message.answer(
                f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибка при получении файла!</b>',
                parse_mode=ParseMode.HTML,
                reply_markup=get_account_manager_keyboard()
            )
            await state.clear()
            return
    
    elif message.text:
        text = message.text.strip()
        if len(text) > 50:
            logger.info(f"Получен текст, длина: {len(text)}")
            session_string = text
    
    if not session_string:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Не удалось извлечь данные!</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_account_manager_keyboard()
        )
        await state.clear()
        return
    
    status_msg = await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["loading"]}">🔄</tg-emoji> Проверяю сессию...</b>',
        parse_mode=ParseMode.HTML
    )
    
    result = await verify_session_string(session_string)
    
    if result and result['valid']:
        await state.update_data(session_string=session_string, detected_phone=result['phone'])
        
        builder = InlineKeyboardBuilder()
        builder.row(InlineKeyboardButton(
            text=f"Использовать: {result['phone']}",
            callback_data="use_detected_phone"
        ))
        
        await status_msg.edit_text(
            f'<b><tg-emoji emoji-id="{EMOJI["check"]}">✅</tg-emoji> Сессия валидна!</b>\n'
            f'Обнаруженный номер: {result["phone"]}\n\n'
            'Введите номер телефона или нажмите кнопку:',
            parse_mode=ParseMode.HTML,
            reply_markup=builder.as_markup()
        )
        await state.set_state(AddAccountFile.waiting_for_phone)
    else:
        await status_msg.edit_text(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Сессия невалидна!</b>\n'
            'Проверьте данные и попробуйте снова.\n\n'
            '<i>Возможные причины:</i>\n'
            '• Файл поврежден\n'
            '• Сессия устарела\n'
            '• Неверный формат файла',
            parse_mode=ParseMode.HTML,
            reply_markup=get_account_manager_keyboard()
        )
        await state.clear()

@dp.callback_query(StateFilter(AddAccountFile.waiting_for_phone), F.data == "use_detected_phone")
async def use_detected_phone(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    phone = data['detected_phone']
    
    await Database.add_account_from_file(phone, data['session_string'])
    
    await callback.message.delete()
    await callback.message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["check"]}">✅</tg-emoji> Аккаунт успешно добавлен!</b>\n'
        f'Номер: {phone}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_account_manager_keyboard()
    )
    await state.clear()
    await callback.answer()

@dp.message(AddAccountFile.waiting_for_phone)
async def process_account_phone(message: Message, state: FSMContext):
    data = await state.get_data()
    phone = message.text.strip()
    
    if not phone.startswith('+') or len(phone) < 10:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Неверный формат номера.</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        return
    
    await Database.add_account_from_file(phone, data['session_string'])
    
    await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["check"]}">✅</tg-emoji> Аккаунт успешно добавлен!</b>\n'
        f'Номер: {phone}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_account_manager_keyboard()
    )
    await state.clear()

@dp.message(F.text == "Список аккаунтов")
async def list_accounts(message: Message):
    accounts = await Database.get_accounts()
    
    if not accounts:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["info"]}">ℹ</tg-emoji> Нет сохраненных аккаунтов</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_account_manager_keyboard()
        )
        return
    
    text = f'<b><tg-emoji emoji-id="{EMOJI["list"]}">👥</tg-emoji> Список аккаунтов:</b>\n\n'
    
    builder = InlineKeyboardBuilder()
    
    for account in accounts:
        phone = account['phone']
        has_2fa = "🔒" if account['has_2fa'] else ""
        text += f"• {phone} {has_2fa}\n"
        
        builder.row(InlineKeyboardButton(
            text=f"🗑 Удалить {phone}",
            callback_data=f"delete_account_{account['id']}"
        ))
    
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("delete_account_"))
async def delete_account(callback: CallbackQuery):
    account_id = int(callback.data.split("_")[2])
    
    await stop_auto_reply(account_id)
    await Database.delete_account(account_id)
    
    if account_id in active_clients:
        try:
            await active_clients[account_id].disconnect()
        except:
            pass
        del active_clients[account_id]
    
    await callback.message.delete()
    await callback.message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["trash"]}">🗑</tg-emoji> Аккаунт удален!</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_account_manager_keyboard()
    )
    await callback.answer()

# ========== Вступление в чаты ==========

@dp.message(F.text == "Вступить в чаты")
async def join_chats_start(message: Message, state: FSMContext):
    accounts = await Database.get_accounts()
    
    if not accounts:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Нет сохраненных аккаунтов!</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        return
    
    builder = InlineKeyboardBuilder()
    
    for account in accounts:
        button_text = f"{account['phone']} {'(2FA)' if account['has_2fa'] else ''}"
        builder.row(InlineKeyboardButton(
            text=button_text,
            callback_data=f"join_account_{account['id']}"
        ))
    
    builder.row(InlineKeyboardButton(
        text="❌ Отмена",
        callback_data="cancel_join"
    ))
    
    await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["profile"]}">👤</tg-emoji> Выберите аккаунт для вступления в чаты:</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )
    await state.set_state(JoinChats.selecting_account)

@dp.callback_query(StateFilter(JoinChats.selecting_account), F.data.startswith("join_account_"))
async def select_join_account(callback: CallbackQuery, state: FSMContext):
    account_id = int(callback.data.split("_")[2])
    account = await Database.get_account_by_id(account_id)
    
    if not account:
        await callback.answer("Аккаунт не найден!", show_alert=True)
        return
    
    await state.update_data(account_id=account_id, account_phone=account['phone'])
    
    await callback.message.delete()
    await callback.message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["join"]}">🔗</tg-emoji> Отправьте ссылки на чаты</b>\n\n'
        '<i>Поддерживаемые форматы:</i>\n'
        '• @username\n'
        '• https://t.me/username\n'
        '• username\n\n'
        '<b>Отправьте ссылки (каждую с новой строки):</b>\n'
        '<code>@chat1\n@chat2\n@chat3</code>\n\n'
        f'<b>Аккаунт:</b> {account["phone"]}\n'
        '<b>Задержка:</b> 20 секунд между вступлениями',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(JoinChats.waiting_for_links)

@dp.message(JoinChats.waiting_for_links)
async def process_join_links(message: Message, state: FSMContext):
    text = message.text.strip()
    data = await state.get_data()
    account_id = data['account_id']
    account_phone = data['account_phone']
    
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
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Не найдено ссылок!</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        return
    
    status_msg = await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["loading"]}">🔄</tg-emoji> Начинаю вступление в чаты...</b>\n'
        f'Аккаунт: {account_phone}\n'
        f'Найдено ссылок: {len(links)}\n'
        f'Задержка: 20 секунд',
        parse_mode=ParseMode.HTML
    )
    
    try:
        if account_id not in active_clients:
            account = await Database.get_account_by_id(account_id)
            client = await create_telethon_client(account['session_string'])
            if not await client.is_user_authorized():
                await status_msg.edit_text(
                    f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибка авторизации!</b>',
                    parse_mode=ParseMode.HTML
                )
                await state.clear()
                return
            active_clients[account_id] = client
        else:
            client = active_clients[account_id]
        
        success_count = 0
        fail_count = 0
        failed_details = []
        
        for i, username in enumerate(links, 1):
            try:
                result = await join_chat_by_username(client, username)
                
                if result['success']:
                    success_count += 1
                else:
                    fail_count += 1
                    if len(failed_details) < 5:
                        failed_details.append(f"@{username}: {result['message']}")
                
                await status_msg.edit_text(
                    f'<b>Вступление в чаты...</b>\n'
                    f'Прогресс: {i}/{len(links)}\n'
                    f'✅ {success_count} | ❌ {fail_count}\n'
                    f'Задержка: 20 сек',
                    parse_mode=ParseMode.HTML
                )
                
                if i < len(links):
                    for sec in range(20, 0, -1):
                        await status_msg.edit_text(
                            f'<b>Вступление в чаты...</b>\n'
                            f'Прогресс: {i}/{len(links)}\n'
                            f'✅ {success_count} | ❌ {fail_count}\n'
                            f'Ожидание: {sec} сек',
                            parse_mode=ParseMode.HTML
                        )
                        await asyncio.sleep(1)
                
            except Exception as e:
                fail_count += 1
                logger.error(f"Ошибка вступления в {username}: {e}")
        
        report = (
            f'<b><tg-emoji emoji-id="{EMOJI["stats"]}">📊</tg-emoji> Результаты вступления:</b>\n\n'
            f'<b>Аккаунт:</b> {account_phone}\n'
            f'<tg-emoji emoji-id="{EMOJI["check"]}">✅</tg-emoji> Успешно: {success_count}\n'
            f'<tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибок: {fail_count}\n'
            f'Всего: {len(links)}'
        )
        
        if failed_details:
            report += '\n\n<b>Первые ошибки:</b>\n' + '\n'.join(failed_details[:3])
        
        await status_msg.edit_text(report, parse_mode=ParseMode.HTML)
        await message.answer(
            '<b>Операция завершена!</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Ошибка при вступлении: {e}")
        await status_msg.edit_text(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибка: {str(e)}</b>',
            parse_mode=ParseMode.HTML
        )
    finally:
        await state.clear()

@dp.callback_query(F.data == "cancel_join")
async def cancel_join(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    await callback.message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Операция отменена</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard()
    )
    await state.clear()
    await callback.answer()

# ========== Система рассылок ==========

@dp.message(F.text == "Рассылка")
async def mailing_menu(message: Message):
    await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["send"]}">📨</tg-emoji> Меню рассылки</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_mailing_keyboard()
    )

@dp.message(F.text == "Запустить рассылку")
async def start_mailing(message: Message, state: FSMContext):
    accounts = await Database.get_accounts()
    
    if not accounts:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Нет сохраненных аккаунтов!</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        return
    
    builder = InlineKeyboardBuilder()
    
    for account in accounts:
        button_text = f"{account['phone']} {'(2FA)' if account['has_2fa'] else ''}"
        builder.row(InlineKeyboardButton(
            text=button_text,
            callback_data=f"mailing_account_{account['id']}"
        ))
    
    builder.row(InlineKeyboardButton(
        text="❌ Отмена",
        callback_data="cancel_mailing"
    ))
    
    await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["profile"]}">👤</tg-emoji> Выберите аккаунт для рассылки:</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )
    await state.set_state(MailingSetup.selecting_account)

@dp.callback_query(StateFilter(MailingSetup.selecting_account), F.data.startswith("mailing_account_"))
async def select_mailing_account(callback: CallbackQuery, state: FSMContext):
    account_id = int(callback.data.split("_")[2])
    account = await Database.get_account_by_id(account_id)
    
    if not account:
        await callback.answer("Аккаунт не найден!", show_alert=True)
        return
    
    await callback.message.delete()
    await callback.message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["download"]}">📥</tg-emoji> Загружаем чаты для {account["phone"]}...</b>',
        parse_mode=ParseMode.HTML
    )
    
    try:
        if account_id not in active_clients:
            client = await create_telethon_client(account['session_string'])
            if not await client.is_user_authorized():
                await callback.message.answer(
                    f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибка авторизации!</b>',
                    parse_mode=ParseMode.HTML,
                    reply_markup=get_mailing_keyboard()
                )
                return
            active_clients[account_id] = client
        else:
            client = active_clients[account_id]
        
        dialogs = await get_dialogs_from_client(client)
        
        if not dialogs:
            await callback.message.answer(
                f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Не удалось получить список чатов</b>',
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
        logger.error(f"Ошибка при загрузке чатов: {e}")
        await callback.message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибка: {str(e)}</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_mailing_keyboard()
        )

async def show_chats_page(message: Message, state: FSMContext):
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
        f'<b><tg-emoji emoji-id="{EMOJI["list"]}">👥</tg-emoji> Выберите чаты для рассылки</b>\n'
        f'Аккаунт: {account_phone}\n'
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
    page = int(callback.data.split("_")[1])
    await state.update_data(current_page=page)
    await show_chats_page(callback.message, state)
    await callback.answer()

@dp.callback_query(StateFilter(MailingSetup.selecting_chats), F.data == "confirm_chats")
async def confirm_chat_selection(callback: CallbackQuery, state: FSMContext):
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

@dp.callback_query(F.data == "cancel_mailing")
async def cancel_mailing_any(callback: CallbackQuery, state: FSMContext):
    current_state = await state.get_state()
    if current_state:
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
    
    html_example = (
        '<b>Жирный текст</b>\n'
        '<i>Курсив</i>\n'
        '<u>Подчеркнутый</u>\n'
        '<s>Зачеркнутый</s>\n'
        '<code>Моноширинный</code>\n'
        '<pre>Блок кода</pre>\n'
        '<a href="https://example.com">Ссылка</a>\n'
        '<blockquote>Цитата</blockquote>\n'
        '<tg-spoiler>Спойлер</tg-spoiler>'
    )
    
    await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["write"]}">✍</tg-emoji> Введите текст сообщения в формате HTML:</b>\n\n'
        f'<b>Поддерживаемые теги:</b>\n'
        f'<pre>{html_example}</pre>\n\n'
        '<i>Текст будет отправлен с полной поддержкой HTML форматирования</i>',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(MailingSetup.waiting_for_message)

@dp.message(MailingSetup.waiting_for_message)
async def process_message_text(message: Message, state: FSMContext):
    message_text = message.html_text if message.html_text else message.text
    
    if not message_text or not message_text.strip():
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Сообщение не может быть пустым</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        return
    
    await state.update_data(message_html=message_text)
    
    data = await state.get_data()
    
    confirmation_text = (
        f'<b><tg-emoji emoji-id="{EMOJI["info"]}">ℹ</tg-emoji> Подтвердите рассылку:</b>\n\n'
        f'<b>Аккаунт:</b> {data.get("account_phone", "Unknown")}\n'
        f'<b>Чатов:</b> {len(data["selected_chats"])}\n'
        f'<b>Задержка:</b> {data["delay"]} сек\n'
        f'<b>Циклов:</b> {data["cycles"]}\n\n'
        f'<b>Предпросмотр сообщения:</b>'
    )
    
    await message.answer(confirmation_text, parse_mode=ParseMode.HTML)
    
    try:
        await message.answer(message_text, parse_mode=ParseMode.HTML)
    except:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибка в HTML разметке!</b>',
            parse_mode=ParseMode.HTML
        )
        return
    
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Запустить", callback_data="start_mailing"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_mailing")
    )
    
    await message.answer(
        '<b>Подтвердите запуск:</b>',
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )
    await state.set_state(MailingSetup.confirming)

@dp.callback_query(StateFilter(MailingSetup.confirming), F.data == "start_mailing")
async def start_mailing_process(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    await callback.message.delete()
    
    account_id = data['account_id']
    client = active_clients.get(account_id)
    
    if not client:
        await callback.message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Клиент не найден!</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_mailing_keyboard()
        )
        return
    
    user_id = callback.from_user.id
    settings = await Database.get_user_settings(user_id)
    notifications_enabled = settings['notifications_enabled']
    
    mailing_id = await Database.add_mailing(
        account_id=account_id,
        chats=data['selected_chats'],
        delay=data['delay'],
        cycles=data['cycles'],
        message_html=data['message_html'],
        notifications_enabled=notifications_enabled
    )
    
    task = asyncio.create_task(
        execute_mailing(
            bot=callback.bot,
            chat_id=callback.message.chat.id,
            mailing_id=mailing_id,
            client=client,
            account_phone=data.get('account_phone', 'Unknown'),
            chats=data['selected_chats'],
            delay=data['delay'],
            cycles=data['cycles'],
            message_html=data['message_html'],
            notifications_enabled=notifications_enabled
        )
    )
    
    active_mailing_tasks[mailing_id] = task
    
    await callback.message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["rocket"]}">🚀</tg-emoji> Рассылка #{mailing_id} запущена!</b>\n'
        f'Аккаунт: {data.get("account_phone", "Unknown")}\n'
        f'Чатов: {len(data["selected_chats"])}\n'
        f'Циклов: {data["cycles"]}\n'
        f'Задержка: {data["delay"]} сек\n'
        f'Уведомления: {"ВКЛ" if notifications_enabled else "ВЫКЛ"}',
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
    account_phone: str,
    chats: List[str],
    delay: int,
    cycles: int,
    message_html: str,
    notifications_enabled: bool = True
):
    try:
        for cycle in range(1, cycles + 1):
            if mailing_id not in active_mailing_tasks:
                if notifications_enabled:
                    await bot.send_message(
                        chat_id,
                        f'<b><tg-emoji emoji-id="{EMOJI["stop"]}">🛑</tg-emoji> Рассылка #{mailing_id} остановлена</b>\n'
                        f'Аккаунт: {account_phone}',
                        parse_mode=ParseMode.HTML
                    )
                await Database.update_mailing_status(mailing_id, 'stopped')
                return
            
            await Database.update_mailing_status(mailing_id, 'running', current_cycle=cycle)
            
            if notifications_enabled:
                await bot.send_message(
                    chat_id,
                    f'<b><tg-emoji emoji-id="{EMOJI["send"]}">📤</tg-emoji> Рассылка #{mailing_id}</b>\n'
                    f'Аккаунт: {account_phone}\n'
                    f'Цикл {cycle} из {cycles}\n'
                    f'Отправка в {len(chats)} чатов...',
                    parse_mode=ParseMode.HTML
                )
            
            success_count = 0
            fail_count = 0
            
            for chat_id_to_send in chats:
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
            
            if notifications_enabled:
                await bot.send_message(
                    chat_id,
                    f'<b><tg-emoji emoji-id="{EMOJI["stats"]}">📊</tg-emoji> Цикл {cycle} завершен</b>\n'
                    f'Аккаунт: {account_phone}\n'
                    f'<tg-emoji emoji-id="{EMOJI["check"]}">✅</tg-emoji> Успешно: {success_count}\n'
                    f'<tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибок: {fail_count}',
                    parse_mode=ParseMode.HTML
                )
            
            if cycle < cycles and mailing_id in active_mailing_tasks:
                if notifications_enabled:
                    wait_msg = await bot.send_message(
                        chat_id,
                        f'<b><tg-emoji emoji-id="{EMOJI["clock"]}">⏰</tg-emoji> Ожидание {delay} секунд...</b>\n'
                        f'Аккаунт: {account_phone}\n'
                        f'Осталось циклов: {cycles - cycle}',
                        parse_mode=ParseMode.HTML
                    )
                else:
                    wait_msg = None
                
                for _ in range(delay):
                    if mailing_id not in active_mailing_tasks:
                        break
                    await asyncio.sleep(1)
                
                if wait_msg and notifications_enabled:
                    try:
                        await wait_msg.delete()
                    except:
                        pass
        
        if mailing_id in active_mailing_tasks:
            if notifications_enabled:
                await bot.send_message(
                    chat_id,
                    f'<b><tg-emoji emoji-id="{EMOJI["celebration"]}">🎉</tg-emoji> Рассылка #{mailing_id} завершена!</b>\n'
                    f'Аккаунт: {account_phone}\n'
                    f'Всего циклов: {cycles}',
                    parse_mode=ParseMode.HTML
                )
            await Database.update_mailing_status(mailing_id, 'completed')
        
    except Exception as e:
        logger.error(f"Ошибка в рассылке #{mailing_id}: {e}")
        if notifications_enabled:
            await bot.send_message(
                chat_id,
                f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибка в рассылке #{mailing_id}</b>\n'
                f'Аккаунт: {account_phone}\n'
                f'{str(e)}',
                parse_mode=ParseMode.HTML
            )
        await Database.update_mailing_status(mailing_id, 'error')
    finally:
        if mailing_id in active_mailing_tasks:
            del active_mailing_tasks[mailing_id]

# ========== Остановка рассылки ==========

@dp.message(F.text == "Остановить рассылку")
@dp.message(Command("stop_ml"))
async def stop_mailing(message: Message):
    if not active_mailing_tasks:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["info"]}">ℹ</tg-emoji> Нет активных рассылок</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        return
    
    builder = InlineKeyboardBuilder()
    
    for mailing_id in list(active_mailing_tasks.keys()):
        mailing = await Database.get_mailing_by_id(mailing_id)
        if mailing:
            account = await Database.get_account_by_id(mailing['account_id'])
            account_phone = account['phone'] if account else "Unknown"
            builder.row(InlineKeyboardButton(
                text=f"Остановить #{mailing_id} ({account_phone}, цикл {mailing['current_cycle']})",
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
    await callback.message.delete()
    await callback.answer()

# ========== Статус рассылки ==========

@dp.message(F.text == "Статус рассылки")
async def show_mailing_status(message: Message):
    if not active_mailing_tasks:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["info"]}">ℹ</tg-emoji> Нет активных рассылок</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_mailing_keyboard()
        )
        return
    
    status_text = f'<b><tg-emoji emoji-id="{EMOJI["stats"]}">📊</tg-emoji> Активные рассылки:</b>\n\n'
    
    for mailing_id in list(active_mailing_tasks.keys()):
        mailing = await Database.get_mailing_by_id(mailing_id)
        if mailing:
            account = await Database.get_account_by_id(mailing['account_id'])
            account_phone = account['phone'] if account else "Unknown"
            notif_status = "ВКЛ" if mailing.get('notifications_enabled', True) else "ВЫКЛ"
            status_text += (
                f'<b>📨 Рассылка #{mailing_id}</b>\n'
                f'• Аккаунт: {account_phone}\n'
                f'• Статус: {mailing["status"]}\n'
                f'• Цикл: {mailing["current_cycle"]} из {mailing["cycles"]}\n'
                f'• Чатов: {len(mailing["chats"].split(","))}\n'
                f'• Задержка: {mailing["delay"]} сек\n'
                f'• Уведомления: {notif_status}\n\n'
            )
    
    await message.answer(status_text, parse_mode=ParseMode.HTML, reply_markup=get_mailing_keyboard())

# ========== Обработчик для игнорирования ==========

@dp.callback_query(F.data == "ignore")
async def ignore_callback(callback: CallbackQuery):
    await callback.answer()

# ========== Главная функция ==========

async def main():
    try:
        logger.info("Подключение к PostgreSQL...")
        await Database.init_pool()
        
        logger.info("Восстановление автоответчиков...")
        await restore_all_auto_replies()
        
        logger.info("Запуск бота...")
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Ошибка при запуске: {e}")
        raise
    finally:
        for account_id in list(auto_reply_clients.keys()):
            await stop_auto_reply(account_id)
        
        for client in active_clients.values():
            try:
                await client.disconnect()
            except:
                pass
        active_clients.clear()
        
        for client in auth_clients.values():
            try:
                await client.disconnect()
            except:
                pass
        auth_clients.clear()
        
        if db_pool:
            await db_pool.close()

if __name__ == "__main__":
    print("=" * 50)
    print("Telegram Bot для массовых рассылок v5.0")
    print("=" * 50)
    print("Функции:")
    print("• Автоответчик на личные сообщения")
    print("• Проверка спам-блока через @spambot")
    print("• Вступление в чаты по username")
    print("• Корректная работа уведомлений")
    print("=" * 50)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nБот остановлен пользователем")
    except Exception as e:
        print(f"Критическая ошибка: {e}")
