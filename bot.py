"""
Telegram Bot для массовых рассылок с PostgreSQL
- Выбор аккаунта при запуске рассылки
- Полная поддержка HTML форматирования
- Добавление аккаунтов через файл сессии Telethon
- Вступление в чаты по ссылке на папку
- Настройка уведомлений
"""

import asyncio
import os
import logging
import re
import tempfile
from typing import List, Dict, Optional
from datetime import datetime
import asyncpg

from aiogram import Bot, Dispatcher, types, F
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, 
    InlineKeyboardButton, Message, CallbackQuery
)
from aiogram.filters import Command, StateFilter
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ParseMode

from telethon import TelegramClient, errors
from telethon.sessions import StringSession
from telethon.tl.types import User, Chat, Channel, PeerChannel, PeerChat
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
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
    "folder": "5884479287171485878",
    "loading": "5345906554510012647"
}

# Глобальные переменные
active_mailing_tasks: Dict[int, asyncio.Task] = {}
auth_clients: Dict[int, TelegramClient] = {}
active_clients: Dict[int, TelegramClient] = {}
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
            await conn.execute("DELETE FROM accounts WHERE id = $1", account_id)

    @staticmethod
    async def add_mailing(account_id: int, chats: List[str], delay: int, 
                         cycles: int, message_html: str, notifications_enabled: bool = True) -> int:
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
    waiting_for_link = State()

class MailingSetup(StatesGroup):
    selecting_account = State()
    selecting_chats = State()
    waiting_for_delay = State()
    waiting_for_cycles = State()
    waiting_for_message = State()
    confirming = State()

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
        retry_delay=2
    )
    await client.connect()
    return client

async def read_session_file(file_path: str) -> Optional[str]:
    """Чтение файла сессии Telethon"""
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
    """Проверка валидности строки сессии с таймаутом"""
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
                return {"phone": phone, "valid": True}
            else:
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

async def join_chat_from_link(client: TelegramClient, link: str) -> Dict:
    """Вступление в чат по ссылке"""
    try:
        # Прямые ссылки-приглашения
        if 't.me/joinchat/' in link:
            hash_part = link.split('/')[-1]
            await client(ImportChatInviteRequest(hash_part))
            return {"success": True, "message": "Успешно вступили"}
        
        if 't.me/+' in link:
            hash_part = link.split('/')[-1].replace('+', '')
            await client(ImportChatInviteRequest(hash_part))
            return {"success": True, "message": "Успешно вступили"}
        
        # Публичные ссылки
        if 't.me/' in link:
            username = link.split('/')[-1].split('?')[0]  # Убираем параметры
            try:
                entity = await client.get_entity(username)
                if hasattr(entity, 'broadcast') or hasattr(entity, 'megagroup'):
                    await client(JoinChannelRequest(entity))
                    return {"success": True, "message": f"Вступили в {getattr(entity, 'title', username)}"}
                else:
                    return {"success": False, "message": f"Не удалось вступить в {username}"}
            except errors.FloodWaitError as e:
                return {"success": False, "message": f"Flood wait {e.seconds}с"}
            except errors.UserAlreadyParticipantError:
                return {"success": True, "message": "Уже состоим"}
            except Exception as e:
                return {"success": False, "message": str(e)}
        
        return {"success": False, "message": "Неверный формат ссылки"}
        
    except errors.FloodWaitError as e:
        await asyncio.sleep(min(e.seconds, 30))
        return {"success": False, "message": f"Flood wait {e.seconds}с"}
    except Exception as e:
        return {"success": False, "message": str(e)}

async def get_chats_from_folder_link(client: TelegramClient, folder_link: str) -> List[str]:
    """Получение списка чатов из ссылки на папку"""
    try:
        # Извлекаем хеш из ссылки
        match = re.search(r't\.me/addlist/([A-Za-z0-9_-]+)', folder_link)
        if not match:
            logger.error(f"Не удалось извлечь хеш из ссылки: {folder_link}")
            return []
        
        folder_hash = match.group(1)
        logger.info(f"Извлечен хеш папки: {folder_hash}")
        
        # Пробуем получить информацию о папке через CheckChatInvite
        try:
            result = await client(CheckChatInviteRequest(folder_hash))
            logger.info(f"Результат CheckChatInvite: {type(result)}")
            
            chat_links = []
            if hasattr(result, 'chats'):
                logger.info(f"Найдено чатов в папке: {len(result.chats)}")
                for chat in result.chats:
                    if hasattr(chat, 'username') and chat.username:
                        link = f"https://t.me/{chat.username}"
                        chat_links.append(link)
                        logger.info(f"Добавлен чат: {link}")
                    else:
                        # Для приватных чатов используем ID
                        chat_links.append(str(chat.id))
                        logger.info(f"Добавлен приватный чат: {chat.id}")
                return chat_links
            else:
                logger.warning("У результата нет атрибута chats")
                
        except errors.InviteHashExpiredError:
            logger.error("Ссылка на папку истекла")
            return []
        except errors.InviteHashInvalidError:
            logger.error("Неверная ссылка на папку")
            return []
        except Exception as e:
            logger.error(f"Ошибка CheckChatInvite: {e}")
        
        # Альтернативный метод: пробуем получить как сущность
        try:
            entity = await client.get_entity(folder_link)
            logger.info(f"Получена сущность: {type(entity)}")
            
            if hasattr(entity, 'chats'):
                chat_links = []
                for chat in entity.chats:
                    if hasattr(chat, 'username') and chat.username:
                        chat_links.append(f"https://t.me/{chat.username}")
                    else:
                        chat_links.append(str(chat.id))
                return chat_links
        except Exception as e:
            logger.error(f"Ошибка получения сущности: {e}")
        
        return []
        
    except Exception as e:
        logger.error(f"Общая ошибка получения чатов из папки: {e}")
        return []

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
        '<i>При выключенных уведомлениях бот не будет присылать сообщения о прогрессе рассылки</i>',
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
        '<i>При выключенных уведомлениях бот не будет присылать сообщения о прогрессе рассылки</i>',
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
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
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Сессия авторизации истекла.</b> '
            'Начните процесс заново.',
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
        except errors.SessionPasswordNeededError:
            await state.update_data(session_string=client.session.save())
            await message.answer(
                f'<b><tg-emoji emoji-id="{EMOJI["lock_closed"]}">🔒</tg-emoji> Требуется двухфакторная аутентификация!</b>\n'
                'Введите пароль 2FA:',
                parse_mode=ParseMode.HTML,
                reply_markup=get_back_keyboard()
            )
            await state.set_state(AddAccount.waiting_for_2fa)
            return
        
        session_string = client.session.save()
        account_id = await Database.add_account(data['phone'], session_string)
        
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
    password = message.text.strip()
    data = await state.get_data()
    user_id = message.from_user.id
    
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
        await client.sign_in(password=password)
        
        session_string = client.session.save()
        account_id = await Database.add_account(data['phone'], session_string, has_2fa=True)
        
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

# ========== Добавление из файла ==========

@dp.message(F.text == "Добавить из файла")
async def add_account_file_start(message: Message, state: FSMContext):
    await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["file"]}">📁</tg-emoji> Отправьте файл сессии Telethon</b>\n\n'
        '<i>Поддерживаемые форматы:</i>\n'
        '• Файл .session (бинарный формат Telethon)\n'
        '• Текстовая строка сессии (StringSession)\n'
        '• Файл с текстовой строкой сессии\n\n'
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
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Не удалось извлечь данные сессии!</b>',
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
            'Проверьте данные и попробуйте снова.',
            parse_mode=ParseMode.HTML
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
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Нет сохраненных аккаунтов!</b>\n'
            'Сначала добавьте аккаунт в менеджере.',
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
        f'<b><tg-emoji emoji-id="{EMOJI["folder"]}">📦</tg-emoji> Отправьте ссылку на папку с чатами</b>\n\n'
        '<i>Пример ссылки:</i>\n'
        '<code>https://t.me/addlist/JYke4rePs4Y2MGYy</code>\n\n'
        f'<b>Аккаунт:</b> {account["phone"]}',
        parse_mode=ParseMode.HTML,
        reply_markup=get_back_keyboard()
    )
    await state.set_state(JoinChats.waiting_for_link)

@dp.message(JoinChats.waiting_for_link)
async def process_folder_link(message: Message, state: FSMContext):
    folder_link = message.text.strip()
    data = await state.get_data()
    account_id = data['account_id']
    account_phone = data['account_phone']
    
    if 't.me/addlist/' not in folder_link:
        await message.answer(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Неверный формат ссылки!</b>\n'
            'Отправьте ссылку вида: https://t.me/addlist/...',
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
        return
    
    status_msg = await message.answer(
        f'<b><tg-emoji emoji-id="{EMOJI["loading"]}">🔄</tg-emoji> Получаю список чатов из папки...</b>',
        parse_mode=ParseMode.HTML
    )
    
    try:
        if account_id not in active_clients:
            account = await Database.get_account_by_id(account_id)
            client = await create_telethon_client(account['session_string'])
            if not await client.is_user_authorized():
                await status_msg.edit_text(
                    f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибка авторизации аккаунта!</b>',
                    parse_mode=ParseMode.HTML
                )
                await state.clear()
                return
            active_clients[account_id] = client
        else:
            client = active_clients[account_id]
        
        chat_links = await get_chats_from_folder_link(client, folder_link)
        
        if not chat_links:
            await status_msg.edit_text(
                f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Не удалось получить список чатов!</b>\n\n'
                '<i>Возможные причины:</i>\n'
                '• Неверная ссылка на папку\n'
                '• Папка пуста\n'
                '• Ссылка устарела\n'
                '• Аккаунт не имеет доступа к папке\n\n'
                '<b>Попробуйте другую ссылку или проверьте доступ к папке.</b>',
                parse_mode=ParseMode.HTML,
                reply_markup=get_main_keyboard()
            )
            await state.clear()
            return
        
        await status_msg.edit_text(
            f'<b><tg-emoji emoji-id="{EMOJI["join"]}">🔗</tg-emoji> Начинаю вступление в чаты...</b>\n'
            f'Аккаунт: {account_phone}\n'
            f'Найдено чатов: {len(chat_links)}',
            parse_mode=ParseMode.HTML
        )
        
        success_count = 0
        fail_count = 0
        failed_details = []
        
        for i, link in enumerate(chat_links, 1):
            try:
                result = await join_chat_from_link(client, link)
                
                if result['success']:
                    success_count += 1
                else:
                    fail_count += 1
                    if len(failed_details) < 5:
                        failed_details.append(f"{link}: {result['message']}")
                
                # Обновляем статус каждые 5 чатов
                if i % 5 == 0:
                    await status_msg.edit_text(
                        f'<b>Вступление в чаты...</b>\n'
                        f'Прогресс: {i}/{len(chat_links)}\n'
                        f'✅ {success_count} | ❌ {fail_count}',
                        parse_mode=ParseMode.HTML
                    )
                
                # Задержка между вступлениями
                await asyncio.sleep(1)
                
            except Exception as e:
                fail_count += 1
                logger.error(f"Ошибка вступления в чат {link}: {e}")
        
        # Финальный отчет
        report = (
            f'<b><tg-emoji emoji-id="{EMOJI["stats"]}">📊</tg-emoji> Результаты вступления:</b>\n\n'
            f'<b>Аккаунт:</b> {account_phone}\n'
            f'<tg-emoji emoji-id="{EMOJI["check"]}">✅</tg-emoji> Успешно: {success_count}\n'
            f'<tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибок: {fail_count}\n'
            f'Всего: {len(chat_links)}'
        )
        
        if failed_details:
            report += '\n\n<b>Первые ошибки:</b>\n' + '\n'.join(failed_details[:3])
        
        await status_msg.edit_text(
            report,
            parse_mode=ParseMode.HTML
        )
        
        await message.answer(
            '<b>Операция завершена!</b>',
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Ошибка при вступлении в чаты: {e}")
        await status_msg.edit_text(
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибка: {str(e)}</b>',
            parse_mode=ParseMode.HTML
        )
        await message.answer(
            'Произошла ошибка. Попробуйте снова.',
            reply_markup=get_main_keyboard()
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
            f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Нет сохраненных аккаунтов!</b>\n'
            'Сначала добавьте аккаунт в менеджере.',
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
                    f'<b><tg-emoji emoji-id="{EMOJI["cross"]}">❌</tg-emoji> Ошибка авторизации аккаунта!</b>',
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
                    await bot.send_message(
                        chat_id,
                        f'<b><tg-emoji emoji-id="{EMOJI["clock"]}">⏰</tg-emoji> Ожидание {delay} секунд...</b>\n'
                        f'Аккаунт: {account_phone}',
                        parse_mode=ParseMode.HTML
                    )
                
                for _ in range(delay):
                    if mailing_id not in active_mailing_tasks:
                        break
                    await asyncio.sleep(1)
        
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
        logger.info("Запуск бота...")
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Ошибка при запуске: {e}")
        raise
    finally:
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
    print("Telegram Bot для массовых рассылок v3.1")
    print("=" * 50)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nБот остановлен пользователем")
    except Exception as e:
        print(f"Критическая ошибка: {e}")
