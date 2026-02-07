import asyncio
import html
import logging
import re
import secrets
import sqlite3
import urllib.parse
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice, Message, PreCheckoutQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

BOT_TOKEN = ""
GOOGLE_API_KEY = ""
GEMINI_MODEL = ""
ADMIN_ID = 123456789

SUBSCRIPTION_PRICE_STARS = 150
SUBSCRIPTION_DAYS = 30
DAILY_LIMIT = 3
DB_PATH = "bot_data.sqlite3"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("autopost_bot")

router = Router()
dp = Dispatcher()
dp.include_router(router)
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

db_lock = asyncio.Lock()
preview_cache: Dict[int, Dict[str, Any]] = {}


def get_conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


conn = get_conn()


def init_db() -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            is_banned INTEGER DEFAULT 0,
            invited INTEGER DEFAULT 0,
            invite_code TEXT,
            subscription_until TEXT,
            timezone_offset INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            notify_3d INTEGER DEFAULT 0,
            notify_expired INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS invites (
            code TEXT PRIMARY KEY,
            created_by INTEGER NOT NULL,
            used_by INTEGER,
            used_at TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            chat_id TEXT NOT NULL,
            title TEXT NOT NULL,
            username TEXT,
            style_prompt TEXT,
            is_active INTEGER DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            topic TEXT NOT NULL,
            style TEXT NOT NULL,
            posts_per_day INTEGER NOT NULL,
            schedule_type TEXT NOT NULL,
            times TEXT NOT NULL,
            language TEXT NOT NULL,
            emojis INTEGER NOT NULL,
            max_length INTEGER NOT NULL,
            hashtags TEXT,
            tone TEXT,
            stop_words TEXT,
            autopin INTEGER DEFAULT 0,
            add_channel_link INTEGER DEFAULT 0,
            use_web_news INTEGER DEFAULT 0,
            news_sources TEXT DEFAULT 'google',
            is_active INTEGER DEFAULT 1,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            task_id INTEGER,
            channel_id INTEGER,
            channel_chat_id TEXT,
            telegram_message_id INTEGER,
            content TEXT NOT NULL,
            published_at TEXT NOT NULL,
            slot_key TEXT
        );

        CREATE TABLE IF NOT EXISTS task_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            slot_key TEXT NOT NULL,
            status TEXT NOT NULL,
            error TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(task_id, slot_key)
        );

        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            stars INTEGER NOT NULL,
            invoice_payload TEXT NOT NULL,
            paid_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS task_media (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL UNIQUE,
            photo_file_id TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS post_stats (
            post_id INTEGER PRIMARY KEY,
            views INTEGER DEFAULT 0,
            reactions INTEGER DEFAULT 0,
            comments INTEGER DEFAULT 0,
            updated_at TEXT NOT NULL
        );
        """
    )

    task_columns = {row["name"] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()}
    if "use_web_news" not in task_columns:
        conn.execute("ALTER TABLE tasks ADD COLUMN use_web_news INTEGER DEFAULT 0")
    if "news_sources" not in task_columns:
        conn.execute("ALTER TABLE tasks ADD COLUMN news_sources TEXT DEFAULT 'google'")

    channel_columns = {row["name"] for row in conn.execute("PRAGMA table_info(channels)").fetchall()}
    if "style_prompt" not in channel_columns:
        conn.execute("ALTER TABLE channels ADD COLUMN style_prompt TEXT")

    post_columns = {row["name"] for row in conn.execute("PRAGMA table_info(posts)").fetchall()}
    if "channel_chat_id" not in post_columns:
        conn.execute("ALTER TABLE posts ADD COLUMN channel_chat_id TEXT")
    if "telegram_message_id" not in post_columns:
        conn.execute("ALTER TABLE posts ADD COLUMN telegram_message_id INTEGER")

    conn.commit()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def dt_to_str(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def str_to_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


async def db_execute(query: str, params: Tuple = ()) -> None:
    async with db_lock:
        conn.execute(query, params)
        conn.commit()


async def db_fetchone(query: str, params: Tuple = ()) -> Optional[sqlite3.Row]:
    async with db_lock:
        return conn.execute(query, params).fetchone()


async def db_fetchall(query: str, params: Tuple = ()) -> List[sqlite3.Row]:
    async with db_lock:
        return conn.execute(query, params).fetchall()


async def safe_edit_text(message: Message, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    try:
        await message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if "there is no text in the message to edit" in str(e).lower():
            await message.answer(text, reply_markup=reply_markup)
        else:
            raise


def extract_reactions_count(msg: Message) -> int:
    total = 0
    try:
        reactions = getattr(msg, "reactions", None)
        if reactions and hasattr(reactions, "reactions"):
            for r in reactions.reactions:
                total += int(getattr(r, "total_count", 0) or 0)
    except Exception:
        return 0
    return total


class InviteState(StatesGroup):
    waiting_invite = State()


class ChannelState(StatesGroup):
    waiting_channel_input = State()


class TaskCreateState(StatesGroup):
    waiting_topic = State()
    waiting_style = State()
    waiting_posts_per_day = State()
    waiting_schedule = State()
    waiting_language = State()
    waiting_emojis = State()
    waiting_max_length = State()
    waiting_hashtags = State()
    waiting_use_web_news = State()
    waiting_news_sources = State()


class SettingsState(StatesGroup):
    waiting_timezone = State()


class TaskMediaState(StatesGroup):
    waiting_task_image = State()


class ChannelStyleState(StatesGroup):
    waiting_channel_id = State()
    waiting_style_examples = State()


@dataclass
class AccessCheck:
    ok: bool
    reason: str = ""


async def ensure_user(user_id: int, username: Optional[str]) -> None:
    row = await db_fetchone("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
    if not row:
        await db_execute(
            "INSERT INTO users (user_id, username, created_at) VALUES (?, ?, ?)",
            (user_id, username or "", dt_to_str(now_utc())),
        )
    else:
        await db_execute("UPDATE users SET username = ? WHERE user_id = ?", (username or "", user_id))


async def get_user(user_id: int) -> Optional[sqlite3.Row]:
    return await db_fetchone("SELECT * FROM users WHERE user_id = ?", (user_id,))


def subscription_is_active(subscription_until: Optional[str]) -> bool:
    dt = str_to_dt(subscription_until)
    return bool(dt and dt > now_utc())


async def access_guard(user_id: int) -> AccessCheck:
    user = await get_user(user_id)
    if not user:
        return AccessCheck(False, "Пользователь не найден.")
    if user["is_banned"] == 1:
        return AccessCheck(False, "Ваш доступ заблокирован.")
    if user["invited"] == 0:
        return AccessCheck(False, "Сначала активируйте инвайт-код.")
    if not subscription_is_active(user["subscription_until"]):
        return AccessCheck(False, "Подписка неактивна. Оплатите доступ.")
    return AccessCheck(True, "")


def parse_hhmm(value: str) -> Optional[str]:
    m = re.match(r"^([01]?\d|2[0-3]):([0-5]\d)$", value.strip())
    if not m:
        return None
    return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"


def normalize_news_sources(raw: str) -> str:
    allowed = {"google", "reddit", "hn", "crypto", "telegram"}
    selected = [s.strip().lower() for s in (raw or "").split(",") if s.strip()]
    selected = [s for s in selected if s in allowed]
    return ",".join(sorted(set(selected))) if selected else "google"


def user_local_bounds(offset_hours: int) -> Tuple[datetime, datetime]:
    local_now = now_utc() + timedelta(hours=offset_hours)
    start_local = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    return start_local - timedelta(hours=offset_hours), end_local - timedelta(hours=offset_hours)


async def count_posts_today(user_id: int, offset_hours: int) -> int:
    s, e = user_local_bounds(offset_hours)
    row = await db_fetchone(
        "SELECT COUNT(*) c FROM posts WHERE user_id = ? AND published_at >= ? AND published_at < ?",
        (user_id, dt_to_str(s), dt_to_str(e)),
    )
    return int(row["c"] if row else 0)


async def extend_subscription(user_id: int, days: int) -> datetime:
    user = await get_user(user_id)
    base = now_utc()
    if user:
        cur = str_to_dt(user["subscription_until"])
        if cur and cur > base:
            base = cur
    new_until = base + timedelta(days=days)
    await db_execute(
        "UPDATE users SET subscription_until = ?, notify_3d = 0, notify_expired = 0 WHERE user_id = ?",
        (dt_to_str(new_until), user_id),
    )
    return new_until


def fetch_web_news_context(topic: str, language: str = "ru", max_items: int = 5) -> str:
    # Google News RSS is used as a lightweight source of fresh headlines.
    lang = "ru" if language.lower().startswith("ru") else "en"
    query = urllib.parse.quote_plus(topic.strip())
    hl = "ru" if lang == "ru" else "en-US"
    gl = "RU" if lang == "ru" else "US"
    ceid = "RU:ru" if lang == "ru" else "US:en"
    url = f"https://news.google.com/rss/search?q={query}&hl={hl}&gl={gl}&ceid={ceid}"

    r = requests.get(url, timeout=20)
    r.raise_for_status()

    root = ET.fromstring(r.text)
    items = root.findall("./channel/item")
    lines: List[str] = []
    for item in items[:max_items]:
        title = (item.findtext("title") or "").strip()
        pub_date = (item.findtext("pubDate") or "").strip()
        if title:
            lines.append(f"- {title} ({pub_date})")

    return "\n".join(lines)


def fetch_reddit_context(topic: str, max_items: int = 4) -> str:
    q = urllib.parse.quote_plus(topic.strip())
    url = f"https://www.reddit.com/search.json?q={q}&sort=new&limit={max_items}"
    headers = {"User-Agent": "telegram-autopost-bot/1.0"}
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    data = r.json()
    lines: List[str] = []
    for child in data.get("data", {}).get("children", [])[:max_items]:
        d = child.get("data", {})
        title = (d.get("title") or "").strip()
        sub = (d.get("subreddit") or "").strip()
        if title:
            lines.append(f"- r/{sub}: {title}")
    return "\n".join(lines)


def fetch_hn_context(topic: str, max_items: int = 4) -> str:
    q = urllib.parse.quote_plus(topic.strip())
    url = f"https://hn.algolia.com/api/v1/search_by_date?query={q}&tags=story&hitsPerPage={max_items}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    lines: List[str] = []
    for hit in data.get("hits", [])[:max_items]:
        title = (hit.get("title") or "").strip()
        points = hit.get("points", 0)
        if title:
            lines.append(f"- {title} (points: {points})")
    return "\n".join(lines)


def fetch_crypto_context(max_items: int = 4) -> str:
    # CoinDesk RSS is a simple, open source for crypto headlines.
    url = "https://www.coindesk.com/arc/outboundfeeds/rss/"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    root = ET.fromstring(r.text)
    items = root.findall("./channel/item")
    lines: List[str] = []
    for item in items[:max_items]:
        title = (item.findtext("title") or "").strip()
        pub_date = (item.findtext("pubDate") or "").strip()
        if title:
            lines.append(f"- {title} ({pub_date})")
    return "\n".join(lines)


def build_news_context(topic: str, language: str, sources: str) -> str:
    source_set = {s.strip().lower() for s in (sources or "").split(",") if s.strip()}
    if not source_set:
        source_set = {"google"}

    chunks: List[str] = []
    if "google" in source_set:
        try:
            chunks.append("Google News:\n" + fetch_web_news_context(topic, language, 5))
        except Exception:
            pass
    if "reddit" in source_set:
        try:
            chunks.append("Reddit:\n" + fetch_reddit_context(topic, 4))
        except Exception:
            pass
    if "hn" in source_set:
        try:
            chunks.append("Hacker News:\n" + fetch_hn_context(topic, 4))
        except Exception:
            pass
    if "crypto" in source_set:
        try:
            chunks.append("Crypto:\n" + fetch_crypto_context(4))
        except Exception:
            pass
    if "telegram" in source_set:
        # Placeholder: parsing public channels can be integrated later.
        chunks.append("Telegram:\n- Источник Telegram включен, но парсинг пока отключен.")

    return "\n\n".join([c for c in chunks if c.strip()])


def analyze_channel_style(samples: List[str], language: str = "ru") -> str:
    joined = "\n\n---\n\n".join(samples)
    prompt = (
        "Проанализируй примеры постов и выдай style_prompt для генерации новых постов в том же стиле.\n"
        f"Язык: {language}\n"
        "Сформируй короткий, практичный prompt (6-10 пунктов): тон, структура, длина абзацев, ритм, эмодзи, CTA, табу.\n"
        "Ответ: только готовый style_prompt.\n\n"
        f"Примеры:\n{joined}"
    )
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GOOGLE_API_KEY}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.5, "topP": 0.9, "maxOutputTokens": 700},
    }
    r = requests.post(url, json=payload, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Gemini style analyze error: {r.status_code} {r.text}")
    data = r.json()
    return data["candidates"][0]["content"]["parts"][0]["text"].strip()


def generate_post(
    topic: str,
    style: str,
    length: int,
    emojis: bool,
    language: str,
    hashtags: str = "",
    tone: str = "нейтральная",
    stop_words: str = "",
    channel_link: str = "",
    web_news_context: str = "",
    channel_style_prompt: str = "",
) -> str:
    emoji_rule = "Добавляй эмодзи умеренно." if emojis else "Не используй эмодзи."
    news_rule = (
        "Используй только факты из блока Актуальные новости, не выдумывай источники и даты."
        if web_news_context
        else ""
    )
    prompt = (
        "Сгенерируй готовый Telegram-пост.\\n"
        f"Тема: {topic}\\n"
        f"Стиль: {style}\\n"
        f"Язык: {language}\\n"
        f"Тональность: {tone}\\n"
        f"Максимальная длина: {length} символов\\n"
        f"{emoji_rule}\\n"
        f"Хэштеги: {hashtags if hashtags else 'не добавляй'}\\n"
        f"Стоп-слова: {stop_words if stop_words else 'нет'}\\n"
        f"Ссылка канала: {channel_link if channel_link else 'не добавляй'}\\n"
        f"Память стиля канала: {channel_style_prompt if channel_style_prompt else 'нет'}\\n"
        f"{news_rule}\\n"
    )

    if web_news_context:
        prompt += f"Актуальные новости:\\n{web_news_context}\\n"

    prompt += "Верни только текст поста без пояснений."

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GOOGLE_API_KEY}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.8, "topP": 0.95, "maxOutputTokens": 1024},
    }
    r = requests.post(url, json=payload, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Gemini API error: {r.status_code} {r.text}")
    data = r.json()
    text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
    return text if len(text) <= length else text[: max(0, length - 3)].rstrip() + "..."

def main_menu_kb(admin: bool = False) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="📢 Каналы", callback_data="menu_channels"), InlineKeyboardButton(text="✍️ Задачи", callback_data="menu_tasks"))
    b.row(InlineKeyboardButton(text="⏰ Расписание", callback_data="menu_schedule"), InlineKeyboardButton(text="⚙️ Настройки", callback_data="menu_settings"))
    b.row(InlineKeyboardButton(text="📊 Статистика", callback_data="menu_stats"), InlineKeyboardButton(text="💳 Подписка", callback_data="menu_subscription"))
    if admin:
        b.row(InlineKeyboardButton(text="👑 Админ-панель", callback_data="menu_admin"))
    return b.as_markup()


def channels_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить канал", callback_data="channels_add")],
            [InlineKeyboardButton(text="📋 Мои каналы", callback_data="channels_list")],
            [InlineKeyboardButton(text="🧠 Память стиля", callback_data="channels_style_memory")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")],
        ]
    )


def tasks_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Создать задачу", callback_data="tasks_create")], [InlineKeyboardButton(text="📋 Список задач", callback_data="tasks_list")], [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")]])


def subscription_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💳 Оплатить 150 Stars / 30 дней", callback_data="sub_buy")], [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")]])


def settings_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🌍 Часовой пояс", callback_data="settings_timezone")], [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")]])


def admin_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📈 Общая статистика", callback_data="admin_stats")],
            [InlineKeyboardButton(text="👀 Пользователи и задачи", callback_data="admin_users_list")],
            [InlineKeyboardButton(text="📊 Аналитика постов", callback_data="admin_posts_analytics")],
            [InlineKeyboardButton(text="🎟 Создать инвайт", callback_data="admin_invite_create")],
            [InlineKeyboardButton(text="📃 Список инвайтов", callback_data="admin_invite_list")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")],
        ]
    )


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    await ensure_user(message.from_user.id, message.from_user.username)
    user = await get_user(message.from_user.id)
    if user["is_banned"] == 1:
        await message.answer("🚫 Вы заблокированы.")
        return
    if user["invited"] == 0:
        await state.set_state(InviteState.waiting_invite)
        await message.answer("🔐 Введите ваш инвайт-код для активации доступа.")
        return
    if not subscription_is_active(user["subscription_until"]):
        await message.answer("💳 Для использования бота нужна активная подписка.", reply_markup=subscription_kb())
        return
    await state.clear()
    await message.answer("Главное меню:", reply_markup=main_menu_kb(admin=(message.from_user.id == ADMIN_ID)))


@router.message(InviteState.waiting_invite)
async def process_invite(message: Message, state: FSMContext) -> None:
    await ensure_user(message.from_user.id, message.from_user.username)
    code = message.text.strip().upper()
    inv = await db_fetchone("SELECT * FROM invites WHERE code = ? AND is_active = 1 AND used_by IS NULL", (code,))
    if not inv:
        await message.answer("❌ Неверный или уже использованный инвайт-код.")
        return
    await db_execute("UPDATE invites SET used_by = ?, used_at = ?, is_active = 0 WHERE code = ?", (message.from_user.id, dt_to_str(now_utc()), code))
    await db_execute("UPDATE users SET invited = 1, invite_code = ? WHERE user_id = ?", (code, message.from_user.id))
    await state.clear()
    await message.answer("✅ Инвайт принят. Теперь оплатите подписку.", reply_markup=subscription_kb())


@router.callback_query(F.data == "sub_buy")
async def buy_subscription(call: CallbackQuery) -> None:
    await call.answer()
    payload = f"sub_{call.from_user.id}_{int(now_utc().timestamp())}"
    prices = [LabeledPrice(label="Подписка 30 дней", amount=SUBSCRIPTION_PRICE_STARS)]
    await bot.send_invoice(
        chat_id=call.from_user.id,
        title="Подписка на автопостинг",
        description="Доступ к генерации и автопубликации постов на 30 дней",
        payload=payload,
        currency="XTR",
        prices=prices,
        provider_token="",
    )


@router.pre_checkout_query()
async def pre_checkout(pre_checkout_query: PreCheckoutQuery) -> None:
    await pre_checkout_query.answer(ok=True)


@router.message(F.successful_payment)
async def successful_payment(message: Message) -> None:
    p = message.successful_payment
    await db_execute("INSERT INTO payments (user_id, stars, invoice_payload, paid_at) VALUES (?, ?, ?, ?)", (message.from_user.id, p.total_amount, p.invoice_payload, dt_to_str(now_utc())))
    new_until = await extend_subscription(message.from_user.id, SUBSCRIPTION_DAYS)
    await message.answer(f"✅ Оплата прошла успешно. Подписка до: <b>{new_until.strftime('%Y-%m-%d %H:%M UTC')}</b>", reply_markup=main_menu_kb(admin=(message.from_user.id == ADMIN_ID)))


@router.callback_query(F.data.startswith("back_"))
async def cb_back(call: CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    await state.clear()
    await call.message.edit_text("Главное меню:", reply_markup=main_menu_kb(admin=(call.from_user.id == ADMIN_ID)))


@router.callback_query(F.data == "menu_channels")
async def menu_channels(call: CallbackQuery) -> None:
    await call.answer()
    a = await access_guard(call.from_user.id)
    if not a.ok:
        await call.message.edit_text(a.reason, reply_markup=subscription_kb())
        return
    await call.message.edit_text("Управление каналами:", reply_markup=channels_menu_kb())


@router.callback_query(F.data == "menu_tasks")
async def menu_tasks(call: CallbackQuery) -> None:
    await call.answer()
    a = await access_guard(call.from_user.id)
    if not a.ok:
        await call.message.edit_text(a.reason, reply_markup=subscription_kb())
        return
    await call.message.edit_text("Управление задачами:", reply_markup=tasks_menu_kb())


@router.callback_query(F.data == "menu_settings")
async def menu_settings(call: CallbackQuery) -> None:
    await call.answer()
    a = await access_guard(call.from_user.id)
    if not a.ok:
        await call.message.edit_text(a.reason, reply_markup=subscription_kb())
        return
    await call.message.edit_text("Настройки:", reply_markup=settings_menu_kb())


@router.callback_query(F.data == "menu_schedule")
async def menu_schedule(call: CallbackQuery) -> None:
    await call.answer()
    a = await access_guard(call.from_user.id)
    if not a.ok:
        await call.message.edit_text(a.reason, reply_markup=subscription_kb())
        return
    rows = await db_fetchall("SELECT id, topic, times, schedule_type, is_active FROM tasks WHERE user_id = ? ORDER BY id DESC", (call.from_user.id,))
    if not rows:
        await call.message.edit_text("Задач нет.", reply_markup=tasks_menu_kb())
        return
    lines = ["⏰ Расписания задач:"]
    for r in rows:
        lines.append(f"#{r['id']} [{'ON' if r['is_active'] == 1 else 'OFF'}] {r['schedule_type']} -> {r['times']} | {r['topic'][:30]}")
    await call.message.edit_text("\n".join(lines), reply_markup=tasks_menu_kb())


@router.callback_query(F.data == "menu_subscription")
async def menu_subscription(call: CallbackQuery) -> None:
    await call.answer()
    u = await get_user(call.from_user.id)
    dt = str_to_dt(u["subscription_until"]) if u else None
    status = f"Активна до {dt.strftime('%Y-%m-%d %H:%M UTC')}" if dt and dt > now_utc() else "Неактивна"
    await call.message.edit_text(f"💳 Подписка\nСтатус: <b>{status}</b>", reply_markup=subscription_kb())


@router.callback_query(F.data == "menu_stats")
async def menu_stats(call: CallbackQuery) -> None:
    await call.answer()
    a = await access_guard(call.from_user.id)
    if not a.ok:
        await call.message.edit_text(a.reason, reply_markup=subscription_kb())
        return
    u = await get_user(call.from_user.id)
    posts = await db_fetchone("SELECT COUNT(*) c FROM posts WHERE user_id = ?", (call.from_user.id,))
    tasks = await db_fetchone("SELECT COUNT(*) c FROM tasks WHERE user_id = ? AND is_active = 1", (call.from_user.id,))
    used = await count_posts_today(call.from_user.id, int(u["timezone_offset"]))
    rem = max(0, DAILY_LIMIT - used)
    dt = str_to_dt(u["subscription_until"])
    await call.message.edit_text(
        "📊 Ваша статистика\n\n"
        f"Опубликовано постов: <b>{posts['c']}</b>\n"
        f"Остаток дневного лимита: <b>{rem}</b> из {DAILY_LIMIT}\n"
        f"Активные задачи: <b>{tasks['c']}</b>\n"
        f"Подписка до: <b>{dt.strftime('%Y-%m-%d %H:%M UTC') if dt else 'Нет'}</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")]]),
    )

@router.callback_query(F.data == "menu_admin")
async def menu_admin(call: CallbackQuery) -> None:
    await call.answer()
    if call.from_user.id != ADMIN_ID:
        await call.message.edit_text("Нет доступа.")
        return
    await call.message.edit_text("👑 Админ-панель", reply_markup=admin_menu_kb())


@router.callback_query(F.data == "admin_stats")
async def admin_stats(call: CallbackQuery) -> None:
    await call.answer()
    if call.from_user.id != ADMIN_ID:
        return
    users_total = await db_fetchone("SELECT COUNT(*) c FROM users")
    posts_total = await db_fetchone("SELECT COUNT(*) c FROM posts")
    rows = await db_fetchall("SELECT subscription_until FROM users WHERE subscription_until IS NOT NULL")
    active = sum(1 for r in rows if str_to_dt(r["subscription_until"]) and str_to_dt(r["subscription_until"]) > now_utc())
    await call.message.edit_text(
        "📈 Общая статистика\n\n"
        f"Всего пользователей: <b>{users_total['c']}</b>\n"
        f"Активных подписок: <b>{active}</b>\n"
        f"Всего публикаций: <b>{posts_total['c']}</b>",
        reply_markup=admin_menu_kb(),
    )


@router.callback_query(F.data == "admin_users_list")
async def admin_users_list(call: CallbackQuery) -> None:
    await call.answer()
    if call.from_user.id != ADMIN_ID:
        return
    users = await db_fetchall("SELECT user_id, username, is_banned, subscription_until FROM users ORDER BY created_at DESC LIMIT 50")
    if not users:
        await safe_edit_text(call.message, "Пользователей пока нет.", reply_markup=admin_menu_kb())
        return
    kb = InlineKeyboardBuilder()
    for u in users:
        username = f"@{u['username']}" if u["username"] else "без username"
        label = f"{'🚫' if u['is_banned'] else '👤'} {u['user_id']} {username}"
        kb.row(InlineKeyboardButton(text=label[:62], callback_data=f"admin_user_{u['user_id']}"))
    kb.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_admin"))
    await safe_edit_text(call.message, "👀 Пользователи:", reply_markup=kb.as_markup())


@router.callback_query(F.data.startswith("admin_user_"))
async def admin_user_detail(call: CallbackQuery) -> None:
    await call.answer()
    if call.from_user.id != ADMIN_ID:
        return
    uid = int(call.data.split("_")[-1])
    user = await db_fetchone("SELECT * FROM users WHERE user_id = ?", (uid,))
    if not user:
        await safe_edit_text(call.message, "Пользователь не найден.", reply_markup=admin_menu_kb())
        return
    tasks = await db_fetchall(
        """
        SELECT t.id, t.topic, t.is_active, c.title as channel_title
        FROM tasks t
        LEFT JOIN channels c ON c.id = t.channel_id
        WHERE t.user_id = ?
        ORDER BY t.id DESC
        LIMIT 20
        """,
        (uid,),
    )
    lines = [f"Пользователь: <code>{uid}</code>", f"username: @{user['username']}" if user["username"] else "username: -", ""]
    if not tasks:
        lines.append("Задач нет.")
    else:
        lines.append("Активные/последние задачи:")
        for t in tasks:
            st = "ON" if t["is_active"] == 1 else "OFF"
            lines.append(f"#{t['id']} [{st}] {t['channel_title'] or '-'} | {t['topic'][:30]}")
    kb = InlineKeyboardBuilder()
    for t in tasks:
        if t["is_active"] == 1:
            kb.row(InlineKeyboardButton(text=f"⏹ Остановить #{t['id']}", callback_data=f"admin_stop_{t['id']}_{uid}"))
    kb.row(InlineKeyboardButton(text="⬅️ К пользователям", callback_data="admin_users_list"))
    await safe_edit_text(call.message, "\n".join(lines), reply_markup=kb.as_markup())


@router.callback_query(F.data.startswith("admin_stop_"))
async def admin_stop_task(call: CallbackQuery) -> None:
    await call.answer()
    if call.from_user.id != ADMIN_ID:
        return
    parts = call.data.split("_")
    if len(parts) != 4:
        return
    task_id = int(parts[2])
    uid = int(parts[3])
    await db_execute("UPDATE tasks SET is_active = 0 WHERE id = ?", (task_id,))
    await call.answer("Задача остановлена", show_alert=False)
    user = await db_fetchone("SELECT * FROM users WHERE user_id = ?", (uid,))
    tasks = await db_fetchall(
        """
        SELECT t.id, t.topic, t.is_active, c.title as channel_title
        FROM tasks t
        LEFT JOIN channels c ON c.id = t.channel_id
        WHERE t.user_id = ?
        ORDER BY t.id DESC
        LIMIT 20
        """,
        (uid,),
    )
    lines = [f"Пользователь: <code>{uid}</code>", f"username: @{user['username']}" if user and user["username"] else "username: -", ""]
    if not tasks:
        lines.append("Задач нет.")
    else:
        lines.append("Активные/последние задачи:")
        for t in tasks:
            st = "ON" if t["is_active"] == 1 else "OFF"
            lines.append(f"#{t['id']} [{st}] {t['channel_title'] or '-'} | {t['topic'][:30]}")
    kb = InlineKeyboardBuilder()
    for t in tasks:
        if t["is_active"] == 1:
            kb.row(InlineKeyboardButton(text=f"⏹ Остановить #{t['id']}", callback_data=f"admin_stop_{t['id']}_{uid}"))
    kb.row(InlineKeyboardButton(text="⬅️ К пользователям", callback_data="admin_users_list"))
    await safe_edit_text(call.message, "\n".join(lines), reply_markup=kb.as_markup())


@router.callback_query(F.data == "admin_posts_analytics")
async def admin_posts_analytics(call: CallbackQuery) -> None:
    await call.answer()
    if call.from_user.id != ADMIN_ID:
        return
    row = await db_fetchone(
        """
        SELECT
            COUNT(*) as total,
            AVG(ps.views) as avg_views,
            AVG(ps.reactions) as avg_reactions,
            AVG(ps.comments) as avg_comments
        FROM posts p
        LEFT JOIN post_stats ps ON ps.post_id = p.id
        """
    )
    best = await db_fetchone(
        """
        SELECT p.id, p.content, COALESCE(ps.views,0) as views, COALESCE(ps.reactions,0) as reactions, COALESCE(ps.comments,0) as comments
        FROM posts p
        LEFT JOIN post_stats ps ON ps.post_id = p.id
        ORDER BY (COALESCE(ps.views,0) + COALESCE(ps.reactions,0) * 10 + COALESCE(ps.comments,0) * 5) DESC
        LIMIT 1
        """
    )
    worst = await db_fetchone(
        """
        SELECT p.id, p.content, COALESCE(ps.views,0) as views, COALESCE(ps.reactions,0) as reactions, COALESCE(ps.comments,0) as comments
        FROM posts p
        LEFT JOIN post_stats ps ON ps.post_id = p.id
        ORDER BY (COALESCE(ps.views,0) + COALESCE(ps.reactions,0) * 10 + COALESCE(ps.comments,0) * 5) ASC
        LIMIT 1
        """
    )

    text = (
        "📊 Аналитика постов\n\n"
        f"Всего постов: <b>{int(row['total'] or 0)}</b>\n"
        f"Средние просмотры: <b>{(row['avg_views'] or 0):.1f}</b>\n"
        f"Средние реакции: <b>{(row['avg_reactions'] or 0):.1f}</b>\n"
        f"Средние комментарии: <b>{(row['avg_comments'] or 0):.1f}</b>\n\n"
    )
    if best:
        text += f"🔝 Лучший: #{best['id']} (V:{best['views']} R:{best['reactions']} C:{best['comments']})\n"
    if worst:
        text += f"📉 Худший: #{worst['id']} (V:{worst['views']} R:{worst['reactions']} C:{worst['comments']})\n"

    await safe_edit_text(call.message, text, reply_markup=admin_menu_kb())


@router.callback_query(F.data == "admin_invite_create")
async def admin_invite_create(call: CallbackQuery) -> None:
    await call.answer()
    if call.from_user.id != ADMIN_ID:
        return
    code = secrets.token_hex(4).upper()
    await db_execute("INSERT OR REPLACE INTO invites (code, created_by, is_active, created_at) VALUES (?, ?, 1, ?)", (code, ADMIN_ID, dt_to_str(now_utc())))
    await call.message.edit_text(f"🎟 Новый инвайт: <code>{code}</code>", reply_markup=admin_menu_kb())


@router.callback_query(F.data == "admin_invite_list")
async def admin_invite_list(call: CallbackQuery) -> None:
    await call.answer()
    if call.from_user.id != ADMIN_ID:
        return
    rows = await db_fetchall("SELECT code, used_by, is_active FROM invites ORDER BY created_at DESC LIMIT 20")
    if not rows:
        await call.message.edit_text("Инвайтов нет.", reply_markup=admin_menu_kb())
        return
    lines = ["📃 Инвайты:"]
    for r in rows:
        st = f"использован ({r['used_by']})" if r["used_by"] else ("активен" if r["is_active"] == 1 else "выключен")
        lines.append(f"<code>{r['code']}</code> - {st}")
    await call.message.edit_text("\n".join(lines), reply_markup=admin_menu_kb())


@router.message(Command("admin"))
async def cmd_admin(message: Message) -> None:
    if message.from_user.id == ADMIN_ID:
        await message.answer("👑 Админ-панель", reply_markup=admin_menu_kb())


@router.message(Command("ban"))
async def cmd_ban(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    p = message.text.split()
    if len(p) != 2 or not p[1].isdigit():
        await message.answer("Использование: /ban <user_id>")
        return
    await db_execute("UPDATE users SET is_banned = 1 WHERE user_id = ?", (int(p[1]),))
    await message.answer("Пользователь забанен.")


@router.message(Command("unban"))
async def cmd_unban(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    p = message.text.split()
    if len(p) != 2 or not p[1].isdigit():
        await message.answer("Использование: /unban <user_id>")
        return
    await db_execute("UPDATE users SET is_banned = 0 WHERE user_id = ?", (int(p[1]),))
    await message.answer("Пользователь разбанен.")


@router.message(Command("give"))
async def cmd_give(message: Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    p = message.text.split()
    if len(p) != 3 or not p[1].isdigit() or not p[2].isdigit():
        await message.answer("Использование: /give <user_id> <days>")
        return
    uid, days = int(p[1]), int(p[2])
    await ensure_user(uid, None)
    new_until = await extend_subscription(uid, days)
    await db_execute("UPDATE users SET invited = 1 WHERE user_id = ?", (uid,))
    await message.answer(f"Подписка выдана до {new_until.strftime('%Y-%m-%d %H:%M UTC')}.")


@router.callback_query(F.data == "channels_add")
async def channels_add(call: CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    a = await access_guard(call.from_user.id)
    if not a.ok:
        await call.message.edit_text(a.reason, reply_markup=subscription_kb())
        return
    await state.set_state(ChannelState.waiting_channel_input)
    await call.message.edit_text("Отправьте @username канала или chat_id. Бот должен быть администратором.")


@router.message(ChannelState.waiting_channel_input)
async def process_channel_input(message: Message, state: FSMContext) -> None:
    a = await access_guard(message.from_user.id)
    if not a.ok:
        await message.answer(a.reason)
        return
    chat_id: Any = message.text.strip()
    try:
        chat = await bot.get_chat(chat_id)
        me = await bot.get_me()
        member = await bot.get_chat_member(chat_id=chat.id, user_id=me.id)
        if member.status not in ("administrator", "creator"):
            await message.answer("❌ Бот не администратор в канале.")
            return
        await db_execute(
            "INSERT INTO channels (user_id, chat_id, title, username, is_active) VALUES (?, ?, ?, ?, 1)",
            (message.from_user.id, str(chat.id), chat.title or "Channel", chat.username),
        )
        await state.clear()
        await message.answer("✅ Канал добавлен.", reply_markup=channels_menu_kb())
    except Exception as e:
        await message.answer(f"Ошибка проверки канала: {e}")


@router.callback_query(F.data == "channels_list")
async def channels_list(call: CallbackQuery) -> None:
    await call.answer()
    rows = await db_fetchall("SELECT * FROM channels WHERE user_id = ? AND is_active = 1 ORDER BY id DESC", (call.from_user.id,))
    if not rows:
        await call.message.edit_text("Каналов пока нет.", reply_markup=channels_menu_kb())
        return
    lines = ["📢 Ваши каналы:"]
    for r in rows:
        lines.append(f"ID:{r['id']} | {r['title']} | <code>{r['chat_id']}</code>")
    await call.message.edit_text("\n".join(lines), reply_markup=channels_menu_kb())


@router.callback_query(F.data == "channels_style_memory")
async def channels_style_memory(call: CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    rows = await db_fetchall("SELECT id, title FROM channels WHERE user_id = ? AND is_active = 1 ORDER BY id DESC", (call.from_user.id,))
    if not rows:
        await safe_edit_text(call.message, "Сначала добавьте канал.", reply_markup=channels_menu_kb())
        return
    kb = InlineKeyboardBuilder()
    for r in rows:
        kb.row(InlineKeyboardButton(text=f"{r['title']} (ID {r['id']})", callback_data=f"style_channel_{r['id']}"))
    kb.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_channels"))
    await safe_edit_text(call.message, "Выберите канал для обучения стилю:", reply_markup=kb.as_markup())


@router.callback_query(F.data.startswith("style_channel_"))
async def style_channel_select(call: CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    cid = int(call.data.split("_")[-1])
    row = await db_fetchone("SELECT id FROM channels WHERE id = ? AND user_id = ?", (cid, call.from_user.id))
    if not row:
        await safe_edit_text(call.message, "Канал не найден.", reply_markup=channels_menu_kb())
        return
    await state.set_state(ChannelStyleState.waiting_style_examples)
    await state.update_data(style_channel_id=cid)
    await safe_edit_text(
        call.message,
        "Отправьте 3-5 реальных постов одним сообщением.\n"
        "Разделяйте посты пустой строкой или строкой ---",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Отмена", callback_data="menu_channels")]]),
    )


@router.message(ChannelStyleState.waiting_style_examples)
async def style_receive_examples(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    channel_id = data.get("style_channel_id")
    if not channel_id:
        await state.clear()
        await message.answer("Сессия истекла.")
        return

    raw = message.text.strip()
    if not raw:
        await message.answer("Отправьте текст с примерами постов.")
        return

    chunks = [x.strip() for x in re.split(r"\n\s*\n|^---+$", raw, flags=re.MULTILINE) if x.strip()]
    if len(chunks) < 3:
        await message.answer("Нужно минимум 3 поста.")
        return
    if len(chunks) > 5:
        chunks = chunks[:5]

    try:
        style_prompt = await asyncio.to_thread(analyze_channel_style, chunks, "ru")
    except Exception as e:
        await message.answer(f"Ошибка анализа стиля: {e}")
        return

    await db_execute("UPDATE channels SET style_prompt = ? WHERE id = ? AND user_id = ?", (style_prompt, int(channel_id), message.from_user.id))
    await state.clear()
    await message.answer("✅ Память стиля сохранена для канала.", reply_markup=channels_menu_kb())


@router.callback_query(F.data == "tasks_create")
async def tasks_create(call: CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    ch = await db_fetchall("SELECT id, title FROM channels WHERE user_id = ? AND is_active = 1", (call.from_user.id,))
    if not ch:
        await call.message.edit_text("Сначала добавьте канал.", reply_markup=channels_menu_kb())
        return
    kb = InlineKeyboardBuilder()
    for c in ch:
        kb.row(InlineKeyboardButton(text=f"{c['title']} (ID {c['id']})", callback_data=f"task_channel_{c['id']}"))
    kb.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_tasks"))
    await call.message.edit_text("Выберите канал для задачи:", reply_markup=kb.as_markup())


@router.callback_query(F.data.startswith("task_channel_"))
async def task_channel(call: CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    await state.update_data(channel_id=int(call.data.split("_")[-1]))
    await state.set_state(TaskCreateState.waiting_topic)
    await call.message.edit_text("Введите тему.")


@router.message(TaskCreateState.waiting_topic)
async def task_topic(message: Message, state: FSMContext) -> None:
    await state.update_data(topic=message.text.strip())
    await state.set_state(TaskCreateState.waiting_style)
    await message.answer("Введите стиль (экспертный/дружелюбный/продающий/инфо).")


@router.message(TaskCreateState.waiting_style)
async def task_style(message: Message, state: FSMContext) -> None:
    await state.update_data(style=message.text.strip())
    await state.set_state(TaskCreateState.waiting_posts_per_day)
    await message.answer("Сколько постов в день (1-3)?")


@router.message(TaskCreateState.waiting_posts_per_day)
async def task_ppd(message: Message, state: FSMContext) -> None:
    if not message.text.isdigit() or int(message.text) not in (1, 2, 3):
        await message.answer("Нужно число 1, 2 или 3.")
        return
    await state.update_data(posts_per_day=int(message.text))
    await state.set_state(TaskCreateState.waiting_schedule)
    await message.answer("Введите время(ена) через запятую в формате HH:MM. Пример: 10:00,18:30")


@router.message(TaskCreateState.waiting_schedule)
async def task_schedule(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    ppd = int(data["posts_per_day"])
    times = [parse_hhmm(x) for x in message.text.split(",")]
    times = [x for x in times if x]
    if len(times) != ppd:
        await message.answer(f"Нужно {ppd} валидных времени.")
        return
    await state.update_data(times=",".join(sorted(set(times))))
    await state.set_state(TaskCreateState.waiting_language)
    await message.answer("Введите язык (ru/en).")


@router.message(TaskCreateState.waiting_language)
async def task_lang(message: Message, state: FSMContext) -> None:
    await state.update_data(language=message.text.strip())
    await state.set_state(TaskCreateState.waiting_emojis)
    await message.answer("Эмодзи? да/нет")


@router.message(TaskCreateState.waiting_emojis)
async def task_emojis(message: Message, state: FSMContext) -> None:
    v = message.text.strip().lower()
    emojis = 1 if v in ("да", "yes", "y", "1") else 0
    await state.update_data(emojis=emojis)
    await state.set_state(TaskCreateState.waiting_max_length)
    await message.answer("Введите максимальную длину поста (100-4000).")


@router.message(TaskCreateState.waiting_max_length)
async def task_len(message: Message, state: FSMContext) -> None:
    if not message.text.isdigit():
        await message.answer("Введите число.")
        return
    n = int(message.text)
    if n < 100 or n > 4000:
        await message.answer("Диапазон: 100-4000")
        return
    await state.update_data(max_length=n)
    await state.set_state(TaskCreateState.waiting_hashtags)
    await message.answer("Введите хэштеги (или -).")


@router.message(TaskCreateState.waiting_hashtags)
async def task_hashtags(message: Message, state: FSMContext) -> None:
    h = "" if message.text.strip() == "-" else message.text.strip()
    await state.update_data(hashtags=h)
    await state.set_state(TaskCreateState.waiting_use_web_news)
    await message.answer("Использовать актуальные новости из интернета? (да/нет)")


@router.message(TaskCreateState.waiting_use_web_news)
async def task_use_web_news(message: Message, state: FSMContext) -> None:
    value = message.text.strip().lower()
    use_web_news = 1 if value in ("да", "yes", "y", "1") else 0
    await state.update_data(use_web_news=use_web_news)
    if use_web_news == 1:
        await state.set_state(TaskCreateState.waiting_news_sources)
        await message.answer(
            "Укажите источники новостей через запятую: google, reddit, hn, crypto, telegram\n"
            "Пример: google,reddit,hn"
        )
        return

    data = await state.get_data()
    await db_execute(
        """
        INSERT INTO tasks (
            user_id, channel_id, topic, style, posts_per_day, schedule_type, times,
            language, emojis, max_length, hashtags, tone, stop_words, autopin,
            add_channel_link, use_web_news, news_sources, is_active, created_at
        )
        VALUES (?, ?, ?, ?, ?, 'fixed', ?, ?, ?, ?, ?, 'нейтральная', '', 0, 0, ?, ?, 1, ?)
        """,
        (
            message.from_user.id,
            int(data["channel_id"]),
            data["topic"],
            data["style"],
            int(data["posts_per_day"]),
            data["times"],
            data["language"],
            int(data["emojis"]),
            int(data["max_length"]),
            data.get("hashtags", ""),
            0,
            "",
            dt_to_str(now_utc()),
        ),
    )
    await state.clear()
    await message.answer("✅ Задача создана (без новостей).", reply_markup=tasks_menu_kb())


@router.message(TaskCreateState.waiting_news_sources)
async def task_news_sources(message: Message, state: FSMContext) -> None:
    raw = message.text.strip().lower()
    allowed = {"google", "reddit", "hn", "crypto", "telegram"}
    selected = [s.strip() for s in raw.split(",") if s.strip()]
    selected = [s for s in selected if s in allowed]
    if not selected:
        selected = ["google"]
    sources = ",".join(sorted(set(selected)))

    data = await state.get_data()
    await db_execute(
        """
        INSERT INTO tasks (
            user_id, channel_id, topic, style, posts_per_day, schedule_type, times,
            language, emojis, max_length, hashtags, tone, stop_words, autopin,
            add_channel_link, use_web_news, news_sources, is_active, created_at
        )
        VALUES (?, ?, ?, ?, ?, 'fixed', ?, ?, ?, ?, ?, 'нейтральная', '', 0, 0, ?, ?, 1, ?)
        """,
        (
            message.from_user.id,
            int(data["channel_id"]),
            data["topic"],
            data["style"],
            int(data["posts_per_day"]),
            data["times"],
            data["language"],
            int(data["emojis"]),
            int(data["max_length"]),
            data.get("hashtags", ""),
            int(data.get("use_web_news", 1)),
            sources,
            dt_to_str(now_utc()),
        ),
    )
    await state.clear()
    await message.answer(f"✅ Задача создана. Источники: {sources}", reply_markup=tasks_menu_kb())


@router.callback_query(F.data == "tasks_list")
async def tasks_list(call: CallbackQuery) -> None:
    await call.answer()
    rows = await db_fetchall(
        "SELECT id, topic, times, is_active, use_web_news, news_sources FROM tasks WHERE user_id = ? ORDER BY id DESC",
        (call.from_user.id,),
    )
    if not rows:
        await call.message.edit_text("Задач пока нет.", reply_markup=tasks_menu_kb())
        return
    kb = InlineKeyboardBuilder()
    for r in rows:
        label = f"{'🟢' if r['is_active'] else '🔴'} #{r['id']} {r['topic'][:24]}"
        if r["use_web_news"] == 1:
            label += " 📰"
        kb.row(InlineKeyboardButton(text=label, callback_data=f"task_open_{r['id']}"))
    kb.row(InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_tasks"))
    await call.message.edit_text("Выберите задачу:", reply_markup=kb.as_markup())


@router.callback_query(F.data.startswith("task_open_"))
async def task_open(call: CallbackQuery) -> None:
    await call.answer()
    tid = int(call.data.split("_")[-1])
    t = await db_fetchone("SELECT * FROM tasks WHERE id = ? AND user_id = ?", (tid, call.from_user.id))
    if not t:
        await safe_edit_text(call.message, "Задача не найдена.", reply_markup=tasks_menu_kb())
        return

    media = await db_fetchone("SELECT photo_file_id FROM task_media WHERE task_id = ?", (tid,))
    ch = await db_fetchone("SELECT style_prompt FROM channels WHERE id = ?", (t["channel_id"],))
    has_style_profile = bool(ch and ch["style_prompt"])
    sources = t["news_sources"] if t["news_sources"] else "-"
    text = (
        f"✍️ Задача #{t['id']}\n"
        f"Тема: {html.escape(t['topic'])}\n"
        f"Время: {t['times']}\n"
        f"Новости из интернета: {'да' if t['use_web_news'] == 1 else 'нет'}\n"
        f"Источники: {sources}\n"
        f"Профиль стиля канала: {'есть' if has_style_profile else 'нет'}\n"
        f"Изображение: {'есть' if media else 'нет'}\n"
        f"Статус: {'активна' if t['is_active'] == 1 else 'остановлена'}"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔁 Вкл/Выкл", callback_data=f"task_toggle_{tid}")],
            [InlineKeyboardButton(text="👁 Предпросмотр", callback_data=f"task_preview_{tid}")],
            [InlineKeyboardButton(text="📤 Опубликовать сейчас", callback_data=f"task_publish_now_{tid}")],
            [InlineKeyboardButton(text="🖼 Загрузить картинку", callback_data=f"task_upload_image_{tid}")],
            [InlineKeyboardButton(text="🗞 Источники новостей", callback_data=f"task_news_sources_{tid}")],
            [InlineKeyboardButton(text="🗑 Удалить задачу", callback_data=f"task_delete_{tid}")],
            [InlineKeyboardButton(text="⬅️ К задачам", callback_data="tasks_list")],
        ]
    )
    await safe_edit_text(call.message, text, reply_markup=kb)


@router.callback_query(F.data.startswith("task_toggle_"))
async def task_toggle(call: CallbackQuery) -> None:
    await call.answer()
    tid = int(call.data.split("_")[-1])
    t = await db_fetchone("SELECT * FROM tasks WHERE id = ? AND user_id = ?", (tid, call.from_user.id))
    if not t:
        return
    nv = 0 if t["is_active"] == 1 else 1
    await db_execute("UPDATE tasks SET is_active = ? WHERE id = ?", (nv, tid))
    await task_open(call)


@router.callback_query(F.data.startswith("task_delete_"))
async def task_delete(call: CallbackQuery) -> None:
    await call.answer()
    tid = int(call.data.split("_")[-1])
    await db_execute("DELETE FROM tasks WHERE id = ? AND user_id = ?", (tid, call.from_user.id))
    await db_execute("DELETE FROM task_media WHERE task_id = ?", (tid,))
    await safe_edit_text(call.message, "Задача удалена.", reply_markup=tasks_menu_kb())


def task_news_kb(task_id: int, selected_sources: str) -> InlineKeyboardMarkup:
    selected = {s.strip() for s in selected_sources.split(",") if s.strip()}

    def mark(name: str, label: str) -> str:
        return f"{'☑' if name in selected else '☐'} {label}"

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=mark("google", "Google News"), callback_data=f"task_news_toggle_{task_id}_google")],
            [InlineKeyboardButton(text=mark("reddit", "Reddit"), callback_data=f"task_news_toggle_{task_id}_reddit")],
            [InlineKeyboardButton(text=mark("hn", "Hacker News"), callback_data=f"task_news_toggle_{task_id}_hn")],
            [InlineKeyboardButton(text=mark("crypto", "Crypto"), callback_data=f"task_news_toggle_{task_id}_crypto")],
            [InlineKeyboardButton(text=mark("telegram", "Telegram"), callback_data=f"task_news_toggle_{task_id}_telegram")],
            [InlineKeyboardButton(text="✅ Готово", callback_data=f"task_open_{task_id}")],
        ]
    )


@router.callback_query(F.data.startswith("task_news_sources_"))
async def task_news_sources_menu(call: CallbackQuery) -> None:
    await call.answer()
    tid = int(call.data.split("_")[-1])
    task = await db_fetchone("SELECT id, news_sources FROM tasks WHERE id = ? AND user_id = ?", (tid, call.from_user.id))
    if not task:
        await safe_edit_text(call.message, "Задача не найдена.", reply_markup=tasks_menu_kb())
        return
    current = normalize_news_sources(task["news_sources"] or "google")
    await safe_edit_text(call.message, "Выберите источники новостей:", reply_markup=task_news_kb(tid, current))


@router.callback_query(F.data.startswith("task_news_toggle_"))
async def task_news_toggle(call: CallbackQuery) -> None:
    await call.answer()
    parts = call.data.split("_")
    if len(parts) < 5:
        return
    tid = int(parts[3])
    source = parts[4]

    task = await db_fetchone("SELECT id, news_sources FROM tasks WHERE id = ? AND user_id = ?", (tid, call.from_user.id))
    if not task:
        await safe_edit_text(call.message, "Задача не найдена.", reply_markup=tasks_menu_kb())
        return
    current = {s.strip() for s in normalize_news_sources(task["news_sources"] or "google").split(",") if s.strip()}
    if source in current:
        current.remove(source)
    else:
        current.add(source)
    if not current:
        current.add("google")
    new_sources = ",".join(sorted(current))
    await db_execute("UPDATE tasks SET news_sources = ?, use_web_news = 1 WHERE id = ?", (new_sources, tid))
    await safe_edit_text(call.message, "Выберите источники новостей:", reply_markup=task_news_kb(tid, new_sources))



async def build_post_for_task(task: sqlite3.Row, channel: sqlite3.Row) -> str:
    web_news_context = ""
    if int(task["use_web_news"]) == 1:
        try:
            web_news_context = await asyncio.to_thread(
                build_news_context,
                task["topic"],
                task["language"],
                task["news_sources"] or "google",
            )
        except Exception as e:
            logger.warning("News fetch failed for task %s: %s", task["id"], e)

    link = f"https://t.me/{channel['username']}" if task["add_channel_link"] == 1 and channel["username"] else ""
    style_prompt = channel["style_prompt"] if "style_prompt" in channel.keys() else ""
    text = await asyncio.to_thread(
        generate_post,
        task["topic"],
        task["style"],
        int(task["max_length"]),
        bool(task["emojis"]),
        task["language"],
        task["hashtags"] or "",
        task["tone"] or "нейтральная",
        task["stop_words"] or "",
        link,
        web_news_context,
        style_prompt or "",
    )
    return text


@router.callback_query(F.data.startswith("task_preview_"))
async def task_preview(call: CallbackQuery) -> None:
    await call.answer("Генерирую...")
    tid = int(call.data.split("_")[-1])
    task = await db_fetchone("SELECT * FROM tasks WHERE id = ? AND user_id = ?", (tid, call.from_user.id))
    if not task:
        await safe_edit_text(call.message, "Задача не найдена.", reply_markup=tasks_menu_kb())
        return
    channel = await db_fetchone("SELECT * FROM channels WHERE id = ?", (task["channel_id"],))
    if not channel:
        await safe_edit_text(call.message, "Канал недоступен.", reply_markup=tasks_menu_kb())
        return

    try:
        text = await build_post_for_task(task, channel)
        preview_cache[call.from_user.id] = {"task_id": tid, "text": text}
    except Exception as e:
        await safe_edit_text(
            call.message,
            f"Ошибка генерации: {e}",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=f"task_open_{tid}")]]
            ),
        )
        return

    media = await db_fetchone("SELECT photo_file_id FROM task_media WHERE task_id = ?", (tid,))
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📤 Опубликовать", callback_data=f"task_publish_now_{tid}")],
            [InlineKeyboardButton(text="🔄 Перегенерировать", callback_data=f"task_preview_{tid}")],
            [InlineKeyboardButton(text="⬅️ К задаче", callback_data=f"task_open_{tid}")],
        ]
    )
    if media:
        await call.message.answer_photo(photo=media["photo_file_id"], caption=text, reply_markup=kb)
    else:
        await safe_edit_text(call.message, f"👁 Предпросмотр:\n\n{text}", reply_markup=kb)


@router.callback_query(F.data.startswith("task_publish_now_"))
async def task_publish_now(call: CallbackQuery) -> None:
    await call.answer("Публикую...")
    tid = int(call.data.split("_")[-1])
    task = await db_fetchone("SELECT * FROM tasks WHERE id = ? AND user_id = ?", (tid, call.from_user.id))
    if not task:
        return
    ok, msg = await publish_task(task, "manual_now")
    if ok:
        await call.message.answer("✅ Опубликовано.")
    else:
        await call.message.answer(f"❌ {msg}")


@router.callback_query(F.data.startswith("task_upload_image_"))
async def task_upload_image(call: CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    tid = int(call.data.split("_")[-1])
    t = await db_fetchone("SELECT id FROM tasks WHERE id = ? AND user_id = ?", (tid, call.from_user.id))
    if not t:
        await call.message.answer("Задача не найдена.")
        return
    await state.set_state(TaskMediaState.waiting_task_image)
    await state.update_data(media_task_id=tid)
    await call.message.answer("Отправьте изображение (как фото). Оно будет использоваться в публикациях этой задачи.")


@router.message(TaskMediaState.waiting_task_image)
async def task_image_receive(message: Message, state: FSMContext) -> None:
    if not message.photo:
        await message.answer("Нужно отправить именно фото.")
        return
    data = await state.get_data()
    tid = data.get("media_task_id")
    if not tid:
        await state.clear()
        await message.answer("Сессия загрузки истекла.")
        return
    photo_id = message.photo[-1].file_id
    await db_execute(
        "INSERT OR REPLACE INTO task_media (task_id, photo_file_id, updated_at) VALUES (?, ?, ?)",
        (int(tid), photo_id, dt_to_str(now_utc())),
    )
    await state.clear()
    await message.answer("✅ Картинка сохранена для задачи.", reply_markup=tasks_menu_kb())
@router.callback_query(F.data == "settings_timezone")
async def settings_timezone(call: CallbackQuery, state: FSMContext) -> None:
    await call.answer()
    await state.set_state(SettingsState.waiting_timezone)
    await call.message.edit_text("Введите UTC offset в часах (-12..+14), например 3 или -5")


@router.message(SettingsState.waiting_timezone)
async def set_timezone(message: Message, state: FSMContext) -> None:
    raw = message.text.strip().replace("UTC", "").replace("+", "")
    try:
        off = int(raw)
    except Exception:
        await message.answer("Неверный формат.")
        return
    if off < -12 or off > 14:
        await message.answer("Диапазон: -12..+14")
        return
    await db_execute("UPDATE users SET timezone_offset = ? WHERE user_id = ?", (off, message.from_user.id))
    await state.clear()
    await message.answer(f"✅ UTC{off:+d}", reply_markup=main_menu_kb(admin=(message.from_user.id == ADMIN_ID)))


async def publish_task(task: sqlite3.Row, slot_key: str) -> Tuple[bool, str]:
    u = await get_user(task["user_id"])
    if not u or not subscription_is_active(u["subscription_until"]) or u["is_banned"] == 1:
        return False, "Нет доступа"
    used = await count_posts_today(task["user_id"], int(u["timezone_offset"]))
    if used >= DAILY_LIMIT:
        return False, "Достигнут дневной лимит"
    ch = await db_fetchone("SELECT * FROM channels WHERE id = ? AND is_active = 1", (task["channel_id"],))
    if not ch:
        return False, "Канал не найден"
    try:
        text = await build_post_for_task(task, ch)
    except Exception as e:
        return False, f"Ошибка генерации: {e}"
    try:
        media = await db_fetchone("SELECT photo_file_id FROM task_media WHERE task_id = ?", (task["id"],))
        if media:
            sent = await bot.send_photo(chat_id=ch["chat_id"], photo=media["photo_file_id"], caption=text)
        else:
            sent = await bot.send_message(chat_id=ch["chat_id"], text=text)
        if task["autopin"] == 1:
            try:
                await bot.pin_chat_message(chat_id=ch["chat_id"], message_id=sent.message_id, disable_notification=True)
            except Exception:
                pass
        await db_execute(
            "INSERT INTO posts (user_id, task_id, channel_id, channel_chat_id, telegram_message_id, content, published_at, slot_key) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (task["user_id"], task["id"], ch["id"], str(ch["chat_id"]), int(sent.message_id), text, dt_to_str(now_utc()), slot_key),
        )
        inserted = await db_fetchone(
            "SELECT id FROM posts WHERE channel_chat_id = ? AND telegram_message_id = ? ORDER BY id DESC LIMIT 1",
            (str(ch["chat_id"]), int(sent.message_id)),
        )
        if inserted:
            await db_execute(
                "INSERT OR IGNORE INTO post_stats (post_id, views, reactions, comments, updated_at) VALUES (?, 0, 0, 0, ?)",
                (inserted["id"], dt_to_str(now_utc())),
            )
        return True, "ok"
    except Exception as e:
        return False, f"Ошибка публикации: {e}"


async def scheduler_loop() -> None:
    while True:
        try:
            tasks = await db_fetchall("SELECT t.*, u.timezone_offset, u.subscription_until, u.is_banned FROM tasks t JOIN users u ON u.user_id=t.user_id WHERE t.is_active=1")
            cur = now_utc()
            for task in tasks:
                if task["is_banned"] == 1 or not subscription_is_active(task["subscription_until"]):
                    continue
                off = int(task["timezone_offset"])
                local_now = cur + timedelta(hours=off)
                d = local_now.date()
                for t in [x.strip() for x in task["times"].split(",") if x.strip()]:
                    hhmm = parse_hhmm(t)
                    if not hhmm:
                        continue
                    hh, mm = hhmm.split(":")
                    slot_utc = datetime(d.year, d.month, d.day, int(hh), int(mm), tzinfo=timezone.utc) - timedelta(hours=off)
                    if slot_utc > cur or (cur - slot_utc).total_seconds() > 300:
                        continue
                    key = f"{d.isoformat()}_{hhmm}"
                    ex = await db_fetchone("SELECT id FROM task_runs WHERE task_id = ? AND slot_key = ?", (task["id"], key))
                    if ex:
                        continue
                    await db_execute("INSERT INTO task_runs (task_id, slot_key, status, created_at) VALUES (?, ?, 'processing', ?)", (task["id"], key, dt_to_str(now_utc())))
                    ok, err = await publish_task(task, key)
                    await db_execute("UPDATE task_runs SET status = ?, error = ? WHERE task_id = ? AND slot_key = ?", ("success" if ok else "failed", None if ok else err, task["id"], key))

            users = await db_fetchall("SELECT user_id, subscription_until, notify_3d, notify_expired FROM users WHERE invited = 1")
            n = now_utc()
            for u in users:
                dt = str_to_dt(u["subscription_until"])
                if not dt:
                    continue
                days_left = (dt - n).total_seconds() / 86400
                if 2.0 <= days_left <= 3.0 and u["notify_3d"] == 0:
                    try:
                        await bot.send_message(u["user_id"], "⏳ Подписка закончится через 3 дня.")
                    except Exception:
                        pass
                    await db_execute("UPDATE users SET notify_3d = 1 WHERE user_id = ?", (u["user_id"],))
                if days_left <= 0 and u["notify_expired"] == 0:
                    try:
                        await bot.send_message(u["user_id"], "❌ Подписка закончилась. Автопостинг остановлен.")
                    except Exception:
                        pass
                    await db_execute("UPDATE users SET notify_expired = 1 WHERE user_id = ?", (u["user_id"],))

        except Exception as e:
            logger.exception("Scheduler error: %s", e)
        await asyncio.sleep(30)


@router.channel_post()
@router.edited_channel_post()
async def track_channel_post_stats(message: Message) -> None:
    try:
        if not message.chat or not message.message_id:
            return
        post = await db_fetchone(
            "SELECT id FROM posts WHERE channel_chat_id = ? AND telegram_message_id = ?",
            (str(message.chat.id), int(message.message_id)),
        )
        if not post:
            return
        views = int(getattr(message, "views", 0) or 0)
        reactions = extract_reactions_count(message)
        comments = 0
        await db_execute(
            """
            INSERT OR REPLACE INTO post_stats (post_id, views, reactions, comments, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (post["id"], views, reactions, comments, dt_to_str(now_utc())),
        )
    except Exception as e:
        logger.warning("Failed to track post stats: %s", e)


async def create_startup_invite() -> str:
    code = secrets.token_hex(4).upper()
    await db_execute(
        "INSERT OR REPLACE INTO invites (code, created_by, is_active, created_at) VALUES (?, ?, 1, ?)",
        (code, ADMIN_ID, dt_to_str(now_utc())),
    )
    print(f"[STARTUP] New invite code: {code}")
    return code


@dp.startup()
async def on_startup() -> None:
    init_db()
    await create_startup_invite()
    asyncio.create_task(scheduler_loop())
    logger.info("Bot started")


@router.message()
async def fallback(message: Message) -> None:
    await ensure_user(message.from_user.id, message.from_user.username)
    u = await get_user(message.from_user.id)
    if u and u["is_banned"] == 1:
        return
    await message.answer("Используйте /start")


async def main() -> None:
    init_db()
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
