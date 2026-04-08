# -*- coding: utf-8 -*-
"""
DIAMOND VAULT MAX
Aiogram 3.25.0 / Python 3.11 / Railway ready / one file

Настрой перед запуском:
- BOT_TOKEN
- ADMIN_IDS
- REQUIRED_CHANNEL_ID
- REQUIRED_CHANNEL_USERNAME
- LOG_CHANNEL_ID
- WITHDRAWALS_CHANNEL_ID
- CRYPTO_PAY_TOKEN
- CRYPTO_PAY_USE_TESTNET
- TREASURY_ASSET

Railway:
- Start command: python bot.py
"""

import asyncio
import os
import html
import logging
import re
import sqlite3
import shutil
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ChatType, ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

# =========================
# CONFIG
# =========================
BOT_TOKEN = "8659417974:AAGqa6FH47rB1HGrX9WFIFaAixpoHwWv-PE"

ADMIN_IDS = {
    626387429,  # <- впиши id
}

REQUIRED_CHANNEL_ID = -1003827772392
REQUIRED_CHANNEL_USERNAME = "DiamondVaultV"

LOG_CHANNEL_ID = -1003736283466
WITHDRAWALS_CHANNEL_ID = -1003600949221

CRYPTO_PAY_TOKEN = "555845:AAT75KSRUMfbmYZTFOv0uOeFMpndKhzwjq4"
CRYPTO_PAY_USE_TESTNET = False
TREASURY_ASSET = "USDT"

PRICE_QR = 4.0
PRICE_CODE = 4.0

ACTIVITY_SECONDS = 60
CODE_SECONDS = 90
HOLD_SECONDS = 5 * 60

QUEUE_AFK_CHECK_EVERY = 15 * 60
QUEUE_AFK_CONFIRM_SECONDS = 60
QUEUE_AFK_MAX_BUMPS = 2
DEAD_SPAM_LIMIT = 3
DEAD_SPAM_BLOCK_HOURS = 1

OLD_DB_PATH = "diamond_vault.sqlite3"
OLD_LOG_PATH = "diamond_vault.log"

if os.name == "nt":
    DB_PATH = "diamond_vault.sqlite3"
    LOG_FILE_PATH = "diamond_vault.log"
else:
    DB_PATH = "/data/diamond_vault.sqlite3"
    LOG_FILE_PATH = "/data/diamond_vault.log"

def migrate_local_files_to_volume():
    if os.name == "nt":
        return
    try:
        if os.path.exists("/data"):
            Path("/data").mkdir(parents=True, exist_ok=True)
            if os.path.exists(OLD_DB_PATH) and not os.path.exists(DB_PATH):
                shutil.copy2(OLD_DB_PATH, DB_PATH)
            if os.path.exists(OLD_LOG_PATH) and not os.path.exists(LOG_FILE_PATH):
                shutil.copy2(OLD_LOG_PATH, LOG_FILE_PATH)
    except Exception:
        pass

PHONE_RE = re.compile(r"^(?:\+7|7|8)\d{10}$")
CODE_RE = re.compile(r"^\d{6}$")
MSK = timezone(timedelta(hours=3))

# =========================
# BOT
# =========================
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher(storage=MemoryStorage())
issue_lock = asyncio.Lock()

# runtime tasks
activity_tasks: dict[int, asyncio.Task] = {}
code_tasks: dict[int, asyncio.Task] = {}
qr_tasks: dict[int, asyncio.Task] = {}
hold_tasks: dict[int, asyncio.Task] = {}
queue_tasks: dict[int, asyncio.Task] = {}
password_tasks: dict[int, asyncio.Task] = {}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("diamond_vault")
try:
    Path("/data").mkdir(parents=True, exist_ok=True)
except Exception:
    pass
try:
    file_handler = logging.FileHandler(LOG_FILE_PATH, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logging.getLogger().addHandler(file_handler)
except Exception:
    pass

# =========================
# FSM
# =========================
class SubmitStates(StatesGroup):
    waiting_phone_qr = State()
    waiting_phone_code = State()

class WithdrawStates(StatesGroup):
    waiting_amount = State()

class AdminStates(StatesGroup):
    add_admin = State()
    del_admin = State()
    add_group = State()
    del_group = State()
    add_balance = State()
    sub_balance = State()
    user_lookup = State()
    ban_user = State()
    unban_user = State()
    remove_numbers = State()
    set_profit = State()
    delete_queue_phone = State()
    temp_disable_text = State()
    treasury_topup_amount = State()
    broadcast_text = State()
    clear_number_block = State()
    set_ref_reward = State()
    upload_db = State()
    upload_log = State()
    dm_user = State()
    dm_text = State()
    report_custom_date = State()
    clear_dead_block = State()
    set_title_prices = State()
    set_rep_settings = State()
    add_role = State()
    del_role = State()
    aprofile_date = State()
    remove_my_queue = State()

# =========================
# DB
# =========================
@contextmanager
def db_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

def db_init():
    try:
        if os.path.exists(OLD_DB_PATH) and not os.path.exists(DB_PATH):
            Path("/data").mkdir(parents=True, exist_ok=True)
            shutil.copy2(OLD_DB_PATH, DB_PATH)
    except Exception:
        pass
    with db_conn() as conn:
        def _ensure_column(name_sql: str):
            try:
                conn.execute(name_sql)
            except Exception:
                pass

        conn.execute("CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT)")
        conn.execute("CREATE TABLE IF NOT EXISTS admins(user_id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE IF NOT EXISTS user_roles(user_id INTEGER PRIMARY KEY, role TEXT NOT NULL)")
        conn.execute("CREATE TABLE IF NOT EXISTS groups_work(chat_id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE IF NOT EXISTS topics_work(chat_id INTEGER NOT NULL, topic_id INTEGER NOT NULL, PRIMARY KEY(chat_id, topic_id))")
        conn.execute("CREATE TABLE IF NOT EXISTS banned_users(user_id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE IF NOT EXISTS number_blocks(phone TEXT PRIMARY KEY, blocked_until TEXT NOT NULL, req_id INTEGER, note TEXT)")
        conn.execute("CREATE TABLE IF NOT EXISTS referrals(referred_user_id INTEGER PRIMARY KEY, referrer_user_id INTEGER NOT NULL, rewarded INTEGER NOT NULL DEFAULT 0, reward_amount REAL NOT NULL DEFAULT 0, created_at TEXT NOT NULL)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users(
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                balance REAL NOT NULL DEFAULT 0,
                pending REAL NOT NULL DEFAULT 0,
                dead_streak INTEGER NOT NULL DEFAULT 0,
                blocked_until TEXT,
                registered_at TEXT
            )
        """)
        _ensure_column("ALTER TABLE users ADD COLUMN registered_at TEXT")
        try:
            conn.execute("ALTER TABLE users ADD COLUMN dead_streak INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE users ADD COLUMN blocked_until TEXT")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE requests ADD COLUMN hold_group_message_id INTEGER")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE requests ADD COLUMN password_requested INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE requests ADD COLUMN password_value TEXT")
        except Exception:
            pass
        conn.execute("""
            CREATE TABLE IF NOT EXISTS requests(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                phone TEXT NOT NULL,
                method TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                claimed_by INTEGER,
                issue_group_id INTEGER,
                issue_thread_id INTEGER,
                user_activity_message_id INTEGER,
                user_code_message_id INTEGER,
                hold_user_message_id INTEGER,
                hold_group_message_id INTEGER,
                hold_started_at TEXT,
                sms_code TEXT,
                qr_file_id TEXT,
                password_requested INTEGER NOT NULL DEFAULT 0,
                password_value TEXT,
                credited INTEGER NOT NULL DEFAULT 0,
                queue_bumps INTEGER NOT NULL DEFAULT 0,
                queue_last_ping_at TEXT,
                queue_ping_deadline_at TEXT,
                repeat_count INTEGER NOT NULL DEFAULT 0
            )
        """)
        # safety migrations for older sqlite files
        _ensure_column("ALTER TABLE requests ADD COLUMN hold_group_message_id INTEGER")
        _ensure_column("ALTER TABLE requests ADD COLUMN password_requested INTEGER NOT NULL DEFAULT 0")
        _ensure_column("ALTER TABLE requests ADD COLUMN password_value TEXT")
        _ensure_column("ALTER TABLE requests ADD COLUMN repeat_count INTEGER NOT NULL DEFAULT 0")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS withdrawals(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                amount REAL NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                payout_url TEXT
            )
        """)
        # bootstrap roles/default settings
        for uid in ADMIN_IDS:
            try:
                conn.execute("INSERT OR IGNORE INTO user_roles(user_id, role) VALUES(?, 'super_admin')", (int(uid),))
            except Exception:
                pass
        defaults = {
            "submit_enabled": "1",
            "title_price_newbie": "4.0",
            "title_price_bronze": "4.1",
            "title_price_silver": "4.2",
            "title_price_gold": "4.25",
            "title_price_platinum": "4.3",
            "rep_bonus_perfect": "10",
            "rep_bonus_reliable": "7",
            "rep_bonus_stable": "4",
            "rep_bonus_risky": "1",
            "rep_bonus_problem": "-3",
            "newbie_priority_bonus": "6",
            "rep_influence_enabled": "1",
            "reputation_window": "50",
        }
        for k, v in defaults.items():
            conn.execute("INSERT OR IGNORE INTO settings(key, value) VALUES(?, ?)", (k, v))

        conn.execute("""
            CREATE TABLE IF NOT EXISTS reports(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                req_id INTEGER,
                number TEXT,
                type TEXT,
                tag TEXT,
                status TEXT,
                amount REAL,
                profit REAL,
                created_at TEXT
            )
        """)
        defaults = {
            "bot_enabled": "1",
            "bot_mode": "work",
            "bot_mode_text": "",
            "profit_per_number": "0",
            "stats_paid_total": "0",
            "stats_profit_total": "0",
            "stats_success_count": "0",
            "stats_drop_count": "0",
            "stats_qr_success": "0",
            "stats_code_success": "0",
            "stats_dead_count": "0",
            "stats_error_count": "0",
            "stats_today_paid": "0",
            "ref_reward_amount": "0",
        }
        for k, v in defaults.items():
            conn.execute("INSERT OR IGNORE INTO settings(key, value) VALUES(?, ?)", (k, v))
        for admin_id in ADMIN_IDS:
            conn.execute("INSERT OR IGNORE INTO admins(user_id) VALUES(?)", (admin_id,))

def get_setting(key: str, default: str = "") -> str:
    with db_conn() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

def set_setting(key: str, value: str):
    with db_conn() as conn:
        conn.execute("""
            INSERT INTO settings(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """, (key, value))

def bot_enabled() -> bool:
    return get_setting("bot_enabled", "1") == "1"

def set_bot_enabled(enabled: bool):
    set_setting("bot_enabled", "1" if enabled else "0")

def get_bot_mode() -> tuple[str, str]:
    return get_setting("bot_mode", "work"), get_setting("bot_mode_text", "")

def set_bot_mode(mode: str, text: str):
    set_setting("bot_mode", mode)
    set_setting("bot_mode_text", text)

def bot_mode_text() -> str:
    mode, text = get_bot_mode()
    title = {
        "work": "✅ Работа",
        "tech": "🛠 Тех.перерыв",
        "stop": "⛔ Стоп работа",
        "temp": "⏳ Временное отключение",
    }.get(mode, "⛔ Отключено")
    return title + (f"\n\n{text}" if text else "")

def submit_enabled() -> bool:
    maybe_resume_from_lunch()
    return get_setting("submit_enabled", "1") == "1"

def set_submit_enabled(enabled: bool):
    set_setting("submit_enabled", "1" if enabled else "0")

def set_lunch_until(dt_iso: str):
    set_setting("lunch_until", dt_iso)

def maybe_resume_from_lunch():
    mode, _ = get_bot_mode()
    if mode != "lunch":
        return
    raw = get_setting("lunch_until", "")
    if not raw:
        return
    try:
        until = datetime.fromisoformat(raw)
    except Exception:
        return
    if datetime.now(timezone.utc) >= until:
        set_bot_mode("work", "")
        set_submit_enabled(True)
        set_lunch_until("")

TITLE_RULES = [
    ("🔰 Новичок", "newbie", 0, 29),
    ("🥉 Бронза", "bronze", 30, 99),
    ("🥈 Серебро", "silver", 100, 249),
    ("🥇 Золото", "gold", 250, 499),
    ("💎 Платина", "platinum", 500, 10**9),
]

def success_count_user(user_id: int) -> int:
    with db_conn() as conn:
        row = conn.execute("SELECT COUNT(*) c FROM requests WHERE user_id=? AND credited=1", (user_id,)).fetchone()
        return int(row["c"]) if row else 0

def get_title_price(code: str) -> float:
    return float(get_setting(f"title_price_{code}", {
        "newbie":"4.0","bronze":"4.1","silver":"4.2","gold":"4.25","platinum":"4.3"
    }[code]))

def set_title_prices_map(mp: dict):
    for k, v in mp.items():
        set_setting(f"title_price_{k}", str(v))

def get_user_title(user_id: int):
    succ = success_count_user(user_id)
    for title, code, lo, hi in TITLE_RULES:
        if lo <= succ <= hi:
            return code, title, get_title_price(code)
    return "newbie", "🔰 Новичок", get_title_price("newbie")

def title_progress_text(user_id: int) -> str:
    succ = success_count_user(user_id)
    for i, (title, code, lo, hi) in enumerate(TITLE_RULES):
        if lo <= succ <= hi:
            if i == len(TITLE_RULES)-1:
                return f"Прогресс титула: {title}\n██████████ 100%"
            next_lo = TITLE_RULES[i+1][2]
            total = next_lo - lo
            current = succ - lo
            pct = max(0, min(100, int(current / total * 100))) if total else 100
            bars = int(pct/10)
            return f"Прогресс титула: {title}\n" + ("█"*bars + "░"*(10-bars)) + f" {pct}%"
    return ""

def get_rep_bonus_map():
    return {
        "🧼 Безупречный": int(get_setting("rep_bonus_perfect", "10")),
        "✅ Надёжный": int(get_setting("rep_bonus_reliable", "7")),
        "👌 Стабильный": int(get_setting("rep_bonus_stable", "4")),
        "⚠ Рискованный": int(get_setting("rep_bonus_risky", "1")),
        "🚫 Проблемный": int(get_setting("rep_bonus_problem", "-3")),
    }

def reputation_from_reports(user_id: int):
    window = int(get_setting("reputation_window", "50"))
    with db_conn() as conn:
        rows = conn.execute("SELECT status FROM reports WHERE tag=? ORDER BY id DESC LIMIT ?", ((get_user(user_id)['username'] or str(user_id)).lower() if get_user(user_id) else str(user_id), window)).fetchall()
    if not rows:
        bonus = int(get_setting("newbie_priority_bonus","6"))
        return "👌 Стабильный", "👌 Стабильный", 100, bonus
    succ = sum(1 for r in rows if r["status"]=="Успешно")
    dead = sum(1 for r in rows if r["status"]=="Мёртвый")
    err = sum(1 for r in rows if r["status"]=="Ошибка")
    total = len(rows)
    pct = max(0, min(100, int((succ / total) * 100)))
    if dead == 0 and pct >= 90:
        title = "🧼 Безупречный"
    elif pct >= 75 and dead <= 1:
        title = "✅ Надёжный"
    elif pct >= 50:
        title = "👌 Стабильный"
    elif dead >= 3 or pct < 25:
        title = "🚫 Проблемный"
    else:
        title = "⚠ Рискованный"
    return title, title, pct, get_rep_bonus_map()[title]

def newbie_priority_bonus(user_id: int) -> int:
    with db_conn() as conn:
        row = conn.execute("SELECT COUNT(*) c FROM requests WHERE user_id=?", (user_id,)).fetchone()
        total = int(row["c"]) if row else 0
    return int(get_setting("newbie_priority_bonus","6")) if total < 30 else 0

def get_role(user_id: int) -> str:
    with db_conn() as conn:
        row = conn.execute("SELECT role FROM user_roles WHERE user_id=?", (user_id,)).fetchone()
    return row["role"] if row else "user"

def set_role(user_id: int, role: str):
    with db_conn() as conn:
        conn.execute("INSERT INTO user_roles(user_id, role) VALUES(?, ?) ON CONFLICT(user_id) DO UPDATE SET role=excluded.role", (user_id, role))

def remove_role(user_id: int):
    with db_conn() as conn:
        conn.execute("DELETE FROM user_roles WHERE user_id=?", (user_id,))

def can_manage_role(actor_id: int, target_role: str) -> bool:
    actor = get_role(actor_id)
    if actor == "super_admin":
        return True
    if actor == "admin":
        return target_role in ("admin","operator","user")
    return False

def role_title(role: str) -> str:
    return {"super_admin":"👑 Супер-Админ","admin":"🛡 Админ","operator":"🎧 Оператор","user":"👤 Пользователь"}.get(role, role)

def is_admin(user_id: int) -> bool:
    return get_role(user_id) in ("super_admin","admin")

def is_operator(user_id: int) -> bool:
    return get_role(user_id) in ("super_admin","admin","operator")

def request_price(method: str, user_id: int | None = None) -> float:
    if user_id is None:
        return PRICE_QR if method == "QR" else PRICE_CODE
    return get_user_title(user_id)[2]

def display_req_id(req_row) -> int:
    try:
        dt = datetime.fromisoformat(req_row["created_at"]).astimezone(MSK)
    except Exception:
        return int(req_row["id"])
    start = dt.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).isoformat()
    end = dt.replace(hour=23, minute=59, second=59, microsecond=999999).astimezone(timezone.utc).isoformat()
    with db_conn() as conn:
        row = conn.execute("SELECT COUNT(*) c FROM requests WHERE created_at>=? AND created_at<=? AND id<=?", (start, end, req_row["id"])).fetchone()
    return int(row["c"]) if row else int(req_row["id"])

def get_profit_per_number() -> float:
    return float(get_setting("profit_per_number", "0"))

def set_profit_per_number(value: float):
    set_setting("profit_per_number", str(value))

def get_stats() -> dict:
    return {
        "paid_total": float(get_setting("stats_paid_total", "0")),
        "profit_total": float(get_setting("stats_profit_total", "0")),
        "success_count": int(get_setting("stats_success_count", "0")),
        "drop_count": int(get_setting("stats_drop_count", "0")),
        "qr_success": int(get_setting("stats_qr_success", "0")),
        "code_success": int(get_setting("stats_code_success", "0")),
        "dead_count": int(get_setting("stats_dead_count", "0")),
        "error_count": int(get_setting("stats_error_count", "0")),
    }

def save_stats(stats: dict):
    set_setting("stats_paid_total", str(stats["paid_total"]))
    set_setting("stats_profit_total", str(stats["profit_total"]))
    set_setting("stats_success_count", str(stats["success_count"]))
    set_setting("stats_drop_count", str(stats["drop_count"]))
    set_setting("stats_qr_success", str(stats["qr_success"]))
    set_setting("stats_code_success", str(stats["code_success"]))
    set_setting("stats_dead_count", str(stats["dead_count"]))
    set_setting("stats_error_count", str(stats["error_count"]))

def get_ref_reward_amount() -> float:
    return float(get_setting("ref_reward_amount", "0"))

def set_ref_reward_amount(value: float):
    set_setting("ref_reward_amount", str(value))

def list_admins() -> list[tuple[int, str]]:
    with db_conn() as conn:
        rows = conn.execute("SELECT user_id, role FROM user_roles WHERE role IN ('super_admin','admin','operator') ORDER BY CASE role WHEN 'super_admin' THEN 3 WHEN 'admin' THEN 2 WHEN 'operator' THEN 1 ELSE 0 END DESC, user_id").fetchall()
        return [(int(r["user_id"]), r["role"]) for r in rows]

def add_admin_db(user_id: int, role: str = "admin"):
    set_role(user_id, role)

def del_admin_db(user_id: int):
    if get_role(user_id) == "super_admin":
        return False
    remove_role(user_id)
    return True

def ensure_user(user):
    with db_conn() as conn:
        conn.execute("""
            INSERT INTO users(user_id, username, first_name, last_name, balance, pending, dead_streak, blocked_until, registered_at)
            VALUES(?, ?, ?, ?, 0, 0, 0, NULL, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name,
                last_name=excluded.last_name
        """, (user.id, getattr(user, "username", None), getattr(user, "first_name", ""), getattr(user, "last_name", ""), datetime.now(timezone.utc).isoformat()))

def get_user(user_id: int):
    with db_conn() as conn:
        return conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()

def get_balance(user_id: int) -> float:
    row = get_user(user_id)
    return float(row["balance"]) if row else 0.0

def get_pending(user_id: int) -> float:
    row = get_user(user_id)
    return float(row["pending"]) if row else 0.0

def add_balance(user_id: int, amount: float):
    with db_conn() as conn:
        conn.execute("UPDATE users SET balance=balance+? WHERE user_id=?", (amount, user_id))

def sub_balance(user_id: int, amount: float):
    with db_conn() as conn:
        conn.execute("UPDATE users SET balance=balance-? WHERE user_id=?", (amount, user_id))

def add_pending(user_id: int, amount: float):
    with db_conn() as conn:
        conn.execute("UPDATE users SET pending=pending+? WHERE user_id=?", (amount, user_id))

def sub_pending(user_id: int, amount: float):
    with db_conn() as conn:
        conn.execute("UPDATE users SET pending=pending-? WHERE user_id=?", (amount, user_id))


def reset_dead_streak(user_id: int):
    with db_conn() as conn:
        conn.execute("UPDATE users SET dead_streak=0 WHERE user_id=?", (user_id,))

def increment_dead_streak(user_id: int):
    with db_conn() as conn:
        row = conn.execute("SELECT dead_streak FROM users WHERE user_id=?", (user_id,)).fetchone()
        current = int(row["dead_streak"]) if row else 0
        current += 1
        blocked_until = None
        if current >= DEAD_SPAM_LIMIT:
            blocked_until = (datetime.now(timezone.utc) + timedelta(hours=DEAD_SPAM_BLOCK_HOURS)).isoformat()
            current = 0
        conn.execute("UPDATE users SET dead_streak=?, blocked_until=? WHERE user_id=?", (current, blocked_until, user_id))

def set_dead_streak(user_id: int, value: int):
    with db_conn() as conn:
        conn.execute("UPDATE users SET dead_streak=? WHERE user_id=?", (value, user_id))

def set_dead_block_until(user_id: int, value: str):
    with db_conn() as conn:
        conn.execute("UPDATE users SET blocked_until=? WHERE user_id=?", (value or None, user_id))

def blocked_for_dead_numbers(user_id: int) -> str | None:
    row = get_user(user_id)
    if not row or not row["blocked_until"]:
        return None
    until = datetime.fromisoformat(row["blocked_until"])
    if datetime.now(timezone.utc) < until:
        return f"⛔️ Из-за мёртвых номеров сдача временно заблокирована до {until.astimezone(MSK).strftime('%H:%M:%S %d.%m')} (МСК)"
    with db_conn() as conn:
        conn.execute("UPDATE users SET blocked_until=NULL WHERE user_id=?", (user_id,))
    return None


def next_midnight_msk() -> datetime:
    now_msk = datetime.now(MSK)
    nxt = (now_msk + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return nxt.astimezone(timezone.utc)

def block_phone_until_midnight(phone: str, req_id: int | None = None):
    with db_conn() as conn:
        conn.execute("""
            INSERT INTO number_blocks(phone, blocked_until, req_id, note) VALUES(?, ?, ?, ?)
            ON CONFLICT(phone) DO UPDATE SET blocked_until=excluded.blocked_until, req_id=excluded.req_id, note=excluded.note
        """, (phone, next_midnight_msk().isoformat(), req_id, "success"))

def active_phone_block(phone: str) -> str | None:
    with db_conn() as conn:
        row = conn.execute("SELECT blocked_until FROM number_blocks WHERE phone=?", (phone,)).fetchone()
    if not row:
        return None
    dt = parse_dt(row["blocked_until"])
    if dt and utc_now() < dt:
        return dt.astimezone(MSK).strftime("%H:%M %d.%m")
    with db_conn() as conn:
        conn.execute("DELETE FROM number_blocks WHERE phone=?", (phone,))
    return None

def clear_phone_block(phone: str) -> bool:
    with db_conn() as conn:
        cur = conn.execute("DELETE FROM number_blocks WHERE phone=?", (phone,))
        return cur.rowcount > 0

def total_paid_today() -> float:
    today = datetime.now(MSK).date()
    with db_conn() as conn:
        rows = conn.execute("SELECT amount, created_at FROM reports WHERE status='Успешно'").fetchall()
    total = 0.0
    for r in rows:
        try:
            dt = datetime.strptime(r["created_at"], "%d.%m.%Y %H:%M:%S").date()
            if dt == today:
                total += float(r["amount"])
        except Exception:
            pass
    return total

def set_referrer_if_missing(user_id: int, referrer_id: int):
    if user_id == referrer_id:
        return
    with db_conn() as conn:
        exists = conn.execute("SELECT 1 FROM referrals WHERE referred_user_id=?", (user_id,)).fetchone()
        if not exists:
            conn.execute("INSERT INTO referrals(referred_user_id, referrer_user_id, rewarded, reward_amount, created_at) VALUES(?, ?, 0, 0, ?)", (user_id, referrer_id, utc_iso()))
    try:
        asyncio.create_task(send_log(f"🎁 Новый реферал | referrer={referrer_id} | referred={user_id}"))
    except Exception:
        pass

def reward_referrer_if_needed(user_id: int):
    reward = get_ref_reward_amount()
    if reward <= 0:
        return
    with db_conn() as conn:
        row = conn.execute("SELECT * FROM referrals WHERE referred_user_id=?", (user_id,)).fetchone()
        if not row or int(row["rewarded"]) == 1:
            return
        add_balance(int(row["referrer_user_id"]), reward)
        conn.execute("UPDATE referrals SET rewarded=1, reward_amount=? WHERE referred_user_id=?", (reward, user_id))
    try:
        asyncio.create_task(bot.send_message(int(row["referrer_user_id"]), f"🎁 <b>Реферальная награда</b>\n\nТвой реферал выполнил условие первого успешного номера.\nНачислено: <b>{fmt_money(reward)}</b>"))
    except Exception:
        pass

def requests_by_user_for_day(user_id: int, mode: str = "today"):
    with db_conn() as conn:
        rows = conn.execute("SELECT * FROM requests WHERE user_id=? ORDER BY id DESC", (user_id,)).fetchall()
    if mode == "all":
        return rows
    today = datetime.now(MSK).date()
    target = today if mode == "today" else (today - timedelta(days=1))
    out = []
    for r in rows:
        dt = parse_dt(r["created_at"])
        if dt and dt.astimezone(MSK).date() == target:
            out.append(r)
    return out
def is_banned(user_id: int) -> bool:
    with db_conn() as conn:
        return bool(conn.execute("SELECT 1 FROM banned_users WHERE user_id=?", (user_id,)).fetchone())

def ban_user_db(user_id: int):
    with db_conn() as conn:
        conn.execute("INSERT OR IGNORE INTO banned_users(user_id) VALUES(?)", (user_id,))

def unban_user_db(user_id: int):
    with db_conn() as conn:
        conn.execute("DELETE FROM banned_users WHERE user_id=?", (user_id,))

def user_by_any(raw: str):
    raw = raw.strip()
    tag = raw.lower().replace("@", "")
    with db_conn() as conn:
        if tag.isdigit():
            row = conn.execute("SELECT * FROM users WHERE user_id=?", (int(tag),)).fetchone()
            if row:
                return row
        row = conn.execute("SELECT * FROM users WHERE lower(username)=?", (tag,)).fetchone()
        if row:
            return row
        phone = normalize_phone(raw)
        if phone:
            row = conn.execute("""
                SELECT u.* FROM requests r
                JOIN users u ON u.user_id=r.user_id
                WHERE r.phone=?
                ORDER BY r.id DESC LIMIT 1
            """, (phone,)).fetchone()
            if row:
                return row
    return None

def enable_group(chat_id: int):
    with db_conn() as conn:
        conn.execute("INSERT OR IGNORE INTO groups_work(chat_id) VALUES(?)", (chat_id,))

def disable_group(chat_id: int):
    with db_conn() as conn:
        conn.execute("DELETE FROM groups_work WHERE chat_id=?", (chat_id,))

def is_group_enabled(chat_id: int) -> bool:
    with db_conn() as conn:
        return bool(conn.execute("SELECT 1 FROM groups_work WHERE chat_id=?", (chat_id,)).fetchone())

def list_groups() -> list[int]:
    with db_conn() as conn:
        return [int(r["chat_id"]) for r in conn.execute("SELECT chat_id FROM groups_work ORDER BY chat_id").fetchall()]

def add_topic(chat_id: int, topic_id: int):
    with db_conn() as conn:
        conn.execute("INSERT OR IGNORE INTO topics_work(chat_id, topic_id) VALUES(?, ?)", (chat_id, topic_id))

def del_topic(chat_id: int, topic_id: int):
    with db_conn() as conn:
        conn.execute("DELETE FROM topics_work WHERE chat_id=? AND topic_id=?", (chat_id, topic_id))

def is_topic_enabled(chat_id: int, topic_id: int | None) -> bool:
    if topic_id is None:
        return False
    with db_conn() as conn:
        return bool(conn.execute("SELECT 1 FROM topics_work WHERE chat_id=? AND topic_id=?", (chat_id, topic_id)).fetchone())

def list_topics():
    with db_conn() as conn:
        return conn.execute("SELECT chat_id, topic_id FROM topics_work ORDER BY chat_id, topic_id").fetchall()

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def utc_iso() -> str:
    return utc_now().isoformat()

def parse_dt(value: str | None) -> datetime | None:
    try:
        return datetime.fromisoformat(value) if value else None
    except Exception:
        return None

def request_text_short(req) -> str:
    return (
        f"• Номер: <code>{req['phone']}</code>\n"
        f"• Метод: <b>{req['method']}</b>\n"
        f"• Заявка: <b>#{req['id']}</b>"
    )

def withdraw_text(user_id: int) -> str:
    return (
        "💸 <b>ВЫВОД СРЕДСТВ | DIAMOND VAULT</b>\n\n"
        "Ваши финансовые данные:\n\n"
        "━━━━━━━━━━━━━━\n"
        f"💰 Доступно: <b>{fmt_money(get_balance(user_id))}</b>\n"
        f"🧾 В обработке: <b>{fmt_money(get_pending(user_id))}</b>\n"
        "━━━━━━━━━━━━━━\n\n"
        "Введите сумму для вывода числом ⬇️\n\n"
        "⚠️ Убедитесь, что сумма не превышает доступный баланс"
    )

def hold_closed_text(number: str, amount: float, balance: float) -> str:
    return (
        "💰 <b>ХОЛД ЗАКРЫТ | DIAMOND VAULT</b>\n\n"
        "Заявка успешно завершена:\n\n"
        "━━━━━━━━━━━━━━\n"
        f"📱 Номер: <code>{number}</code>\n"
        f"💎 Начислено: <b>{fmt_money(amount)}</b>\n"
        f"💰 Баланс: <b>{fmt_money(balance)}</b>\n"
        "━━━━━━━━━━━━━━\n\n"
        "Средства зачислены на ваш счёт ✔️"
    )

def normalize_phone(phone: str) -> str | None:
    phone = phone.strip().replace(" ", "")
    if not PHONE_RE.match(phone):
        return None
    if phone.startswith("8"):
        return "+7" + phone[1:]
    if phone.startswith("7"):
        return "+" + phone
    return phone

def fmt_money(value: float) -> str:
    return f"{int(value)}$" if int(value) == value else f"{value:.2f}$"

def seconds_to_mmss(seconds: int) -> str:
    seconds = max(0, int(seconds))
    m, s = divmod(seconds, 60)
    return f"{m:02d}:{s:02d}"

def seconds_to_hhmmss(seconds: int) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def progress_bar(done: int, total: int, cells: int = 10) -> str:
    if total <= 0:
        return "▱" * cells
    fill = min(cells, int(done / total * cells))
    return "▰" * fill + "▱" * (cells - fill)

def make_username(username: str | None, user_id: int) -> str:
    return f"@{username}" if username else f"<code>{user_id}</code>"

def normalize_trigger(text: str) -> str:
    return (text or "").strip().lower()

def is_qr_trigger(text: str) -> bool:
    return normalize_trigger(text) in {"/qr", "qr", "куар", "/куар"}

def is_code_trigger(text: str) -> bool:
    return normalize_trigger(text) in {"/code", "code", "номер", "ном", "/номер", "/ном"}

def request_public_status(status: str) -> str:
    mapping = {
        "queued": "🟡 В очереди",
        "awaiting_activity": "⚡️ Ждём активность",
        "alive_confirmed": "✅ Пользователь в сети",
        "awaiting_sms": "🔑 Ждём код",
        "awaiting_qr": "📷 Ждём QR",
        "code_received": "📨 Код получен",
        "qr_sent_to_user": "📷 QR отправлен",
        "qr_scanned": "✅ Готово",
        "qr_skipped": "⏭ Скип",
        "success_hold": "⏳ Холд",
        "closed_paid": "✅ Оплачено",
        "closed_dead": "⛔️ Мёртвая",
        "closed_error": "❌ Ошибка",
        "closed_cancelled": "🚫 Отменена",
        "closed_drop_no_pay": "💥 Слёт без оплаты",
        "closed_drop_paid": "💥 Слёт после холда",
    }
    return mapping.get(status, status)

def phone_submitted_last_24h(phone: str) -> bool:
    since = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    with db_conn() as conn:
        return bool(conn.execute("SELECT 1 FROM requests WHERE phone=? AND created_at>=? LIMIT 1", (phone, since)).fetchone())

def phone_already_held(phone: str) -> bool:
    with db_conn() as conn:
        return bool(conn.execute("SELECT 1 FROM requests WHERE phone=? AND hold_started_at IS NOT NULL LIMIT 1", (phone,)).fetchone())

def create_request(user, phone: str, method: str) -> int:
    now = datetime.now(timezone.utc).isoformat()
    with db_conn() as conn:
        cur = conn.execute("""
            INSERT INTO requests(
                user_id, username, first_name, last_name, phone, method, status,
                created_at, updated_at, queue_last_ping_at
            ) VALUES(?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?)
        """, (
            user.id,
            getattr(user, "username", None),
            getattr(user, "first_name", ""),
            getattr(user, "last_name", ""),
            phone, method, now, now, now
        ))
        return cur.lastrowid

def get_request(req_id: int):
    with db_conn() as conn:
        return conn.execute("SELECT * FROM requests WHERE id=?", (req_id,)).fetchone()

def update_request(req_id: int, **fields):
    if not fields:
        return
    fields["updated_at"] = datetime.now(timezone.utc).isoformat()
    cols = ", ".join([f"{k}=?" for k in fields.keys()])
    vals = list(fields.values()) + [req_id]
    with db_conn() as conn:
        conn.execute(f"UPDATE requests SET {cols} WHERE id=?", vals)

def queue_ids(method: str) -> list[int]:
    with db_conn() as conn:
        rows = conn.execute("SELECT * FROM requests WHERE method=? AND status IN ('queued','qr_skipped')", (method,)).fetchall()
    rep_enabled = get_setting("rep_influence_enabled", "1") == "1"
    now = datetime.now(timezone.utc)
    scored = []
    for r in rows:
        try:
            waited = max(0, int((now - datetime.fromisoformat(r["created_at"])).total_seconds() // 60))
        except Exception:
            waited = 0
        bonus = newbie_priority_bonus(r["user_id"])
        if rep_enabled:
            bonus += reputation_from_reports(r["user_id"])[3]
        score = waited + bonus
        scored.append((score, r["created_at"], int(r["id"])))
    scored.sort(key=lambda x: (-x[0], x[1], x[2]))
    return [rid for _, _, rid in scored]

def queue_position(req_id: int, method: str):
    ids = queue_ids(method)
    return ids.index(req_id) + 1 if req_id in ids else None

def render_queue_text(method: str) -> str:
    ids = queue_ids(method)
    if not ids:
        return f"{method} очередь пуста."
    lines = [f"{method} очередь:\n"]
    with db_conn() as conn:
        for pos, rid in enumerate(ids, 1):
            r = conn.execute("SELECT * FROM requests WHERE id=?", (rid,)).fetchone()
            dt = datetime.fromisoformat(r["created_at"]).astimezone(MSK).strftime("%H:%M")
            lines.append(f"{r['phone']} | @{r['username'] or r['user_id']} | {pos} | {dt}")
    return "\n".join(lines)

def add_report(req, status_text: str, amount: float):
    profit = get_profit_per_number() if amount > 0 else 0.0
    with db_conn() as conn:
        conn.execute("""
            INSERT INTO reports(req_id, number, type, tag, status, amount, profit, created_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            req["id"], req["phone"], req["method"],
            (req["username"] or str(req["user_id"])).lower(),
            status_text, amount, profit,
            datetime.now(MSK).strftime("%d.%m.%Y %H:%M:%S")
        ))
    stats = get_stats()
    if status_text == "Успешно":
        stats["paid_total"] += amount
        stats["profit_total"] += profit
        stats["success_count"] += 1
        if req["method"] == "QR":
            stats["qr_success"] += 1
        else:
            stats["code_success"] += 1
    elif status_text == "Слёт":
        stats["drop_count"] += 1
    elif status_text == "Мёртвый":
        stats["dead_count"] += 1
    elif status_text == "Ошибка":
        stats["error_count"] += 1
    save_stats(stats)

def parse_report_created_at(value: str):
    try:
        return datetime.strptime(value, "%d.%m.%Y %H:%M:%S").replace(tzinfo=MSK)
    except Exception:
        return None

def report_rows_filtered(report_type: str, mode: str = "all"):
    rows = report_rows(report_type)
    if mode == "all":
        return rows
    today = datetime.now(MSK).date()
    target = today if mode == "today" else (today - timedelta(days=1))
    out = []
    for r in rows:
        dt = parse_report_created_at(r["created_at"])
        if dt and dt.date() == target:
            out.append(r)
    return out

def build_report_text_filtered(report_type: str, mode: str = "all") -> str:
    rows = report_rows_filtered(report_type, mode)
    if not rows:
        return "Отчёт пуст."
    total_paid = sum(float(r["amount"]) for r in rows)
    total_profit = sum(float(r["profit"]) for r in rows)
    period_name = {"all": "За всё время", "today": "За сегодня", "yesterday": "За вчера"}.get(mode, mode)
    lines = [f"📄 <b>{period_name} | {report_type}</b>", ""]
    for r in rows:
        lines.append(f"{r['number']} | {r['type']} | @{r['tag']} | {r['status']} | {fmt_money(float(r['amount']))} | {r['created_at']}")
    lines.append("")
    lines.append(f"Тотал оплат: {fmt_money(total_paid)}")
    lines.append(f"Профит: {fmt_money(total_profit)}")
    return "\n".join(lines)

def stats_for_mode(mode: str = "all") -> dict:
    if mode == "all":
        return get_stats()
    rows = report_rows_filtered("all", mode)
    s = {"paid_total": 0.0, "profit_total": 0.0, "success_count": 0, "drop_count": 0, "qr_success": 0, "code_success": 0, "dead_count": 0, "error_count": 0}
    for r in rows:
        status = r["status"]
        amt = float(r["amount"])
        prof = float(r["profit"])
        if status == "Успешно":
            s["success_count"] += 1
            s["paid_total"] += amt
            s["profit_total"] += prof
            if r["type"] == "QR":
                s["qr_success"] += 1
            else:
                s["code_success"] += 1
        elif status == "Слёт":
            s["drop_count"] += 1
        elif status == "Мёртвый":
            s["dead_count"] += 1
        elif status == "Ошибка":
            s["error_count"] += 1
    return s

def report_rows_for_exact_date(report_type: str, date_str: str):
    rows = report_rows(report_type)
    out = []
    for r in rows:
        dt = parse_report_created_at(r["created_at"])
        if dt and dt.strftime("%d.%m.%Y") == date_str:
            out.append(r)
    return out

def build_report_text_for_exact_date(report_type: str, date_str: str) -> str:
    rows = report_rows_for_exact_date(report_type, date_str)
    if not rows:
        return f"Отчёт за {date_str} пуст."
    total_paid = sum(float(r["amount"]) for r in rows)
    total_profit = sum(float(r["profit"]) for r in rows)
    lines = [f"📄 <b>Отчёт за {date_str} | {report_type}</b>", ""]
    for r in rows:
        lines.append(f"{r['number']} | {r['type']} | @{r['tag']} | {r['status']} | {fmt_money(float(r['amount']))} | {r['created_at']}")
    lines.append("")
    lines.append(f"Тотал оплат: {fmt_money(total_paid)}")
    lines.append(f"Профит: {fmt_money(total_profit)}")
    return "\n".join(lines)

def report_rows(report_type: str):
    with db_conn() as conn:
        if report_type == "all":
            return conn.execute("SELECT * FROM reports ORDER BY id DESC").fetchall()
        return conn.execute("SELECT * FROM reports WHERE type=? ORDER BY id DESC", (report_type,)).fetchall()

def build_report_text(report_type: str) -> str:
    rows = report_rows(report_type)
    if not rows:
        return "Отчёт пуст."
    total_paid = sum(float(r["amount"]) for r in rows)
    total_profit = sum(float(r["profit"]) for r in rows)
    lines = []
    for r in rows:
        lines.append(
            f"{r['number']} | {r['type']} | @{r['tag']} | {r['status']} | "
            f"{fmt_money(float(r['amount']))} | {r['created_at']}"
        )
    lines.append("")
    lines.append(f"Тотал оплат: {fmt_money(total_paid)}")
    lines.append(f"Профит: {fmt_money(total_profit)}")
    return "\n".join(lines)

async def send_log(text: str):
    try:
        await bot.send_message(LOG_CHANNEL_ID, text)
    except Exception:
        pass

async def require_subscription(message: Message) -> bool:
    try:
        member = await bot.get_chat_member(REQUIRED_CHANNEL_ID, message.from_user.id)
        if member.status in {ChatMemberStatus.LEFT, ChatMemberStatus.KICKED}:
            raise RuntimeError()
        return True
    except Exception:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 Подписаться", url=f"https://t.me/{REQUIRED_CHANNEL_USERNAME}")],
            [InlineKeyboardButton(text="⭕️ Проверить", callback_data="check_sub")]
        ])
        await message.answer(
            "📢 <b>Для работы с ботом нужна обязательная подписка на канал.</b>\n\n"
            "Подпишись и потом снова нажми /start",
            reply_markup=kb,
        )
        return False

def can_work_here(message: Message) -> bool:
    if message.chat.type not in {ChatType.GROUP, ChatType.SUPERGROUP}:
        return False
    return is_group_enabled(message.chat.id) or is_topic_enabled(message.chat.id, getattr(message, "message_thread_id", None))

async def group_send(req, text: str, reply_markup=None):
    kwargs = {"chat_id": req["issue_group_id"], "text": text, "reply_markup": reply_markup}
    if req["issue_thread_id"] is not None:
        kwargs["message_thread_id"] = req["issue_thread_id"]
    return await bot.send_message(**kwargs)

def operator_owns(user_id: int, req) -> bool:
    return bool(req and req["claimed_by"] == user_id)

# =========================
# CRYPTO PAY
# =========================
def crypto_base() -> str:
    return "https://testnet-pay.crypt.bot/api/" if CRYPTO_PAY_USE_TESTNET else "https://pay.crypt.bot/api/"

async def crypto_api(method: str, payload=None):
    if not CRYPTO_PAY_TOKEN or CRYPTO_PAY_TOKEN.startswith("PASTE_"):
        raise RuntimeError("CRYPTO_PAY_TOKEN not configured")
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.post(crypto_base() + method, json=payload or {}) as resp:
            data = await resp.json(content_type=None)
            if not data.get("ok"):
                raise RuntimeError(data.get("error", "Crypto Pay API error"))
            return data["result"]

async def crypto_create_invoice(amount: float):
    return await crypto_api("createInvoice", {
        "asset": TREASURY_ASSET,
        "amount": f"{amount:.2f}",
        "description": "DIAMOND VAULT TREASURY TOPUP",
    })

async def crypto_create_check(amount: float, user_id: int):
    return await crypto_api("createCheck", {
        "asset": TREASURY_ASSET,
        "amount": f"{amount:.2f}",
        "pin_to_user_id": user_id,
    })

async def crypto_delete_checks():
    checks = await crypto_api("getChecks", {})
    deleted = 0
    for ch in checks:
        if ch.get("status") == "active":
            try:
                await crypto_api("deleteCheck", {"check_id": ch["check_id"]})
                deleted += 1
            except Exception:
                pass
    return deleted

async def crypto_balance_text():
    balances = await crypto_api("getBalance", {})
    if not balances:
        return "Пусто"
    return "\n".join(f"{b['currency_code']}: {b['available']} (hold {b.get('onhold','0')})" for b in balances)

# =========================
# TEXTS
# =========================
def home_text(user_id: int) -> str:
    stats = get_stats()
    _, title_name, price = get_user_title(user_id)
    _, rep_name, rep_pct, rep_bonus = reputation_from_reports(user_id)
    submit_line = "🟢 Сдача номеров доступна" if submit_enabled() else "🔴 Сдача номеров временно недоступна"
    return (
        "💠 <b>DIAMOND VAULT MAX</b>\n\n"
        "Добро пожаловать в сервис приёма номеров MAX\n"
        "У нас есть: статус, надёжность, выплаты без лишнего шума\n\n"
        f"{submit_line}\n\n"
        "━━━━━━━━━━━━━━\n"
        f"🆔 ID: <code>{user_id}</code>\n"
        f"💰 Баланс: <b>{fmt_money(get_balance(user_id))}</b>\n"
        f"💎 Ваш прайс: <b>{fmt_money(price)}</b>\n"
        f"🏆 Титул: <b>{title_name}</b>\n"
        f"📊 Репутация: <b>{rep_name}</b> (<b>{rep_pct}%</b>)\n"
        "━━━━━━━━━━━━━━\n"
        "📤 Очереди:\n"
        f"🔹Очередь QR: <b>{len(queue_ids('QR'))}</b>\n"
        f"🔹Очередь Код: <b>{len(queue_ids('КОД'))}</b>\n\n"
        "💸Оплаты в боте:\n"
        f"Всего выплачено: <b>{fmt_money(stats['paid_total'])}</b>\n"
        f"Оплат за сегодня: <b>{fmt_money(total_paid_today())}</b>\n\n"
        f"{title_progress_text(user_id)}\n\n"
        "Вы находитесь в главном меню\n"
        "👇 Выберите нужное действие ниже:"
    )



def submit_text() -> str:
    return (
        "🧨 <b>СДАЧА НОМЕРА | DIAMOND VAULT</b>\n\n"
        "Выберите способ передачи номера MAX:\n\n"
        "━━━━━━━━━━━━━━\n"
        "🟦 QR — вход через сканирование\n"
        "🟩 Код — вход через SMS-код\n"
        "━━━━━━━━━━━━━━\n\n"
        "💎 <b>Актуальные выплаты:</b>\n"
        f"• QR — {fmt_money(PRICE_QR)}\n"
        f"• Код — {fmt_money(PRICE_CODE)}\n\n"
        "📌 Выберите подходящий вариант ниже ⬇️"
    )

def referral_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="m_home")]
    ])

def rank_text(user_id: int) -> str:
    _, title_name, price = get_user_title(user_id)
    _, rep_name, rep_pct, rep_bonus = reputation_from_reports(user_id)
    newbie = newbie_priority_bonus(user_id)
    return (
        "🏆 <b>ТИТУЛ И РЕПУТАЦИЯ</b>\n\n"
        f"Титул: <b>{title_name}</b>\n"
        f"Прайс: <b>{fmt_money(price)}</b>\n\n"
        f"Репутация: <b>{rep_name}</b> (<b>{rep_pct}%</b>)\n"
        f"Бонус очереди: <b>{rep_bonus}</b>\n"
        f"Бонус новичка: <b>{newbie}</b>\n\n"
        f"{title_progress_text(user_id)}\n\n"
        f"Успешных сдач: <b>{success_count_user(user_id)}</b>"
    )

def profile_menu_kb(admin_view: bool = False, target_user_id: int | None = None):
    if not admin_view:
        return back_kb()
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📞 Посмотреть номера", callback_data=f"aprof_nums:{target_user_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="m_admin")]
    ])

def profile_numbers_kb(target_user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📂 Открыть номера", callback_data=f"aprof_open:{target_user_id}"),
         InlineKeyboardButton(text="📄 Выгрузить TXT", callback_data=f"aprof_txt:{target_user_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"aprof_back:{target_user_id}")]
    ])

def profile_text(user_id: int, admin_view: bool = False) -> str:
    row = get_user(user_id)
    username_raw = row["username"] if row and row["username"] else "—"
    username = f"@{username_raw}" if username_raw != "—" else "—"
    reg = "—"
    if row and row["registered_at"]:
        try:
            reg = datetime.fromisoformat(row["registered_at"]).astimezone(MSK).strftime("%d.%m.%Y %H:%M")
        except Exception:
            reg = row["registered_at"]
    _, title_name, price = get_user_title(user_id)
    _, rep_name, rep_pct, rep_bonus = reputation_from_reports(user_id)
    with db_conn() as conn:
        total = int(conn.execute("SELECT COUNT(*) c FROM requests WHERE user_id=?", (user_id,)).fetchone()["c"])
        paid_count = int(conn.execute("SELECT COUNT(*) c FROM requests WHERE user_id=? AND credited=1", (user_id,)).fetchone()["c"])
        paid_sum = float(conn.execute("SELECT COALESCE(SUM(amount),0) s FROM reports WHERE tag=?", ((username_raw or str(user_id)).lower(),)).fetchone()["s"])
        drop_count = int(conn.execute("SELECT COUNT(*) c FROM reports WHERE tag=? AND status='Слёт'", ((username_raw or str(user_id)).lower(),)).fetchone()["c"])
        err_count = int(conn.execute("SELECT COUNT(*) c FROM reports WHERE tag=? AND status='Ошибка'", ((username_raw or str(user_id)).lower(),)).fetchone()["c"])
        dead_count = int(conn.execute("SELECT COUNT(*) c FROM reports WHERE tag=? AND status='Мёртвый'", ((username_raw or str(user_id)).lower(),)).fetchone()["c"])
    return (
        f"👤 <b>Пользователь {html.escape(username)}</b>\n\n"
        f"Дата регистрации: <b>{reg}</b>\n\n"
        "РЕПУТАЦИЯ:\n"
        f"Титул: <b>{title_name}</b>\n"
        f"Репутация: <b>{rep_name}</b> (<b>{rep_pct}%</b>)\n\n"
        f"ID: <code>{user_id}</code>\n"
        f"Username: <b>{html.escape(username)}</b>\n"
        f"Баланс: <b>{fmt_money(get_balance(user_id))}</b>\n"
        f"На выводе: <b>{fmt_money(get_pending(user_id))}</b>\n"
        f"Оплачено: <b>{fmt_money(paid_sum)}</b>\n"
        f"Успешных номеров: <b>{success_count_user(user_id)}</b>\n"
        f"Слёт: <b>{drop_count}</b>\n"
        f"Ошибок: <b>{err_count}</b>\n"
        f"Мёртвых номеров: <b>{dead_count}</b>\n"
        f"Всего номеров: <b>{total}</b>\n"
        f"Оплачено номеров: <b>{paid_count}</b>"
    )

def faq_text() -> str:
    return (
        "🔰 <b>FAQ | DIAMOND VAULT</b>\n\n"
        "Как начать работу:\n\n"
        "━━━━━━━━━━━━━━\n"
        "1. Нажмите «Сдать номер ☎️»\n"
        "2. Выберите тип: QR или Код\n"
        "3. Отправьте номер\n"
        "4. Подтвердите активность\n"
        "5. Следуйте инструкциям бота\n"
        "━━━━━━━━━━━━━━\n\n"
        "🏆 <b>Система титулов и репутации:</b>\n"
        "• титул влияет на ваш прайс\n"
        "• репутация влияет на позицию в очереди\n"
        "• успешные сдачи повышают титул\n"
        "• ошибки и мёртвые номера снижают качество репутации\n\n"
        "⚠️ <b>Важно:</b> соблюдайте указания на каждом этапе — это гарантирует успешную сдачу номера и выплату"
    )

def numbers_text(user_id: int, mode: str = "today") -> str:
    rows = requests_by_user_for_day(user_id, mode)
    with db_conn() as conn:
        total = conn.execute("SELECT COUNT(*) c FROM requests WHERE user_id=?", (user_id,)).fetchone()["c"]
    title_map = {"today": "за сегодня", "yesterday": "за вчера", "all": "за всё время"}
    if not rows:
        return (
            "📞 <b>МОИ НОМЕРА | DIAMOND VAULT</b>\n\n"
            f"Список ваших заявок {title_map.get(mode, '')} пуст.\n\n"
            "━━━━━━━━━━━━━━\n"
            f"📊 Всего заявок: <b>{total}</b>\n"
            "━━━━━━━━━━━━━━"
        )
    lines = ["📞 <b>МОИ НОМЕРА | DIAMOND VAULT</b>\n", f"Список ваших заявок {title_map.get(mode, '')}:\n", "━━━━━━━━━━━━━━"]
    for row in rows[:15]:
        pos_text = ""
        if row["status"] in ("queued", "qr_skipped", "awaiting_activity"):
            pos = queue_position(row["id"], row["method"])
            if pos:
                pos_text = f" | 📍 Очередь: <b>{pos}</b>"
        lines.append(
            f"🧾 #{row['id']} | 📱 <code>{row['phone']}</code>\n"
            f"📌 {row['method']} | 📍 {request_public_status(row['status'])}{pos_text}\n"
        )
    lines.append("━━━━━━━━━━━━━━")
    lines.append(f"📊 Всего заявок: <b>{total}</b>")
    return "\n".join(lines)

def my_numbers_filter_kb(mode: str = "today"):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=("• Сегодня •" if mode=="today" else "Сегодня"), callback_data="m_numbers:today"),
            InlineKeyboardButton(text=("• Вчера •" if mode=="yesterday" else "Вчера"), callback_data="m_numbers:yesterday"),
            InlineKeyboardButton(text=("• Всё •" if mode=="all" else "Всё"), callback_data="m_numbers:all"),
        ],
        [InlineKeyboardButton(text="🗑 Убрать номер из очереди", callback_data="m_remove_queue")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="m_home")]
    ])

# =========================
# KEYBOARDS
# =========================
# KEYBOARDS
# =========================
def start_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="☎️ Сдать номер", callback_data="m_submit"),
         InlineKeyboardButton(text="👤 Профиль", callback_data="m_profile")],
        [InlineKeyboardButton(text="🏆 Титул и репутация", callback_data="m_rank"),
         InlineKeyboardButton(text="📞 Мои номера", callback_data="m_numbers")],
        [InlineKeyboardButton(text="🔰 FAQ", callback_data="m_faq"),
         InlineKeyboardButton(text="💸 Вывод средств", callback_data="m_withdraw")],
        [InlineKeyboardButton(text="🎁 Реф. система", callback_data="m_ref")],
    ])

def back_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="m_home")]])

def admin_back_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="m_admin")]])

def submit_choice_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟦 QR", callback_data="submit_qr"),
         InlineKeyboardButton(text="🟩 Код", callback_data="submit_code")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="m_home")]
    ])

def admin_main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👮 Админы", callback_data="a_admins"),
         InlineKeyboardButton(text="🏢 Группы", callback_data="a_groups")],
        [InlineKeyboardButton(text="📦 Очереди", callback_data="a_queues"),
         InlineKeyboardButton(text="👥 Участники", callback_data="a_users")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="a_stats"),
         InlineKeyboardButton(text="💰 Профит", callback_data="a_profit")],
        [InlineKeyboardButton(text="🏦 Казна", callback_data="a_treasury"),
         InlineKeyboardButton(text="🟢 Вкл/Выкл", callback_data="a_toggle")],
        [InlineKeyboardButton(text="🎖 Прайс титулов", callback_data="a_title_prices"),
         InlineKeyboardButton(text="📈 Репутация/очередь", callback_data="a_rep_settings")],
        [InlineKeyboardButton(text="📣 Рассылка", callback_data="a_broadcast"),
         InlineKeyboardButton(text="🎁 Реферал", callback_data="a_referral")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="m_home")]
    ])

def admin_admins_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить", callback_data="aa_add"),
         InlineKeyboardButton(text="➖ Удалить", callback_data="aa_del")],
        [InlineKeyboardButton(text="📋 Список", callback_data="aa_list"),
         InlineKeyboardButton(text="🛡 Роли", callback_data="a_roles")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="m_admin")]
    ])

def admin_groups_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить группу", callback_data="ag_add"),
         InlineKeyboardButton(text="➖ Удалить группу", callback_data="ag_del")],
        [InlineKeyboardButton(text="📋 Список групп", callback_data="ag_list"),
         InlineKeyboardButton(text="📋 Список топиков", callback_data="ag_topics")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="m_admin")]
    ])

def admin_queues_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟦 QR очередь", callback_data="aq_qr"),
         InlineKeyboardButton(text="🟩 Код очередь", callback_data="aq_code")],
        [InlineKeyboardButton(text="🗑 Удалить номер", callback_data="aq_delete"),
         InlineKeyboardButton(text="🧹 Очистить", callback_data="aq_clear")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="m_admin")]
    ])

def admin_users_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⛔ Забанить", callback_data="au_ban"),
         InlineKeyboardButton(text="✅ Разбанить", callback_data="au_unban")],
        [InlineKeyboardButton(text="📵 Убрать номера", callback_data="au_remove"),
         InlineKeyboardButton(text="➕ Начислить", callback_data="au_addbal")],
        [InlineKeyboardButton(text="➖ Снять", callback_data="au_subbal"),
         InlineKeyboardButton(text="👤 Найти", callback_data="au_find")],
        [InlineKeyboardButton(text="💬 Написать в ЛС", callback_data="au_dm"),
         InlineKeyboardButton(text="🔓 Убрать блок номера", callback_data="au_clearblock")],
        [InlineKeyboardButton(text="🧹 Снять блок за мёртвые", callback_data="au_cleardead")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="m_admin")]
    ])

def admin_stats_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 Отчет QR", callback_data="rs:QR"),
         InlineKeyboardButton(text="📄 Отчет Код", callback_data="rs:КОД")],
        [InlineKeyboardButton(text="📄 Полный отчет", callback_data="rs:all"),
         InlineKeyboardButton(text="📊 Стата: всё", callback_data="rs_total:all")],
        [InlineKeyboardButton(text="📅 Стата: сегодня", callback_data="rs_total:today"),
         InlineKeyboardButton(text="📆 Стата: вчера", callback_data="rs_total:yesterday")],
        [InlineKeyboardButton(text="🗓 Отчет по дате", callback_data="rs_custom_date")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="m_admin")]
    ])

def report_period_kb(report_type: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📁 Всё время", callback_data=f"report_show:{report_type}:all"),
         InlineKeyboardButton(text="📅 Сегодня", callback_data=f"report_show:{report_type}:today")],
        [InlineKeyboardButton(text="📆 Вчера", callback_data=f"report_show:{report_type}:yesterday"),
         InlineKeyboardButton(text="📄 TXT", callback_data=f"report_txt:{report_type}:all")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="a_stats")]
    ])

def admin_treasury_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Пополнить казну", callback_data="tr_top"),
         InlineKeyboardButton(text="💼 Баланс казны", callback_data="tr_bal")],
        [InlineKeyboardButton(text="🧹 Удалить чеки", callback_data="tr_del")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="m_admin")]
    ])

def admin_toggle_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Включить", callback_data="bt_on")],
        [InlineKeyboardButton(text="🛠 Тех.перерыв", callback_data="bt_tech")],
        [InlineKeyboardButton(text="⛔ Стоп работа", callback_data="bt_stop")],
        [InlineKeyboardButton(text="🍽 Обед", callback_data="bt_lunch")],
        [InlineKeyboardButton(text="⏳ Временное отключение", callback_data="bt_temp")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="m_admin")]
    ])

def report_output_kb(report_type: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👁 Показать", callback_data=f"report_show:{report_type}"),
         InlineKeyboardButton(text="📄 TXT", callback_data=f"report_txt:{report_type}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="a_stats")]
    ])

def confirm_activity_kb(req_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Я в сети", callback_data=f"alive:{req_id}")]])

def operator_code_stage_kb(req_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Я отправил код", callback_data=f"sendcode:{req_id}")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data=f"cancelop:{req_id}")]
    ])

def operator_repeat_confirm_kb(req_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить повтор", callback_data=f"confirmrepeat:{req_id}")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data=f"cancelop:{req_id}")]
    ])

def operator_qr_stage_kb(req_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📷 Выдать QR", callback_data=f"sendqr:{req_id}")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data=f"cancelop:{req_id}")]
    ])

def operator_result_kb(req_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Встал", callback_data=f"ok:{req_id}"),
         InlineKeyboardButton(text="❌ Ошибка", callback_data=f"err:{req_id}")],
        [InlineKeyboardButton(text="🔁 Повтор", callback_data=f"oprepeat_code:{req_id}"),
         InlineKeyboardButton(text="🔐 Пароль", callback_data=f"askpass:{req_id}")]
    ])

def operator_result_qr_kb(req_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Встал", callback_data=f"ok:{req_id}"),
         InlineKeyboardButton(text="❌ Ошибка", callback_data=f"err:{req_id}")],
        [InlineKeyboardButton(text="🔁 Повтор", callback_data=f"oprepeat_qr:{req_id}"),
         InlineKeyboardButton(text="🔐 Пароль", callback_data=f"askpass:{req_id}")]
    ])

def repeat_request_kb(req_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔁 Запросить повтор", callback_data=f"user_repeat:{req_id}")]
    ])

def qr_user_result_kb(req_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Сканировал", callback_data=f"sc:{req_id}"),
         InlineKeyboardButton(text="⏭ Скип", callback_data=f"skip:{req_id}")],
        [InlineKeyboardButton(text="🔁 Запросить повтор", callback_data=f"user_qr_repeat:{req_id}")]
    ])

def operator_repeat_confirm_qr_kb(req_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить повтор QR", callback_data=f"confirmrepeatqr:{req_id}")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data=f"cancelop:{req_id}")]
    ])

def hold_kb(req_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💥 Слёт", callback_data=f"drop:{req_id}")]
    ])

def withdraw_admin_kb(withdraw_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Принять", callback_data=f"wok:{withdraw_id}"),
         InlineKeyboardButton(text="❌ Отклонить", callback_data=f"wno:{withdraw_id}")]
    ])

def queue_keep_kb(req_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Номер актуален", callback_data=f"keep:{req_id}")]
    ])

# =========================
# TIMERS
# =========================
async def activity_timer(req_id: int):
    remaining = ACTIVITY_SECONDS
    while remaining >= 0:
        req = get_request(req_id)
        if not req or req["status"] != "awaiting_activity":
            return
        try:
            await bot.edit_message_text(
                chat_id=req["user_id"],
                message_id=req["user_activity_message_id"],
                text=(
                    "⚡️ <b>Подтверди активность</b>\n\n"
                    f"Осталось: <b>{remaining} сек</b>\n"
                    "Нажми «Я в сети», иначе заявка закроется."
                ),
                reply_markup=confirm_activity_kb(req_id),
            )
        except Exception:
            pass
        if remaining == 0:
            update_request(req_id, status="closed_dead")
            add_report(get_request(req_id), "Мёртвый", 0)
            increment_dead_streak(req["user_id"])
            try:
                await bot.send_message(req["user_id"], f"⛔️ <b>Мёртвая заявка</b>\n\nНомер: <code>{req['phone']}</code>\nПричина: активность не подтверждена.", reply_markup=admin_back_kb())
            except Exception:
                pass
            try:
                await group_send(req, f"⛔️ <b>Заявка стала мёртвой</b>\n\nЗаявка: <b>#{req_id}</b>\nНомер: <code>{req['phone']}</code>")
            except Exception:
                pass
            return
        await asyncio.sleep(15)
        remaining -= 15

async def code_timer(req_id: int):
    remaining = CODE_SECONDS
    while remaining >= 0:
        req = get_request(req_id)
        if not req or req["status"] != "awaiting_sms":
            return
        try:
            await bot.edit_message_text(
                chat_id=req["user_id"],
                message_id=req["user_code_message_id"],
                text=(
                    "🔑 <b>Нужен код</b>\n\n"
                    "Пришли 6-значный код одним сообщением.\n"
                    f"У вас есть <b>{remaining} секунд</b> на отправку кода.\n\n"
                    "Если передумал — вернись назад."
                ),
                reply_markup=repeat_request_kb(req_id),
            )
        except Exception:
            pass
        if remaining == 0:
            update_request(req_id, status="closed_dead")
            add_report(get_request(req_id), "Мёртвый", 0)
            increment_dead_streak(req["user_id"])
            try:
                await bot.send_message(req["user_id"], f"⛔️ <b>Мёртвая заявка</b>\n\nНомер: <code>{req['phone']}</code>\nПричина: время ожидания кода вышло.", reply_markup=admin_back_kb())
            except Exception:
                pass
            return
        await asyncio.sleep(15)
        remaining -= 15

async def password_timer(req_id: int):
    await asyncio.sleep(CODE_SECONDS)
    req = get_request(req_id)
    if req and req["password_requested"] == 1 and req["status"] in ("qr_scanned", "code_received"):
        update_request(req_id, password_requested=0, status="closed_dead")
        add_report(req, "Мёртвый", 0)
        increment_dead_streak(req["user_id"])
        try:
            await bot.send_message(req["user_id"], f"⛔️ <b>Мёртвая заявка</b>\n\nНомер: <code>{req['phone']}</code>\nПричина: пароль не был отправлен вовремя.", reply_markup=back_kb())
        except Exception:
            pass

async def qr_timer(req_id: int):
    await asyncio.sleep(CODE_SECONDS)
    req = get_request(req_id)
    if req and req["status"] == "awaiting_qr":
        update_request(req_id, status="closed_dead")
        add_report(req, "Мёртвый", 0)
        increment_dead_streak(req["user_id"])
        try:
            await bot.send_message(req["user_id"], f"⛔️ <b>Мёртвая заявка</b>\n\nНомер: <code>{req['phone']}</code>\nПричина: QR не был отправлен вовремя.", reply_markup=back_kb())
        except Exception:
            pass

async def hold_timer(req_id: int):
    req = get_request(req_id)
    if not req:
        return
    if not req["hold_started_at"]:
        update_request(req_id, hold_started_at=datetime.now(timezone.utc).isoformat())
    while True:
        req = get_request(req_id)
        if not req or req["status"] != "success_hold":
            return
        started = datetime.fromisoformat(req["hold_started_at"])
        elapsed = int((datetime.now(timezone.utc) - started).total_seconds())
        remaining = HOLD_SECONDS - elapsed
        hold_text = (
            "⏳ <b>Холд активен</b>\n\n"
            f"• Номер: <code>{req['phone']}</code>\n"
            f"• Поставлен (МСК): <b>{datetime.now(MSK).strftime('%H:%M:%S')}</b>\n\n"
            f"{progress_bar(elapsed, HOLD_SECONDS)}\n"
            f"Осталось: <b>{seconds_to_hhmmss(remaining)}</b>\n\n"
            "Начисление будет автоматически после окончания холда ✅"
        )
        try:
            if req["hold_user_message_id"]:
                await bot.edit_message_text(
                    chat_id=req["user_id"],
                    message_id=req["hold_user_message_id"],
                    text=hold_text,
                )
        except Exception:
            pass
        try:
            if req["hold_group_message_id"] and req["issue_group_id"]:
                kwargs = {"chat_id": req["issue_group_id"], "message_id": req["hold_group_message_id"], "text": hold_text, "reply_markup": hold_kb(req_id)}
                await bot.edit_message_text(**kwargs)
        except Exception:
            pass
        if remaining <= 0:
            if not req["credited"]:
                add_balance(req["user_id"], request_price(req["method"], req["user_id"]))
            update_request(req_id, status="closed_paid", credited=1)
            req = get_request(req_id)
            add_report(req, "Успешно", request_price(req["method"], req["user_id"]))
            reset_dead_streak(req["user_id"])
            reward_referrer_if_needed(req["user_id"])
            try:
                await bot.send_message(
                    req["user_id"],
                    hold_closed_text(req["phone"], request_price(req["method"], req["user_id"]), get_balance(req["user_id"])),
                    reply_markup=back_kb(),
                )
            except Exception:
                pass
            return
        await asyncio.sleep(30)

async def queue_afk_timer(req_id: int):
    while True:
        req = get_request(req_id)
        if not req or req["status"] not in ("queued", "qr_skipped"):
            return
        last_ping = datetime.fromisoformat(req["queue_last_ping_at"])
        if (datetime.now(timezone.utc) - last_ping).total_seconds() < QUEUE_AFK_CHECK_EVERY:
            await asyncio.sleep(30)
            continue

        if req["queue_ping_deadline_at"]:
            deadline = datetime.fromisoformat(req["queue_ping_deadline_at"])
            if datetime.now(timezone.utc) >= deadline:
                update_request(req_id, status="closed_cancelled")
                try:
                    await bot.send_message(req["user_id"], "⛔️ Номер удалён из очереди: проверка активности не пройдена.", reply_markup=back_kb())
                except Exception:
                    pass
                return

        if req["queue_bumps"] >= QUEUE_AFK_MAX_BUMPS:
            update_request(req_id, status="closed_cancelled")
            try:
                await bot.send_message(req["user_id"], "⛔️ Номер удалён из очереди: лимит поднятий исчерпан.", reply_markup=back_kb())
            except Exception:
                pass
            return

        try:
            await bot.send_message(
                req["user_id"],
                "⚠️ <b>Проверка активности номера</b>\n\n"
                "Номер долго стоит в очереди.\n"
                "Подтверди, что он ещё актуален.\n\n"
                f"У тебя <b>{QUEUE_AFK_CONFIRM_SECONDS} сек</b>.",
                reply_markup=queue_keep_kb(req_id),
            )
            update_request(req_id, queue_ping_deadline_at=(datetime.now(timezone.utc) + timedelta(seconds=QUEUE_AFK_CONFIRM_SECONDS)).isoformat())
        except Exception:
            update_request(req_id, status="closed_cancelled")
            return

        await asyncio.sleep(QUEUE_AFK_CONFIRM_SECONDS + 1)

# =========================
# START / MENUS
# =========================
@dp.callback_query(F.data == "check_sub")
async def check_sub(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        member = await bot.get_chat_member(REQUIRED_CHANNEL_ID, callback.from_user.id)
        if member.status in {ChatMemberStatus.LEFT, ChatMemberStatus.KICKED}:
            raise RuntimeError()
        ensure_user(callback.from_user)
        if not bot_enabled():
            await callback.message.edit_text(f"⛔️ <b>Бот сейчас недоступен</b>\n\n{bot_mode_text()}", reply_markup=back_kb())
        else:
            await callback.message.edit_text(home_text(callback.from_user.id), reply_markup=start_menu_kb())
    except Exception:
        await callback.answer("Подписка не найдена", show_alert=True)
        return
    await callback.answer()

@dp.message(Command("admin"))
async def admin_open(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer("🛠 <b>ADMIN PANEL | DIAMOND VAULT</b>\n\nВыберите раздел управления:", reply_markup=admin_main_kb())

@dp.message(Command("getdb"))
async def getdb(message: Message):
    if not is_admin(message.from_user.id):
        return
    try:
        data = Path(DB_PATH).read_bytes()
        await message.answer_document(
            BufferedInputFile(data, filename="diamond_vault.sqlite3"),
            caption="📤 Текущая база данных"
        )
    except Exception as e:
        await message.answer(f"❌ Не удалось выгрузить базу.\n<code>{html.escape(str(e))}</code>")

@dp.message(Command("getlog"))
async def getlog(message: Message):
    if not is_admin(message.from_user.id):
        return
    try:
        data = Path(LOG_FILE_PATH).read_bytes()
        await message.answer_document(
            BufferedInputFile(data, filename="diamond_vault.log"),
            caption="📤 Текущий лог-файл"
        )
    except Exception as e:
        await message.answer(f"❌ Не удалось выгрузить лог.\n<code>{html.escape(str(e))}</code>")

@dp.message(Command("uploaddb"))
async def uploaddb(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(AdminStates.upload_db)
    await message.answer("📥 Пришли файл базы <code>.sqlite3</code> одним документом.")

@dp.message(Command("uploadlog"))
async def uploadlog(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(AdminStates.upload_log)
    await message.answer("📥 Пришли файл логов <code>.log</code> одним документом.")

@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    await state.clear()
    maybe_resume_from_lunch()
    if not await require_subscription(message):
        return
    ensure_user(message.from_user)
    try:
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) > 1 and parts[1].startswith("ref_"):
            ref_id = int(parts[1].split("_", 1)[1])
            set_referrer_if_missing(message.from_user.id, ref_id)
    except Exception:
        pass
    if not bot_enabled():
        await message.answer(f"⛔️ <b>Бот сейчас недоступен</b>\n\n{bot_mode_text()}")
        return
    await message.answer(home_text(message.from_user.id), reply_markup=start_menu_kb())

@dp.callback_query(F.data == "m_home")
async def m_home(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    if not bot_enabled():
        await callback.message.edit_text(f"⛔️ <b>Бот сейчас недоступен</b>\n\n{bot_mode_text()}", reply_markup=back_kb())
    else:
        await callback.message.edit_text(home_text(callback.from_user.id), reply_markup=start_menu_kb())
    await callback.answer()

@dp.message(Command("profile"))
async def profile_lookup_cmd(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Используй: /profile @юзернейм / id / номер")
        return
    row = user_by_any(parts[1])
    if not row:
        await message.answer("Пользователь не найден.")
        return
    await message.answer(profile_text(int(row["user_id"]), admin_view=True), reply_markup=profile_menu_kb(True, int(row["user_id"])))

@dp.callback_query(F.data == "m_rank")
async def m_rank(callback: CallbackQuery):
    await callback.message.edit_text(rank_text(callback.from_user.id), reply_markup=back_kb())
    await callback.answer()

@dp.callback_query(F.data == "m_profile")
async def m_profile(callback: CallbackQuery):
    await callback.message.edit_text(profile_text(callback.from_user.id), reply_markup=profile_menu_kb(False))
    await callback.answer()

@dp.callback_query(F.data == "m_faq")
async def m_faq(callback: CallbackQuery):
    await callback.message.edit_text(faq_text(), reply_markup=back_kb())
    await callback.answer()

@dp.callback_query(F.data == "m_numbers")
async def m_numbers(callback: CallbackQuery):
    await callback.message.edit_text(numbers_text(callback.from_user.id, "today"), reply_markup=my_numbers_filter_kb("today"))
    await callback.answer()

@dp.callback_query(F.data.startswith("m_numbers:"))
async def m_numbers_filtered(callback: CallbackQuery):
    mode = callback.data.split(":", 1)[1]
    await callback.message.edit_text(numbers_text(callback.from_user.id, mode), reply_markup=my_numbers_filter_kb(mode))
    await callback.answer()

@dp.callback_query(F.data == "m_remove_queue")
async def m_remove_queue(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.remove_my_queue)
    await callback.message.edit_text("Пришли номер заявки, который нужно убрать из очереди.", reply_markup=back_kb())
    await callback.answer()

@dp.callback_query(F.data == "m_withdraw")
async def m_withdraw(callback: CallbackQuery, state: FSMContext):
    await state.set_state(WithdrawStates.waiting_amount)
    await callback.message.edit_text(withdraw_text(callback.from_user.id), reply_markup=back_kb())
    await callback.answer()

@dp.callback_query(F.data == "m_submit")
async def m_submit(callback: CallbackQuery):
    if not submit_enabled():
        await callback.answer("Сдача номеров временно недоступна", show_alert=True)
        return
    await callback.message.edit_text(submit_text(), reply_markup=submit_choice_kb())
    await callback.answer()

@dp.callback_query(F.data == "m_ref")
async def m_ref(callback: CallbackQuery):
    me = await bot.get_me()
    link = f"https://t.me/{me.username}?start=ref_{callback.from_user.id}" if me.username else "Недоступно"
    reward = get_ref_reward_amount()
    with db_conn() as conn:
        refs = conn.execute("SELECT COUNT(*) c FROM referrals WHERE referrer_user_id=?", (callback.from_user.id,)).fetchone()["c"]
        earned = conn.execute("SELECT COALESCE(SUM(reward_amount),0) s FROM referrals WHERE referrer_user_id=? AND rewarded=1", (callback.from_user.id,)).fetchone()["s"]
    text = (
        "🎁 <b>РЕФЕРАЛЬНАЯ СИСТЕМА | DIAMOND VAULT</b>\n\n"
        "Приглашай друзей и получай единоразовую награду за каждого реферала, который сдаст хотя бы 1 номер успешно.\n\n"
        "━━━━━━━━━━━━━━\n"
        f"🔗 Твоя ссылка: <code>{link}</code>\n"
        f"💎 Награда за реферала: <b>{fmt_money(reward)}</b>\n"
        f"👥 Всего приглашено: <b>{refs}</b>\n"
        f"💰 Заработано: <b>{fmt_money(float(earned or 0))}</b>\n"
        "━━━━━━━━━━━━━━\n\n"
        "✅ Условия:\n"
        "• награда начисляется 1 раз за 1 человека\n"
        "• начисление идёт после первого успешного номера реферала\n"
        "• сам себе рефералом быть нельзя"
    )
    await callback.message.edit_text(text, reply_markup=referral_menu_kb())
    await callback.answer()

@dp.callback_query(F.data.startswith("aprof_nums:"))
async def aprof_nums(callback: CallbackQuery, state: FSMContext):
    uid = int(callback.data.split(":")[1])
    await state.update_data(aprof_uid=uid)
    await callback.message.edit_text("📞 <b>Номера пользователя</b>\n\nВыбери действие ниже.", reply_markup=profile_numbers_kb(uid))
    await callback.answer()

@dp.callback_query(F.data.startswith("aprof_back:"))
async def aprof_back(callback: CallbackQuery):
    uid = int(callback.data.split(":")[1])
    await callback.message.edit_text(profile_text(uid, admin_view=True), reply_markup=profile_menu_kb(True, uid))
    await callback.answer()

@dp.callback_query(F.data.startswith("aprof_open:"))
async def aprof_open(callback: CallbackQuery, state: FSMContext):
    uid = int(callback.data.split(":")[1])
    await state.update_data(aprof_uid=uid, aprof_mode="open")
    await state.set_state(AdminStates.aprofile_date)
    await callback.message.edit_text("Пришли дату в формате <code>31.03.2026</code> за которую показать номера.", reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data.startswith("aprof_txt:"))
async def aprof_txt(callback: CallbackQuery, state: FSMContext):
    uid = int(callback.data.split(":")[1])
    await state.update_data(aprof_uid=uid, aprof_mode="txt")
    await state.set_state(AdminStates.aprofile_date)
    await callback.message.edit_text("Пришли дату в формате <code>31.03.2026</code> для TXT выгрузки.", reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "m_admin")
async def m_admin(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    await callback.message.edit_text("🛠 <b>ADMIN PANEL | DIAMOND VAULT</b>\n\nВыберите раздел управления:", reply_markup=admin_main_kb())
    await callback.answer()

# =========================
# ADMIN MENUS
# =========================
@dp.callback_query(F.data == "a_admins")
async def a_admins(callback: CallbackQuery):
    await callback.message.edit_text("👮 Управление администраторами", reply_markup=admin_admins_kb())
    await callback.answer()

@dp.callback_query(F.data == "a_roles")
async def a_roles(callback: CallbackQuery):
    txt = "🛡 <b>Роли</b>\n\n" + ("\n".join(f"• <code>{uid}</code> — {role_title(role)}" for uid, role in list_admins()) or "Пусто")
    txt += "\n\nРоли: super_admin / admin / operator"
    await callback.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Назначить роль", callback_data="aa_addrole"),
         InlineKeyboardButton(text="➖ Снять роль", callback_data="aa_delrole")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="a_admins")]
    ]))
    await callback.answer()

@dp.callback_query(F.data == "aa_addrole")
async def aa_addrole(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.add_role)
    await callback.message.edit_text("Пришли: <code>@тег роль</code> или <code>id роль</code>", reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "aa_delrole")
async def aa_delrole(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.del_role)
    await callback.message.edit_text("Пришли @тег / id пользователя.", reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "a_title_prices")
async def a_title_prices(callback: CallbackQuery, state: FSMContext):
    txt = (
        "🎖 <b>Прайс титулов</b>\n\n"
        f"Новичок: {fmt_money(get_title_price('newbie'))}\n"
        f"Бронза: {fmt_money(get_title_price('bronze'))}\n"
        f"Серебро: {fmt_money(get_title_price('silver'))}\n"
        f"Золото: {fmt_money(get_title_price('gold'))}\n"
        f"Платина: {fmt_money(get_title_price('platinum'))}\n\n"
        "Пришли 5 цен через пробел:\n<code>4.0 4.1 4.2 4.25 4.3</code>"
    )
    await state.set_state(AdminStates.set_title_prices)
    await callback.message.edit_text(txt, reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "a_rep_settings")
async def a_rep_settings(callback: CallbackQuery, state: FSMContext):
    b = get_rep_bonus_map()
    txt = (
        "📈 <b>Репутация / очередь</b>\n\n"
        f"🧼 Безупречный: {b['🧼 Безупречный']}\n"
        f"✅ Надёжный: {b['✅ Надёжный']}\n"
        f"👌 Стабильный: {b['👌 Стабильный']}\n"
        f"⚠ Рискованный: {b['⚠ Рискованный']}\n"
        f"🚫 Проблемный: {b['🚫 Проблемный']}\n"
        f"👶 Бонус новичка: {get_setting('newbie_priority_bonus','6')}\n"
        f"Влияние репутации: {'включено' if get_setting('rep_influence_enabled','1')=='1' else 'выключено'}\n\n"
        "Пришли: <code>perfect reliable stable risky problem newbie enabled(1/0)</code>"
    )
    await state.set_state(AdminStates.set_rep_settings)
    await callback.message.edit_text(txt, reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "aa_add")
async def aa_add(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.add_admin)
    await callback.message.edit_text("Пришли @тег / id / номер телефона.", reply_markup=back_kb())
    await callback.answer()

@dp.callback_query(F.data == "aa_del")
async def aa_del(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.del_admin)
    await callback.message.edit_text("Пришли @тег / id / номер телефона.", reply_markup=back_kb())
    await callback.answer()

@dp.callback_query(F.data == "aa_list")
async def aa_list(callback: CallbackQuery):
    await callback.message.edit_text(
        "📋 <b>Список ролей</b>\n\n" + ("\n".join(f"• <code>{uid}</code> — {role_title(role)}" for uid, role in list_admins()) or "Пусто"),
        reply_markup=admin_back_kb()
    )
    await callback.answer()

@dp.callback_query(F.data == "a_groups")
async def a_groups(callback: CallbackQuery):
    await callback.message.edit_text(
        "🏢 Управление группами и топиками\n\n"
        "Команда <code>/topic</code> должна быть отправлена внутри нужного топика.",
        reply_markup=admin_groups_kb()
    )
    await callback.answer()

@dp.callback_query(F.data == "ag_add")
async def ag_add(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.add_group)
    await callback.message.edit_text("Пришли id группы.", reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "ag_del")
async def ag_del(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.del_group)
    await callback.message.edit_text("Пришли id группы.", reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "ag_list")
async def ag_list(callback: CallbackQuery):
    await callback.message.edit_text(
        "🏢 <b>Список групп</b>\n\n" + ("\n".join(f"• <code>{x}</code>" for x in list_groups()) or "Пусто"),
        reply_markup=admin_back_kb()
    )
    await callback.answer()

@dp.callback_query(F.data == "ag_topics")
async def ag_topics(callback: CallbackQuery):
    rows = list_topics()
    txt = "🧵 <b>Список топиков</b>\n\n" + ("\n".join(f"Группа <code>{r['chat_id']}</code> → топик <code>{r['topic_id']}</code>" for r in rows) if rows else "Пусто")
    await callback.message.edit_text(txt, reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "a_queues")
async def a_queues(callback: CallbackQuery):
    await callback.message.edit_text("📦 Управление очередями", reply_markup=admin_queues_kb())
    await callback.answer()

@dp.callback_query(F.data == "aq_qr")
async def aq_qr(callback: CallbackQuery):
    await callback.message.edit_text(render_queue_text("QR"), reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "aq_code")
async def aq_code(callback: CallbackQuery):
    await callback.message.edit_text(render_queue_text("КОД"), reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "aq_delete")
async def aq_delete(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.delete_queue_phone)
    await callback.message.edit_text("Пришли номер телефона.", reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "aq_clear")
async def aq_clear(callback: CallbackQuery):
    with db_conn() as conn:
        conn.execute("UPDATE requests SET status='closed_cancelled' WHERE status IN ('queued','qr_skipped')")
    await callback.message.edit_text("✅ Очереди очищены.", reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "a_users")
async def a_users(callback: CallbackQuery):
    await callback.message.edit_text("👥 Управление пользователями", reply_markup=admin_users_kb())
    await callback.answer()

@dp.callback_query(F.data == "au_ban")
async def au_ban(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.ban_user)
    await callback.message.edit_text("Пришли @тег / id / номер телефона.", reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "au_unban")
async def au_unban(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.unban_user)
    await callback.message.edit_text("Пришли @тег / id / номер телефона.", reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "au_remove")
async def au_remove(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.remove_numbers)
    await callback.message.edit_text("Пришли @тег / id / номер телефона.", reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "au_addbal")
async def au_addbal(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.add_balance)
    await callback.message.edit_text("Пришли: @тег сумма", reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "au_subbal")
async def au_subbal(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.sub_balance)
    await callback.message.edit_text("Пришли: @тег сумма", reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "au_find")
async def au_find(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.user_lookup)
    await callback.message.edit_text("Пришли @тег / id / номер телефона.", reply_markup=admin_back_kb())
    await callback.answer()


@dp.callback_query(F.data == "au_dm")
async def au_dm(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.dm_user)
    await callback.message.edit_text("💬 Пришли @тег / id / номер телефона пользователя.", reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "au_cleardead")
async def au_cleardead(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.clear_dead_block)
    await callback.message.edit_text("🧹 Пришли @тег / id пользователя, чтобы снять временный блок за мёртвые номера.", reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "a_broadcast")
async def a_broadcast(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.broadcast_text)
    await callback.message.edit_text("📣 Пришли текст для рассылки всем пользователям бота.", reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "a_referral")
async def a_referral(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.set_ref_reward)
    await callback.message.edit_text(f"🎁 Текущая награда за реферала: <b>{fmt_money(get_ref_reward_amount())}</b>\n\nПришли новую сумму.", reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "au_clearblock")
async def au_clearblock(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.clear_number_block)
    await callback.message.edit_text("🔓 Пришли номер телефона, который нужно убрать из блока.", reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "a_stats")
async def a_stats(callback: CallbackQuery):
    await callback.message.edit_text("📊 Статистика и отчёты", reply_markup=admin_stats_kb())
    await callback.answer()

@dp.callback_query(F.data.startswith("rs:"))
async def rs(callback: CallbackQuery):
    report_type = callback.data.split(":", 1)[1]
    await callback.message.edit_text("Выбери период отчёта 👇", reply_markup=report_period_kb(report_type))
    await callback.answer()

@dp.callback_query(F.data.startswith("rs_total"))
async def rs_total(callback: CallbackQuery):
    parts = callback.data.split(":", 1)
    mode = parts[1] if len(parts) > 1 else "all"
    s = stats_for_mode(mode)
    period_name = {"all": "За всё время", "today": "За сегодня", "yesterday": "За вчера"}.get(mode, mode)
    await callback.message.edit_text(
        f"📊 <b>Общая статистика</b>\n<b>{period_name}</b>\n\n"
        f"✅ Успешно: <b>{s['success_count']}</b>\n"
        f"💥 Слётов: <b>{s['drop_count']}</b>\n"
        f"⛔️ Мёртвые номера: <b>{s['dead_count']}</b>\n"
        f"❌ Ошибки: <b>{s['error_count']}</b>\n"
        f"🟦 QR успешно: <b>{s['qr_success']}</b>\n"
        f"🟩 Код успешно: <b>{s['code_success']}</b>\n"
        f"💸 Тотал оплат: <b>{fmt_money(s['paid_total'])}</b>\n"
        f"💰 Профит: <b>{fmt_money(s['profit_total'])}</b>",
        reply_markup=admin_back_kb()
    )
    await callback.answer()

@dp.callback_query(F.data == "rs_custom_date")
async def rs_custom_date(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.report_custom_date)
    await callback.message.edit_text(
        "🗓 Пришли дату и тип отчёта.\n\nФормат:\n<code>31.03.2026 all</code>\n<code>31.03.2026 QR</code>\n<code>31.03.2026 КОД</code>",
        reply_markup=admin_back_kb()
    )
    await callback.answer()

@dp.callback_query(F.data == "a_profit")
async def a_profit(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.set_profit)
    await callback.message.edit_text(
        f"Текущий профит за 1 номер: <b>{fmt_money(get_profit_per_number())}</b>\n\nПришли новое значение.",
        reply_markup=admin_back_kb()
    )
    await callback.answer()

@dp.callback_query(F.data == "a_toggle")
async def a_toggle(callback: CallbackQuery):
    await callback.message.edit_text("🟢 <b>Управление ботом</b>\n\nВыбери режим работы:", reply_markup=admin_toggle_kb())
    await callback.answer()

@dp.callback_query(F.data == "bt_on")
async def bt_on(callback: CallbackQuery):
    set_bot_enabled(True)
    set_submit_enabled(True)
    set_bot_mode("work", "")
    set_lunch_until("")
    await callback.message.edit_text("✅ <b>Бот включён</b>\n\nРежим работы активен.", reply_markup=admin_toggle_kb())
    await callback.answer("Бот включён")

@dp.callback_query(F.data == "bt_tech")
async def bt_tech(callback: CallbackQuery):
    set_submit_enabled(False)
    set_bot_mode("tech", "🛠 Идут технические работы. Попробуйте позже.")
    await callback.message.edit_text("🛠 <b>Тех.перерыв включён</b>", reply_markup=admin_toggle_kb())
    await callback.answer("Тех.перерыв включён")

@dp.callback_query(F.data == "bt_stop")
async def bt_stop(callback: CallbackQuery):
    set_submit_enabled(False)
    set_bot_mode("stop", "⛔ В данный момент приём номеров остановлен.")
    await callback.message.edit_text("⛔ <b>Стоп работа включён</b>", reply_markup=admin_toggle_kb())
    await callback.answer("Стоп работа включён")

@dp.callback_query(F.data == "bt_lunch")
async def bt_lunch(callback: CallbackQuery):
    set_submit_enabled(False)
    set_bot_mode("lunch", "🍽 Перерыв на обед, после обеда бот снова будет включен и продолжится работа")
    set_lunch_until((datetime.now(timezone.utc)+timedelta(hours=1)).isoformat())
    await callback.message.edit_text("🍽 <b>Режим обеда включён</b>\n\nЧерез 1 час сдача номеров включится автоматически.", reply_markup=admin_toggle_kb())
    await callback.answer("Обед включён")

@dp.callback_query(F.data == "bt_temp")
async def bt_temp(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.temp_disable_text)
    await callback.message.edit_text(
        "⏳ <b>Временное отключение</b>\n\nПришли текст, который увидят пользователи.",
        reply_markup=admin_back_kb()
    )
    await callback.answer()

@dp.callback_query(F.data == "a_treasury")
async def a_treasury(callback: CallbackQuery):
    await callback.message.edit_text("🏦 Управление казной", reply_markup=admin_treasury_kb())
    await callback.answer()

@dp.callback_query(F.data == "tr_top")
async def tr_top(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.treasury_topup_amount)
    await callback.message.edit_text("Пришли сумму пополнения казны в USDT.", reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "tr_bal")
async def tr_bal(callback: CallbackQuery):
    try:
        text = await crypto_balance_text()
    except Exception as e:
        text = f"Ошибка: {e}"
    await callback.message.edit_text(f"💼 <b>Баланс казны</b>\n\n<code>{html.escape(text)}</code>", reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data == "tr_del")
async def tr_del(callback: CallbackQuery):
    try:
        deleted = await crypto_delete_checks()
        txt = f"🧹 Удалено чеков: <b>{deleted}</b>"
    except Exception as e:
        txt = f"Ошибка: <code>{html.escape(str(e))}</code>"
    await callback.message.edit_text(txt, reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data.startswith("report_show:"))
async def report_show(callback: CallbackQuery):
    parts = callback.data.split(":")
    if len(parts) == 2:
        report_type = parts[1]
        mode = "all"
    else:
        _, report_type, mode = parts
    await callback.message.edit_text(build_report_text_filtered(report_type, mode), reply_markup=admin_back_kb())
    await callback.answer()

@dp.callback_query(F.data.startswith("report_txt:"))
async def report_txt(callback: CallbackQuery):
    parts = callback.data.split(":")
    report_type = parts[1]
    mode = parts[2] if len(parts) > 2 else "all"
    txt = build_report_text_filtered(report_type, mode)
    filename = {"QR": "report_qr.txt", "КОД": "report_code.txt", "all": "report_full.txt"}[report_type]
    await callback.message.answer_document(BufferedInputFile(txt.encode("utf-8"), filename=filename), caption="📄 Выгрузка отчёта")
    await callback.answer("Готово")

# =========================
# WORK / TOPIC
# =========================
@dp.message(Command("work"))
async def work(message: Message):
    if not is_admin(message.from_user.id):
        return
    if message.chat.type not in {ChatType.GROUP, ChatType.SUPERGROUP}:
        return
    if is_group_enabled(message.chat.id):
        disable_group(message.chat.id)
        await message.answer("⛔ Бот выключен в этой группе.")
    else:
        enable_group(message.chat.id)
        await message.answer("✅ Бот активирован в этой группе.")

@dp.message(Command("topic"))
async def topic(message: Message):
    if not is_admin(message.from_user.id):
        return
    if message.chat.type not in {ChatType.GROUP, ChatType.SUPERGROUP}:
        return
    thread_id = getattr(message, "message_thread_id", None)
    if thread_id is None:
        await message.answer("⛔ Команду /topic нужно писать внутри нужного топика.")
        return
    if is_topic_enabled(message.chat.id, thread_id):
        del_topic(message.chat.id, thread_id)
        await message.answer("⛔ Бот выключен в этом топике.")
    else:
        add_topic(message.chat.id, thread_id)
        await message.answer("✅ Бот активирован в этом топике.")

# =========================
# ISSUE
# =========================
async def issue_next(message: Message, method: str):
    if not can_work_here(message):
        return False
    if not is_operator(message.from_user.id):
        return False

    async with issue_lock:
        ids = queue_ids(method)
        if not ids:
            await message.answer(f"❌ Свободных заявок {method} сейчас нет.")
            return True

        req_id = ids[0]
        req = get_request(req_id)
        if not req or req["status"] not in ("queued", "qr_skipped"):
            return True

        update_request(
            req_id,
            status="awaiting_activity",
            claimed_by=message.from_user.id,
            issue_group_id=message.chat.id,
            issue_thread_id=getattr(message, "message_thread_id", None),
        )

        task = queue_tasks.pop(req_id, None)
        if task:
            task.cancel()

        await message.answer(
            "📌 <b>Номер выдан</b>\n\n"
            f"• Номер: <code>{req['phone']}</code>\n"
            f"• Метод: <b>{method}</b>\n"
            f"• Заявка: <b>#{req_id}</b>\n\n"
            "⚡️ Ждём подтверждение пользователя…\n"
            "Если не подтвердит — заявка станет мёртвой."
        )
        msg = await bot.send_message(
            req["user_id"],
            "⚡️ <b>Подтверди активность</b>\n\n"
            f"Осталось: <b>{ACTIVITY_SECONDS} сек</b>\n"
            "Нажми «Я в сети», иначе заявка закроется.",
            reply_markup=confirm_activity_kb(req_id),
        )
        update_request(req_id, user_activity_message_id=msg.message_id)
        activity_tasks[req_id] = asyncio.create_task(activity_timer(req_id))
        await send_log(f"📌 Выдан номер #{req_id} | {req['phone']} | метод {method} | оператор {message.from_user.id}")
        return True

# =========================
# REQUEST CALLBACKS
# =========================
@dp.callback_query(F.data == "submit_qr")
async def submit_qr(callback: CallbackQuery, state: FSMContext):
    block_msg = blocked_for_dead_numbers(callback.from_user.id)
    if block_msg:
        await callback.message.edit_text(block_msg, reply_markup=admin_back_kb())
        await callback.answer()
        return
    await state.set_state(SubmitStates.waiting_phone_qr)
    await callback.message.edit_text(
        "📥 <b>Отправь номер</b>\n\n"
        "Формат: <code>+7/7/8XXXXXXXXXX</code>\n"
        "Пример: <code>+79991234567</code>\n\n"
        "Без пробелов и лишних символов.",
        reply_markup=back_kb(),
    )
    await callback.answer()

@dp.callback_query(F.data == "submit_code")
async def submit_code(callback: CallbackQuery, state: FSMContext):
    block_msg = blocked_for_dead_numbers(callback.from_user.id)
    if block_msg:
        await callback.message.edit_text(block_msg, reply_markup=admin_back_kb())
        await callback.answer()
        return
    await state.set_state(SubmitStates.waiting_phone_code)
    await callback.message.edit_text(
        "📥 <b>Отправь номер</b>\n\n"
        "Формат: <code>+7/7/8XXXXXXXXXX</code>\n"
        "Пример: <code>+79991234567</code>\n\n"
        "Без пробелов и лишних символов.",
        reply_markup=back_kb(),
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("alive:"))
async def alive(callback: CallbackQuery):
    req_id = int(callback.data.split(":")[1])
    req = get_request(req_id)
    if not req or req["user_id"] != callback.from_user.id or req["status"] != "awaiting_activity":
        await callback.answer("Недоступно", show_alert=True)
        return
    update_request(req_id, status="alive_confirmed")
    t = activity_tasks.pop(req_id, None)
    if t:
        t.cancel()
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await callback.message.edit_text("✅ <b>Активность подтверждена</b>\nОжидайте запрос кода\nили фото QR кода")
    if req["method"] == "QR":
        await group_send(
            req,
            "🔑 <b>Нужен QR-КОД</b>\n\n"
            f"Заявка: <b>#{req_id}</b>\n"
            f"Номер: <code>{req['phone']}</code>\n\n"
            "Пришли фото QR одним сообщением.\n"
            f"У вас есть <b>{CODE_SECONDS} секунд</b> на отправку QR.\n\n"
            "Если передумали — нажмите «Отмена».",
            reply_markup=operator_qr_stage_kb(req_id),
        )
    else:
        await group_send(
            req,
            "✅ <b>Пользователь в сети</b>\n\n"
            f"Заявка: <b>#{req_id}</b>\n"
            f"Номер: <code>{req['phone']}</code>\n\n"
            "Выбери следующий шаг 👇",
            reply_markup=operator_code_stage_kb(req_id),
        )
    await callback.answer("Подтверждено")

@dp.callback_query(F.data.startswith("sendcode:"))
async def sendcode(callback: CallbackQuery):
    req_id = int(callback.data.split(":")[1])
    req = get_request(req_id)
    if not req or req["status"] != "alive_confirmed":
        await callback.answer("Недоступно", show_alert=True)
        return
    if req["claimed_by"] != callback.from_user.id:
        update_request(req_id, claimed_by=callback.from_user.id)
        req = get_request(req_id)
    update_request(req_id, status="awaiting_sms")
    msg = await bot.send_message(
        req["user_id"],
        "🔑 <b>Нужен код</b>\n\n"
        "Оператор запросил код для входа.\n"
        "Пришли 6-значный код одним сообщением.\n"
        f"У тебя есть <b>{CODE_SECONDS} секунд</b> на отправку кода.\n\n"
        "Если нужен новый код — нажми «Запросить повтор».",
        reply_markup=repeat_request_kb(req_id),
    )
    update_request(req_id, user_code_message_id=msg.message_id)
    t = code_tasks.pop(req_id, None)
    if t:
        t.cancel()
    code_tasks[req_id] = asyncio.create_task(code_timer(req_id))
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await send_log(f"📤 Запрошен код #{req_id} | {req['phone']} | оператор {callback.from_user.id} | claimed_by {req['claimed_by']}")
    await callback.answer("Запрос кода отправлен")

@dp.callback_query(F.data.startswith("sendqr:"))
async def sendqr(callback: CallbackQuery):
    req_id = int(callback.data.split(":")[1])
    req = get_request(req_id)
    if not req or req["status"] in ("closed_paid", "closed_dead", "closed_error", "closed_cancelled", "closed_drop_no_pay", "closed_drop_paid"):
        await callback.answer("Недоступно", show_alert=True)
        return
    if req["claimed_by"] != callback.from_user.id:
        update_request(req_id, claimed_by=callback.from_user.id)
        req = get_request(req_id)
    update_request(req_id, status="awaiting_qr")
    try:
        await bot.send_message(req["user_id"], "📷 <b>Ожидай QR для сканирования</b>\n\nОператор запросил QR-код. Как только он будет отправлен — я сразу пришлю его тебе.", reply_markup=back_kb())
    except Exception:
        pass
    t = qr_tasks.pop(req_id, None)
    if t:
        t.cancel()
    qr_tasks[req_id] = asyncio.create_task(qr_timer(req_id))
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await send_log(f"📷 Запрошен QR #{req_id} | {req['phone']} | оператор {callback.from_user.id}")
    await send_log(f"📷 Запрошен QR #{req_id} | {req['phone']} | оператор {callback.from_user.id} | claimed_by {req['claimed_by']}")
    await callback.answer("Запрос QR отправлен")

@dp.callback_query(F.data.startswith("cancelop:"))
async def cancelop(callback: CallbackQuery):
    req_id = int(callback.data.split(":")[1])
    req = get_request(req_id)
    if not operator_owns(callback.from_user.id, req):
        await callback.answer("Не твой номер", show_alert=True)
        return
    update_request(req_id, status="closed_cancelled")
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await send_log(f"🚫 Отмена оператором #{req_id} | {req['phone']} | оператор {callback.from_user.id}")
    await callback.answer("Отменено")

@dp.callback_query(F.data.startswith("ok:"))
async def ok_handler(callback: CallbackQuery):
    req_id = int(callback.data.split(":")[1])
    req = get_request(req_id)
    if not operator_owns(callback.from_user.id, req) or req["status"] not in ("code_received", "qr_scanned"):
        await callback.answer("Недоступно", show_alert=True)
        return
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    block_phone_until_midnight(req["phone"], req_id)
    update_request(req_id, status="success_hold", hold_started_at=datetime.now(timezone.utc).isoformat(), hold_group_message_id=callback.message.message_id)
    msg = await bot.send_message(
        req["user_id"],
        "✅ <b>ВСТАЛО</b>\n\n"
        f"Заявка: <b>#{req_id}</b>\n"
        f"Номер: <code>{req['phone']}</code>\n\n"
        "⏳ <b>Холд активен</b>\n\n"
        f"• Поставлен (МСК): <b>{datetime.now(MSK).strftime('%H:%M:%S')}</b>\n\n"
        f"{progress_bar(0, HOLD_SECONDS)}\n"
        f"Осталось: <b>{seconds_to_hhmmss(HOLD_SECONDS)}</b>\n\n"
        "Начисление будет автоматически после окончания холда ✅",
        reply_markup=admin_back_kb(),
    )
    update_request(req_id, hold_user_message_id=msg.message_id)
    t = hold_tasks.pop(req_id, None)
    if t:
        t.cancel()
    hold_tasks[req_id] = asyncio.create_task(hold_timer(req_id))
    try:
        await callback.message.edit_text(
            "✅ <b>ВСТАЛО</b>\n\n"
            f"Заявка: <b>#{req_id}</b>\n"
            f"Номер: <code>{req['phone']}</code>\n\n"
            "⏳ <b>Холд активен</b>\n\n"
            f"• Поставлен (МСК): <b>{datetime.now(MSK).strftime('%H:%M:%S')}</b>\n\n"
            f"{progress_bar(0, HOLD_SECONDS)}\n"
            f"Осталось: <b>{seconds_to_hhmmss(HOLD_SECONDS)}</b>\n\n"
            "Начисление будет автоматически после окончания холда ✅",
            reply_markup=hold_kb(req_id)
        )
    except Exception:
        pass
    await send_log(f"✅ Встал #{req_id} | {req['phone']} | оператор {callback.from_user.id}")
    await callback.answer("Холд запущен")

@dp.callback_query(F.data.startswith("err:"))
async def err_handler(callback: CallbackQuery):
    req_id = int(callback.data.split(":")[1])
    req = get_request(req_id)
    if not operator_owns(callback.from_user.id, req) or req["status"] not in ("code_received", "qr_scanned"):
        await callback.answer("Недоступно", show_alert=True)
        return
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    update_request(req_id, status="closed_error")
    add_report(req, "Ошибка", 0)
    try:
        await callback.message.edit_text(
            f"❌ <b>Ошибка по заявке</b>\n\nЗаявка: <b>#{req_id}</b>\nНомер: <code>{req['phone']}</code>"
        )
    except Exception:
        pass
    try:
        await bot.send_message(
            req["user_id"],
            "❌ <b>Заявка закрыта по ошибке</b>\n\n"
            f"Номер: <code>{req['phone']}</code>\n"
            "Не удалось успешно ввести код или отсканировать QR. Попробуй отправить номер заново позже.",
            reply_markup=back_kb()
        )
    except Exception:
        pass
    await send_log(f"❌ Ошибка #{req_id} | {req['phone']} | оператор {callback.from_user.id}")
    await callback.answer("Отмечено")

@dp.callback_query(F.data.startswith("user_repeat:"))
async def user_repeat_handler(callback: CallbackQuery):
    req_id = int(callback.data.split(":")[1])
    req = get_request(req_id)
    if not req or req["user_id"] != callback.from_user.id or req["status"] != "awaiting_sms":
        await callback.answer("Недоступно", show_alert=True)
        return

    # Отключаем кнопку у старого сообщения, чтобы не спамили по нему.
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await group_send(
        req,
        "🔁 <b>Пользователь запросил повтор</b>\n\n"
        "Подтверди повтор в группе, после этого пользователю придёт новый запрос на ввод кода.",
        reply_markup=operator_repeat_confirm_kb(req_id),
    )
    await send_log(f"🔁 Пользователь запросил повтор #{req_id} | номер {req['phone']}")
    await callback.answer("Запрос на повтор отправлен")


@dp.callback_query(F.data.startswith("confirmrepeat:"))
async def confirm_repeat_handler(callback: CallbackQuery):
    req_id = int(callback.data.split(":")[1])
    req = get_request(req_id)
    if not req or req["status"] != "awaiting_sms":
        await callback.answer("Недоступно", show_alert=True)
        return
    if req["claimed_by"] != callback.from_user.id:
        await callback.answer("Не твой номер", show_alert=True)
        return

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    t = code_tasks.pop(req_id, None)
    if t:
        t.cancel()

    new_msg = await bot.send_message(
        req["user_id"],
        "🔁 <b>Повторный запрос кода</b>\n\n"
        "Оператор подтвердил повтор.\n"
        "Пришли 6-значный код одним сообщением.\n"
        f"У вас есть <b>{CODE_SECONDS} секунд</b> на отправку кода.\n\n"
        "Повтор можно запрашивать до тех пор, пока не будет отмены в группе или пока не пришлют код.",
        reply_markup=repeat_request_kb(req_id),
    )
    update_request(req_id, user_code_message_id=new_msg.message_id, repeat_count=int(req["repeat_count"]) + 1)
    code_tasks[req_id] = asyncio.create_task(code_timer(req_id))

    await send_log(f"✅ Подтвержден повтор #{req_id} | номер {req['phone']} | оператор {callback.from_user.id}")
    await callback.answer("Повтор подтверждён")


@dp.callback_query(F.data.startswith("askpass:"))
async def askpass_handler(callback: CallbackQuery):
    req_id = int(callback.data.split(":")[1])
    req = get_request(req_id)
    if not operator_owns(callback.from_user.id, req) or req["status"] not in ("qr_scanned", "code_received"):
        await callback.answer("Недоступно", show_alert=True)
        return
    update_request(req_id, password_requested=1)
    pt = password_tasks.pop(req_id, None)
    if pt:
        pt.cancel()
    password_tasks[req_id] = asyncio.create_task(password_timer(req_id))
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await bot.send_message(req["user_id"], "🔐 <b>Требуется пароль</b>\n\nЕсли на аккаунте есть пароль — отправь его одним сообщением. После этого я передам пароль оператору. Пароль можно отправить буквами, цифрами или смешанным текстом.")
    await group_send(req, f"🔐 <b>Запрошен пароль</b>\n\nЗаявка: <b>#{req_id}</b>\nНомер: <code>{req['phone']}</code>")
    await send_log(f"🔐 Запрошен пароль #{req_id} | {req['phone']} | оператор {callback.from_user.id}")
    await callback.answer("Пароль запрошен")

@dp.callback_query(F.data.startswith("user_qr_repeat:"))
async def user_qr_repeat_handler(callback: CallbackQuery):
    req_id = int(callback.data.split(":")[1])
    req = get_request(req_id)
    if not req or req["user_id"] != callback.from_user.id or req["status"] != "qr_sent_to_user":
        await callback.answer("Недоступно", show_alert=True)
        return
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await group_send(
        req,
        "🔁 <b>Пользователь запросил повтор QR</b>\n\nПодтверди повтор в группе, после этого можно будет снова выдать QR.",
        reply_markup=operator_repeat_confirm_qr_kb(req_id),
    )
    await send_log(f"🔁 Пользователь запросил повтор QR #{req_id} | номер {req['phone']}")
    await callback.answer("Запрос на повтор QR отправлен")

@dp.callback_query(F.data.startswith("confirmrepeatqr:"))
async def confirmrepeatqr_handler(callback: CallbackQuery):
    req_id = int(callback.data.split(":")[1])
    req = get_request(req_id)
    if not req or req["status"] not in ("qr_sent_to_user", "awaiting_qr"):
        await callback.answer("Недоступно", show_alert=True)
        return
    if req["claimed_by"] != callback.from_user.id:
        update_request(req_id, claimed_by=callback.from_user.id)
        req = get_request(req_id)
    update_request(req_id, status="awaiting_qr")
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await bot.send_message(req["user_id"], "🔁 <b>Повтор по QR подтверждён</b>\n\nОжидай новый QR для сканирования. Как только оператор отправит QR — я сразу пришлю его тебе.", reply_markup=back_kb())
    await group_send(req, "✅ <b>Пользователь запросил повтор QR</b>\n\nВыбери следующий шаг 👇", reply_markup=operator_qr_stage_kb(req_id))
    t = qr_tasks.pop(req_id, None)
    if t:
        t.cancel()
    qr_tasks[req_id] = asyncio.create_task(qr_timer(req_id))
    await send_log(f"✅ Подтвержден повтор QR #{req_id} | номер {req['phone']} | оператор {callback.from_user.id}")
    await callback.answer("Повтор QR подтверждён")

@dp.callback_query(F.data.startswith("oprepeat_code:"))
async def oprepeat_code_handler(callback: CallbackQuery):
    req_id = int(callback.data.split(":")[1])
    req = get_request(req_id)
    if not operator_owns(callback.from_user.id, req) or req["status"] not in ("code_received", "awaiting_sms"):
        await callback.answer("Недоступно", show_alert=True)
        return
    update_request(req_id, status="awaiting_sms")
    t = code_tasks.pop(req_id, None)
    if t:
        t.cancel()
    new_msg = await bot.send_message(
        req["user_id"],
        "🔁 <b>Повторный запрос кода</b>\n\n"
        "Прошлый код оказался неверным или устарел.\n"
        "Пришли новый 6-значный код одним сообщением.\n\n"
        f"У тебя есть <b>{CODE_SECONDS} секунд</b> на отправку кода.",
        reply_markup=repeat_request_kb(req_id),
    )
    update_request(req_id, user_code_message_id=new_msg.message_id)
    code_tasks[req_id] = asyncio.create_task(code_timer(req_id))
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await send_log(f"🔁 Оператор запросил повтор кода #{req_id} | {req['phone']} | оператор {callback.from_user.id}")
    await callback.answer("Повтор кода отправлен")

@dp.callback_query(F.data.startswith("oprepeat_qr:"))
async def oprepeat_qr_handler(callback: CallbackQuery):
    req_id = int(callback.data.split(":")[1])
    req = get_request(req_id)
    if not operator_owns(callback.from_user.id, req) or req["status"] not in ("qr_scanned", "qr_sent_to_user", "awaiting_qr"):
        await callback.answer("Недоступно", show_alert=True)
        return
    update_request(req_id, status="awaiting_qr")
    t = qr_tasks.pop(req_id, None)
    if t:
        t.cancel()
    qr_tasks[req_id] = asyncio.create_task(qr_timer(req_id))
    try:
        await bot.send_message(
            req["user_id"],
            "🔁 <b>Повторный запрос QR</b>\n\n"
            "Прошлый QR оказался неверным или устарел.\n"
            "Ожидай новый QR для сканирования."
        )
    except Exception:
        pass
    await group_send(
        req,
        f"🔁 <b>Оператор запросил повтор QR</b>\n\nЗаявка: <b>#{req_id}</b>\nНомер: <code>{req['phone']}</code>\n\nПришли новый QR одним сообщением.",
        reply_markup=operator_qr_stage_kb(req_id)
    )
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await send_log(f"🔁 Оператор запросил повтор QR #{req_id} | {req['phone']} | оператор {callback.from_user.id}")
    await callback.answer("Повтор QR отправлен")

@dp.callback_query(F.data.startswith("drop:"))
async def drop_handler(callback: CallbackQuery):
    req_id = int(callback.data.split(":")[1])
    req = get_request(req_id)
    if not operator_owns(callback.from_user.id, req) or req["status"] != "success_hold":
        await callback.answer("Недоступно", show_alert=True)
        return
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    started = datetime.fromisoformat(req["hold_started_at"]) if req["hold_started_at"] else datetime.now(timezone.utc)
    elapsed = int((datetime.now(timezone.utc) - started).total_seconds())
    task = hold_tasks.pop(req_id, None)
    if task:
        task.cancel()
    if elapsed >= HOLD_SECONDS:
        add_balance(req["user_id"], request_price(req["method"], req["user_id"]))
        update_request(req_id, status="closed_drop_paid", credited=1)
        add_report(req, "Успешно", request_price(req["method"], req["user_id"]))
        try:
            await bot.send_message(
                req["user_id"],
                hold_closed_text(req["phone"], request_price(req["method"], req["user_id"]), get_balance(req["user_id"])),
                reply_markup=back_kb(),
            )
        except Exception:
            pass
    else:
        update_request(req_id, status="closed_drop_no_pay")
        add_report(req, "Слёт", 0)
        increment_dead_streak(req["user_id"])
        try:
            await bot.send_message(req["user_id"], f"💥 <b>Слёт по заявке</b>\n\nНомер: <code>{req['phone']}</code>\nХолд остановлен. Начисление не произведено.", reply_markup=back_kb())
        except Exception:
            pass
    await send_log(f"💥 Слёт #{req_id} | {req['phone']} | оператор {callback.from_user.id}")
    await callback.answer("Слёт зафиксирован")

@dp.callback_query(F.data.startswith("sc:"))
async def sc_handler(callback: CallbackQuery):
    req_id = int(callback.data.split(":")[1])
    req = get_request(req_id)
    if not req or req["user_id"] != callback.from_user.id or req["status"] != "qr_sent_to_user":
        await callback.answer("Недоступно", show_alert=True)
        return
    update_request(req_id, status="qr_scanned")
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await group_send(
        req,
        "📷 <b>Реакция по QR</b>\n"
        f"Заявка: <b>#{req_id}</b>\n"
        f"Номер: <code>{req['phone']}</code>\n"
        "Статус: <b>ГОТОВО</b>",
        reply_markup=operator_result_qr_kb(req_id),
    )
    await callback.answer("Отмечено")

@dp.callback_query(F.data.startswith("skip:"))
async def skip_handler(callback: CallbackQuery):
    req_id = int(callback.data.split(":")[1])
    req = get_request(req_id)
    if not req or req["user_id"] != callback.from_user.id or req["status"] != "qr_sent_to_user":
        await callback.answer("Недоступно", show_alert=True)
        return
    update_request(req_id, status="closed_cancelled", queue_last_ping_at=None, queue_ping_deadline_at=None)
    t = queue_tasks.pop(req_id, None)
    if t:
        t.cancel()
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    try:
        await callback.message.edit_caption(
            caption=(
                "⏭ <b>QR пропущен</b>\n\n"
                f"Заявка: <b>#{req_id}</b>\n"
                f"Номер: <code>{req['phone']}</code>\n\n"
                "Заявка убрана из очереди до повторной ручной сдачи номера."
            ),
            reply_markup=None
        )
    except Exception:
        pass
    try:
        await bot.send_message(
            req["user_id"],
            "⏭ <b>Вы нажали Скип</b>\n\n"
            f"Номер: <code>{req['phone']}</code>\n"
            "Заявка убрана из очереди. Чтобы снова поставить номер — отправь его заново."
        )
    except Exception:
        pass
    await group_send(
        req,
        "⏭ <b>Пользователь нажал Скип</b>\n\n"
        f"Заявка: <b>#{req_id}</b>\n"
        f"Номер: <code>{req['phone']}</code>\n\n"
        "Заявка убрана из очереди до повторной сдачи номера."
    )
    await send_log(f"⏭ Скип QR #{req_id} | {req['phone']} | user={req['user_id']}")
    await callback.answer("QR пропущен. Заявка убрана из очереди")

@dp.callback_query(F.data.startswith("keep:"))
async def keep_handler(callback: CallbackQuery):
    req_id = int(callback.data.split(":")[1])
    req = get_request(req_id)
    if not req or req["user_id"] != callback.from_user.id or req["status"] not in ("queued", "qr_skipped"):
        await callback.answer("Недоступно", show_alert=True)
        return
    bumps = int(req["queue_bumps"]) + 1
    # old timestamp -> queue top
    older = (datetime.now(timezone.utc) - timedelta(days=1, minutes=bumps)).isoformat()
    update_request(
        req_id,
        queue_bumps=bumps,
        queue_last_ping_at=datetime.now(timezone.utc).isoformat(),
        queue_ping_deadline_at=None,
        created_at=older,
    )
    try:
        await callback.message.edit_text(f"✅ <b>Номер подтверждён</b>\n\nПоднятие в очереди: <b>{bumps}/{QUEUE_AFK_MAX_BUMPS}</b>")
    except Exception:
        pass
    await callback.answer("Подтверждено")

# =========================
# WITHDRAW CALLBACKS
# =========================
@dp.callback_query(F.data.startswith("wok:"))
async def wok(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    wid = int(callback.data.split(":")[1])
    with db_conn() as conn:
        w = conn.execute("SELECT * FROM withdrawals WHERE id=?", (wid,)).fetchone()
    if not w or w["status"] != "pending":
        await callback.answer("Уже обработано", show_alert=True)
        return
    if not CRYPTO_PAY_TOKEN or CRYPTO_PAY_TOKEN.startswith("PASTE_"):
        await callback.answer("CRYPTO_PAY_TOKEN не настроен", show_alert=True)
        return
    if float(w["amount"]) <= 0:
        await callback.answer("Некорректная сумма", show_alert=True)
        return
    if get_balance(int(w["user_id"])) < float(w["amount"]):
        await callback.answer("Недостаточно баланса пользователя", show_alert=True)
        return
    try:
        balances = await crypto_api("getBalance", {})
        total_available = 0.0
        for b in balances:
            try:
                if b.get("currency_code") == TREASURY_ASSET:
                    total_available += float(b.get("available", 0) or 0)
            except Exception:
                pass
        if total_available <= 0:
            await callback.answer("Казна пустая", show_alert=True)
            return
        if total_available < float(w["amount"]):
            await callback.answer("Недостаточно средств в казне", show_alert=True)
            return
        check = await crypto_create_check(float(w["amount"]), int(w["user_id"]))
        link = check.get("bot_check_url", "")
        with db_conn() as conn:
            conn.execute("UPDATE withdrawals SET status='accepted', payout_url=? WHERE id=?", (link, wid))
            conn.execute("UPDATE users SET balance=balance-?, pending=pending-? WHERE user_id=?", (float(w["amount"]), float(w["amount"]), int(w["user_id"])))
        try:
            await bot.send_message(
                w["user_id"],
                "✅ <b>ВЫВОД ЗАВЕРШЁН | DIAMOND VAULT</b>\n\n"
                "Заявка успешно обработана:\n\n"
                "━━━━━━━━━━━━━━\n"
                f"{link}\n"
                f"💸 Сумма: <b>{fmt_money(float(w['amount']))}</b>\n"
                "📍 Статус: выполнено\n"
                "━━━━━━━━━━━━━━\n\n"
                "Средства отправлены ✔️",
                reply_markup=admin_back_kb(),
            )
        except Exception:
            pass
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await send_log(f"💸 Выплата #{wid} | user {w['user_id']} | сумма {w['amount']} | чек {link}")
        await callback.answer("Принято")
    except Exception as e:
        await callback.answer(f"Ошибка выплаты: {e}", show_alert=True)

@dp.callback_query(F.data.startswith("wno:"))
async def wno(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    wid = int(callback.data.split(":")[1])
    with db_conn() as conn:
        w = conn.execute("SELECT * FROM withdrawals WHERE id=?", (wid,)).fetchone()
    if not w or w["status"] != "pending":
        await callback.answer("Уже обработано", show_alert=True)
        return
    with db_conn() as conn:
        conn.execute("UPDATE withdrawals SET status='declined' WHERE id=?", (wid,))
        conn.execute("UPDATE users SET pending=pending-? WHERE user_id=?", (float(w["amount"]), int(w["user_id"])))
    try:
        await bot.send_message(w["user_id"], f"❌ Заявка на вывод отклонена\n\nСумма: <b>{fmt_money(float(w['amount']))}</b>", reply_markup=admin_back_kb())
    except Exception:
        pass
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await send_log(f"❌ Выплата отклонена #{wid} | user {w['user_id']} | сумма {w['amount']}")
    await callback.answer("Отклонено")

# =========================
# TEXT ROUTER
# =========================
@dp.message(Command("work"))
async def work_cmd(message: Message):
    if not is_admin(message.from_user.id):
        return
    if message.chat.type not in {ChatType.GROUP, ChatType.SUPERGROUP}:
        return
    if is_group_enabled(message.chat.id):
        disable_group(message.chat.id)
        await message.answer("⛔ Бот выключен в этой группе.")
    else:
        enable_group(message.chat.id)
        await message.answer("✅ Бот активирован в этой группе.")

@dp.message(Command("topic"))
async def topic_cmd(message: Message):
    if not is_admin(message.from_user.id):
        return
    if message.chat.type not in {ChatType.GROUP, ChatType.SUPERGROUP}:
        return
    topic_id = getattr(message, "message_thread_id", None)
    if topic_id is None:
        await message.answer("⛔ Команду /topic нужно писать внутри нужного топика.")
        return
    if is_topic_enabled(message.chat.id, topic_id):
        del_topic(message.chat.id, topic_id)
        await message.answer("⛔ Бот выключен в этом топике.")
    else:
        add_topic(message.chat.id, topic_id)
        await message.answer("✅ Бот активирован в этом топике.")

@dp.message(F.document)
async def admin_document_router(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    current_state = await state.get_state()
    if current_state not in (AdminStates.upload_db, AdminStates.upload_log):
        return

    doc = message.document
    if current_state == AdminStates.upload_db:
        if not (doc.file_name or "").lower().endswith(".sqlite3"):
            await message.answer("❌ Нужен файл базы с расширением <code>.sqlite3</code>.")
            return
        file = await bot.get_file(doc.file_id)
        await bot.download_file(file.file_path, destination=DB_PATH)
        await state.clear()
        await message.answer("✅ База данных успешно загружена в /data")
        await send_log(f"📥 Загружена база данных | admin={message.from_user.id}")
        return

    if current_state == AdminStates.upload_log:
        if not (doc.file_name or "").lower().endswith(".log"):
            await message.answer("❌ Нужен файл логов с расширением <code>.log</code>.")
            return
        file = await bot.get_file(doc.file_id)
        await bot.download_file(file.file_path, destination=LOG_FILE_PATH)
        await state.clear()
        await message.answer("✅ Лог-файл успешно загружен в /data")
        await send_log(f"📥 Загружен лог-файл | admin={message.from_user.id}")
        return

@dp.message()
async def text_router(message: Message, state: FSMContext):
    text = (message.text or "").strip()

    if message.chat.type == ChatType.PRIVATE:
        if not await require_subscription(message):
            return
        ensure_user(message.from_user)
        if not bot_enabled():
            await message.answer(f"⛔️ <b>Бот сейчас недоступен</b>\n\n{bot_mode_text()}")
            return

    if is_banned(message.from_user.id):
        await message.answer("⛔ Вы заблокированы.")
        return

    current_state = await state.get_state()

    if current_state == AdminStates.temp_disable_text and is_admin(message.from_user.id):
        set_bot_enabled(False)
        set_bot_mode("temp", text)
        await state.clear()
        await message.answer("⏳ Временное отключение включено.")
        return

    if current_state == AdminStates.broadcast_text and is_admin(message.from_user.id):
        sent = 0
        failed = 0
        with db_conn() as conn:
            users = conn.execute("SELECT user_id FROM users").fetchall()
        for u in users:
            try:
                await bot.send_message(int(u["user_id"]), text)
                sent += 1
            except Exception:
                failed += 1
        await state.clear()
        await send_log(f"📣 Рассылка завершена | sent {sent} | failed {failed}")
        await message.answer(f"📣 Рассылка завершена\n\nУспешно: <b>{sent}</b>\nОшибок: <b>{failed}</b>", reply_markup=admin_back_kb())
        return

    if current_state == AdminStates.report_custom_date and is_admin(message.from_user.id):
        parts = text.split()
        if len(parts) != 2:
            await message.answer("Неверный формат. Пример: <code>31.03.2026 all</code>", reply_markup=admin_back_kb())
            return
        date_str, report_type = parts[0], parts[1].upper()
        if report_type == "ALL":
            report_type = "all"
        if report_type not in ("all", "QR", "КОД"):
            await message.answer("Тип отчёта должен быть: all / QR / КОД", reply_markup=admin_back_kb())
            return
        try:
            from datetime import datetime as _dt
            _dt.strptime(date_str, "%d.%m.%Y")
        except Exception:
            await message.answer("Неверная дата. Используй формат <code>31.03.2026</code>", reply_markup=admin_back_kb())
            return
        await state.clear()
        await message.answer(build_report_text_for_exact_date(report_type, date_str), reply_markup=admin_back_kb())
        return

    if current_state == AdminStates.dm_user and is_admin(message.from_user.id):
        row = user_by_any(text)
        if not row:
            await message.answer("Пользователь не найден.", reply_markup=admin_back_kb())
            return
        await state.update_data(dm_user_id=int(row["user_id"]))
        await state.set_state(AdminStates.dm_text)
        await message.answer(f"💬 Пользователь найден: <code>{row['user_id']}</code>\n\nПришли текст для отправки.", reply_markup=admin_back_kb())
        return

    if current_state == AdminStates.dm_text and is_admin(message.from_user.id):
        data = await state.get_data()
        uid = int(data.get("dm_user_id", 0))
        try:
            await bot.send_message(uid, text)
            await message.answer("✅ Сообщение отправлено пользователю.", reply_markup=admin_back_kb())
            await send_log(f"💬 Админ написал в ЛС | to={uid} | admin={message.from_user.id}")
        except Exception as e:
            await message.answer(f"❌ Не удалось отправить сообщение.\n<code>{html.escape(str(e))}</code>", reply_markup=admin_back_kb())
        await state.clear()
        return

    if current_state == AdminStates.clear_dead_block and is_admin(message.from_user.id):
        row = user_by_any(text)
        if not row:
            await message.answer("Пользователь не найден.", reply_markup=admin_back_kb())
            return
        set_dead_streak(int(row["user_id"]), 0)
        set_dead_block_until(int(row["user_id"]), "")
        await state.clear()
        await message.answer("✅ Временный блок за мёртвые номера снят.", reply_markup=admin_back_kb())
        await send_log(f"🧹 Снят блок за мёртвые | user={row['user_id']} | admin={message.from_user.id}")
        return

    if current_state == AdminStates.clear_number_block and is_admin(message.from_user.id):
        phone = normalize_phone(text)
        if not phone:
            await message.answer("Неверный формат номера.")
            return
        ok = clear_phone_block(phone)
        await state.clear()
        await message.answer("✅ Блок номера снят." if ok else "Блок по номеру не найден.", reply_markup=admin_back_kb())
        return

    if current_state == AdminStates.set_ref_reward and is_admin(message.from_user.id):
        set_ref_reward_amount(float(text.replace(",", ".")))
        await state.clear()
        await message.answer(f"✅ Награда за реферала обновлена: <b>{fmt_money(get_ref_reward_amount())}</b>", reply_markup=admin_back_kb())
        return

    if current_state == AdminStates.treasury_topup_amount and is_admin(message.from_user.id):
        amount = float(text.replace(",", "."))
        try:
            inv = await crypto_create_invoice(amount)
            url = inv.get("bot_invoice_url") or inv.get("mini_app_invoice_url") or inv.get("web_app_invoice_url", "")
            await state.clear()
            await message.answer(
                "🏦 <b>Пополнение казны</b>\n\n"
                f"Сумма: <b>{amount:.2f} {TREASURY_ASSET}</b>\n"
                f"Ссылка: {url}"
            )
        except Exception as e:
            await message.answer(f"❌ Ошибка создания инвойса:\n<code>{html.escape(str(e))}</code>")
        return

    if current_state == AdminStates.add_role and is_admin(message.from_user.id):
        parts = text.split()
        if len(parts) != 2:
            await message.answer("Формат: id/тег роль", reply_markup=admin_back_kb())
            return
        row = user_by_any(parts[0])
        uid = int(row["user_id"]) if row else (int(parts[0]) if parts[0].isdigit() else None)
        role = parts[1].strip().lower()
        if uid is None or role not in ("super_admin","admin","operator"):
            await message.answer("Неверные данные.", reply_markup=admin_back_kb())
            return
        if not can_manage_role(message.from_user.id, role):
            await message.answer("Нет прав назначать эту роль.", reply_markup=admin_back_kb())
            return
        set_role(uid, role)
        await state.clear()
        await message.answer(f"✅ Роль обновлена: <code>{uid}</code> → {role_title(role)}", reply_markup=admin_back_kb())
        return

    if current_state == AdminStates.del_role and is_admin(message.from_user.id):
        row = user_by_any(text)
        uid = int(row["user_id"]) if row else (int(text) if text.isdigit() else None)
        if uid is None:
            await message.answer("Пользователь не найден.", reply_markup=admin_back_kb())
            return
        if get_role(uid) == "super_admin" and get_role(message.from_user.id) != "super_admin":
            await message.answer("Супер-админа снять нельзя.", reply_markup=admin_back_kb())
            return
        remove_role(uid)
        await state.clear()
        await message.answer(f"✅ Роль снята: <code>{uid}</code>", reply_markup=admin_back_kb())
        return

    if current_state == AdminStates.set_title_prices and is_admin(message.from_user.id):
        parts = text.replace(",", ".").split()
        if len(parts) != 5:
            await message.answer("Нужно 5 цен через пробел.", reply_markup=admin_back_kb())
            return
        vals = list(map(float, parts))
        set_title_prices_map({"newbie":vals[0],"bronze":vals[1],"silver":vals[2],"gold":vals[3],"platinum":vals[4]})
        await state.clear()
        await message.answer("✅ Прайсы титулов обновлены.", reply_markup=admin_back_kb())
        return

    if current_state == AdminStates.set_rep_settings and is_admin(message.from_user.id):
        parts = text.split()
        if len(parts) != 7:
            await message.answer("Нужно 7 значений.", reply_markup=admin_back_kb())
            return
        set_setting("rep_bonus_perfect", parts[0]); set_setting("rep_bonus_reliable", parts[1]); set_setting("rep_bonus_stable", parts[2]); set_setting("rep_bonus_risky", parts[3]); set_setting("rep_bonus_problem", parts[4]); set_setting("newbie_priority_bonus", parts[5]); set_setting("rep_influence_enabled", parts[6])
        await state.clear()
        await message.answer("✅ Настройки репутации обновлены.", reply_markup=admin_back_kb())
        return

    if current_state == AdminStates.aprofile_date and is_admin(message.from_user.id):
        data = await state.get_data()
        uid = int(data.get("aprof_uid", 0))
        mode = data.get("aprof_mode", "open")
        try:
            target = datetime.strptime(text.strip(), "%d.%m.%Y").date()
        except Exception:
            await message.answer("Неверная дата. Формат: 31.03.2026", reply_markup=admin_back_kb())
            return
        with db_conn() as conn:
            rows = conn.execute("SELECT * FROM requests WHERE user_id=? ORDER BY id DESC", (uid,)).fetchall()
        rows = [r for r in rows if datetime.fromisoformat(r["created_at"]).astimezone(MSK).date() == target]
        if not rows:
            out = "Номера за эту дату не найдены."
        else:
            lines = [f"Номера пользователя за {text.strip()}\n"]
            for r in rows:
                lines.append(f"#{display_req_id(r)} | {r['phone']} | {r['method']} | {request_public_status(r['status'])}")
            out = "\n".join(lines)
        await state.clear()
        if mode == "txt":
            await message.answer_document(BufferedInputFile(out.encode("utf-8"), filename=f"user_{uid}_{target}.txt"), caption="📄 Выгрузка номеров")
        else:
            await message.answer(out, reply_markup=admin_back_kb())
        return

    if current_state == AdminStates.remove_my_queue and message.chat.type == ChatType.PRIVATE:
        req_num = text.strip().lstrip("#")
        if not req_num.isdigit():
            await message.answer("Пришли номер заявки.", reply_markup=back_kb())
            return
        with db_conn() as conn:
            rows = conn.execute("SELECT * FROM requests WHERE user_id=? ORDER BY id DESC", (message.from_user.id,)).fetchall()
        target = None
        for r in rows:
            if str(display_req_id(r)) == req_num and r["status"] in ("queued","qr_skipped"):
                target = r
                break
        if not target:
            await message.answer("Заявка не найдена или уже взята.", reply_markup=back_kb())
            return
        update_request(target["id"], status="closed_cancelled")
        await state.clear()
        await message.answer(f"✅ Заявка #{req_num} убрана из очереди.", reply_markup=back_kb())
        return

    if current_state == AdminStates.add_admin and is_admin(message.from_user.id):
        row = user_by_any(text)
        uid = int(row["user_id"]) if row else (int(text) if text.isdigit() else None)
        if uid is None:
            await message.answer("Пользователь не найден.")
            return
        add_admin_db(uid, "admin")
        await state.clear()
        await message.answer(f"✅ Админ добавлен: <code>{uid}</code>", reply_markup=admin_back_kb())
        return

    if current_state == AdminStates.del_admin and is_admin(message.from_user.id):
        row = user_by_any(text)
        uid = int(row["user_id"]) if row else (int(text) if text.isdigit() else None)
        if uid is None:
            await message.answer("Пользователь не найден.")
            return
        ok = del_admin_db(uid)
        await state.clear()
        await message.answer((f"✅ Админ удалён: <code>{uid}</code>" if ok is not False else "⛔ Супер-админа удалить нельзя."), reply_markup=admin_back_kb())
        return

    if current_state == AdminStates.add_group and is_admin(message.from_user.id):
        if not text.lstrip("-").isdigit():
            await message.answer("Нужен id группы.")
            return
        enable_group(int(text))
        await state.clear()
        await message.answer(f"✅ Группа добавлена: <code>{int(text)}</code>", reply_markup=admin_back_kb())
        return

    if current_state == AdminStates.del_group and is_admin(message.from_user.id):
        if not text.lstrip("-").isdigit():
            await message.answer("Нужен id группы.")
            return
        gid = int(text)
        disable_group(gid)
        with db_conn() as conn:
            conn.execute("DELETE FROM topics_work WHERE chat_id=?", (gid,))
        await state.clear()
        await message.answer(f"✅ Группа удалена: <code>{gid}</code>", reply_markup=admin_back_kb())
        return

    if current_state == AdminStates.ban_user and is_admin(message.from_user.id):
        row = user_by_any(text)
        if not row:
            await message.answer("Пользователь не найден.")
            return
        ban_user_db(int(row["user_id"]))
        await state.clear()
        await message.answer(f"✅ Пользователь забанен: <code>{row['user_id']}</code>", reply_markup=admin_back_kb())
        return

    if current_state == AdminStates.unban_user and is_admin(message.from_user.id):
        row = user_by_any(text)
        uid = int(row["user_id"]) if row else (int(text) if text.isdigit() else None)
        if uid is None:
            await message.answer("Пользователь не найден.")
            return
        unban_user_db(uid)
        await state.clear()
        await message.answer(f"✅ Пользователь разбанен: <code>{uid}</code>", reply_markup=admin_back_kb())
        return

    if current_state == AdminStates.remove_numbers and is_admin(message.from_user.id):
        row = user_by_any(text)
        if not row:
            await message.answer("Пользователь не найден.")
            return
        with db_conn() as conn:
            conn.execute("UPDATE requests SET status='closed_cancelled' WHERE user_id=? AND status IN ('queued','qr_skipped')", (row["user_id"],))
        await state.clear()
        await message.answer("✅ Номера пользователя убраны.", reply_markup=admin_back_kb())
        return

    if current_state == AdminStates.add_balance and is_admin(message.from_user.id):
        parts = text.split()
        if len(parts) != 2:
            await message.answer("Формат: @тег сумма")
            return
        row = user_by_any(parts[0])
        if not row:
            await message.answer("Пользователь не найден.")
            return
        add_balance(int(row["user_id"]), float(parts[1].replace(",", ".")))
        await state.clear()
        await message.answer("✅ Баланс пополнен.", reply_markup=admin_back_kb())
        return

    if current_state == AdminStates.sub_balance and is_admin(message.from_user.id):
        parts = text.split()
        if len(parts) != 2:
            await message.answer("Формат: @тег сумма")
            return
        row = user_by_any(parts[0])
        if not row:
            await message.answer("Пользователь не найден.")
            return
        sub_balance(int(row["user_id"]), float(parts[1].replace(",", ".")))
        await state.clear()
        await message.answer("✅ Баланс уменьшен.", reply_markup=admin_back_kb())
        return

    if current_state == AdminStates.user_lookup and is_admin(message.from_user.id):
        row = user_by_any(text)
        if not row:
            await message.answer("Пользователь не найден.")
            return
        with db_conn() as conn:
            total = conn.execute("SELECT COUNT(*) c FROM requests WHERE user_id=?", (row["user_id"],)).fetchone()["c"]
            paid = conn.execute("SELECT COUNT(*) c FROM requests WHERE user_id=? AND credited=1", (row["user_id"],)).fetchone()["c"]
        await state.clear()
        await message.answer(
            "👤 <b>Пользователь</b>\n\n"
            f"ID: <code>{row['user_id']}</code>\n"
            f"Username: <b>{('@' + row['username']) if row['username'] else '—'}</b>\n"
            f"Баланс: <b>{fmt_money(float(row['balance']))}</b>\n"
            f"На выводе: <b>{fmt_money(float(row['pending']))}</b>\n"
            f"Заявок: <b>{total}</b>\n"
            f"Оплачено: <b>{paid}</b>"
        )
        return

    if current_state == AdminStates.set_profit and is_admin(message.from_user.id):
        set_profit_per_number(float(text.replace(",", ".")))
        await state.clear()
        await message.answer("✅ Профит обновлён.", reply_markup=admin_back_kb())
        return

    if current_state == AdminStates.delete_queue_phone and is_admin(message.from_user.id):
        phone = normalize_phone(text)
        if not phone:
            await message.answer("Неверный формат номера.")
            return
        with db_conn() as conn:
            cur = conn.execute("UPDATE requests SET status='closed_cancelled' WHERE phone=? AND status IN ('queued','qr_skipped')", (phone,))
        await state.clear()
        await message.answer("✅ Номер удалён из очереди." if cur.rowcount else "Номер не найден в очереди.")
        return

    if current_state == WithdrawStates.waiting_amount:
        raw_amount = text.strip().replace(" ", "").replace(",", ".")
        try:
            amount = float(raw_amount)
        except ValueError:
            await message.answer("Введите сумму числом. Пример: <b>10</b> или <b>10.5</b>")
            return

        available = get_balance(message.from_user.id) - get_pending(message.from_user.id)
        if amount <= 0:
            await message.answer("Сумма должна быть больше 0.")
            return
        if amount > available:
            await message.answer(
                "Недостаточно доступного баланса.\n"
                f"Доступно к выводу: <b>{fmt_money(available)}</b>"
            )
            return

        user = get_user(message.from_user.id)
        wid = None
        try:
            with db_conn() as conn:
                conn.execute("UPDATE users SET pending=pending+? WHERE user_id=?", (amount, message.from_user.id))
                cur = conn.execute("""
                    INSERT INTO withdrawals(user_id, username, amount, status, created_at)
                    VALUES(?, ?, ?, 'pending', ?)
                """, (message.from_user.id, user["username"], amount, datetime.now(timezone.utc).isoformat()))
                wid = cur.lastrowid

            await bot.send_message(
                WITHDRAWALS_CHANNEL_ID,
                "💸 <b>Заявка на вывод | DIAMOND VAULT</b>\n\n"
                f"ID заявки: <b>#{wid}</b>\n"
                f"Пользователь: <code>{message.from_user.id}</code>\n"
                f"Тег: <b>{('@' + user['username']) if user['username'] else '—'}</b>\n"
                f"Сумма: <b>{fmt_money(amount)}</b>",
                reply_markup=withdraw_admin_kb(wid),
            )
        except Exception as e:
            logger.exception(
                "Ошибка создания/отправки заявки на вывод: user_id=%s amount=%s wid=%s error=%s",
                message.from_user.id, amount, wid, e
            )
            with db_conn() as conn:
                conn.execute("UPDATE users SET pending=pending-? WHERE user_id=?", (amount, message.from_user.id))
                if wid is not None:
                    conn.execute("DELETE FROM withdrawals WHERE id=? AND status='pending'", (wid,))
            await message.answer(
                "❌ Не удалось создать заявку на вывод. Попробуйте ещё раз чуть позже."
            )
            return

        await state.clear()
        await message.answer(
            "✅ <b>ЗАЯВКА ПРИНЯТА | DIAMOND VAULT</b>\n\n"
            "Запрос на вывод успешно создан:\n\n"
            "━━━━━━━━━━━━━━\n"
            f"💸 Сумма: <b>{fmt_money(amount)}</b>\n"
            "📍 Статус: на рассмотрении\n"
            "━━━━━━━━━━━━━━\n\n"
            "Ожидайте обработки заявки.\n"
            "Средства будут отправлены после подтверждения ✔️"
        )
        return

    if current_state == SubmitStates.waiting_phone_qr:
        block_msg = blocked_for_dead_numbers(message.from_user.id)
        if block_msg:
            await state.clear()
            await message.answer(block_msg)
            return
        phone = normalize_phone(text)
        if not phone:
            await message.answer("❌ Неверный формат номера.")
            return
        blocked_until = active_phone_block(phone)
        if blocked_until:
            await message.answer(f"⛔️ Этот номер уже в блоке до <b>{blocked_until}</b> (МСК).")
            return
        rid = create_request(message.from_user, phone, "QR")
        req = get_request(rid)
        await state.clear()
        await message.answer(
            "✅ <b>Номер принят</b>\n\n"
            f"{request_text_short(req)}\n\n"
            "Когда номер возьмут в работу — я пришлю уведомление."
        )
        queue_tasks[rid] = asyncio.create_task(queue_afk_timer(rid))
        await send_log(f"📥 Новая заявка #{rid} | {phone} | QR | user {message.from_user.id}")
        return

    if current_state == SubmitStates.waiting_phone_code:
        block_msg = blocked_for_dead_numbers(message.from_user.id)
        if block_msg:
            await state.clear()
            await message.answer(block_msg)
            return
        phone = normalize_phone(text)
        if not phone:
            await message.answer("❌ Неверный формат номера.")
            return
        blocked_until = active_phone_block(phone)
        if blocked_until:
            await message.answer(f"⛔️ Этот номер уже в блоке до <b>{blocked_until}</b> (МСК).")
            return
        rid = create_request(message.from_user, phone, "КОД")
        req = get_request(rid)
        await state.clear()
        await message.answer(
            "✅ <b>Номер принят</b>\n\n"
            f"{request_text_short(req)}\n\n"
            "Когда номер возьмут в работу — я пришлю уведомление."
        )
        queue_tasks[rid] = asyncio.create_task(queue_afk_timer(rid))
        await send_log(f"📥 Новая заявка #{rid} | {phone} | КОД | user {message.from_user.id}")
        return

    if message.chat.type in {ChatType.GROUP, ChatType.SUPERGROUP}:
        if is_qr_trigger(text):
            if await issue_next(message, "QR"):
                return
        if is_code_trigger(text):
            if await issue_next(message, "КОД"):
                return

    with db_conn() as conn:
        req = conn.execute("SELECT * FROM requests WHERE user_id=? AND status='awaiting_sms' ORDER BY id DESC LIMIT 1", (message.from_user.id,)).fetchone()
    if req:
        if not CODE_RE.match(text):
            await message.answer("❌ Код должен содержать ровно 6 цифр.")
            return
        update_request(req["id"], sms_code=text, status="code_received")
        await message.answer("✅ <b>Код принят. Ожидай подтверждения!</b>")
        await group_send(
            req,
            "🔑 <b>Код получен по заявке</b>\n\n"
            f"Заявка: <b>#{req['id']}</b>\n"
            f"Номер: <code>{req['phone']}</code>\n"
            f"Код: <code>{text}</code>\n\n"
            "Выбери следующий шаг 👇",
            reply_markup=operator_result_kb(req["id"]),
        )
        await send_log(f"🔑 Код получен #{req['id']} | {req['phone']} | оператор {req['claimed_by']}")
        return

    with db_conn() as conn:
        pass_req = conn.execute("SELECT * FROM requests WHERE user_id=? AND password_requested=1 AND status IN ('qr_scanned', 'code_received') ORDER BY id DESC LIMIT 1", (message.from_user.id,)).fetchone()
    if pass_req and message.chat.type == ChatType.PRIVATE:
        t = password_tasks.pop(pass_req["id"], None)
        if t:
            t.cancel()
        update_request(pass_req["id"], password_requested=0, password_value=text)
        await message.answer("✅ <b>Пароль принят. Ожидай подтверждения!</b>")
        await group_send(
            pass_req,
            "🔐 <b>Пароль получен по заявке</b>\n\n"
            f"Заявка: <b>#{pass_req['id']}</b>\n"
            f"Номер: <code>{pass_req['phone']}</code>\n"
            f"Пароль: <code>{html.escape(text)}</code>\n\n"
            "Выбери следующий шаг 👇",
            reply_markup=operator_result_kb(pass_req["id"]),
        )
        await send_log(f"🔐 Пароль получен #{pass_req['id']} | {pass_req['phone']}")
        return

    if message.chat.type in {ChatType.GROUP, ChatType.SUPERGROUP} and message.photo:
        current_thread_id = getattr(message, "message_thread_id", None)
        with db_conn() as conn:
            req = conn.execute("""
                SELECT * FROM requests
                WHERE claimed_by=? AND status='awaiting_qr' AND issue_group_id=?
                ORDER BY id DESC LIMIT 10
            """, (message.from_user.id, message.chat.id)).fetchall()
        req = next((r for r in req if r["issue_thread_id"] == current_thread_id), None)
        if req:
            update_request(req["id"], qr_file_id=message.photo[-1].file_id, status="qr_sent_to_user")
            await message.answer(
                "✅ <b>QR принят</b>\n\n"
                f"Заявка: <b>#{req['id']}</b>\n"
                f"Номер: <code>{req['phone']}</code>\n\n"
                "QR отправлен пользователю. Ждём его реакцию."
            )
            await bot.send_photo(
                req["user_id"],
                photo=message.photo[-1].file_id,
                caption=(
                    "📷 <b>QR получен</b>\n\n"
                    f"Заявка: <b>#{req['id']}</b>\n"
                    f"Номер: <code>{req['phone']}</code>\n\n"
                    "Проверь QR и выбери действие ниже."
                ),
                reply_markup=qr_user_result_kb(req["id"]),
            )
            await send_log(f"📷 QR отправлен пользователю #{req['id']} | {req['phone']} | оператор {message.from_user.id}")
            return

# =========================
# MAIN
# =========================
async def restore_runtime():
    with db_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM requests
            WHERE status IN ('queued','qr_skipped','awaiting_activity','awaiting_sms','awaiting_qr','success_hold')
        """).fetchall()
    for req in rows:
        rid = req["id"]
        if req["status"] in ("queued", "qr_skipped"):
            queue_tasks[rid] = asyncio.create_task(queue_afk_timer(rid))
        elif req["password_requested"] == 1 and req["status"] in ("qr_scanned", "code_received"):
            password_tasks[rid] = asyncio.create_task(password_timer(rid))
        elif req["status"] == "awaiting_activity":
            activity_tasks[rid] = asyncio.create_task(activity_timer(rid))
        elif req["status"] == "awaiting_sms":
            code_tasks[rid] = asyncio.create_task(code_timer(rid))
        elif req["status"] == "awaiting_qr":
            qr_tasks[rid] = asyncio.create_task(qr_timer(rid))
        elif req["status"] == "success_hold":
            hold_tasks[rid] = asyncio.create_task(hold_timer(rid))

async def main():
    migrate_local_files_to_volume()
    db_init()
    await restore_runtime()
    await send_log("🚀 DIAMOND VAULT started")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())