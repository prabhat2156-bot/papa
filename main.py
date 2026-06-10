import os, asyncio, logging, time, sys, shutil, zipfile, re, secrets, base64, gc
from datetime import datetime, timezone
from asyncio import create_subprocess_exec
from asyncio.subprocess import PIPE

import psutil
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
)
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ConversationHandler, ContextTypes,
    filters
)

# ─────────────────────────────────────────────────────────────
# Live progress animation
# ─────────────────────────────────────────────────────────────

_PROGRESS_BAR_WIDTH = 20
_PROGRESS_EDIT_INTERVAL = 2.0


def _progress_fmt_time(secs: float) -> str:
    s = int(secs)
    m, s = divmod(s, 60)
    return f"{m:02d}:{s:02d}"


def _progress_bar(pct: int, frame_idx: int, width: int = _PROGRESS_BAR_WIDTH) -> str:
    filled = int(width * pct / 100)
    remaining = width - filled
    bar_chars = ["█"] * filled + ["░"] * remaining
    if remaining > 0 and pct < 100:
        pulse_pos = filled + (frame_idx % remaining)
        bar_chars[pulse_pos] = "▒"
    return "[" + "".join(bar_chars) + f"] {pct}%"


class LiveProgress:
    def __init__(self, message, title: str = "Working"):
        self.message = message
        self.title = title
        self._running = False
        self._task = None
        self._start_ts = 0.0
        self._estimated = 60.0
        self._last_text = ""

    def _render(self, pct: int, frame_idx: int, elapsed: float, status: str) -> str:
        bar = _progress_bar(pct, frame_idx)
        return (
            f"⚙️ *{self.title}*\n\n"
            f"⏳ {status}\n"
            f"`{bar}`\n"
            f"⏱ {_progress_fmt_time(elapsed)}"
        )

    async def _safe_edit(self, text: str):
        if text == self._last_text:
            return
        self._last_text = text
        try:
            await self.message.edit_text(text, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            pass

    async def start(self, status: str = "Starting..."):
        self._start_ts = time.time()
        await self._safe_edit(self._render(0, 0, 0.0, status))

    async def animate(self, estimated_seconds: float = 60.0, status: str = "Working..."):
        self._running = True
        self._estimated = max(5.0, estimated_seconds)
        self._start_ts = time.time()
        frame = 0
        try:
            while self._running:
                elapsed = time.time() - self._start_ts
                pct = min(95, int(elapsed / self._estimated * 100))
                await self._safe_edit(self._render(pct, frame, elapsed, status))
                frame += 1
                await asyncio.sleep(_PROGRESS_EDIT_INTERVAL)
        except asyncio.CancelledError:
            pass

    def run_in_background(self, estimated_seconds: float = 60.0, status: str = "Working..."):
        self._task = asyncio.create_task(self.animate(estimated_seconds, status))
        return self._task

    async def stop(self, success: bool = True, final_text: str = "Done"):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass
        elapsed = time.time() - self._start_ts
        if success:
            bar = _progress_bar(100, 0)
            text = (
                f"✅ *{self.title}*\n\n"
                f"{final_text}\n"
                f"`{bar}`\n"
                f"⏱ {_progress_fmt_time(elapsed)}"
            )
        else:
            text = (
                f"❌ *{self.title} — Failed*\n\n"
                f"{final_text}\n"
                f"⏱ {_progress_fmt_time(elapsed)}"
            )
        self._last_text = ""
        await self._safe_edit(text)

# ─────────────────────────────────────────────────────────────
# Bootstrap
# ─────────────────────────────────────────────────────────────
load_dotenv()
logging.basicConfig(
    format="%(asctime)s — %(name)s — %(levelname)s — %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOT_TOKEN       = os.getenv("BOT_TOKEN", "")
OWNER_ID        = int(os.getenv("OWNER_ID", "0"))
OWNER_USERNAME  = os.getenv("OWNER_USERNAME", "owner")
MONGODB_URI     = os.getenv("MONGODB_URI", "")
DATABASE_NAME   = os.getenv("DATABASE_NAME", "god_madara_hosting")
BASE_URL        = os.getenv("BASE_URL", "http://localhost:8080")
PORT            = int(os.getenv("PORT", "8080"))

# Local SQLite DB path
LOCAL_DB_PATH   = os.path.join(os.path.dirname(__file__), "local_data.db")

# Primary MongoDB
mongo_client = AsyncIOMotorClient(MONGODB_URI)
db           = mongo_client[DATABASE_NAME]
users_col    = db["users"]
projects_col = db["projects"]
tokens_col   = db["file_tokens"]
backups_col  = db["backups"]
settings_col = db["settings"]   # Bot-wide settings (lock, maintenance, active_db)

# ─────────────────────────────────────────────────────────────
# Multiple Extra Databases (UNLIMITED)
# ─────────────────────────────────────────────────────────────
extra_clients = []
extra_dbs     = []

def _load_extra_databases():
    seen_names = set()
    legacy_uri  = os.getenv("MONGODB_URI_2", "")
    legacy_name = os.getenv("DATABASE_NAME_2", "")
    if legacy_uri and legacy_name and legacy_name not in seen_names:
        try:
            client = AsyncIOMotorClient(legacy_uri)
            extra_clients.append(client)
            extra_dbs.append({"name": legacy_name, "db": client[legacy_name], "client": client})
            seen_names.add(legacy_name)
            logger.info(f"✅ Extra DB connected (legacy): {legacy_name}")
        except Exception as e:
            logger.error(f"❌ Failed to connect legacy DB: {e}")

    for i in range(1, 51):
        uri  = os.getenv(f"MONGODB_URI_{i}", "")
        name = os.getenv(f"DATABASE_NAME_{i}", "")
        if not uri or not name or name in seen_names:
            continue
        try:
            client = AsyncIOMotorClient(uri)
            extra_clients.append(client)
            extra_dbs.append({"name": name, "db": client[name], "client": client})
            seen_names.add(name)
            logger.info(f"✅ Extra DB #{i} connected: {name}")
        except Exception as e:
            logger.error(f"❌ Failed to connect DB #{i} ({name}): {e}")

_load_extra_databases()
logger.info(f"📊 Total extra databases connected: {len(extra_dbs)}")

db_2 = extra_dbs[0]["db"] if extra_dbs else None
mongo_client_2 = extra_clients[0] if extra_clients else None
MONGODB_URI_2 = os.getenv("MONGODB_URI_2", "")
DATABASE_NAME_2 = os.getenv("DATABASE_NAME_2", "")

def get_extra_db_by_name(name: str):
    for entry in extra_dbs:
        if entry["name"] == name:
            return entry["db"]
    return None

def list_extra_db_names() -> list:
    return [e["name"] for e in extra_dbs]

# ─────────────────────────────────────────────────────────────
# Sharding helpers
# ─────────────────────────────────────────────────────────────

def all_backup_cols() -> list:
    return [backups_col] + [e["db"]["backups"] for e in extra_dbs]

def all_db_names() -> list:
    return [DATABASE_NAME] + [e["name"] for e in extra_dbs]

def pick_backup_col(user_id: int, project_name: str):
    import hashlib
    cols  = all_backup_cols()
    names = all_db_names()
    if len(cols) == 1:
        return (names[0], cols[0])
    key = f"{user_id}:{project_name}".encode("utf-8")
    h = int(hashlib.md5(key).hexdigest(), 16)
    idx = h % len(cols)
    return (names[idx], cols[idx])

BOT_START_TIME = time.time()
notification_bot = None

# ─────────────────────────────────────────────────────────────
# ⚙️ Bot Settings (lock, maintenance, active_db)
# ─────────────────────────────────────────────────────────────

_settings_cache: dict = {}
_settings_cache_ts: float = 0.0
_SETTINGS_CACHE_TTL = 10.0  # seconds

async def get_bot_settings() -> dict:
    global _settings_cache, _settings_cache_ts
    now = time.time()
    if now - _settings_cache_ts < _SETTINGS_CACHE_TTL and _settings_cache:
        return _settings_cache
    doc = await settings_col.find_one({"_id": "bot_settings"})
    if not doc:
        doc = {
            "_id": "bot_settings",
            "bot_locked": False,
            "maintenance_mode": False,
            "active_db": "mongodb",  # "mongodb" or "local"
        }
        try:
            await settings_col.insert_one(doc)
        except Exception:
            pass
    _settings_cache = doc
    _settings_cache_ts = now
    return doc

async def set_bot_setting(key: str, value) -> None:
    global _settings_cache, _settings_cache_ts
    await settings_col.update_one(
        {"_id": "bot_settings"},
        {"$set": {key: value}},
        upsert=True,
    )
    _settings_cache = {}
    _settings_cache_ts = 0.0

async def is_bot_locked() -> bool:
    s = await get_bot_settings()
    return bool(s.get("bot_locked", False))

async def is_maintenance_mode() -> bool:
    s = await get_bot_settings()
    return bool(s.get("maintenance_mode", False))

async def get_active_db() -> str:
    s = await get_bot_settings()
    return s.get("active_db", "mongodb")

# ─────────────────────────────────────────────────────────────
# 🗄️ Local SQLite Database
# ─────────────────────────────────────────────────────────────

import sqlite3 as _sqlite3

def init_local_db():
    """Create SQLite DB and tables if they don't exist."""
    conn = _sqlite3.connect(LOCAL_DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        is_premium INTEGER DEFAULT 0,
        premium_expiry TEXT,
        is_banned INTEGER DEFAULT 0,
        is_admin INTEGER DEFAULT 0,
        joined_date TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS projects (
        user_id INTEGER,
        name TEXT,
        run_command TEXT,
        created_date TEXT,
        last_run TEXT,
        exit_code INTEGER,
        status TEXT DEFAULT 'stopped',
        pid INTEGER,
        admin_stopped INTEGER DEFAULT 0,
        auto_restart INTEGER DEFAULT 1,
        restart_count INTEGER DEFAULT 0,
        last_restart_at TEXT,
        started_at TEXT,
        locked INTEGER DEFAULT 0,
        PRIMARY KEY (user_id, name)
    )""")
    conn.commit()
    conn.close()
    logger.info(f"✅ Local SQLite DB ready at {LOCAL_DB_PATH}")

async def migrate_mongo_to_local() -> tuple:
    """Copy all data from MongoDB to local SQLite. Returns (users_count, projects_count)."""
    try:
        init_local_db()
        loop = asyncio.get_event_loop()

        all_users = await users_col.find({}).to_list(length=100000)
        all_projects = await projects_col.find({}).to_list(length=100000)

        def _do_migrate(users, projects):
            conn = _sqlite3.connect(LOCAL_DB_PATH)
            c = conn.cursor()
            for u in users:
                expiry = u.get("premium_expiry")
                expiry_str = expiry.isoformat() if expiry else None
                c.execute("""INSERT OR REPLACE INTO users
                    (user_id, username, first_name, is_premium, premium_expiry,
                     is_banned, is_admin, joined_date)
                    VALUES (?,?,?,?,?,?,?,?)""",
                    (u["user_id"], u.get("username",""), u.get("first_name",""),
                     1 if u.get("is_premium") else 0,
                     expiry_str,
                     1 if u.get("is_banned") else 0,
                     1 if u.get("is_admin") else 0,
                     u.get("joined_date", datetime.now(timezone.utc)).isoformat()
                         if hasattr(u.get("joined_date"), "isoformat") else str(u.get("joined_date",""))
                    )
                )
            for p in projects:
                def _ts(field):
                    v = p.get(field)
                    return v.isoformat() if v and hasattr(v, "isoformat") else None
                c.execute("""INSERT OR REPLACE INTO projects
                    (user_id, name, run_command, created_date, last_run, exit_code,
                     status, pid, admin_stopped, auto_restart, restart_count,
                     last_restart_at, started_at, locked)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (p["user_id"], p["name"], p.get("run_command"),
                     _ts("created_date"), _ts("last_run"), p.get("exit_code"),
                     p.get("status","stopped"), p.get("pid"),
                     1 if p.get("admin_stopped") else 0,
                     1 if p.get("auto_restart", True) else 0,
                     p.get("restart_count", 0), _ts("last_restart_at"),
                     _ts("started_at"),
                     1 if p.get("locked") else 0
                    )
                )
            conn.commit()
            conn.close()
            return len(users), len(projects)

        uc, pc = await loop.run_in_executor(None, _do_migrate, all_users, all_projects)
        logger.info(f"✅ Migration Mongo→Local: {uc} users, {pc} projects")
        return uc, pc
    except Exception as e:
        logger.error(f"Migration Mongo→Local failed: {e}")
        raise

async def migrate_local_to_mongo() -> tuple:
    """Copy all data from local SQLite back to MongoDB. Returns (users_count, projects_count)."""
    try:
        loop = asyncio.get_event_loop()

        def _read_local():
            conn = _sqlite3.connect(LOCAL_DB_PATH)
            conn.row_factory = _sqlite3.Row
            c = conn.cursor()
            users = [dict(r) for r in c.execute("SELECT * FROM users").fetchall()]
            projects = [dict(r) for r in c.execute("SELECT * FROM projects").fetchall()]
            conn.close()
            return users, projects

        local_users, local_projects = await loop.run_in_executor(None, _read_local)

        def _parse_dt(s):
            if not s:
                return None
            try:
                return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
            except Exception:
                return None

        for u in local_users:
            doc = {
                "user_id": u["user_id"],
                "username": u.get("username",""),
                "first_name": u.get("first_name",""),
                "is_premium": bool(u.get("is_premium",0)),
                "premium_expiry": _parse_dt(u.get("premium_expiry")),
                "is_banned": bool(u.get("is_banned",0)),
                "is_admin": bool(u.get("is_admin",0)),
                "joined_date": _parse_dt(u.get("joined_date")) or datetime.now(timezone.utc),
            }
            await users_col.update_one({"user_id": doc["user_id"]}, {"$set": doc}, upsert=True)

        for p in local_projects:
            doc = {
                "user_id": p["user_id"],
                "name": p["name"],
                "run_command": p.get("run_command"),
                "created_date": _parse_dt(p.get("created_date")) or datetime.now(timezone.utc),
                "last_run": _parse_dt(p.get("last_run")),
                "exit_code": p.get("exit_code"),
                "status": p.get("status","stopped"),
                "pid": p.get("pid"),
                "admin_stopped": bool(p.get("admin_stopped",0)),
                "auto_restart": bool(p.get("auto_restart",1)),
                "restart_count": p.get("restart_count",0),
                "last_restart_at": _parse_dt(p.get("last_restart_at")),
                "started_at": _parse_dt(p.get("started_at")),
                "locked": bool(p.get("locked",0)),
            }
            await projects_col.update_one(
                {"user_id": doc["user_id"], "name": doc["name"]}, {"$set": doc}, upsert=True
            )

        logger.info(f"✅ Migration Local→Mongo: {len(local_users)} users, {len(local_projects)} projects")
        return len(local_users), len(local_projects)
    except Exception as e:
        logger.error(f"Migration Local→Mongo failed: {e}")
        raise

# ─────────────────────────────────────────────────────────────
# Conversation states
# ─────────────────────────────────────────────────────────────
(
    NEW_PROJECT_NAME,
    NEW_PROJECT_FILES,
    EDIT_RUN_CMD,
    ADMIN_GIVE_PREMIUM_ID,
    ADMIN_REMOVE_PREMIUM_ID,
    ADMIN_TEMP_PREMIUM_ID,
    ADMIN_TEMP_PREMIUM_DUR,
    ADMIN_BAN_ID,
    ADMIN_UNBAN_ID,
    ADMIN_BROADCAST_MSG,
    ADMIN_SEND_USER_ID,
    ADMIN_SEND_USER_MSG,
    ENV_ADD_KEY,
    ENV_ADD_VALUE,
    ENV_EDIT_VALUE,
    ADMIN_ADD_ADMIN_ID,
    ADMIN_REMOVE_ADMIN_ID,
) = range(17)

FREE_LIMIT    = 1
PREMIUM_LIMIT = 9999

PROJECTS_ROOT = os.path.join(os.path.dirname(__file__), "projects")
os.makedirs(PROJECTS_ROOT, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def project_dir(user_id: int, project_name: str) -> str:
    return os.path.join(PROJECTS_ROOT, str(user_id), project_name)

def fmt_bytes(b: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"

def fmt_uptime(seconds: float) -> str:
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h}h {m}m {sec}s" if h else (f"{m}m {sec}s" if m else f"{sec}s")

def fmt_duration(total_seconds: float) -> str:
    return fmt_uptime(total_seconds)

def escape_md(text: str) -> str:
    """Escape Markdown v1 special characters."""
    for ch in ('_', '*', '`', '['):
        text = str(text).replace(ch, f'\\{ch}')
    return text

async def safe_edit(query, text: str, reply_markup=None, parse_mode=ParseMode.MARKDOWN):
    try:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except BadRequest as e:
        logger.warning(f"safe_edit BadRequest: {e}")
        try:
            await query.edit_message_text(text, reply_markup=reply_markup)
        except Exception:
            pass
    except Exception as e:
        logger.error(f"safe_edit error: {e}")

async def ensure_user(user):
    await users_col.update_one(
        {"user_id": user.id},
        {"$setOnInsert": {
            "user_id":       user.id,
            "username":      user.username or "",
            "first_name":    user.first_name or "",
            "is_premium":    False,
            "premium_expiry": None,
            "is_banned":     False,
            "is_admin":      False,
            "joined_date":   datetime.now(timezone.utc),
        }},
        upsert=True,
    )
    await users_col.update_one(
        {"user_id": user.id},
        {"$set": {
            "username":   user.username or "",
            "first_name": user.first_name or "",
        }},
    )

async def check_premium_expiry(user_id: int):
    """Strip premium if expired. Also lock extra projects for expired premium users."""
    doc = await users_col.find_one({"user_id": user_id})
    if not doc:
        return

    was_premium = doc.get("is_premium", False)
    expiry = doc.get("premium_expiry")

    if was_premium and expiry:
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        if expiry < datetime.now(timezone.utc):
            # Premium expired — remove it
            await users_col.update_one(
                {"user_id": user_id},
                {"$set": {"is_premium": False, "premium_expiry": None}},
            )
            # Lock all projects except the first one (by created_date)
            await _lock_extra_projects_on_expiry(user_id)
            logger.info(f"Premium expired for user {user_id}. Extra projects locked.")

async def _lock_extra_projects_on_expiry(user_id: int):
    """When premium expires, keep only the 1st project (by creation date) unlocked. Stop & lock the rest."""
    all_projs = await projects_col.find({"user_id": user_id}).sort("created_date", 1).to_list(length=1000)
    if len(all_projs) <= FREE_LIMIT:
        return

    # First project stays free
    first_proj = all_projs[0]["name"]

    for i, p in enumerate(all_projs):
        if i == 0:
            # Unlock the first project
            await projects_col.update_one(
                {"user_id": user_id, "name": p["name"]},
                {"$set": {"locked": False}},
            )
            continue

        # Lock and stop all others
        was_running = p.get("status") == "running"
        await projects_col.update_one(
            {"user_id": user_id, "name": p["name"]},
            {"$set": {"locked": True}},
        )
        if was_running:
            await kill_project(user_id, p["name"])
            # Notify user
            if notification_bot:
                try:
                    await notification_bot.send_message(
                        chat_id=user_id,
                        text=(
                            f"⚠️ *Premium Expired*\n\n"
                            f"Your premium has expired. Project `{p['name']}` has been stopped and locked.\n"
                            f"Only `{first_proj}` remains active.\n\n"
                            f"Upgrade to Premium to unlock all your projects!"
                        ),
                        parse_mode=ParseMode.MARKDOWN,
                    )
                except Exception:
                    pass

async def get_user(user_id: int):
    return await users_col.find_one({"user_id": user_id})

async def is_banned(user_id: int) -> bool:
    doc = await get_user(user_id)
    return bool(doc and doc.get("is_banned"))

async def is_premium(user_id: int) -> bool:
    await check_premium_expiry(user_id)
    doc = await get_user(user_id)
    return bool(doc and doc.get("is_premium"))

async def is_admin(user_id: int) -> bool:
    doc = await get_user(user_id)
    return bool(doc and doc.get("is_admin"))

async def is_owner_or_admin(user_id: int) -> bool:
    return user_id == OWNER_ID or await is_admin(user_id)

def owner_only(func):
    import functools
    @functools.wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if uid != OWNER_ID:
            if update.callback_query:
                await update.callback_query.answer("⛔ Owner only", show_alert=True)
            return
        return await func(update, context)
    return wrapper

def admin_or_owner(func):
    import functools
    @functools.wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if not await is_owner_or_admin(uid):
            if update.callback_query:
                await update.callback_query.answer("⛔ Admin only", show_alert=True)
            return
        return await func(update, context)
    return wrapper

async def project_count(user_id: int) -> int:
    return await projects_col.count_documents({"user_id": user_id})

async def get_project(user_id: int, name: str):
    return await projects_col.find_one({"user_id": user_id, "name": name})

async def running_project_count() -> int:
    return await projects_col.count_documents({"status": "running"})

# ─────────────────────────────────────────────────────────────
# Log rotation helper
# ─────────────────────────────────────────────────────────────

MAX_LOG_SIZE = 4 * 1024 * 1024  # 4 MB

def rotate_log_if_needed(log_path: str):
    """If log > 4MB, keep only the last 2MB."""
    try:
        if os.path.exists(log_path) and os.path.getsize(log_path) > MAX_LOG_SIZE:
            with open(log_path, "rb") as f:
                f.seek(-2 * 1024 * 1024, 2)
                data = f.read()
            with open(log_path, "wb") as f:
                f.write(b"...[log rotated]...\n")
                f.write(data)
    except Exception:
        pass

# ─────────────────────────────────────────────────────────────
# /start
# ─────────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await ensure_user(user)
    await check_premium_expiry(user.id)

    # Maintenance mode — only owner allowed
    if await is_maintenance_mode() and user.id != OWNER_ID:
        await update.message.reply_text(
            "🔧 *Bot is under maintenance.*\n\nOnly the owner can use it right now. Please try again later.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if await is_banned(user.id):
        await update.message.reply_text("🚫 You are banned. Contact owner.", parse_mode=ParseMode.MARKDOWN)
        return

    doc      = await get_user(user.id)
    premium  = doc.get("is_premium", False)
    count    = await project_count(user.id)
    plan_lbl = "Premium ✨" if premium else "Free"
    limit_lbl = "∞" if premium else str(FREE_LIMIT)

    # Bot lock status line
    lock_line = ""
    if await is_bot_locked() and not premium and user.id != OWNER_ID:
        lock_line = "\n\n🔒 *Bot is locked.* New projects & running restricted to Premium users."

    text = (
        f"🌟 *Welcome to God Hosting Bot!*\n\n"
        f"👋 Hello {user.first_name}!\n\n"
        f"🚀 *What I can do:*\n"
        f"• Host python / node.js projects 24/7\n"
        f"• Web File Manager — Edit files in browser\n"
        f"• Auto-install requirements.txt/package.json\n"
        f"• Real-time logs & monitoring\n"
        f"• Free: 1 project | Premium: Unlimited\n\n"
        f"📊 *Your Status:*\n"
        f"👤 ID: `{user.id}`\n"
        f"💎 Plan: {plan_lbl}\n"
        f"📁 Projects: {count}/{limit_lbl}"
        f"{lock_line}\n\n"
        f"Choose an option below:"
    )

    kb = [
        [
            InlineKeyboardButton("🆕 New Project",   callback_data="new_project"),
            InlineKeyboardButton("📂 My Projects",   callback_data="my_projects"),
        ],
        [
            InlineKeyboardButton("💎 Premium",        callback_data="premium"),
            InlineKeyboardButton("📊 My Status",      callback_data="my_status"),
        ],
    ]
    if user.id == OWNER_ID or await is_admin(user.id):
        kb.append([InlineKeyboardButton("⚙️ Admin Panel", callback_data="admin_panel")])

    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)

async def cb_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    await ensure_user(user)
    await check_premium_expiry(user.id)

    if await is_maintenance_mode() and user.id != OWNER_ID:
        await safe_edit(query, "🔧 *Bot is under maintenance.* Only the owner can use it right now.", parse_mode=ParseMode.MARKDOWN)
        return

    if await is_banned(user.id):
        await safe_edit(query, "🚫 You are banned. Contact owner.", parse_mode=ParseMode.MARKDOWN)
        return

    doc      = await get_user(user.id)
    premium  = doc.get("is_premium", False)
    count    = await project_count(user.id)
    plan_lbl = "Premium ✨" if premium else "Free"
    limit_lbl = "∞" if premium else str(FREE_LIMIT)

    lock_line = ""
    if await is_bot_locked() and not premium and user.id != OWNER_ID:
        lock_line = "\n\n🔒 *Bot is locked.* New projects & running restricted to Premium users."

    text = (
        f"🌟 *Welcome to God Madara Hosting Bot!*\n\n"
        f"👋 Hello {user.first_name}!\n\n"
        f"🚀 *What I can do:*\n"
        f"• Host python / node.js projects\n"
        f"• Web File Manager — Edit files in browser\n"
        f"• Auto-install requirements.txt/package.json\n"
        f"• Real-time logs & monitoring\n"
        f"• Free: 1 project | Premium: Unlimited\n\n"
        f"📊 *Your Status:*\n"
        f"👤 ID: `{user.id}`\n"
        f"💎 Plan: {plan_lbl}\n"
        f"📁 Projects: {count}/{limit_lbl}"
        f"{lock_line}\n\n"
        f"Choose an option below:"
    )

    kb = [
        [
            InlineKeyboardButton("🆕 New Project",   callback_data="new_project"),
            InlineKeyboardButton("📂 My Projects",   callback_data="my_projects"),
        ],
        [
            InlineKeyboardButton("💎 Premium",        callback_data="premium"),
            InlineKeyboardButton("📊 My Status",      callback_data="my_status"),
        ],
    ]
    if user.id == OWNER_ID or await is_admin(user.id):
        kb.append([InlineKeyboardButton("⚙️ Admin Panel", callback_data="admin_panel")])

    await safe_edit(query, text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)

# ─────────────────────────────────────────────────────────────
# 📊 Bot Status
# ─────────────────────────────────────────────────────────────

@admin_or_owner
async def cb_bot_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        db_ping = 0
        try:
            t0 = time.time()
            await db.command("ping")
            db_ping = int((time.time() - t0) * 1000)
        except Exception:
            db_ping = -1

        api_ping = 0
        try:
            t1 = time.time()
            await context.bot.get_me()
            api_ping = int((time.time() - t1) * 1000)
        except Exception:
            api_ping = -1

        total_users = await users_col.count_documents({})
        premium_users = await users_col.count_documents({"is_premium": True})
        total_proj = await projects_col.count_documents({})
        running_proj = await running_project_count()

        cpu = psutil.cpu_percent(interval=None)
        ram = psutil.virtual_memory()
        disk = psutil.disk_usage("/")

        uptime = fmt_uptime(time.time() - BOT_START_TIME)
        py_ver = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"

        backup_line = "💾 Last Backup: `Never`\n"
        try:
            meta = await backups_col.find_one({"type": "backup_meta"})
            if meta:
                backup_time = meta["backed_up_at"].strftime("%Y-%m-%d %H:%M UTC")
                backup_size = fmt_bytes(meta.get("total_size", 0))
                backup_files = meta.get("total_files", 0)
                backup_line = (
                    f"💾 Last Backup: `{backup_time}`\n"
                    f"📦 Backup: `{backup_files}` files, `{backup_size}`\n"
                )
        except Exception:
            pass

        extra_db_lines = ""
        extra_online = 0
        per_db_stats = []

        try:
            primary_proj_count = await backups_col.count_documents({"type": "file_backup"})
        except Exception:
            primary_proj_count = 0
        per_db_stats.append((DATABASE_NAME, db_ping >= 0, primary_proj_count))

        for entry in extra_dbs:
            online = False
            count = 0
            try:
                await entry["db"].command("ping")
                online = True
                extra_online += 1
                count = await entry["db"]["backups"].count_documents({"type": "file_backup"})
            except Exception:
                pass
            per_db_stats.append((entry["name"], online, count))

        total_dbs = 1 + len(extra_dbs)
        total_online = (1 if db_ping >= 0 else 0) + extra_online

        if extra_dbs:
            extra_db_lines = f"\n*🗄 Storage Distribution:*\n"
            for name, online, count in per_db_stats:
                icon = "🟢" if online else "🔴"
                extra_db_lines += f"   {icon} `{name}`: `{count}` projects\n"

        db_ping_str = f"{db_ping}ms" if db_ping >= 0 else "Error"
        api_ping_str = f"{api_ping}ms" if api_ping >= 0 else "Error"

        # Bot settings status
        settings = await get_bot_settings()
        lock_icon = "🔒 ON" if settings.get("bot_locked") else "🔓 OFF"
        maint_icon = "🔧 ON" if settings.get("maintenance_mode") else "✅ OFF"
        active_db_label = "🗄 Local (SQLite)" if settings.get("active_db") == "local" else "☁️ MongoDB"
        local_db_exists = "✅" if os.path.exists(LOCAL_DB_PATH) else "❌"

        text = (
            f"📊 *Bot Dashboard*\n\n"
            f"👥 Total Users: `{total_users}`\n"
            f"💎 Premium Users: `{premium_users}`\n"
            f"📁 Total Projects: `{total_proj}`\n"
            f"🟢 Running Projects: `{running_proj}`\n"
            f"🔒 Bot Lock: `{lock_icon}`\n"
            f"🔧 Maintenance: `{maint_icon}`\n"
            f"💾 Active DB: `{active_db_label}`\n"
            f"🗄 Local DB File: `{local_db_exists}`\n"
            f"🔗 Connected DBs: `{total_online}/{total_dbs}`\n"
            f"{extra_db_lines}"
            f"🐍 Python: `{py_ver}`\n\n"
            f"💻 *System:*\n"
            f"├ CPU: `{cpu}%`\n"
            f"├ RAM: `{fmt_bytes(ram.used)}/{fmt_bytes(ram.total)}` (`{ram.percent}%`)\n"
            f"└ Disk: `{fmt_bytes(disk.used)}/{fmt_bytes(disk.total)}` (`{disk.percent}%`)\n\n"
            f"🏓 Bot Ping: `{api_ping_str}`\n"
            f"💾 DB Ping: `{db_ping_str}`\n"
            f"⏰ Uptime: `{uptime}`\n\n"
            f"*Backup Status:*\n"
            f"{backup_line}"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔃 Refresh", callback_data="bot_status"),
             InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")],
        ])
        await safe_edit(query, text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"bot_status error: {e}")
        await safe_edit(
            query,
            f"📊 *Bot Dashboard*\n\n⚠️ Error: {str(e)[:200]}\n\nBot is online!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔃 Retry", callback_data="bot_status"),
                 InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")],
            ]),
            parse_mode=ParseMode.MARKDOWN,
        )

# ─────────────────────────────────────────────────────────────
# 💎 Premium page
# ─────────────────────────────────────────────────────────────

async def cb_premium(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    if await is_banned(uid):
        await safe_edit(query, "🚫 You are banned. Contact owner.")
        return

    premium = await is_premium(uid)

    features = (
        f"*Free Plan:*\n"
        f"• 1 Project only\n"
        f"• File Manager (10 min)\n\n"
        f"*Premium Plan:*\n"
        f"• ✅ Unlimited projects\n"
        f"• ✅ Priority support\n"
        f"• ✅ Extended file manager\n"
        f"• ✅ Advanced monitoring\n"
        f"• ✅ Bot lock bypass\n\n"
    )

    if premium:
        text = (
            f"💎 *Premium Membership*\n\n"
            f"✨ *You are Premium!* ✨\n\n"
            + features +
            f"🌟 Premium is active!"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_start")]])
    else:
        text = (
            f"💎 *Premium Membership*\n\n"
            + features +
            f"To get Premium, contact the owner!"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📩 Contact Owner", url=f"https://t.me/{OWNER_USERNAME}")],
            [InlineKeyboardButton("🔙 Back",          callback_data="back_start")],
        ])

    await safe_edit(query, text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

# ─────────────────────────────────────────────────────────────
# 📂 My Projects
# ─────────────────────────────────────────────────────────────

async def cb_my_projects(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    if await is_banned(uid):
        await safe_edit(query, "🚫 You are banned. Contact owner.")
        return

    if await is_maintenance_mode() and uid != OWNER_ID:
        await safe_edit(query, "🔧 Bot is under maintenance. Please try later.")
        return

    projects = await projects_col.find({"user_id": uid}).to_list(length=100)
    if not projects:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_start")]])
        await safe_edit(query, "📂 *My Projects*\n\nYou have no projects yet.", reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        return

    kb_rows = []
    for p in projects:
        icon = "🟢" if p.get("status") == "running" else ("🔒" if p.get("locked") else "🔴")
        kb_rows.append([InlineKeyboardButton(f"{icon} {p['name']}", callback_data=f"proj:{p['name']}")])
    kb_rows.append([InlineKeyboardButton("🔙 Back", callback_data="back_start")])

    await safe_edit(query, "📂 *My Projects*\n\nSelect a project:", reply_markup=InlineKeyboardMarkup(kb_rows), parse_mode=ParseMode.MARKDOWN)

# ─────────────────────────────────────────────────────────────
# 📊 My Status
# ─────────────────────────────────────────────────────────────

async def cb_my_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id

    if await is_banned(uid):
        await safe_edit(query, "🚫 You are banned. Contact owner.")
        return

    projects = await projects_col.find({"user_id": uid}).to_list(length=100)
    doc      = await get_user(uid)
    premium  = doc.get("is_premium", False)
    count    = len(projects)
    limit_lbl = "∞" if premium else str(FREE_LIMIT)
    plan_lbl  = "💎 Premium" if premium else "🆓 Free"

    if not projects:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🆕 New Project", callback_data="new_project"),
                                    InlineKeyboardButton("🔙 Back", callback_data="back_start")]])
        await safe_edit(
            query,
            f"📊 *My Status*\n\n{plan_lbl} | 📁 0/{limit_lbl} projects\n\nNo projects yet.",
            reply_markup=kb,
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    lines = [f"📊 *My Projects Status*\n"]
    lines.append(f"{plan_lbl}  •  📁 {count}/{limit_lbl} projects\n")

    for i, p in enumerate(projects, 1):
        name   = p.get("name", "?")
        status = p.get("status", "stopped")
        cmd    = p.get("run_command") or "Not set"
        ar     = p.get("auto_restart", True)
        locked = p.get("locked", False)

        uptime_str = "—"
        if status == "running" and p.get("started_at"):
            try:
                started = p["started_at"]
                if started.tzinfo is None:
                    started = started.replace(tzinfo=timezone.utc)
                secs = (datetime.now(timezone.utc) - started).total_seconds()
                uptime_str = fmt_uptime(max(0, secs))
            except Exception:
                uptime_str = "—"

        exit_code = p.get("exit_code")

        if locked:
            status_line = f"🔒 Locked"
            extra_line  = f"   ├ ⚠️ Premium expired"
        elif status == "running":
            status_line = f"🟢 Running"
            extra_line  = f"   ├ ⏱ Uptime: `{uptime_str}`"
        elif exit_code is not None and exit_code != 0:
            status_line = f"🔴 Crashed"
            extra_line  = f"   ├ ⚠️ Exit Code: `{exit_code}`"
        else:
            status_line = f"🔴 Stopped"
            extra_line  = f"   ├ ⏱ Uptime: `—`"

        ar_line = "ON ✅" if ar else "OFF ❌"

        lines.append(
            f"{i}️⃣  *{escape_md(name)}*\n"
            f"   ├ {status_line}\n"
            f"{extra_line}\n"
            f"   ├ 🔁 Auto\\-Restart: {ar_line}\n"
            f"   └ 🖥 `{escape_md(cmd)}`\n"
        )

    text = "\n".join(lines)
    if len(text) > 3800:
        text = text[:3800] + "\n\n_...more projects, use /start_"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔃 Refresh",    callback_data="my_status"),
         InlineKeyboardButton("📂 Projects",   callback_data="my_projects")],
        [InlineKeyboardButton("🔙 Back",       callback_data="back_start")],
    ])
    await safe_edit(query, text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

# ─────────────────────────────────────────────────────────────
# Project Dashboard
# ─────────────────────────────────────────────────────────────

def project_dashboard_text(p: dict) -> str:
    status  = p.get("status", "stopped")
    locked  = p.get("locked", False)
    if locked:
        icon = "🔒 Locked"
    elif status == "running":
        icon = "🟢 Running"
    else:
        icon = "🔴 Stopped"
    pid     = str(p.get("pid")) if p.get("pid") else "N/A"
    uptime  = "N/A"
    if status == "running" and p.get("started_at"):
        try:
            started = p["started_at"]
            if started.tzinfo is None:
                started = started.replace(tzinfo=timezone.utc)
            elapsed = (datetime.now(timezone.utc) - started).total_seconds()
            uptime  = fmt_uptime(max(0, elapsed))
        except Exception:
            uptime = "N/A"
    last_run = "Never"
    if p.get("last_run"):
        try:
            last_run = p["last_run"].strftime("%Y-%m-%d %H:%M UTC")
        except Exception:
            last_run = str(p["last_run"])
    exit_code = str(p.get("exit_code")) if p.get("exit_code") is not None else "None"
    run_cmd   = p.get("run_command") or "Not set"
    created   = "N/A"
    if p.get("created_date"):
        try:
            created = p["created_date"].strftime("%Y-%m-%d")
        except Exception:
            created = str(p["created_date"])

    ar_status = "✅ ON" if p.get("auto_restart", True) else "❌ OFF"
    locked_line = "\n🔒 Status: *LOCKED* (Premium expired)" if locked else ""

    return (
        f"📊 Project: *{p['name']}*\n\n"
        f"🔹 Status: {icon}{locked_line}\n"
        f"🔹 PID: `{pid}`\n"
        f"🔹 Uptime: `{uptime}`\n"
        f"🔹 Last Run: `{last_run}`\n"
        f"🔹 Exit Code: `{exit_code}`\n"
        f"🔹 Run Command: `{run_cmd}`\n"
        f"🔹 Auto-Restart: {ar_status}\n"
        f"📅 Created: `{created}`"
    )

def project_dashboard_kb(user_id: int, project_name: str, auto_restart: bool = True, is_running: bool = False, is_locked: bool = False) -> InlineKeyboardMarkup:
    pn = project_name
    ar_label = "⏰ Auto-Restart: ✅" if auto_restart else "⏰ Auto-Restart: ❌"

    if is_locked:
        row1 = [InlineKeyboardButton("🔒 Project Locked", callback_data=f"locked_info:{pn}")]
    elif is_running:
        row1 = [
            InlineKeyboardButton("⏹ Stop",      callback_data=f"stop:{pn}"),
            InlineKeyboardButton("🔄 Restart",   callback_data=f"restart:{pn}"),
            InlineKeyboardButton("📋 Logs",      callback_data=f"logs:{pn}"),
        ]
    else:
        row1 = [
            InlineKeyboardButton("▶️ Run",       callback_data=f"run:{pn}"),
            InlineKeyboardButton("🔄 Restart",   callback_data=f"restart:{pn}"),
            InlineKeyboardButton("📋 Logs",      callback_data=f"logs:{pn}"),
        ]

    return InlineKeyboardMarkup([
        row1,
        [
            InlineKeyboardButton("🔃 Refresh",   callback_data=f"proj:{pn}"),
            InlineKeyboardButton("✏️ Edit CMD",  callback_data=f"editcmd:{pn}"),
            InlineKeyboardButton("📁 Files",     callback_data=f"filemgr:{pn}"),
        ],
        [
            InlineKeyboardButton(ar_label,        callback_data=f"toggle_ar:{pn}"),
            InlineKeyboardButton("🔐 Env Vars",  callback_data=f"envvars:{pn}"),
        ],
        [
            InlineKeyboardButton("📦 Reinstall Requirements", callback_data=f"reinstall_reqs:{pn}"),
        ],
        [
            InlineKeyboardButton("🗑 Delete",    callback_data=f"delete:{pn}"),
            InlineKeyboardButton("🔙 Back",      callback_data="my_projects"),
        ],
    ])

async def cb_project_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid  = query.from_user.id
    name = query.data.split(":", 1)[1]

    if await is_banned(uid):
        await safe_edit(query, "🚫 You are banned. Contact owner.")
        return

    p = await get_project(uid, name)
    if not p:
        await safe_edit(query, "❌ Project not found.", parse_mode=ParseMode.MARKDOWN)
        return

    await safe_edit(
        query,
        project_dashboard_text(p),
        reply_markup=project_dashboard_kb(uid, name, p.get("auto_restart", True),
                                          p.get("status") == "running", p.get("locked", False)),
        parse_mode=ParseMode.MARKDOWN,
    )

async def cb_locked_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer(
        "🔒 This project is locked because your Premium expired. Upgrade to Premium to unlock!",
        show_alert=True,
    )

# ─────────────────────────────────────────────────────────────
# 📦 Reinstall Requirements
# ─────────────────────────────────────────────────────────────

async def cb_reinstall_reqs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid  = query.from_user.id
    name = query.data.split(":", 1)[1]

    if await is_banned(uid):
        await safe_edit(query, "🚫 You are banned. Contact owner.")
        return

    p = await get_project(uid, name)
    if not p:
        await safe_edit(query, "❌ Project not found.", parse_mode=ParseMode.MARKDOWN)
        return

    pdir     = project_dir(uid, name)
    req_path = os.path.join(pdir, "requirements.txt")
    pkg_json = os.path.join(pdir, "package.json")
    venv_dir = os.path.join(pdir, "venv")
    pip_path = os.path.join(venv_dir, "bin", "pip")

    back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"proj:{name}")]])

    if os.path.exists(pkg_json) and not os.path.exists(req_path):
        progress = LiveProgress(query.message, title=f"Installing npm packages — {name}")
        await progress.start("npm install starting...")
        progress.run_in_background(estimated_seconds=90, status="npm install (downloading + linking)")
        try:
            proc_n = await asyncio.wait_for(
                create_subprocess_exec("npm", "install", "--no-audit", "--no-fund",
                                       stdout=PIPE, stderr=PIPE, cwd=pdir),
                timeout=600,
            )
            stdout_n, stderr_n = await asyncio.wait_for(proc_n.communicate(), timeout=600)
            if proc_n.returncode == 0:
                await progress.stop(success=True, final_text=f"npm packages reinstalled for {name}")
            else:
                err = (stderr_n or b"").decode()[:400]
                await progress.stop(success=False, final_text=f"```\n{err}\n```")
        except asyncio.TimeoutError:
            await progress.stop(success=False, final_text="npm install timed out")
        except FileNotFoundError:
            await progress.stop(success=False, final_text="npm not installed on host.")
        except Exception as e:
            await progress.stop(success=False, final_text=f"npm error: {escape_md(str(e))}")

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Restart Project", callback_data=f"restart:{name}")],
            [InlineKeyboardButton("🔙 Back", callback_data=f"proj:{name}")],
        ])
        await query.message.reply_text("Choose next:", reply_markup=kb)
        return

    if not os.path.exists(req_path):
        await safe_edit(
            query,
            f"⚠️ *No requirements.txt or package.json found* in `{name}`.\n\nUpload one via 📁 Files first.",
            reply_markup=back_kb,
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    results = []

    if not os.path.exists(pip_path):
        progress = LiveProgress(query.message, title=f"Creating venv — {name}")
        await progress.start("python -m venv ...")
        progress.run_in_background(estimated_seconds=20, status="Building virtual environment")
        try:
            proc = await asyncio.wait_for(
                create_subprocess_exec(sys.executable, "-m", "venv", venv_dir,
                                       stdout=PIPE, stderr=PIPE),
                timeout=120,
            )
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
            if proc.returncode == 0:
                await progress.stop(success=True, final_text="Virtual environment created")
                results.append("✅ Virtual environment created")
            else:
                err = stderr.decode()[:200]
                await progress.stop(success=False, final_text=err)
                results.append(f"❌ venv failed: {err}")
                await query.message.reply_text(
                    f"📦 *Reinstall failed*\n\n" + "\n".join(results),
                    reply_markup=back_kb, parse_mode=ParseMode.MARKDOWN,
                )
                return
        except Exception as e:
            await progress.stop(success=False, final_text=str(e))
            results.append(f"❌ venv error: {e}")
            await query.message.reply_text(
                f"📦 *Reinstall failed*\n\n" + "\n".join(results),
                reply_markup=back_kb, parse_mode=ParseMode.MARKDOWN,
            )
            return

    pip_progress = LiveProgress(query.message, title=f"Upgrading pip — {name}")
    await pip_progress.start("pip install --upgrade pip")
    pip_progress.run_in_background(estimated_seconds=15, status="Fetching latest pip")
    try:
        proc = await asyncio.wait_for(
            create_subprocess_exec(pip_path, "install", "--upgrade", "pip",
                                   stdout=PIPE, stderr=PIPE, cwd=pdir),
            timeout=120,
        )
        await asyncio.wait_for(proc.communicate(), timeout=120)
        await pip_progress.stop(success=True, final_text="pip upgraded")
        results.append("✅ pip upgraded")
    except Exception:
        await pip_progress.stop(success=False, final_text="pip upgrade skipped")
        results.append("⚠️ pip upgrade skipped")

    req_progress = LiveProgress(query.message, title=f"Installing requirements — {name}")
    await req_progress.start("pip install -r requirements.txt")
    req_progress.run_in_background(estimated_seconds=120, status="Resolving + downloading wheels")
    try:
        proc = await asyncio.wait_for(
            create_subprocess_exec(pip_path, "install", "-r", req_path, "--upgrade",
                                   stdout=PIPE, stderr=PIPE, cwd=pdir),
            timeout=600,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=600)
        if proc.returncode == 0:
            await req_progress.stop(success=True, final_text="Requirements installed")
            results.append("✅ Requirements installed successfully")
        else:
            err = stderr.decode()[:400] if stderr else "unknown error"
            await req_progress.stop(success=False, final_text=f"```\n{err}\n```")
            results.append(f"❌ pip install failed:\n```\n{err}\n```")
            await query.message.reply_text(
                f"📦 *Reinstall failed for {name}*\n\n" + "\n".join(results),
                reply_markup=back_kb, parse_mode=ParseMode.MARKDOWN,
            )
            return
    except asyncio.TimeoutError:
        await req_progress.stop(success=False, final_text="pip install timed out")
        results.append("❌ pip install timed out")
        await query.message.reply_text(
            f"📦 *Reinstall failed for {name}*\n\n" + "\n".join(results),
            reply_markup=back_kb, parse_mode=ParseMode.MARKDOWN,
        )
        return
    except Exception as e:
        await req_progress.stop(success=False, final_text=str(e))
        results.append(f"❌ pip error: {e}")
        await query.message.reply_text(
            f"📦 *Reinstall failed for {name}*\n\n" + "\n".join(results),
            reply_markup=back_kb, parse_mode=ParseMode.MARKDOWN,
        )
        return

    # FIX: max(0, ...) to prevent negative package count
    try:
        proc2 = await asyncio.wait_for(
            create_subprocess_exec(pip_path, "list", stdout=PIPE, stderr=PIPE),
            timeout=30,
        )
        out2, _ = await asyncio.wait_for(proc2.communicate(), timeout=30)
        pkg_count = max(0, len(out2.decode().strip().splitlines()) - 2)
        results.append(f"✅ {pkg_count} packages available")
    except Exception:
        results.append("⚠️ Could not verify packages")

    is_running = p.get("status") == "running"
    note = ""
    if is_running:
        note = "\n\nℹ️ Project is running. Click 🔄 *Restart* to apply new packages."

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Restart Project", callback_data=f"restart:{name}")],
        [InlineKeyboardButton("🔙 Back", callback_data=f"proj:{name}")],
    ])

    await safe_edit(
        query,
        f"🎉 *Requirements reinstalled for {name}!*\n\n" + "\n".join(results) + note,
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )

# ─────────────────────────────────────────────────────────────
# ▶️ Run project
# ─────────────────────────────────────────────────────────────

context_store: dict = {}

async def start_project_process(uid: int, name: str) -> dict:
    """Start project subprocess. Returns updated project dict."""
    p   = await get_project(uid, name)
    pdir = project_dir(uid, name)
    cmd  = p.get("run_command") or "python main.py"

    log_path = os.path.join(pdir, "output.log")
    rotate_log_if_needed(log_path)  # RAM optimization: rotate large logs

    venv_python = os.path.join(pdir, "venv", "bin", "python")
    if not os.path.exists(venv_python):
        venv_python = sys.executable

    import shlex
    parts = shlex.split(cmd)
    if parts and parts[0] in ("python", "python3"):
        parts[0] = venv_python

    logger.info(f"Starting process: {' '.join(parts)} in {pdir}")

    import copy
    proc_env = copy.copy(os.environ)
    env_path = os.path.join(pdir, ".env")
    if os.path.exists(env_path):
        with open(env_path, "r") as ef:
            for eline in ef:
                eline = eline.strip()
                if eline and not eline.startswith("#") and "=" in eline:
                    ekey, _, evalue = eline.partition("=")
                    proc_env[ekey.strip()] = evalue.strip()
        logger.info(f"Loaded .env for project {name}")

    log_fd = open(log_path, "a")
    proc = await create_subprocess_exec(
        *parts,
        stdout=log_fd,
        stderr=log_fd,
        cwd=pdir,
        env=proc_env,
        start_new_session=True,
    )
    log_fd.close()

    logger.info(f"Process started with PID {proc.pid}")

    now = datetime.now(timezone.utc)
    await projects_col.update_one(
        {"user_id": uid, "name": name},
        {"$set": {
            "status":       "running",
            "pid":          proc.pid,
            "started_at":   now,
            "last_run":     now,
            "exit_code":    None,
            "admin_stopped": False,
        }},
    )
    # Clean up old entries from context_store to prevent memory leak
    key = f"{uid}:{name}"
    context_store[key] = proc

    updated = await get_project(uid, name)
    logger.info(f"DB updated - status: {updated.get('status')}, pid: {updated.get('pid')}")
    return updated

async def cb_run(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid  = query.from_user.id
    name = query.data.split(":", 1)[1]

    if await is_banned(uid):
        await safe_edit(query, "🚫 You are banned. Contact owner.")
        return

    if await is_maintenance_mode() and uid != OWNER_ID:
        await safe_edit(query, "🔧 Bot is under maintenance. Only owner can use the bot.")
        return

    user_premium = await is_premium(uid)

    # Bot lock check: free users can't run projects
    if await is_bot_locked() and not user_premium and uid != OWNER_ID:
        await safe_edit(
            query,
            "🔒 *Bot is locked.*\n\nOnly Premium users can run projects while the bot is locked.\nContact owner to upgrade!",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"proj:{name}")]]),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    p = await get_project(uid, name)
    if not p:
        await safe_edit(query, "❌ Project not found.", parse_mode=ParseMode.MARKDOWN)
        return

    # Locked project check
    if p.get("locked"):
        await safe_edit(
            query,
            "🔒 *Project is locked.*\n\nYour premium expired. Upgrade to Premium to unlock all projects!",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"proj:{name}")]]),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if p.get("admin_stopped"):
        await safe_edit(
            query,
            "⚠️ Your project was stopped by admin. Contact owner.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"proj:{name}")]]),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if p.get("status") == "running" and p.get("pid"):
        if psutil.pid_exists(p["pid"]):
            await safe_edit(query, "▶️ Project is already running.", parse_mode=ParseMode.MARKDOWN)
            return

    if not p.get("run_command"):
        await safe_edit(
            query,
            "❌ No run command set. Use ✏️ Edit CMD first.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"proj:{name}")]]),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    await safe_edit(query, f"▶️ Starting {name}...")

    try:
        updated = await start_project_process(uid, name)
        logger.info(f"Started project {name} for user {uid}, PID: {updated.get('pid')}")
        await safe_edit(
            query,
            project_dashboard_text(updated),
            reply_markup=project_dashboard_kb(uid, name, updated.get("auto_restart", True),
                                              updated.get("status") == "running",
                                              updated.get("locked", False)),
        )
    except Exception as e:
        logger.error(f"Failed to start project {name}: {e}")
        await safe_edit(query, f"❌ Failed to start: {str(e)[:300]}")

# ─────────────────────────────────────────────────────────────
# ⏹ Stop project
# ─────────────────────────────────────────────────────────────

async def kill_project(uid: int, name: str):
    p = await get_project(uid, name)
    if p and p.get("pid"):
        try:
            proc = psutil.Process(p["pid"])
            for child in proc.children(recursive=True):
                try:
                    child.kill()
                except Exception:
                    pass
            proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    await projects_col.update_one(
        {"user_id": uid, "name": name},
        {"$set": {"status": "stopped", "pid": None}},
    )
    # Clean up context_store entry
    context_store.pop(f"{uid}:{name}", None)

async def cb_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    name = query.data.split(":", 1)[1]

    if await is_banned(uid):
        await safe_edit(query, "🚫 You are banned. Contact owner.")
        return

    p = await get_project(uid, name)
    if not p:
        await safe_edit(query, "❌ Project not found.")
        return

    if p.get("status") != "running":
        await safe_edit(query, "⏹ Project is not running.", parse_mode=ParseMode.MARKDOWN)
        return

    await safe_edit(query, f"⏹ Stopping {name}...")
    await kill_project(uid, name)

    p = await get_project(uid, name)
    await safe_edit(
        query,
        project_dashboard_text(p),
        reply_markup=project_dashboard_kb(uid, name, p.get("auto_restart", True),
                                          p.get("status") == "running", p.get("locked", False)),
    )

# ─────────────────────────────────────────────────────────────
# 🔄 Restart
# ─────────────────────────────────────────────────────────────

async def cb_restart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid  = query.from_user.id
    name = query.data.split(":", 1)[1]

    if await is_banned(uid):
        await safe_edit(query, "🚫 You are banned. Contact owner.")
        return

    if await is_maintenance_mode() and uid != OWNER_ID:
        await safe_edit(query, "🔧 Bot is under maintenance. Only owner can use the bot.")
        return

    user_premium = await is_premium(uid)
    if await is_bot_locked() and not user_premium and uid != OWNER_ID:
        await safe_edit(
            query,
            "🔒 *Bot is locked.* Only Premium users can restart projects.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"proj:{name}")]]),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    p = await get_project(uid, name)
    if not p:
        await safe_edit(query, "❌ Project not found.", parse_mode=ParseMode.MARKDOWN)
        return

    if p.get("locked"):
        await safe_edit(query, "🔒 Project is locked. Upgrade to Premium to unlock.", parse_mode=ParseMode.MARKDOWN)
        return

    if p.get("admin_stopped"):
        await safe_edit(query, "⚠️ Your project was stopped by admin. Contact owner.", parse_mode=ParseMode.MARKDOWN)
        return

    await safe_edit(query, f"🔄 Restarting *{escape_md(name)}*...", parse_mode=ParseMode.MARKDOWN)
    await kill_project(uid, name)
    await asyncio.sleep(1)

    try:
        updated = await start_project_process(uid, name)
        await safe_edit(
            query,
            project_dashboard_text(updated),
            reply_markup=project_dashboard_kb(uid, name, updated.get("auto_restart", True),
                                              updated.get("status") == "running",
                                              updated.get("locked", False)),
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception as e:
        await safe_edit(query, f"❌ Restart failed: {escape_md(str(e))}", parse_mode=ParseMode.MARKDOWN)

# ─────────────────────────────────────────────────────────────
# 📋 Logs
# ─────────────────────────────────────────────────────────────

async def cb_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid  = query.from_user.id
    name = query.data.split(":", 1)[1]

    if await is_banned(uid):
        await safe_edit(query, "🚫 You are banned. Contact owner.")
        return

    log_path = os.path.join(project_dir(uid, name), "output.log")
    if not os.path.exists(log_path):
        lines = "No logs yet."
    else:
        with open(log_path, "r", errors="replace") as f:
            all_lines = f.readlines()
        lines = "".join(all_lines[-50:]) or "Log file is empty."

    if len(lines) > 3500:
        lines = "...(truncated)...\n" + lines[-3500:]

    text = f"📋 *Logs — {escape_md(name)}*\n\n```\n{escape_md(lines)}\n```"
    kb   = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"proj:{name}")]])
    await safe_edit(query, text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

# ─────────────────────────────────────────────────────────────
# ✏️ Edit Run CMD
# ─────────────────────────────────────────────────────────────

async def cb_editcmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data.split(":", 1)[1]
    context.user_data["editcmd_project"] = name
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"proj:{name}")]])
    await safe_edit(
        query,
        f"✏️ *Edit Run Command for {escape_md(name)}*\n\nSend the new run command.\nExample: `python main.py`",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )
    return EDIT_RUN_CMD

async def editcmd_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    cmd  = update.message.text.strip()
    name = context.user_data.get("editcmd_project")

    await projects_col.update_one(
        {"user_id": uid, "name": name},
        {"$set": {"run_command": cmd}},
    )
    p   = await get_project(uid, name)
    kb  = project_dashboard_kb(uid, name, p.get("auto_restart", True),
                               p.get("status") == "running", p.get("locked", False))
    await update.message.reply_text(
        f"✅ Run command updated!\n\n" + project_dashboard_text(p),
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )
    return ConversationHandler.END

# ─────────────────────────────────────────────────────────────
# 📁 File Manager
# ─────────────────────────────────────────────────────────────

async def cb_filemgr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid  = query.from_user.id
    name = query.data.split(":", 1)[1]

    if await is_banned(uid):
        await safe_edit(query, "🚫 You are banned. Contact owner.")
        return

    token    = secrets.token_urlsafe(24)
    now      = datetime.now(timezone.utc)
    expires  = now.timestamp() + 600

    from file_manager import token_store
    token_store[token] = {
        "user_id":      uid,
        "project_name": name,
        "project_dir":  project_dir(uid, name),
        "expires_at":   expires,
    }
    await tokens_col.insert_one({
        "token":        token,
        "user_id":      uid,
        "project_name": name,
        "created_at":   now,
        "expires_at":   datetime.fromtimestamp(expires, tz=timezone.utc),
    })

    url = f"{BASE_URL}/fm/{token}/"
    kb  = InlineKeyboardMarkup([
        [InlineKeyboardButton("🌐 Open File Manager", url=url)],
        [InlineKeyboardButton("🔙 Back",              callback_data=f"proj:{name}")],
    ])
    await safe_edit(
        query,
        f"📁 *File Manager*\n\nYour session link (valid 10 min):\n`{escape_md(url)}`",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )

# ─────────────────────────────────────────────────────────────
# 🗑 Delete project
# ─────────────────────────────────────────────────────────────

async def cb_delete_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data.split(":", 1)[1]
    kb   = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Yes, Delete", callback_data=f"delete_yes:{name}"),
            InlineKeyboardButton("❌ Cancel",       callback_data=f"proj:{name}"),
        ],
    ])
    await safe_edit(
        query,
        f"🗑 *Delete {escape_md(name)}?*\n\nThis cannot be undone.",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )

async def cb_delete_yes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid  = query.from_user.id
    name = query.data.split(":", 1)[1]

    await kill_project(uid, name)
    pdir = project_dir(uid, name)
    if os.path.exists(pdir):
        shutil.rmtree(pdir, ignore_errors=True)
    await projects_col.delete_one({"user_id": uid, "name": name})
    for col in all_backup_cols():
        try:
            await col.delete_many({"type": "file_backup", "user_id": uid, "project_name": name})
        except Exception as e:
            logger.warning(f"Backup cleanup failed on one DB: {e}")

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 My Projects", callback_data="my_projects")]])
    await safe_edit(query, f"✅ Project *{escape_md(name)}* deleted.", reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

# ─────────────────────────────────────────────────────────────
# 🆕 New Project — ConversationHandler
# ─────────────────────────────────────────────────────────────

async def cb_new_project(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id

    if await is_banned(uid):
        await safe_edit(query, "🚫 You are banned. Contact owner.")
        return ConversationHandler.END

    if await is_maintenance_mode() and uid != OWNER_ID:
        await safe_edit(query, "🔧 Bot is under maintenance. Please try later.")
        return ConversationHandler.END

    user_premium = await is_premium(uid)

    # Bot lock: free users can't create new projects
    if await is_bot_locked() and not user_premium and uid != OWNER_ID:
        await safe_edit(
            query,
            "🔒 *Bot is locked.*\n\nOnly Premium users can create new projects while the bot is locked.\nContact owner to upgrade!",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_start")]]),
            parse_mode=ParseMode.MARKDOWN,
        )
        return ConversationHandler.END

    count   = await project_count(uid)
    limit   = PREMIUM_LIMIT if user_premium else FREE_LIMIT

    if count >= limit:
        lbl = "∞" if user_premium else str(FREE_LIMIT)
        await safe_edit(
            query,
            f"❌ Project limit reached ({count}/{lbl}).\nUpgrade to Premium for more!",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_start")]]),
            parse_mode=ParseMode.MARKDOWN,
        )
        return ConversationHandler.END

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="back_start")]])
    await safe_edit(
        query,
        "📝 *New Project*\n\nEnter a project name:\n(alphanumeric + underscore, max 20 chars)",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )
    return NEW_PROJECT_NAME

async def new_project_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    name = update.message.text.strip()

    if not re.match(r"^[a-zA-Z0-9_]{1,20}$", name):
        await update.message.reply_text(
            "❌ Invalid name. Use only letters, numbers, underscore (max 20). Try again:",
            parse_mode=ParseMode.MARKDOWN,
        )
        return NEW_PROJECT_NAME

    existing = await get_project(uid, name)
    if existing:
        await update.message.reply_text(
            f"❌ You already have a project named *{escape_md(name)}*. Choose another:",
            parse_mode=ParseMode.MARKDOWN,
        )
        return NEW_PROJECT_NAME

    context.user_data["new_project_name"]  = name
    context.user_data["new_project_files"] = []

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Done Uploading", callback_data="upload_done")]])
    await update.message.reply_text(
        f"📁 *Project: {escape_md(name)}*\n\n"
        f"Now send your files one by one, or a single `.zip` file.\n"
        f"When done, click *Done Uploading* or send /done.",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )
    return NEW_PROJECT_FILES

async def new_project_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid       = update.effective_user.id
    name      = context.user_data.get("new_project_name")
    pdir      = project_dir(uid, name)
    os.makedirs(pdir, exist_ok=True)

    doc       = update.message.document
    file_name = doc.file_name or "file"
    tg_file   = await context.bot.get_file(doc.file_id)
    dest      = os.path.join(pdir, file_name)
    await tg_file.download_to_drive(dest)

    if file_name.lower().endswith(".zip"):
        try:
            with zipfile.ZipFile(dest, "r") as z:
                names = z.namelist()
                # Detect single top-level directory (strip it)
                top_dirs = set()
                for n in names:
                    parts = n.split("/")
                    if len(parts) > 1:
                        top_dirs.add(parts[0])
                has_single_root = len(top_dirs) == 1

                z.extractall(pdir)

                if has_single_root:
                    root_dir = list(top_dirs)[0]
                    root_path = os.path.join(pdir, root_dir)
                    if os.path.isdir(root_path):
                        for item in os.listdir(root_path):
                            src = os.path.join(root_path, item)
                            dst = os.path.join(pdir, item)
                            if os.path.exists(dst):
                                if os.path.isdir(dst):
                                    shutil.rmtree(dst)
                                else:
                                    os.remove(dst)
                            shutil.move(src, dst)
                        try:
                            shutil.rmtree(root_path)
                        except Exception:
                            pass

            extracted_count = len([n for n in names if not n.endswith("/")])
            await update.message.reply_text(
                f"📦 `{escape_md(file_name)}` extracted ({extracted_count} files).\n"
                f"Send more files or click *Done Uploading*.",
                parse_mode=ParseMode.MARKDOWN,
            )
        except zipfile.BadZipFile:
            try: os.remove(dest)
            except Exception: pass
            await update.message.reply_text(
                f"❌ `{escape_md(file_name)}` corrupt zip. Try again.",
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception as e:
            logger.error(f"Zip extract error for {file_name}: {e}")
            await update.message.reply_text(
                f"❌ Extract failed: `{escape_md(str(e))[:200]}`",
                parse_mode=ParseMode.MARKDOWN,
            )
    else:
        await update.message.reply_text(
            f"✅ `{escape_md(file_name)}` uploaded. Send more or click Done.",
            parse_mode=ParseMode.MARKDOWN,
        )

    return NEW_PROJECT_FILES

async def new_project_done_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await _finalize_new_project(update, context, via_message=True)

async def new_project_done_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    return await _finalize_new_project(update, context, via_message=False)

async def _finalize_new_project(update: Update, context: ContextTypes.DEFAULT_TYPE, via_message: bool):
    uid  = update.effective_user.id
    name = context.user_data.get("new_project_name")
    pdir = project_dir(uid, name)

    status_msg = await (update.message or update.callback_query.message).reply_text(
        f"⚙️ *Setting up {escape_md(name)}*\n\n⏳ Initializing project...",
        parse_mode=ParseMode.MARKDOWN,
    )

    results = []

    # Step 1: Create venv
    venv_progress = LiveProgress(status_msg, title=f"Setup — {name} (venv)")
    await venv_progress.start("python -m venv venv")
    venv_progress.run_in_background(estimated_seconds=20, status="Creating virtual environment")
    try:
        proc = await asyncio.wait_for(
            create_subprocess_exec(sys.executable, "-m", "venv", os.path.join(pdir, "venv"),
                                   stdout=PIPE, stderr=PIPE),
            timeout=60,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)
        if proc.returncode == 0:
            await venv_progress.stop(success=True, final_text="Virtual environment created")
            results.append("✅ Virtual environment created")
        else:
            err = stderr.decode()[:200]
            await venv_progress.stop(success=False, final_text=err)
            results.append(f"❌ venv failed: {err}")
    except asyncio.TimeoutError:
        await venv_progress.stop(success=False, final_text="venv timed out")
        results.append("❌ venv timed out")
    except Exception as e:
        await venv_progress.stop(success=False, final_text=str(e))
        results.append(f"❌ venv error: {e}")

    # Step 2a: Node.js dependencies
    pkg_json_path = os.path.join(pdir, "package.json")
    if os.path.exists(pkg_json_path):
        npm_progress = LiveProgress(status_msg, title=f"Setup — {name} (npm)")
        await npm_progress.start("npm install starting...")
        npm_progress.run_in_background(estimated_seconds=90, status="Installing npm packages")
        try:
            proc_n = await asyncio.wait_for(
                create_subprocess_exec("npm", "install", "--no-audit", "--no-fund",
                                       stdout=PIPE, stderr=PIPE, cwd=pdir),
                timeout=600,
            )
            _, stderr_n = await asyncio.wait_for(proc_n.communicate(), timeout=600)
            if proc_n.returncode == 0:
                await npm_progress.stop(success=True, final_text="npm packages installed")
                results.append("✅ npm packages installed")
            else:
                err = stderr_n.decode()[:300]
                await npm_progress.stop(success=False, final_text=err)
                results.append(f"❌ npm install failed: {err}")
        except asyncio.TimeoutError:
            await npm_progress.stop(success=False, final_text="npm install timed out")
            results.append("❌ npm install timed out")
        except FileNotFoundError:
            await npm_progress.stop(success=False, final_text="npm not found on host")
            results.append("❌ npm not found on host")
        except Exception as e:
            await npm_progress.stop(success=False, final_text=str(e))
            results.append(f"❌ npm error: {e}")

    # Step 2b: Python requirements
    req_path = os.path.join(pdir, "requirements.txt")
    pip_path = os.path.join(pdir, "venv", "bin", "pip")
    if os.path.exists(req_path) and os.path.exists(pip_path):
        req_progress = LiveProgress(status_msg, title=f"Setup — {name} (requirements)")
        await req_progress.start("pip install -r requirements.txt")
        req_progress.run_in_background(estimated_seconds=120, status="Resolving + downloading wheels")
        try:
            proc = await asyncio.wait_for(
                create_subprocess_exec(pip_path, "install", "-r", req_path,
                                       stdout=PIPE, stderr=PIPE, cwd=pdir),
                timeout=300,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
            if proc.returncode == 0:
                await req_progress.stop(success=True, final_text="Requirements installed")
                results.append("✅ Requirements installed")
            else:
                err = stderr.decode()[:300]
                await req_progress.stop(success=False, final_text=err)
                results.append(f"❌ pip install failed: {err}")
        except asyncio.TimeoutError:
            await req_progress.stop(success=False, final_text="pip install timed out")
            results.append("❌ pip install timed out")
        except Exception as e:
            await req_progress.stop(success=False, final_text=str(e))
            results.append(f"❌ pip error: {e}")

        if os.path.exists(pip_path):
            try:
                proc2 = await asyncio.wait_for(
                    create_subprocess_exec(pip_path, "list", stdout=PIPE, stderr=PIPE),
                    timeout=30,
                )
                out2, _ = await asyncio.wait_for(proc2.communicate(), timeout=30)
                # FIX: max(0, ...) prevents negative count
                pkg_count = max(0, len(out2.decode().strip().splitlines()) - 2)
                results.append(f"✅ {pkg_count} packages verified")
            except Exception:
                results.append("⚠️ Could not verify packages")
    else:
        results.append("ℹ️ No requirements.txt found")

    # Determine default run command
    py_candidates   = ["main.py", "bot.py", "app.py", "index.py", "run.py"]
    node_candidates = ["index.js", "bot.js", "app.js", "main.js", "server.js"]
    default_cmd = None

    if os.path.exists(pkg_json_path):
        try:
            import json as _json
            with open(pkg_json_path, "r", encoding="utf-8") as _pf:
                _pkg = _json.load(_pf)
            if isinstance(_pkg, dict) and isinstance(_pkg.get("scripts"), dict) and _pkg["scripts"].get("start"):
                default_cmd = "npm start"
            elif isinstance(_pkg, dict) and _pkg.get("main") and os.path.exists(os.path.join(pdir, _pkg["main"])):
                default_cmd = f"node {_pkg['main']}"
        except Exception as _e:
            logger.warning(f"package.json parse failed for {name}: {_e}")

    if not default_cmd:
        for c in py_candidates:
            if os.path.exists(os.path.join(pdir, c)):
                default_cmd = f"python {c}"
                break
    if not default_cmd:
        for c in node_candidates:
            if os.path.exists(os.path.join(pdir, c)):
                default_cmd = f"node {c}"
                break

    await projects_col.insert_one({
        "user_id":      uid,
        "name":         name,
        "run_command":  default_cmd,
        "created_date": datetime.now(timezone.utc),
        "last_run":     None,
        "exit_code":    None,
        "status":       "stopped",
        "pid":          None,
        "admin_stopped": False,
        "auto_restart":  True,
        "restart_count": 0,
        "last_restart_at": None,
        "locked":        False,
    })

    result_text = "\n".join(results)
    if default_cmd:
        result_text += f"\n\n🚀 Default run cmd: `{escape_md(default_cmd)}`"
    else:
        result_text += "\n\n⚠️ No main file detected. Set run command manually."

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Open Dashboard", callback_data=f"proj:{name}")],
        [InlineKeyboardButton("🔙 My Projects",    callback_data="my_projects")],
    ])
    await status_msg.edit_text(
        f"🎉 *Project {escape_md(name)} ready!*\n\n{result_text}\n\n[████████████] ✅ Complete!",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )
    context.user_data.clear()
    return ConversationHandler.END

async def new_project_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    if update.callback_query:
        await update.callback_query.answer()
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_start")]])
    msg = update.effective_message
    await msg.reply_text("❌ Cancelled.", reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

# ─────────────────────────────────────────────────────────────
# ⚙️ Admin Panel
# ─────────────────────────────────────────────────────────────

@admin_or_owner
async def cb_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id

    total_users   = await users_col.count_documents({})
    premium_count = await users_col.count_documents({"is_premium": True})
    banned_count  = await users_col.count_documents({"is_banned": True})
    admin_count   = await users_col.count_documents({"is_admin": True})
    total_proj    = await projects_col.count_documents({})
    running_proj  = await running_project_count()

    meta = await backups_col.find_one({"type": "backup_meta"})
    if meta:
        backup_time = escape_md(meta["backed_up_at"].strftime("%Y-%m-%d %H:%M UTC"))
        backup_info = f"\n💾 Last Backup: `{backup_time}`"
    else:
        backup_info = "\n💾 Last Backup: `Never`"

    db_count_line = f"\n🗄 Databases: `{1 + len(extra_dbs)}` (1 primary + {len(extra_dbs)} extra)"

    settings = await get_bot_settings()
    lock_icon = "🔒 ON" if settings.get("bot_locked") else "🔓 OFF"
    maint_icon = "🔧 ON" if settings.get("maintenance_mode") else "✅ OFF"
    active_db_label = "SQLite" if settings.get("active_db") == "local" else "MongoDB"

    role_label = "👑 Owner" if uid == OWNER_ID else "🛡 Admin"

    text = (
        f"⚙️ *Admin Panel* ({role_label})\n\n"
        f"👥 Total Users: `{total_users}`\n"
        f"💎 Premium: `{premium_count}`\n"
        f"🛡 Admins: `{admin_count}`\n"
        f"🚫 Banned: `{banned_count}`\n"
        f"📁 Projects: `{total_proj}`\n"
        f"🟢 Running: `{running_proj}`"
        f"{db_count_line}"
        f"{backup_info}\n"
        f"🔒 Bot Lock: `{lock_icon}`\n"
        f"🔧 Maintenance: `{maint_icon}`\n"
        f"🗄 Active DB: `{active_db_label}`"
    )

    kb_rows = [
        [InlineKeyboardButton("👥 User List",        callback_data="admin:user_list:0"),
         InlineKeyboardButton("🟢 Running Scripts",  callback_data="admin:running")],
        [InlineKeyboardButton("💎 Give Premium",     callback_data="admin:give_premium"),
         InlineKeyboardButton("❌ Remove Premium",   callback_data="admin:remove_premium")],
        [InlineKeyboardButton("⏰ Temp Premium",     callback_data="admin:temp_premium"),
         InlineKeyboardButton("🚫 Ban User",         callback_data="admin:ban")],
        [InlineKeyboardButton("✅ Unban User",       callback_data="admin:unban"),
         InlineKeyboardButton("📢 Broadcast",        callback_data="admin:broadcast_menu")],
        [InlineKeyboardButton("💾 Backup Now",       callback_data="admin:backup_now"),
         InlineKeyboardButton("🗑 Delete All Backup", callback_data="admin:del_backups")],
        [InlineKeyboardButton("📊 Bot Status",       callback_data="bot_status")],
    ]

    # Owner-only buttons
    if uid == OWNER_ID:
        kb_rows.append([
            InlineKeyboardButton("➕ Add Admin",    callback_data="admin:add_admin"),
            InlineKeyboardButton("➖ Remove Admin", callback_data="admin:remove_admin"),
        ])
        # Bot lock & maintenance (owner only)
        lock_btn_label = "🔓 Unlock Bot" if settings.get("bot_locked") else "🔒 Lock Bot"
        maint_btn_label = "✅ Disable Maintenance" if settings.get("maintenance_mode") else "🔧 Maintenance Mode"
        kb_rows.append([
            InlineKeyboardButton(lock_btn_label,  callback_data="admin:toggle_lock"),
            InlineKeyboardButton(maint_btn_label, callback_data="admin:toggle_maintenance"),
        ])
        kb_rows.append([
            InlineKeyboardButton("🗄 Database Settings", callback_data="admin:db_settings"),
        ])

    kb_rows.append([InlineKeyboardButton("🔙 Back", callback_data="back_start")])

    await safe_edit(query, text, reply_markup=InlineKeyboardMarkup(kb_rows), parse_mode=ParseMode.MARKDOWN)

# ─────────────────────────────────────────────────────────────
# 🔒 Bot Lock Toggle (Owner only)
# ─────────────────────────────────────────────────────────────

@owner_only
async def cb_admin_toggle_lock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    settings = await get_bot_settings()
    current = settings.get("bot_locked", False)
    new_val = not current

    await set_bot_setting("bot_locked", new_val)

    if new_val:
        # Lock is now ON — stop all free users' running projects
        await safe_edit(
            query,
            "🔒 *Locking bot...*\n\nStopping all free users' running projects...",
            parse_mode=ParseMode.MARKDOWN,
        )
        stopped_count = 0
        running = await projects_col.find({"status": "running"}).to_list(length=1000)
        for p in running:
            p_uid = p["user_id"]
            if p_uid == OWNER_ID:
                continue
            prem = await is_premium(p_uid)
            if not prem:
                await kill_project(p_uid, p["name"])
                await projects_col.update_one(
                    {"user_id": p_uid, "name": p["name"]},
                    {"$set": {"admin_stopped": True}},
                )
                stopped_count += 1
                if notification_bot:
                    try:
                        await notification_bot.send_message(
                            chat_id=p_uid,
                            text=(
                                f"🔒 *Bot Locked*\n\n"
                                f"Project `{p['name']}` has been stopped because the bot is now locked.\n"
                                f"Only Premium users can run projects.\n"
                                f"Contact owner to upgrade!"
                            ),
                            parse_mode=ParseMode.MARKDOWN,
                        )
                    except Exception:
                        pass
        status_msg = f"🔒 *Bot is now LOCKED*\n\n✅ {stopped_count} free user project(s) stopped.\nPremium users are unaffected."
    else:
        status_msg = "🔓 *Bot is now UNLOCKED*\n\nAll users can create and run projects."

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]])
    await safe_edit(query, status_msg, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

# ─────────────────────────────────────────────────────────────
# 🔧 Maintenance Mode Toggle (Owner only)
# ─────────────────────────────────────────────────────────────

@owner_only
async def cb_admin_toggle_maintenance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    settings = await get_bot_settings()
    current = settings.get("maintenance_mode", False)
    new_val = not current

    await set_bot_setting("maintenance_mode", new_val)

    if new_val:
        # Maintenance ON — stop ALL projects (except owner's)
        await safe_edit(
            query,
            "🔧 *Enabling maintenance mode...*\n\nStopping all running projects...",
            parse_mode=ParseMode.MARKDOWN,
        )
        stopped_count = 0
        running = await projects_col.find({"status": "running"}).to_list(length=1000)
        for p in running:
            p_uid = p["user_id"]
            if p_uid == OWNER_ID:
                continue
            await kill_project(p_uid, p["name"])
            stopped_count += 1
            if notification_bot:
                try:
                    await notification_bot.send_message(
                        chat_id=p_uid,
                        text=(
                            f"🔧 *Maintenance Mode*\n\n"
                            f"Project `{p['name']}` has been stopped for maintenance.\n"
                            f"The bot will be back soon!"
                        ),
                        parse_mode=ParseMode.MARKDOWN,
                    )
                except Exception:
                    pass
        status_msg = (
            f"🔧 *Maintenance Mode is now ON*\n\n"
            f"✅ {stopped_count} project(s) stopped.\n"
            f"Only owner can use the bot now."
        )
    else:
        status_msg = "✅ *Maintenance Mode is now OFF*\n\nAll users can use the bot again."

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]])
    await safe_edit(query, status_msg, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

# ─────────────────────────────────────────────────────────────
# 🗄 Database Settings (Owner only)
# ─────────────────────────────────────────────────────────────

@owner_only
async def cb_admin_db_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    settings = await get_bot_settings()
    active_db = settings.get("active_db", "mongodb")
    local_exists = os.path.exists(LOCAL_DB_PATH)
    local_size = fmt_bytes(os.path.getsize(LOCAL_DB_PATH)) if local_exists else "N/A"

    mongo_proj = await projects_col.count_documents({})
    mongo_users = await users_col.count_documents({})

    text = (
        f"🗄 *Database Settings*\n\n"
        f"*Currently Active:* `{'SQLite (Local)' if active_db == 'local' else 'MongoDB'}`\n\n"
        f"*MongoDB:*\n"
        f"├ Users: `{mongo_users}`\n"
        f"├ Projects: `{mongo_proj}`\n"
        f"└ Status: `{'🟢 Connected'}`\n\n"
        f"*Local SQLite:*\n"
        f"├ File: `{LOCAL_DB_PATH}`\n"
        f"├ Exists: `{'✅ Yes' if local_exists else '❌ No'}`\n"
        f"└ Size: `{local_size}`\n\n"
        f"_When switching, all data is copied automatically._\n"
        f"_MongoDB always continues to receive backups regardless of active DB._"
    )

    kb_rows = []
    if active_db == "mongodb":
        kb_rows.append([InlineKeyboardButton("🗄 Switch to Local (SQLite)", callback_data="admin:db_switch_to_local")])
    else:
        kb_rows.append([InlineKeyboardButton("☁️ Switch to MongoDB", callback_data="admin:db_switch_to_mongo")])

    kb_rows.append([InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")])
    await safe_edit(query, text, reply_markup=InlineKeyboardMarkup(kb_rows), parse_mode=ParseMode.MARKDOWN)

@owner_only
async def cb_admin_db_switch_to_local(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    kb_confirm = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Yes, Switch to Local", callback_data="admin:db_confirm_local")],
        [InlineKeyboardButton("❌ Cancel", callback_data="admin:db_settings")],
    ])
    await safe_edit(
        query,
        "🗄 *Switch to Local SQLite?*\n\n"
        "• All MongoDB data will be copied to local SQLite\n"
        "• Bot will use local SQLite going forward\n"
        "• MongoDB will continue receiving backups\n\n"
        "⚠️ Make sure you have enough disk space!",
        reply_markup=kb_confirm,
        parse_mode=ParseMode.MARKDOWN,
    )

@owner_only
async def cb_admin_db_confirm_local(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    await safe_edit(
        query,
        "🗄 *Migrating MongoDB → Local SQLite...*\n\n⏳ Please wait...",
        parse_mode=ParseMode.MARKDOWN,
    )

    try:
        uc, pc = await migrate_mongo_to_local()
        await set_bot_setting("active_db", "local")
        result_text = (
            f"✅ *Switched to Local SQLite!*\n\n"
            f"📊 Migrated:\n"
            f"├ Users: `{uc}`\n"
            f"└ Projects: `{pc}`\n\n"
            f"Bot is now using local SQLite database.\n"
            f"MongoDB backup continues in background."
        )
    except Exception as e:
        result_text = f"❌ *Migration failed!*\n\n`{escape_md(str(e)[:300])}`\n\nStill on MongoDB."

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 DB Settings", callback_data="admin:db_settings")]])
    await safe_edit(query, result_text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

@owner_only
async def cb_admin_db_switch_to_mongo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    kb_confirm = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Yes, Switch to MongoDB", callback_data="admin:db_confirm_mongo")],
        [InlineKeyboardButton("❌ Cancel", callback_data="admin:db_settings")],
    ])
    await safe_edit(
        query,
        "☁️ *Switch to MongoDB?*\n\n"
        "• All local SQLite data will be copied to MongoDB\n"
        "• Bot will use MongoDB going forward\n"
        "• Local SQLite file is kept as backup\n\n"
        "Proceed?",
        reply_markup=kb_confirm,
        parse_mode=ParseMode.MARKDOWN,
    )

@owner_only
async def cb_admin_db_confirm_mongo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    await safe_edit(
        query,
        "☁️ *Migrating Local SQLite → MongoDB...*\n\n⏳ Please wait...",
        parse_mode=ParseMode.MARKDOWN,
    )

    try:
        uc, pc = await migrate_local_to_mongo()
        await set_bot_setting("active_db", "mongodb")
        result_text = (
            f"✅ *Switched to MongoDB!*\n\n"
            f"📊 Migrated:\n"
            f"├ Users: `{uc}`\n"
            f"└ Projects: `{pc}`\n\n"
            f"Bot is now using MongoDB.\n"
            f"Local SQLite file kept as backup at:\n`{escape_md(LOCAL_DB_PATH)}`"
        )
    except Exception as e:
        result_text = f"❌ *Migration failed!*\n\n`{escape_md(str(e)[:300])}`\n\nStill on local SQLite."

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 DB Settings", callback_data="admin:db_settings")]])
    await safe_edit(query, result_text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

# ─────────────────────────────────────────────────────────────
# 💾 Backup (admin)
# ─────────────────────────────────────────────────────────────

@admin_or_owner
async def cb_admin_backup_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("⏳ Running backup...", show_alert=False)

    await safe_edit(
        query,
        "💾 *Backup in progress...*\n\nThis may take a moment.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]),
        parse_mode=ParseMode.MARKDOWN,
    )

    try:
        all_projects = await projects_col.find({}).to_list(length=10000)
        total_files = 0
        total_size = 0
        db_distribution = {}

        for proj in all_projects:
            uid  = proj["user_id"]
            name = proj["name"]
            pdir = project_dir(uid, name)

            if not os.path.exists(pdir):
                continue

            files_data = []
            for root, dirs, files in os.walk(pdir):
                dirs[:] = [d for d in dirs if d not in ("venv", "__pycache__", ".git", "node_modules")]
                for fname in files:
                    if fname in ("output.log",) or fname.endswith(".pyc"):
                        continue
                    fpath   = os.path.join(root, fname)
                    rel_path = os.path.relpath(fpath, pdir)
                    try:
                        file_size = os.path.getsize(fpath)
                        if file_size > 15 * 1024 * 1024:
                            continue
                        try:
                            with open(fpath, "r", encoding="utf-8") as f:
                                content = f.read()
                            content_b64 = base64.b64encode(content.encode("utf-8")).decode("ascii")
                            is_binary = False
                        except (UnicodeDecodeError, ValueError):
                            with open(fpath, "rb") as f:
                                content_bytes = f.read()
                            content_b64 = base64.b64encode(content_bytes).decode("ascii")
                            is_binary = True
                        files_data.append({
                            "path": rel_path, "content_b64": content_b64,
                            "size": file_size, "is_binary": is_binary,
                        })
                        total_files += 1
                        total_size  += file_size
                    except Exception:
                        continue

            if files_data:
                target_db_name, target_col = pick_backup_col(uid, name)
                doc = {
                    "type": "file_backup", "user_id": uid,
                    "project_name": name, "files": files_data,
                    "backed_up_at": datetime.now(timezone.utc),
                    "stored_in": target_db_name,
                }
                try:
                    for col in all_backup_cols():
                        try:
                            await col.delete_many({"type": "file_backup", "user_id": uid, "project_name": name})
                        except Exception:
                            pass
                    await target_col.insert_one(doc)
                    db_distribution[target_db_name] = db_distribution.get(target_db_name, 0) + 1
                except Exception as e:
                    logger.warning(f"Backup write failed for {name} on {target_db_name}: {e}")

        now = datetime.now(timezone.utc)
        try:
            await backups_col.delete_many({"type": "backup_meta"})
            await backups_col.insert_one({
                "type": "backup_meta", "total_projects": len(all_projects),
                "total_files": total_files, "total_size": total_size,
                "backed_up_at": now, "distribution": db_distribution,
            })
        except Exception as e:
            logger.warning(f"Backup meta write failed: {e}")

        total_db_count = 1 + len(extra_dbs)
        backup_time = escape_md(now.strftime("%Y-%m-%d %H:%M UTC"))
        dist_lines = ""
        if db_distribution:
            dist_lines = "\n*📊 Storage Distribution:*\n"
            for db_name, count in sorted(db_distribution.items()):
                dist_lines += f"   • `{escape_md(db_name)}`: `{count}` projects\n"

        result_text = (
            f"✅ *Backup Complete!*\n\n"
            f"📁 Projects: `{len(all_projects)}`\n"
            f"📄 Files: `{total_files}`\n"
            f"📦 Size: `{escape_md(fmt_bytes(total_size))}`\n"
            f"🗄 Distributed across: `{total_db_count}` database(s)\n"
            f"🕐 Time: `{backup_time}`"
            f"{dist_lines}"
        )
    except Exception as e:
        logger.error(f"Manual backup failed: {e}")
        result_text = f"❌ *Backup Failed!*\n\n`{escape_md(str(e))}`"

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]])
    await safe_edit(query, result_text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

# ─────────────────────────────────────────────────────────────
# 🗑 Delete All Backups (owner)
# ─────────────────────────────────────────────────────────────

@owner_only
async def cb_admin_delete_backups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    primary_count = await backups_col.count_documents({})
    extra_counts = []
    total_extra = 0
    for entry in extra_dbs:
        try:
            c = await entry["db"]["backups"].count_documents({})
        except Exception:
            c = 0
        extra_counts.append((entry["name"], c))
        total_extra += c

    lines = [
        "⚠️ *Delete ALL Backups?*\n",
        f"📂 Primary DB (`{DATABASE_NAME}`): `{primary_count}` docs",
    ]
    if extra_counts:
        lines.append(f"\n📂 *Extra DBs ({len(extra_counts)}):*")
        for name, c in extra_counts:
            lines.append(f"   • `{name}`: `{c}` docs")
        lines.append(f"\n📊 *Total to delete:* `{primary_count + total_extra}` documents")
    else:
        lines.append("\nℹ️ No extra DBs configured.")
    lines.append("\nThis action is *permanent* and cannot be undone.")
    lines.append("Project files will NOT be deleted — only MongoDB backups.")

    text = "\n".join(lines)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🗑 Yes, Delete All", callback_data="admin:del_backups_confirm")],
        [InlineKeyboardButton("🔙 Cancel",          callback_data="admin_panel")],
    ])
    await safe_edit(query, text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

@owner_only
async def cb_admin_delete_backups_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("⏳ Deleting backups...", show_alert=False)

    await safe_edit(
        query, "🗑 *Deleting all backups...*\n\nPlease wait.",
        reply_markup=None, parse_mode=ParseMode.MARKDOWN,
    )

    primary_deleted = 0
    extra_results = []
    errors = []

    try:
        res = await backups_col.delete_many({})
        primary_deleted = res.deleted_count
    except Exception as e:
        errors.append(f"Primary DB error: {e}")

    for entry in extra_dbs:
        name = entry["name"]
        try:
            res_x = await entry["db"]["backups"].delete_many({})
            extra_results.append((name, res_x.deleted_count, None))
        except Exception as e:
            extra_results.append((name, 0, str(e)))
            errors.append(f"DB '{name}' error: {e}")

    total_deleted = primary_deleted + sum(c for _, c, _ in extra_results)

    lines = [
        "✅ *All Backups Deleted!*\n",
        f"📂 Primary (`{DATABASE_NAME}`): `{primary_deleted}` removed",
    ]
    if extra_results:
        lines.append(f"\n📂 *Extra DBs ({len(extra_results)}):*")
        for name, count, err in extra_results:
            if err:
                lines.append(f"   • `{name}`: ❌ failed")
            else:
                lines.append(f"   • `{name}`: `{count}` removed")
        lines.append(f"\n📊 *Total deleted:* `{total_deleted}` documents")
    if errors:
        lines.append("\n⚠️ *Some errors occurred:*")
        for err in errors[:5]:
            lines.append(f"`{escape_md(err)}`")

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]])
    await safe_edit(query, "\n".join(lines), reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

# ─────────────────────────────────────────────────────────────
# 👥 Admin User List
# ─────────────────────────────────────────────────────────────

@admin_or_owner
async def cb_admin_user_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = int(query.data.split(":")[-1])
    per_page = 10

    total = await users_col.count_documents({})
    users = await users_col.find({}).skip(page * per_page).limit(per_page).to_list(length=per_page)

    lines = [f"👥 *User List* (page {page+1})\n"]
    for u in users:
        badges = ""
        if u.get("is_admin"):
            badges += " 🛡"
        if u.get("is_premium"):
            badges += " 💎"
        if u.get("is_banned"):
            badges += " 🚫"
        uname = f"@{u['username']}" if u.get("username") else "no-username"
        lines.append(f"`{u['user_id']}` {escape_md(uname)}{badges}")

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"admin:user_list:{page-1}"))
    if (page + 1) * per_page < total:
        nav.append(InlineKeyboardButton("➡️ Next", callback_data=f"admin:user_list:{page+1}"))

    kb_rows = []
    if nav:
        kb_rows.append(nav)
    kb_rows.append([InlineKeyboardButton("🔙 Back", callback_data="admin_panel")])

    await safe_edit(query, "\n".join(lines), reply_markup=InlineKeyboardMarkup(kb_rows), parse_mode=ParseMode.MARKDOWN)

# ─────────────────────────────────────────────────────────────
# 🟢 Running Scripts
# ─────────────────────────────────────────────────────────────

@admin_or_owner
async def cb_admin_running(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        running = await projects_col.find({"status": "running"}).to_list(length=100)
        if not running:
            await safe_edit(
                query,
                "🟢 *Running Scripts*\n\nNo projects running.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]),
                parse_mode=ParseMode.MARKDOWN,
            )
            return

        lines = ["🟢 *Running Scripts*\n"]
        kb_rows = []

        for p in running:
            user_doc = await get_user(p["user_id"])
            fname = user_doc.get("first_name", "Unknown") if user_doc else "Unknown"
            uname = f"@{user_doc['username']}" if user_doc and user_doc.get("username") else "no-username"
            pid = p.get("pid", "N/A")
            uptime = "N/A"
            if p.get("started_at"):
                try:
                    started = p["started_at"]
                    if started.tzinfo is None:
                        started = started.replace(tzinfo=timezone.utc)
                    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
                    uptime = fmt_uptime(max(0, elapsed))
                except Exception:
                    uptime = "N/A"

            lines.append(
                f"- - - - - - - - - - -\n"
                f"👤 {fname} ({uname})\n"
                f"📁 Project: {p['name']}\n"
                f"🔹 PID: {pid} | Uptime: {uptime}"
            )
            row_btns = [InlineKeyboardButton(f"⏹ Stop {p['name']}", callback_data=f"admin_stop:{p['user_id']}:{p['name']}")]
            if query.from_user.id == OWNER_ID:
                row_btns.append(InlineKeyboardButton(f"📥 Download", callback_data=f"admin_dl:{p['user_id']}:{p['name']}"))
            kb_rows.append(row_btns)

        kb_rows.append([InlineKeyboardButton("👥 All Users & Projects", callback_data="admin:all_projects:0")])
        kb_rows.append([InlineKeyboardButton("🔙 Back", callback_data="admin_panel")])

        full_text = "\n".join(lines)
        if len(full_text) > 4000:
            full_text = full_text[:3900] + "\n...(truncated)"

        await safe_edit(query, full_text, reply_markup=InlineKeyboardMarkup(kb_rows), parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"cb_admin_running error: {e}")
        await safe_edit(
            query,
            f"❌ Error loading running scripts: {str(e)[:200]}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]),
        )

# ─────────────────────────────────────────────────────────────
# All Users & Projects
# ─────────────────────────────────────────────────────────────

@admin_or_owner
async def cb_admin_all_projects(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = int(query.data.split(":")[-1])
    per_page = 5

    all_projects = await projects_col.find({}).to_list(length=10000)
    user_projects = {}
    for p in all_projects:
        uid = p["user_id"]
        if uid not in user_projects:
            user_projects[uid] = []
        user_projects[uid].append(p)

    user_ids = list(user_projects.keys())
    total = len(user_ids)
    start = page * per_page
    end = min(start + per_page, total)
    page_user_ids = user_ids[start:end]

    if not page_user_ids:
        await safe_edit(
            query,
            "👥 *All Users & Projects*\n\nNo projects found.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin:running")]]),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    lines = [f"👥 *All Users & Projects* (page {page+1})\n"]
    kb_rows = []

    for uid in page_user_ids:
        user_doc = await get_user(uid)
        fname = user_doc.get("first_name", "Unknown") if user_doc else "Unknown"
        uname = f"@{user_doc['username']}" if user_doc and user_doc.get("username") else ""

        projects = user_projects[uid]
        proj_lines = []
        for p in projects:
            status_icon = "🟢" if p.get("status") == "running" else ("🔒" if p.get("locked") else "🔴")
            proj_lines.append(f"  {status_icon} {p['name']}")

            is_caller_owner = query.from_user.id == OWNER_ID
            if p.get("status") == "running":
                row = [InlineKeyboardButton(f"⏹ Stop {p['name']}", callback_data=f"admin_stop:{uid}:{p['name']}")]
                if is_caller_owner:
                    row.append(InlineKeyboardButton(f"📥 DL {p['name']}", callback_data=f"admin_dl:{uid}:{p['name']}"))
                kb_rows.append(row)
            else:
                row = [InlineKeyboardButton(f"▶️ Run {p['name']}", callback_data=f"admin_run:{uid}:{p['name']}")]
                if is_caller_owner:
                    row.append(InlineKeyboardButton(f"📥 DL {p['name']}", callback_data=f"admin_dl:{uid}:{p['name']}"))
                kb_rows.append(row)

        lines.append(
            f"- - - - - - - - - - -\n"
            f"👤 {fname} {uname} (`{uid}`)\n"
            f"📁 {len(projects)} project(s):\n" + "\n".join(proj_lines)
        )

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"admin:all_projects:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton("➡️ Next", callback_data=f"admin:all_projects:{page+1}"))
    if nav:
        kb_rows.append(nav)

    kb_rows.append([InlineKeyboardButton("🔙 Back", callback_data="admin:running")])

    full_text = "\n".join(lines)
    if len(full_text) > 4000:
        full_text = full_text[:3900] + "\n...(truncated)"

    await safe_edit(query, full_text, reply_markup=InlineKeyboardMarkup(kb_rows), parse_mode=ParseMode.MARKDOWN)

@admin_or_owner
async def cb_admin_run_project(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, uid_str, name = query.data.split(":", 2)
    uid = int(uid_str)

    p = await get_project(uid, name)
    if not p:
        await safe_edit(
            query, "❌ Project not found.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin:all_projects:0")]]),
        )
        return

    if not p.get("run_command"):
        await safe_edit(
            query,
            f"❌ No run command set for *{escape_md(name)}*.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin:all_projects:0")]]),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if p.get("status") == "running" and p.get("pid") and psutil.pid_exists(p["pid"]):
        await safe_edit(
            query,
            f"▶️ Project *{escape_md(name)}* is already running.\nPID: `{p.get('pid')}`",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin:all_projects:0")]]),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    try:
        updated = await start_project_process(uid, name)
        try:
            if notification_bot:
                await notification_bot.send_message(
                    uid,
                    f"▶️ Your project *{escape_md(name)}* was started by admin.",
                    parse_mode=ParseMode.MARKDOWN,
                )
        except Exception:
            pass

        await safe_edit(
            query,
            f"✅ Project *{escape_md(name)}* started by admin.\nPID: `{updated.get('pid', 'N/A')}`",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("👥 All Projects", callback_data="admin:all_projects:0")],
                [InlineKeyboardButton("🟢 Running Scripts", callback_data="admin:running")],
            ]),
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception as e:
        logger.error(f"Admin run project failed for {uid}/{name}: {e}")
        await safe_edit(
            query,
            f"❌ Failed to start *{escape_md(name)}*: `{escape_md(str(e))[:250]}`",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin:all_projects:0")]]),
            parse_mode=ParseMode.MARKDOWN,
        )

@owner_only
async def cb_admin_download_project(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("📥 Creating zip...", show_alert=False)
    _, uid_str, name = query.data.split(":", 2)
    uid = int(uid_str)

    pdir = project_dir(uid, name)
    if not os.path.exists(pdir):
        await query.answer("❌ Project directory not found!", show_alert=True)
        return

    zip_path = os.path.join(PROJECTS_ROOT, f"{uid}_{name}.zip")
    try:
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, dirs, files in os.walk(pdir):
                dirs[:] = [d for d in dirs if d not in ("venv", "__pycache__", ".git", "node_modules")]
                for fname_file in files:
                    if fname_file in ("output.log",) or fname_file.endswith(".pyc"):
                        continue
                    fpath = os.path.join(root, fname_file)
                    arcname = os.path.relpath(fpath, pdir)
                    zf.write(fpath, arcname)

        with open(zip_path, "rb") as f:
            await query.message.reply_document(
                document=f,
                filename=f"{name}.zip",
                caption=f"📥 Project: {name}\nUser ID: {uid}",
            )
    except Exception as e:
        logger.error(f"Admin download failed: {e}")
        await query.answer(f"❌ Download failed: {str(e)[:100]}", show_alert=True)
    finally:
        if os.path.exists(zip_path):
            os.remove(zip_path)

@admin_or_owner
async def cb_admin_stop_project(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, uid_str, name = query.data.split(":", 2)
    uid = int(uid_str)

    p = await get_project(uid, name)
    if not p:
        await safe_edit(
            query, "❌ Project not found.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin:running")]]),
        )
        return

    try:
        await kill_project(uid, name)
        await projects_col.update_one(
            {"user_id": uid, "name": name},
            {"$set": {"admin_stopped": True}},
        )
        try:
            if notification_bot:
                await notification_bot.send_message(
                    uid,
                    f"⏹ Your project *{escape_md(name)}* was stopped by admin.\nContact owner to resume.",
                    parse_mode=ParseMode.MARKDOWN,
                )
        except Exception:
            pass

        await safe_edit(
            query,
            f"✅ Project *{escape_md(name)}* stopped (admin).",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🟢 Running Scripts", callback_data="admin:running")],
                [InlineKeyboardButton("👥 All Projects", callback_data="admin:all_projects:0")],
            ]),
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception as e:
        logger.error(f"Admin stop project failed for {uid}/{name}: {e}")
        await safe_edit(
            query,
            f"❌ Failed to stop *{escape_md(name)}*: `{escape_md(str(e))[:250]}`",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin:running")]]),
            parse_mode=ParseMode.MARKDOWN,
        )

# ─────────────────────────────────────────────────────────────
# Admin premium / ban / broadcast conversations
# ─────────────────────────────────────────────────────────────

@admin_or_owner
async def cb_admin_give_premium(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await safe_edit(query, "💎 *Give Premium*\n\nSend the user ID:", parse_mode=ParseMode.MARKDOWN)
    return ADMIN_GIVE_PREMIUM_ID

async def admin_give_premium_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid ID. Send a numeric user ID:", parse_mode=ParseMode.MARKDOWN)
        return ADMIN_GIVE_PREMIUM_ID

    await users_col.update_one(
        {"user_id": uid},
        {"$set": {"is_premium": True, "premium_expiry": None}},
    )
    # Unlock all projects for this user
    await projects_col.update_many({"user_id": uid}, {"$set": {"locked": False}})

    try:
        await update.get_bot().send_message(uid, "🎉 You have been granted *Premium*! Enjoy unlimited projects!", parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass
    await update.message.reply_text(f"✅ Premium granted to `{uid}`. All their projects unlocked.", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

@admin_or_owner
async def cb_admin_remove_premium(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await safe_edit(query, "❌ *Remove Premium*\n\nSend the user ID:", parse_mode=ParseMode.MARKDOWN)
    return ADMIN_REMOVE_PREMIUM_ID

async def admin_remove_premium_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid ID:", parse_mode=ParseMode.MARKDOWN)
        return ADMIN_REMOVE_PREMIUM_ID

    await users_col.update_one({"user_id": uid}, {"$set": {"is_premium": False, "premium_expiry": None}})
    # Lock extra projects
    await _lock_extra_projects_on_expiry(uid)
    await update.message.reply_text(f"✅ Premium removed from `{uid}`. Extra projects locked.", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

@admin_or_owner
async def cb_admin_temp_premium(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await safe_edit(query, "⏰ *Temp Premium*\n\nSend the user ID:", parse_mode=ParseMode.MARKDOWN)
    return ADMIN_TEMP_PREMIUM_ID

async def admin_temp_premium_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid ID:", parse_mode=ParseMode.MARKDOWN)
        return ADMIN_TEMP_PREMIUM_ID
    context.user_data["temp_premium_uid"] = uid
    await update.message.reply_text(
        "⏰ Send duration (e.g. `24h` or `7d`):",
        parse_mode=ParseMode.MARKDOWN,
    )
    return ADMIN_TEMP_PREMIUM_DUR

async def admin_temp_premium_dur(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    uid  = context.user_data.get("temp_premium_uid")
    m = re.match(r"^(\d+)([hd])$", text)
    if not m:
        await update.message.reply_text("❌ Invalid format. Use `24h` or `7d`:", parse_mode=ParseMode.MARKDOWN)
        return ADMIN_TEMP_PREMIUM_DUR

    amount, unit = int(m.group(1)), m.group(2)
    seconds = amount * 3600 if unit == "h" else amount * 86400
    expiry  = datetime.fromtimestamp(time.time() + seconds, tz=timezone.utc)

    await users_col.update_one(
        {"user_id": uid},
        {"$set": {"is_premium": True, "premium_expiry": expiry}},
    )
    # Unlock all projects
    await projects_col.update_many({"user_id": uid}, {"$set": {"locked": False}})

    try:
        await update.get_bot().send_message(uid, f"🎉 You received *Temp Premium* for {escape_md(text)}! All projects unlocked!", parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass
    await update.message.reply_text(
        f"✅ Temp premium set for `{uid}` — expires {escape_md(expiry.strftime('%Y-%m-%d %H:%M UTC'))}.",
        parse_mode=ParseMode.MARKDOWN,
    )
    return ConversationHandler.END

@admin_or_owner
async def cb_admin_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await safe_edit(query, "🚫 *Ban User*\n\nSend the user ID:", parse_mode=ParseMode.MARKDOWN)
    return ADMIN_BAN_ID

async def admin_ban_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid ID:", parse_mode=ParseMode.MARKDOWN)
        return ADMIN_BAN_ID

    await users_col.update_one({"user_id": uid}, {"$set": {"is_banned": True}})
    user_projects = await projects_col.find({"user_id": uid, "status": "running"}).to_list(length=100)
    for p in user_projects:
        await kill_project(uid, p["name"])
    await update.message.reply_text(f"✅ User `{uid}` banned and all projects stopped.", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

@admin_or_owner
async def cb_admin_unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await safe_edit(query, "✅ *Unban User*\n\nSend the user ID:", parse_mode=ParseMode.MARKDOWN)
    return ADMIN_UNBAN_ID

async def admin_unban_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid ID:", parse_mode=ParseMode.MARKDOWN)
        return ADMIN_UNBAN_ID

    await users_col.update_one({"user_id": uid}, {"$set": {"is_banned": False}})
    try:
        await update.get_bot().send_message(uid, "✅ You have been unbanned! You can use the bot again.", parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass
    await update.message.reply_text(f"✅ User `{uid}` unbanned.", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

@admin_or_owner
async def cb_admin_broadcast_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Broadcast All",  callback_data="admin:broadcast_all")],
        [InlineKeyboardButton("📩 Send to User",   callback_data="admin:send_to_user")],
        [InlineKeyboardButton("🔙 Back",           callback_data="admin_panel")],
    ])
    await safe_edit(query, "📢 *Broadcast Menu*", reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

@admin_or_owner
async def cb_admin_broadcast_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["broadcast_type"] = "all"
    await safe_edit(query, "📢 *Broadcast All*\n\nSend the message:", parse_mode=ParseMode.MARKDOWN)
    return ADMIN_BROADCAST_MSG

@admin_or_owner
async def cb_admin_send_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await safe_edit(query, "📩 *Send to User*\n\nSend the target user ID:", parse_mode=ParseMode.MARKDOWN)
    return ADMIN_SEND_USER_ID

async def admin_send_user_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid ID:", parse_mode=ParseMode.MARKDOWN)
        return ADMIN_SEND_USER_ID
    context.user_data["broadcast_target"] = uid
    await update.message.reply_text("Send the message:", parse_mode=ParseMode.MARKDOWN)
    return ADMIN_SEND_USER_MSG

async def admin_send_user_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = context.user_data.get("broadcast_target")
    msg = update.message.text
    try:
        await update.get_bot().send_message(uid, msg)
        await update.message.reply_text(f"✅ Sent to `{uid}`.", parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await update.message.reply_text(f"❌ Failed: {escape_md(str(e))}", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

async def admin_broadcast_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg  = update.message.text
    bot  = update.get_bot()
    all_users = await users_col.find({}).to_list(length=10000)
    sent = failed = 0
    for u in all_users:
        try:
            await bot.send_message(u["user_id"], msg)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)  # Rate limit friendly
    await update.message.reply_text(
        f"📢 Broadcast complete!\n✅ Sent: `{sent}`\n❌ Failed: `{failed}`",
        parse_mode=ParseMode.MARKDOWN,
    )
    return ConversationHandler.END

async def admin_conv_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    if update.callback_query:
        await update.callback_query.answer()
    await (update.effective_message).reply_text("❌ Cancelled.", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

# ─────────────────────────────────────────────────────────────
# 🛡 Add / Remove Admin (Owner only)
# ─────────────────────────────────────────────────────────────

@owner_only
async def cb_admin_add_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await safe_edit(
        query,
        "🛡 *Add Admin*\n\nSend the user ID:",
        parse_mode=ParseMode.MARKDOWN,
    )
    return ADMIN_ADD_ADMIN_ID

async def admin_add_admin_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid ID:", parse_mode=ParseMode.MARKDOWN)
        return ADMIN_ADD_ADMIN_ID

    if uid == OWNER_ID:
        await update.message.reply_text("⚠️ Owner is already the highest role!", parse_mode=ParseMode.MARKDOWN)
        return ConversationHandler.END

    await users_col.update_one(
        {"user_id": uid},
        {"$set": {"is_admin": True}},
        upsert=False,
    )
    try:
        await update.get_bot().send_message(
            uid,
            "🎉 You have been made *Admin*! You can now access the Admin Panel.",
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception:
        pass
    await update.message.reply_text(f"✅ User `{uid}` is now Admin.", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

@owner_only
async def cb_admin_remove_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await safe_edit(
        query,
        "➖ *Remove Admin*\n\nSend the user ID:",
        parse_mode=ParseMode.MARKDOWN,
    )
    return ADMIN_REMOVE_ADMIN_ID

async def admin_remove_admin_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid ID:", parse_mode=ParseMode.MARKDOWN)
        return ADMIN_REMOVE_ADMIN_ID

    await users_col.update_one({"user_id": uid}, {"$set": {"is_admin": False}})
    try:
        await update.get_bot().send_message(
            uid,
            "⚠️ Your *Admin* access has been removed.",
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception:
        pass
    await update.message.reply_text(f"✅ Admin access removed from `{uid}`.", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

# ─────────────────────────────────────────────────────────────
# ⏰ Auto-Restart Toggle
# ─────────────────────────────────────────────────────────────

async def cb_toggle_auto_restart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    name = query.data.split(":", 1)[1]

    p = await get_project(uid, name)
    if not p:
        await query.answer("❌ Project not found.", show_alert=True)
        return

    current = p.get("auto_restart", True)
    new_val = not current

    await projects_col.update_one(
        {"user_id": uid, "name": name},
        {"$set": {"auto_restart": new_val}},
    )

    status = "✅ ON" if new_val else "❌ OFF"
    await query.answer(f"Auto-Restart: {status}", show_alert=True)

    p = await get_project(uid, name)
    await safe_edit(
        query,
        project_dashboard_text(p),
        reply_markup=project_dashboard_kb(uid, name, p.get("auto_restart", True),
                                          p.get("status") == "running", p.get("locked", False)),
    )

# ─────────────────────────────────────────────────────────────
# 🔐 Environment Variables Manager
# ─────────────────────────────────────────────────────────────

async def cb_envvars(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    name = query.data.split(":", 1)[1]

    if await is_banned(uid):
        await safe_edit(query, "🚫 You are banned. Contact owner.")
        return

    pdir = project_dir(uid, name)
    env_path = os.path.join(pdir, ".env")

    env_vars = {}
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, value = line.partition("=")
                    env_vars[key.strip()] = value.strip()

    if not env_vars:
        text = f"🔐 *Environment Variables — {escape_md(name)}*\n\nNo variables set yet.\n\n_Tip: Click Add Variable and send like:_\n`BOT_TOKEN=your_value`"
    else:
        lines = [f"🔐 *Environment Variables — {escape_md(name)}*\n"]
        for key, value in env_vars.items():
            masked = value[:3] + "***" if len(value) > 3 else "***"
            lines.append(f"• `{key}` = `{masked}`")
        text = "\n".join(lines)

    kb_rows = []
    for key in env_vars:
        kb_rows.append([
            InlineKeyboardButton(f"✏️ {key}", callback_data=f"env_edit:{name}:{key}"),
            InlineKeyboardButton(f"🗑 {key}", callback_data=f"env_del:{name}:{key}"),
        ])
    kb_rows.append([InlineKeyboardButton("➕ Add Variable", callback_data=f"env_add:{name}")])
    kb_rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"proj:{name}")])

    await safe_edit(query, text, reply_markup=InlineKeyboardMarkup(kb_rows), parse_mode=ParseMode.MARKDOWN)

async def cb_env_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data.split(":", 1)[1]
    context.user_data["env_project"] = name

    await safe_edit(
        query,
        "➕ *Add Environment Variables*\n\n"
        "Send in any format:\n\n"
        "1️⃣ *Single:*\n`API_KEY=your_value`\n\n"
        "2️⃣ *Multiple (one per line):*\n`TOKEN=abc123`\n`DB_URI=mongodb://...`\n\n"
        "3️⃣ *Just key name:*\n`API_KEY`\n_(bot will ask for value next)_",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"envvars:{name}")]]),
        parse_mode=ParseMode.MARKDOWN,
    )
    return ENV_ADD_KEY

async def env_add_key(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    name = context.user_data.get("env_project")
    uid = update.effective_user.id
    pdir = project_dir(uid, name)
    env_path = os.path.join(pdir, ".env")

    lines = text.strip().split("\n")
    pairs_to_save = []

    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if key:
                pairs_to_save.append((key, value))

    if pairs_to_save:
        # Read existing env file
        existing = {}
        existing_order = []
        if os.path.exists(env_path):
            with open(env_path, "r") as f:
                for eline in f:
                    eline_stripped = eline.strip()
                    if eline_stripped and not eline_stripped.startswith("#") and "=" in eline_stripped:
                        ekey, _, evalue = eline_stripped.partition("=")
                        ekey = ekey.strip()
                        existing[ekey] = evalue.strip()
                        if ekey not in existing_order:
                            existing_order.append(ekey)

        # Update/add new pairs
        for key, value in pairs_to_save:
            existing[key] = value
            if key not in existing_order:
                existing_order.append(key)

        # FIX: Write all vars back (removed duplicate write bug)
        with open(env_path, "w") as f:
            for key in existing_order:
                f.write(f"{key}={existing[key]}\n")

        saved_keys = [k for k, v in pairs_to_save]
        saved_list = "\n".join([f"• `{k}` ✅" for k in saved_keys])

        await update.message.reply_text(
            f"✅ *{len(pairs_to_save)} variable(s) saved!*\n\n"
            f"{saved_list}\n\n"
            f"_Restart your project for changes to take effect._",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Add More", callback_data=f"env_add:{name}")],
                [InlineKeyboardButton("🔙 Back to Env Vars", callback_data=f"envvars:{name}")],
            ]),
        )
        context.user_data.pop("env_key", None)
        context.user_data.pop("env_project", None)
        return ConversationHandler.END

    key = text.strip().split()[0] if text.strip() else ""
    if not key or len(key) > 100:
        await update.message.reply_text(
            "❌ Could not parse variables.\n\n"
            "Send like: `API_KEY=your_value`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return ENV_ADD_KEY

    context.user_data["env_key"] = key
    await update.message.reply_text(
        f"Now send the value for `{key}`:",
        parse_mode=ParseMode.MARKDOWN,
    )
    return ENV_ADD_VALUE

async def env_add_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    value = update.message.text.strip()
    name = context.user_data.get("env_project")
    key = context.user_data.get("env_key")
    uid = update.effective_user.id

    pdir = project_dir(uid, name)
    env_path = os.path.join(pdir, ".env")

    env_lines = []
    key_found = False
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if line.strip().startswith(f"{key}="):
                    env_lines.append(f"{key}={value}\n")
                    key_found = True
                else:
                    env_lines.append(line)

    if not key_found:
        env_lines.append(f"{key}={value}\n")

    with open(env_path, "w") as f:
        f.writelines(env_lines)

    await update.message.reply_text(
        f"✅ Variable `{key}` saved!\n\n_Restart your project for changes to take effect._",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Env Vars", callback_data=f"envvars:{name}")]]),
    )
    context.user_data.pop("env_key", None)
    context.user_data.pop("env_project", None)
    return ConversationHandler.END

async def cb_env_edit_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split(":", 2)
    name = parts[1]
    key = parts[2]
    context.user_data["env_project"] = name
    context.user_data["env_key"] = key

    await safe_edit(
        query,
        f"✏️ *Edit `{key}`*\n\nSend the new value:",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"envvars:{name}")]]),
        parse_mode=ParseMode.MARKDOWN,
    )
    return ENV_EDIT_VALUE

async def env_edit_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await env_add_value(update, context)

async def cb_env_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()  # FIX: was missing answer(), causing Telegram timeout
    parts = query.data.split(":", 2)
    name = parts[1]
    key = parts[2]
    uid = query.from_user.id

    pdir = project_dir(uid, name)
    env_path = os.path.join(pdir, ".env")

    deleted = False
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            lines = f.readlines()
        with open(env_path, "w") as f:
            for line in lines:
                if line.strip().startswith(f"{key}="):
                    deleted = True
                    continue
                f.write(line)

    if deleted:
        await query.answer(f"🗑 {key} deleted!", show_alert=True)
    else:
        await query.answer(f"⚠️ {key} not found.", show_alert=True)

    # Refresh the env vars screen
    env_vars = {}
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    env_vars[k.strip()] = v.strip()

    if not env_vars:
        text = (
            f"🔐 *Environment Variables — {escape_md(name)}*\n\n"
            f"No variables set yet.\n\n"
            f"_Tip: Click Add Variable and send like:_\n`BOT_TOKEN=your_value`"
        )
    else:
        lines_out = [f"🔐 *Environment Variables — {escape_md(name)}*\n"]
        for k, v in env_vars.items():
            masked = v[:3] + "***" if len(v) > 3 else "***"
            lines_out.append(f"• `{k}` = `{masked}`")
        text = "\n".join(lines_out)

    kb_rows = []
    for k in env_vars:
        kb_rows.append([
            InlineKeyboardButton(f"✏️ {k}", callback_data=f"env_edit:{name}:{k}"),
            InlineKeyboardButton(f"🗑 {k}", callback_data=f"env_del:{name}:{k}"),
        ])
    kb_rows.append([InlineKeyboardButton("➕ Add Variable", callback_data=f"env_add:{name}")])
    kb_rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"proj:{name}")])

    await safe_edit(query, text, reply_markup=InlineKeyboardMarkup(kb_rows), parse_mode=ParseMode.MARKDOWN)

# ─────────────────────────────────────────────────────────────
# 🔄 Process Monitor (auto-restart, crash notifications)
# ─────────────────────────────────────────────────────────────

async def process_monitor():
    while True:
        await asyncio.sleep(30)
        try:
            running = await projects_col.find({"status": "running"}).to_list(length=1000)
            for p in running:
                pid = p.get("pid")
                if pid and not psutil.pid_exists(pid):
                    key  = f"{p['user_id']}:{p['name']}"
                    proc = context_store.get(key)
                    code = None
                    if proc:
                        code = proc.returncode

                    await projects_col.update_one(
                        {"user_id": p["user_id"], "name": p["name"]},
                        {"$set": {"status": "stopped", "pid": None, "exit_code": code}},
                    )
                    context_store.pop(key, None)  # RAM optimization: remove stale entry

                    logger.info(f"Process {key} exited with code {code}")

                    # Auto-restart logic
                    if p.get("auto_restart", True) and code != 0 and not p.get("admin_stopped") and not p.get("locked"):
                        now = datetime.now(timezone.utc)
                        last_restart = p.get("last_restart_at")
                        restart_count = p.get("restart_count", 0)

                        if last_restart:
                            if last_restart.tzinfo is None:
                                last_restart = last_restart.replace(tzinfo=timezone.utc)
                            if (now - last_restart).total_seconds() > 300:
                                restart_count = 0

                        if restart_count < 3:
                            try:
                                logger.info(f"Auto-restarting {key} (attempt {restart_count + 1}/3)")
                                await asyncio.sleep(3)
                                await start_project_process(p["user_id"], p["name"])
                                await projects_col.update_one(
                                    {"user_id": p["user_id"], "name": p["name"]},
                                    {"$set": {"restart_count": restart_count + 1, "last_restart_at": now}},
                                )
                                if notification_bot:
                                    try:
                                        await notification_bot.send_message(
                                            chat_id=p["user_id"],
                                            text=(
                                                f"🔄 *Auto-Restart*\n\n"
                                                f"Project `{p['name']}` crashed (exit code: {code}).\n"
                                                f"Auto-restarted successfully! ({restart_count + 1}/3)"
                                            ),
                                            parse_mode=ParseMode.MARKDOWN,
                                        )
                                    except Exception:
                                        pass
                            except Exception as e:
                                logger.error(f"Auto-restart failed for {key}: {e}")
                        else:
                            logger.warning(f"Auto-restart limit reached for {key}")
                            if notification_bot:
                                try:
                                    await notification_bot.send_message(
                                        chat_id=p["user_id"],
                                        text=(
                                            f"⚠️ *Auto-Restart Limit Reached*\n\n"
                                            f"Project `{p['name']}` crashed {restart_count} times in 5 minutes.\n"
                                            f"Auto-restart disabled temporarily.\n\n"
                                            f"Please check your logs and restart manually."
                                        ),
                                        parse_mode=ParseMode.MARKDOWN,
                                    )
                                except Exception:
                                    pass

                    elif code != 0 and not p.get("admin_stopped") and not p.get("locked"):
                        if notification_bot:
                            try:
                                log_path = os.path.join(project_dir(p["user_id"], p["name"]), "output.log")
                                error_lines = ""
                                if os.path.exists(log_path):
                                    with open(log_path, "r", errors="replace") as f:
                                        lines_list = f.readlines()
                                    error_lines = "".join(lines_list[-10:]).strip()
                                    if len(error_lines) > 500:
                                        error_lines = "..." + error_lines[-500:]

                                msg_text = (
                                    f"❌ *Project Crashed*\n\n"
                                    f"Project: `{p['name']}`\n"
                                    f"Exit Code: `{code}`\n"
                                    f"Auto-Restart: OFF\n\n"
                                    f"📋 *Last Log Lines:*\n```\n{error_lines}\n```"
                                )
                                if len(msg_text) > 4000:
                                    msg_text = msg_text[:4000] + "..."

                                await notification_bot.send_message(
                                    chat_id=p["user_id"],
                                    text=msg_text,
                                    parse_mode=ParseMode.MARKDOWN,
                                )
                            except Exception:
                                pass

        except Exception as e:
            logger.warning(f"Monitor error: {e}")

# ─────────────────────────────────────────────────────────────
# 💾 Auto Backup Task
# ─────────────────────────────────────────────────────────────

async def backup_task():
    while True:
        await asyncio.sleep(300)
        try:
            all_projects = await projects_col.find({}).to_list(length=10000)
            db_distribution = {}
            total_files = 0
            total_size  = 0

            for proj in all_projects:
                uid  = proj["user_id"]
                name = proj["name"]
                pdir = project_dir(uid, name)

                if not os.path.exists(pdir):
                    continue

                files_data = []
                for root, dirs, files in os.walk(pdir):
                    dirs[:] = [d for d in dirs if d not in ("venv", "__pycache__", ".git", "node_modules")]
                    for fname in files:
                        if fname in ("output.log",) or fname.endswith(".pyc"):
                            continue
                        fpath    = os.path.join(root, fname)
                        rel_path = os.path.relpath(fpath, pdir)
                        try:
                            try:
                                with open(fpath, "r", encoding="utf-8") as f:
                                    content = f.read()
                                content_b64 = base64.b64encode(content.encode("utf-8")).decode("ascii")
                                is_binary = False
                            except (UnicodeDecodeError, ValueError):
                                with open(fpath, "rb") as f:
                                    content_bytes = f.read()
                                content_b64 = base64.b64encode(content_bytes).decode("ascii")
                                is_binary = True

                            file_size = os.path.getsize(fpath)
                            if file_size > 15 * 1024 * 1024:
                                continue

                            files_data.append({
                                "path": rel_path, "content_b64": content_b64,
                                "size": file_size, "is_binary": is_binary,
                            })
                            total_files += 1
                            total_size  += file_size
                        except Exception:
                            continue

                if files_data:
                    target_db_name, target_col = pick_backup_col(uid, name)
                    for col in all_backup_cols():
                        try:
                            await col.delete_many({"type": "file_backup", "user_id": uid, "project_name": name})
                        except Exception:
                            pass
                    await target_col.insert_one({
                        "type": "file_backup", "user_id": uid,
                        "project_name": name, "files": files_data,
                        "backed_up_at": datetime.now(timezone.utc),
                        "stored_in": target_db_name,
                    })
                    db_distribution[target_db_name] = db_distribution.get(target_db_name, 0) + 1

            await backups_col.delete_many({"type": "backup_meta"})
            await backups_col.insert_one({
                "type": "backup_meta", "total_projects": len(all_projects),
                "total_files": total_files, "total_size": total_size,
                "backed_up_at": datetime.now(timezone.utc),
                "distribution": db_distribution,
            })
            logger.info(f"Auto backup: {len(all_projects)} projects, {total_files} files — distribution: {db_distribution}")

        except Exception as e:
            logger.error(f"Backup failed: {e}")

# ─────────────────────────────────────────────────────────────
# 🧹 RAM Optimization Task
# ─────────────────────────────────────────────────────────────

async def ram_cleanup_task():
    """Periodic RAM cleanup: rotate logs, clean context_store, force GC."""
    while True:
        await asyncio.sleep(120)  # every 2 minutes
        try:
            # Clean up stale context_store entries (processes that no longer exist)
            stale_keys = []
            for key, proc in list(context_store.items()):
                try:
                    if proc.returncode is not None:
                        stale_keys.append(key)
                    elif proc.pid and not psutil.pid_exists(proc.pid):
                        stale_keys.append(key)
                except Exception:
                    stale_keys.append(key)
            for key in stale_keys:
                context_store.pop(key, None)
            if stale_keys:
                logger.info(f"RAM cleanup: removed {len(stale_keys)} stale context_store entries")

            # Rotate large log files for all projects
            try:
                for user_dir in os.listdir(PROJECTS_ROOT):
                    user_path = os.path.join(PROJECTS_ROOT, user_dir)
                    if not os.path.isdir(user_path):
                        continue
                    for proj_dir in os.listdir(user_path):
                        log_path = os.path.join(user_path, proj_dir, "output.log")
                        rotate_log_if_needed(log_path)
            except Exception:
                pass

            # Force garbage collection
            gc.collect()

        except Exception as e:
            logger.warning(f"RAM cleanup error: {e}")

# ─────────────────────────────────────────────────────────────
# 🔄 Keep-Alive Task
# ─────────────────────────────────────────────────────────────

async def keep_alive_task():
    """Ping health endpoint every 10 minutes, clean expired tokens."""
    import urllib.request
    health_url = f"{BASE_URL}/health"
    logger.info(f"Keep-alive task started. Pinging {health_url} every 10 minutes.")

    while True:
        await asyncio.sleep(600)

        try:
            result = await tokens_col.delete_many(
                {"expires_at": {"$lt": datetime.now(timezone.utc)}}
            )
            if result.deleted_count:
                logger.info(f"Cleaned {result.deleted_count} expired tokens")
        except Exception as e:
            logger.warning(f"Token cleanup failed: {e}")

        try:
            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(
                None,
                lambda: urllib.request.urlopen(health_url, timeout=30).status
            )
            logger.info(f"Keep-alive ping OK ({resp})")
        except Exception as e:
            logger.warning(f"Keep-alive ping failed: {e}")

# ─────────────────────────────────────────────────────────────
# 🔄 Auto Restore (startup)
# ─────────────────────────────────────────────────────────────

async def restore_from_backup():
    try:
        logger.info("Checking for backups to restore...")

        meta = await backups_col.find_one({"type": "backup_meta"})
        if not meta:
            logger.info("No backup found. Fresh start.")
            return

        logger.info(
            f"Found backup from {meta['backed_up_at']} — "
            f"{meta['total_projects']} projects, {meta['total_files']} files"
        )

        seen = {}
        for col in all_backup_cols():
            try:
                async for backup in col.find({"type": "file_backup"}):
                    key = (backup["user_id"], backup["project_name"])
                    existing = seen.get(key)
                    if (existing is None
                            or backup.get("backed_up_at", datetime.min.replace(tzinfo=timezone.utc))
                                > existing.get("backed_up_at", datetime.min.replace(tzinfo=timezone.utc))):
                        seen[key] = backup
            except Exception as e:
                logger.warning(f"Restore read failed on one DB: {e}")

        restored_projects = 0
        restored_files    = 0

        for backup in seen.values():
            uid  = backup["user_id"]
            name = backup["project_name"]
            pdir = project_dir(uid, name)
            os.makedirs(pdir, exist_ok=True)

            for file_data in backup.get("files", []):
                rel_path    = file_data["path"]
                content_b64 = file_data["content_b64"]
                is_binary   = file_data.get("is_binary", False)
                file_path = os.path.join(pdir, rel_path)
                parent_dir = os.path.dirname(file_path)
                if parent_dir:
                    os.makedirs(parent_dir, exist_ok=True)
                try:
                    decoded = base64.b64decode(content_b64)
                    if is_binary:
                        with open(file_path, "wb") as f:
                            f.write(decoded)
                    else:
                        with open(file_path, "w", encoding="utf-8") as f:
                            f.write(decoded.decode("utf-8"))
                    restored_files += 1
                except Exception as e:
                    logger.warning(f"Failed to restore {rel_path}: {e}")

            restored_projects += 1

        logger.info(f"Files restored: {restored_projects} projects, {restored_files} files")
        asyncio.create_task(setup_venvs_background())
        asyncio.create_task(auto_restart_on_startup())

    except Exception as e:
        logger.error(f"Restore failed (non-fatal): {e}")

async def _install_requirements_for_project(uid: int, name: str) -> tuple:
    pdir     = project_dir(uid, name)
    req_path = os.path.join(pdir, "requirements.txt")
    pkg_json = os.path.join(pdir, "package.json")
    venv_dir = os.path.join(pdir, "venv")
    pip_path = os.path.join(venv_dir, "bin", "pip")

    if os.path.exists(pkg_json) and not os.path.exists(req_path):
        try:
            proc = await asyncio.wait_for(
                create_subprocess_exec("npm", "install", "--no-audit", "--no-fund",
                                       stdout=PIPE, stderr=PIPE, cwd=pdir),
                timeout=300,
            )
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
            if proc.returncode == 0:
                return (True, "npm install success")
            else:
                return (False, f"npm install failed: {stderr.decode()[:200]}")
        except asyncio.TimeoutError:
            return (False, "npm install timed out")
        except Exception as e:
            return (False, f"npm error: {e}")

    if not os.path.exists(req_path):
        return (True, "no requirements file, skip")

    if not os.path.exists(pip_path):
        try:
            proc = await asyncio.wait_for(
                create_subprocess_exec(sys.executable, "-m", "venv", venv_dir, stdout=PIPE, stderr=PIPE),
                timeout=120,
            )
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
            if proc.returncode != 0:
                return (False, f"venv create failed: {stderr.decode()[:200]}")
        except Exception as e:
            return (False, f"venv error: {e}")

    try:
        proc = await asyncio.wait_for(
            create_subprocess_exec(pip_path, "install", "-r", req_path, stdout=PIPE, stderr=PIPE, cwd=pdir),
            timeout=300,
        )
        _, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
        if proc.returncode == 0:
            return (True, "pip install success")
        else:
            return (False, f"pip install failed: {stderr.decode()[:300]}")
    except asyncio.TimeoutError:
        return (False, "pip install timed out")
    except Exception as e:
        return (False, f"pip error: {e}")

async def auto_restart_on_startup():
    """Restart projects that were running before bot restart. Respects bot_lock & maintenance."""
    await asyncio.sleep(30)
    try:
        # Don't auto-restart if maintenance mode is on
        if await is_maintenance_mode():
            logger.info("Maintenance mode ON — skipping auto-restart on startup")
            return

        running_projects = await projects_col.find({
            "status": "running",
            "admin_stopped": {"$ne": True},
            "locked": {"$ne": True},
        }).to_list(length=10000)

        if not running_projects:
            logger.info("Auto-restart on startup: no running projects found.")
            return

        logger.info(f"Auto-restart on startup: {len(running_projects)} projects...")
        bot_locked = await is_bot_locked()

        for proj in running_projects:
            uid  = proj["user_id"]
            name = proj["name"]
            try:
                # Bot lock: skip free users' projects
                if bot_locked and uid != OWNER_ID:
                    user_prem = await is_premium(uid)
                    if not user_prem:
                        await projects_col.update_one(
                            {"user_id": uid, "name": name},
                            {"$set": {"status": "stopped", "pid": None}},
                        )
                        logger.info(f"Skipped {uid}:{name} — bot locked and user is free")
                        continue

                await projects_col.update_one(
                    {"user_id": uid, "name": name},
                    {"$set": {"status": "stopped", "pid": None}},
                )

                if notification_bot:
                    try:
                        await notification_bot.send_message(
                            chat_id=uid,
                            text=(
                                f"🔄 *Bot Restarted*\n\n"
                                f"Project `{name}` requirements are being installed...\n"
                                f"⏳ Your project will start automatically in a few moments."
                            ),
                            parse_mode=ParseMode.MARKDOWN,
                        )
                    except Exception:
                        pass

                logger.info(f"Installing requirements for {uid}:{name} before startup...")
                success, msg = await _install_requirements_for_project(uid, name)
                logger.info(f"Requirements for {uid}:{name}: {msg}")

                await asyncio.sleep(1)
                updated = await start_project_process(uid, name)
                logger.info(f"Auto-restarted on startup: {uid}:{name} PID={updated.get('pid')}")

                if notification_bot:
                    try:
                        req_status = "✅ Requirements installed" if success else f"⚠️ Issue: {msg[:100]}"
                        await notification_bot.send_message(
                            chat_id=uid,
                            text=(
                                f"✅ *Project Started*\n\n"
                                f"Project: `{name}`\n"
                                f"{req_status}\n"
                                f"🟢 Your project is running!"
                            ),
                            parse_mode=ParseMode.MARKDOWN,
                        )
                    except Exception:
                        pass

            except Exception as e:
                logger.error(f"Auto-restart on startup failed for {uid}:{name}: {e}")
                await projects_col.update_one(
                    {"user_id": uid, "name": name},
                    {"$set": {"status": "stopped", "pid": None}},
                )
                if notification_bot:
                    try:
                        await notification_bot.send_message(
                            chat_id=uid,
                            text=(
                                f"❌ *Project Start Failed*\n\n"
                                f"Project `{name}` could not start after bot restart.\n"
                                f"Error: `{str(e)[:200]}`\n\n"
                                f"Please start it manually."
                            ),
                            parse_mode=ParseMode.MARKDOWN,
                        )
                    except Exception:
                        pass

        logger.info("Auto-restart on startup complete.")
    except Exception as e:
        logger.error(f"auto_restart_on_startup failed: {e}")

async def setup_venvs_background():
    try:
        all_projects = await projects_col.find({}).to_list(length=10000)
        for proj in all_projects:
            uid  = proj["user_id"]
            name = proj["name"]
            pdir = project_dir(uid, name)
            venv_dir = os.path.join(pdir, "venv")

            if os.path.exists(pdir) and not os.path.exists(venv_dir):
                try:
                    proc = await create_subprocess_exec(
                        sys.executable, "-m", "venv", venv_dir, stdout=PIPE, stderr=PIPE
                    )
                    await asyncio.wait_for(proc.communicate(), timeout=120)

                    req_file = os.path.join(pdir, "requirements.txt")
                    pip_path = os.path.join(pdir, "venv", "bin", "pip")
                    if os.path.exists(req_file) and os.path.exists(pip_path):
                        proc2 = await create_subprocess_exec(
                            pip_path, "install", "-r", req_file, "--quiet",
                            stdout=PIPE, stderr=PIPE, cwd=pdir
                        )
                        await asyncio.wait_for(proc2.communicate(), timeout=300)
                    logger.info(f"Venv setup complete for {name}")
                except Exception as e:
                    logger.warning(f"Failed to setup venv for {name}: {e}")
    except Exception as e:
        logger.error(f"Background venv setup failed: {e}")

# ─────────────────────────────────────────────────────────────
# App setup
# ─────────────────────────────────────────────────────────────

def build_application() -> Application:
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)
        .post_init(post_init)
        .build()
    )

    # New project conversation
    new_proj_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(cb_new_project, pattern="^new_project$")],
        states={
            NEW_PROJECT_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, new_project_name),
                CallbackQueryHandler(new_project_cancel, pattern="^back_start$"),
            ],
            NEW_PROJECT_FILES: [
                MessageHandler(filters.Document.ALL, new_project_file),
                CommandHandler("done", new_project_done_cmd),
                CallbackQueryHandler(new_project_done_cb, pattern="^upload_done$"),
                CallbackQueryHandler(new_project_cancel, pattern="^back_start$"),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", new_project_cancel),
            CommandHandler("start", new_project_cancel),
        ],
        per_chat=True,
    )

    editcmd_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(cb_editcmd_start, pattern=r"^editcmd:")],
        states={
            EDIT_RUN_CMD: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, editcmd_receive),
                CallbackQueryHandler(admin_conv_cancel, pattern=r"^proj:"),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", admin_conv_cancel),
            CommandHandler("start", admin_conv_cancel),
        ],
        per_chat=True,
    )

    env_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(cb_env_add_start,  pattern=r"^env_add:"),
            CallbackQueryHandler(cb_env_edit_start, pattern=r"^env_edit:"),
        ],
        states={
            ENV_ADD_KEY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, env_add_key),
            ],
            ENV_ADD_VALUE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, env_add_value),
            ],
            ENV_EDIT_VALUE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, env_edit_value),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", admin_conv_cancel),
            CommandHandler("start", admin_conv_cancel),
        ],
        per_chat=True,
    )

    admin_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(cb_admin_give_premium,   pattern="^admin:give_premium$"),
            CallbackQueryHandler(cb_admin_remove_premium, pattern="^admin:remove_premium$"),
            CallbackQueryHandler(cb_admin_temp_premium,   pattern="^admin:temp_premium$"),
            CallbackQueryHandler(cb_admin_ban,            pattern="^admin:ban$"),
            CallbackQueryHandler(cb_admin_unban,          pattern="^admin:unban$"),
            CallbackQueryHandler(cb_admin_broadcast_all,  pattern="^admin:broadcast_all$"),
            CallbackQueryHandler(cb_admin_send_to_user,   pattern="^admin:send_to_user$"),
            CallbackQueryHandler(cb_admin_add_admin,      pattern="^admin:add_admin$"),
            CallbackQueryHandler(cb_admin_remove_admin,   pattern="^admin:remove_admin$"),
        ],
        states={
            ADMIN_GIVE_PREMIUM_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_give_premium_id),
                CallbackQueryHandler(admin_conv_cancel, pattern="^admin_panel$"),
            ],
            ADMIN_REMOVE_PREMIUM_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_remove_premium_id),
                CallbackQueryHandler(admin_conv_cancel, pattern="^admin_panel$"),
            ],
            ADMIN_TEMP_PREMIUM_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_temp_premium_id),
                CallbackQueryHandler(admin_conv_cancel, pattern="^admin_panel$"),
            ],
            ADMIN_TEMP_PREMIUM_DUR: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_temp_premium_dur),
                CallbackQueryHandler(admin_conv_cancel, pattern="^admin_panel$"),
            ],
            ADMIN_BAN_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_ban_id),
                CallbackQueryHandler(admin_conv_cancel, pattern="^admin_panel$"),
            ],
            ADMIN_UNBAN_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_unban_id),
                CallbackQueryHandler(admin_conv_cancel, pattern="^admin_panel$"),
            ],
            ADMIN_BROADCAST_MSG: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_broadcast_msg),
                CallbackQueryHandler(admin_conv_cancel, pattern="^admin_panel$"),
            ],
            ADMIN_SEND_USER_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_send_user_id),
                CallbackQueryHandler(admin_conv_cancel, pattern="^admin_panel$"),
            ],
            ADMIN_SEND_USER_MSG: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_send_user_msg),
                CallbackQueryHandler(admin_conv_cancel, pattern="^admin_panel$"),
            ],
            ADMIN_ADD_ADMIN_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_admin_id),
                CallbackQueryHandler(admin_conv_cancel, pattern="^admin_panel$"),
            ],
            ADMIN_REMOVE_ADMIN_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_remove_admin_id),
                CallbackQueryHandler(admin_conv_cancel, pattern="^admin_panel$"),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", admin_conv_cancel),
            CommandHandler("start", admin_conv_cancel),
        ],
        per_chat=True,
    )

    # Register conversations first
    app.add_handler(new_proj_conv)
    app.add_handler(editcmd_conv)
    app.add_handler(env_conv)
    app.add_handler(admin_conv)

    app.add_handler(CommandHandler("start", start))

    # Callback handlers
    app.add_handler(CallbackQueryHandler(cb_start,             pattern="^back_start$"))
    app.add_handler(CallbackQueryHandler(cb_my_projects,       pattern="^my_projects$"))
    app.add_handler(CallbackQueryHandler(cb_my_status,         pattern="^my_status$"))
    app.add_handler(CallbackQueryHandler(cb_bot_status,        pattern="^bot_status$"))
    app.add_handler(CallbackQueryHandler(cb_premium,           pattern="^premium$"))
    app.add_handler(CallbackQueryHandler(cb_admin_panel,       pattern="^admin_panel$"))
    app.add_handler(CallbackQueryHandler(cb_admin_user_list,   pattern=r"^admin:user_list:\d+$"))
    app.add_handler(CallbackQueryHandler(cb_admin_running,     pattern="^admin:running$"))
    app.add_handler(CallbackQueryHandler(cb_admin_stop_project, pattern=r"^admin_stop:"))
    app.add_handler(CallbackQueryHandler(cb_admin_broadcast_menu, pattern="^admin:broadcast_menu$"))
    app.add_handler(CallbackQueryHandler(cb_admin_backup_now,  pattern="^admin:backup_now$"))
    app.add_handler(CallbackQueryHandler(cb_admin_delete_backups,         pattern="^admin:del_backups$"))
    app.add_handler(CallbackQueryHandler(cb_admin_delete_backups_confirm, pattern="^admin:del_backups_confirm$"))
    app.add_handler(CallbackQueryHandler(cb_admin_all_projects,     pattern=r"^admin:all_projects:\d+$"))
    app.add_handler(CallbackQueryHandler(cb_admin_run_project,      pattern=r"^admin_run:"))
    app.add_handler(CallbackQueryHandler(cb_admin_download_project, pattern=r"^admin_dl:"))

    # New feature handlers
    app.add_handler(CallbackQueryHandler(cb_admin_toggle_lock,        pattern="^admin:toggle_lock$"))
    app.add_handler(CallbackQueryHandler(cb_admin_toggle_maintenance, pattern="^admin:toggle_maintenance$"))
    app.add_handler(CallbackQueryHandler(cb_admin_db_settings,        pattern="^admin:db_settings$"))
    app.add_handler(CallbackQueryHandler(cb_admin_db_switch_to_local, pattern="^admin:db_switch_to_local$"))
    app.add_handler(CallbackQueryHandler(cb_admin_db_confirm_local,   pattern="^admin:db_confirm_local$"))
    app.add_handler(CallbackQueryHandler(cb_admin_db_switch_to_mongo, pattern="^admin:db_switch_to_mongo$"))
    app.add_handler(CallbackQueryHandler(cb_admin_db_confirm_mongo,   pattern="^admin:db_confirm_mongo$"))
    app.add_handler(CallbackQueryHandler(cb_locked_info,              pattern=r"^locked_info:"))

    app.add_handler(CallbackQueryHandler(cb_project_dashboard, pattern=r"^proj:"))
    app.add_handler(CallbackQueryHandler(cb_run,               pattern=r"^run:"))
    app.add_handler(CallbackQueryHandler(cb_stop,              pattern=r"^stop:"))
    app.add_handler(CallbackQueryHandler(cb_restart,           pattern=r"^restart:"))
    app.add_handler(CallbackQueryHandler(cb_logs,              pattern=r"^logs:"))
    app.add_handler(CallbackQueryHandler(cb_filemgr,           pattern=r"^filemgr:"))
    app.add_handler(CallbackQueryHandler(cb_delete_confirm,    pattern=r"^delete:[a-zA-Z0-9_]+$"))
    app.add_handler(CallbackQueryHandler(cb_delete_yes,        pattern=r"^delete_yes:"))
    app.add_handler(CallbackQueryHandler(cb_toggle_auto_restart, pattern=r"^toggle_ar:"))
    app.add_handler(CallbackQueryHandler(cb_envvars,             pattern=r"^envvars:"))
    app.add_handler(CallbackQueryHandler(cb_env_delete,          pattern=r"^env_del:"))
    app.add_handler(CallbackQueryHandler(cb_reinstall_reqs,      pattern=r"^reinstall_reqs:"))

    return app


async def post_init(app: Application):
    global notification_bot
    notification_bot = app.bot

    # Initialize local SQLite DB (always, so it's ready when needed)
    init_local_db()

    await app.bot.set_my_commands([
        BotCommand("start",  "Start the bot"),
        BotCommand("done",   "Finish file upload"),
        BotCommand("cancel", "Cancel current action"),
    ])
    await restore_from_backup()
    asyncio.create_task(process_monitor())
    asyncio.create_task(backup_task())
    asyncio.create_task(keep_alive_task())
    asyncio.create_task(ram_cleanup_task())  # RAM optimization


def main():
    from file_manager import start_flask
    import threading
    t = threading.Thread(target=start_flask, args=(PORT,), daemon=True)
    t.start()
    logger.info(f"Flask file manager started on port {PORT}")

    application = build_application()
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
