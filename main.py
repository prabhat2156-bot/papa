import os, asyncio, logging, time, sys, shutil, zipfile, re, secrets, base64
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

HTML_BASE_URL   = os.getenv("HTML_BASE_URL", BASE_URL)


# ─────────────────────────────────────────────────────────────
# DB Backend selector — MUST be defined before DB init below
# ─────────────────────────────────────────────────────────────
DATA_DIR        = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_BACKEND_FILE = os.path.join(DATA_DIR, "db_backend.txt")
LOCAL_DB_PATH   = os.path.join(DATA_DIR, "bot_local.db")

def _read_db_backend() -> str:
    """Returns 'mongodb' or 'local'."""
    try:
        with open(DB_BACKEND_FILE, "r") as _f:
            v = _f.read().strip()
            return v if v in ("mongodb", "local") else "mongodb"
    except FileNotFoundError:
        return "mongodb"

def _write_db_backend(backend: str):
    with open(DB_BACKEND_FILE, "w") as _f:
        _f.write(backend)


# ─────────────────────────────────────────────────────────────
# Local SQLite Database Backend (no external service needed)
# ─────────────────────────────────────────────────────────────

class _LocalCursor:
    def __init__(self, col, filter_):
        self._col = col
        self._filter = filter_

    async def to_list(self, length: int = 10000) -> list:
        docs = await self._col._find_many(self._filter)
        return docs[:length]

    def __aiter__(self):
        self._docs = None
        self._idx = 0
        return self

    async def __anext__(self):
        if self._docs is None:
            self._docs = await self._col._find_many(self._filter)
            self._idx = 0
        if self._idx >= len(self._docs):
            raise StopAsyncIteration
        doc = self._docs[self._idx]
        self._idx += 1
        return doc


class LocalCollection:
    """SQLite-backed async collection that mirrors Motor's API."""

    def __init__(self, db_path: str, name: str):
        self._db_path = db_path
        self._name = name
        self._lock = asyncio.Lock()
        self._init_table()

    def _init_table(self):
        conn = sqlite3.connect(self._db_path)
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS [{self._name}] "
            "(id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT NOT NULL)"
        )
        conn.commit()
        conn.close()

    def _load_all(self) -> list:
        conn = sqlite3.connect(self._db_path)
        rows = conn.execute(f"SELECT id, data FROM [{self._name}]").fetchall()
        conn.close()
        result = []
        for row_id, data in rows:
            try:
                result.append((row_id, _json.loads(data)))
            except Exception:
                pass
        return result

    @staticmethod
    def _matches(doc: dict, filter_: dict) -> bool:
        if not filter_:
            return True
        for key, val in filter_.items():
            doc_val = doc.get(key)
            if isinstance(val, dict):
                for op, op_val in val.items():
                    if op == "$ne" and doc_val == op_val:
                        return False
                    elif op == "$lt" and (doc_val is None or doc_val >= op_val):
                        return False
                    elif op == "$gt" and (doc_val is None or doc_val <= op_val):
                        return False
                    elif op == "$lte" and (doc_val is None or doc_val > op_val):
                        return False
                    elif op == "$gte" and (doc_val is None or doc_val < op_val):
                        return False
                    elif op == "$in" and doc_val not in op_val:
                        return False
            else:
                if doc_val != val:
                    return False
        return True

    async def _find_many(self, filter_: dict = None) -> list:
        loop = asyncio.get_running_loop()
        async with self._lock:
            rows = await loop.run_in_executor(None, self._load_all)
        return [doc for _, doc in rows if self._matches(doc, filter_)]

    async def find_one(self, filter_: dict = None):
        loop = asyncio.get_running_loop()
        async with self._lock:
            rows = await loop.run_in_executor(None, self._load_all)
        for _, doc in rows:
            if self._matches(doc, filter_):
                return doc
        return None

    def find(self, filter_: dict = None):
        return _LocalCursor(self, filter_)

    async def count_documents(self, filter_: dict = None) -> int:
        return len(await self._find_many(filter_))

    async def insert_one(self, doc: dict):
        loop = asyncio.get_running_loop()
        def _ins():
            conn = sqlite3.connect(self._db_path)
            conn.execute(f"INSERT INTO [{self._name}] (data) VALUES (?)", (_json.dumps(doc, default=str),))
            conn.commit()
            conn.close()
        async with self._lock:
            await loop.run_in_executor(None, _ins)

    async def update_one(self, filter_: dict, update: dict, upsert: bool = False):
        loop = asyncio.get_running_loop()
        async with self._lock:
            rows = await loop.run_in_executor(None, self._load_all)
            found_id, found_doc = None, None
            for row_id, doc in rows:
                if self._matches(doc, filter_):
                    found_id, found_doc = row_id, dict(doc)
                    break

            if found_doc is not None:
                if "$set" in update:
                    found_doc.update(update["$set"])
                if "$inc" in update:
                    for k, v in update["$inc"].items():
                        found_doc[k] = found_doc.get(k, 0) + v
                if "$unset" in update:
                    for k in update["$unset"]:
                        found_doc.pop(k, None)
                def _upd(fid=found_id, fd=found_doc):
                    conn = sqlite3.connect(self._db_path)
                    conn.execute(f"UPDATE [{self._name}] SET data=? WHERE id=?", (_json.dumps(fd, default=str), fid))
                    conn.commit()
                    conn.close()
                await loop.run_in_executor(None, _upd)
            elif upsert:
                new_doc = {k: v for k, v in filter_.items() if not isinstance(v, dict)}
                if "$setOnInsert" in update:
                    new_doc.update(update["$setOnInsert"])
                if "$set" in update:
                    new_doc.update(update["$set"])
                def _ins_up(nd=new_doc):
                    conn = sqlite3.connect(self._db_path)
                    conn.execute(f"INSERT INTO [{self._name}] (data) VALUES (?)", (_json.dumps(nd, default=str),))
                    conn.commit()
                    conn.close()
                await loop.run_in_executor(None, _ins_up)

    async def delete_one(self, filter_: dict):
        loop = asyncio.get_running_loop()
        async with self._lock:
            rows = await loop.run_in_executor(None, self._load_all)
            for row_id, doc in rows:
                if self._matches(doc, filter_):
                    def _del(rid=row_id):
                        conn = sqlite3.connect(self._db_path)
                        conn.execute(f"DELETE FROM [{self._name}] WHERE id=?", (rid,))
                        conn.commit()
                        conn.close()
                    await loop.run_in_executor(None, _del)
                    break

    async def delete_many(self, filter_: dict = None):
        loop = asyncio.get_running_loop()
        async with self._lock:
            rows = await loop.run_in_executor(None, self._load_all)
            ids = [rid for rid, doc in rows if self._matches(doc, filter_)]
            if not ids:
                return
            def _delm(id_list=ids):
                conn = sqlite3.connect(self._db_path)
                for rid in id_list:
                    conn.execute(f"DELETE FROM [{self._name}] WHERE id=?", (rid,))
                conn.commit()
                conn.close()
            await loop.run_in_executor(None, _delm)

    async def command(self, cmd):
        return {"ok": 1}


class LocalDatabase:
    """Wraps multiple LocalCollection objects, one per collection name."""

    def __init__(self, db_path: str):
        self._db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._cols: dict = {}

    def __getitem__(self, name: str) -> LocalCollection:
        if name not in self._cols:
            self._cols[name] = LocalCollection(self._db_path, name)
        return self._cols[name]

    async def command(self, cmd):
        return {"ok": 1}

    async def list_collection_names(self) -> list:
        def _ls():
            conn = sqlite3.connect(self._db_path)
            rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            conn.close()
            return [r[0] for r in rows]
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _ls)

# ── Database initialisation (MongoDB or Local SQLite) ──────────
_current_db_backend = _read_db_backend()

if _current_db_backend == "local" or not MONGODB_URI:
    # Local SQLite backend — no external service needed
    logger.info("🗄 Using LOCAL SQLite database backend")
    mongo_client = None
    _local_db    = LocalDatabase(LOCAL_DB_PATH)
    db           = _local_db
    users_col    = _local_db["users"]
    projects_col = _local_db["projects"]
    tokens_col   = _local_db["file_tokens"]
    backups_col  = _local_db["backups"]
    settings_col = _local_db["bot_settings"]
else:
    # MongoDB backend
    logger.info(f"🍃 Using MongoDB backend: {DATABASE_NAME}")
    mongo_client = AsyncIOMotorClient(MONGODB_URI)
    db           = mongo_client[DATABASE_NAME]
    users_col    = db["users"]
    projects_col = db["projects"]
    tokens_col   = db["file_tokens"]
    backups_col  = db["backups"]
    settings_col = db["bot_settings"]

# ─────────────────────────────────────────────────────────────
# Multiple Extra Databases (UNLIMITED)
# MONGODB_URI_1, DATABASE_NAME_1 ... MONGODB_URI_N, DATABASE_NAME_N
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
            logging.getLogger(__name__).info(f"✅ Extra DB connected (legacy): {legacy_name}")
        except Exception as e:
            logging.getLogger(__name__).error(f"❌ Failed to connect legacy DB: {e}")

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
            logging.getLogger(__name__).info(f"✅ Extra DB #{i} connected: {name}")
        except Exception as e:
            logging.getLogger(__name__).error(f"❌ Failed to connect DB #{i} ({name}): {e}")

_load_extra_databases()
logging.getLogger(__name__).info(f"📊 Total extra databases connected: {len(extra_dbs)}")

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
# Sharding — Hash-based storage distribution
# ─────────────────────────────────────────────────────────────

def all_backup_cols() -> list:
    return [backups_col] + [e["db"]["backups"] for e in extra_dbs]

def all_db_names() -> list:
    return [DATABASE_NAME] + [e["name"] for e in extra_dbs]

def pick_backup_col(user_id: int, project_name: str):
    cols  = all_backup_cols()
    names = all_db_names()
    if len(cols) == 1:
        return (names[0], cols[0])
    import hashlib
    key = f"{user_id}:{project_name}".encode("utf-8")
    h = int(hashlib.md5(key).hexdigest(), 16)
    idx = h % len(cols)
    return (names[idx], cols[idx])


BOT_START_TIME = time.time()
notification_bot = None

# ─────────────────────────────────────────────────────────────
# Conversation states
# ─────────────────────────────────────────────────────────────
(
    NEW_PROJECT_NAME,
    NEW_PROJECT_TYPE,        # NEW: user picks Python/Node.js/Java/HTML
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
    HELP_TICKET_MSG,
) = range(19)

FREE_LIMIT    = 1
PREMIUM_LIMIT = 9999

PROJECTS_ROOT = os.path.join(os.path.dirname(__file__), "projects")
os.makedirs(PROJECTS_ROOT, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# Project type config
# ─────────────────────────────────────────────────────────────
PROJECT_TYPE_LABELS = {
    "python": "🐍 Python",
    "nodejs": "📦 Node.js",
    "java":   "☕ Java",
    "html":   "🌐 HTML",
}

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
    doc = await users_col.find_one({"user_id": user_id})
    if doc and doc.get("premium_expiry"):
        expiry = doc["premium_expiry"]
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        if expiry < datetime.now(timezone.utc):
            await users_col.update_one(
                {"user_id": user_id},
                {"$set": {"is_premium": False, "premium_expiry": None}},
            )
            # Lock extra projects down to FREE_LIMIT (1) on expiry
            await _downgrade_to_free(user_id)

async def _downgrade_to_free(user_id: int):
    """Stop & lock all but 1 project when premium expires."""
    all_proj = await projects_col.find({"user_id": user_id}).to_list(length=1000)
    if len(all_proj) <= FREE_LIMIT:
        return
    # Keep the most recently run project active
    def _sort_key(p):
        return p.get("last_run") or p.get("created_date") or datetime.min.replace(tzinfo=timezone.utc)
    sorted_proj = sorted(all_proj, key=_sort_key, reverse=True)
    keep = sorted_proj[:FREE_LIMIT]
    keep_names = {p["name"] for p in keep}
    for p in all_proj:
        if p["name"] not in keep_names:
            # Stop if running
            if p.get("status") == "running":
                try:
                    await kill_project(user_id, p["name"])
                except Exception:
                    pass
            await projects_col.update_one(
                {"user_id": user_id, "name": p["name"]},
                {"$set": {"status": "stopped", "admin_stopped": True, "locked": True}},
            )
    # Notify user
    if notification_bot:
        try:
            locked_names = [p["name"] for p in all_proj if p["name"] not in keep_names]
            locked_str = "`, `".join(locked_names) if locked_names else "none"
            active_str = "`, `".join(keep_names) if keep_names else "none"
            await notification_bot.send_message(
                chat_id=user_id,
                text=(
                    "⚠️ *Premium Expired*\n\n"
                    "Your premium has expired. Extra projects have been locked.\n"
                    f"🔒 Locked: `{locked_str}`\n"
                    f"✅ Active: `{active_str}`\n\n"
                    "Renew premium to unlock all projects."
                ),
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception:
            pass


# ── Bot Lock helpers ─────────────────────────────────────────────
async def is_bot_locked() -> bool:
    doc = await settings_col.find_one({"key": "bot_locked"})
    return bool(doc and doc.get("value", False))

async def set_bot_locked(locked: bool):
    await settings_col.update_one(
        {"key": "bot_locked"},
        {"$set": {"key": "bot_locked", "value": locked}},
        upsert=True,
    )

# ── DB migration helper ──────────────────────────────────────────
async def migrate_mongodb_to_local() -> tuple:
    """Copy all MongoDB data into local SQLite. Returns (ok, message)."""
    if mongo_client is None:
        return False, "MongoDB client not available"
    try:
        src_db   = mongo_client[DATABASE_NAME]
        local_db = LocalDatabase(LOCAL_DB_PATH)
        cols = ["users", "projects", "file_tokens", "backups", "bot_settings"]
        total = 0
        for cname in cols:
            docs = await src_db[cname].find({}).to_list(length=100000)
            for doc in docs:
                doc.pop("_id", None)
                await local_db[cname].insert_one(doc)
                total += 1
        return True, f"Migrated {total} documents to local SQLite"
    except Exception as e:
        return False, str(e)

async def migrate_local_to_mongodb() -> tuple:
    """Copy local SQLite data into MongoDB. Returns (ok, message)."""
    if mongo_client is None:
        return False, "MongoDB not configured (no MONGODB_URI)"
    try:
        src_db   = LocalDatabase(LOCAL_DB_PATH)
        tgt_db   = mongo_client[DATABASE_NAME]
        cols = ["users", "projects", "file_tokens", "backups", "bot_settings"]
        total = 0
        for cname in cols:
            docs = await src_db[cname].find({}).to_list(length=100000)
            for doc in docs:
                await tgt_db[cname].insert_one(doc)
                total += 1
        return True, f"Migrated {total} documents to MongoDB"
    except Exception as e:
        return False, str(e)

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

def html_project_url(uid: int, name: str) -> str:
    """Permanent URL for an HTML project."""
    return f"{HTML_BASE_URL.rstrip('/')}/html/{uid}/{name}/"

# ─────────────────────────────────────────────────────────────
# /start
# ─────────────────────────────────────────────────────────────

BOT_NAME = "God Madara Hosting Bot"

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await ensure_user(user)
    await check_premium_expiry(user.id)

    if await is_banned(user.id):
        await update.message.reply_text("🚫 You are banned. Contact owner.", parse_mode=ParseMode.MARKDOWN)
        return

    doc      = await get_user(user.id)
    premium  = doc.get("is_premium", False)
    count    = await project_count(user.id)
    plan_lbl = "Premium ✨" if premium else "Free"
    limit_lbl = "∞" if premium else str(FREE_LIMIT)

    text = (
        f"🌟 *Welcome to {BOT_NAME}!*\n\n"
        f"👋 Hello {user.first_name}!\n\n"
        f"🚀 *Supported Project Types:*\n"
        f"• 🐍 Python — bot, script, Flask, FastAPI\n"
        f"• 📦 Node.js / JS — JavaScript, Express, Discord bot, etc.\n"
        f"• ☕ Java \\(not JS\\) — Maven, Gradle, plain Java\n"
        f"• 🌐 HTML — Static website, landing page\n\n"
        f"✨ *Features:*\n"
        f"• Auto requirements install on start/restart\n"
        f"• Web File Manager — edit files in browser\n"
        f"• Real-time logs & auto-restart on crash\n"
        f"• Permanent URL for HTML projects\n"
        f"• Free: 1 project | Premium: Unlimited\n\n"
        f"📊 *Your Status:*\n"
        f"👤 ID: `{user.id}`\n"
        f"💎 Plan: {plan_lbl}\n"
        f"📁 Projects: {count}/{limit_lbl}\n\n"
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
        [
            InlineKeyboardButton("❓ Help & Support", callback_data="help_menu"),
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

    if await is_banned(user.id):
        await safe_edit(query, "🚫 You are banned. Contact owner.")
        return

    if user.id != OWNER_ID and not await is_admin(user.id):
        if await is_bot_locked() and not await is_premium(user.id):
            await safe_edit(query, "🔒 *Bot Locked*\n\nPremium-only mode. Contact owner.", parse_mode=ParseMode.MARKDOWN)
            return

    doc      = await get_user(user.id)
    premium  = doc.get("is_premium", False)
    count    = await project_count(user.id)
    plan_lbl = "Premium ✨" if premium else "Free"
    limit_lbl = "∞" if premium else str(FREE_LIMIT)

    text = (
        f"🌟 *Welcome to {BOT_NAME}!*\n\n"
        f"👋 Hello {user.first_name}!\n\n"
        f"🚀 *Supported Project Types:*\n"
        f"• 🐍 Python — bot, script, Flask, FastAPI\n"
        f"• 📦 Node.js / JS — JavaScript, Express, Discord bot, etc.\n"
        f"• ☕ Java \\(not JS\\) — Maven, Gradle, plain Java\n"
        f"• 🌐 HTML — Static website, landing page\n\n"
        f"✨ *Features:*\n"
        f"• Auto requirements install on start/restart\n"
        f"• Web File Manager — edit files in browser\n"
        f"• Real-time logs & auto-restart on crash\n"
        f"• Permanent URL for HTML projects\n"
        f"• Free: 1 project | Premium: Unlimited\n\n"
        f"📊 *Your Status:*\n"
        f"👤 ID: `{user.id}`\n"
        f"💎 Plan: {plan_lbl}\n"
        f"📁 Projects: {count}/{limit_lbl}\n\n"
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
        [
            InlineKeyboardButton("❓ Help & Support", callback_data="help_menu"),
        ],
    ]
    if user.id == OWNER_ID or await is_admin(user.id):
        kb.append([InlineKeyboardButton("⚙️ Admin Panel", callback_data="admin_panel")])

    await safe_edit(query, text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)

# ─────────────────────────────────────────────────────────────
# Bot Status
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
                try:
                    count = await entry["db"]["backups"].count_documents({"type": "file_backup"})
                except Exception:
                    count = 0
            except Exception:
                pass
            per_db_stats.append((entry["name"], online, count))

        total_dbs = 1 + len(extra_dbs)
        total_online = (1 if db_ping >= 0 else 0) + extra_online

        if extra_dbs:
            extra_db_lines = "\n*Storage Distribution:*\n"
            for name, online, count in per_db_stats:
                icon = "🟢" if online else "🔴"
                extra_db_lines += f"   {icon} `{name}`: `{count}` projects\n"

        db_ping_str = f"{db_ping}ms" if db_ping >= 0 else "Error"
        api_ping_str = f"{api_ping}ms" if api_ping >= 0 else "Error"

        text = (
            f"📊 *Bot Dashboard*\n\n"
            f"👥 Total Users: `{total_users}`\n"
            f"💎 Premium Users: `{premium_users}`\n"
            f"📁 Total Projects: `{total_proj}`\n"
            f"🟢 Running Projects: `{running_proj}`\n"
            f"💾 Database: MongoDB ✅\n"
            f"🔗 Connected DBs: `{total_online}/{total_dbs}` "
            f"(1 primary + {len(extra_dbs)} extra)\n"
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
            f"📊 *Bot Dashboard*\n\n⚠️ Error loading stats: {str(e)[:200]}\n\nBot is online!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔃 Retry", callback_data="bot_status"),
                 InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")],
            ]),
            parse_mode=ParseMode.MARKDOWN,
        )

# ─────────────────────────────────────────────────────────────
# Premium page
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
        f"• ✅ All project types (Python/Node/Java/HTML)\n\n"
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
# My Projects
# ─────────────────────────────────────────────────────────────

async def cb_my_projects(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    if await is_banned(uid):
        await safe_edit(query, "🚫 You are banned. Contact owner.")
        return

    projects = await projects_col.find({"user_id": uid}).to_list(length=100)
    if not projects:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_start")]])
        await safe_edit(query, "📂 *My Projects*\n\nYou have no projects yet.", reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        return

    kb_rows = []
    for p in projects:
        icon = "🟢" if p.get("status") == "running" else "🔴"
        ptype = PROJECT_TYPE_LABELS.get(p.get("project_type", "python"), "🐍")
        kb_rows.append([InlineKeyboardButton(f"{icon} {ptype} {p['name']}", callback_data=f"proj:{p['name']}")])
    kb_rows.append([InlineKeyboardButton("🔙 Back", callback_data="back_start")])

    await safe_edit(query, "📂 *My Projects*\n\nSelect a project:", reply_markup=InlineKeyboardMarkup(kb_rows), parse_mode=ParseMode.MARKDOWN)

# ─────────────────────────────────────────────────────────────
# My Status
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
            f"📊 *My Status*\n\n{plan_lbl} | 📁 0/{limit_lbl} projects\n\nNo projects yet.\nCreate your first project!",
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
        ptype  = PROJECT_TYPE_LABELS.get(p.get("project_type", "python"), "🐍 Python")

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

        if status == "running":
            status_line = "🟢 Running"
            extra_line  = f"   ├ ⏱ Uptime: `{uptime_str}`"
        elif exit_code is not None and exit_code != 0:
            status_line = "🔴 Crashed"
            extra_line  = f"   ├ ⚠️ Exit Code: `{exit_code}`"
        else:
            status_line = "🔴 Stopped"
            extra_line  = "   ├ ⏱ Uptime: `—`"

        ar_line = "ON ✅" if ar else "OFF ❌"

        lines.append(
            f"{i}\u20e3  *{escape_md(name)}*  {ptype}\n"
            f"   ├ {status_line}\n"
            f"{extra_line}\n"
            f"   ├ 🔁 Auto-Restart: {ar_line}\n"
            f"   └ 🖥 `{escape_md(str(cmd))}`\n"
        )

    text = "\n".join(lines)
    if len(text) > 3800:
        text = text[:3800] + "\n\n_...more projects exist, use /start to see all_"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔃 Refresh",    callback_data="my_status"),
         InlineKeyboardButton("📂 Projects",   callback_data="my_projects")],
        [InlineKeyboardButton("🔙 Back",       callback_data="back_start")],
    ])
    await safe_edit(query, text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

# ─────────────────────────────────────────────────────────────
# Project Dashboard
# ─────────────────────────────────────────────────────────────

def escape_md(text: str) -> str:
    """Escape Markdown v1 special characters."""
    for ch in ('_', '*', '`', '['):
        text = str(text).replace(ch, f'\\{ch}')
    return text

def project_dashboard_text(p: dict) -> str:
    status  = p.get("status", "stopped")
    ptype   = p.get("project_type", "python")
    type_lbl = PROJECT_TYPE_LABELS.get(ptype, "🐍 Python")

    is_locked = p.get("locked", False)
    if status == "running":
        if ptype == "html":
            icon = "🟢 Live (HTML)"
        else:
            icon = "🟢 Running"
    elif is_locked:
        icon = "🔒 Locked (Premium Expired)"
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
    run_cmd   = escape_md(str(p.get("run_command") or "Not set"))
    created   = "N/A"
    if p.get("created_date"):
        try:
            created = p["created_date"].strftime("%Y-%m-%d")
        except Exception:
            created = str(p["created_date"])

    ar_status = "✅ ON" if p.get("auto_restart", True) else "❌ OFF"

    text = (
        f"📊 Project: *{escape_md(p['name'])}*\n\n"
        f"🔹 Type: {type_lbl}\n"
        f"🔹 Status: {icon}\n"
    )

    # HTML projects get a permanent URL
    if ptype == "html":
        url = html_project_url(p["user_id"], p["name"])
        text += f"🌐 URL: `{escape_md(url)}`\n"
    else:
        text += f"🔹 PID: `{pid}`\n"

    text += (
        f"🔹 Uptime: `{uptime}`\n"
        f"🔹 Last Run: `{escape_md(last_run)}`\n"
        f"🔹 Exit Code: `{exit_code}`\n"
        f"🔹 Run Command: `{run_cmd}`\n"
        f"🔹 Auto-Restart: {ar_status}\n"
        f"📅 Created: `{created}`"
    )
    return text

def project_dashboard_kb(user_id: int, project_name: str, auto_restart: bool = True,
                          is_running: bool = False, project_type: str = "python",
                          is_locked: bool = False) -> InlineKeyboardMarkup:
    pn = project_name
    ar_label = "⏰ Auto-Restart: ✅" if auto_restart else "⏰ Auto-Restart: ❌"

    if is_running:
        if project_type == "html":
            row1 = [
                InlineKeyboardButton("⏹ Stop",     callback_data=f"stop:{pn}"),
                InlineKeyboardButton("📋 Logs",     callback_data=f"logs:{pn}"),
            ]
        else:
            row1 = [
                InlineKeyboardButton("⏹ Stop",     callback_data=f"stop:{pn}"),
                InlineKeyboardButton("🔄 Restart",  callback_data=f"restart:{pn}"),
                InlineKeyboardButton("📋 Logs",     callback_data=f"logs:{pn}"),
            ]
    else:
        row1 = [
            InlineKeyboardButton("▶️ Run",       callback_data=f"run:{pn}"),
            InlineKeyboardButton("🔄 Restart",   callback_data=f"restart:{pn}"),
            InlineKeyboardButton("📋 Logs",      callback_data=f"logs:{pn}"),
        ]

    rows = [
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
    ]

    # HTML projects: view site button
    if project_type == "html" and is_running:
        url = html_project_url(user_id, pn)
        rows.append([InlineKeyboardButton("🌐 View Site", url=url)])

    rows.append([InlineKeyboardButton("📦 Reinstall Requirements", callback_data=f"reinstall_reqs:{pn}")])
    rows.append([InlineKeyboardButton("🗑 Delete", callback_data=f"delete:{pn}")])
    rows.append([InlineKeyboardButton("🔙 Back",   callback_data="my_projects")])

    return InlineKeyboardMarkup(rows)

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

    ptype   = p.get("project_type", "python")
    locked  = p.get("locked", False)
    await safe_edit(
        query,
        project_dashboard_text(p),
        reply_markup=project_dashboard_kb(uid, name, p.get("auto_restart", True),
                                          p.get("status") == "running", ptype, locked),
        parse_mode=ParseMode.MARKDOWN,
    )

# ─────────────────────────────────────────────────────────────
# Reinstall Requirements
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
    ptype    = p.get("project_type", "python")
    req_path = os.path.join(pdir, "requirements.txt")
    pkg_json = os.path.join(pdir, "package.json")
    pom_xml  = os.path.join(pdir, "pom.xml")
    venv_dir = os.path.join(pdir, "venv")
    pip_path = os.path.join(venv_dir, "bin", "pip")

    back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"proj:{name}")]])

    # HTML — no requirements
    if ptype == "html":
        await safe_edit(query, "🌐 *HTML projects have no requirements to install.*",
                        reply_markup=back_kb, parse_mode=ParseMode.MARKDOWN)
        return

    # Java — Maven
    if ptype == "java" and os.path.exists(pom_xml):
        progress = LiveProgress(query.message, title=f"Maven Build — {name}")
        await progress.start("🔍 Looking for Maven ...")
        progress.run_in_background(estimated_seconds=150, status="Setting up Maven")
        mvn_exe = await _find_or_install_mvn(pdir)
        if not mvn_exe:
            await progress.stop(success=False, final_text="Maven not found & auto-install failed.")
            await query.message.reply_text(
                "❌ *Maven not found on server*\n\n"
                "Fix options:\n"
                "• `apt-get install -y maven` (Debian/Ubuntu)\n"
                "• `brew install maven` (Mac)\n"
                "• Add `mvnw` wrapper to your project ZIP",
                parse_mode=ParseMode.MARKDOWN,
            )
            return
        try:
            proc = await asyncio.wait_for(
                create_subprocess_exec(mvn_exe, "dependency:resolve", "-q", stdout=PIPE, stderr=PIPE, cwd=pdir),
                timeout=300,
            )
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
            if proc.returncode == 0:
                await progress.stop(success=True, final_text="Maven dependencies resolved!")
            else:
                await progress.stop(success=False, final_text=stderr.decode()[:300])
        except asyncio.TimeoutError:
            await progress.stop(success=False, final_text="Maven timed out")
        except Exception as e:
            await progress.stop(success=False, final_text=str(e))
        await query.message.reply_text("Choose next:", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Restart", callback_data=f"restart:{name}")],
            [InlineKeyboardButton("🔙 Back", callback_data=f"proj:{name}")],
        ]))
        return

    # Node.js
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
            _, stderr_n = await asyncio.wait_for(proc_n.communicate(), timeout=600)
            if proc_n.returncode == 0:
                await progress.stop(success=True, final_text=f"npm packages reinstalled for {name}")
            else:
                await progress.stop(success=False, final_text=f"```\n{stderr_n.decode()[:400]}\n```")
        except asyncio.TimeoutError:
            await progress.stop(success=False, final_text="npm install timed out")
        except FileNotFoundError:
            await progress.stop(success=False, final_text="npm not installed on host.")
        except Exception as e:
            await progress.stop(success=False, final_text=f"npm error: {escape_md(str(e))}")
        await query.message.reply_text("Choose next:", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Restart Project", callback_data=f"restart:{name}")],
            [InlineKeyboardButton("🔙 Back", callback_data=f"proj:{name}")],
        ]))
        return

    if not os.path.exists(req_path):
        await safe_edit(query,
            f"⚠️ *No requirements.txt or package.json found* in `{escape_md(name)}`.\n\nUpload one via Files first.",
            reply_markup=back_kb, parse_mode=ParseMode.MARKDOWN)
        return

    results = []

    if not os.path.exists(pip_path):
        progress = LiveProgress(query.message, title=f"Creating venv — {name}")
        await progress.start("python -m venv ...")
        progress.run_in_background(estimated_seconds=20, status="Building virtual environment")
        try:
            proc = await asyncio.wait_for(
                create_subprocess_exec(sys.executable, "-m", "venv", venv_dir, stdout=PIPE, stderr=PIPE),
                timeout=120,
            )
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
            if proc.returncode == 0:
                await progress.stop(success=True, final_text="Virtual environment created")
                results.append("✅ Virtual environment created")
            else:
                await progress.stop(success=False, final_text=stderr.decode()[:200])
                results.append(f"❌ venv failed: {stderr.decode()[:200]}")
                await query.message.reply_text(f"📦 *Reinstall failed*\n\n" + "\n".join(results),
                                               reply_markup=back_kb, parse_mode=ParseMode.MARKDOWN)
                return
        except Exception as e:
            await progress.stop(success=False, final_text=str(e))
            await query.message.reply_text(f"📦 *Reinstall failed*\n\n❌ venv error: {e}",
                                           reply_markup=back_kb, parse_mode=ParseMode.MARKDOWN)
            return

    pip_progress = LiveProgress(query.message, title=f"Upgrading pip — {name}")
    await pip_progress.start("pip install --upgrade pip")
    pip_progress.run_in_background(estimated_seconds=15, status="Fetching latest pip")
    try:
        proc = await asyncio.wait_for(
            create_subprocess_exec(pip_path, "install", "--upgrade", "pip", stdout=PIPE, stderr=PIPE, cwd=pdir),
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
                f"📦 *Reinstall failed for {escape_md(name)}*\n\n" + "\n".join(results),
                reply_markup=back_kb, parse_mode=ParseMode.MARKDOWN)
            return
    except asyncio.TimeoutError:
        await req_progress.stop(success=False, final_text="pip install timed out")
        await query.message.reply_text(f"📦 *Reinstall failed for {escape_md(name)}*\n\n❌ pip timed out",
                                       reply_markup=back_kb, parse_mode=ParseMode.MARKDOWN)
        return
    except Exception as e:
        await req_progress.stop(success=False, final_text=str(e))
        await query.message.reply_text(f"📦 *Reinstall failed*\n\n❌ pip error: {escape_md(str(e))}",
                                       reply_markup=back_kb, parse_mode=ParseMode.MARKDOWN)
        return

    try:
        proc2 = await asyncio.wait_for(
            create_subprocess_exec(pip_path, "list", stdout=PIPE, stderr=PIPE), timeout=30)
        out2, _ = await asyncio.wait_for(proc2.communicate(), timeout=30)
        pkg_count = max(len(out2.decode().strip().splitlines()) - 2, 0)
        results.append(f"✅ {pkg_count} packages available")
    except Exception:
        results.append("⚠️ Could not verify packages")

    is_running = p.get("status") == "running"
    note = ""
    if is_running:
        note = "\n\nℹ️ Project is currently running. 🔄 *Restart* it to apply new packages."

    await safe_edit(query,
        f"🎉 *Requirements reinstalled for {escape_md(name)}!*\n\n" + "\n".join(results) + note,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Restart Project", callback_data=f"restart:{name}")],
            [InlineKeyboardButton("🔙 Back", callback_data=f"proj:{name}")],
        ]),
        parse_mode=ParseMode.MARKDOWN,
    )

# ─────────────────────────────────────────────────────────────
# Process store & Run project
# ─────────────────────────────────────────────────────────────

context_store: dict = {}

async def start_project_process(uid: int, name: str) -> dict:
    """Start project subprocess. Returns updated project dict."""
    p    = await get_project(uid, name)
    pdir = project_dir(uid, name)
    ptype = p.get("project_type", "python")
    cmd  = p.get("run_command") or _default_run_command(pdir, ptype)

    # HTML projects don't need a subprocess — Flask serves them
    if ptype == "html":
        now = datetime.now(timezone.utc)
        await projects_col.update_one(
            {"user_id": uid, "name": name},
            {"$set": {
                "status":       "running",
                "pid":          None,
                "started_at":   now,
                "last_run":     now,
                "exit_code":    None,
                "admin_stopped": False,
            }},
        )
        updated = await get_project(uid, name)
        logger.info(f"HTML project {name} for user {uid} marked as live")
        return updated

    log_path = os.path.join(pdir, "output.log")

    # ── RAM: rotate log if > 2 MB to avoid disk bloat ─────────────
    try:
        if os.path.exists(log_path) and os.path.getsize(log_path) > 2 * 1024 * 1024:
            with open(log_path, "rb") as _lf:
                _lf.seek(-512 * 1024, 2)
                tail = _lf.read()
            with open(log_path, "wb") as _lf:
                _lf.write(tail)
    except Exception:
        pass

    # ── RAM: wait for a slot if too many processes already running ─
    global _process_semaphore
    if _process_semaphore is None:
        _process_semaphore = asyncio.Semaphore(MAX_CONCURRENT_PROJECTS)
    # Check running count before acquiring (non-blocking check)
    running_now = await projects_col.count_documents({"status": "running"})
    if running_now >= MAX_CONCURRENT_PROJECTS:
        logger.warning(f"RAM guard: {running_now} projects running — queuing {name}")
    await _process_semaphore.acquire()

    # Resolve actual executable
    venv_python = os.path.join(pdir, "venv", "bin", "python")
    if not os.path.exists(venv_python):
        venv_python = sys.executable

    import shlex, copy

    # Resolve mvn full path if not in system PATH
    _mvn_local = os.path.expanduser("~/.local/maven/bin/mvn")
    def _resolve_mvn_in_cmd(c: str) -> str:
        import shutil as _sh
        if not _sh.which("mvn") and os.path.exists(_mvn_local):
            # Only replace if command STARTS with bare "mvn" token, not inside a path
            if c == "mvn":
                c = _mvn_local
            elif c.startswith("mvn "):
                c = _mvn_local + c[3:]  # replace only the leading "mvn"
        return c

    cmd = _resolve_mvn_in_cmd(cmd)

    # Shell needed for &&, |, ;, globs
    needs_shell = any(ch in cmd for ch in ("&&", "||", ";", "*", "|", ">"))
    if needs_shell:
        parts = ["/bin/bash", "-c", cmd]
    else:
        parts = shlex.split(cmd)
        if parts and parts[0] in ("python", "python3"):
            parts[0] = venv_python
        elif parts and parts[0] == "mvn":
            # Final fallback — replace bare mvn with full path
            if os.path.exists(_mvn_local):
                parts[0] = _mvn_local

    logger.info(f"Starting process: {parts} in {pdir}")

    proc_env = copy.copy(os.environ)
    # Add Maven to PATH so child processes (e.g. mvnw) can also find it
    if os.path.exists(_mvn_local):
        maven_bin_dir = os.path.dirname(_mvn_local)
        proc_env["PATH"] = maven_bin_dir + os.pathsep + proc_env.get("PATH", "")
    env_path = os.path.join(pdir, ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as ef:
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
    # Store proc object in memory for monitoring
    context_store[f"{uid}:{name}"] = proc

    updated = await get_project(uid, name)
    logger.info(f"DB updated - status: {updated.get('status')}, pid: {updated.get('pid')}")
    return updated

async def _find_or_install_mvn(pdir: str = ".") -> str | None:
    """
    Find Maven executable. Priority:
      1. ./mvnw  (Maven Wrapper in project)
      2. shutil.which("mvn")  (system PATH)
      3. Common install paths
      4. Auto-install via apt-get (Linux servers)
    Returns executable path or None if unavailable.
    """
    import shutil

    # 1. Maven Wrapper in project directory
    mvnw = os.path.join(pdir, "mvnw")
    if os.path.exists(mvnw):
        try:
            os.chmod(mvnw, 0o755)
        except Exception:
            pass
        return mvnw

    # 2. System PATH
    if shutil.which("mvn"):
        return "mvn"

    # 3. Common install paths
    common = [
        "/usr/share/maven/bin/mvn",
        "/usr/local/maven/bin/mvn",
        "/opt/maven/bin/mvn",
        "/usr/bin/mvn",
    ]
    for path in common:
        if os.path.exists(path):
            return path

    # 4. Try apt-get (works only if sudo available — e.g. some VPS)
    try:
        proc = await asyncio.wait_for(
            create_subprocess_exec(
                "apt-get", "install", "-y", "maven",
                stdout=PIPE, stderr=PIPE,
                env={**os.environ, "DEBIAN_FRONTEND": "noninteractive"},
            ),
            timeout=120,
        )
        await asyncio.wait_for(proc.communicate(), timeout=120)
        if proc.returncode == 0 and shutil.which("mvn"):
            logger.info("Maven installed via apt-get successfully")
            return "mvn"
    except Exception:
        pass

    # 5. Download Maven binary directly (no sudo needed — works on Render, Railway, etc.)
    logger.info("Trying to download Maven binary directly...")
    mvn_home  = os.path.expanduser("~/.local/maven")
    mvn_bin   = os.path.join(mvn_home, "bin", "mvn")

    # Already downloaded previously?
    if os.path.exists(mvn_bin):
        logger.info(f"Using previously downloaded Maven: {mvn_bin}")
        return mvn_bin

    try:
        import urllib.request, tarfile as _tarfile, shutil as _shutil

        # Try multiple versions in case one mirror/version is unavailable
        mvn_versions = ["3.9.9", "3.9.6", "3.9.5"]
        tmp_tar   = "/tmp/apache-maven.tar.gz"
        local_dir = os.path.expanduser("~/.local")
        os.makedirs(local_dir, exist_ok=True)

        # FIX: use get_running_loop() — get_event_loop() is deprecated in Python 3.10+
        loop = asyncio.get_running_loop()

        downloaded = False
        for mvn_version in mvn_versions:
            mvn_urls = [
                f"https://downloads.apache.org/maven/maven-3/{mvn_version}/binaries/apache-maven-{mvn_version}-bin.tar.gz",
                f"https://archive.apache.org/dist/maven/maven-3/{mvn_version}/binaries/apache-maven-{mvn_version}-bin.tar.gz",
                f"https://dlcdn.apache.org/maven/maven-3/{mvn_version}/binaries/apache-maven-{mvn_version}-bin.tar.gz",
            ]
            for mvn_url in mvn_urls:
                try:
                    logger.info(f"Downloading Maven {mvn_version} from: {mvn_url}")
                    # FIX: wrap in asyncio.wait_for so slow network doesn't hang forever
                    await asyncio.wait_for(
                        loop.run_in_executor(
                            None, lambda u=mvn_url: urllib.request.urlretrieve(u, tmp_tar)
                        ),
                        timeout=300,
                    )
                    # FIX: verify downloaded file is actually a valid gzip (not a 404 HTML page)
                    file_size = os.path.getsize(tmp_tar) if os.path.exists(tmp_tar) else 0
                    if file_size < 500_000:
                        logger.warning(f"Downloaded file too small ({file_size} bytes) — likely a 404 page, skipping")
                        continue
                    downloaded = True
                    logger.info(f"Downloaded Maven {mvn_version} ({file_size // 1024} KB)")
                    break
                except asyncio.TimeoutError:
                    logger.warning(f"Download timed out: {mvn_url}")
                except Exception as dl_err:
                    logger.warning(f"Mirror failed ({mvn_url}): {dl_err}")
            if downloaded:
                break

        if not downloaded:
            raise RuntimeError("All Maven versions and mirror URLs failed to download")

        # FIX: extract with filter='fully_trusted' for Python 3.12 compatibility
        def _extract_tar():
            with _tarfile.open(tmp_tar, "r:gz") as tf:
                try:
                    tf.extractall(local_dir, filter="fully_trusted")
                except TypeError:
                    # Python < 3.12 doesn't have filter param — fall back
                    tf.extractall(local_dir)  # noqa: S202

        await asyncio.wait_for(loop.run_in_executor(None, _extract_tar), timeout=120)

        # Rename extracted dir → ~/.local/maven
        renamed = False
        for entry in os.listdir(local_dir):
            if entry.startswith("apache-maven"):
                extracted = os.path.join(local_dir, entry)
                if os.path.isdir(extracted) and os.path.abspath(extracted) != os.path.abspath(mvn_home):
                    if os.path.exists(mvn_home):
                        _shutil.rmtree(mvn_home)
                    os.rename(extracted, mvn_home)
                    renamed = True
                    logger.info(f"Renamed {extracted} → {mvn_home}")
                    break

        if not renamed:
            logger.error(f"Could not find extracted apache-maven-* dir in {local_dir}. Contents: {os.listdir(local_dir)}")

        # Clean up temp file
        try:
            os.remove(tmp_tar)
        except Exception:
            pass

        if os.path.exists(mvn_bin):
            os.chmod(mvn_bin, 0o755)
            logger.info(f"Maven downloaded and ready → {mvn_bin}")
            return mvn_bin
        else:
            logger.error(f"Maven binary not found after extraction. mvn_home contents: {os.listdir(mvn_home) if os.path.exists(mvn_home) else 'dir missing'}")

    except Exception as e:
        logger.error(f"Maven direct download failed: {e}", exc_info=True)

    return None


def _detect_java_main_class(pdir: str) -> str:
    """
    Scan .java files to find the one with 'public static void main'.
    Returns the class name (without .java). Defaults to 'Main'.
    """
    try:
        java_files = [f for f in os.listdir(pdir) if f.endswith(".java")]
        for jf in java_files:
            try:
                with open(os.path.join(pdir, jf), encoding="utf-8", errors="replace") as f:
                    content = f.read()
                if "public static void main" in content:
                    return jf.replace(".java", "")
            except Exception:
                continue
        # Fallback: use first .java file name
        if java_files:
            return java_files[0].replace(".java", "")
    except Exception:
        pass
    return "Main"


def _default_run_command(pdir: str, ptype: str) -> str:
    """Return sensible default run command based on project type and files."""
    if ptype == "html":
        return "html_serve"

    if ptype == "java":
        if os.path.exists(os.path.join(pdir, "pom.xml")):
            # Auto-detect main class from .java files
            main_class = _detect_java_main_class(pdir)
            return f"mvn -q compile exec:java -Dexec.mainClass={main_class}"
        if os.path.exists(os.path.join(pdir, "build.gradle")):
            return "./gradlew run"
        java_files = [f for f in os.listdir(pdir) if f.endswith(".java")] if os.path.isdir(pdir) else []
        if java_files:
            main_class = java_files[0].replace(".java", "")
            return f"javac {main_class}.java && java {main_class}"
        return "java -jar app.jar"

    if ptype == "nodejs":
        pkg_json = os.path.join(pdir, "package.json")
        if os.path.exists(pkg_json):
            try:
                import json as _json
                with open(pkg_json, "r", encoding="utf-8") as _pf:
                    _pkg = _json.load(_pf)
                if isinstance(_pkg, dict):
                    if isinstance(_pkg.get("scripts"), dict) and _pkg["scripts"].get("start"):
                        return "npm start"
                    if _pkg.get("main") and os.path.exists(os.path.join(pdir, _pkg["main"])):
                        return f"node {_pkg['main']}"
            except Exception:
                pass
        for c in ["index.js", "bot.js", "app.js", "main.js", "server.js"]:
            if os.path.exists(os.path.join(pdir, c)):
                return f"node {c}"
        return "npm start"

    # Python (default)
    for c in ["main.py", "bot.py", "app.py", "index.py", "run.py"]:
        if os.path.exists(os.path.join(pdir, c)):
            return f"python {c}"
    return "python main.py"

async def cb_run(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    # Premium-expired lock guard
    if p.get("locked") and not await is_premium(uid) and uid != OWNER_ID:
        await safe_edit(
            query,
            "🔒 *Project Locked*\n\nThis project is locked because your premium expired.\n"
            "Renew premium to unlock all your projects.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"proj:{name}")]]),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if p.get("admin_stopped"):
        await safe_edit(query, "⚠️ Your project was stopped by admin. Contact owner.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"proj:{name}")]]),
                        parse_mode=ParseMode.MARKDOWN)
        return

    ptype = p.get("project_type", "python")

    if p.get("status") == "running":
        if ptype == "html":
            await safe_edit(query, "🌐 HTML project is already live.", parse_mode=ParseMode.MARKDOWN)
            return
        if p.get("pid") and psutil.pid_exists(p["pid"]):
            await safe_edit(query, "▶️ Project is already running.", parse_mode=ParseMode.MARKDOWN)
            return

    if not p.get("run_command") and ptype != "html":
        pdir = project_dir(uid, name)
        auto_cmd = _default_run_command(pdir, ptype)
        if not auto_cmd or auto_cmd in ("python main.py", "npm start", "java -jar app.jar"):
            # Only block if no files were found to auto-detect a meaningful command
            import os as _os
            has_files = any(
                _os.path.exists(_os.path.join(pdir, f))
                for f in ["main.py","bot.py","app.py","index.py","run.py",
                          "index.js","bot.js","app.js","main.js","server.js",
                          "package.json","pom.xml","build.gradle"]
            )
            if not has_files:
                await safe_edit(query, "❌ No run command set. Use ✏️ Edit CMD first.",
                                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"proj:{name}")]]),
                                parse_mode=ParseMode.MARKDOWN)
                return

    await safe_edit(query, f"▶️ Starting *{escape_md(name)}*...", parse_mode=ParseMode.MARKDOWN)

    try:
        updated = await start_project_process(uid, name)
        await safe_edit(
            query,
            project_dashboard_text(updated),
            reply_markup=project_dashboard_kb(uid, name, updated.get("auto_restart", True),
                                              updated.get("status") == "running", ptype),
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception as e:
        logger.error(f"Failed to start project {name}: {e}")
        await safe_edit(query, f"❌ Failed to start: {escape_md(str(e)[:300])}", parse_mode=ParseMode.MARKDOWN)

# ─────────────────────────────────────────────────────────────
# Stop project
# ─────────────────────────────────────────────────────────────

async def kill_project(uid: int, name: str):
    """Kill project subprocess and update DB. Cleans context_store."""
    p = await get_project(uid, name)
    if p and p.get("pid"):
        try:
            proc = psutil.Process(p["pid"])
            for child in proc.children(recursive=True):
                child.kill()
            proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    # BUGFIX: Clean up context_store to prevent memory leak
    context_store.pop(f"{uid}:{name}", None)
    await projects_col.update_one(
        {"user_id": uid, "name": name},
        {"$set": {"status": "stopped", "pid": None}},
    )

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

    await safe_edit(query, f"⏹ Stopping *{escape_md(name)}*...", parse_mode=ParseMode.MARKDOWN)
    await kill_project(uid, name)

    p = await get_project(uid, name)
    ptype   = p.get("project_type", "python")
    locked  = p.get("locked", False)
    await safe_edit(
        query,
        project_dashboard_text(p),
        reply_markup=project_dashboard_kb(uid, name, p.get("auto_restart", True),
                                          p.get("status") == "running", ptype, locked),
        parse_mode=ParseMode.MARKDOWN,
    )

# ─────────────────────────────────────────────────────────────
# Restart
# ─────────────────────────────────────────────────────────────

async def cb_restart(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    if p.get("admin_stopped"):
        await safe_edit(query, "⚠️ Your project was stopped by admin. Contact owner.", parse_mode=ParseMode.MARKDOWN)
        return

    ptype = p.get("project_type", "python")
    await safe_edit(query, f"🔄 Restarting *{escape_md(name)}*...", parse_mode=ParseMode.MARKDOWN)
    await kill_project(uid, name)
    await asyncio.sleep(1)

    try:
        updated = await start_project_process(uid, name)
        await safe_edit(
            query,
            project_dashboard_text(updated),
            reply_markup=project_dashboard_kb(uid, name, updated.get("auto_restart", True),
                                              updated.get("status") == "running", ptype),
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception as e:
        await safe_edit(query, f"❌ Restart failed: {escape_md(str(e))}", parse_mode=ParseMode.MARKDOWN)

# ─────────────────────────────────────────────────────────────
# Logs
# ─────────────────────────────────────────────────────────────

async def cb_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid  = query.from_user.id
    name = query.data.split(":", 1)[1]

    if await is_banned(uid):
        await safe_edit(query, "🚫 You are banned. Contact owner.")
        return

    p = await get_project(uid, name)
    ptype = p.get("project_type", "python") if p else "python"

    if ptype == "html":
        url = html_project_url(uid, name) if p else "N/A"
        await safe_edit(query,
            f"🌐 *HTML Project — {escape_md(name)}*\n\n"
            f"HTML projects are served as static files.\n"
            f"No process logs available.\n\n"
            f"🔗 Site URL: `{escape_md(url)}`",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"proj:{name}")]]),
            parse_mode=ParseMode.MARKDOWN,
        )
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
# Edit Run CMD
# ─────────────────────────────────────────────────────────────

async def cb_editcmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    name = query.data.split(":", 1)[1]
    context.user_data["editcmd_project"] = name
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"proj:{name}")]])
    await safe_edit(
        query,
        f"✏️ *Edit Run Command for {escape_md(name)}*\n\nSend the new run command.\n\n"
        f"Examples:\n`python main.py`\n`node index.js`\n`npm start`\n`java -jar app.jar`",
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
    p    = await get_project(uid, name)
    ptype = p.get("project_type", "python")
    kb   = project_dashboard_kb(uid, name, p.get("auto_restart", True),
                                 p.get("status") == "running", ptype)
    await update.message.reply_text(
        f"✅ Run command updated!\n\n" + project_dashboard_text(p),
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )
    return ConversationHandler.END

# ─────────────────────────────────────────────────────────────
# File Manager
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
    expires  = now.timestamp() + 600  # 10 minutes

    try:
        from file_manager import token_store
        token_store[token] = {
            "user_id":      uid,
            "project_name": name,
            "project_dir":  project_dir(uid, name),
            "expires_at":   expires,
        }
    except ImportError:
        pass

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
# Delete project
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

    await kill_project(uid, name)  # also cleans context_store
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
# New Project — ConversationHandler
# Flow: name → type selection → file upload → finalize
# ─────────────────────────────────────────────────────────────

async def cb_new_project(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id

    if await is_banned(uid):
        await safe_edit(query, "🚫 You are banned. Contact owner.")
        return ConversationHandler.END

    if uid != OWNER_ID and not await is_admin(uid):
        if await is_bot_locked() and not await is_premium(uid):
            await safe_edit(query, "🔒 *Bot Locked*\n\nPremium-only mode. Contact owner to upgrade.", parse_mode=ParseMode.MARKDOWN)
            return ConversationHandler.END

    premium = await is_premium(uid)
    count   = await project_count(uid)
    limit   = PREMIUM_LIMIT if premium else FREE_LIMIT

    # Block locked projects from being run
    if count >= limit:
        lbl = "∞" if premium else str(FREE_LIMIT)
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
        "📝 *New Project*\n\nEnter a project name:\n(only letters, numbers, underscore — max 20 chars)",
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

    # Show project type selection buttons
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🐍 Python",         callback_data="proj_type:python"),
            InlineKeyboardButton("📦 Node.js / JS",   callback_data="proj_type:nodejs"),
        ],
        [
            InlineKeyboardButton("☕ Java (not JS)",  callback_data="proj_type:java"),
            InlineKeyboardButton("🌐 HTML",            callback_data="proj_type:html"),
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="back_start")],
    ])
    await update.message.reply_text(
        f"✅ Project name: *{escape_md(name)}*\n\n"
        f"🔨 *Select your project type:*\n\n"
        f"• 🐍 *Python* — bot, script, Flask, FastAPI\n"
        f"• 📦 *Node.js / JS* — JavaScript, Express, Discord bot, etc.\n"
        f"• ☕ *Java (not JS)* — Maven, Gradle, plain Java (`.java` files)\n"
        f"• 🌐 *HTML* — Static website (permanent URL milega)\n\n"
        f"⚠️ JavaScript and Java are *different* languages!\n"
        f"For JS projects, choose *Node.js / JS*.\n\n"
        f"Select your project type:",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )
    return NEW_PROJECT_TYPE

async def new_project_type_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    ptype = query.data.split(":", 1)[1]  # python / nodejs / java / html
    context.user_data["new_project_type"] = ptype
    name = context.user_data.get("new_project_name")
    type_lbl = PROJECT_TYPE_LABELS.get(ptype, "🐍 Python")

    if ptype == "html":
        upload_tip = (
            f"📤 *Upload your HTML files*\n\n"
            f"Send your files one by one, or a single `.zip` file containing:\n"
            f"• `index.html` (main page)\n"
            f"• CSS, JS, images, etc.\n\n"
            f"After upload, you'll get a *permanent URL* to view your site."
        )
    elif ptype == "java":
        upload_tip = (
            f"📤 *Upload your Java project files*\n\n"
            f"Send your files or a `.zip` file:\n"
            f"• Maven project: include `pom.xml`\n"
            f"• Gradle project: include `build.gradle`\n"
            f"• Plain Java: include `.java` files\n\n"
            f"After upload, the bot will set run command automatically."
        )
    elif ptype == "nodejs":
        upload_tip = (
            f"📤 *Upload your Node.js project files*\n\n"
            f"Send your files or a `.zip` file:\n"
            f"• Include `package.json`\n"
            f"• Include `index.js` / `app.js` / `bot.js`\n\n"
            f"After upload, npm install will run automatically."
        )
    else:
        upload_tip = (
            f"📤 *Upload your Python project files*\n\n"
            f"Send your files or a `.zip` file:\n"
            f"• Include `main.py` / `bot.py` / `app.py`\n"
            f"• Include `requirements.txt` (if needed)\n\n"
            f"After upload, pip install will run automatically."
        )

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Done Uploading", callback_data="upload_done")]])
    await safe_edit(
        query,
        f"📁 *Project: {escape_md(name)}* — {type_lbl}\n\n{upload_tip}\n\n"
        f"When done, click *Done Uploading* or send /done.",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )
    return NEW_PROJECT_FILES

async def new_project_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    name = context.user_data.get("new_project_name")
    pdir = project_dir(uid, name)
    os.makedirs(pdir, exist_ok=True)

    doc = update.message.document
    if not doc:
        await update.message.reply_text("Please send a file document.", parse_mode=ParseMode.MARKDOWN)
        return NEW_PROJECT_FILES

    file_obj  = await doc.get_file()
    file_name = doc.file_name or "file"
    dest      = os.path.join(pdir, file_name)
    await file_obj.download_to_drive(dest)

    context.user_data["new_project_files"].append(file_name)

    if file_name.lower().endswith(".zip"):
        try:
            with zipfile.ZipFile(dest, "r") as zf:
                names = [n for n in zf.namelist() if not n.startswith("__MACOSX")]
                zf.extractall(pdir, members=names)
            os.remove(dest)

            top_levels = {n.split("/", 1)[0] for n in names if n and not n.startswith("/")}
            top_levels.discard("")
            if len(top_levels) == 1:
                only_root = next(iter(top_levels))
                root_path = os.path.join(pdir, only_root)
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
                f"❌ `{escape_md(file_name)}` corrupt zip. Dobara upload karein.",
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception as e:
            logger.error(f"Zip extract error for {file_name}: {e}")
            await update.message.reply_text(
                f"❌ Extract failed: `{escape_md(str(e)[:200])}`",
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
    uid   = update.effective_user.id
    name  = context.user_data.get("new_project_name")
    ptype = context.user_data.get("new_project_type", "python")
    pdir  = project_dir(uid, name)

    # ── Smart type correction ──
    # If user selected "java" but uploaded JS/Node files (and no .java files), auto-switch to nodejs
    if ptype == "java" and os.path.isdir(pdir):
        all_files = []
        for root, dirs, files in os.walk(pdir):
            dirs[:] = [d for d in dirs if d not in ("node_modules", ".git", "__pycache__", "venv")]
            for f in files:
                all_files.append(f)
        has_java_files = any(f.endswith(".java") for f in all_files)
        has_pom        = os.path.exists(os.path.join(pdir, "pom.xml"))
        has_gradle     = os.path.exists(os.path.join(pdir, "build.gradle"))
        has_js_files   = any(f.endswith(".js") or f.endswith(".ts") or f.endswith(".mjs") for f in all_files)
        has_pkg_json   = os.path.exists(os.path.join(pdir, "package.json"))
        if not has_java_files and not has_pom and not has_gradle and (has_js_files or has_pkg_json):
            ptype = "nodejs"
            msg_source_warn = update.message if via_message else update.callback_query.message
            await msg_source_warn.reply_text(
                "⚠️ *Auto-correction:* You selected *Java* but your files are JavaScript (Node.js)!\n\n"
                "Bot automatically switched to *Node.js* mode. ✅",
                parse_mode=ParseMode.MARKDOWN,
            )

    type_lbl = PROJECT_TYPE_LABELS.get(ptype, "🐍 Python")

    msg_source = update.message if via_message else update.callback_query.message
    status_msg = await msg_source.reply_text(
        f"⚙️ *Setting up {escape_md(name)}* — {type_lbl}\n\n⏳ Initializing...",
        parse_mode=ParseMode.MARKDOWN,
    )

    results = []
    default_cmd = None

    # ── HTML: no venv needed ──
    if ptype == "html":
        results.append("🌐 HTML project — no requirements to install")
        default_cmd = "html_serve"
        # Check for index.html
        if os.path.exists(os.path.join(pdir, "index.html")):
            results.append("✅ index.html found")
        else:
            results.append("⚠️ index.html not found — upload via File Manager")

    # ── Java ──
    elif ptype == "java":
        pom_xml   = os.path.join(pdir, "pom.xml")
        build_grd = os.path.join(pdir, "build.gradle")

        if os.path.exists(pom_xml):
            results.append("✅ Maven project (pom.xml) found")
            mvn_progress = LiveProgress(status_msg, title=f"Setup — {name} (Maven)")
            await mvn_progress.start("🔍 Looking for Maven ...")
            mvn_progress.run_in_background(estimated_seconds=150, status="Setting up Maven")
            mvn_exe = await _find_or_install_mvn(pdir)
            if not mvn_exe:
                await mvn_progress.stop(success=False, final_text="Maven not found & auto-install failed")
                results.append("❌ Maven not found — run: apt-get install -y maven on your server")
            else:
                try:
                    proc = await asyncio.wait_for(
                        create_subprocess_exec(mvn_exe, "dependency:resolve", "-q",
                                               stdout=PIPE, stderr=PIPE, cwd=pdir),
                        timeout=300,
                    )
                    _, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
                    if proc.returncode == 0:
                        await mvn_progress.stop(success=True, final_text="Maven dependencies resolved!")
                        results.append("✅ Maven dependencies resolved")
                    else:
                        await mvn_progress.stop(success=False, final_text=stderr.decode()[:200])
                        results.append("⚠️ Maven dependency resolve failed (check pom.xml)")
                except asyncio.TimeoutError:
                    await mvn_progress.stop(success=False, final_text="Maven timed out")
                    results.append("⚠️ Maven timed out")
                except Exception as e:
                    await mvn_progress.stop(success=False, final_text=str(e))
                    results.append(f"⚠️ Maven error: {e}")
            main_class  = _detect_java_main_class(pdir)
            mvn_bin     = mvn_exe or "mvn"
            default_cmd = f"{mvn_bin} -q compile exec:java -Dexec.mainClass={main_class}"

        elif os.path.exists(build_grd):
            results.append("✅ Gradle project (build.gradle) found")
            default_cmd = "./gradlew run"

        else:
            java_files = [f for f in os.listdir(pdir) if f.endswith(".java")] if os.path.isdir(pdir) else []
            if java_files:
                main_class = java_files[0].replace(".java", "")
                results.append(f"✅ Java file found: {java_files[0]}")
                default_cmd = f"javac {main_class}.java && java {main_class}"
            else:
                results.append("⚠️ No pom.xml, build.gradle, or .java files found")
                default_cmd = "java -jar app.jar"

    # ── Node.js ──
    elif ptype == "nodejs":
        # Create venv step skipped for Node.js
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
        else:
            results.append("⚠️ No package.json found")

        default_cmd = _default_run_command(pdir, "nodejs")

    # ── Python ──
    else:
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
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)
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

        # Step 2: Install requirements
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
                _, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
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
                        create_subprocess_exec(pip_path, "list", stdout=PIPE, stderr=PIPE), timeout=30)
                    out2, _ = await asyncio.wait_for(proc2.communicate(), timeout=30)
                    pkg_count = len(out2.decode().strip().splitlines()) - 2
                    results.append(f"✅ {pkg_count} packages verified")
                except Exception:
                    results.append("⚠️ Could not verify packages")
        else:
            results.append("ℹ️ No requirements.txt found")

        default_cmd = _default_run_command(pdir, "python")

    # Save to DB
    await projects_col.insert_one({
        "user_id":         uid,
        "name":            name,
        "project_type":    ptype,
        "run_command":     default_cmd,
        "created_date":    datetime.now(timezone.utc),
        "last_run":        None,
        "exit_code":       None,
        "status":          "stopped",
        "pid":             None,
        "admin_stopped":   False,
        "auto_restart":    True,
        "restart_count":   0,
        "last_restart_at": None,
    })

    result_text = "\n".join(results)

    if ptype == "html":
        html_url = html_project_url(uid, name)
        result_text += (
            f"\n\n🌐 *Permanent URL:*\n`{escape_md(html_url)}`\n"
            f"_Click ▶️ Run to make your site live!_"
        )
    elif default_cmd:
        result_text += f"\n\n🚀 Default run cmd: `{escape_md(str(default_cmd))}`"
    else:
        result_text += "\n\n⚠️ No main file detected. Set run command manually."

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Open Dashboard", callback_data=f"proj:{name}")],
        [InlineKeyboardButton("🔙 My Projects",    callback_data="my_projects")],
    ])
    await status_msg.edit_text(
        f"🎉 *Project {escape_md(name)} ready!* — {type_lbl}\n\n{result_text}\n\n`[████████████] ✅ Complete!`",
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
# Admin Panel
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
        f"{backup_info}"
    )

    bot_locked_now = await is_bot_locked()
    lock_lbl       = "🔓 Unlock Bot (Free)" if bot_locked_now else "🔒 Lock Bot (Premium Only)"
    db_backend_now = _read_db_backend()
    db_lbl         = f"🗄 DB: {'Local SQLite' if db_backend_now == 'local' else 'MongoDB'} → Switch"

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
        [InlineKeyboardButton(lock_lbl,              callback_data="admin:toggle_lock")],
        [InlineKeyboardButton(db_lbl,                callback_data="admin:switch_db")],
        [InlineKeyboardButton("📊 Bot Status",       callback_data="bot_status")],
    ]
    if uid == OWNER_ID:
        kb_rows.append([
            InlineKeyboardButton("➕ Add Admin",    callback_data="admin:add_admin"),
            InlineKeyboardButton("➖ Remove Admin", callback_data="admin:remove_admin"),
        ])
    kb_rows.append([InlineKeyboardButton("🔙 Back", callback_data="back_start")])

    await safe_edit(query, text, reply_markup=InlineKeyboardMarkup(kb_rows), parse_mode=ParseMode.MARKDOWN)

@admin_or_owner
async def cb_admin_backup_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("⏳ Running backup...", show_alert=False)

    await safe_edit(query, "💾 *Backup in progress...*\n\nThis may take a moment.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]),
                    parse_mode=ParseMode.MARKDOWN)

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
                    fpath    = os.path.join(root, fname)
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
                    "type": "file_backup", "user_id": uid, "project_name": name,
                    "files": files_data, "backed_up_at": datetime.now(timezone.utc),
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
                    logger.warning(f"Backup write failed for {name}: {e}")

        now = datetime.now(timezone.utc)
        await backups_col.delete_many({"type": "backup_meta"})
        await backups_col.insert_one({
            "type": "backup_meta", "total_projects": len(all_projects),
            "total_files": total_files, "total_size": total_size,
            "backed_up_at": now, "distribution": db_distribution,
        })

        backup_time = escape_md(now.strftime("%Y-%m-%d %H:%M UTC"))
        dist_lines = ""
        if db_distribution:
            dist_lines = "\n*Storage Distribution:*\n"
            for db_name, count in sorted(db_distribution.items()):
                dist_lines += f"   • `{escape_md(db_name)}`: `{count}` projects\n"

        result_text = (
            f"✅ *Backup Complete!*\n\n"
            f"📁 Projects: `{len(all_projects)}`\n"
            f"📄 Files: `{total_files}`\n"
            f"📦 Size: `{escape_md(fmt_bytes(total_size))}`\n"
            f"🕐 Time: `{backup_time}`"
            f"{dist_lines}"
        )
    except Exception as e:
        logger.error(f"Manual backup failed: {e}")
        result_text = f"❌ *Backup Failed!*\n\n`{escape_md(str(e))}`"

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]])
    await safe_edit(query, result_text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

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
    lines.append("\nThis action is *permanent* and *cannot be undone*.")

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🗑 Yes, Delete All", callback_data="admin:del_backups_confirm")],
        [InlineKeyboardButton("🔙 Cancel",          callback_data="admin_panel")],
    ])
    await safe_edit(query, "\n".join(lines), reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

@owner_only
async def cb_admin_delete_backups_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("⏳ Deleting backups...", show_alert=False)

    await safe_edit(query, "🗑 *Deleting all backups...*\n\nPlease wait.", parse_mode=ParseMode.MARKDOWN)

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
        lines.append("\n⚠️ *Some errors:*")
        for err in errors[:5]:
            lines.append(f"`{escape_md(err)}`")

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]])
    await safe_edit(query, "\n".join(lines), reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

@admin_or_owner


# ─────────────────────────────────────────────────────────────
# Admin: Toggle Bot Lock
# ─────────────────────────────────────────────────────────────

async def cb_admin_toggle_lock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    if uid != OWNER_ID and not await is_admin(uid):
        await safe_edit(query, "⛔ Access denied.")
        return

    currently_locked = await is_bot_locked()
    new_state = not currently_locked
    await set_bot_locked(new_state)

    state_txt = "🔒 *Bot Locked*\n\nFree users can no longer use the bot.\nOnly Premium users have access." if new_state else "🔓 *Bot Unlocked*\n\nAll users can use the bot normally."
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]])
    await safe_edit(query, state_txt, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)


# ─────────────────────────────────────────────────────────────
# Admin: Switch Database Backend
# ─────────────────────────────────────────────────────────────

async def cb_admin_switch_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    if uid != OWNER_ID and not await is_admin(uid):
        await safe_edit(query, "⛔ Access denied.")
        return

    current = _read_db_backend()
    target  = "local" if current == "mongodb" else "mongodb"
    target_lbl = "Local SQLite" if target == "local" else "MongoDB"
    current_lbl = "Local SQLite" if current == "local" else "MongoDB"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"✅ Yes, switch to {target_lbl}", callback_data=f"admin:switch_db_confirm:{target}")],
        [InlineKeyboardButton("❌ Cancel", callback_data="admin_panel")],
    ])
    await safe_edit(
        query,
        f"🗄 *Switch Database Backend*\n\n"
        f"Current: *{current_lbl}*\n"
        f"Switch to: *{target_lbl}*\n\n"
        f"{'⚠️ Data will be migrated automatically.' if MONGODB_URI else '⚠️ No MONGODB_URI set — switching to Local SQLite only.'}\n\n"
        f"Bot will use the new database on next restart.",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )


async def cb_admin_switch_db_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id
    if uid != OWNER_ID and not await is_admin(uid):
        await safe_edit(query, "⛔ Access denied.")
        return

    target = query.data.split(":", 2)[2]   # "local" or "mongodb"

    await safe_edit(query, f"⏳ Migrating data to {'Local SQLite' if target == 'local' else 'MongoDB'}…")

    try:
        if target == "local":
            ok, msg = await migrate_mongodb_to_local()
        else:
            ok, msg = await migrate_local_to_mongodb()
    except Exception as e:
        ok, msg = False, str(e)

    _write_db_backend(target)
    target_lbl = "Local SQLite" if target == "local" else "MongoDB"
    status_icon = "✅" if ok else "⚠️"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Admin Panel", callback_data="admin_panel")]])
    await safe_edit(
        query,
        f"{status_icon} *DB switched to {target_lbl}*\n\n"
        f"{'Migration: ' + msg}\n\n"
        f"Restart the bot for changes to fully take effect.",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )

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
        if u.get("is_admin"):   badges += " 🛡"
        if u.get("is_premium"): badges += " 💎"
        if u.get("is_banned"):  badges += " 🚫"
        uname = f"@{u['username']}" if u.get("username") else "no-username"
        lines.append(f"`{u['user_id']}` {escape_md(uname)}{badges}")

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"admin:user_list:{page-1}"))
    if (page + 1) * per_page < total:
        nav.append(InlineKeyboardButton("➡️ Next", callback_data=f"admin:user_list:{page+1}"))

    kb_rows = []
    if nav: kb_rows.append(nav)
    kb_rows.append([InlineKeyboardButton("🔙 Back", callback_data="admin_panel")])

    await safe_edit(query, "\n".join(lines), reply_markup=InlineKeyboardMarkup(kb_rows), parse_mode=ParseMode.MARKDOWN)

@admin_or_owner
async def cb_admin_running(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        running = await projects_col.find({"status": "running"}).to_list(length=100)
        if not running:
            await safe_edit(query, "🟢 *Running Scripts*\n\nNo projects running.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]),
                            parse_mode=ParseMode.MARKDOWN)
            return

        lines = ["🟢 *Running Scripts*\n"]
        kb_rows = []

        for p in running:
            user_doc = await get_user(p["user_id"])
            fname = user_doc.get("first_name", "Unknown") if user_doc else "Unknown"
            uname = f"@{user_doc['username']}" if user_doc and user_doc.get("username") else "no-username"
            pid   = p.get("pid", "N/A")
            ptype = PROJECT_TYPE_LABELS.get(p.get("project_type", "python"), "🐍")

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
                f"📁 {ptype} {p['name']}\n"
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
        await safe_edit(query, f"❌ Error: {str(e)[:200]}",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]))

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
        await safe_edit(query, "👥 *All Users & Projects*\n\nNo projects found.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin:running")]]),
                        parse_mode=ParseMode.MARKDOWN)
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
            status_icon = "🟢" if p.get("status") == "running" else "🔴"
            ptype = PROJECT_TYPE_LABELS.get(p.get("project_type", "python"), "🐍")
            proj_lines.append(f"  {status_icon} {ptype} {p['name']}")

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
        await safe_edit(query, "❌ Project not found.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin:all_projects:0")]]))
        return

    ptype = p.get("project_type", "python")
    if not p.get("run_command") and ptype != "html":
        await safe_edit(query, f"❌ No run command set for *{escape_md(name)}*.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin:all_projects:0")]]),
                        parse_mode=ParseMode.MARKDOWN)
        return

    if p.get("status") == "running":
        if ptype != "html" and p.get("pid") and psutil.pid_exists(p["pid"]):
            await safe_edit(query, f"▶️ Project *{escape_md(name)}* is already running.\nPID: `{p.get('pid')}`",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin:all_projects:0")]]),
                            parse_mode=ParseMode.MARKDOWN)
            return

    try:
        updated = await start_project_process(uid, name)
        try:
            if notification_bot:
                await notification_bot.send_message(uid, f"▶️ Your project *{escape_md(name)}* was started by admin.", parse_mode=ParseMode.MARKDOWN)
        except Exception:
            pass

        await safe_edit(query, f"✅ Project *{escape_md(name)}* started by admin.\nPID: `{updated.get('pid', 'N/A')}`",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("👥 All Projects", callback_data="admin:all_projects:0")],
                            [InlineKeyboardButton("🟢 Running Scripts", callback_data="admin:running")],
                        ]),
                        parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await safe_edit(query, f"❌ Failed to start *{escape_md(name)}*: `{escape_md(str(e))[:250]}`",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin:all_projects:0")]]),
                        parse_mode=ParseMode.MARKDOWN)

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
            await query.message.reply_document(document=f, filename=f"{name}.zip",
                                               caption=f"📥 Project: {name}\nUser ID: {uid}")
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
        await safe_edit(query, "❌ Project not found.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin:running")]]))
        return

    try:
        await kill_project(uid, name)
        await projects_col.update_one({"user_id": uid, "name": name}, {"$set": {"admin_stopped": True}})
        try:
            if notification_bot:
                await notification_bot.send_message(uid, f"⏹ Your project *{escape_md(name)}* was stopped by admin.\nContact owner to resume.", parse_mode=ParseMode.MARKDOWN)
        except Exception:
            pass

        await safe_edit(query, f"✅ Project *{escape_md(name)}* stopped (admin).",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🟢 Running Scripts", callback_data="admin:running")],
                            [InlineKeyboardButton("👥 All Projects", callback_data="admin:all_projects:0")],
                        ]),
                        parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await safe_edit(query, f"❌ Failed to stop *{escape_md(name)}*: `{escape_md(str(e))[:250]}`",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin:running")]]),
                        parse_mode=ParseMode.MARKDOWN)

# ─────────────────────────────────────────────────────────────
# Admin — Give/Remove/Temp Premium, Ban/Unban, Broadcast
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

    # BUGFIX: upsert=True so user doesn't need to have started the bot first
    await users_col.update_one(
        {"user_id": uid},
        {"$set": {"is_premium": True, "premium_expiry": None}},
        upsert=True,
    )
    # Unlock any projects that were locked due to premium expiry
    await projects_col.update_one(
        {"user_id": uid},
        {"$unset": {"locked": ""}},
    )
    # Unlock ALL locked projects for this user
    all_locked = await projects_col.find({"user_id": uid, "locked": True}).to_list(length=1000)
    for lp in all_locked:
        await projects_col.update_one(
            {"user_id": uid, "name": lp["name"]},
            {"$unset": {"locked": ""}},
        )
    try:
        await update.get_bot().send_message(uid, "🎉 You have been granted *Premium*! Enjoy unlimited projects!\n\nAll locked projects have been unlocked.", parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass
    await update.message.reply_text(f"✅ Premium granted to `{uid}`.", parse_mode=ParseMode.MARKDOWN)
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
    await update.message.reply_text(f"✅ Premium removed from `{uid}`.", parse_mode=ParseMode.MARKDOWN)
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
    await update.message.reply_text("⏰ Send duration (e.g. `24h` or `7d`):", parse_mode=ParseMode.MARKDOWN)
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

    # BUGFIX: upsert=True
    await users_col.update_one(
        {"user_id": uid},
        {"$set": {"is_premium": True, "premium_expiry": expiry}},
        upsert=True,
    )
    try:
        await update.get_bot().send_message(uid, f"🎉 You received *Temp Premium* for {escape_md(text)}!", parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass
    await update.message.reply_text(f"✅ Temp premium set for `{uid}` — expires {escape_md(expiry.strftime('%Y-%m-%d %H:%M UTC'))}.", parse_mode=ParseMode.MARKDOWN)
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
        # BUGFIX: rate limit — 30 msg/sec max
        await asyncio.sleep(0.05)
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
# Add / Remove Admin (Owner only)
# ─────────────────────────────────────────────────────────────

@owner_only
async def cb_admin_add_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await safe_edit(query, "🛡 *Add Admin*\n\nSend the numeric user ID to make admin:", parse_mode=ParseMode.MARKDOWN)
    return ADMIN_ADD_ADMIN_ID

async def admin_add_admin_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid ID. Send a numeric user ID:", parse_mode=ParseMode.MARKDOWN)
        return ADMIN_ADD_ADMIN_ID

    if uid == OWNER_ID:
        await update.message.reply_text("⚠️ Owner already has the highest privileges!", parse_mode=ParseMode.MARKDOWN)
        return ConversationHandler.END

    # BUGFIX: upsert=True so admin can be set even if user hasn't used bot yet
    await users_col.update_one(
        {"user_id": uid},
        {"$set": {"is_admin": True}},
        upsert=True,
    )
    try:
        await update.get_bot().send_message(uid, "🎉 You have been made *Admin*! You can now access the Admin Panel.", parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass
    await update.message.reply_text(f"✅ User `{uid}` has been made Admin.", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

@owner_only
async def cb_admin_remove_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await safe_edit(query, "➖ *Remove Admin*\n\nSend the user ID to remove admin access:", parse_mode=ParseMode.MARKDOWN)
    return ADMIN_REMOVE_ADMIN_ID

async def admin_remove_admin_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid ID. Send a numeric user ID:", parse_mode=ParseMode.MARKDOWN)
        return ADMIN_REMOVE_ADMIN_ID

    await users_col.update_one({"user_id": uid}, {"$set": {"is_admin": False}})
    try:
        await update.get_bot().send_message(uid, "⚠️ Your *Admin* access has been removed.", parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass
    await update.message.reply_text(f"✅ Admin access removed for user `{uid}`.", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

# ─────────────────────────────────────────────────────────────
# Auto-Restart Toggle
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
    ptype = p.get("project_type", "python")
    # BUGFIX: added parse_mode
    await safe_edit(
        query,
        project_dashboard_text(p),
        reply_markup=project_dashboard_kb(uid, name, p.get("auto_restart", True),
                                          p.get("status") == "running", ptype),
        parse_mode=ParseMode.MARKDOWN,
    )

# ─────────────────────────────────────────────────────────────
# Environment Variables Manager
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
        with open(env_path, "r", encoding="utf-8") as f:
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
        "Send your variables in any format:\n\n"
        "1️⃣ *Single variable:*\n"
        "`API_KEY=your_value`\n\n"
        "2️⃣ *Multiple at once (one per line):*\n"
        "`TOKEN=abc123`\n"
        "`DB_URI=mongodb://...`\n"
        "`OWNER_ID=12345`\n\n"
        "3️⃣ *Just key name:*\n"
        "`API_KEY`\n"
        "_(bot will ask for value next)_\n\n"
        "💡 Spaces around `=` are fine!",
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

    lines_input = text.strip().split("\n")
    pairs_to_save = []

    for line in lines_input:
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
        existing = {}
        existing_order = []
        if os.path.exists(env_path):
            with open(env_path, "r", encoding="utf-8") as f:
                for eline in f:
                    eline_stripped = eline.strip()
                    if eline_stripped and not eline_stripped.startswith("#") and "=" in eline_stripped:
                        ekey, _, evalue = eline_stripped.partition("=")
                        ekey = ekey.strip()
                        existing[ekey] = evalue.strip()
                        existing_order.append(ekey)

        for key, value in pairs_to_save:
            existing[key] = value
            if key not in existing_order:
                existing_order.append(key)

        # Write all vars (BUGFIX: removed dead second loop)
        with open(env_path, "w", encoding="utf-8") as f:
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

    # No = found — treat as single KEY name
    key = text.strip().split()[0] if text.strip() else ""
    if not key or len(key) > 100:
        await update.message.reply_text(
            "❌ Could not parse variables.\n\n"
            "Send in format:\n`API_KEY=your_value`\nor just the key name: `API_KEY`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return ENV_ADD_KEY

    context.user_data["env_key"] = key
    await update.message.reply_text(f"Now send the value for `{key}`:", parse_mode=ParseMode.MARKDOWN)
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
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip().startswith(f"{key}="):
                    env_lines.append(f"{key}={value}\n")
                    key_found = True
                else:
                    env_lines.append(line)

    if not key_found:
        env_lines.append(f"{key}={value}\n")

    with open(env_path, "w", encoding="utf-8") as f:
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
    parts = query.data.split(":", 2)
    name = parts[1]
    key = parts[2]
    uid = query.from_user.id

    pdir = project_dir(uid, name)
    env_path = os.path.join(pdir, ".env")

    deleted = False
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        with open(env_path, "w", encoding="utf-8") as f:
            for line in lines:
                if line.strip().startswith(f"{key}="):
                    deleted = True
                    continue
                f.write(line)

    if deleted:
        await query.answer(f"🗑 {key} deleted!", show_alert=True)
    else:
        await query.answer(f"⚠️ {key} not found.", show_alert=True)

    # Refresh env vars screen
    env_vars = {}
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
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
# Help & Support Ticket System
# ─────────────────────────────────────────────────────────────

async def cb_help_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id

    if await is_banned(uid):
        await safe_edit(query, "🚫 You are banned. Contact owner.")
        return

    text = (
        "❓ *Help & Support*\n\n"
        "Having a problem? Raise a ticket — the owner will help you directly.\n\n"
        "📋 *Include in your ticket:*\n"
        "• Project name (if related)\n"
        "• What problem you are facing\n"
        "• Error message (if any)\n\n"
        "📌 *Common Solutions:*\n"
        "• Project not running → try `📦 Reinstall Requirements`\n"
        "• Logs not showing → click the `📋 Logs` button\n"
        "• Files not uploading → send files smaller than 10MB\n\n"
        "Press the button below to raise a ticket 👇"
    )

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🎫 Raise Ticket", callback_data="raise_ticket")],
        [InlineKeyboardButton("🔙 Back",          callback_data="back_start")],
    ])
    await safe_edit(query, text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)


async def cb_raise_ticket(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid = query.from_user.id

    if await is_banned(uid):
        await safe_edit(query, "🚫 You are banned. Contact owner.")
        return

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="help_menu")]])
    await safe_edit(
        query,
        "🎫 *New Support Ticket*\n\n"
        "Describe your problem and send it.\n"
        "Owner will receive your message directly.\n\n"
        "_The more detail you provide, the faster we can help._",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )
    return HELP_TICKET_MSG


async def help_ticket_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid   = update.effective_user
    text  = update.message.text.strip()

    if not text:
        await update.message.reply_text("⚠️ Ticket cannot be empty. Please describe your issue.")
        return HELP_TICKET_MSG

    user_doc = await get_user(uid.id)
    premium  = "💎 Premium" if user_doc and user_doc.get("is_premium") else "🆓 Free"
    count    = await project_count(uid.id)
    uname    = f"@{uid.username}" if uid.username else "no-username"

    ticket_text = (
        f"🎫 *New Support Ticket*\n\n"
        f"👤 User: {escape_md(uid.first_name)} ({uname})\n"
        f"🆔 ID: `{uid.id}`\n"
        f"💎 Plan: {premium}\n"
        f"📁 Projects: `{count}`\n"
        f"🕐 Time: `{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}`\n\n"
        f"📝 *Problem:*\n{escape_md(text)}"
    )

    sent = False
    if notification_bot:
        try:
            await notification_bot.send_message(
                chat_id=OWNER_ID,
                text=ticket_text,
                parse_mode=ParseMode.MARKDOWN,
            )
            sent = True
        except Exception as e:
            logger.error(f"Ticket send failed: {e}")

    if sent:
        await update.message.reply_text(
            "✅ *Ticket Sent!*\n\n"
            "Owner has been notified and will reply soon.\n"
            "_Please be patient — response usually comes within 1-24 hours._",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_start")]]),
        )
    else:
        await update.message.reply_text(
            "❌ Failed to send ticket. Please try again later.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_start")]]),
        )

    return ConversationHandler.END


# ─────────────────────────────────────────────────────────────
# Smart Error Detector + Auto Dependency Fixer
# ─────────────────────────────────────────────────────────────

def _analyze_error(log_text: str) -> str:
    """Analyze log text and return a simple English explanation."""
    log_lower = log_text.lower()

    if "modulenotfounderror" in log_lower or "no module named" in log_lower:
        import re
        match = re.search(r"no module named ['\"]([^'\"]+)['\"]", log_lower)
        mod = match.group(1) if match else "koi module"
        return (
            f"📦 *Missing Package Detected*\n\n"
            f"Python package `{mod}` is not installed.\n"
            f"Bot will automatically try to install it. ⚙️"
        )

    if "syntaxerror" in log_lower:
        import re
        line_match = re.search(r'line (\d+)', log_lower)
        line_info = f" (line {line_match.group(1)})" if line_match else ""
        return (
            f"🔴 *Syntax Error{line_info}*\n\n"
            f"There is a syntax mistake in your code.\n"
            f"📁 Open Files → edit your code and check{line_info}."
        )

    if "permission denied" in log_lower:
        return (
            "🔒 *Permission Denied*\n\n"
            "No permission to access that file or folder.\n"
            "Check that you are using the correct file path."
        )

    if "address already in use" in log_lower or "port" in log_lower and "in use" in log_lower:
        return (
            "🌐 *Port Already In Use*\n\n"
            "That port is already being used by another process.\n"
            "Try a different port number in your code."
        )

    if "connection refused" in log_lower or "connectionrefusederror" in log_lower:
        return (
            "🔌 *Connection Refused*\n\n"
            "Could not connect to a service (database, API).\n"
            "Check that MONGODB_URI or other env vars are correctly set."
        )

    if "keyerror" in log_lower or "missing key" in log_lower:
        return (
            "🔑 *Missing Config Key*\n\n"
            "A required variable or env key is not set.\n"
            "🔐 Check Env Vars — a token or key might be missing."
        )

    if "timeout" in log_lower or "timed out" in log_lower:
        return (
            "⏱ *Connection Timeout*\n\n"
            "No response received from a service.\n"
            "Internet or database connection may be slow/unavailable."
        )

    if "out of memory" in log_lower or "memoryerror" in log_lower:
        return (
            "💾 *Out of Memory*\n\n"
            "Project is using too much RAM.\n"
            "Optimize your code or reduce heavy operations."
        )

    if "typeerror" in log_lower:
        return (
            "🔴 *Type Error*\n\n"
            "Wrong type of data is being used in your code.\n"
            "Check the line number in your logs and fix it."
        )

    if "importerror" in log_lower:
        return (
            "📦 *Import Error*\n\n"
            "A library cannot be imported.\n"
            "Try `📦 Reinstall Requirements`."
        )

    return (
        "❌ *Project Crashed*\n\n"
        "See the last log lines below to understand the issue.\n"
        "📁 Check your code in the Files Manager."
    )


async def _try_auto_fix(uid: int, name: str, log_text: str) -> bool:
    """
    Detect ModuleNotFoundError and auto pip install the missing module.
    Returns True if fix was successful.
    """
    import re
    if "no module named" not in log_text.lower():
        return False

    match = re.search(r"no module named ['\"]([^'\".']+)", log_text.lower())
    if not match:
        return False

    module = match.group(1).strip()
    if not module or len(module) > 50:
        return False

    pdir     = project_dir(uid, name)
    pip_path = os.path.join(pdir, "venv", "bin", "pip")
    if not os.path.exists(pip_path):
        pip_path = "pip3"

    logger.info(f"Auto-fix: installing missing module '{module}' for {uid}:{name}")

    try:
        proc = await asyncio.wait_for(
            create_subprocess_exec(pip_path, "install", module, stdout=PIPE, stderr=PIPE),
            timeout=120,
        )
        _, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
        if proc.returncode == 0:
            logger.info(f"Auto-fix: '{module}' installed successfully for {uid}:{name}")
            return True
        else:
            logger.warning(f"Auto-fix: pip install '{module}' failed: {stderr.decode()[:200]}")
            return False
    except Exception as e:
        logger.error(f"Auto-fix error for {uid}:{name}: {e}")
        return False


# ─────────────────────────────────────────────────────────────
# Background: Process Monitor
# ─────────────────────────────────────────────────────────────

async def process_monitor():
    while True:
        await asyncio.sleep(30)
        try:
            running = await projects_col.find({"status": "running"}).to_list(length=1000)
            for p in running:
                ptype = p.get("project_type", "python")

                # HTML projects have no subprocess — skip PID check
                if ptype == "html":
                    continue

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
                    # Clean up context_store
                    context_store.pop(key, None)

                    logger.info(f"Process {key} exited with code {code}")

                    # Read log for smart analysis
                    log_text = ""
                    log_path = os.path.join(project_dir(p["user_id"], p["name"]), "output.log")
                    if os.path.exists(log_path):
                        with open(log_path, "r", errors="replace") as _lf:
                            _lines = _lf.readlines()
                        log_text = "".join(_lines[-30:])

                    # Auto-fix missing Python modules before restart
                    auto_fixed = False
                    if code != 0 and log_text and p.get("project_type", "python") == "python":
                        auto_fixed = await _try_auto_fix(p["user_id"], p["name"], log_text)
                        if auto_fixed and notification_bot:
                            try:
                                await notification_bot.send_message(
                                    chat_id=p["user_id"],
                                    text=(
                                        f"🔧 *Auto-Fix Applied*\n\n"
                                        f"Project `{p['name']}` had a missing package detected.\n"
                                        f"Auto-installing and restarting now... ⚙️"
                                    ),
                                    parse_mode=ParseMode.MARKDOWN,
                                )
                            except Exception:
                                pass

                    # Smart error explanation
                    smart_msg = _analyze_error(log_text) if log_text and code != 0 else ""

                    # Auto-restart logic (only for non-zero exits)
                    if p.get("auto_restart", True) and code != 0 and not p.get("admin_stopped"):
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
                                        restart_note = f"\n\n{smart_msg}" if smart_msg and not auto_fixed else ""
                                        await notification_bot.send_message(
                                            chat_id=p["user_id"],
                                            text=(
                                                f"🔄 *Auto-Restart*\n\n"
                                                f"Project `{p['name']}` crashed (exit code: {code}).\n"
                                                f"Auto-restarted! ({restart_count + 1}/3)"
                                                f"{restart_note}"
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
                                            f"{smart_msg}\n\n"
                                            f"Please restart it manually from the bot."
                                        ),
                                        parse_mode=ParseMode.MARKDOWN,
                                    )
                                except Exception:
                                    pass

                    elif code != 0 and not p.get("admin_stopped"):
                        if notification_bot:
                            try:
                                error_lines = log_text[-500:] if len(log_text) > 500 else log_text

                                msg_text = (
                                    f"{smart_msg}\n\n"
                                    f"📁 Project: `{p['name']}`\n"
                                    f"🔢 Exit Code: `{code}`\n\n"
                                    f"📋 *Last Logs:*\n```\n{escape_md(error_lines.strip())}\n```"
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
# Auto Backup Task
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
                        "type": "file_backup", "user_id": uid, "project_name": name,
                        "files": files_data, "backed_up_at": datetime.now(timezone.utc),
                        "stored_in": target_db_name,
                    })
                    db_distribution[target_db_name] = db_distribution.get(target_db_name, 0) + 1

            await backups_col.delete_many({"type": "backup_meta"})
            await backups_col.insert_one({
                "type": "backup_meta", "total_projects": len(all_projects),
                "total_files": total_files, "total_size": total_size,
                "backed_up_at": datetime.now(timezone.utc), "distribution": db_distribution,
            })
            logger.info(f"Auto backup: {len(all_projects)} projects, {total_files} files — {db_distribution}")

        except Exception as e:
            logger.error(f"Backup failed: {e}")

# ─────────────────────────────────────────────────────────────
# Keep-Alive Task
# ─────────────────────────────────────────────────────────────

async def keep_alive_task():
    import urllib.request
    health_url = f"{BASE_URL}/health"
    logger.info(f"Keep-alive task started. Pinging {health_url} every 10 minutes.")

    while True:
        await asyncio.sleep(600)

        # Clean expired file-manager tokens
        try:
            result = await tokens_col.delete_many({"expires_at": {"$lt": time.time()}})
            if result.deleted_count:
                logger.info(f"Cleaned {result.deleted_count} expired file-manager tokens")
        except Exception as e:
            logger.warning(f"Token cleanup failed: {e}")

        # Keep-alive ping — BUGFIX: use get_running_loop() instead of get_event_loop()
        try:
            loop = asyncio.get_running_loop()
            resp = await loop.run_in_executor(
                None,
                lambda: urllib.request.urlopen(health_url, timeout=30).status
            )
            logger.info(f"Keep-alive ping OK ({resp})")
        except Exception as e:
            logger.warning(f"Keep-alive ping failed: {e}")

# ─────────────────────────────────────────────────────────────
# Auto Restore (runs ONCE at startup)
# ─────────────────────────────────────────────────────────────

async def restore_from_backup():
    try:
        logger.info("Checking for backups to restore...")

        meta = await backups_col.find_one({"type": "backup_meta"})
        if not meta:
            logger.info("No backup found. Fresh start.")
            return

        logger.info(f"Found backup from {meta['backed_up_at']} — {meta['total_projects']} projects, {meta['total_files']} files")

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

# ─────────────────────────────────────────────────────────────
# Install requirements for project (used on startup auto-restart)
# ─────────────────────────────────────────────────────────────

async def _install_requirements_for_project(uid: int, name: str) -> tuple:
    """
    Install project requirements (pip / npm / mvn).
    Returns: (success: bool, message: str)
    """
    pdir     = project_dir(uid, name)
    ptype    = "python"
    try:
        p = await get_project(uid, name)
        if p:
            ptype = p.get("project_type", "python")
    except Exception:
        pass

    # HTML — nothing to install
    if ptype == "html":
        return (True, "HTML project — no requirements needed")

    req_path = os.path.join(pdir, "requirements.txt")
    pkg_json = os.path.join(pdir, "package.json")
    pom_xml  = os.path.join(pdir, "pom.xml")
    venv_dir = os.path.join(pdir, "venv")
    pip_path = os.path.join(venv_dir, "bin", "pip")

    # Java — Maven
    if ptype == "java" and os.path.exists(pom_xml):
        mvn_exe = await _find_or_install_mvn(pdir)
        if not mvn_exe:
            return (False, "Maven not found. Run: apt-get install -y maven on your server")
        try:
            proc = await asyncio.wait_for(
                create_subprocess_exec(mvn_exe, "dependency:resolve", "-q", stdout=PIPE, stderr=PIPE, cwd=pdir),
                timeout=300,
            )
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
            if proc.returncode == 0:
                return (True, "Maven dependencies resolved")
            else:
                return (False, f"Maven failed: {stderr.decode()[:200]}")
        except asyncio.TimeoutError:
            return (False, "Maven timed out")
        except Exception as e:
            return (False, f"Maven error: {e}")

    # Node.js
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

    # Python — no requirements file → skip
    if not os.path.exists(req_path):
        return (True, "no requirements file found, skip")

    # Python venv
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

    # pip install
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

# ─────────────────────────────────────────────────────────────
# Auto-restart on startup (bot restart ke baad)
# ─────────────────────────────────────────────────────────────

async def auto_restart_on_startup():
    """
    After bot restart — projects that were 'running' in the database
    will have their requirements installed first, then be started.
    Waits 30 seconds for files to be restored first.
    """
    await asyncio.sleep(30)
    try:
        running_projects = await projects_col.find({
            "status": "running",
            "admin_stopped": {"$ne": True},
        }).to_list(length=10000)

        if not running_projects:
            logger.info("Auto-restart on startup: no running projects found.")
            return

        logger.info(f"Auto-restart on startup: processing {len(running_projects)} projects...")

        for proj in running_projects:
            uid  = proj["user_id"]
            name = proj["name"]
            ptype = proj.get("project_type", "python")
            try:
                # Stale PID saaf karo
                await projects_col.update_one(
                    {"user_id": uid, "name": name},
                    {"$set": {"status": "stopped", "pid": None}},
                )

                # User ko batao — requirements install ho raha hai
                if notification_bot:
                    type_lbl = PROJECT_TYPE_LABELS.get(ptype, "🐍")
                    try:
                        await notification_bot.send_message(
                            chat_id=uid,
                            text=(
                                f"🔄 *Bot Restarted*\n\n"
                                f"Project `{name}` ({type_lbl}) requirements are being installed...\n"
                                f"⏳ Your project will start automatically in a few moments."
                            ),
                            parse_mode=ParseMode.MARKDOWN,
                        )
                    except Exception:
                        pass

                # HTML projects — just mark as running again, no subprocess
                if ptype == "html":
                    await projects_col.update_one(
                        {"user_id": uid, "name": name},
                        {"$set": {"status": "running", "pid": None}},
                    )
                    logger.info(f"HTML project {uid}:{name} restored as live")
                    if notification_bot:
                        try:
                            html_url = html_project_url(uid, name)
                            await notification_bot.send_message(
                                chat_id=uid,
                                text=(
                                    f"✅ *HTML Project Live*\n\n"
                                    f"Project: `{name}`\n"
                                    f"🌐 URL: {html_url}"
                                ),
                                parse_mode=ParseMode.MARKDOWN,
                            )
                        except Exception:
                            pass
                    continue

                # Requirements install karo pehle
                logger.info(f"Installing requirements for {uid}:{name} before startup...")
                success, msg = await _install_requirements_for_project(uid, name)
                logger.info(f"Requirements for {uid}:{name}: {msg}")

                # Ab project start karo
                await asyncio.sleep(1)
                updated = await start_project_process(uid, name)
                logger.info(f"Auto-restarted on startup: {uid}:{name} PID={updated.get('pid')}")

                # User ko final status batao
                if notification_bot:
                    try:
                        req_status = "✅ Requirements installed" if success else f"⚠️ Requirements issue: {msg[:100]}"
                        await notification_bot.send_message(
                            chat_id=uid,
                            text=(
                                f"✅ *Project Started*\n\n"
                                f"Project: `{name}`\n"
                                f"{req_status}\n"
                                f"🟢 Your project is running now"
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
                                f"Please start it manually from the bot."
                            ),
                            parse_mode=ParseMode.MARKDOWN,
                        )
                    except Exception:
                        pass

        logger.info("Auto-restart on startup complete.")
    except Exception as e:
        logger.error(f"auto_restart_on_startup failed: {e}")

async def setup_venvs_background():
    """Setup virtualenvs for all restored Python projects in background."""
    try:
        all_projects = await projects_col.find({}).to_list(length=10000)
        for proj in all_projects:
            uid  = proj["user_id"]
            name = proj["name"]
            ptype = proj.get("project_type", "python")

            # Only Python projects need venv
            if ptype not in ("python", None):
                continue

            pdir = project_dir(uid, name)
            venv_dir = os.path.join(pdir, "venv")

            if os.path.exists(pdir) and not os.path.exists(venv_dir):
                try:
                    proc = await create_subprocess_exec(
                        sys.executable, "-m", "venv", venv_dir,
                        stdout=PIPE, stderr=PIPE
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
# HTML Flask Server (serves HTML projects at permanent URLs)
# ─────────────────────────────────────────────────────────────

# HTML project hosting is handled by file_manager.py Flask app (same port as BASE_URL)
# Routes: /html/<uid>/<project_name>/ and /html/<uid>/<project_name>/<filepath>

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

    # New project conversation (UPDATED: added NEW_PROJECT_TYPE state)
    new_proj_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(cb_new_project, pattern="^new_project$")],
        states={
            NEW_PROJECT_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, new_project_name),
                CallbackQueryHandler(new_project_cancel, pattern="^back_start$"),
            ],
            NEW_PROJECT_TYPE: [
                CallbackQueryHandler(new_project_type_select, pattern=r"^proj_type:"),
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
            ADMIN_GIVE_PREMIUM_ID:   [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_give_premium_id),
                                      CallbackQueryHandler(admin_conv_cancel, pattern="^admin_panel$")],
            ADMIN_REMOVE_PREMIUM_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_remove_premium_id),
                                      CallbackQueryHandler(admin_conv_cancel, pattern="^admin_panel$")],
            ADMIN_TEMP_PREMIUM_ID:   [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_temp_premium_id),
                                      CallbackQueryHandler(admin_conv_cancel, pattern="^admin_panel$")],
            ADMIN_TEMP_PREMIUM_DUR:  [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_temp_premium_dur),
                                      CallbackQueryHandler(admin_conv_cancel, pattern="^admin_panel$")],
            ADMIN_BAN_ID:            [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_ban_id),
                                      CallbackQueryHandler(admin_conv_cancel, pattern="^admin_panel$")],
            ADMIN_UNBAN_ID:          [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_unban_id),
                                      CallbackQueryHandler(admin_conv_cancel, pattern="^admin_panel$")],
            ADMIN_BROADCAST_MSG:     [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_broadcast_msg),
                                      CallbackQueryHandler(admin_conv_cancel, pattern="^admin_panel$")],
            ADMIN_SEND_USER_ID:      [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_send_user_id),
                                      CallbackQueryHandler(admin_conv_cancel, pattern="^admin_panel$")],
            ADMIN_SEND_USER_MSG:     [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_send_user_msg),
                                      CallbackQueryHandler(admin_conv_cancel, pattern="^admin_panel$")],
            ADMIN_ADD_ADMIN_ID:      [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_admin_id),
                                      CallbackQueryHandler(admin_conv_cancel, pattern="^admin_panel$")],
            ADMIN_REMOVE_ADMIN_ID:   [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_remove_admin_id),
                                      CallbackQueryHandler(admin_conv_cancel, pattern="^admin_panel$")],
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
    app.add_handler(CallbackQueryHandler(cb_admin_stop_project,pattern=r"^admin_stop:"))
    app.add_handler(CallbackQueryHandler(cb_admin_broadcast_menu, pattern="^admin:broadcast_menu$"))
    app.add_handler(CallbackQueryHandler(cb_admin_backup_now,  pattern="^admin:backup_now$"))
    app.add_handler(CallbackQueryHandler(cb_admin_delete_backups,         pattern="^admin:del_backups$"))
    app.add_handler(CallbackQueryHandler(cb_admin_delete_backups_confirm, pattern="^admin:del_backups_confirm$"))
    app.add_handler(CallbackQueryHandler(cb_admin_all_projects,     pattern=r"^admin:all_projects:\d+$"))
    app.add_handler(CallbackQueryHandler(cb_admin_run_project,      pattern=r"^admin_run:"))
    app.add_handler(CallbackQueryHandler(cb_admin_download_project,    pattern=r"^admin_dl:"))
    app.add_handler(CallbackQueryHandler(cb_admin_toggle_lock,          pattern="^admin:toggle_lock$"))
    app.add_handler(CallbackQueryHandler(cb_admin_switch_db,            pattern="^admin:switch_db$"))
    app.add_handler(CallbackQueryHandler(cb_admin_switch_db_confirm,    pattern=r"^admin:switch_db_confirm:"))

    app.add_handler(CallbackQueryHandler(cb_project_dashboard, pattern=r"^proj:"))
    app.add_handler(CallbackQueryHandler(cb_run,               pattern=r"^run:"))
    app.add_handler(CallbackQueryHandler(cb_stop,              pattern=r"^stop:"))
    app.add_handler(CallbackQueryHandler(cb_restart,           pattern=r"^restart:"))
    app.add_handler(CallbackQueryHandler(cb_logs,              pattern=r"^logs:"))
    app.add_handler(CallbackQueryHandler(cb_filemgr,           pattern=r"^filemgr:"))
    app.add_handler(CallbackQueryHandler(cb_delete_confirm,    pattern=r"^delete:.+$"))
    app.add_handler(CallbackQueryHandler(cb_delete_yes,        pattern=r"^delete_yes:"))
    app.add_handler(CallbackQueryHandler(cb_toggle_auto_restart, pattern=r"^toggle_ar:"))
    app.add_handler(CallbackQueryHandler(cb_envvars,             pattern=r"^envvars:"))
    app.add_handler(CallbackQueryHandler(cb_env_delete,          pattern=r"^env_del:"))
    app.add_handler(CallbackQueryHandler(cb_reinstall_reqs,      pattern=r"^reinstall_reqs:"))
    app.add_handler(CallbackQueryHandler(cb_help_menu,           pattern="^help_menu$"))

    # Help / Support Ticket conversation
    help_ticket_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(cb_raise_ticket, pattern="^raise_ticket$")],
        states={
            HELP_TICKET_MSG: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, help_ticket_receive),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", lambda u, c: ConversationHandler.END),
            CommandHandler("start",  lambda u, c: ConversationHandler.END),
            CallbackQueryHandler(lambda u, c: ConversationHandler.END, pattern="^help_menu$"),
        ],
        per_chat=True,
    )
    app.add_handler(help_ticket_conv)

    return app



# ─────────────────────────────────────────────────────────────
# Background Task: Periodic Premium Expiry Lock Check
# ─────────────────────────────────────────────────────────────

async def premium_expiry_lock_task():
    """Runs hourly — demotes any expired premium users and locks their extra projects."""
    await asyncio.sleep(60)   # wait 1 min before first check
    while True:
        try:
            all_users = await users_col.find({"is_premium": True}).to_list(length=10000)
            for u in all_users:
                try:
                    await check_premium_expiry(u["user_id"])
                except Exception as e:
                    logger.warning(f"Expiry check failed for {u.get('user_id')}: {e}")
        except Exception as e:
            logger.warning(f"premium_expiry_lock_task error: {e}")
        await asyncio.sleep(3600)   # run every hour

async def post_init(app: Application):
    global notification_bot, _process_semaphore
    notification_bot = app.bot
    _process_semaphore = asyncio.Semaphore(MAX_CONCURRENT_PROJECTS)

    await app.bot.set_my_commands([
        BotCommand("start",  "Start the bot"),
        BotCommand("done",   "Finish file upload"),
        BotCommand("cancel", "Cancel current action"),
    ])
    await restore_from_backup()
    asyncio.create_task(process_monitor())
    asyncio.create_task(backup_task())
    asyncio.create_task(keep_alive_task())
    asyncio.create_task(premium_expiry_lock_task())

def main():
    import threading

    # Start Flask file manager in daemon thread (also serves /html/ routes)
    try:
        os.environ["PROJECTS_ROOT"] = PROJECTS_ROOT  # share path with file_manager.py
        from file_manager import start_flask
        t = threading.Thread(target=start_flask, args=(PORT,), daemon=True)
        t.start()
        logger.info(f"Flask file manager started on port {PORT}")
    except ImportError:
        logger.warning("file_manager.py not found — file manager disabled")

    # HTML static files are served by the same Flask file manager (no extra thread needed)
    logger.info(f"HTML hosting enabled — URL pattern: {BASE_URL}/html/UID/PROJECT/")

    application = build_application()
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
