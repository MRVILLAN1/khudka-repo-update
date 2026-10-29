import os
import re
import time
import json
import shutil
import logging
import asyncio
import tempfile
from aiohttp import web
from pyrogram import Client, filters, idle, enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand
from pyrogram.errors import (
    FloodWait,
    ChannelPrivate,
    ChatAdminRequired,
    UserNotParticipant,
    PhoneNumberInvalid,
    PhoneNumberBanned,
    PhoneCodeInvalid,
    PhoneCodeExpired,
    SessionPasswordNeeded,
    PasswordHashInvalid,
    PeerIdInvalid,
)

# --- PYROGRAM PATCH: naye bade channel/group IDs ---
# Telegram ab 2^31 se bade channel/group IDs de raha hai (jaise 3329634673).
# Pyrogram 2.0.106 ke utils me purana MIN_CHANNEL_ID (-1002147483647) inhe
# "Peer id invalid" (ValueError) bolke reject kar deta hai. Isliye range fix:
import pyrogram.utils as _pyro_utils
_pyro_utils.MIN_CHANNEL_ID = -100999999999999

# Pyrogram ka Client module-level par event loop capture kar leta hai,
# isliye Client banane SE PEHLE ek current loop set karna zaroori hai.
# (uvloop yahan crash karta hai, isliye default asyncio loop hi use karo)
_loop = asyncio.new_event_loop()
asyncio.set_event_loop(_loop)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("VILLAN_BOT")

# Code version - startup logs me dikhega taaki pata chale kaunsa code deploy hai
VERSION = "v56"

# --- CONFIGURATION (Koyeb Environment Variables) ---
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "").strip()
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
SESSION_STRING = os.environ.get("SESSION_STRING", "").strip()
PORT = int(os.environ.get("PORT", 8000))

# (Sequential mode me istemal nahi hota; rakha hai taaki purana env var tootey nahi)
PARALLEL_TASKS = max(1, int(os.environ.get("PARALLEL_TASKS", "3")))
# Progress card kitne second me update ho
PROGRESS_EVERY = float(os.environ.get("PROGRESS_EVERY", "5"))
# /login command sirf is Telegram user ID ke liye (0 = sab ke liye khula).
# Apni ID @userinfobot se le sakte ho. Bot public hai to set karna behtar hai.
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
# Owner ka username (contact/buy ke liye dikhega)
OWNER_USERNAME = os.environ.get("OWNER_USERNAME", "@THE_OG_VILLAN").strip()
# Per-user sessions, allowlist aur paid subscriptions ki files
# DATA_DIR par rakho taaki Koyeb Volume lage to redeploy par na udein.
# (Volume: Koyeb dashboard -> Volumes -> Create, mount path /data,
#  phir env var DATA_DIR=/data set karo. Bina volume ke "." = ephemeral.)
DATA_DIR = os.environ.get("DATA_DIR", ".")
os.makedirs(DATA_DIR, exist_ok=True)
SESSIONS_FILE = os.environ.get("SESSIONS_FILE", os.path.join(DATA_DIR, "sessions.json"))
ALLOWED_FILE = os.environ.get("ALLOWED_FILE", os.path.join(DATA_DIR, "allowed_users.json"))
SUBS_FILE = os.environ.get("SUBS_FILE", os.path.join(DATA_DIR, "subscriptions.json"))
# Log group: har user ke naam ka folder (forum topic) yahi banega,
# aur nikali hui files yahan bhi jayengi (personal ke saath-saath)
LOG_GROUP_ID = int(os.environ.get("LOG_GROUP_ID", "-1004328020590") or 0) or None
# v35: Force-join updates channel (@username ya -100... ID). Khali = force join OFF.
UPDATES_CHANNEL = os.environ.get("UPDATES_CHANNEL", "").strip()
# v35: /start welcome photo ka URL ya Telegram file_id. Khali = bina photo ke welcome.
START_IMAGE = os.environ.get("START_IMAGE", "").strip()
TOPICS_FILE = os.environ.get("TOPICS_FILE", os.path.join(DATA_DIR, "user_topics.json"))
# Ek batch me max kitne posts (default 1000; Koyeb env me BATCH_LIMIT se badlo)
BATCH_LIMIT = max(1, int(os.environ.get("BATCH_LIMIT", "1000")))
# v40 SPEED: file delivery pacing. Fast path (file_id/copy, instant) ke baad
# chhota gap, slow path (download+upload) ke baad bada gap. Dono env se
# badal sakte hain. Bada gap = zyada safety, chhota = zyada speed.
FAST_GAP = float(os.environ.get("FAST_GAP", "2"))
SLOW_GAP = float(os.environ.get("SLOW_GAP", "3"))
# v50 PRIVATE GAP: sirf private channel batch me 10s countdown gap.
# Public (/public) par ye nahi lagta, wahan FAST/SLOW hi rehta hai.
PRIVATE_GAP = float(os.environ.get("PRIVATE_GAP", "10"))
# v40 SPEED: metadata ek hi get_messages call me itne messages ka aata hai
# (pehle har message ke liye alag call thi — 100 files par 100 requests).
# Max 200 (Telegram limit).
FETCH_CHUNK = max(1, min(200, int(os.environ.get("FETCH_CHUNK", "100"))))

# --- BACKUP: redeploy par data bachao (FREE tareeka, Koyeb Eco par bhi) ---
# Koyeb Volume sirf paid Standard instance par lagta hai. Agar tum free Eco
# par ho to ye karo: ek PRIVATE channel banao, bot ko usme admin banao,
# uska ID Koyeb env me BACKUP_CHAT_ID me daalo. Har JSON save ke baad bot
# uski copy channel me bhej dega; redeploy ke baad jo file local nahi
# milegi, wo channel se wapas aa jayegi. DATA_DIR/volume wala tareeka bhi
# saath me chalta rahega — dono me se jo mile, wahi use hoga.
# NOTE: sessions.json me users ke login session strings hote hain, isliye
# backup channel HAMESHA private rakho — sirf tum (owner) usme raho.
try:
    _raw_backup = os.environ.get("BACKUP_CHAT_ID", "").strip()
    BACKUP_CHAT_ID = int(_raw_backup) if _raw_backup else None
except ValueError:
    BACKUP_CHAT_ID = None
_BACKUP_KNOWN = ("sessions.json", "allowed_users.json", "subscriptions.json",
                  "user_settings.json", "user_topics.json")
_BACKUP_MSG_IDS = {}  # filename -> aakhri backup message id (delete-and-replace)


def _backup_caption(fname):
    return f"backup:{fname}"


async def _backup_upload(fname, path):
    """Local save ke baad backup channel me copy bhejo (best-effort)."""
    if not BACKUP_CHAT_ID:
        return
    try:
        if not os.path.exists(path):
            return
        msg = await app.send_document(BACKUP_CHAT_ID, path,
                                      caption=_backup_caption(fname))
        old = _BACKUP_MSG_IDS.get(fname)
        _BACKUP_MSG_IDS[fname] = msg.id
        if old and old != msg.id:
            try:
                await app.delete_messages(BACKUP_CHAT_ID, old)
            except Exception:
                pass
    except Exception as e:
        print(f"backup upload {fname} failed: {e}", flush=True)


async def _backup_restore_all():
    """Startup par: jo JSON store local nahi hai, use backup channel se lao.
    Bot MTProto se channel history NAHI padh sakta (messages.GetHistory par
    BOT_METHOD_INVALID aata hai — v36 ke live logs me confirm hua), isliye
    history scan OWNER ke userbot se hota hai. Upload bot se hi hota hai
    (channel me post karna bot ke liye allowed hai)."""
    if not BACKUP_CHAT_ID:
        return
    global _OWNER_UB_STARTED
    if user is None:
        print("backup restore SKIP: SESSION_STRING wala owner userbot nahi "
              "mila — bot akele history nahi padh sakta.", flush=True)
        return
    # Deploy overlap me purana instance kuch der zinda rehta hai — usi
    # beech userbot start karo to 406 AUTH_KEY_DUPLICATED aata hai.
    # Isliye 406 par ruko aur dobara try karo (purana instance marne do).
    for _att in range(4):
        try:
            if not _OWNER_UB_STARTED:
                await user.start()
                _OWNER_UB_STARTED = True
            break
        except Exception as e:
            es = str(e)
            if "already" in es.lower():
                _OWNER_UB_STARTED = True
                break
            if "AUTH_KEY_DUPLICATED" in es and _att < 3:
                print(f"backup restore: 406 duplicate (attempt {_att + 1}/4)"
                      " — 20s ruk ke dobara try...", flush=True)
                await asyncio.sleep(20)
                continue
            print(f"backup restore: userbot start fail: {e}", flush=True)
            return
    stores = {
        "sessions.json": SESSIONS_FILE,
        "allowed_users.json": ALLOWED_FILE,
        "subscriptions.json": SUBS_FILE,
        "user_settings.json": SETTINGS_FILE,
        "user_topics.json": TOPICS_FILE,
    }
    for fname, path in stores.items():
        try:
            if os.path.exists(path):
                continue
            cap = _backup_caption(fname)
            latest = None
            async for m in user.get_chat_history(BACKUP_CHAT_ID, limit=100):
                if m.document and (m.caption or "") == cap:
                    latest = m  # history newest-first: pehla match = latest
                    break
            if latest is None:
                continue
            await user.download_media(latest, file_name=path)
            _BACKUP_MSG_IDS[fname] = latest.id
            print(f"backup restore: {fname} OK", flush=True)
        except Exception as e:
            print(f"backup restore {fname} failed: {e}", flush=True)

# Pyrogram Clients Setup
# workers=16: 100 worker threads per client free plan ki RAM bhi khate hain
# (bot + owner userbot + per-user userbots sab milkar 100+ threads ban jate
# the). 16 updates handle karne ke liye kaafi hai.
# sleep_threshold=60: 60 sec tak ke FloodWait Pyrogram khud absorb karega
# (default 10 tha — bade wait par exception aata tha aur file fail ho jati thi).
app = Client(
    "bot_service",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=16,
    sleep_threshold=60,
)

user = None
if SESSION_STRING and len(SESSION_STRING) > 30:
    user = Client(
        "user_service",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=SESSION_STRING,
        workers=16,
        sleep_threshold=60,
    )

USER_STATES = {}
ACTIVE_BATCHES = {}  # user_id -> asyncio.Event (cancel flag)
LOGIN_STATES = {}  # user_id -> {"step", "phone", "phone_code_hash", "tmp"}
USER_CLIENTS = {}  # user_id -> running pyrogram Client (per-user userbot)
_OWNER_UB_STARTED = False


# --- JSON STORAGE + PERMISSION HELPERS ---
def _load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception as e:
        print(f"save {path} failed: {e}", flush=True)
        return
    # Backup channel set hai to copy wahan bhi bhejo (best-effort, non-blocking)
    fname = os.path.basename(path)
    if BACKUP_CHAT_ID and fname in _BACKUP_KNOWN:
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(_backup_upload(fname, path))
        except Exception:
            pass


def _load_allowed():
    data = _load_json(ALLOWED_FILE, [])
    return {int(x) for x in data if str(x).isdigit()}


def _save_allowed(allowed):
    _save_json(ALLOWED_FILE, sorted(allowed))


def _get_allowed():
    allowed = _load_allowed()
    for x in os.environ.get("ALLOWED_USERS", "").replace(" ", "").split(","):
        if x.isdigit():
            allowed.add(int(x))
    if OWNER_ID:
        allowed.add(OWNER_ID)
    return allowed


def _is_owner(uid: int) -> bool:
    return OWNER_ID != 0 and uid == OWNER_ID


def _load_subs():
    return _load_json(SUBS_FILE, {})


def _save_subs(subs):
    _save_json(SUBS_FILE, subs)


def _sub_remaining(uid: int) -> int:
    """Paid subscription me kitne seconds bache hain (0 = koi active plan nahi)."""
    sub = _load_subs().get(str(uid))
    if not sub:
        return 0
    return max(0, int(sub.get("expiry", 0) - time.time()))


def _is_allowed(uid: int) -> bool:
    if OWNER_ID == 0:
        return True  # open mode: sab allowed
    if uid in _get_allowed():
        return True  # free (lifetime) access
    return _sub_remaining(uid) > 0  # paid access (expiry tak)


async def _logout_user(uid):
    c = USER_CLIENTS.pop(uid, None)
    if c is not None:
        try:
            await c.disconnect()
        except Exception:
            pass
    # v44: is user ke cached media sessions bhi mar jayenge — saaf kar do
    # (warna stale sessions cache me padi rahengi)
    try:
        _evict_media_sessions(c)
    except Exception:
        pass
    sessions = _load_json(SESSIONS_FILE, {})
    if str(uid) in sessions:
        del sessions[str(uid)]
        _save_json(SESSIONS_FILE, sessions)


async def get_userbot(uid):
    """Is user ka userbot client lao (per-user session, owner fallback)."""
    global _OWNER_UB_STARTED
    if uid in USER_CLIENTS:
        return USER_CLIENTS[uid]
    sessions = _load_json(SESSIONS_FILE, {})
    sess = sessions.get(str(uid))
    if sess:
        c = Client(
            f"user_{uid}",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=sess,
            workers=16,
            sleep_threshold=60,
        )
        try:
            await asyncio.wait_for(c.start(), timeout=20.0)
        except Exception as e:
            print(f"userbot start failed for {uid}: {e}", flush=True)
            return None
        USER_CLIENTS[uid] = c
        return c
    # Owner fallback: sirf ASLI owner ke liye. Pehle OWNER_ID set na hone
    # par (open mode) HAR user owner ka global userbot use kar leta tha —
    # strangers apni batch owner ke account se chalate the, jisse us
    # account par download flood/restriction ka poora risk aa jata tha.
    if user is not None and OWNER_ID != 0 and uid == OWNER_ID:
        if not _OWNER_UB_STARTED:
            try:
                await asyncio.wait_for(user.start(), timeout=20.0)
                _OWNER_UB_STARTED = True
            except Exception as e:
                print(f"owner userbot start failed: {e}", flush=True)
                return None
        return user
    return None


# --- KOYEB HEALTH CHECK WEB SERVER ---
async def health_check(request):
    return web.Response(text="⚡ VILLAN Restricted Bot is Active!")


async def start_web_server():
    server = web.Application()
    server.router.add_get("/", health_check)
    runner = web.AppRunner(server)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"🌐 Health server running on port {PORT}", flush=True)


# --- FORMATTING UTILITIES ---
def humanbytes(size):
    if not size:
        return "0 B"
    power = 2**10
    n = 0
    units = ["B", "KB", "MB", "GB", "TB"]
    while size > power:
        size /= power
        n += 1
    return f"{size:.2f} {units[n]}"


def _bold_text(text):
    """Extracted caption/text ko BOLD me convert karo (HTML mode).
    HTML me sirf <, >, & escape karne padte hain — koi visible
    backslash nahi dikhega. Content same rahega, sirf bold dikhega."""
    if not text:
        return text
    # HTML special chars escape karo
    text = text.replace("&", "&amp;")
    text = text.replace("<", "&lt;")
    text = text.replace(">", "&gt;")
    return f"<b>{text}</b>"


def time_formatter(milliseconds: int) -> str:
    seconds, milliseconds = divmod(int(milliseconds), 1000)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds:
        parts.append(f"{seconds}s")
    return ", ".join(parts) if parts else "0s"


def get_progress_bar(percentage):
    completed = int(percentage // 10)
    remaining = 10 - completed
    return "▰" * completed + "▱" * remaining


async def safe_edit(msg, text):
    """Parallel tasks me edit conflict/FloodWait aaye to silently skip."""
    try:
        await msg.edit_text(text, parse_mode=enums.ParseMode.MARKDOWN)
    except FloodWait:
        pass
    except Exception:
        pass


# --- BATCH STATS (poore batch ka live hisaab) ---
class BatchStats:
    def __init__(self, total):
        self.total = total
        self.done = 0
        self.ok = 0
        self.fail = 0
        self.start = time.time()
        self.lock = asyncio.Lock()
        self._last_status_update = 0


async def _update_batch_status(status_msg, stats, force=False):
    """Pinned batch status message ko live update karo (har 3 sec me max 1 baar)."""
    now = time.time()
    async with stats.lock:
        if not force and (now - stats._last_status_update) < 3:
            return
        stats._last_status_update = now
        done, ok, fail, total = stats.done, stats.ok, stats.fail, stats.total
        elapsed = max(now - stats.start, 1)
    pct = (done * 100 / total) if total > 0 else 0
    filled = int(pct // 10)
    bar = ("🟩" * filled + "⬜" * (10 - filled))
    # ETA
    remaining = total - done
    per_file = elapsed / max(done, 1)
    eta_s = int(remaining * per_file)
    eta_str = time_formatter(eta_s * 1000) if done > 0 else "—"
    text = (
        "📦 **VILLAN BATCH**\n"
        f"{bar} `{pct:.0f}%`\n"
        "━━━━━━━━━━━━━━━\n"
        f"📁 Total: `{total}`\n"
        f"✅ Done: `{ok}`  ❌ Failed: `{fail}`\n"
        f"⏱️ ETA: `{eta_str}`\n"
        "⏹ Cancel: `/cancel`"
    )
    await safe_edit(status_msg, text)


# --- LIVE PROGRESS TRACKER (file + batch dashboard) ---
async def _premium_card(uid, bot_client=None):
    """⚜️ ᴘʀᴇᴍɪᴜᴍ ᴜꜱᴇʀ ᴅᴀᴛᴀ card — plan/premium check ke liye."""
    try:
        _u = await bot_client.get_users(uid) if bot_client else None
        _name = (_u.first_name if _u and _u.first_name else "User")
    except Exception:
        _name = "User"
    if _is_owner(uid):
        _tl, _ed, _et = "Lifetime 👑", "—", "—"
    elif uid in _get_allowed():
        _tl, _ed, _et = "Lifetime ✅", "—", "—"
    else:
        _rem = _sub_remaining(uid)
        if _rem > 0:
            _d, _h = int(_rem // 86400), int((_rem % 86400) // 3600)
            _m = int((_rem % 3600) // 60)
            _tl = f"{_d} ᴅᴀʏꜱ, {_h} ʜᴏᴜʀꜱ, {_m} ᴍɪɴᴜᴛᴇꜱ"
            _exp = time.time() + _rem
            _ed = time.strftime("%d-%m-%Y", time.localtime(_exp))
            _et = time.strftime("%I:%M:%S %p", time.localtime(_exp))
        else:
            _tl, _ed, _et = "Expired ❌", "—", "—"
    return "\n".join([
        "⚜️ ᴘʀᴇᴍɪᴜᴍ ᴜꜱᴇʀ ᴅᴀᴛᴀ :",
        "",
        f"👤 ᴜꜱᴇʀ : {_name}",
        f"⚡ ᴜꜱᴇʀ ɪᴅ : {uid}",
        f"⏰ ᴛɪᴍᴇ ʟᴇꜰᴛ : {_tl}",
        f"⌛️ ᴇxᴘɪʀʏ ᴅᴀᴛᴇ : {_ed}",
        f"⏱️ ᴇxᴘɪʀʏ ᴛɪᴍᴇ : {_et}",
    ])


async def progress_bar(
    current, total, message, start_time, last_update, action_title,
    stats=None, uid=None, bot_client=None
):
    """Premium style progress bar (screenshot wala design)."""
    now = time.time()
    if (now - last_update[0]) < PROGRESS_EVERY and current != total:
        return
    last_update[0] = now

    elapsed_time = max(now - start_time, 0.001)
    speed = current / elapsed_time
    percentage = (current * 100 / total) if total > 0 else 0

    # 🟥 Squares wala progress bar (10 blocks)
    filled = int(percentage // 10)
    sq_bar = ("🟥 " * filled + "⬜ " * (10 - filled)).strip()

    remaining_bytes = max(total - current, 0)
    time_left = round(remaining_bytes / speed) if speed > 0 else 0

    # Message ID nikalo (action_title me "ID `123`" format me hai)
    _mid = re.search(r"ID `(\d+)`", action_title or "")
    mid_str = _mid.group(1) if _mid else "—"

    # UPLOADING ya DOWNLOADING
    op = "UPLOADING" if "Upload" in (action_title or "") else "DOWNLOADING"

    # ━━━ SHORT PROGRESS BAR (sirf download/upload ke liye) ━━━
    # Batch line aur premium card YAHAN nahi — wo alag pinned status me hai
    lines = [
        op,
        "",
        sq_bar,
        "╭━━━━❰ᴘʀᴏɢʀᴇss ʙᴀʀ❱━➣",
        f"┣⪼ 🗂️ : {humanbytes(current)} | {humanbytes(total)}",
        f"┣⪼ ⏳️ : {percentage:.1f}%",
        f"┣⪼ 🚀 : {humanbytes(speed)}/s",
        f"┣⪼ ⏱️ : {time_formatter(time_left * 1000)}",
        f"┣⪼Message ID: {mid_str}",
        "╰━━━━━━━━━━━━━━━➣",
    ]
    await safe_edit(message, "\n".join(lines))


def parse_tg_link(link: str):
    regex = r"https://t\.me/(?:c/(\d+)|([a-zA-Z0-9_]+))/(?:(\d+)/)?(\d+)"
    match = re.search(regex, link)
    if not match:
        return None, None
    chat_id = int(f"-100{match.group(1)}") if match.group(1) else match.group(2)
    message_id = int(match.group(4))
    return chat_id, message_id


# --- PHASE 1: DOWNLOAD (parallel, tez) ---
async def _fetch_with_warmup(user_client, src_chat, msg_id):
    """get_messages karo. Peer resolve na ho ('Peer id invalid') to dialogs
    sync karke peer cache warm karo aur ek baar retry karo."""
    try:
        return await user_client.get_messages(src_chat, msg_id)
    except PeerIdInvalid:
        pass
    except ValueError as e:
        # Purana Pyrogram naye bade channel IDs par ValueError deta hai
        if "Peer id invalid" not in str(e):
            raise
    try:
        await user_client.get_dialogs(limit=500)
    except Exception as e:
        print(f"dialog warmup failed: {e}", flush=True)
    return await user_client.get_messages(src_chat, msg_id)


async def _fetch_chunk(user_client, src_chat, id_chunk):
    """v40 SPEED: ek hi get_messages call me poore chunk ka metadata lao.
    Pehle har message ke liye alag request thi (100 files = 100 requests,
    har ek par FloodWait ka risk + ~100-300ms latency). Ab 100 files = 1
    request. Returns: {msg_id: Message} | None (chunk fail — per-id
    fallback chalega) | 'BREAK' (channel access hi nahi hai)."""
    if not user_client:
        return None
    last_err = None
    for attempt in range(3):
        try:
            msgs = await user_client.get_messages(src_chat, list(id_chunk))
            out = {}
            for m in msgs:
                if m and not m.empty and m.id:
                    out[m.id] = m
            return out
        except FloodWait as e:
            wait = e.value + 1
            print(f"FloodWait on chunk fetch: waiting {wait}s "
                  f"(attempt {attempt+1}/3)", flush=True)
            await asyncio.sleep(wait)
            last_err = e
        except (TimeoutError, asyncio.TimeoutError, ConnectionError) as e:
            last_err = e
            print(f"Transient chunk fetch error (attempt {attempt+1}/3): {e}",
                  flush=True)
            await asyncio.sleep(2 * (attempt + 1))
        except (ChannelPrivate, ChatAdminRequired, UserNotParticipant):
            return "BREAK"
        except (PeerIdInvalid, ValueError) as e:
            if isinstance(e, ValueError) and "Peer id invalid" not in str(e):
                return None
            # Peer cache cold hai — dialogs sync karke warm karo, retry
            try:
                await user_client.get_dialogs(limit=500)
            except Exception as de:
                print(f"dialog warmup failed: {de}", flush=True)
            last_err = e
        except Exception as e:
            last_err = e
            print(f"Chunk fetch error (attempt {attempt+1}/3): {e}", flush=True)
            await asyncio.sleep(1)
    print(f"Chunk fetch failed after 3 attempts: {last_err}", flush=True)
    return None


async def fetch_one(user_client, src_chat, msg_id, preloaded=None):
    """Message ka metadata + file_id lao. Download MAT karo (fast path ke liye).
    preloaded: chunk-fetch se aaya Message object (network call skip hoti hai),
    ya 'MISSING' (chunk me confirm ho gaya ki ye id delete/invalid hai).
    Returns: info dict | None (skip) | 'BREAK' (fatal) | 'OFFLINE' | 'PEER_INVALID'."""
    if not user_client:
        return "OFFLINE"
    if preloaded == "MISSING":
        return None
    if preloaded is not None:
        target_msg = preloaded
    else:
        # Retry transient failures (max 3 attempts) — sirf tab jab chunk se
        # message nahi mila tha
        last_err = None
        for attempt in range(3):
            try:
                target_msg = await _fetch_with_warmup(user_client, src_chat, msg_id)
                if not target_msg or target_msg.empty:
                    return None
                break  # success, exit retry loop
            except FloodWait as e:
                wait = e.value + 1
                print(f"FloodWait on {msg_id}: waiting {wait}s (attempt {attempt+1}/3)", flush=True)
                await asyncio.sleep(wait)
                last_err = e
                continue
            except (TimeoutError, asyncio.TimeoutError, ConnectionError) as e:
                last_err = e
                print(f"Transient error on {msg_id} (attempt {attempt+1}/3): {e}", flush=True)
                await asyncio.sleep(2 * (attempt + 1))
                continue
            except PeerIdInvalid:
                return "PEER_INVALID"
            except ValueError as e:
                if "Peer id invalid" in str(e):
                    return "PEER_INVALID"
                raise
            except (ChannelPrivate, ChatAdminRequired, UserNotParticipant):
                return "BREAK"
            except Exception as e:
                last_err = e
                print(f"Fetch error on {msg_id} (attempt {attempt+1}/3): {e}", flush=True)
                await asyncio.sleep(1)
                continue
        else:
            # All 3 attempts failed
            print(f"Fetch failed after 3 attempts on {msg_id}: {last_err}", flush=True)
            return None
    return _info_from_msg(target_msg, msg_id)


def _info_from_msg(target_msg, msg_id):
    """Fetch ho chuke message se delivery info dict banao."""
    # Metadata nikalo (fetch already successful)
    if not target_msg.media:
        if target_msg.text:
            return {"kind": "text", "text": target_msg.text, "msg_id": msg_id,
                    "delivered_fast": True}
        return None

    kind = target_msg.media.value
    media_obj = getattr(target_msg, kind, None)
    file_id = getattr(media_obj, "file_id", None) if media_obj else None
    file_size = getattr(media_obj, "file_size", 0) or 0
    if file_size > 2097152000:
        return {"kind": "too_big", "msg_id": msg_id}

    fname = getattr(media_obj, "file_name", "") or ""
    if not fname:
        # Photo/voice/sticker jaisi media me file_name hota hi nahi.
        # Bina extension ke download hui photo ko upload karne par
        # Telegram PHOTO_EXT_INVALID deta hai (v35 ke live logs me
        # yahi photo-fail ki asli wajah nikli) — sahi extension lagao.
        _ext = {"photo": ".jpg", "voice": ".ogg", "video_note": ".mp4",
                "sticker": ".webp", "animation": ".mp4"}.get(kind, "")
        fname = f"media_{msg_id}{_ext}"
    info = {
        "kind": kind,
        "msg_id": msg_id,
        "file_id": file_id,
        "file_size": file_size,
        "file_name": os.path.basename(fname),
        "caption": target_msg.caption or "",
        "target_msg": target_msg,  # slow path download ke liye
        # v53: reference kab fetch hua — lambi throttled batch me chunk
        # fetch ke ghanton baad download ho to reference expire ho jata
        # hai (2026-10-28 05:45, msgs 405-407). Age dekh kar proactive
        # refetch karenge, fail hone ka wait nahi karenge.
        "fetched_at": time.time(),
        "tmpdir": None,
        "file_path": None,
        "thumb_path": None,
        "delivered_fast": False,  # run_batch pacing (FAST_GAP/SLOW_GAP) ke liye
    }
    if target_msg.video:
        info.update({
            "duration": target_msg.video.duration,
            "width": target_msg.video.width,
            "height": target_msg.video.height,
        })
    if target_msg.audio:
        info.update({
            "duration": target_msg.audio.duration,
            "performer": target_msg.audio.performer,
            "title": target_msg.audio.title,
        })
    if target_msg.voice:
        info.update({"duration": target_msg.voice.duration})
    return info


async def download_file_classic(user_client, info, prog_msg, stats=None, uid=None, bot_client=None):
    """Slow path: file ko disk par download karo (settings ya file_id fail par).
    prog_msg = alag progress message (pinned status_msg se alag)."""
    target_msg = info.get("target_msg")
    msg_id = info["msg_id"]
    if not target_msg or not user_client:
        return None
    tmpdir = tempfile.mkdtemp(prefix=f"vln_{msg_id}_")
    info["tmpdir"] = tmpdir
    # v51: download shuru hote hi dikh jaye (pehle sirf complete hone par
    # pata chalta tha, badi file par user ko lagta tha download start hi
    # nahi hua). Log me size ke saath, user ko prog_msg par.
    print(f"[DOWNLOAD] Starting msg {msg_id} ({info.get('file_size', 0)} bytes)", flush=True)
    if prog_msg is not None:
        try:
            await safe_edit(prog_msg, f"📥 Downloading • ID `{msg_id}` • {humanbytes(info.get('file_size', 0))}")
        except Exception:
            pass
    try:
        dl_path = os.path.join(tmpdir, info["file_name"])
        last_update = [time.time()]
        start_time = time.time()
        file_path = await user_client.download_media(
            target_msg,
            file_name=dl_path,
            progress=progress_bar,
            progress_args=(
                prog_msg, start_time, last_update,
                f"📥 Downloading • ID `{msg_id}`", stats, uid, bot_client,
            ),
        )
        if not file_path or not os.path.exists(file_path):
            return None
        # v52 0-BYTE GUARD: 2026-10-28 logs me download ke baad khaali
        # (0 byte) file upload par gayi aur "File size equals to 0 B" se
        # fail hui. Aisi file ko kabhi file_path mat banao — wapas
        # None do taaki wrapper retry/fail saaf tareeke se kare, aur
        # upload_one kabhi khaali file send karne ki koshish na kare.
        # Document/video/audio par exact size bhi check karo (photo par
        # sirf 0-byte check, uska reported size thumb se alag ho sakta hai).
        _got = os.path.getsize(file_path)
        _exp = info.get("file_size", 0) or 0
        if _got == 0 or (_exp > 0 and info.get("kind") in (
                "document", "video", "audio", "voice", "video_note",
                "animation") and _got != _exp):
            print(f"[SIZE] Download kharab msg {msg_id}: mila {_got} bytes, "
                  f"chahiye {_exp} bytes", flush=True)
            info["download_error"] = (
                f"size mismatch {_got} != {_exp}")
            try:
                os.remove(file_path)
            except Exception:
                pass
            return None
        if info.get("kind") == "photo" and not file_path.lower().endswith(
                (".jpg", ".jpeg", ".png", ".webp")):
            # Photo bina sahi extension ke upload nahi hoti (PHOTO_EXT_INVALID)
            try:
                os.rename(file_path, file_path + ".jpg")
                file_path = file_path + ".jpg"
            except Exception:
                pass
        info["file_path"] = file_path
        if target_msg.video and target_msg.video.thumbs:
            try:
                info["thumb_path"] = await user_client.download_media(
                    target_msg.video.thumbs[0].file_id,
                    file_name=os.path.join(tmpdir, "thumb.jpg"),
                )
            except Exception:
                info["thumb_path"] = None
        return file_path
    except Exception as e:
        print(f"Download error on {msg_id}: {e}", flush=True)
        info["download_error"] = str(e)  # v47: wrapper isko dekh kar refetch karega
        return None



# --- v41 MULTI-CONNECTION DOWNLOAD (v40 se tagra change) ---
# Rule intact: file EK BAAR ME EK HI download hoti hai (sequential batch
# order same hai) — lekin us EK file ke 1MB parts ek saath (MULTI_PARTS
# concurrent GetFile requests, ek hi media session par) aate hain.
# Single-connection download latency-bound hota hai (har 1MB part ke liye
# round-trip wait), isliye concurrent parts se throughput kai guna badh
# sakta hai. Koi bhi error -> classic download (fallback), to worst case
# v40 jitni speed, kabhi file fail nahi hoti is wajah se.
# v47 FLOOD: default 6 -> 3. 6 parts ek saath GetFile bhejte the, to
# Telegram us session par jaldi rate-limit lagata tha (15-16s Waiting
# waves). 3 parts se burst aadha, throttle kam trigger hota hai; peak
# speed thodi kam, par batch zyada steady chalta hai. Env se wapas
# 6 kar sakte ho (MULTI_PARTS=6), par flood wale session par 3 hi rakho.
MULTI_PARTS = max(1, min(8, int(os.environ.get("MULTI_PARTS", "3"))))
# v52: 10MB -> 1MB. 2026-10-28 logs me 5-9MB ke BIJZLI PDF (DC4) classic
# path par ja rahe the, aur classic download_media HAR file par DC4 ko
# naya auth-export karta hai -> ExportAuthorization FloodWait (121s...).
# Cached multi session me auth-export sirf pehli baar hota hai, isliye
# chhoti PDF bhi ab cached session se download hongi. MULTI_PARTS=1 live
# hai to parts sequential hi rahenge, order/speed rule same hai.
MULTI_MIN_SIZE = int(os.environ.get("MULTI_MIN_SIZE", str(1 * 1024 * 1024)))
MULTI_CHUNK = 1024 * 1024  # 1MB parts (Pyrogram ke get_file jaisa)

# v44 SPEED: media DC session CACHE. Pehle HAR file par naya media session
# + auth key exchange hota tha (~1-3 sec/file sirf setup me). Ab ek hi
# session ban kar reuse hoti hai — 100 files ke batch me 2-5 minute bachte
# hain sirf setup me. Session kharab ho jaye to evict karke agli file me
# naya ban jata hai (fallback classic waise hi hai).
_MEDIA_SESSION_CACHE = {}  # (id(client), dc_id) -> Session


async def _stop_session_quietly(sess):
    try:
        await sess.stop()
    except Exception:
        pass


def _evict_media_sessions(user_client, dc_id=None):
    """Cache se session(s) hatao (stale/broken hone par)."""
    for k in [k for k in list(_MEDIA_SESSION_CACHE)
              if k[0] == id(user_client) and (dc_id is None or k[1] == dc_id)]:
        sess = _MEDIA_SESSION_CACHE.pop(k)
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(_stop_session_quietly(sess))
        except Exception:
            pass


async def _get_media_session(user_client, dc_id):
    """Is client+DC ka (cached) media session lao — pehli baar banega."""
    from pyrogram import raw as _raw
    from pyrogram.session import Auth, Session
    key = (id(user_client), dc_id)
    cached = _MEDIA_SESSION_CACHE.get(key)
    if cached is not None:
        return cached
    main_dc = await user_client.storage.dc_id()
    test_mode = await user_client.storage.test_mode()
    if dc_id != main_dc:
        auth_key = await Auth(user_client, dc_id, test_mode).create()
    else:
        auth_key = await user_client.storage.auth_key()
    session = Session(user_client, dc_id, auth_key, test_mode, is_media=True)
    await session.start()
    if dc_id != main_dc:
        exported = await user_client.invoke(
            _raw.functions.auth.ExportAuthorization(dc_id=dc_id))
        await session.invoke(
            _raw.functions.auth.ImportAuthorization(
                id=exported.id, bytes=exported.bytes))
    _MEDIA_SESSION_CACHE[key] = session
    return session


async def _download_multi(user_client, info, prog_msg, stats, uid, bot_client):
    """Ek file ke parts concurrent fetch karo. Success par dl_path,
    kisi bhi failure par exception (caller classic fallback lega)."""
    from pyrogram import raw as _raw
    from pyrogram.file_id import FileId, FileType

    file_id_str = info.get("file_id")
    file_size = info.get("file_size", 0) or 0
    msg_id = info["msg_id"]
    if not file_id_str or file_size <= 0:
        raise ValueError("multi: no file_id/size")
    fid = FileId.decode(file_id_str)
    if fid.file_type == FileType.CHAT_PHOTO:
        raise ValueError("multi: chat photo not supported")
    if fid.file_type == FileType.PHOTO:
        location = _raw.types.InputPhotoFileLocation(
            id=fid.media_id, access_hash=fid.access_hash,
            file_reference=fid.file_reference, thumb_size=fid.thumbnail_size)
    else:
        location = _raw.types.InputDocumentFileLocation(
            id=fid.media_id, access_hash=fid.access_hash,
            file_reference=fid.file_reference, thumb_size=fid.thumbnail_size)

    dc_id = fid.dc_id
    # v44: cached session (per-file session start ka ~1-3s overhead gaya)
    session = await _get_media_session(user_client, dc_id)
    tmpdir = tempfile.mkdtemp(prefix=f"vln_{msg_id}_")
    info["tmpdir"] = tmpdir
    print(f"[DOWNLOAD] Starting msg {msg_id} ({file_size} bytes, multi)", flush=True)
    dl_path = os.path.join(tmpdir, info["file_name"])
    try:
        n_parts = (file_size + MULTI_CHUNK - 1) // MULTI_CHUNK
        downloaded = [0]
        start_time = time.time()
        last_update = [time.time()]
        label = f"📥 Downloading • ID `{msg_id}`"
        f = open(dl_path, "wb")
        try:
            for wave_start in range(0, n_parts, MULTI_PARTS):
                wave = list(range(
                    wave_start, min(wave_start + MULTI_PARTS, n_parts)))

                async def _fetch_part(p):
                    offset = p * MULTI_CHUNK
                    for attempt in range(3):
                        try:
                            r = await session.invoke(
                                _raw.functions.upload.GetFile(
                                    location=location, offset=offset,
                                    limit=MULTI_CHUNK),
                                sleep_threshold=60)
                            break
                        except FloodWait as e:
                            if attempt == 2:
                                raise
                            await asyncio.sleep(e.value + 1)
                    if isinstance(r, _raw.types.upload.File):
                        return p, r.bytes
                    raise RuntimeError(
                        "multi: unexpected response "
                        f"{type(r).__name__} (CDN redirect -> classic)")

                results = await asyncio.gather(
                    *[_fetch_part(p) for p in wave])
                for p, data in sorted(results):
                    f.seek(p * MULTI_CHUNK)
                    f.write(data)
                    downloaded[0] += len(data)
                if prog_msg is not None:
                    try:
                        await progress_bar(
                            min(downloaded[0], file_size), file_size,
                            prog_msg, start_time, last_update, label,
                            stats, uid, bot_client)
                    except Exception:
                        pass
        finally:
            f.close()
        if os.path.getsize(dl_path) != file_size:
            raise RuntimeError(
                f"multi: size mismatch {os.path.getsize(dl_path)} "
                f"!= {file_size}")
        return dl_path
    except BaseException as e:
        # FloodWait transient hai — session cache me hi rehne do. Baaki
        # failure par session stale ho sakta hai, evict kar do.
        if not isinstance(e, FloodWait):
            _evict_media_sessions(user_client, dc_id)
        try:
            shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception:
            pass
        raise


async def _refresh_info(user_client, info):
    """v47 FLOOD/EXPIRED: stale file reference ko fresh karo.
    Batch chunk me 100 messages ka metadata ek saath fetch hota hai; throttling
    ki wajah se kisi file ka download ghanton baad shuru ho to uska reference
    expire ho jata hai (2026-10-26 logs: 03:10 FILE_REFERENCE_EXPIRED x2).
    Is case me usi message ko dobara fetch karo — naye reference ke saath —
    aur info ke download fields update karo. Returns True/False."""
    old = info.get("target_msg")
    if not user_client or not old:
        return False
    chat = getattr(getattr(old, "chat", None), "id", None) or getattr(old, "chat", None)
    if not chat:
        return False
    try:
        fresh = await _fetch_with_warmup(user_client, chat, info["msg_id"])
    except Exception as e:
        print(f"[REFETCH] fail ({info['msg_id']}): {e}", flush=True)
        return False
    if not fresh or getattr(fresh, "empty", False):
        return False
    new = _info_from_msg(fresh, info["msg_id"])
    if not isinstance(new, dict) or new.get("kind") in ("text", "too_big"):
        return False
    for k in ("file_id", "file_size", "file_name", "caption", "target_msg",
              "fetched_at"):
        if k in new:
            info[k] = new[k]
    info["file_path"] = None
    info["tmpdir"] = None
    info["download_error"] = None
    print(f"[REFETCH] OK ({info['msg_id']}): fresh reference mil gaya",
          flush=True)
    return True


def _flood_seconds(err_text):
    """FloodWait error string se wait seconds nikalo (None = FloodWait nahi)."""
    if not err_text:
        return None
    if "FLOOD_WAIT" not in err_text and "FloodWait" not in err_text:
        return None
    m = re.search(r"wait of (\d+) seconds", err_text)
    if m:
        return int(m.group(1))
    m = re.search(r"FLOOD_WAIT_(\d+)", err_text)
    if m:
        return int(m.group(1))
    return 60


async def download_file(user_client, info, prog_msg, stats=None, uid=None, bot_client=None):
    """v41 wrapper: pehle multi-connection download try karo, fail par classic.
    v52: FloodWait par file ko turant fail mat karo — poora wait karke
    ek baar dobara try karo. 2026-10-28 logs me 121s ExportAuthorization
    FloodWait par file turant fail ho rahi thi (wrapper sirf
    FILE_REFERENCE_EXPIRED par retry karta tha).
    File order abhi bhi sequential hai — sirf ek file ke parts parallel aate hain.
    v47: agar download FILE_REFERENCE_EXPIRED se fail ho, to ek baar fresh
    reference lekar retry karo (purane reference par dobara try karna bekaar
    hai — multi aur classic dono wahi stale reference use karte the).
    v53: do naye cover. (1) PROACTIVE: reference 30 min se purana ho
    (chunk fetch ke baad lambi throttling) to download se PEHLE hi fresh
    fetch karo — fail hone ka wait mat karo. (2) SIZE-MISMATCH bhi stale
    hai: 2026-10-28 05:45 me expired error ke baad download ne khaali
    file ko success maan liya tha, isliye v47 ka expired-check kabhi chala
    hi nahi (koi [REFETCH] line nahi aayi). Ab size mismatch / 0-byte par
    bhi fresh fetch + retry hoga, sirf expired string par nahi."""
    msg_id = info.get("msg_id")
    # v53: reference age limit (env se badal sakte ho, default 30 min)
    _refresh_age = int(os.environ.get("REFRESH_AGE_SEC", "1800"))

    async def _attempt():
        if (MULTI_PARTS > 1 and user_client and info.get("target_msg")
                and info.get("file_id")
                and (info.get("file_size", 0) or 0) >= MULTI_MIN_SIZE):
            try:
                dl_path = await _download_multi(
                    user_client, info, prog_msg, stats, uid, bot_client)
                # classic jaisa tail: photo extension fix + video thumb
                if info.get("kind") == "photo" and not dl_path.lower().endswith(
                        (".jpg", ".jpeg", ".png", ".webp")):
                    try:
                        os.rename(dl_path, dl_path + ".jpg")
                        dl_path = dl_path + ".jpg"
                    except Exception:
                        pass
                info["file_path"] = dl_path
                target_msg = info.get("target_msg")
                if target_msg and target_msg.video and target_msg.video.thumbs:
                    try:
                        info["thumb_path"] = await user_client.download_media(
                            target_msg.video.thumbs[0].file_id,
                            file_name=os.path.join(info["tmpdir"], "thumb.jpg"))
                    except Exception:
                        info["thumb_path"] = None
                print(f"Multi download OK ({msg_id}): {info.get('file_size', 0)} "
                      f"bytes, {MULTI_PARTS} parts/file", flush=True)
                return dl_path
            except Exception as e:
                info["download_error"] = str(e)  # v47: expired check ke liye
                print(f"Multi download fail ({msg_id}), classic par ja raha: {e}",
                      flush=True)
        return await download_file_classic(
            user_client, info, prog_msg, stats, uid, bot_client)

    # v53 PROACTIVE REFRESH: chunk me fetch hua reference purana ho to
    # pehle hi fresh kar lo. Refetch fail ho to purane reference se hi
    # try karo (batch is wajah se fail nahi hoga).
    _fetched_at = info.get("fetched_at") or 0
    if user_client and info.get("target_msg") and _fetched_at:
        _age = time.time() - _fetched_at
        if _age > _refresh_age:
            print(f"[REFETCH] purana reference ({msg_id}, age {int(_age)}s), "
                  f"download se pehle fresh fetch", flush=True)
            await _refresh_info(user_client, info)

    result = await _attempt()
    if result:
        return result
    # v52: FloodWait par ek baar poora wait + retry. Pyrogram sirf 60s
    # tak ke wait khud absorb karta hai (sleep_threshold=60); usse bade
    # wait (jaise 121s ExportAuthorization) exception bankar aate hain
    # aur pehle file turant fail ho jati thi. Wait karke retry karne se
    # wo file bach jayegi — wait ke dauraan batch rukega, fail nahi hoga.
    _fw = _flood_seconds(info.get("download_error") or "")
    if _fw is not None:
        print(f"[RETRY] FloodWait {msg_id}: {_fw}s wait karke dobara try",
              flush=True)
        if prog_msg is not None:
            try:
                await safe_edit(
                    prog_msg,
                    f"⏳ Telegram limit — {_fw}s wait, phir dobara try • ID `{msg_id}`")
            except Exception:
                pass
        await asyncio.sleep(_fw + 5)
        info["download_error"] = None
        info["file_path"] = None
        result = await _attempt()
        if result:
            return result
    # v53: expired-reference case me ek baar fresh fetch + retry — AUR
    # size-mismatch case me bhi (v52 ka classic guard khaali/galat-size
    # file par download_error="size mismatch..." set karta hai, expired
    # string nahi; isliye purana check us case me kabhi trigger nahi
    # hota tha aur dobara download bhi wahi stale reference se hota tha).
    # Baaki failures (network, deleted msg) par dobara try nahi —
    # wo file normal fail count hogi, batch aage badhega.
    _err = info.get("download_error") or ""
    if ("FILE_REFERENCE_EXPIRED" not in _err
            and "size mismatch" not in _err):
        return result
    print(f"[REFETCH] stale reference ({msg_id}), fresh fetch karke retry "
          f"(error: {_err[:80]})", flush=True)
    if not await _refresh_info(user_client, info):
        return result
    return await _attempt()


def _needs_file(s, info):
    """Kya is file ko disk par download karna zaroori hai (settings ke liye)?"""
    if s["rename_tag"] or s["remove_words"] or s["replace_words"]:
        return True
    if s["thumb_file_id"]:
        return True
    kind = info["kind"]
    if kind == "video" and (s["video_wm"] or s["upload_as_doc"]):
        return True
    if kind == "document" and s["pdf_wm"]:
        if info.get("file_name", "").lower().endswith(".pdf"):
            return True
    return False


async def _send_by_file_id(client, dest, kind, file_id, caption, info, topic_id=None):
    """file_id se bhejo — INSTANT, server-side (no download/upload)."""
    kw = {"reply_to_message_id": topic_id} if topic_id else {}
    # Caption _bold_text() se HTML (<b>...</b>) me aata hai — bina parse_mode
    # ke Telegram tags ko literal text dikhayega, isliye HTML mode zaroori hai.
    cap_kw = {"parse_mode": enums.ParseMode.HTML} if caption else {}
    if kind == "video":
        return await client.send_video(
            dest, video=file_id, caption=caption,
            duration=info.get("duration"),
            width=info.get("width"), height=info.get("height"),
            **cap_kw, **kw)
    elif kind == "audio":
        return await client.send_audio(
            dest, audio=file_id, caption=caption,
            duration=info.get("duration"),
            performer=info.get("performer"), title=info.get("title"),
            **cap_kw, **kw)
    elif kind == "photo":
        return await client.send_photo(
            dest, photo=file_id, caption=caption, **cap_kw, **kw)
    elif kind == "voice":
        return await client.send_voice(
            dest, voice=file_id, caption=caption,
            duration=info.get("duration"), **cap_kw, **kw)
    elif kind == "video_note":
        return await client.send_video_note(dest, video_note=file_id, **kw)
    else:
        return await client.send_document(
            dest, document=file_id, caption=caption, **cap_kw, **kw)


def _sent_file_id(sent_msg, kind):
    """Bot ne jo message bheja, uske media ka file_id nikalo.
    Ye file_id BOT ka apna hai — isko dobara use kar sakte hain
    (userbot wala file_id bot use NAHI kar sakta -> MEDIA_EMPTY)."""
    if not sent_msg:
        return None
    attr = {"video": "video", "audio": "audio", "photo": "photo",
            "voice": "voice", "video_note": "video_note"}.get(kind, "document")
    media = getattr(sent_msg, attr, None)
    return getattr(media, "file_id", None) if media else None


# --- GROUP TOPIC (har user ke naam ka folder) ---
# --- GROUP TOPIC: Bot API se bhejo (message_thread_id) ---
# MTProto raw API me top_msg_id kaam nahi kar raha, isliye Bot API ka
# message_thread_id use karenge — ye forum topics ke liye GUARANTEED kaam karta hai.
import aiohttp as _aiohttp

# v40 SPEED: ek hi aiohttp session REUSE karo. Pehle har Bot API call par
# naya ClientSession (naya TLS handshake) banta tha — 1000 files ke batch me
# har file par ~200-500ms sirf handshake me waste hote the.
_HTTP_SESSION = None


async def _http():
    global _HTTP_SESSION
    if _HTTP_SESSION is None or _HTTP_SESSION.closed:
        _HTTP_SESSION = _aiohttp.ClientSession()
    return _HTTP_SESSION

async def _botapi_send_to_topic(kind, file_id=None, file_path=None,
                                caption="", topic_id=None, info=None):
    """Bot API HTTP se topic me bhejo. file_id ho to instant, warna upload.
    Returns True/False."""
    if not BOT_TOKEN or not LOG_GROUP_ID or not topic_id:
        return False
    info = info or {}
    try:
        method_map = {
            "video": "sendVideo", "audio": "sendAudio", "photo": "sendPhoto",
            "voice": "sendVoice", "video_note": "sendVideoNote",
            "document": "sendDocument", "text": "sendMessage",
        }
        method = method_map.get(kind, "sendDocument")
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
        data = {
            "chat_id": str(LOG_GROUP_ID),
            "message_thread_id": str(topic_id),
            "caption": caption or "",
        }
        # Caption _bold_text() se HTML (<b>) me hai — bina parse_mode ke
        # Bot API tags ko literal dikhayega, isliye HTML mode do.
        if caption:
            data["parse_mode"] = "HTML"
        files = None
        if kind == "text":
            data = {
                "chat_id": str(LOG_GROUP_ID),
                "message_thread_id": str(topic_id),
                "text": caption or "",
            }
            if caption:
                data["parse_mode"] = "HTML"
        elif file_id:
            # file_id se instant bhejo
            param = {"video": "video", "audio": "audio", "photo": "photo",
                     "voice": "voice", "video_note": "video_note"}.get(kind, "document")
            data[param] = file_id
            if kind == "video":
                if info.get("duration"):
                    data["duration"] = str(info["duration"])
                if info.get("width"):
                    data["width"] = str(info["width"])
                if info.get("height"):
                    data["height"] = str(info["height"])
            elif kind in ("audio", "voice"):
                if info.get("duration"):
                    data["duration"] = str(info["duration"])
        elif file_path and os.path.exists(file_path):
            # file upload karo (50MB tak Bot API limit)
            size = os.path.getsize(file_path)
            if size > 50 * 1024 * 1024:
                print(f"Bot API: file too big ({size}), skipping", flush=True)
                return False
            param = {"video": "video", "audio": "audio", "photo": "photo",
                     "voice": "voice", "video_note": "video_note"}.get(kind, "document")
            # v40 FIX: pehle file object add_field ke baad close() ho rahi
            # thi — aiohttp post ke waqt band file padhne ki koshish karta
            # tha. Ab pehle bytes padh lo, phir form me daalo.
            with open(file_path, "rb") as f:
                files = {param: f.read()}
        else:
            return False
        sess = await _http()
        if files:
            form = _aiohttp.FormData()
            for k, v in data.items():
                form.add_field(k, v)
            for k, payload in files.items():
                form.add_field(k, payload, filename=os.path.basename(file_path))
            async with sess.post(url, data=form, timeout=300) as resp:
                result = await resp.json()
        else:
            async with sess.post(url, data=data, timeout=60) as resp:
                result = await resp.json()
        if result.get("ok"):
            return True
        print(f"Bot API failed: {result}", flush=True)
        return False
    except Exception as e:
        print(f"Bot API send failed: {e}", flush=True)
        return False


# --- GROUP TOPIC: raw API se bhejo (top_msg_id) [FALLBACK] ---
# Pyrogram 2.0.106 ke high-level send_* me top_msg_id nahi hai, isliye
# forum topic me bhejne ke liye raw messages.SendMedia use karna padta hai.
async def _raw_send_to_topic(client, peer, topic_id, kind, file_id=None,
                             file_path=None, caption="", info=None, thumb=None):
    """Topic me media bhejo (raw API, top_msg_id se).
    file_id diya to server-side instant, file_path diya to upload karke.
    Returns True/False."""
    from pyrogram import raw
    from pyrogram.file_id import FileId, FileType
    import random as _rand
    import mimetypes
    info = info or {}
    try:
        if file_id:
            # file_id se InputMedia banao
            fid = FileId.decode(file_id)
            if fid.file_type in (FileType.PHOTO, FileType.PROFILE_PHOTO):
                media = raw.types.InputMediaPhoto(
                    id=raw.types.InputPhoto(
                        id=fid.media_id,
                        access_hash=fid.access_hash,
                        file_reference=fid.file_reference or b""))
            else:
                media = raw.types.InputMediaDocument(
                    id=raw.types.InputDocument(
                        id=fid.media_id,
                        access_hash=fid.access_hash,
                        file_reference=fid.file_reference or b""))
        elif file_path and os.path.exists(file_path):
            # file upload karo
            uploaded = await client.save_file(file_path)
            mime, _ = mimetypes.guess_type(file_path)
            mime = mime or "application/octet-stream"
            fname = os.path.basename(file_path)
            attrs = [raw.types.DocumentAttributeFilename(file_name=fname)]
            if kind == "video":
                attrs.append(raw.types.DocumentAttributeVideo(
                    duration=info.get("duration") or 0,
                    w=info.get("width") or 0,
                    h=info.get("height") or 0,
                    supports_streaming=True))
            elif kind == "audio":
                attrs.append(raw.types.DocumentAttributeAudio(
                    duration=info.get("duration") or 0,
                    performer=info.get("performer") or "",
                    title=info.get("title") or ""))
            elif kind == "voice":
                attrs.append(raw.types.DocumentAttributeAudio(
                    duration=info.get("duration") or 0, voice=True))
            elif kind == "video_note":
                attrs.append(raw.types.DocumentAttributeVideo(
                    duration=info.get("duration") or 0,
                    w=0, h=0, round_message=True))
            media = raw.types.InputMediaUploadedDocument(
                file=uploaded, mime_type=mime, attributes=attrs)
            if thumb and isinstance(thumb, str) and os.path.exists(thumb):
                try:
                    media.thumb = await client.save_file(thumb)
                except Exception:
                    pass
        else:
            return False
        # Caption _bold_text() se HTML (<b>) me hai — raw API ko tags
        # hatake bold entity deni padti hai, warna tags literal dikhenge.
        cap_text = caption or ""
        cap_entities = None
        if (cap_text.startswith("<b>") and cap_text.endswith("</b>")
                and len(cap_text) > 7):
            import html as _html
            cap_text = _html.unescape(cap_text[3:-4])
            # v46 FIX: Telegram entity length UTF-16 code units me ginta hai,
            # Python len() emoji text me kam ginata tha -> ENTITY_BOUNDS_INVALID
            _cu16 = len(cap_text.encode("utf-16-le")) // 2
            cap_entities = [raw.types.MessageEntityBold(
                offset=0, length=_cu16)]
        await client.invoke(raw.functions.messages.SendMedia(
            peer=peer,
            media=media,
            message=cap_text,
            entities=cap_entities,
            random_id=_rand.randint(1, 2**63 - 1),
            top_msg_id=topic_id,
        ))
        return True
    except Exception as e:
        print(f"Raw topic send failed: {e}", flush=True)
        return False


async def _find_topic_id(client, channel, title):
    """Group me title se topic dhoondo. Returns topic ID ya None.
    Partial match bhi karta hai (uid search ke liye).
    Do pass: pehle server-side q search, phir bina q ke poori list
    scan karke title me uid match (q kabhi-kabhi miss kar deta hai)."""
    from pyrogram import raw
    for query in (title, ""):
        try:
            res = await client.invoke(raw.functions.channels.GetForumTopics(
                channel=channel, offset_date=0, offset_id=0,
                offset_topic=0, limit=100, q=query))
            for t in res.topics:
                # Exact match ya uid/title ka partial match
                if t.title == title or title in t.title:
                    return t.id
        except Exception as e:
            print(f"Find topic failed: {e}", flush=True)
            return None
    return None


async def _botapi_create_topic(title):
    """Bot API se forum topic banao. Returns topic_id ya None.
    (Bots MTProto se nahi, lekin Bot API se topic bana SAKTE hain!)"""
    if not BOT_TOKEN or not LOG_GROUP_ID:
        return None
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/createForumTopic"
        data = {"chat_id": str(LOG_GROUP_ID), "name": title}
        sess = await _http()
        async with sess.post(url, data=data, timeout=30) as resp:
            result = await resp.json()
        if result.get("ok"):
            tid = result["result"].get("message_thread_id")
            print(f"[TOPIC] Bot API se topic banaya: {tid}", flush=True)
            return tid
        print(f"[TOPIC] createForumTopic failed: {result}", flush=True)
    except Exception as e:
        print(f"[TOPIC] createForumTopic exception: {e}", flush=True)
    return None


async def get_or_create_topic(userbot, uid):
    """LOG_GROUP me user ke naam ka forum topic (folder). Ek baar banega,
    phir wahi reuse hoga.
    NOTE: userbot use hota hai kyunki BOTS channels.GetForumTopics/
    CreateForumTopic use NAHI kar sakte (BOT_METHOD_INVALID)!
    Returns: topic ID. Fail par None."""
    print(f"[TOPIC] Called for uid={uid}, LOG_GROUP_ID={LOG_GROUP_ID}", flush=True)
    if not LOG_GROUP_ID:
        return None
    # STEP 1: Pehle stored topic ID check karo (fast path)
    topics = _load_json(TOPICS_FILE, {})
    stored = topics.get(str(uid))
    if stored:
        print(f"[TOPIC] Stored topic_id mil gaya: {stored}", flush=True)
        return stored
    # STEP 2: Stored nahi hai (redeploy par wipe ho gaya?) —
    # to group me EXISTING topic search karo (uid se), naya mat banao!
    # Title format: "{Name} • {uid}" — uid se search karo.
    # PEHLE owner ke apne userbot (global `user`) se search karo — wahi
    # account log group ka member/admin hai. Per-user userbot group me
    # hota hi nahi, isliye usse search hamesha fail hota tha aur har
    # redeploy ke baad NAYA folder ban jata tha (duplicate folders bug)!
    search_clients = []
    if user is not None:
        search_clients.append(user)
    if userbot is not None and userbot is not user:
        search_clients.append(userbot)
    for sc in search_clients:
        try:
            peer = await sc.resolve_peer(LOG_GROUP_ID)
            found_id = await _find_topic_id(sc, peer, str(uid))
            if found_id:
                print(f"[TOPIC] ✅ EXISTING topic mil gaya: {found_id} for uid={uid}", flush=True)
                topics[str(uid)] = found_id
                _save_json(TOPICS_FILE, topics)
                return found_id
        except Exception as e:
            print(f"[TOPIC] Search failed: {e}", flush=True)
    if search_clients:
        print(f"[TOPIC] Existing topic nahi mila, naya banayenge", flush=True)
    # STEP 3: Naya topic Bot API se banao (bot MTProto se nahi bana sakta!)
    # Title ke liye user ka naam lao (bot client se, userbot se nahi)
    title = f"User • {uid}"
    try:
        # Bot client global 'app' se user info lao
        u = await app.get_users(uid)
        title = f"{(u.first_name or 'User')[:25]} • {uid}"
    except Exception:
        pass
    print(f"[TOPIC] Naya topic bana raha hun: '{title}'", flush=True)
    topic_id = await _botapi_create_topic(title)
    if topic_id:
        topics[str(uid)] = topic_id
        _save_json(TOPICS_FILE, topics)
        print(f"[TOPIC] ✅ SUCCESS: topic={topic_id} for uid={uid}", flush=True)
        return topic_id
    print(f"[TOPIC] ❌ FAILED: topic nahi ban saka", flush=True)
    return None


# --- PHASE 2: UPLOAD (hamesha ORDER me, ek-ek karke) ---
async def _fast_group_copy(client, group_id, topic_id, kind, file_id, cap, info):
    """Fast path ke baad log-group topic me copy (bot ka apna file_id —
    instant, server-side). Fail ho to bas log karo, delivery rukti nahi."""
    if not group_id:
        return
    if topic_id:
        ok = await _botapi_send_to_topic(
            kind, file_id=file_id, caption=cap, topic_id=topic_id, info=info)
        if not ok:
            try:
                peer = await client.resolve_peer(group_id)
                await _raw_send_to_topic(
                    client, peer, topic_id, kind,
                    file_id=file_id, caption=cap, info=info)
            except Exception as e:
                print(f"Group file_id send failed: {e}", flush=True)
    else:
        # topic nahi bana to General me bhejo (fallback)
        try:
            await _send_by_file_id(client, group_id, kind, file_id, cap, info)
        except Exception as e:
            print(f"Group file_id send failed: {e}", flush=True)


async def _direct_fast(client, dest_chat, group_id, topic_id, kind, file_id, cap, info):
    """Bot ka apna file_id direct bhejo (public source jab bot khud fetch
    karta hai). Returns True/False."""
    try:
        await _send_by_file_id(client, dest_chat, kind, file_id, cap, info)
    except Exception as e:
        print(f"Direct fast path failed ({info['msg_id']}): {e}", flush=True)
        return False
    await _fast_group_copy(client, group_id, topic_id, kind, file_id, cap, info)
    print(f"⚡ INSTANT (direct) msg {info['msg_id']}", flush=True)
    return True


async def upload_one(client, userbot, uid, dest_chat, topic_id, status_msg, info, stats=None, prog_msg=None, fast_only=False):
    """Ek file deliver karo: personal (dest_chat) + group topic (LOG_GROUP_ID).
    FAST PATH: file_id se instant bhejo (no download/upload).
    SLOW PATH: download karo, settings lagao, phir upload karo.
    status_msg = pinned batch status (sirf _update_batch_status).
    prog_msg = alag per-file progress message (sirf progress_bar)."""
    kind = info["kind"]
    s = get_settings(uid)
    group_id = LOG_GROUP_ID
    # v43 caption helper: (1) original caption par remove/replace rules,
    # (2) caption template REPLACE nahi — original ke NICHE append hoga,
    # (3) phir bold. Slow path me rename ke baad naye naam se dobara
    # banane ke liye bhi yahi use hota hai.
    _orig_cap = info.get("caption") or ""

    def _make_cap(fname):
        c = apply_text_rules(_orig_cap, s)
        if c != _orig_cap:
            print(f"[CAPTION] rules lage msg {info.get('msg_id')}", flush=True)
        if s["caption"]:
            try:
                tpl = s["caption"].format(filename=fname)
            except Exception:
                tpl = s["caption"]
            c = f"{c}\n{tpl}" if c else tpl
        return _bold_text(c) if c else ""

    def _clean():
        if info.get("tmpdir"):
            shutil.rmtree(info["tmpdir"], ignore_errors=True)

    async def _to_group_text(text):
        if not group_id:
            return
        try:
            if topic_id:
                peer = await client.resolve_peer(group_id)
                from pyrogram import raw
                import random as _rand
                # Bold entity lagao (text me <b> tags hain, unko hatake entity banao)
                entities = None
                plain = text
                if text.startswith("<b>") and text.endswith("</b>") and len(text) > 7:
                    plain = text[3:-4]
                    # HTML entities decode karo (length sahi ho)
                    import html as _html
                    plain_decoded = _html.unescape(plain)
                    # v46 FIX: entity length UTF-16 code units me
                    _u16 = len(plain_decoded.encode("utf-16-le")) // 2
                    entities = [raw.types.MessageEntityBold(
                        offset=0, length=_u16)]
                    plain = plain_decoded
                try:
                    await client.invoke(raw.functions.messages.SendMessage(
                        peer=peer, message=plain,
                        random_id=_rand.randint(1, 2**63 - 1),
                        top_msg_id=topic_id, entities=entities))
                except Exception:
                    # v46 FIX: entity send fail ho to plain text dobara bhejo,
                    # taaki group copy ruk na jaye
                    await client.invoke(raw.functions.messages.SendMessage(
                        peer=peer, message=plain,
                        random_id=_rand.randint(1, 2**63 - 1),
                        top_msg_id=topic_id, entities=None))
            else:
                await client.send_message(
                    group_id, text, parse_mode=enums.ParseMode.HTML)
        except Exception as e:
            print(f"Group text send failed: {e}", flush=True)

    try:
        # caption: rules + append template (v43), bold ke saath
        cap = _make_cap(info.get("file_name", "file"))

        if kind == "text":
            bold_text = _bold_text(apply_text_rules(info["text"], s))
            await client.send_message(
                dest_chat, bold_text, parse_mode=enums.ParseMode.HTML)
            await _to_group_text(bold_text)
            info["delivered_fast"] = True
            return True
        if kind == "too_big":
            await client.send_message(
                dest_chat,
                f"⚠️ **Message {info['msg_id']}:** File 2GB se badi hai.",
                parse_mode=enums.ParseMode.MARKDOWN,
            )
            return False

        # === FAST PATH: file_id se INSTANT bhejo (sirf BOT ka apna file_id) ===
        # v44: transfer/instant-share path HATA DIYA (user demand). Ab instant
        # sirf tab jab bot khud fetch karta ho (public source) — userbot wale
        # file_id par bot ke send karne ki koshish hi bekar thi (MEDIA_EMPTY),
        # isliye private source par seedha download+upload chalta hai.
        file_id = info.get("file_id")
        # v49 /public: file_id fast path tab bhi jab settings (remove/
        # rename/watermark) lagi hon — caption ke rules upar _make_cap me
        # lag chuke hain, isliye caption saaf milega; sirf filename
        # rename/watermark/thumbnail skip honge (unhe download chahiye).
        if (file_id and (fast_only or not _needs_file(s, info))
                and (userbot is None or userbot is client)):
            ok = await _direct_fast(
                client, dest_chat, group_id, topic_id,
                kind, file_id, cap, info)
            if ok:
                info["delivered_fast"] = True
                return True
            if fast_only:
                print(f"[PUBLIC] fast fail ({info['msg_id']}) — slow fallback",
                      flush=True)
            print(f"Fast path failed ({info['msg_id']}) — downloading...",
                  flush=True)

        # === SLOW PATH: download + settings + upload ===
        # v44 PIPELINE: run_batch agli file pehle se background me download
        # kar chuka hota hai — info["file_path"] mila to wahi use karo.
        fp = info.get("file_path")
        # v52: prefetch file ko bina size check ke "mil gayi" mat maano.
        # 2026-10-28 me khaali prefetch file seedha upload par gayi aur
        # "File size equals to 0 B" se fail hui. Kharab file ko yahin
        # discard karo taaki neeche dobara download ho.
        if fp and os.path.exists(fp):
            _sz = os.path.getsize(fp)
            _exp = info.get("file_size", 0) or 0
            if _sz == 0 or (_exp > 0 and info.get("kind") in (
                    "document", "video", "audio", "voice", "video_note",
                    "animation") and _sz != _exp):
                print(f"[SIZE] Prefetch file kharab msg {info['msg_id']}: "
                      f"{_sz} bytes (chahiye {_exp}), dobara download",
                      flush=True)
                info["file_path"] = None
                fp = None
        if fp and os.path.exists(fp):
            print(f"📦 Prefetch se file mil gayi msg {info['msg_id']} "
                  "(download pehle se ho chuki)", flush=True)
        else:
            print(f"⬇️ DOWNLOAD chal raha hai msg {info['msg_id']}", flush=True)
            fp = await download_file(userbot, info, prog_msg, stats, uid, client)
        if not fp:
            _clean()
            return False

        # settings: filename (remove/replace/tag)
        # v43: rename ke baad info["file_name"] bhi update — warna aage
        # purana naam use hota tha aur rename ka asar gayab lagta tha.
        base = os.path.basename(fp)
        new_base = apply_filename_rules(base, s)
        if new_base != base:
            new_fp = os.path.join(os.path.dirname(fp), new_base)
            try:
                os.rename(fp, new_fp)
                fp = new_fp
                info["file_path"] = fp
                info["file_name"] = new_base
                print(f"[NAME] {base} -> {new_base}", flush=True)
            except Exception:
                pass
        # caption dobara banao: naye filename ke saath (append template)
        cap = _make_cap(new_base)

        # settings: custom thumbnail
        th = info.get("thumb_path")
        if s["thumb_file_id"]:
            th = s["thumb_file_id"]

        # settings: watermarks
        if kind == "document" and new_base.lower().endswith(".pdf") and s["pdf_wm"]:
            fp = apply_pdf_watermark(fp, s["pdf_wm"], info["tmpdir"])
            info["file_path"] = fp
        if kind == "video" and s["video_wm"]:
            fp = apply_video_watermark(fp, s["video_wm"], info["tmpdir"])
            info["file_path"] = fp

        # settings: upload method
        send_kind = kind
        if kind == "video" and s["upload_as_doc"]:
            send_kind = "document"

        async def _do_send(dchat, dtopic, show_progress):
            kw = {"reply_to_message_id": dtopic} if dtopic else {}
            # Caption bold hai to parse_mode chahiye
            cap_kw = {"parse_mode": enums.ParseMode.HTML} if cap else {}
            if show_progress:
                last_update = [time.time()]
                start_time = time.time()
                pa = (prog_msg, start_time, last_update)
                title = f"🚀 Uploading • ID `{info.get('msg_id')}`"
                prog = {"progress": progress_bar,
                        "progress_args": pa + (title, stats, uid, client)}
            else:
                prog = {}
            if send_kind == "video":
                return await client.send_video(
                    dchat, video=fp, caption=cap, thumb=th,
                    duration=info.get("duration"),
                    width=info.get("width"), height=info.get("height"),
                    **prog, **kw, **cap_kw)
            elif send_kind == "document":
                return await client.send_document(
                    dchat, document=fp, caption=cap, thumb=th,
                    **prog, **kw, **cap_kw)
            elif send_kind == "audio":
                return await client.send_audio(
                    dchat, audio=fp, caption=cap,
                    duration=info.get("duration"),
                    performer=info.get("performer"), title=info.get("title"),
                    **prog, **kw, **cap_kw)
            elif send_kind == "photo":
                try:
                    return await client.send_photo(
                        dchat, photo=fp, caption=cap, **kw, **cap_kw)
                except Exception as e:
                    if "PHOTO_EXT_INVALID" in str(e):
                        # Photo ke roop me reject ho jaye to file/document
                        # bana kar bhej do — image user ko mil hi jayegi
                        return await client.send_document(
                            dchat, document=fp, caption=cap, **kw, **cap_kw)
                    raise
            elif send_kind == "voice":
                return await client.send_voice(
                    dchat, voice=fp, caption=cap,
                    duration=info.get("duration"), **kw, **cap_kw)
            elif send_kind == "video_note":
                return await client.send_video_note(dchat, video_note=fp, **kw)
            else:
                return await client.send_document(
                    dchat, document=fp, caption=cap, **prog, **kw)

        # personal delivery (progress ke saath)
        try:
            sent_msg = await _do_send(dest_chat, None, True)
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
            _clean()
            return False
        except Exception as e:
            print(f"Personal upload failed ({info.get('msg_id')}): {e}", flush=True)
            _clean()
            return False

        # group topic delivery (personal ke baad)
        # v24: personal upload se mila BOT ka file_id use karo -> group copy INSTANT
        # (userbot wala file_id bot ke kaam ka nahi -> MEDIA_EMPTY, isliye dobara upload nahi)
        if group_id and topic_id:
            ok = False
            bot_fid = _sent_file_id(sent_msg, send_kind)
            if bot_fid:
                ok = await _botapi_send_to_topic(
                    send_kind, file_id=bot_fid, caption=cap,
                    topic_id=topic_id, info=info)
                if ok:
                    print(f"Group copy instant (file_id) for {info.get('msg_id')}", flush=True)
            if not ok:
                ok = await _botapi_send_to_topic(
                    send_kind, file_path=fp, caption=cap,
                    topic_id=topic_id, info=info)
            if not ok:
                try:
                    peer = await client.resolve_peer(group_id)
                    ok = await _raw_send_to_topic(
                        client, peer, topic_id, send_kind,
                        file_path=fp, caption=cap, info=info, thumb=th)
                    if not ok:
                        print(f"Group topic upload failed, trying fallback", flush=True)
                        await _do_send(group_id, None, False)
                except Exception as e:
                    print(f"Group upload failed: {e}", flush=True)
        elif group_id:
            try:
                await _do_send(group_id, None, False)
            except Exception as e:
                print(f"Group upload failed: {e}", flush=True)

        _clean()
        return True

    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
        return False
    except Exception as e:
        print(f"Upload error on {info.get('msg_id')}: {e}", flush=True)
        return False
    finally:
        if info.get("tmpdir"):
            shutil.rmtree(info["tmpdir"], ignore_errors=True)


async def _pin_status(client, chat_id, msg_id):
    """Batch status message ko pin karo. Bot private chats me Pyrogram se
    pin NAHI kar sakta (BOT_ONESIDE_NOT_AVAIL — har file par error log hota
    tha), isliye v40 me PEHLE Bot API try karte hain, Pyrogram sirf
    fallback hai. Returns True/False."""
    if BOT_TOKEN:
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/pinChatMessage"
            data = {"chat_id": str(chat_id), "message_id": str(msg_id),
                    "disable_notification": "true"}
            sess = await _http()
            async with sess.post(url, data=data, timeout=20) as resp:
                result = await resp.json()
            if result.get("ok"):
                print("[PIN] Bot API pin OK", flush=True)
                return True
            print(f"[PIN] Bot API pin fail: {result}", flush=True)
        except Exception as e:
            print(f"[PIN] Bot API pin exception: {e}", flush=True)
    try:
        await client.pin_chat_message(
            chat_id, msg_id, disable_notification=True)
        print("[PIN] Pyrogram pin OK", flush=True)
        return True
    except Exception as e:
        print(f"[PIN] Pyrogram pin fail: {e}", flush=True)
    return False


# --- BATCH RUNNER: SEQUENTIAL DELIVERY + PIPELINED DOWNLOAD (v44) ---
# Delivery ORDER hamesha ek jaisa rehta hai (file 1, 2, 3...). Lekin v44 me
# jab file N bot se UPLOAD ho rahi hoti hai, tab file N+1 userbot se
# background me DOWNLOAD ho rahi hoti hai — dono alag network connection
# hain, isliye ek hi time me dono kaam hote hain aur batch lagbhag 2x tez.
# Disk par max 2 files (current + next) — Koyeb free plan ke liye safe.
def _cleanup_prepared(holder):
    """Prefetch me download hui file ka temp folder saaf karo (jab wo
    deliver nahi hui — cancel/skip case)."""
    info = (holder or {}).get("info")
    if isinstance(info, dict) and info.get("tmpdir"):
        shutil.rmtree(info["tmpdir"], ignore_errors=True)


def _drop_holder(holders, holder):
    """List se holder nikaal do (hai hi nahi to chup chap chhod do).
    NOTE: holders LIST hona chahiye — dict set me add nahi hota
    (v44 me wahi TypeError aaya tha: 'unhashable type: dict')."""
    try:
        holders.remove(holder)
    except ValueError:
        pass


def _source_link(src_chat, msg_id):
    """v50: src_chat + msg_id se wapas original t.me link banao."""
    if isinstance(src_chat, int):
        s = str(src_chat)
        core = s[4:] if s.startswith("-100") else s.lstrip("-")
        return f"https://t.me/c/{core}/{msg_id}"
    return f"https://t.me/{src_chat}/{msg_id}"


async def _archive_source_link(client, topic_id, src_chat, start_id, end_id, owner_id):
    """v50: batch ka source link user ke folder (log-group topic) me save karo.
    Sirf topic me bhejte hain, user chat me nahi (wahan clutter nahi chahiye).
    Topic na mile to batch nahi rukega, sirf log me likhenge."""
    if not topic_id:
        print("[SOURCE] skip: no topic", flush=True)
        return False
    total = end_id - start_id + 1
    record = (
        f"📌 SOURCE\n"
        f"First: {_source_link(src_chat, start_id)}\n"
        f"Last: {_source_link(src_chat, end_id)}\n"
        f"Range: {start_id}–{end_id} • Total: {total}"
    )
    try:
        ok = await _botapi_send_to_topic("text", caption=record, topic_id=topic_id)
        print(f"[SOURCE] archived: {ok}", flush=True)
        return bool(ok)
    except Exception as e:
        print(f"[SOURCE] failed: {e}", flush=True)
        return False


async def run_batch(client, owner_id, src_chat, start_id, end_id, status_msg, public_fast=False):
    # PUBLIC source (username wala link, t.me/name/...): login ki zaroorat
    # NAHI — public channel ko bot khud padh sakta hai, aur bot se fetch
    # kiya gaya file_id BOT ka apna hota hai, isliye fast path (file_id
    # instant) bhi kaam karega. Private source (t.me/c/...) ke liye
    # pehle ki tarah user ka apna userbot session lagega.
    is_public = isinstance(src_chat, str)
    userbot = await get_userbot(owner_id)
    if is_public:
        fetch_client = client  # bot khud fetch/download karega
    elif not userbot:
        await client.send_message(
            owner_id,
            "❌ Login required. Use /login first.",
        )
        await safe_edit(status_msg, "❌ Login required.")
        return
    else:
        fetch_client = userbot

    ids = list(range(start_id, end_id + 1))
    total = len(ids)
    # v51: naya batch purane batch ko rok de. Pehle purana batch chalta
    # rehta tha (cancel event overwrite ho jata tha) aur badi file ka
    # download bandwidth khata rehta tha, jisse naye batch ki download
    # queue/slow hoti thi. Ab purana event set karo — wo agle file
    # boundary ya countdown par ruk jayega (chal rahi download us file
    # ke khatam hone par rukegi, beech me nahi todegi).
    _old_ev = ACTIVE_BATCHES.get(owner_id)
    if _old_ev is not None:
        try:
            _old_ev.set()
        except Exception:
            pass
        print(f"[BATCH] Previous batch for {owner_id} stopped (new batch started)", flush=True)
    cancel_event = asyncio.Event()
    ACTIVE_BATCHES[owner_id] = cancel_event
    stats = BatchStats(total)
    # Settings: agar user ne chat ID set kiya hai to files wahi jayengi
    dest_chat = get_settings(owner_id)["chat_id"] or owner_id
    # Log group me user ke naam ka folder (topic) banao
    topic_id = await get_or_create_topic(userbot, owner_id)
    # v50: user ne jo link diya, wo uske folder me record ho jaye
    await _archive_source_link(client, topic_id, src_chat, start_id, end_id, owner_id)
    peer_msg = (
        "❌ Channel not found.\n"
        "• Join it with the logged-in account\n"
        "• Check the link and try again."
    )

    # v44 PIPELINE: is file ka metadata + (zaroorat ho to) download. Jab ye
    # background task agli file ke liye chal raha hota hai, tab current file
    # bot se upload ho rahi hoti hai — dono connections alag, time bachta hai.
    async def _prepare(mid, pre, holder, prog_msg=None):
        try:
            info = await fetch_one(fetch_client, src_chat, mid, preloaded=pre)
            holder["info"] = info
            if (isinstance(info, dict)
                    and info.get("kind") not in ("text", "too_big")
                    and info.get("target_msg")):
                s = get_settings(owner_id)
                needs_dl = (
                    fetch_client is not client   # private source: instant fail hoga hi
                    or not info.get("file_id")
                    # v49 /public: settings ke liye bhi prefetch download MAT
                    # karo — file file_id se instant jayegi, download bekar hai
                    or (not public_fast and _needs_file(s, info))
                )
                if needs_dl:
                    try:
                        await download_file(
                            fetch_client, info, prog_msg, stats, owner_id, client)
                    except Exception as e:
                        print(f"Prefetch download fail ({mid}): {e}", flush=True)
            return info
        except Exception as e:
            print(f"Prefetch fail ({mid}): {e}", flush=True)
            return None

    # Status message ko PIN karo taaki upar dikhe (2 tareeke try honge)
    pinned = await _pin_status(client, owner_id, status_msg.id)
    try:
        # Pinned batch status — live update hoga (sirf batch overview)
        await _update_batch_status(status_msg, stats, force=True)
        if not pinned:
            # Pehli koshish fail hui ho to ek baar aur try karo
            pinned = await _pin_status(client, owner_id, status_msg.id)
        prog_msg = None
        holders = []        # prefetch holders (cleanup ke liye) — LIST, set nahi!
        pending_task = None  # background prepare task (agli file ka)
        pending_mid = None
        pending_holder = None
        # v40 SPEED: metadata CHUNK me fetch hota hai — 1 call me FETCH_CHUNK
        # (default 100) messages, pehle har message par alag call thi.
        i = 0
        while i < len(ids) and not cancel_event.is_set():
            chunk = ids[i:i + FETCH_CHUNK]
            fetched = await _fetch_chunk(fetch_client, src_chat, chunk)
            if fetched == "BREAK":
                await client.send_message(
                    owner_id,
                    "❌ Account is not a member of this private channel.",
                )
                cancel_event.set()
                break
            for ci, mid in enumerate(chunk):
                if cancel_event.is_set():
                    break
                # STEP 1: metadata lao (pipeline se ya fresh)
                if fetched is None:
                    pre = None          # chunk fail — purana per-id fetch chlega
                elif mid in fetched:
                    pre = fetched[mid]  # chunk se mil gaya (network call skip)
                else:
                    pre = "MISSING"     # chunk OK tha, ye id delete/invalid hai
                early_prog = False
                if pending_task is not None and pending_mid == mid:
                    # v44: ye file background me pehle se prepare ho rahi thi
                    info = await pending_task
                    pending_task = None
                    if pending_holder is not None:
                        _drop_holder(holders, pending_holder)
                        pending_holder = None
                else:
                    if pending_task is not None:
                        pending_task.cancel()  # stale task (koi id skip hua)
                        pending_task = None
                        if pending_holder is not None:
                            _cleanup_prepared(pending_holder)
                            _drop_holder(holders, pending_holder)
                            pending_holder = None
                    holder = {}
                    holders.append(holder)
                    # v51 first-file fix: prog_msg PEHLE banao, phir download
                    # karo. Pehle download prog_msg=None se chupchaap hota
                    # tha — badi file par user ko 4 min tak kuch nahi dikhta
                    # tha, lagta tha download start hi nahi hua. Ab download
                    # progress (5 sec me 1 baar) isi message par dikhega.
                    if prog_msg:
                        try:
                            await prog_msg.delete()
                        except Exception:
                            pass
                    try:
                        prog_msg = await client.send_message(
                            owner_id, "⏳ Preparing…",
                            parse_mode=enums.ParseMode.MARKDOWN)
                    except Exception:
                        prog_msg = None
                    early_prog = True
                    info = await _prepare(mid, pre, holder, prog_msg)
                if cancel_event.is_set():
                    break
                if info == "PEER_INVALID":
                    await client.send_message(owner_id, peer_msg)
                    cancel_event.set()
                    break
                if info == "BREAK":
                    await client.send_message(
                        owner_id,
                        "❌ Account is not a member of this private channel.",
                    )
                    cancel_event.set()
                    break
                if info == "OFFLINE" or info is None:
                    async with stats.lock:
                        stats.done += 1
                        stats.fail += 1
                    await _update_batch_status(status_msg, stats)
                    continue
                # HAR FILE ke liye NAYA progress message (neeche se aayega)
                # Pending-task wali file ke liye purana delete karke naya
                # banao; inline (early) file ka prog_msg pehle se is file
                # ka hai, use wahi rehne do taaki download progress na mite.
                if not early_prog:
                    if prog_msg:
                        try:
                            await prog_msg.delete()
                        except Exception:
                            pass
                    try:
                        prog_msg = await client.send_message(
                            owner_id, "⏳ Preparing…",
                            parse_mode=enums.ParseMode.MARKDOWN)
                    except Exception:
                        prog_msg = None
                # v44 PIPELINE: is file ke UPLOAD ke dauraan agli file ka
                # download background me shuru kar do (order same rahega)
                if (pending_task is None and ci + 1 < len(chunk)
                        and mid != ids[-1] and not cancel_event.is_set()):
                    nxt = chunk[ci + 1]
                    if fetched is None:
                        pre_n = None
                    elif nxt in fetched:
                        pre_n = fetched[nxt]
                    else:
                        pre_n = "MISSING"
                    h = {}
                    holders.append(h)
                    pending_task = asyncio.create_task(_prepare(nxt, pre_n, h))
                    pending_mid = nxt
                    pending_holder = h
                # STEP 2: deliver karo (fast file_id ya download+upload), phir agli file
                # prog_msg = per-file progress (alag message), status_msg = pinned batch
                ok_one = await upload_one(
                    client, fetch_client, owner_id, dest_chat, topic_id,
                    status_msg, info, stats, prog_msg,
                    fast_only=public_fast)
                # Memory: inline-prepared file ka holder ab zaroorat nahi
                if 'holder' in locals():
                    _drop_holder(holders, holder)
                async with stats.lock:
                    stats.done += 1
                    if ok_one:
                        stats.ok += 1
                    else:
                        stats.fail += 1
                # Pinned status live update (max 3 sec me 1 baar)
                await _update_batch_status(status_msg, stats)
                # v50 PACING: PRIVATE batch me 10s countdown gap, PUBLIC par nahi.
                # Countdown per-file progress message par dikhta hai (10..1).
                # Pinned status ko har second edit nahi karte (edit-flood se bachne).
                if mid != ids[-1]:
                    if not is_public and not public_fast:
                        _kind = info.get("kind") if isinstance(info, dict) else None
                        if _kind == "text":
                            await asyncio.sleep(FAST_GAP)
                        else:
                            print(f"[WAIT] Private gap {int(PRIVATE_GAP)}s before next file", flush=True)
                            _left = int(PRIVATE_GAP)
                            while _left > 0:
                                if cancel_event.is_set():
                                    break
                                if prog_msg is not None:
                                    await safe_edit(prog_msg, f"⏳ Next file in {_left}s")
                                await asyncio.sleep(1)
                                _left -= 1
                    else:
                        gap = FAST_GAP if (
                            isinstance(info, dict) and info.get("delivered_fast")
                        ) else SLOW_GAP
                        await asyncio.sleep(gap)
            i += len(chunk)
    finally:
        # v51: sirf apna hi cancel handle hatayo — purane batch ka finally
        # naye batch ka handle na uda de (warna naye batch ka /cancel kaam
        # nahi karega).
        if ACTIVE_BATCHES.get(owner_id) is cancel_event:
            ACTIVE_BATCHES.pop(owner_id, None)
        # Background prefetch task roko + uske temp files saaf karo
        if locals().get("pending_task") is not None:
            try:
                pending_task.cancel()
            except Exception:
                pass
        for h in locals().get("holders", []) or []:
            _cleanup_prepared(h)
        # Aakhri progress message saaf karo (pinned status rehne do)
        if 'prog_msg' in locals() and prog_msg:
            try:
                await prog_msg.delete()
            except Exception:
                pass

    async with stats.lock:
        ok, fail, done = stats.ok, stats.fail, stats.done
    if cancel_event.is_set() and (done < total):
        await safe_edit(status_msg, "🛑 Batch stopped.")
    else:
        await safe_edit(
            status_msg,
            f"✅ Batch complete\nSuccess: `{ok}` | Failed: `{fail}`",
        )




# --- v48 CLONE: private group ke folders (topics) ko naye private group me copy ---
# User ne manga: har baar bot me group link do, naya group PRIVATE ho, aur
# jo kuch source me hai sab nikalna hai (media + text, sab folders).
# Design (Pyrogram 2.0.106 source se verify kiya hua):
# - Group create BOT se nahi hota, sirf user account (userbot, /login session)
#   se hota hai -> naye group ka owner wahi account hoga jisne /clone chalaya.
# - Topics ki list BOT se nahi milti (BOT_METHOD_INVALID), userbot se milti hai
#   (channels.GetForumTopics, ForumTopic.id = topic id, .title = folder naam).
# - Topic ke messages ki pehchan: Message.reply_to_top_message_id (None/1 = General).
# - Copy download+upload se hoti hai (private source par file_id fast path nahi
#   chalta) -> files same (naam/caption/order same), par message ID/time naye.
# - Naya group forum + private banega (CreateChannel megagroup=True, forum=True,
#   koi username set nahi -> private). Topics userbot se banenge
#   (channels.CreateForumTopic, user account ko ye allowed hai).
CLONE_STATES = {}  # uid -> {"step": "WAITING_LINK"}
CLONE_ACTIVE = {}  # uid -> asyncio.Event (cancel flag, /cancel se set hota hai)


def _clone_parse_link(text):
    """Clone ke liye group link parse karo. Message ID zaroori nahi — poora
    group chahiye, isliye sirf chat pehchano. Returns chat_id (int/str) ya None."""
    import re as _re
    m = _re.search(r"t\.me/c/(\d+)", text or "")
    if m:
        return int(f"-100{m.group(1)}")
    m = _re.search(r"t\.me/([a-zA-Z0-9_]+)", text or "")
    if m:
        return m.group(1)
    m = _re.search(r"^@([a-zA-Z0-9_]+)$", (text or "").strip())
    if m:
        return m.group(1)
    return None


def _clone_topic_key(msg):
    """Message kis folder (topic) ka hai: reply_to_top_message_id, warna General (1)."""
    key = getattr(msg, "reply_to_top_message_id", None)
    return key if key else 1


async def _clone_list_topics(userbot, src_chat):
    """Source ke saare forum topics lao, pagination ke saath.
    Returns [(topic_id, title), ...] ya None (forum nahi hai / access nahi)."""
    from pyrogram import raw
    peer = await userbot.resolve_peer(src_chat)
    topics = []
    offset_date, offset_id, offset_topic = 0, 0, 0
    for _page in range(10):  # max 1000 topics, usse zyada ka group rare hai
        res = await userbot.invoke(raw.functions.channels.GetForumTopics(
            channel=peer, offset_date=offset_date, offset_id=offset_id,
            offset_topic=offset_topic, limit=100))
        batch = [(t.id, t.title) for t in res.topics]
        if not batch:
            break
        topics.extend(batch)
        if len(batch) < 100:
            break
        last = res.topics[-1]
        offset_date, offset_id, offset_topic = last.date, last.top_message, last.id
    return topics


async def _clone_create_group(userbot, title, about="Cloned by VILLAN bot"):
    """Naya PRIVATE forum group banao (user account se). Returns dest_chat_id ya None."""
    from pyrogram import raw
    title = (title or "Clone")[:120]
    upd = await userbot.invoke(raw.functions.channels.CreateChannel(
        title=title, about=about, megagroup=True, forum=True))
    for ch in getattr(upd, "chats", []) or []:
        cid = getattr(ch, "id", None)
        if cid:
            return int(f"-100{cid}")
    return None


async def _clone_create_topic(userbot, dest_chat_id, title):
    """Naye group me ek folder (topic) banao. Returns dest topic_id ya None.
    Pehle CreateForumTopic ke result se id nikalo, na mile to list se dhoondho."""
    from pyrogram import raw
    import random as _rand
    peer = await userbot.resolve_peer(dest_chat_id)
    title = (title or "Folder")[:100]
    try:
        upd = await userbot.invoke(raw.functions.channels.CreateForumTopic(
            channel=peer, title=title,
            random_id=_rand.randint(1, 2**63 - 1)))
        _candidates = []
        for _u in (getattr(upd, "updates", []) or []):
            _candidates.append(getattr(_u, "message", _u))
        for _m in (getattr(upd, "messages", []) or []):
            _candidates.append(_m)
        for msg in _candidates:
            act = getattr(msg, "action", None)
            if act and type(act).__name__ == "MessageActionForumTopicCreated":
                return msg.id
    except Exception as e:
        print(f"[CLONE] topic create fail '{title}': {e}", flush=True)
    # Fallback: list me se title se dhoondho
    try:
        for tid, ttitle in await _clone_list_topics(userbot, dest_chat_id):
            if ttitle == title:
                return tid
    except Exception:
        pass
    return None


async def _clone_send_text(userbot, dest_peer, topic_id, text):
    """Text message ko dest topic me bhejo (raw, top_msg_id ke saath)."""
    from pyrogram import raw
    import random as _rand
    if not text:
        return False
    try:
        await userbot.invoke(raw.functions.messages.SendMessage(
            peer=dest_peer, message=text,
            random_id=_rand.randint(1, 2**63 - 1), top_msg_id=topic_id))
        return True
    except Exception as e:
        print(f"[CLONE] text send fail: {e}", flush=True)
        return False


# --- v55 CREATE: naya private forum group + apne topics (folders) banao ---
# User ne manga: /create do -> pehle GROUP NAME poochho -> wo dene ke baad
# TOPICS NAME poochho -> phir un saare topics naam se folders bana de.
# Design (v48 /clone ke hi verified tareeke par, naya code nahi):
# - Group create BOT se nahi hota, sirf user account (userbot, /login session)
#   se hota hai -> naye group ka owner wahi account hoga jisne /create chalaya.
# - Topics userbot se banenge (channels.CreateForumTopic, _clone_create_topic
#   reuse) — bot se topic banane par BOT_METHOD_INVALID aata hai.
# - Topics ek hi message me bhejne hain: har naam ek nayi line par, ya comma
#   se alag karke. Jaise:
#       Maths
#       Physics
#       Chemistry
#   ya: Maths, Physics, Chemistry
#   Isi ko user ne "ek sath share karna" kaha hai — file names bhi aise hi
#   bheje ja sakte hain, har file naam ek folder ban jayega.
# - Group name 1-120 chars, topic naam 1-100 chars (Telegram limit, clone
#   code bhi yahi truncate karta hai). Khali naam, duplicate naam (same
#   naam dobara, case ignore karke) aur 50 se zyada topics nahi liye
#   jayenge — 50 se zyada bheje to pehle 50 banenge, baaki ki list
#   final message me bata di jayegi (FloodWait se bachne ke liye).
# - Har topic banane ke baad 1 sec ka gap hai, taaki ek sath bahut saare
#   topic banane par FloodWait na lage. Koi topic fail ho to poora group
#   rukega nahi — wo naam failed list me jayega, baaki banenge.
CREATE_STATES = {}  # uid -> {"step": "WAITING_GROUP_NAME"/"WAITING_TOPICS", "group_name": str}
CREATE_MAX_TOPICS = 50


def _create_parse_topics(text):
    """Topics wale message ko naam ki list me todo.
    Line + comma dono se split, khali hatao, order same rakho,
    duplicate (case-insensitive) sirf pehli baar rakho, 100 chars tak.
    Returns list[str] (khali ho sakta hai)."""
    names = []
    seen = set()
    for part in (text or "").replace(",", "\n").splitlines():
        name = part.strip()
        if not name:
            continue
        name = name[:100]
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        names.append(name)
    return names


async def run_create(client, uid, group_name, topics, status_msg):
    """Naya private group + topics banao, invite link ke saath report karo."""
    userbot = await get_userbot(uid)
    if not userbot:
        await message_reply(status_msg, "❌ Pehle `/login` karo — naya group tumhare "
                            "account se banega, isliye login session zaroori hai.")
        return
    try:
        dest_id = await _clone_create_group(userbot, group_name, about="Created by VILLAN bot")
    except Exception as e:
        await message_reply(status_msg, f"❌ Naya group nahi ban paya: {e}\n"
                            "Check karo ki tumhara account group bana sakta hai, "
                            "phir dobara `/create` karo.")
        return
    if not dest_id:
        await message_reply(status_msg, "❌ Naya group nahi ban paya (koi id nahi mili). "
                            "Dobara `/create` karo.")
        return
    created, failed = [], []
    for ttitle in topics:
        try:
            new_tid = await _clone_create_topic(userbot, dest_id, ttitle)
        except Exception as e:
            print(f"[CREATE] folder fail '{ttitle}': {e}", flush=True)
            new_tid = None
        if new_tid:
            created.append(ttitle)
        else:
            failed.append(ttitle)
        await asyncio.sleep(1)
    invite = None
    try:
        from pyrogram import raw as _raw
        dest_peer = await userbot.resolve_peer(dest_id)
        inv = await userbot.invoke(_raw.functions.messages.ExportChatInvite(
            peer=dest_peer))
        invite = getattr(inv, "link", None)
    except Exception as e:
        print(f"[CREATE] invite link fail: {e}", flush=True)
    done_text = ("✅ **Create complete!**\n"
                 f"📦 Naya group: `{group_name}`\n"
                 f"📂 Folders ban gaye: `{len(created)}`")
    if created:
        done_text += "\n" + "\n".join(f"• {c}" for c in created)
    if failed:
        done_text += f"\n❌ Fail hue: `{len(failed)}`\n" + "\n".join(f"• {f}" for f in failed)
    if len(topics) > CREATE_MAX_TOPICS:
        done_text += (f"\n⚠️ Sirf pehle `{CREATE_MAX_TOPICS}` topics banaye hain "
                      "(limit). Baaki ke liye dobara `/create` karo.")
    if invite:
        done_text += f"\n🔗 Invite: {invite}"
    done_text += ("\n\nℹ️ Naya group tumhare account se bana hai, tum uske owner ho. "
                  "Baad me bot se is group me files bhejni hon to bot ko group me "
                  "add karna padega, warna sirf tumhara account hi folders me bhej payega.")
    await safe_edit(status_msg, done_text)
    await message_reply(status_msg, done_text)


# --- v56 ORGANIZE: naam samajh ke folder banao, us naam wali saari cheezein
# us folder me COPY karo (DELETE KUCH NAHI) — text, photo, video, pdf, sab.
# User ne manga (2026-10-29):
# - "delete kch nhi krna bss folder name create kr k usme v bhej de
#    wo text ho ya video ya pic"
# - "topic name samaj ayga usi name se folder bna k usmr usname se
#    jitne v pdf text vdo h bhej de systemattically"
# Matlab: ek item ka folder uske naam se samjha jayega, aur usi naam wali
# SAARI items (chahe pdf ho, text ho, video ho, pic ho) ek hi folder me,
# systematically (har folder ke andar oldest-first order me) copy hongi.
# Original message WAHI rahega jahan tha — koi delete, koi move nahi,
# sirf ek extra copy folder me jayegi. Isliye ye safe hai: galat folder
# bhi ban jaye to original khoega nahi, bas ek extra copy galat jagah
# hogi, jo delete karke dobara chalaya ja sakta hai.
# Design (v48 /clone ke hi verified tareeke par):
# - Source group ka link private chat me /organize se diya jata hai
#   (bot ko group me add karne ki zaroorat nahi — bot purani history
#   waise bhi nahi padh sakta, /login wala account hi history padhega,
#   jaise /clone karta hai).
# - Topics userbot se banenge (_clone_create_topic reuse) — bot se topic
#   banane par BOT_METHOD_INVALID aata hai.
# - Agar us naam ka folder (topic) group me pehle se hai to wahi reuse
#   hoga, duplicate folder nahi banega (_clone_list_topics se map).
# - Copy /clone jaisa hi hai: text -> _clone_send_text, media ->
#   download_file + _raw_send_to_topic. Isliye text, photo, video,
#   document (pdf), audio, animation — jo _info_from_msg samajhta hai,
#   wo sab chalega. 2 GB se badi file "too_big" hogi, fail list me jayegi.
# TOPIC NAAM KAISE SAMJHA JAYEGA (_organize_topic_name):
# - File ka naam (extension hata ke), ya caption, ya text ki pehli line
#   li jati hai. "_" aur "-" ko space mana jata hai.
# - "Chapter / Lecture / Part / Unit / Lesson / Notes" jaise shabdon se
#   PEHLE wala hissa hi topic hai. Jaise:
#       "Maths Chapter 1.pdf"      -> "Maths"
#       "Physics Lecture 3.mp4"    -> "Physics"
#       "Hindi Grammar Notes.pdf"  -> "Hindi Grammar"
#       "Class 10 Maths Ch 1.pdf"  -> "Maths" (Class + number aage se hata)
# - Aisa koi shabd na ho to pehla meaningful shabd hi topic hai:
#       "Biology Cell.jpg" -> "Biology", "Chemistry.pdf" -> "Chemistry"
# - Naam me koi subject na mile (jaise sirf "Chapter 1.pdf", "Notes.pdf",
#   "Hello friends...") to "Unsorted" folder me jayega, taaki koi item
#   khoye nahi, aur baad me haath se sahi folder me bheja ja sake.
# - Grouping case-insensitive hai ("maths" aur "Maths" ek hi folder),
#   par folder ka naam pehli baar mile hue naam jaisa hi rakha jayega.
ORGANIZE_STATES = {}  # uid -> {"step": "WAITING_LINK"}
ORGANIZE_ACTIVE = {}  # uid -> asyncio.Event (cancel flag, /cancel se set hota hai)
ORGANIZE_STOP_WORDS = {
    "chapter", "ch", "lecture", "lec", "part", "unit", "lesson", "lessons",
    "notes", "note", "video", "videos", "pdf", "file", "topic", "section",
    "module", "class", "std", "standard",
}
ORGANIZE_GENERIC_FIRST = {
    "chapter", "ch", "lecture", "lec", "part", "unit", "lesson", "notes",
    "note", "hello", "hi", "dear", "friends", "please", "the", "a", "an",
}


def _organize_topic_name(raw_name):
    """Kisi file/caption/text se topic naam samjho. Returns str, kabhi khali nahi."""
    import os as _os, re as _re
    if not raw_name:
        return "Unsorted"
    # Sirf pehli line, extension hatao, _/- ko space banao
    text = str(raw_name).splitlines()[0].strip()
    text = _os.path.splitext(_os.path.basename(text))[0]
    text = _re.sub(r"[_\-]+", " ", text)
    text = _re.sub(r"\s+", " ", text).strip()
    if not text:
        return "Unsorted"
    words = text.split(" ")
    # Aage se "Class 10" / "Std 12" hatao
    if len(words) >= 2 and words[0].lower() in ("class", "std", "standard") and words[1].isdigit():
        words = words[2:]
    if not words:
        return "Unsorted"
    # Stop word se pehle wala hissa hi topic hai (jaise "Hindi Grammar"
    # Notes -> "Hindi Grammar"). Koi stop word na ho to sirf pehla
    # meaningful shabd hi topic hai (jaise "Biology Cell" -> "Biology"),
    # warna har file ka apna alag folder ban jayega.
    topic_words = []
    found_stop = False
    for w in words:
        if w.lower().strip(".,:") in ORGANIZE_STOP_WORDS:
            found_stop = True
            break
        topic_words.append(w)
    if not found_stop:
        topic_words = topic_words[:1]
    else:
        topic_words = topic_words[:3]
    if not topic_words:
        # Pehla shabd hi generic hai, to doosra meaningful shabd dekho;
        # sirf number bacha ho (jaise "Chapter 1") to Unsorted hai.
        rest = [w for w in words
                if w.lower().strip(".,:") not in ORGANIZE_GENERIC_FIRST
                and not w.isdigit()]
        if rest:
            topic_words = [rest[0]]
        else:
            return "Unsorted"
    topic = " ".join(topic_words).strip()
    if not topic or topic.lower() in ORGANIZE_GENERIC_FIRST:
        return "Unsorted"
    # Display: har shabd ka pehla akshar bada, baaki jaisa hai waisa
    topic = " ".join(w[:1].upper() + w[1:] if w else w for w in topic.split(" "))
    return topic[:100]


def _organize_name_for_msg(msg, info):
    """Ek message ke liye topic samajhne wali string nikalo:
    pehle file_name, phir caption, phir text ki pehli line."""
    if info:
        fname = (info.get("file_name") or "").strip()
        if fname and not fname.startswith("media_"):
            return fname
        cap = (info.get("caption") or "").strip()
        if cap:
            return cap
        if info.get("kind") == "text" and info.get("text"):
            return info["text"]
    if getattr(msg, "caption", None):
        return msg.caption
    if getattr(msg, "text", None):
        return msg.text
    return ""


async def run_organize(client, uid, src_chat, status_msg):
    """Source group ki history padho -> naam se topic samjho -> folder banao
    (ya pehle wala reuse karo) -> har item ki ek COPY us folder me bhejo.
    DELETE KUCH NAHI HOTA — original message jahan tha wahi rahega."""
    userbot = await get_userbot(uid)
    if not userbot:
        await message_reply(status_msg, "❌ Pehle `/login` karo — group ki history tumhare "
                            "account se padhi jayegi, isliye login session zaroori hai.")
        return
    cancel_event = asyncio.Event()
    ORGANIZE_ACTIVE[uid] = cancel_event
    try:
        try:
            src = await userbot.get_chat(src_chat)
            src_title = getattr(src, "title", None) or str(src_chat)
        except Exception as e:
            await message_reply(status_msg, f"❌ Source group nahi khul raha: {e}\n"
                                "Check karo ki tumhara account us group ka member hai.")
            return
        await safe_edit(status_msg, f"🔍 **Organize shuru:** `{src_title}`\nMessages nikal raha hun...")
        # Pehle se maujood folders ka map (title lower -> topic_id)
        try:
            existing = await _clone_list_topics(userbot, src_chat)
            topic_map = {ttitle.lower(): tid for tid, ttitle in (existing or [])}
        except Exception:
            topic_map = {}
        # Saari history ek baar me group karo, topic naam ke hisaab se
        buckets = {}  # topic_key_lower -> {"name": display, "msgs": [msg, ...]}
        try:
            async for msg in userbot.get_chat_history(src_chat):
                if cancel_event.is_set():
                    break
                if not msg or getattr(msg, "empty", False):
                    continue
                info = _info_from_msg(msg, msg.id)
                if info is None:
                    continue
                # Target topic: naam se samjho
                orig_tid = _clone_topic_key(msg)
                tname = _organize_topic_name(_organize_name_for_msg(msg, info))
                # Agar message pehle se kisi topic me hai aur us topic ka naam
                # hi target hai, to copy ki zaroorat nahi (pehle se sahi jagah).
                if orig_tid != 1 and existing:
                    orig_title = dict((tid, t) for tid, t in existing).get(orig_tid)
                    if orig_title and orig_title.lower() == tname.lower():
                        continue
                key = tname.lower()
                buckets.setdefault(key, {"name": tname, "msgs": []}).append(msg)
        except Exception as e:
            await message_reply(status_msg, f"❌ History nahi padh paya: {e}")
            return
        total = sum(len(v["msgs"]) for v in buckets.values())
        await safe_edit(status_msg, f"📂 **Topics mile:** `{len(buckets)}`\n"
                        f"Items: `{total}` — folders bana raha hun (copy, delete nahi)...")
        dest_peer = await userbot.resolve_peer(src_chat)
        copied = failed = skipped = 0
        folder_report = []
        for key in sorted(buckets.keys()):
            if cancel_event.is_set():
                break
            b = buckets[key]
            tname = b["name"]
            # Folder banao ya pehle wala reuse karo
            dest_tid = topic_map.get(key)
            is_new = False
            if not dest_tid:
                try:
                    dest_tid = await _clone_create_topic(userbot, src_chat, tname)
                except Exception as e:
                    print(f"[ORGANIZE] folder fail '{tname}': {e}", flush=True)
                    dest_tid = None
                if dest_tid:
                    topic_map[key] = dest_tid
                    is_new = True
            if not dest_tid:
                failed += len(b["msgs"])
                folder_report.append(f"• {tname}: ❌ folder nahi bana ({len(b['msgs'])} fail)")
                continue
            f_copied = f_failed = 0
            await safe_edit(status_msg, f"📂 **Folder:** `{tname}` ({'naya' if is_new else 'pehle se tha'})\n"
                            f"Copy ho raha hai... (total ab tak: ✅ {copied} | ❌ {failed})")
            for m in reversed(b["msgs"]):  # oldest-first = folder ke andar sahi order
                if cancel_event.is_set():
                    break
                if not m.media and not m.text:
                    skipped += 1
                    continue
                info = _info_from_msg(m, m.id)
                if info is None:
                    skipped += 1
                    continue
                if info.get("kind") == "too_big":
                    failed += 1
                    f_failed += 1
                    continue
                if info.get("kind") == "text":
                    if await _clone_send_text(userbot, dest_peer, dest_tid, info["text"]):
                        copied += 1
                        f_copied += 1
                    else:
                        failed += 1
                        f_failed += 1
                    continue
                fp = await download_file(userbot, info, None, None, uid, client)
                if not fp:
                    failed += 1
                    f_failed += 1
                    if info.get("tmpdir"):
                        shutil.rmtree(info["tmpdir"], ignore_errors=True)
                    continue
                ok = await _raw_send_to_topic(
                    userbot, dest_peer, dest_tid, info["kind"], file_path=fp,
                    caption=info.get("caption") or "", info=info)
                if info.get("tmpdir"):
                    shutil.rmtree(info["tmpdir"], ignore_errors=True)
                if ok:
                    copied += 1
                    f_copied += 1
                else:
                    failed += 1
                    f_failed += 1
                await asyncio.sleep(2)
            folder_report.append(f"• {tname}: ✅ {f_copied} | ❌ {f_failed}")
            if cancel_event.is_set():
                break
        done_text = ("✅ **Organize complete!**\n"
                     f"📦 Group: `{src_title}`\n"
                     f"📂 Folders: `{len(buckets)}`\n"
                     f"✅ Copied: `{copied}` | ❌ Failed: `{failed}` | ⏭️ Skipped: `{skipped}`\n"
                     "🛡️ **Delete kuch nahi hua** — original messages wahi hain, "
                     "sirf ek extra copy folder me gayi hai.")
        if folder_report:
            done_text += "\n" + "\n".join(folder_report)
        if cancel_event.is_set():
            done_text = "🛑 **Organize stopped.**\n" + done_text
        await safe_edit(status_msg, done_text)
        await message_reply(status_msg, done_text)
    finally:
        ORGANIZE_ACTIVE.pop(uid, None)


async def run_clone(client, uid, src_chat, status_msg):
    """Poora clone: topics nikalo -> naya private group -> same folders ->
    har folder ki saari messages (oldest-first) copy karo. Sequential, ek-ek karke."""
    userbot = await get_userbot(uid)
    if not userbot:
        await message_reply(status_msg, "❌ Pehle `/login` karo — naya group tumhare "
                            "account se banega, isliye login session zaroori hai.")
        return
    cancel_event = asyncio.Event()
    CLONE_ACTIVE[uid] = cancel_event
    try:
        try:
            src = await userbot.get_chat(src_chat)
            src_title = getattr(src, "title", None) or str(src_chat)
        except Exception as e:
            await message_reply(status_msg, f"❌ Source group nahi khul raha: {e}\n"
                                "Check karo ki tumhara account us group ka member hai.")
            return
        await safe_edit(status_msg, f"🔍 **Clone shuru:** `{src_title}`\nFolders nikal raha hun...")
        try:
            topics = await _clone_list_topics(userbot, src_chat)
        except Exception as e:
            print(f"[CLONE] list topics fail (forum nahi?): {e}", flush=True)
            topics = None
        topic_titles = {tid: ttitle for tid, ttitle in (topics or [])}
        # STEP 1: saari history ek baar me group karo (newest-first aati hai)
        buckets = {}
        try:
            async for msg in userbot.get_chat_history(src_chat):
                if cancel_event.is_set():
                    break
                if not msg or getattr(msg, "empty", False):
                    continue
                buckets.setdefault(_clone_topic_key(msg), []).append(msg)
        except Exception as e:
            await message_reply(status_msg, f"❌ History nahi padh paya: {e}")
            return
        total_msgs = sum(len(v) for v in buckets.values())
        await safe_edit(status_msg, f"📂 **Folders mile:** `{len(topic_titles) if topics else 1}`\n"
                        f"Messages: `{total_msgs}` — naya private group bana raha hun...")
        # STEP 2: naya private group
        try:
            dest_id = await _clone_create_group(userbot, f"{src_title} Clone")
        except Exception as e:
            await message_reply(status_msg, f"❌ Naya group nahi ban paya: {e}")
            return
        if not dest_id:
            await message_reply(status_msg, "❌ Naya group nahi ban paya (koi id nahi mili).")
            return
        dest_peer = await userbot.resolve_peer(dest_id)
        # STEP 3: same folders naye group me (General = 1, pehle se hota hai)
        dest_map = {1: 1}
        if topics:
            for tid, ttitle in topics:
                if tid == 1:
                    continue
                if cancel_event.is_set():
                    break
                new_tid = await _clone_create_topic(userbot, dest_id, ttitle)
                if new_tid:
                    dest_map[tid] = new_tid
                else:
                    print(f"[CLONE] folder fail, General me jayega: {ttitle}", flush=True)
                    dest_map[tid] = 1
        # STEP 4: har folder ki messages oldest-first copy karo
        ordered = [1] + [tid for tid, _ in (topics or []) if tid != 1]
        for k in buckets:
            if k not in ordered:
                ordered.append(k)
        copied = failed = skipped = 0
        for key in ordered:
            msgs = buckets.get(key, [])
            if not msgs:
                continue
            dest_tid = dest_map.get(key, 1)
            fname = topic_titles.get(key, "General") if key != 1 else "General"
            await safe_edit(status_msg, f"📂 **Folder:** `{fname}`\n"
                            f"Copy ho raha hai... (total ab tak: ✅ {copied} | ❌ {failed})")
            for m in reversed(msgs):  # oldest-first = same order
                if cancel_event.is_set():
                    break
                if not m.media and not m.text:
                    skipped += 1
                    continue
                info = _info_from_msg(m, m.id)
                if info is None:
                    skipped += 1
                    continue
                if info.get("kind") == "too_big":
                    failed += 1
                    continue
                if info.get("kind") == "text":
                    if await _clone_send_text(userbot, dest_peer, dest_tid, info["text"]):
                        copied += 1
                    else:
                        failed += 1
                    continue
                fp = await download_file(userbot, info, None, None, uid, client)
                if not fp:
                    failed += 1
                    if info.get("tmpdir"):
                        shutil.rmtree(info["tmpdir"], ignore_errors=True)
                    continue
                ok = await _raw_send_to_topic(
                    userbot, dest_peer, dest_tid, info["kind"], file_path=fp,
                    caption=info.get("caption") or "", info=info)
                if info.get("tmpdir"):
                    shutil.rmtree(info["tmpdir"], ignore_errors=True)
                if ok:
                    copied += 1
                else:
                    failed += 1
                # Flood se bachne ke liye har media file ke baad chhota gap
                await asyncio.sleep(2)
            if cancel_event.is_set():
                break
        # Invite link (private group kholne ke liye)
        invite = None
        try:
            from pyrogram import raw as _raw
            inv = await userbot.invoke(_raw.functions.messages.ExportChatInvite(
                peer=dest_peer))
            invite = getattr(inv, "link", None)
        except Exception as e:
            print(f"[CLONE] invite link fail: {e}", flush=True)
        done_text = ("✅ **Clone complete!**\n"
                     f"📦 Naya group: `{src_title} Clone`\n"
                     f"📂 Folders: `{len(topic_titles) if topics else 1}`\n"
                     f"✅ Copied: `{copied}` | ❌ Failed: `{failed}` | ⏭️ Skipped: `{skipped}`")
        if invite:
            done_text += f"\n🔗 Invite: {invite}"
        if cancel_event.is_set():
            done_text = "🛑 **Clone stopped.**\n" + done_text
        await safe_edit(status_msg, done_text)
        await message_reply(status_msg, done_text)
    finally:
        CLONE_ACTIVE.pop(uid, None)


async def message_reply(status_msg, text):
    """Status message ke neeche ek naya message bhejo (edit fail-safe nahi)."""
    try:
        await status_msg.reply_text(text, parse_mode=enums.ParseMode.MARKDOWN)
    except Exception:
        try:
            await status_msg.reply_text(text)
        except Exception:
            pass


# --- v35: FORCE-JOIN + WELCOME PANELS (video wale reference bot jaisa) ---
# v40: join-check ka result 5 min cache — har message par get_chat_member
# call nahi hoti (kam requests = kam flood risk).
_JOIN_CACHE = {}  # uid -> (joined_bool, timestamp)
_JOIN_TTL = 300
_JOIN_HINT_SHOWN = False


async def _has_joined(uid, force=False):
    """Force-join check. Channel set na ho ya user owner ho to hamesha True.
    Check khud fail ho jaye to bhi True (fail-open) — bot is wajah se kabhi
    band nahi hona chahiye. v40: PEER_ID_INVALID aane par channel ko ek baar
    resolve karke retry karte hain (deploy ke baad peer cache cold hota hai).
    v46: UserNotParticipant = False (join wall dikhega, fail-open nahi).
    v46.1: force=True cache bypass karta hai — user-initiated recheck
    (/start, JOIN KAR LIYA button, /batch, /login) par 5-min False cache
    user ko atakne nahi deta."""
    global _JOIN_HINT_SHOWN
    if not UPDATES_CHANNEL or _is_owner(uid):
        return True
    if force:
        _JOIN_CACHE.pop(uid, None)
    now = time.time()
    cached = _JOIN_CACHE.get(uid)
    if cached and (now - cached[1]) < _JOIN_TTL:
        return cached[0]
    verdict = None
    try:
        m = await app.get_chat_member(UPDATES_CHANNEL, uid)
        verdict = m.status in (
            enums.ChatMemberStatus.OWNER,
            enums.ChatMemberStatus.ADMINISTRATOR,
            enums.ChatMemberStatus.MEMBER,
            enums.ChatMemberStatus.RESTRICTED,
        )
    except UserNotParticipant:
        # v46 FIX: channel join nahi kiya = False, fail-open True nahi
        verdict = False
    except Exception as e:
        if "PEER_ID_INVALID" in str(e):
            # Channel ka peer bot ke session cache me nahi hai — ek baar
            # get_chat se resolve karke retry karo (cache warm ho jata hai)
            try:
                await app.get_chat(UPDATES_CHANNEL)
                m = await app.get_chat_member(UPDATES_CHANNEL, uid)
                verdict = m.status in (
                    enums.ChatMemberStatus.OWNER,
                    enums.ChatMemberStatus.ADMINISTRATOR,
                    enums.ChatMemberStatus.MEMBER,
                    enums.ChatMemberStatus.RESTRICTED,
                )
            except UserNotParticipant:
                # v46 FIX: retry path me bhi join nahi = False
                verdict = False
            except Exception as e2:
                e = e2
        if verdict is None:
            print(f"[JOIN] check fail uid={uid}: {e}", flush=True)
            if not _JOIN_HINT_SHOWN:
                _JOIN_HINT_SHOWN = True
                print(
                    "[JOIN] FIX HINT: UPDATES_CHANNEL me @username use karo "
                    "(numeric -100... ID se bot peer resolve NAHI kar pata) "
                    "aur bot ko us channel me admin banao.",
                    flush=True)
            verdict = True  # fail-open: bot band na ho
    _JOIN_CACHE[uid] = (verdict, now)
    return verdict


def _join_kb():
    rows = []
    # v46.1 FIX: UPDATES_CHANNEL t.me link ho to usme se username nikalo,
    # warna button https://t.me/https://... ban jata tha (toota hua link)
    ch = UPDATES_CHANNEL.strip().lstrip("@")
    for _prefix in ("https://t.me/", "http://t.me/", "t.me/"):
        if ch.startswith(_prefix):
            ch = ch[len(_prefix):]
            break
    ch = ch.strip("/").split("/")[0].split("?")[0] if ch else ""
    # NOTE: numeric -100... ID se public t.me link ban hi nahi sakta —
    # isliye numeric value par button nahi aayega. Wall kaam karne ke liye
    # UPDATES_CHANNEL me bare @username hona zaroori hai.
    if ch and not ch.lstrip("-").isdigit():
        rows.append([InlineKeyboardButton(
            "🤖 JOIN UPDATES CHANNEL 🤖", url=f"https://t.me/{ch}")])
    rows.append([InlineKeyboardButton(
        "✅ JOIN KAR LIYA — CHECK KARO", callback_data="home_joined")])
    return InlineKeyboardMarkup(rows)


JOIN_WALL_TEXT = (
    "**⚠️ Attention ⚠️**\n\n"
    "**📢 Is bot ko use karne ke liye pehle hamara**\n"
    "**updates channel join karna hoga!**\n\n"
    "**✅ Required Action:**\n"
    "**1. Neeche wale button se channel JOIN karo**\n"
    "**2. Wapas aakar `/start` dabao ya ✅ button dabao**\n\n"
    "**🚫 Access restricted until you join!**"
)


async def _send_join_wall(message):
    if START_IMAGE:
        try:
            await message.reply_photo(
                START_IMAGE, caption=JOIN_WALL_TEXT,
                parse_mode=enums.ParseMode.MARKDOWN, reply_markup=_join_kb())
            return
        except Exception:
            pass
    await message.reply_text(
        JOIN_WALL_TEXT, parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=_join_kb())


def _welcome_text(uid):
    has_session = uid in USER_CLIENTS or str(uid) in _load_json(SESSIONS_FILE, {})
    login = "🟢 Logged in" if has_session else "🔴 Not logged in — /login"
    access = "✅ Allowed" if _is_allowed(uid) else "⛔ Not allowed"
    return (
        "**VILLAN — Restricted Content Saver**\n\n"
        "*Save restricted content, fast and private.*\n\n"
        "**Features**\n"
        "• Restriction bypass\n"
        "• Fast file-ID copy, where supported\n"
        "• Bulk batch, up to 1000 items\n"
        "• Public + private channels\n"
        "• Rename, caption, watermark, thumbnail\n\n"
        f"Login: {login}\n"
        f"Access: {access}\n"
        f"Owner: {OWNER_USERNAME}"
    )


def _welcome_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👑 ADMIN PANEL", callback_data="home_admin")],
        [InlineKeyboardButton("📋 COMMANDS", callback_data="home_commands")],
        [InlineKeyboardButton("📜 DISCLAIMER", callback_data="home_disclaimer"),
         InlineKeyboardButton("⚠️ WARNINGS", callback_data="home_warnings")],
        [InlineKeyboardButton("❌ CLOSE", callback_data="home_close")],
    ])


def _page_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ BACK", callback_data="home_start"),
         InlineKeyboardButton("❌ CLOSE", callback_data="home_close")],
    ])


COMMANDS_TEXT = (
    "**VILLAN — Commands**\n\n"
    "`/login` — Sign in with your number\n"
    "`/logout` — Sign out\n"
    "`/batch` — Bulk extraction\n"
    "`/public` — Public channel fast copy\n"
    "`/clone` — Clone group folders\n"
    "`/create` — Naya group + folders banao\n"
    "`/organize` — Naam se folder + copy (no delete)\n"
    "`/settings` — Rename, caption, watermark\n"
    "`/plan` — Your plan\n"
    "`/buy` — Premium\n"
    "`/help` — Guide\n"
    "`/cancel` — Stop current task\n"
    "Send a link directly for a single file.\n\n"
    "**Owner:** `/paid <id> <days>` • `/allow` • `/unallow` • `/users`"
)

DISCLAIMER_TEXT = (
    "**Legal Notice**\n\n"
    "This bot stores no files and endorses no illegal activity. "
    "It provides tools only. You are responsible for your own use, "
    "local laws, and copyright compliance."
)

WARNINGS_TEXT = (
    "**Safety**\n\n"
    "• Never share your session string.\n"
    "• Use a secondary account for testing.\n"
    "• Respect copyright law.\n\n"
    "**Prohibited:** child exploitation material, copyright infringement, "
    "scams, phishing, illegal transactions."
)

ADMIN_TEXT = (
    "**Admin Panel**\n\n"
    "`/paid <user_id> <days>` — Grant premium\n"
    "`/allow <user_id>` — Lifetime access\n"
    "`/unallow <user_id>` — Remove access\n"
    "`/users` — Paid + allowed users\n\n"
    "Payment is manual (UPI). Plans activate here."
)


async def _send_welcome(message, uid):
    if START_IMAGE:
        try:
            await message.reply_photo(
                START_IMAGE, caption=_welcome_text(uid),
                parse_mode=enums.ParseMode.MARKDOWN, reply_markup=_welcome_kb())
            return
        except Exception:
            pass
    await message.reply_text(
        _welcome_text(uid), parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=_welcome_kb())


async def _edit_panel(query, text, kb):
    msg = query.message
    try:
        if msg.photo:
            await msg.edit_caption(
                text, parse_mode=enums.ParseMode.MARKDOWN, reply_markup=kb)
        else:
            await msg.edit_text(
                text, parse_mode=enums.ParseMode.MARKDOWN, reply_markup=kb)
    except Exception:
        pass


async def _handle_home_callbacks(client, query, uid, data):
    try:
        await query.answer()
    except Exception:
        pass
    if data == "home_close":
        try:
            await query.message.delete()
        except Exception:
            pass
        return
    if data == "home_joined":
        if await _has_joined(uid, force=True):
            await _edit_panel(query, _welcome_text(uid), _welcome_kb())
        else:
            try:
                await query.answer(
                    "❌ Abhi bhi join nahi dikh raha. Pehle channel join karo!",
                    show_alert=True)
            except Exception:
                pass
        return
    if data == "home_start":
        await _edit_panel(query, _welcome_text(uid), _welcome_kb())
        return
    if data == "home_commands":
        await _edit_panel(query, COMMANDS_TEXT, _page_kb())
        return
    if data == "home_disclaimer":
        await _edit_panel(query, DISCLAIMER_TEXT, _page_kb())
        return
    if data == "home_warnings":
        await _edit_panel(query, WARNINGS_TEXT, _page_kb())
        return
    if data == "home_admin":
        if _is_owner(uid):
            await _edit_panel(query, ADMIN_TEXT, _page_kb())
        else:
            try:
                await query.answer("⛔ Ye sirf owner ke liye hai.", show_alert=True)
            except Exception:
                pass
        return


# --- BOT HANDLERS ---
@app.on_message(filters.command("start") & filters.private)
async def start_handler(client, message):
    uid = message.chat.id
    print(f"⚡ Processing /start for user {uid}", flush=True)
    # v35: Force-join gate (UPDATES_CHANNEL set ho tabhi)
    # v46.1: /start user-initiated recheck hai, cache bypass karo
    if not await _has_joined(uid, force=True):
        await _send_join_wall(message)
        return
    await _send_welcome(message, uid)
    return



@app.on_message(filters.command("cancel") & filters.private)
async def cancel_handler(client, message):
    uid = message.chat.id
    hit = False
    if uid in USER_STATES:
        USER_STATES.pop(uid, None)
        hit = True
    if uid in PUBLIC_STATES:
        PUBLIC_STATES.pop(uid, None)
        hit = True
    if uid in LOGIN_STATES:
        await _cleanup_login(uid)
        hit = True
    if uid in SETTING_STATES:
        SETTING_STATES.pop(uid, None)
        hit = True
    if uid in CREATE_STATES:
        CREATE_STATES.pop(uid, None)
        hit = True
    if uid in ORGANIZE_STATES:
        ORGANIZE_STATES.pop(uid, None)
        hit = True
    ev = ORGANIZE_ACTIVE.get(uid)
    if ev is not None:
        ev.set()
        hit = True
    ev = ACTIVE_BATCHES.get(uid)
    if ev is not None:
        ev.set()
        hit = True
    ev = CLONE_ACTIVE.get(uid)
    if ev is not None:
        ev.set()
        hit = True
    if hit:
        await message.reply_text("🛑 Task cancelled.")
    else:
        await message.reply_text("Nothing active to cancel.")


# --- OWNER ADMIN COMMANDS ---
@app.on_message(filters.command("allow") & filters.private)
async def allow_handler(client, message):
    uid = message.chat.id
    if not _is_owner(uid):
        await message.reply_text("⛔ Owner only.")
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.reply_text(
            "Usage: `/allow <user_id>`\n(User ID @userinfobot se milegi)"
        )
        return
    target = int(parts[1])
    allowed = _load_allowed()
    allowed.add(target)
    _save_allowed(allowed)
    await message.reply_text(
        f"✅ `{target}` ko permission mil gayi.\nAb wo `/login` karke bot use kar sakta hai."
    )


@app.on_message(filters.command("unallow") & filters.private)
async def unallow_handler(client, message):
    uid = message.chat.id
    if not _is_owner(uid):
        await message.reply_text("⛔ Owner only.")
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.reply_text("Usage: `/unallow <user_id>`")
        return
    target = int(parts[1])
    allowed = _load_allowed()
    allowed.discard(target)
    _save_allowed(allowed)
    subs = _load_subs()
    if str(target) in subs:
        del subs[str(target)]
        _save_subs(subs)
    ev = ACTIVE_BATCHES.get(target)
    if ev is not None:
        ev.set()
    await _logout_user(target)
    await message.reply_text(f"🚫 `{target}` ki permission hata di gayi.")


@app.on_message(filters.command("users") & filters.private)
async def users_handler(client, message):
    uid = message.chat.id
    if not _is_owner(uid):
        await message.reply_text("⛔ Owner only.")
        return
    free = sorted(u for u in _get_allowed() if u != OWNER_ID)
    free_lines = "\n".join(f"• `{x}`" for x in free) or "—"
    subs = _load_subs()
    now = time.time()
    paid_lines = []
    for k in sorted((kk for kk in subs if kk.isdigit()), key=int):
        rem_days = (subs[k].get("expiry", 0) - now) / 86400
        if rem_days > 0:
            paid_lines.append(f"• `{k}` — `{rem_days:.1f}` din bache")
    paid_text = "\n".join(paid_lines) or "—"
    await message.reply_text(
        f"👥 **Free users:**\n{free_lines}\n\n💎 **Paid users:**\n{paid_text}"
    )


@app.on_message(filters.command("paid") & filters.private)
async def paid_handler(client, message):
    """Owner kisi ko time-based paid access de: /paid <user_id> <days>"""
    uid = message.chat.id
    if not _is_owner(uid):
        await message.reply_text("⛔ Owner only.")
        return
    parts = message.text.split()
    if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
        await message.reply_text(
            "Usage: `/paid <user_id> <days>`\nJaise: `/paid 123456789 30`"
        )
        return
    target, days = int(parts[1]), int(parts[2])
    if days <= 0 or days > 3650:
        await message.reply_text("❌ Days 1 se 3650 ke beech hone chahiye.")
        return
    subs = _load_subs()
    now = time.time()
    cur = subs.get(str(target), {}).get("expiry", 0)
    subs[str(target)] = {"type": "paid", "expiry": max(now, cur) + days * 86400}
    _save_subs(subs)
    card = await _premium_card(target, client)
    await message.reply_text(
        f"✅ Premium activate ho gaya!\n\n{card}"
    )
    try:
        await client.send_message(
            target,
            f"🎉 Tumhe **{days} din** ka premium access mil gaya hai!\n\n{card}\n\n"
            "`/login` karke shuru karo.",
            parse_mode=enums.ParseMode.MARKDOWN,
        )
    except Exception:
        pass


@app.on_message(filters.command("plan") & filters.private)
async def plan_handler(client, message):
    """User apna plan premium card style me dekhe."""
    uid = message.chat.id
    if not _is_allowed(uid):
        await message.reply_text("⛔ Access denied.")
        return
    card = await _premium_card(uid, client)
    has_session = uid in USER_CLIENTS or str(uid) in _load_json(SESSIONS_FILE, {})
    login = "🟢 Logged in" if has_session else "🔴 Not logged in (`/login` karo)"
    await message.reply_text(f"{card}\n\n🔑 {login}")


@app.on_message(filters.command("help") & filters.private)
async def help_handler(client, message):
    """Bot kaise use karein — poori guide."""
    help_text = (
        "**VILLAN — Guide**\n\n"
        "1. `/login` — Sign in with your number (OTP, then 2FA if set).\n"
        "2. `/batch` — Send the first link, then the last link.\n"
        "   Or send a range directly: `https://t.me/c/123456789/100-110`\n"
        "3. Files arrive one by one.\n\n"
        "`/settings` — Rename, caption, watermark\n"
        "`/plan` — Your plan • `/buy` — Premium • `/cancel` — Stop\n\n"
        f"Owner: `{OWNER_USERNAME}`"
    )
    await message.reply_text(help_text, parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.command("buy") & filters.private)
async def buy_handler(client, message):
    """Premium kharidne ke liye owner ka username dikhao."""
    await message.reply_text(
        "**VILLAN Premium**\n\n"
        f"Contact the owner: `{OWNER_USERNAME}`\n"
        "Message: `Hi, I want premium`\n"
        "Your plan activates after payment.",
        parse_mode=enums.ParseMode.MARKDOWN,
    )


@app.on_message(filters.command("logout") & filters.private)
async def logout_handler(client, message):
    uid = message.chat.id
    await _cleanup_login(uid)
    await _logout_user(uid)
    await message.reply_text(
        "👋 Logged out. Use /login to sign in again."
    )


@app.on_message(filters.command("batch") & filters.private)
async def batch_start_handler(client, message):
    if not await _has_joined(message.chat.id, force=True):
        await _send_join_wall(message)
        return
    if not _is_allowed(message.chat.id):
        await message.reply_text("⛔ Access denied.")
        return
    print(f"⚡ Processing /batch for user {message.chat.id}", flush=True)
    # Purani adhuri setting-input state ho to hatao, warna batch ka link
    # galti se setting handler me chala jayega
    SETTING_STATES.pop(message.chat.id, None)
    CLONE_STATES.pop(message.chat.id, None)
    PUBLIC_STATES.pop(message.chat.id, None)
    USER_STATES[message.chat.id] = {"step": "WAITING_FIRST_LINK"}
    await message.reply_text(
        "**VILLAN Batch**\n\n"
        "Send the first message link.\n\n"
        "Cancel: `/cancel`",
        parse_mode=enums.ParseMode.MARKDOWN,
    )


@app.on_message(filters.command("clone") & filters.private)
async def clone_start_handler(client, message):
    if not await _has_joined(message.chat.id, force=True):
        await _send_join_wall(message)
        return
    if not _is_allowed(message.chat.id):
        await message.reply_text("⛔ Access denied.")
        return
    print(f"⚡ Processing /clone for user {message.chat.id}", flush=True)
    USER_STATES.pop(message.chat.id, None)  # purana adhura batch state na atak jaye
    CREATE_STATES.pop(message.chat.id, None)  # v55: adhura /create state na atak jaye
    CLONE_STATES[message.chat.id] = {"step": "WAITING_LINK"}
    await message.reply_text(
        "**📂 VILLAN GROUP CLONE**\n"
        "**━━━━━━━━━━━━━━━**\n\n"
        "**Source group ka link bhejo**\n"
        "(jiske folders clone karne hain).\n\n"
        "Naya **private** group tumhare account se banega,\n"
        "usme wahi folders banenge, aur jo kuch hai\n"
        "sab same-to-same copy hoga.\n\n"
        "**❌ Cancel:** `/cancel`",
        parse_mode=enums.ParseMode.MARKDOWN,
    )


@app.on_message(filters.command("create") & filters.private)
async def create_start_handler(client, message):
    if not await _has_joined(message.chat.id, force=True):
        await _send_join_wall(message)
        return
    if not _is_allowed(message.chat.id):
        await message.reply_text("⛔ Access denied.")
        return
    print(f"⚡ Processing /create for user {message.chat.id}", flush=True)
    USER_STATES.pop(message.chat.id, None)  # purana adhura batch state na atak jaye
    CLONE_STATES.pop(message.chat.id, None)  # adhura /clone state na atak jaye
    PUBLIC_STATES.pop(message.chat.id, None)  # adhura /public state na atak jaye
    CREATE_STATES[message.chat.id] = {"step": "WAITING_GROUP_NAME"}
    await message.reply_text(
        "**📂 VILLAN GROUP CREATE**\n"
        "**━━━━━━━━━━━━━━━**\n\n"
        "**Group ka naam bhejo**\n"
        "(jis naam ka naya private group banana hai).\n\n"
        "Uske baad topics ke naam poochhunga,\n"
        "phir un saare naam se folders ban jayenge.\n\n"
        "Naya **private** group tumhare account se banega\n"
        "(bot se group nahi banta, isliye `/login` zaroori hai).\n\n"
        "**❌ Cancel:** `/cancel`",
        parse_mode=enums.ParseMode.MARKDOWN,
    )


@app.on_message(filters.command("organize") & filters.private)
async def organize_start_handler(client, message):
    if not await _has_joined(message.chat.id, force=True):
        await _send_join_wall(message)
        return
    if not _is_allowed(message.chat.id):
        await message.reply_text("⛔ Access denied.")
        return
    print(f"⚡ Processing /organize for user {message.chat.id}", flush=True)
    USER_STATES.pop(message.chat.id, None)
    CLONE_STATES.pop(message.chat.id, None)
    CREATE_STATES.pop(message.chat.id, None)
    PUBLIC_STATES.pop(message.chat.id, None)
    ORGANIZE_STATES[message.chat.id] = {"step": "WAITING_LINK"}
    await message.reply_text(
        "**📂 VILLAN ORGANIZE — naam se folder, copy, NO DELETE**\n"
        "**━━━━━━━━━━━━━━━**\n\n"
        "**Source group ka link bhejo**\n"
        "(jiske pdf / text / video / pic ko folders me bhejna hai).\n\n"
        "Bot naam padh ke topic samjhega, us naam ka folder banayega\n"
        "(pehle se ho to wahi use karega), aur us naam wali saari\n"
        "cheezein us folder me **COPY** kar dega — **original delete\n"
        "nahi hoga, wahi rahega.**\n\n"
        "Jaise: `Maths Chapter 1.pdf` -> `Maths` folder,\n"
        "`Physics Lecture 3.mp4` -> `Physics` folder.\n\n"
        "**❌ Cancel:** `/cancel`",
        parse_mode=enums.ParseMode.MARKDOWN,
    )


# --- v49 /public: PUBLIC channel ke liye alag FAST command ---
# Wajah: /batch me remove/rename/watermark settings lagi hon to _needs_file
# True ho jata hai aur public channel bhi download+upload (slow) ho jata
# hai, jabki public ka file_id bot ke paas khud hota hai aur instant bheja
# ja sakta hai. /public usi fast path ko force karta hai: prefetch download
# band, file_id se instant delivery. Caption/text ke remove/replace rules
# phir bhi lagenge (wo text-only hain); filename rename, watermark aur
# thumbnail /public me nahi lagenge — unhe download chahiye hota hai, aur
# wahi slow karta tha. Private link /public me mana hai (uske liye /batch).
PUBLIC_STATES = {}  # uid -> {"step": "WAITING_FIRST_LINK"/"WAITING_LAST_LINK", ...}


@app.on_message(filters.command("public") & filters.private)
async def public_start_handler(client, message):
    if not await _has_joined(message.chat.id, force=True):
        await _send_join_wall(message)
        return
    if not _is_allowed(message.chat.id):
        await message.reply_text("⛔ Access denied.")
        return
    print(f"⚡ Processing /public for user {message.chat.id}", flush=True)
    SETTING_STATES.pop(message.chat.id, None)
    CLONE_STATES.pop(message.chat.id, None)
    USER_STATES.pop(message.chat.id, None)
    PUBLIC_STATES[message.chat.id] = {"step": "WAITING_FIRST_LINK"}
    await message.reply_text(
        "**⚡ VILLAN PUBLIC FAST**\n"
        "**━━━━━━━━━━━━━━━**\n\n"
        "**STEP 1/2 — Public channel ka start link bhejo**\n\n"
        "Sirf **public** channel (jaise `t.me/channelname/123`) —\n"
        "private link ke liye `/batch` use karo.\n\n"
        "⚡ Files **file_id se instant** jayengi (download/upload nahi).\n"
        "Caption ke remove/replace rules lagenge, par **filename\n"
        "rename, watermark aur thumbnail is mode me nahi lagenge.**\n\n"
        "**❌ Cancel:** `/cancel`",
        parse_mode=enums.ParseMode.MARKDOWN,
    )


# --- LOGIN FLOW (har user apne number se login karega) ---
async def _cleanup_login(uid):
    st = LOGIN_STATES.pop(uid, None)
    if st and st.get("tmp"):
        try:
            await st["tmp"].disconnect()
        except Exception:
            pass


@app.on_message(filters.command("login") & filters.private)
async def login_start_handler(client, message):
    uid = message.chat.id
    if not await _has_joined(uid, force=True):
        await _send_join_wall(message)
        return
    if not _is_allowed(uid):
        await message.reply_text(
            "⛔ Access denied. Owner se contact karo."
        )
        return
    await _cleanup_login(uid)
    LOGIN_STATES[uid] = {"step": "PHONE"}
    await message.reply_text(
        "**📱 SECURE LOGIN**\n"
        "**━━━━━━━━━━━━━━━**\n"
        "**STEP 1/3 — Phone Number**\n\n"
        "**📞 Apna phone number bhejein**\n"
        "**country code ke saath**\n"
        "**Example:** `+919876543210`\n\n"
        "**❌ Cancel:** `/cancel`",
        parse_mode=enums.ParseMode.MARKDOWN,
    )


async def _finish_login(client, message, st):
    uid = message.chat.id
    tmp = st["tmp"]
    try:
        me = await tmp.get_me()
        sess = await tmp.export_session_string()
    except Exception as e:
        await message.reply_text(f"❌ Session banane me error: `{e}`")
        await _cleanup_login(uid)
        return
    # Purana session ho to hatao, naya save karo, aur isi connected
    # client ko is user ka persistent userbot banao (disconnect nahi!)
    old = USER_CLIENTS.get(uid)
    if old is not None and old is not tmp:
        try:
            await old.disconnect()
        except Exception:
            pass
    sessions = _load_json(SESSIONS_FILE, {})
    sessions[str(uid)] = sess
    _save_json(SESSIONS_FILE, sessions)
    USER_CLIENTS[uid] = tmp
    LOGIN_STATES.pop(uid, None)

    name = f"@{me.username}" if me.username else me.first_name
    await message.reply_text(
        "**✅ LOGIN SUCCESSFUL**\n"
        "**━━━━━━━━━━━━━━━**\n"
        f"**🎉 Welcome, {name}!**\n\n"
        "**🔗 Account successfully linked!**\n\n"
        "**📌 Next Steps:**\n"
        "**• /batch — Bulk extraction**\n"
        "**• Channel link — Direct extract**\n\n"
        "**⚠️ Security: OTP/password wale**\n"
        "**messages delete kar dein.**\n\n"
        "**🚪 Logout:** `/logout`",
        parse_mode=enums.ParseMode.MARKDOWN,
    )
    try:
        await message.delete()
    except Exception:
        pass


async def handle_login_step(client, message):
    uid = message.chat.id
    st = LOGIN_STATES.get(uid)
    if not st:
        return
    text = message.text.strip()

    if st["step"] == "PHONE":
        phone = re.sub(r"[\s\-()]", "", text)
        if not re.fullmatch(r"\+\d{7,15}", phone):
            await message.reply_text(
                "❌ Number sahi format me bhejo, jaise `+919876543210`"
            )
            return
        tmp = Client(
            f"login_{uid}", api_id=API_ID, api_hash=API_HASH, in_memory=True
        )
        try:
            await tmp.connect()
            sent = await tmp.send_code(phone)
        except FloodWait as e:
            try:
                await tmp.disconnect()
            except Exception:
                pass
            await message.reply_text(
                f"⏳ Telegram ne rok diya hai. `{e.value}` sec baad `/login` dobara try karo."
            )
            await _cleanup_login(uid)
            return
        except (PhoneNumberInvalid, PhoneNumberBanned) as e:
            try:
                await tmp.disconnect()
            except Exception:
                pass
            await message.reply_text(f"❌ Number me problem hai: `{e}`")
            await _cleanup_login(uid)
            return
        except Exception as e:
            try:
                await tmp.disconnect()
            except Exception:
                pass
            await message.reply_text(f"❌ Code bhejne me error: `{e}`")
            await _cleanup_login(uid)
            return
        st.update({
            "step": "OTP",
            "phone": phone,
            "phone_code_hash": sent.phone_code_hash,
            "tmp": tmp,
        })
        code_type = str(getattr(sent, "type", "") or "")
        print(f"[LOGIN] code sent -> type={code_type}", flush=True)
        if "SMS" in code_type:
            hint = ("**📱 Code SMS se bheja gaya hai —**\n"
                    "**apne SMS inbox me check karo.**")
        elif "CALL" in code_type:
            hint = "**📞 Code CALL par aayega — call ka wait karo.**"
        else:
            hint = ("**📱 Code SMS par NAHI aayega!**\n"
                    "**Telegram app ke andar 'Telegram' naam wali**\n"
                    "**official chat me aayega.**\n"
                    "**Jis device me ye number login hai,**\n"
                    "**wahan chat list me sabse upar dekho.**")
        await message.reply_text(
            "**📩 VERIFICATION**\n"
            "**━━━━━━━━━━━━━━━**\n"
            "**STEP 2/3 — OTP Code**\n\n"
            f"{hint}\n\n"
            "**🔑 Wo OTP code yahan bhejein.**\n\n"
            "**❌ Cancel:** `/cancel`",
            parse_mode=enums.ParseMode.MARKDOWN,
        )
        return

    if st["step"] == "OTP":
        otp = re.sub(r"\D", "", text)
        if len(otp) < 4:
            await message.reply_text("❌ Sahi OTP bhejo.")
            return
        tmp = st["tmp"]
        try:
            await tmp.sign_in(st["phone"], st["phone_code_hash"], otp)
        except PhoneCodeInvalid:
            await message.reply_text("❌ OTP galat hai, dobara bhejo.")
            return
        except PhoneCodeExpired:
            await message.reply_text(
                "❌ OTP expire ho gaya. `/login` se dobara shuru karo."
            )
            await _cleanup_login(uid)
            return
        except SessionPasswordNeeded:
            st["step"] = "PASSWORD"
            await message.reply_text(
                "**🔐 2-STEP VERIFICATION**\n"
                "**━━━━━━━━━━━━━━━**\n"
                "**STEP 3/3 — Password**\n\n"
                "**🛡️ Is account par 2-step verification**\n"
                "**enabled hai.**\n\n"
                "**🔑 Apna password bhejein.**\n\n"
                "**⚠️ Ye message baad me delete kar dein.**\n\n"
                "**❌ Cancel:** `/cancel`",
                parse_mode=enums.ParseMode.MARKDOWN,
            )
            return
        except FloodWait as e:
            await message.reply_text(
                f"⏳ `{e.value}` sec wait karke OTP dobara bhejo."
            )
            return
        except Exception as e:
            await message.reply_text(f"❌ Login fail: `{e}`")
            await _cleanup_login(uid)
            return
        await _finish_login(client, message, st)
        return

    if st["step"] == "PASSWORD":
        tmp = st["tmp"]
        try:
            await tmp.check_password(text)
        except PasswordHashInvalid:
            await message.reply_text("❌ Password galat hai, dobara bhejo.")
            return
        except Exception as e:
            await message.reply_text(f"❌ Login fail: `{e}`")
            await _cleanup_login(uid)
            return
        await _finish_login(client, message, st)
        return


# --- USER SETTINGS (customize panel) ---
SETTINGS_FILE = os.environ.get("SETTINGS_FILE", os.path.join(DATA_DIR, "user_settings.json"))
SETTING_STATES = {}  # uid -> {"action": ...}

DEFAULT_SETTINGS = {
    "chat_id": None,        # files kahan bheji jayein (None = khud ko)
    "rename_tag": None,     # filename me [Tag] prefix
    "caption": None,        # caption template ({filename} supported)
    "replace_words": [],    # [[old, new], ...]
    "remove_words": [],     # [w1, w2, ...]
    "thumb_file_id": None,  # custom thumbnail
    "pdf_wm": None,         # PDF watermark text
    "video_wm": None,       # video watermark text
    "upload_as_doc": False, # video ko document ki tarah bhejo
}


def get_settings(uid):
    all_s = _load_json(SETTINGS_FILE, {})
    s = dict(DEFAULT_SETTINGS)
    s.update(all_s.get(str(uid), {}))
    return s


def save_settings(uid, s):
    all_s = _load_json(SETTINGS_FILE, {})
    all_s[str(uid)] = s
    _save_json(SETTINGS_FILE, all_s)


def update_setting(uid, key, value):
    s = get_settings(uid)
    s[key] = value
    save_settings(uid, s)


def reset_settings(uid):
    all_s = _load_json(SETTINGS_FILE, {})
    all_s.pop(str(uid), None)
    _save_json(SETTINGS_FILE, all_s)


def settings_keyboard(uid):
    s = get_settings(uid)
    on = lambda v: "✅" if v else ""
    um = "Document" if s["upload_as_doc"] else "Auto"
    kb = [
        [InlineKeyboardButton("Set Chat ID", callback_data="st_chat"),
         InlineKeyboardButton("Set Rename Tag", callback_data="st_rename")],
        [InlineKeyboardButton("Caption", callback_data="st_caption"),
         InlineKeyboardButton("Replace Words", callback_data="st_replace")],
        [InlineKeyboardButton("Remove Words", callback_data="st_remove"),
         InlineKeyboardButton("Reset", callback_data="st_reset")],
        [InlineKeyboardButton("Session Login", callback_data="st_login"),
         InlineKeyboardButton("Logout", callback_data="st_logout")],
        [InlineKeyboardButton(f"{on(s['thumb_file_id'])} Set Thumbnail".strip(),
                              callback_data="st_thumb"),
         InlineKeyboardButton("Remove Thumbnail", callback_data="st_unthumb")],
        [InlineKeyboardButton(f"{on(s['pdf_wm'])} PDF Wtmrk".strip(),
                              callback_data="st_pdfwm"),
         InlineKeyboardButton(f"{on(s['video_wm'])} Video Wtmrk".strip(),
                              callback_data="st_videowm")],
        [InlineKeyboardButton(f"Upload Method: {um}",
                              callback_data="st_uploadmethod")],
        [InlineKeyboardButton("Report Errors ↗", callback_data="st_report")],
    ]
    return InlineKeyboardMarkup(kb)


def apply_text_rules(text, s):
    """Caption/text par remove_words -> replace_words lagao (case-insensitive).
    v43: pehle ye rules sirf filename par lagte the, isliye caption dekh
    kar lagta tha 'kaam nahi kar raha' — ab caption/text par bhi lagenge."""
    out = text or ""
    for w in s.get("remove_words", []):
        if w:
            out = re.sub(re.escape(w), "", out, flags=re.IGNORECASE)
    for pair in s.get("replace_words", []):
        if isinstance(pair, (list, tuple)) and len(pair) == 2 and pair[0]:
            out = re.sub(re.escape(pair[0]), lambda m: pair[1], out,
                         flags=re.IGNORECASE)
    out = re.sub(r" {2,}", " ", out).strip()
    return out


def _parse_remove_words(text):
    """v49: Remove words ek saath kai — comma, space ya nayi line, kisi se
    bhi alag karke bhejo. ('x265, PSA, 1080p' ya har line par ek word ya
    space se alag words — teeno ek hi list banenge.) Duplicates hatake,
    order wahi rahega, max 50 words."""
    import re as _re
    seen = set()
    words = []
    for w in _re.split(r"[\s,]+", text or ""):
        w = w.strip()
        if not w:
            continue
        key = w.lower()
        if key in seen:
            continue
        seen.add(key)
        words.append(w)
        if len(words) >= 50:
            break
    return words


def apply_filename_rules(filename, s):
    """remove_words -> replace_words -> rename_tag lagao (filename par).
    v43: matching case-insensitive — 'JOIN' bhi 'join' rule se hatega."""
    name, ext = os.path.splitext(filename)
    name = apply_text_rules(name, s)
    name = re.sub(r"\.{2,}", ".", name)
    name = re.sub(r"\s{2,}", " ", name).strip(" .")
    tag = s.get("rename_tag")
    if tag and not name.startswith(f"[{tag}]"):
        name = f"[{tag}] {name}"
    name = re.sub(r'[\\/*?:"<>|]', "", name).strip()
    if not name:
        name = "file"
    return name + ext


def apply_pdf_watermark(pdf_path, text, tmpdir):
    """Har page par tircha text watermark lagao (pypdf + reportlab)."""
    try:
        from pypdf import PdfReader, PdfWriter
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import A4
        import io
        reader = PdfReader(pdf_path)
        if not reader.pages:
            return pdf_path
        packet = io.BytesIO()
        c = canvas.Canvas(packet, pagesize=A4)
        c.setFont("Helvetica-Bold", 36)
        c.setFillGray(0.55)
        c.saveState()
        c.translate(297, 420)
        c.rotate(45)
        c.drawCentredString(0, 0, text[:60])
        c.restoreState()
        c.save()
        packet.seek(0)
        wm_page = PdfReader(packet).pages[0]
        writer = PdfWriter()
        for page in reader.pages:
            page.merge_page(wm_page)
            writer.add_page(page)
        out = os.path.join(tmpdir, "wm_" + os.path.basename(pdf_path))
        with open(out, "wb") as f:
            writer.write(f)
        return out
    except Exception as e:
        print(f"PDF watermark failed: {e}", flush=True)
        return pdf_path


def apply_video_watermark(video_path, text, tmpdir):
    """Video par text watermark jalao (ffmpeg). Slow ho sakta hai."""
    try:
        import subprocess
        if not shutil.which("ffmpeg"):
            print("ffmpeg nahi mila, video watermark skip", flush=True)
            return video_path
        out = os.path.join(tmpdir, "wm_" + os.path.basename(video_path))
        safe = re.sub(r"[^a-zA-Z0-9 _-]", "", text)[:40] or "VILLAN"
        cmd = ["ffmpeg", "-y", "-i", video_path, "-vf",
               f"drawtext=text='{safe}':fontsize=28:fontcolor=white@0.85:x=20:y=20",
               "-c:a", "copy", "-preset", "ultrafast", out]
        subprocess.run(cmd, capture_output=True, timeout=7200, check=True)
        if os.path.exists(out) and os.path.getsize(out) > 0:
            return out
        return video_path
    except Exception as e:
        print(f"Video watermark failed: {e}", flush=True)
        return video_path


@app.on_message(filters.command("settings") & filters.private)
async def settings_handler(client, message):
    uid = message.chat.id
    if not _is_allowed(uid):
        await message.reply_text("⛔ Access denied.")
        return
    await message.reply_text(
        "⚙️ Settings",
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=settings_keyboard(uid),
    )


@app.on_callback_query()
async def settings_callbacks(client, query):
    uid = query.from_user.id
    data = query.data or ""
    if data.startswith("home_"):
        await _handle_home_callbacks(client, query, uid, data)
        return
    if not data.startswith("st_"):
        return
    if not _is_allowed(uid):
        try:
            await query.answer("⛔ Permission nahi hai.", show_alert=True)
        except Exception:
            pass
        return
    try:
        await query.answer()
    except Exception:
        pass

    if data == "st_chat":
        SETTING_STATES[uid] = {"action": "set_chat_id"}
        await query.message.reply_text(
            "📨 **Set Chat ID**\n\nFiles kis chat me bheji jayein, wo ID bhejo\n"
            "(jaise `-1001234567890`).\nWapas apne paas bhejne ke liye `off` bhejo.",
            parse_mode=enums.ParseMode.MARKDOWN)
    elif data == "st_rename":
        SETTING_STATES[uid] = {"action": "set_rename"}
        await query.message.reply_text(
            "🏷️ **Set Rename Tag**\n\nEk tag bhejo, file ka naam `[Tag] naam.ext` hoga.\n"
            "Hatane ke liye `off` bhejo.",
            parse_mode=enums.ParseMode.MARKDOWN)
    elif data == "st_caption":
        SETTING_STATES[uid] = {"action": "set_caption"}
        await query.message.reply_text(
            "💬 **Caption**\n\nYe text original caption ke NICHE add hoga (purana caption hataya nahi jayega). `{filename}` likhne par file ka naam aayega.\n"
            "Jaise: `🔥 {filename}`\nHatane ke liye `off` bhejo.",
            parse_mode=enums.ParseMode.MARKDOWN)
    elif data == "st_replace":
        SETTING_STATES[uid] = {"action": "set_replace"}
        await query.message.reply_text(
            "🔄 **Replace Words**\n\nHar line par `purana | naya` bhejo. Ye caption, text aur filename — teeno par lagega.\n"
            "Jaise:\n`x265 | x264`\n`1080p | 720p`\nHatane ke liye `off` bhejo.",
            parse_mode=enums.ParseMode.MARKDOWN)
    elif data == "st_remove":
        SETTING_STATES[uid] = {"action": "set_remove"}
        await query.message.reply_text(
            "🧹 **Remove Words**\n\nEk saath KAI words bhej sakte ho — comma, space ya har line par alag word, jo caption, text aur filename se hatane hain.\n"
            "Jaise: `x265, PSA, 1080p`\nHatane ke liye `off` bhejo.",
            parse_mode=enums.ParseMode.MARKDOWN)
    elif data == "st_reset":
        reset_settings(uid)
        await query.message.reply_text("♻️ Sab settings reset ho gayi!",
                                       reply_markup=settings_keyboard(uid))
    elif data == "st_login":
        await _cleanup_login(uid)
        LOGIN_STATES[uid] = {"step": "PHONE"}
        await query.message.reply_text(
            "**📱 SECURE LOGIN**\n"
            "**━━━━━━━━━━━━━━━**\n"
            "**STEP 1/3 — Phone Number**\n\n"
            "**📞 Apna phone number bhejein**\n"
            "**country code ke saath**\n"
            "**Example:** `+919876543210`",
            parse_mode=enums.ParseMode.MARKDOWN)
    elif data == "st_logout":
        await _cleanup_login(uid)
        await _logout_user(uid)
        await query.message.reply_text("👋 Logged out. Use /login to sign in again.")
    elif data == "st_thumb":
        SETTING_STATES[uid] = {"action": "set_thumb"}
        await query.message.reply_text(
            "🖼️ **Set Thumbnail**\n\nThumbnail ke liye ek **photo** bhejo.",
            parse_mode=enums.ParseMode.MARKDOWN)
    elif data == "st_unthumb":
        update_setting(uid, "thumb_file_id", None)
        await query.message.reply_text("✅ Thumbnail hata diya.",
                                       reply_markup=settings_keyboard(uid))
    elif data == "st_pdfwm":
        SETTING_STATES[uid] = {"action": "set_pdfwm"}
        await query.message.reply_text(
            "📄 **PDF Watermark**\n\nWatermark text bhejo (PDF ke har page par tircha lagega).\n"
            "Hatane ke liye `off` bhejo.",
            parse_mode=enums.ParseMode.MARKDOWN)
    elif data == "st_videowm":
        SETTING_STATES[uid] = {"action": "set_videowm"}
        await query.message.reply_text(
            "🎬 **Video Watermark**\n\nWatermark text bhejo (video par likha aayega).\n"
            "⚠️ Note: video dobara encode hogi, isliye **slow** hoga.\n"
            "Hatane ke liye `off` bhejo.",
            parse_mode=enums.ParseMode.MARKDOWN)
    elif data == "st_uploadmethod":
        s = get_settings(uid)
        update_setting(uid, "upload_as_doc", not s["upload_as_doc"])
        mode = "Document" if not s["upload_as_doc"] else "Auto"
        await query.message.reply_text(f"✅ Upload method: **{mode}**",
                                       parse_mode=enums.ParseMode.MARKDOWN,
                                       reply_markup=settings_keyboard(uid))
    elif data == "st_report":
        SETTING_STATES[uid] = {"action": "report"}
        await query.message.reply_text(
            "🐞 **Report Errors**\n\nKya problem aayi? Detail me likh ke bhejo, owner tak pahuncha dunga.",
            parse_mode=enums.ParseMode.MARKDOWN)


async def handle_setting_input(client, message):
    uid = message.chat.id
    st = SETTING_STATES.get(uid)
    if not st:
        return
    action = st["action"]
    text = (message.text or "").strip()

    async def done(msg):
        SETTING_STATES.pop(uid, None)
        await message.reply_text(msg, parse_mode=enums.ParseMode.MARKDOWN,
                                 reply_markup=settings_keyboard(uid))

    if action == "set_chat_id":
        if text.lower() == "off":
            update_setting(uid, "chat_id", None)
            await done("✅ **Chat ID hata diya. Files ab tumhe milengi.**")
            return
        try:
            cid = int(text)
        except ValueError:
            await message.reply_text("❌ Sahi ID bhejo, jaise `-1001234567890`.")
            return
        update_setting(uid, "chat_id", cid)
        await done(f"✅ **Chat ID set: `{cid}`**\n**Ab files wahi bheji jayengi.**")
    elif action == "set_rename":
        if text.lower() == "off":
            update_setting(uid, "rename_tag", None)
            await done("✅ **Rename tag hata diya.**")
        else:
            update_setting(uid, "rename_tag", text[:40])
            await done(f"✅ **Tag set: `[{text[:40]}]`**")
    elif action == "set_caption":
        if text.lower() == "off":
            update_setting(uid, "caption", None)
            await done("✅ **Caption hata diya.**")
        else:
            update_setting(uid, "caption", text[:500])
            await done("✅ **Caption set ho gaya.**")
    elif action == "set_replace":
        if text.lower() == "off":
            update_setting(uid, "replace_words", [])
            await done("✅ **Replace rules hata diye.**")
        else:
            pairs = []
            for line in text.split("\n"):
                if "|" in line:
                    old, new = line.split("|", 1)
                    old, new = old.strip(), new.strip()
                    if old:
                        pairs.append([old, new])
            update_setting(uid, "replace_words", pairs)
            await done(f"✅ **`{len(pairs)}` replace rules set.**")
    elif action == "set_remove":
        if text.lower() == "off":
            update_setting(uid, "remove_words", [])
            await done("✅ **Remove words hata diye.**")
        else:
            words = _parse_remove_words(text)
            update_setting(uid, "remove_words", words)
            shown = ", ".join(f"`{w}`" for w in words) if words else "koi nahi"
            await done(f"✅ **`{len(words)}` words hatane ke liye set:** {shown}")
    elif action == "set_pdfwm":
        if text.lower() == "off":
            update_setting(uid, "pdf_wm", None)
            await done("✅ **PDF watermark hata diya.**")
        else:
            update_setting(uid, "pdf_wm", text[:60])
            await done(f"✅ **PDF watermark set: `{text[:60]}`**")
    elif action == "set_videowm":
        if text.lower() == "off":
            update_setting(uid, "video_wm", None)
            await done("✅ **Video watermark hata diya.**")
        else:
            update_setting(uid, "video_wm", text[:40])
            await done(f"✅ **Video watermark set: `{text[:40]}`**\n⚠️ **Video slow encode hogi.**")
    elif action == "report":
        if OWNER_ID:
            try:
                await client.send_message(
                    OWNER_ID,
                    f"🐞 **Error report** from `{uid}`:\n\n{text[:1000]}",
                    parse_mode=enums.ParseMode.MARKDOWN)
                await done("✅ **Report owner ko bhej diya.**")
            except Exception:
                await done("❌ **Report nahi bheja ja saka.**")
        else:
            await done("❌ **Owner set nahi hai.**")


@app.on_message(filters.photo & filters.private)
async def thumb_photo_handler(client, message):
    uid = message.chat.id
    st = SETTING_STATES.get(uid)
    if not st or st.get("action") != "set_thumb":
        return
    if not _is_allowed(uid):
        SETTING_STATES.pop(uid, None)
        await message.reply_text("⛔ Access denied.")
        return
    SETTING_STATES.pop(uid, None)
    file_id = message.photo[-1].file_id
    update_setting(uid, "thumb_file_id", file_id)
    await message.reply_text("✅ Thumbnail set ho gaya!",
                             reply_markup=settings_keyboard(uid))


@app.on_message(
    filters.text
    & filters.private
    & ~filters.command(
        ["start", "batch", "cancel", "login", "logout", "settings",
         "allow", "unallow", "users", "paid", "plan", "help", "buy"]
    )
)
async def message_dispatcher(client, message):
    user_id = message.chat.id
    text = message.text.strip()

    # v40: Force-join gate direct links par bhi (pehle sirf /start, /batch,
    # /login par tha — seedha link bhej kar bypass ho jata tha). Cached hai,
    # isliye har message par API call nahi jaati.
    if not await _has_joined(user_id):
        await _send_join_wall(message)
        return

    # Login flow chal raha ho to wahi handle karega
    if user_id in LOGIN_STATES:
        if not _is_allowed(user_id):
            await _cleanup_login(user_id)
            await message.reply_text("⛔ Access denied.")
            return
        await handle_login_step(client, message)
        return

    # Permission gate: sirf allowed users
    if not _is_allowed(user_id):
        await message.reply_text(
            "⛔ Access denied. Contact the owner."
        )
        return

    # Settings input flow (koi setting ka jawab aa raha ho)
    if user_id in SETTING_STATES:
        await handle_setting_input(client, message)
        return

    # Clone flow: group link ka jawab aa raha ho (har baar bot me link manga jata hai)
    if user_id in CLONE_STATES and CLONE_STATES[user_id].get("step") == "WAITING_LINK":
        src_chat = _clone_parse_link(text)
        if src_chat is None:
            await message.reply_text("❌ **Invalid link!** Source group ka link bhejo "
                                     "(jaise `https://t.me/c/1234567890` ya `@groupname`).")
            return
        CLONE_STATES.pop(user_id, None)
        status_msg = await message.reply_text(
            "📂 **Clone Started!** Folders aur messages nikal raha hun (ek-ek karke)..."
        )
        await run_clone(client, user_id, src_chat, status_msg)
        return

    # Create flow: pehle group naam, phir topics naam (ek hi message me)
    if user_id in CREATE_STATES:
        step = CREATE_STATES[user_id].get("step")
        if step == "WAITING_GROUP_NAME":
            group_name = text.strip()[:120]
            if not group_name:
                await message.reply_text("❌ Group ka naam khali hai. Dobara naam bhejo.")
                return
            CREATE_STATES[user_id] = {"step": "WAITING_TOPICS", "group_name": group_name}
            await message.reply_text(
                "**📂 Group naam lock ho gaya:**\n"
                f"`{group_name}`\n\n"
                "**Ab topics ke naam bhejo — ek hi message me,**\n"
                "har naam ek nayi line par, ya comma se alag karke.\n"
                "Jaise:\n"
                "`Maths`\n`Physics`\n`Chemistry`\n"
                "ya: `Maths, Physics, Chemistry`\n\n"
                "Bhejte hi group + saare folders ban jayenge.\n\n"
                "**❌ Cancel:** `/cancel`",
                parse_mode=enums.ParseMode.MARKDOWN,
            )
            return
        if step == "WAITING_TOPICS":
            topics = _create_parse_topics(text)
            if not topics:
                await message.reply_text(
                    "❌ Koi topic naam nahi mila. Topics ke naam dobara bhejo —\n"
                    "har naam ek nayi line par, ya comma se alag karke.")
                return
            group_name = CREATE_STATES[user_id].get("group_name", "New Group")
            CREATE_STATES.pop(user_id, None)
            if len(topics) > CREATE_MAX_TOPICS:
                topics = topics[:CREATE_MAX_TOPICS]
            status_msg = await message.reply_text(
                f"📂 **Create Started!**\nGroup: `{group_name}`\n"
                f"Folders: `{len(topics)}` — ek-ek karke bana raha hun..."
            )
            await run_create(client, user_id, group_name, topics, status_msg)
            return

    # Organize flow: group link ka jawab aa raha ho
    if user_id in ORGANIZE_STATES and ORGANIZE_STATES[user_id].get("step") == "WAITING_LINK":
        src_chat = _clone_parse_link(text)
        if src_chat is None:
            await message.reply_text("❌ **Invalid link!** Source group ka link bhejo "
                                     "(jaise `https://t.me/c/1234567890` ya `@groupname`).")
            return
        ORGANIZE_STATES.pop(user_id, None)
        status_msg = await message.reply_text(
            "📂 **Organize Started!** Naam padh ke folders bana raha hun, "
            "phir saari cheezein un folders me copy karunga (delete nahi)..."
        )
        await run_organize(client, user_id, src_chat, status_msg)
        return

    if (
        user_id in PUBLIC_STATES
        and PUBLIC_STATES[user_id].get("step") == "WAITING_FIRST_LINK"
    ):
        chat_id, start_id = parse_tg_link(text)
        if not chat_id or not start_id:
            await message.reply_text("❌ Invalid link. Send a valid public channel link.")
            return
        if isinstance(chat_id, int):
            # t.me/c/... = private link — /public sirf public ke liye hai
            await message.reply_text(
                "❌ **Ye private channel ka link hai!**\n"
                "`/public` sirf public channel ke liye hai.\n"
                "Private ke liye `/batch` use karo, ya public channel ka link bhejo.")
            return

        PUBLIC_STATES[user_id] = {
            "step": "WAITING_LAST_LINK",
            "chat_id": chat_id,
            "start_id": start_id,
        }
        await message.reply_text(
            "**VILLAN Public Fast**\n\n"
            f"Start locked: `{start_id}`\n"
            "Send the last message link.\n\n"
            "Cancel: `/cancel`",
            parse_mode=enums.ParseMode.MARKDOWN,
        )
        return

    elif (
        user_id in PUBLIC_STATES
        and PUBLIC_STATES[user_id].get("step") == "WAITING_LAST_LINK"
    ):
        chat_id, end_id = parse_tg_link(text)
        if not chat_id or not end_id:
            await message.reply_text("❌ Invalid link. Send a valid public channel link.")
            return
        if isinstance(chat_id, int):
            await message.reply_text(
                "❌ **Ye private channel ka link hai!** `/public` sirf public ke liye hai.")
            return

        state = PUBLIC_STATES.pop(user_id)
        if state["chat_id"] != chat_id:
            await message.reply_text("❌ Links ek hi channel ke hone chahiye.")
            return

        start_id = state["start_id"]
        if start_id > end_id:
            start_id, end_id = end_id, start_id

        total_files = end_id - start_id + 1
        if total_files > BATCH_LIMIT:
            await message.reply_text(f"⚠️ Max {BATCH_LIMIT} posts allow hain.")
            return

        status_msg = await message.reply_text(
            f"Public fast started — `{total_files}` items."
        )
        await run_batch(client, user_id, chat_id, start_id, end_id, status_msg,
                        public_fast=True)
        return

    if (
        user_id in USER_STATES
        and USER_STATES[user_id].get("step") == "WAITING_FIRST_LINK"
    ):
        chat_id, start_id = parse_tg_link(text)
        if not chat_id or not start_id:
            await message.reply_text("❌ Invalid link. Send a valid channel link.")
            return

        USER_STATES[user_id] = {
            "step": "WAITING_LAST_LINK",
            "chat_id": chat_id,
            "start_id": start_id,
        }
        await message.reply_text(
            "**VILLAN Batch**\n\n"
            f"Start locked: `{start_id}`\n"
            "Send the last message link.\n\n"
            "Cancel: `/cancel`",
            parse_mode=enums.ParseMode.MARKDOWN,
        )
        return

    elif (
        user_id in USER_STATES
        and USER_STATES[user_id].get("step") == "WAITING_LAST_LINK"
    ):
        chat_id, end_id = parse_tg_link(text)
        if not chat_id or not end_id:
            await message.reply_text("❌ Invalid link. Send a valid channel link.")
            return

        state = USER_STATES.pop(user_id)
        if state["chat_id"] != chat_id:
            await message.reply_text("❌ Links ek hi channel ke hone chahiye.")
            return

        start_id = state["start_id"]
        if start_id > end_id:
            start_id, end_id = end_id, start_id

        total_files = end_id - start_id + 1
        if total_files > BATCH_LIMIT:
            await message.reply_text(f"⚠️ Max {BATCH_LIMIT} posts allow hain.")
            return

        status_msg = await message.reply_text(
            f"Batch started — `{total_files}` items."
        )
        await run_batch(client, user_id, chat_id, start_id, end_id, status_msg)
        return

    range_regex = (
        r"https://t\.me/(?:c/(\d+)|([a-zA-Z0-9_]+))/(?:(\d+)/)?(\d+)(?:-(\d+))?"
    )
    match = re.search(range_regex, text)
    if match:
        chat_id = int(f"-100{match.group(1)}") if match.group(1) else match.group(2)
        start_id = int(match.group(4))
        end_id = int(match.group(5)) if match.group(5) else start_id

        if start_id > end_id:
            start_id, end_id = end_id, start_id

        total = end_id - start_id + 1
        if total > BATCH_LIMIT:
            await message.reply_text(f"⚠️ Max {BATCH_LIMIT} posts allow hain.")
            return

        status_msg = await message.reply_text(
            f"Processing `{total}` items…"
        )
        await run_batch(client, user_id, chat_id, start_id, end_id, status_msg)



# --- v54: DEPLOY BROADCAST ---
# Har deploy (= har bot restart) par sab users ko ek short message jata hai
# taaki unhe pata chale bot update hua hai aur purana batch restart karna hai.
# Band karna ho to Koyeb env me UPDATE_BROADCAST=0 set karo (default ON).
# Gap: users ke beech itne second ka gap (default 1s) taaki broadcast se
# FloodWait na lage. Ye background task me chalta hai, bot start hone me
# ya chal rahe batch me der nahi karta.
UPDATE_BROADCAST = os.environ.get("UPDATE_BROADCAST", "1").strip() != "0"
UPDATE_BROADCAST_GAP = float(os.environ.get("UPDATE_BROADCAST_GAP", "1"))


def _broadcast_user_ids():
    """Sab jaane-pehchane users: allowed + paid subs + login sessions + topics."""
    ids = set()
    try:
        ids |= _get_allowed()
    except Exception:
        pass
    for _src in (_load_subs(), _load_json(SESSIONS_FILE, {}),
                 _load_json(TOPICS_FILE, {})):
        try:
            for _k in _src.keys():
                if str(_k).isdigit():
                    ids.add(int(_k))
        except Exception:
            pass
    ids.discard(0)
    return sorted(ids)


async def _broadcast_update():
    if not UPDATE_BROADCAST:
        print("[BROADCAST] OFF (UPDATE_BROADCAST=0), koi message nahi bheja",
              flush=True)
        return
    users = _broadcast_user_ids()
    print(f"[BROADCAST] {len(users)} users ko BOT UPDATED message bhej raha hun",
          flush=True)
    sent = failed = 0
    text = (
        "<b>\U0001f504 BOT UPDATED</b>\n\n"
        "<b>Please restart your download.</b>\n"
        "<b>Agar tumhara batch ruk gaya ho to dobara /batch bhejo "
        "- naya version live hai.</b>"
    )
    for uid in users:
        try:
            await app.send_message(uid, text, parse_mode=enums.ParseMode.HTML)
            sent += 1
        except FloodWait as e:
            try:
                await asyncio.sleep(int(e.value) + 2)
                await app.send_message(uid, text,
                                       parse_mode=enums.ParseMode.HTML)
                sent += 1
            except Exception as ex:
                failed += 1
                print(f"[BROADCAST] fail {uid}: {ex}", flush=True)
        except Exception as e:
            failed += 1
            print(f"[BROADCAST] fail {uid}: {e}", flush=True)
        await asyncio.sleep(UPDATE_BROADCAST_GAP)
    print(f"[BROADCAST] done: sent={sent} failed={failed}", flush=True)


# --- MAIN ENGINE ---
async def main():
    await start_web_server()

    print("🚀 Starting Bot Client...", flush=True)
    await app.start()

    me = await app.get_me()
    try:
        await app.set_bot_commands([
            BotCommand("start", "Bot start karo"),
            BotCommand("login", "Apne number se login"),
            BotCommand("logout", "Session khatam karo"),
            BotCommand("batch", "Bulk extraction (range)"),
            BotCommand("public", "Public channel fast copy"),
            BotCommand("clone", "Group folders clone karo"),
            BotCommand("create", "Naya group + folders banao"),
            BotCommand("organize", "Naam se folder + copy (no delete)"),
            BotCommand("settings", "Customize panel"),
            BotCommand("plan", "Apna plan dekho"),
            BotCommand("buy", "Premium kharido"),
            BotCommand("help", "Usage guide"),
            BotCommand("cancel", "Chal raha task roko"),
        ])
        print("👉 Bot command menu set ho gaya", flush=True)
    except Exception as e:
        print(f"[MENU] set_bot_commands fail: {e}", flush=True)
    print("========================================", flush=True)
    print(f"👉 BOT LISTENING AS: @{me.username}", flush=True)
    print(f"👉 VERSION: {VERSION}", flush=True)
    print("👉 MODE: sequential delivery + PIPELINED download (N upload + N+1 download)", flush=True)
    print(f"👉 OWNER_ID: {OWNER_ID if OWNER_ID else 'not set (open mode)'}", flush=True)
    print(f"👉 PACING: FAST_GAP={FAST_GAP}s SLOW_GAP={SLOW_GAP}s PRIVATE_GAP={PRIVATE_GAP}s "
          f"FETCH_CHUNK={FETCH_CHUNK}", flush=True)
    print(f"👉 MULTI_DL: {MULTI_PARTS} parts/file (ek file ke parts parallel, order sequential)", flush=True)
    if not OWNER_ID and SESSION_STRING:
        print("⚠️ OWNER_ID set nahi hai — owner userbot fallback BAND hai "
              "(strangers owner account par download na chala payen). Apni "
              "ID @userinfobot se leke OWNER_ID me set karo.", flush=True)
    print("========================================", flush=True)

    # Redeploy-proof data: backup channel se missing JSON wapas lao
    if BACKUP_CHAT_ID:
        print(f"👉 BACKUP_CHAT_ID: {BACKUP_CHAT_ID} (redeploy-proof backup ON)", flush=True)
        await _backup_restore_all()
    else:
        print("👉 BACKUP_CHAT_ID: not set (bina volume ke redeploy par data udega)", flush=True)

    # v54: har deploy par sab users ko update message (background me,
    # taaki bot ke start hone me der na lage; chal raha batch nahi rukta)
    asyncio.create_task(_broadcast_update())

    await idle()
    await app.stop()
    for c in list(USER_CLIENTS.values()):
        try:
            await c.stop()
        except Exception:
            pass
    if user is not None:
        try:
            await user.stop()
        except Exception:
            pass


if __name__ == "__main__":
    # NOTE: asyncio.run() NAYA loop banata hai, jabki Client ne upar wala
    # loop capture kar rakha hai. app.run() current (wahi) loop use karta
    # hai, tabhi handlers aur updates sahi chalenge.
    app.run(main())
