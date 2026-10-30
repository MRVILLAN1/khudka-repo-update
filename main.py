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
VERSION = "v71"

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
BATCH_RESUME_FILE = os.path.join(DATA_DIR, "batch_resume.json")
CHANNELS_FILE = os.path.join(DATA_DIR, "channels.json")
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
                  "user_settings.json", "user_topics.json", "channels.json")
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
        "channels.json": CHANNELS_FILE,
        "batch_resume.json": BATCH_RESUME_FILE,
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


def _is_premium(uid: int) -> bool:
    """Premium = paid subscription active hai. Owner hamesha premium."""
    if _is_owner(uid):
        return True
    return _sub_remaining(uid) > 0


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
                             file_path=None, caption="", info=None, thumb=None, prog_msg=None):
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
            if prog_msg is not None:
                _up_start = time.time()
                _up_last = [time.time()]
                uploaded = await client.save_file(
                    file_path, progress=progress_bar,
                    progress_args=(
                        prog_msg, _up_start, _up_last,
                        f"🚀 Uploading • ID `{info.get('msg_id')}`"))
            else:
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


async def _botapi_create_topic_for_chat(chat_id, title):
    if not BOT_TOKEN or not chat_id:
        return None
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/createForumTopic"
        data = {"chat_id": str(chat_id), "name": (title or "Folder")[:100]}
        sess = await _http()
        async with sess.post(url, data=data, timeout=30) as resp:
            result = await resp.json()
        if result.get("ok"):
            tid = result["result"].get("message_thread_id")
            print(f"[FBATCH] Bot API se topic banaya: {tid} in {chat_id}", flush=True)
            return tid
        print(f"[FBATCH] Bot API topic fail: {result}", flush=True)
    except Exception as e:
        print(f"[FBATCH] Bot API topic exception: {e}", flush=True)
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


# --- v58 RESUME: batch rukne par wahi se dobara shuru ---
# "jha se batch ruka" — resume hamesha aakhri COMPLETE hui file ke BAAD
# wali ID se shuru hoga, pehli file se nahi. Har file complete hone par
# batch_resume.json me last_done_id save hota hai (local file, har file
# par backup channel spam nahi hota — backup sirf batch rukne/poora hone
# par ek baar upload hota hai, taaki redeploy ke baad bhi resume mile
# agar backup channel set hai).
# Failed/offline files bhi "attempted" gini jati hain — resume unhe
# dobara nahi bhejega, sirf aage ki unattempted files bhejega.
_RESUME_LOCK = set()


def _batch_save_progress(uid, src_chat, start_id, end_id, last_done_id, public_fast=False):
    try:
        data = _load_json(BATCH_RESUME_FILE, {})
        if not isinstance(data, dict):
            data = {}
        data[str(uid)] = {
            "src_chat": src_chat, "start_id": start_id, "end_id": end_id,
            "last_done_id": last_done_id, "public_fast": bool(public_fast),
        }
        _save_json(BATCH_RESUME_FILE, data)
    except Exception as e:
        print(f"[RESUME] save fail: {e}", flush=True)


def _batch_get_progress(uid):
    try:
        data = _load_json(BATCH_RESUME_FILE, {})
        if isinstance(data, dict):
            entry = data.get(str(uid))
            if isinstance(entry, dict) and "last_done_id" in entry and "end_id" in entry:
                return entry
    except Exception:
        pass
    return None


def _batch_clear_progress(uid):
    try:
        data = _load_json(BATCH_RESUME_FILE, {})
        if isinstance(data, dict) and str(uid) in data:
            data.pop(str(uid), None)
            _save_json(BATCH_RESUME_FILE, data)
    except Exception:
        pass


async def _handle_resume(client, uid, trigger_msg):
    """Resume button / /resume — ruka hua batch wahi se dobara shuru karo."""
    if not _is_allowed(uid):
        await client.send_message(uid, "⛔ Access denied.")
        return
    if trigger_msg is not None:
        try:
            if not await _has_joined(uid, force=True):
                await _send_join_wall(trigger_msg)
                return
        except Exception:
            pass
    if uid in ACTIVE_BATCHES or uid in _RESUME_LOCK:
        await client.send_message(uid, "⚠️ Batch pehle se chal raha hai, resume nahi kar sakte.")
        return
    entry = _batch_get_progress(uid)
    if not entry:
        await client.send_message(uid, "❌ Koi ruka hua batch nahi mila. Naya batch chalane ke liye `/batch` ya `/public` use karo.")
        return
    src_chat = entry.get("src_chat")
    end_id = entry.get("end_id")
    last_done = entry.get("last_done_id")
    public_fast = entry.get("public_fast", False)
    next_id = (last_done + 1) if isinstance(last_done, int) else None
    if next_id is None or next_id > end_id:
        _batch_clear_progress(uid)
        await client.send_message(uid, "✅ Ye batch pehle hi poora ho gaya tha, resume karne ko kuch nahi bacha.")
        return
    _RESUME_LOCK.add(uid)
    try:
        if trigger_msg is not None:
            status_msg = await trigger_msg.reply_text(
                f"▶️ **Batch resumed!**\n`{next_id}` se `{end_id}` tak, bacha hua `{end_id - next_id + 1}` items.")
        else:
            status_msg = await client.send_message(
                uid, f"▶️ **Batch resumed!**\n`{next_id}` se `{end_id}` tak.")
        await run_batch(client, uid, src_chat, next_id, end_id, status_msg, public_fast=public_fast)
    finally:
        _RESUME_LOCK.discard(uid)


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
    _batch_save_progress(owner_id, src_chat, start_id, end_id, start_id - 1, public_fast)
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
                    _batch_save_progress(owner_id, src_chat, start_id, end_id, mid, public_fast)
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
                _batch_save_progress(owner_id, src_chat, start_id, end_id, mid, public_fast)
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
        # Ruka hua batch: resume entry rakho, backup ek baar upload karo,
        # aur RESUME BUTTON wala message bhejo (pinned status par button
        # safe_edit se nahi lagta, isliye alag message hai).
        _entry = _batch_get_progress(owner_id)
        _last = _entry.get("last_done_id") if _entry else None
        if not isinstance(_last, int):
            _last = (start_id + done - 1) if done > 0 else (start_id - 1)
            _batch_save_progress(owner_id, src_chat, start_id, end_id, _last, public_fast)
        _next = _last + 1
        try:
            await _backup_upload("batch_resume.json", BATCH_RESUME_FILE)
        except Exception:
            pass
        await safe_edit(status_msg, f"🛑 Batch stopped.\nDone: `{done}` / `{total}`\nNext: `{_next}` se resume hoga.")
        try:
            await client.send_message(
                owner_id,
                f"🛑 **Batch ruk gaya hai.**\n"
                f"Done: `{done}` / `{total}` (Success: `{ok}` | Failed: `{fail}`)\n"
                f"**Resume `{_next}` se `{end_id}` tak hoga** — pehli file se nahi, wahi se jahan ruka tha.\n\n"
                "Resume karne ke liye neeche wala button dabao, ya `/resume` likho.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("▶️ Resume Batch", callback_data="resume_batch")
                ]]),
            )
        except Exception as e:
            print(f"[RESUME] button message fail: {e}", flush=True)
    else:
        _batch_clear_progress(owner_id)
        try:
            await _backup_upload("batch_resume.json", BATCH_RESUME_FILE)
        except Exception:
            pass
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
    # Invite link (https://t.me/+xxxx ya joinchat) — username regex isko
    # pakad nahi pata tha, isliye /organize par "Invalid link" aata tha
    # (screenshot 2026-10-29). Poora link hi wapas do, get_chat/join_chat
    # wahi handle karega (_get_chat_maybe_invite dekho).
    if text and ("t.me/+" in text or "joinchat" in text):
        return text.strip()
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
        batch = []
        for t in res.topics:
            if type(t).__name__ == "ForumTopicDeleted":
                continue
            _tid = getattr(t, "id", None)
            _ttitle = getattr(t, "title", None)
            if _tid is None or not _ttitle:
                continue
            batch.append((_tid, _ttitle))
        if not batch:
            break
        topics.extend(batch)
        if len(batch) < 100:
            break
        last = res.topics[-1]
        offset_date = getattr(last, "date", offset_date)
        offset_id = getattr(last, "top_message", offset_id)
        offset_topic = getattr(last, "id", offset_topic)
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



# --- v60 MKCHANNELS + SENDALL: 10 username se PUBLIC channel banao, phir
# ek message sab channels me bhejo ---
# User ne manga (2026-10-29):
# - "bot me aisa function add karna hai jo mere liye PUBLIC CHANNELS banaye,
#    main bas 10 username de du, wo mere liye channel create kar de"
# - "jo video / text / image forward karne bolu, usme wo bhej bhi de SABME"
# Design (verified /create pattern par, naya tareeka nahi):
# - Channel BOT se nahi banta, sirf /login wale user account (userbot) se
#   banta hai, jaise /create me group banta hai. Isliye /mkchannels ke
#   liye bhi /login zaroori hai, aur naye channels ke owner wahi account
#   hoga jisne /mkchannels chalaya.
# - Public channel = channels.CreateChannel(broadcast=True, megagroup=False)
#   + channels.UpdateUsername (username set karne par hi channel public
#   hota hai). Username 5-32 chars, pehla char letter, sirf a-z/0-9/_.
# - Ek run me MAX 10 username (user ne 10 bola hai). Zyada bheje to
#   pehle 10 banenge, baaki ki list final message me bata di jayegi.
# - v63: Channel ka title = USER NE DIYA HUA EK HI NAAM hota hai
#   (jaise "Dholu Bholu Kids"), username alag-alag hote hain.
#   User ne pehle poochha tha (2026-10-29): 10 channel ek hi naam se
#   banenge, sirf username alag hoga. Isliye /mkchannels ab do-step
#   hai — pehle naam, phir 10 username. Purane v60-v62 design me title
#   username hi hota tha, jo 10 alag naam bana deta — wo galat tha.
#   Username taken/invalid ho to wo
#   channel banake chhoda nahi jata — banaya hua channel delete karke
#   us username ko failed list me dala jata hai, taaki orphan private
#   channel na bachen.
# - Har channel ke beech 2 MIN (120 sec) ka gap hai (v61, user ne bola:
#   ek channel banao, 2 min timer, phir second, aise hi). Channel create
#   + username set heavy call hai, FloodWait se bachne ke liye. FloodWait
#   aaye to baaki username ruk jayenge aur report me bata diye jayenge.
#   Gap ke beech /cancel bhi kaam karta hai.
# - Channels ki list per-user channels.json me save hoti hai (backup
#   channel me bhi jati hai, taaki redeploy par na ude).
# - /sendall: kisi message par reply karke /sendall likho, YA /sendall
#   likh ke agla message (text / photo / video / document / animation /
#   audio) bhejo. Wo message userbot se sab channels me COPY hoga
#   (pehle copy_message, fail ho to forward_messages fallback — user ne
#   "forward karne" bola hai, isliye forward fallback rakha hai).
#   Bot khud channels me post nahi karta (bot un channels ka admin nahi
#   hai) — userbot creator hai, isliye wahi bhejta hai. Har channel ke
#   beech 2 sec ka gap hai.
MKCHANNELS_STATES = {}  # uid -> {"step": "WAITING_NAME"/"WAITING_DP"/"WAITING_BIO"/"WAITING_USERNAMES", "title": str, "bio": str, "dp_path": str|None}
MKCHANNELS_DEFAULT_BIO = "Created by VILLAN bot"
SENDALL_STATES = {}  # uid -> True (agla message source hoga)
MKCHANNELS_MAX = 10
# v61: user ne bola — ek channel banao, phir 2 MIN ka timer, phir second,
# aise hi saare. 5 sec ka gap channel create jaisi heavy call ke liye
# bahut kam tha, isliye 120 sec kar diya hai. 10 channel = 9 gap =
# ~18 min + create time. Gap ke beech /cancel bhi kaam kare, isliye
# gap ko event.wait() se toda gaya hai (neeche run_mkchannels me).
MKCHANNELS_GAP = 120
MKCHANNELS_ACTIVE = {}  # uid -> asyncio.Event (cancel flag)
SENDALL_GAP = 2


def _channels_load(uid):
    data = _load_json(CHANNELS_FILE, {})
    lst = data.get(str(uid), [])
    return lst if isinstance(lst, list) else []


def _channels_save(uid, lst):
    data = _load_json(CHANNELS_FILE, {})
    data[str(uid)] = lst
    _save_json(CHANNELS_FILE, data)


def _mk_parse_usernames(text):
    """Username wale message ko list me todo: line + comma dono se split,
    @ hatao, lower karo, order same rakho, duplicate sirf pehli baar.
    Sirf format-valid (5-32, pehla letter, a-z/0-9/_) rakho, baaki
    invalid list me alag se caller ko batane ke liye return nahi karte —
    yahan sirf valid return hote hain, invalid ko caller dobara parse
    karke ginta hai (isliye _mk_split_raw bhi hai)."""
    names = []
    seen = set()
    for part in (text or "").replace(",", "\n").splitlines():
        name = part.strip().lstrip("@").strip().lower()
        if not name or name in seen:
            continue
        seen.add(name)
        if re.fullmatch(r"[a-z][a-z0-9_]{4,31}", name):
            names.append(name)
    return names


def _mk_split_raw(text):
    raw = []
    for part in (text or "").replace(",", "\n").splitlines():
        name = part.strip().lstrip("@").strip().lower()
        if name:
            raw.append(name)
    return raw


async def _mk_create_public_channel(userbot, username, title=None, bio=None, bot_id=None, bot_username=None):
    """Ek PUBLIC channel banao + username set karo.
    Title = user ka diya hua common naam (v63), na diya ho to username.
    Bio = user ka diya hua bio (v64), na diya ho to default bio.
    Telegram about max ~255 chars hai, isliye bio ko 255 par kaato.
    v66: username set hone ke baad bot ko bhi admin banaya jata hai
    (Post Messages + Edit Messages ON), taaki baad me /sendall /
    /sendallrange ka bot fallback (v65) in channels par bhi chale, bina
    login account ke admin hone ke. Bot admin fail ho to channel nahi
    delete hota, sirf warning wapas milti hai.
    Returns (chat_id, link, bot_admin_warn) ya (None, error_string, "")."""
    from pyrogram import raw
    title = (title or username).strip()[:128]
    about = (bio or MKCHANNELS_DEFAULT_BIO).strip()[:255]
    try:
        upd = await userbot.invoke(raw.functions.channels.CreateChannel(
            title=title, about=about,
            broadcast=True, megagroup=False, forum=False))
    except Exception as e:
        return None, str(e)
    cid = None
    for ch in getattr(upd, "chats", []) or []:
        _id = getattr(ch, "id", None)
        if _id:
            cid = int(f"-100{_id}")
            break
    if not cid:
        return None, "channel id nahi mili"
    try:
        peer = await userbot.resolve_peer(cid)
        await userbot.invoke(raw.functions.channels.UpdateUsername(
            channel=peer, username=username))
    except Exception as e:
        # Username set nahi hua to channel public nahi hua — orphan
        # private channel chhodne se behtar hai use delete kar do.
        try:
            peer = await userbot.resolve_peer(cid)
            await userbot.invoke(raw.functions.channels.DeleteChannel(
                channel=peer))
        except Exception:
            pass
        return None, str(e), ""
    bot_warn = ""
    if not bot_id:
        bot_warn = "bot id nahi mila (get_me fail)"
    else:
        # v70 FIX: sirf promote karna kaafi nahi tha — Telegram channel
        # me EditAdmin tabhi chalta hai jab wo user channel ka member ho.
        # Bot channel ka member nahi hota hai, isliye seedha promote
        # USER_NOT_PARTICIPANT jaisi error se fail ho jata tha, aur
        # channel ban jata tha par bot admin nahi banta tha (yehi user
        # ki shikayat thi). Isliye pehle promote try karo, fail ho to
        # bot ko channel me ADD (invite) karo, phir dobara promote karo.
        from pyrogram.types import ChatPrivileges
        _priv = ChatPrivileges(
            can_post_messages=True,
            can_edit_messages=True,
            can_delete_messages=False,
            can_manage_chat=False,
            can_restrict_members=False,
            can_promote_members=False,
            can_change_info=False,
            can_invite_users=False,
            can_pin_messages=False,
            can_manage_topics=False,
        )
        try:
            await userbot.promote_chat_member(cid, bot_id, privileges=_priv)
        except Exception as e1:
            try:
                await userbot.add_chat_members(cid, bot_id)
            except Exception:
                try:
                    _bot_peer = await userbot.resolve_peer(bot_username) if bot_username else await userbot.resolve_peer(bot_id)
                    _chan_peer = await userbot.resolve_peer(cid)
                    await userbot.invoke(raw.functions.channels.InviteToChannel(
                        channel=_chan_peer, users=[_bot_peer]))
                except Exception:
                    pass
            try:
                await userbot.promote_chat_member(cid, bot_id, privileges=_priv)
            except Exception as e2:
                bot_warn = f"bot admin fail: {e2} (pehle try: {e1})"
                print(f"[MKCHANNELS] bot admin fail @{username}: {bot_warn}", flush=True)
    return cid, f"https://t.me/{username}", bot_warn


async def run_mkchannels(client, uid, usernames, status_msg, title=None, bio=None, dp_path=None):
    _ev = asyncio.Event()
    MKCHANNELS_ACTIVE[uid] = _ev
    try:
        await _run_mkchannels_inner(client, uid, usernames, status_msg, _ev, title=title, bio=bio, dp_path=dp_path)
    finally:
        MKCHANNELS_ACTIVE.pop(uid, None)
        # DP temp file sirf is run ke liye thi, khatam hone par delete karo
        if dp_path and os.path.exists(dp_path):
            try:
                os.remove(dp_path)
            except Exception:
                pass


async def _run_mkchannels_inner(client, uid, usernames, status_msg, _ev, title=None, bio=None, dp_path=None):
    userbot = await get_userbot(uid)
    if not userbot:
        await message_reply(status_msg, "❌ Pehle `/login` karo — public channel "
                            "tumhare account se banega, isliye login session zaroori hai.")
        return
    existing = _channels_load(uid)
    by_name = {c.get("username"): c for c in existing if isinstance(c, dict)}
    created, failed = [], []
    bot_admin_failed = []
    bot_id = None
    bot_username = None
    try:
        _me = await client.get_me()
        bot_id = _me.id
        bot_username = getattr(_me, "username", None)
    except Exception:
        bot_id = None
        bot_username = None
    for i, username in enumerate(usernames):
        if _ev.is_set():
            failed.append((username, "cancel kar diya gaya"))
            # Baaki ke username bhi cancel hi samjho, loop se bahar
            for _rest in usernames[i + 1:]:
                failed.append((_rest, "cancel kar diya gaya"))
            break
        try:
            await safe_edit(status_msg, f"📢 **Channel ban raha hai: `{i+1}` / `{len(usernames)}`**\n\n📛 Naam: `{title or username}`\n🔗 Username: `@{username}`\n📢 Ban gaye: `{len(created)}` • Baki: `{len(usernames) - i}`")
        except Exception:
            pass
        try:
            cid, res, bot_warn = await _mk_create_public_channel(userbot, username, title=title, bio=bio, bot_id=bot_id, bot_username=bot_username)
        except FloodWait as e:
            failed.append((username, f"FloodWait {e.value}s — baaki ruk gaye"))
            print(f"[MKCHANNELS] FloodWait {username}: {e.value}s", flush=True)
            break
        except Exception as e:
            cid, res, bot_warn = None, str(e), ""
        dp_warn = ""
        if cid and dp_path and os.path.exists(dp_path):
            # DP alag call se lagti hai (CreateChannel me photo nahi hoti).
            # DP fail ho to channel delete mat karo — channel + username
            # ban chuka hai, sirf DP ka warning report me de do.
            try:
                await userbot.set_chat_photo(cid, photo=dp_path)
            except Exception as e:
                dp_warn = f" (DP fail: {e})"
        if cid:
            if bot_warn:
                bot_admin_failed.append((username, bot_warn))
            entry = {"username": username, "chat_id": cid, "title": (title or username),
                     "link": res, "dp": "yes" if (dp_path and not dp_warn) else ("fail" if dp_warn else "default"),
                     "bot_admin": "fail" if bot_warn else "yes"}
            by_name[username] = entry
            created.append(entry)
            _channels_save(uid, list(by_name.values()))
        else:
            failed.append((username, res))
        if i < len(usernames) - 1:
            # v64 timer: 2 min ka gap download jaisi GREEN progress bar ke
            # saath dikhao, har 10 sec me status update karo (har sec edit
            # karne se Telegram edit flood ho jayega, isliye 10 sec ka gap).
            # Saath me ban gaye / baki ka count bhi dikhao.
            # /cancel aaye to turant ruk jao, poora 120 sec wait mat karo.
            _elapsed = 0
            _cancelled = False
            while _elapsed < int(MKCHANNELS_GAP):
                if _ev.is_set():
                    _cancelled = True
                    break
                _remaining = int(MKCHANNELS_GAP) - _elapsed
                _filled = int(_elapsed / MKCHANNELS_GAP * 10) if MKCHANNELS_GAP else 10
                _bar = "🟩" * _filled + "⬜" * (10 - _filled)
                _pct = int(_elapsed / MKCHANNELS_GAP * 100) if MKCHANNELS_GAP else 100
                _rm, _rs = divmod(_remaining, 60)
                try:
                    await safe_edit(status_msg,
                        f"⏳ **Agla channel banne me timer chal raha hai**\n\n"
                        f"{_bar} {_pct}%\n"
                        f"⏱️ Bacha hua time: `{_rm}:{_rs:02d}` / `2:00`\n\n"
                        f"📢 Ban gaye: `{len(created)}` / `{len(usernames)}` • Baki: `{len(usernames) - i - 1}`\n"
                        f"📛 Naam: `{title or usernames[0]}` • Agla: `@{usernames[i+1]}`\n\n"
                        f"**❌ Cancel:** `/cancel`")
                except Exception:
                    pass
                try:
                    await asyncio.wait_for(_ev.wait(), timeout=10)
                    _cancelled = True
                    break
                except asyncio.TimeoutError:
                    _elapsed += 10
            if not _cancelled:
                try:
                    await safe_edit(status_msg, f"⏳ **Timer khatam, agla channel ban raha hai: `@{usernames[i+1]}`**\n\n📢 Ban gaye: `{len(created)}` / `{len(usernames)}` • Baki: `{len(usernames) - i - 1}`")
                except Exception:
                    pass
    _channels_save(uid, list(by_name.values()))
    done = ("✅ **Mkchannels complete!**\n"
            f"📢 Public channels ban gaye: `{len(created)}`\n"
            f"📛 Sab ka naam: `{title or 'username jaisa'}` (username alag-alag hai)\n"
            f"🖼️ DP: `{'tumhari photo' if dp_path else 'default (naam ka pehla akshar)'}` • 📝 Bio: `{((bio or MKCHANNELS_DEFAULT_BIO)[:80])}`")
    for c in created:
        done += f"\n• {c.get('title', c['username'])} — @{c['username']} — {c['link']}"
    if bot_admin_failed:
        done += f"\n🤖 Bot admin fail hue: `{len(bot_admin_failed)}` channels (in par bot se broadcast nahi hoga, login account se hoga)"
        for uname, err in bot_admin_failed:
            done += f"\n• @{uname} — {err}"
    else:
        done += "\n🤖 Bot admin: sab naye channels me bot ko admin + Post Messages bana diya gaya hai"
    if failed:
        done += f"\n❌ Fail hue: `{len(failed)}`"
        for uname, err in failed:
            done += f"\n• @{uname} — {err}"
    done += ("\n\nℹ️ Sab channels ka naam ek hi hai, sirf username alag hai. In channels me bhejne ke liye "
             "`/sendall` use karo (kisi message par reply karke, ya "
             "`/sendall` likh ke agla message bhej ke).")
    await safe_edit(status_msg, done)
    await message_reply(status_msg, done)


# --- v65 BOT FALLBACK (sendall / sendallrange) ---
# User ne poochha: bot ko sab channels me admin bana do to kya ho jayega?
# Jawab: haan, par tabhi jab send bot se bhi try ho. v64 tak /sendall sirf
# /login wale account (userbot) se bhejta tha — bot admin hone ka koi
# fayda nahi tha, kyunki bot ko try hi nahi kiya jata tha.
# v65 me order ye hai, har channel par:
#   1. userbot copy_message (pehle jaisa, creator/admin hone par chalega)
#   2. userbot forward_messages fallback
#   3. BOT copy_message (bot admin + Post Messages permission hone par)
#   4. BOT forward_messages fallback
# Bot se bheja hua message channel me bot ke through aayega, content
# wahi rahega. Bot fallback sirf tab kaam karega jab bot dest channel
# me admin hai aur Post Messages ON hai; warna wo bhi fail hoga aur
# report me fail aayega. Login account ka admin hona ab zaroori nahi
# raha, par pehli koshish abhi bhi userbot se hi hogi (pehle wala
# behaviour badla nahi hai, sirf fallback juda hai).
async def run_sendall(client, uid, source_msg, status_msg):
    userbot = await get_userbot(uid)
    if not userbot:
        await message_reply(status_msg, "❌ Pehle `/login` karo — channels me "
                            "tumhare account (creator) se bheja jayega.")
        return
    channels = _channels_load(uid)
    if not channels:
        await message_reply(status_msg, "❌ Koi channel saved nahi hai. Pehle "
                            "`/mkchannels` se channels banao, phir `/sendall` karo.")
        return
    # Source message bot-chat me hai; userbot ke liye wo chat bot se hai,
    # isliye from_chat_id = bot ka user id hota hai, user ka nahi.
    bot_id = None
    try:
        me = await client.get_me()
        bot_id = me.id
    except Exception:
        bot_id = None
    from_chat = bot_id if bot_id else source_msg.chat.id
    if not (source_msg.text or source_msg.caption or source_msg.photo
            or source_msg.video or source_msg.document or source_msg.animation
            or source_msg.audio or source_msg.sticker):
        await message_reply(status_msg, "❌ Is message me bhejne layak content "
                            "nahi hai (text / photo / video / document chahiye).")
        return
    sent, failed = [], []
    bot_via = 0
    for ch in channels:
        dest = ch.get("chat_id")
        uname = ch.get("username", "?")
        if not dest:
            failed.append((uname, "chat_id nahi hai"))
            continue
        ok = False
        err = ""
        via_bot = False
        try:
            await userbot.copy_message(dest, from_chat, source_msg.id)
            ok = True
        except Exception as e:
            err = str(e)
            try:
                await userbot.forward_messages(dest, from_chat, [source_msg.id])
                ok = True
            except Exception as e2:
                err = str(e2)
        if not ok:
            # v65 bot fallback: bot dest me admin + Post Messages ON ho to chalega
            try:
                await client.copy_message(dest, source_msg.chat.id, source_msg.id)
                ok = True
                via_bot = True
            except Exception as e:
                err = str(e)
                try:
                    await client.forward_messages(dest, source_msg.chat.id, [source_msg.id])
                    ok = True
                    via_bot = True
                except Exception as e2:
                    err = str(e2)
        if ok:
            sent.append(uname)
            if via_bot:
                bot_via += 1
        else:
            failed.append((uname, err))
        await asyncio.sleep(SENDALL_GAP)
    done = ("✅ **Sendall complete!**\n"
            f"📢 Bhej diya: `{len(sent)}` / `{len(channels)}` channels me")
    if bot_via:
        done += f"\n🤖 Bot se bheje: `{bot_via}` channels (login account fail hua, bot admin se kaam hua)"
    if sent:
        done += "\n" + "\n".join(f"• @{u}" for u in sent)
    if failed:
        done += f"\n❌ Fail hue: `{len(failed)}`"
        for uname, err in failed:
            done += f"\n• @{uname} — {err}"
    await safe_edit(status_msg, done)
    await message_reply(status_msg, done)



# --- v62 ADDCHANNEL + SENDALLRANGE ---
# User ne manga (2026-10-29):
# - "channels ADD karne ka option add kardo" — pehle se bane hue channel
#   bhi broadcast list me judne chahiye, sirf /mkchannels se bane hue nahi.
# - "ek baar me sab par 50 MESSAGE (audio / video / text) bhej saku"
#   — ek message ka /sendall kaafi nahi, 50 message ka batch chahiye.
# Design:
# - /addchannel: channel ka link (@username / t.me/username / t.me/c/id /
#   -100... id) bhejo, userbot se channel verify karo, channels.json me
#   save karo (source="added", mkchannels wale source="created" hote
#   hain — list me dono dikhte hain, /sendall dono par kaam karta hai).
#   Admin check best-effort hai: login account channel ka creator/admin
#   na ho to bhi add kar diya jayega, par warning di jayegi — kyunki
#   user admin baad me bhi bana sakta hai, sendall ke time par fail
#   hoga to report me aa jayega. Add karte time channel ka title,
#   username aur chat_id save hote hain, duplicate chat_id dobara nahi
#   judta (purani entry update ho jati hai).
# - /sendallrange: /public jaisa do-step flow — pehli message ka link,
#   phir aakhri message ka link. Range MAX 50 message (user ne 50 bola
#   hai), zyada ho to pehle 50 hi jayenge, baaki ke liye dobara chalao.
#   Source public ya private channel/group ho sakta hai (userbot ko
#   uska access hona chahiye, jaise /batch me hota hai). Messages
#   oldest-first order me, har message ko har saved channel me COPY
#   kiya jata hai (copy fail ho to forward fallback), har send ke
#   beech SENDALL_GAP (2 sec) hai. Time estimate: messages * channels
#   * 2 sec — jaise 50 message * 10 channel = 500 send = ~17 min.
#   Empty/service message skip hote hain, report me bataye jayenge.
#   Cancel: SENDALLRANGE_ACTIVE event, /cancel se loop ruk jayega.
ADDCHANNEL_STATES = {}  # uid -> {"step": "WAITING_LINK"}
SENDALLRANGE_STATES = {}  # uid -> {"step": "WAITING_FIRST"/"WAITING_LAST", "chat_id", "start_id"}
SENDALLRANGE_ACTIVE = {}  # uid -> asyncio.Event (cancel flag)
SENDALLRANGE_MAX = 50


def _add_parse_channels(text):
    """Channel input ko alag-alag part me todo: line + comma dono se.
    Har part ho sakta hai: t.me link, @username, plain username,
    t.me/c/id, ya -100... id. Returns list[str] (raw parts, order same)."""
    parts = []
    for part in (text or "").replace(",", "\n").splitlines():
        part = part.strip()
        if part:
            parts.append(part)
    return parts


def _add_resolve_identifier(part):
    """Ek part ko _clone_parse_link jaisi identifier me badlo, ya None."""
    part = (part or "").strip()
    if not part:
        return None
    if part.lstrip("-").isdigit():
        return int(part)
    ident = _clone_parse_link(part)
    if ident is not None:
        return ident
    if re.fullmatch(r"[a-zA-Z0-9_]{4,32}", part.lstrip("@")):
        return part.lstrip("@")
    return None


async def run_addchannels(client, uid, parts, status_msg):
    userbot = await get_userbot(uid)
    if not userbot:
        await message_reply(status_msg, "❌ Pehle `/login` karo — channel verify "
                            "tumhare account se hoga, isliye login zaroori hai.")
        return
    existing = _channels_load(uid)
    by_id = {c.get("chat_id"): c for c in existing if isinstance(c, dict) and c.get("chat_id")}
    added, failed = [], []
    for part in parts:
        ident = _add_resolve_identifier(part)
        if ident is None:
            failed.append((part, "link/username samajh nahi aaya"))
            continue
        try:
            chat = await userbot.get_chat(ident)
        except Exception as e:
            failed.append((part, str(e)))
            continue
        chat_id = chat.id
        username = getattr(chat, "username", None)
        title = getattr(chat, "title", None) or (f"@{username}" if username else str(chat_id))
        link = f"https://t.me/{username}" if username else ""
        # Admin check (best-effort, add ko rokta nahi)
        warn = ""
        try:
            me = await userbot.get_me()
            member = await userbot.get_chat_member(chat_id, me.id)
            if str(member.status) not in ("creator", "administrator", "ChatMemberStatus.OWNER", "ChatMemberStatus.ADMINISTRATOR"):
                warn = " (warning: login account is channel ka admin nahi hai — sendall se pehle admin banao, warna send fail hoga)"
        except Exception:
            warn = " (warning: admin status check nahi ho paya)"
        entry = {"username": username, "chat_id": chat_id, "title": title,
                 "link": link, "source": "added"}
        by_id[chat_id] = entry
        added.append((entry, warn))
    _channels_save(uid, list(by_id.values()))
    done = ("✅ **Addchannel complete!**\n"
            f"📢 Channel add hue: `{len(added)}`")
    for entry, warn in added:
        done += f"\n• {entry['title']} — {entry['link'] or entry['chat_id']}{warn}"
    if failed:
        done += f"\n❌ Fail hue: `{len(failed)}`"
        for part, err in failed:
            done += f"\n• {part} — {err}"
    done += ("\n\nAb in par bhi `/sendall` (1 message) ya `/sendallrange` "
             "(50 message tak) kaam karega. List dekhne ke liye `/mychannels`.")
    await safe_edit(status_msg, done)
    await message_reply(status_msg, done)


async def run_sendallrange(client, uid, src_chat, start_id, end_id, status_msg):
    _ev = asyncio.Event()
    SENDALLRANGE_ACTIVE[uid] = _ev
    try:
        await _run_sendallrange_inner(client, uid, src_chat, start_id, end_id, status_msg, _ev)
    finally:
        SENDALLRANGE_ACTIVE.pop(uid, None)


async def _run_sendallrange_inner(client, uid, src_chat, start_id, end_id, status_msg, _ev):
    userbot = await get_userbot(uid)
    if not userbot:
        await message_reply(status_msg, "❌ Pehle `/login` karo — source messages "
                            "tumhare account se padhe jayenge.")
        return
    channels = _channels_load(uid)
    if not channels:
        await message_reply(status_msg, "❌ Koi channel saved nahi hai. Pehle "
                            "`/mkchannels` ya `/addchannel` se channels jodo.")
        return
    ids = list(range(start_id, end_id + 1))
    msgs = {}
    try:
        chunk = await _fetch_chunk(userbot, src_chat, ids)
        if isinstance(chunk, dict):
            msgs = chunk
    except Exception as e:
        print(f"[SENDALLRANGE] chunk fetch fail: {e}", flush=True)
    if not msgs:
        # Fallback: ek-ek karke
        for mid in ids:
            if _ev.is_set():
                break
            try:
                m = await userbot.get_messages(src_chat, mid)
                if m and not m.empty:
                    msgs[mid] = m
            except Exception:
                continue
    ordered = [msgs[mid] for mid in sorted(msgs.keys())]
    usable = [m for m in ordered if (m.text or m.caption or m.photo or m.video
              or m.document or m.animation or m.audio or m.sticker)]
    skipped = len(ordered) - len(usable)
    if not usable:
        await message_reply(status_msg, "❌ Is range me bhejne layak koi message "
                            "nahi mila (empty / deleted / access nahi).")
        return
    per_channel = {str(c.get("chat_id")): 0 for c in channels if c.get("chat_id")}
    failed_ch = {}
    total_sends = 0
    bot_sends = 0
    cancelled = False
    for m in usable:
        if _ev.is_set():
            cancelled = True
            break
        for ch in channels:
            if _ev.is_set():
                cancelled = True
                break
            dest = ch.get("chat_id")
            if not dest:
                continue
            total_sends += 1
            ok = False
            err = ""
            via_bot = False
            try:
                await userbot.copy_message(dest, src_chat, m.id)
                ok = True
            except Exception as e:
                err = str(e)
                try:
                    await userbot.forward_messages(dest, src_chat, [m.id])
                    ok = True
                except Exception as e2:
                    err = str(e2)
            if not ok:
                # v65 bot fallback: dest me bot admin ho to bot se try karo.
                # Source private ho aur bot uska member na ho to ye bhi fail
                # hoga — ye fallback ki limit hai, report me fail aayega.
                try:
                    await client.copy_message(dest, src_chat, m.id)
                    ok = True
                    via_bot = True
                except Exception as e:
                    err = str(e)
                    try:
                        await client.forward_messages(dest, src_chat, [m.id])
                        ok = True
                        via_bot = True
                    except Exception as e2:
                        err = str(e2)
            if ok:
                per_channel[str(dest)] = per_channel.get(str(dest), 0) + 1
                if via_bot:
                    bot_sends += 1
            else:
                failed_ch[str(dest)] = err
            await asyncio.sleep(SENDALL_GAP)
    done = ("✅ **Sendallrange complete!**\n"
            f"📝 Messages: `{len(usable)}` (skip hue empty: `{skipped}`)\n"
            f"📢 Channels: `{len(channels)}` • Total send: `{total_sends}`")
    if bot_sends:
        done += f"\n🤖 Bot fallback se bheje: `{bot_sends}` sends (login account fail hua, bot admin se kaam hua)"
    if cancelled:
        done += "\n🛑 Cancel kar diya gaya — iske baad ke message nahi bheje gaye."
    for ch in channels:
        dest = str(ch.get("chat_id"))
        sent_n = per_channel.get(dest, 0)
        uname = ch.get("username") or ch.get("title") or dest
        if dest in failed_ch and sent_n == 0:
            done += f"\n• @{uname} — ❌ fail ({failed_ch[dest]})"
        else:
            done += f"\n• @{uname} — ✅ {sent_n}/{len(usable)}"
    await safe_edit(status_msg, done)
    await message_reply(status_msg, done)


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


async def _get_chat_maybe_invite(userbot, src):
    """get_chat karo; invite link ho aur get_chat fail ho to ek baar
    join_chat karke dobara try karo (pehle se member ho to join wali
    error ignore karo, dobara get_chat hi chalega)."""
    try:
        return await userbot.get_chat(src)
    except Exception:
        if isinstance(src, str) and ("t.me/+" in src or "joinchat" in src):
            try:
                await userbot.join_chat(src)
            except Exception:
                pass
            return await userbot.get_chat(src)
        raise


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
            src = await _get_chat_maybe_invite(userbot, src_chat)
            src_title = getattr(src, "title", None) or str(src_chat)
            # Invite link se aaye to aage ke saare kaam (topics, peer) ke liye
            # numeric id hi use karo, warna resolve_peer invite par fail hoga.
            if getattr(src, "id", None):
                src_chat = src.id
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
                buckets.setdefault(key, {"name": tname, "msgs": []})["msgs"].append(msg)
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


# --- v57 ARRANGE: source group/channel -> dest group, naam se folder, sequencewise copy ---
# User ne manga (2026-10-29, /organize wale screenshot ke baad):
# - "bot dono group m admin rhega"
# - "bot phle uss group/channel ka link mangega jisme video ho, then wo
#   group link mangega jisme bhejna h folder bna k, then apna process
#   start krega or sequencewise bhej dega"
# - Command: /Arrange (Telegram command lower-case me register hai, isliye
#   /arrange likhna hi kaam karega; /Arrange bhi wahi command hai).
# Flow: /arrange -> SOURCE link poochho -> DEST link poochho -> process.
# Asli kaam /login wala account (userbot) karega, jaise /clone aur
# /organize karte hain — bot akela purani history nahi padh sakta, isliye
# bot ko dono groups me admin rakhna achha hai, par kaafi nahi hai:
# login account ko SOURCE me member aur DEST me admin (topic banane ke
# liye) hona hi chahiye, warna topic creation fail hogi.
# - DEST group forum hona chahiye (Topics ON). Forum nahi hai to pehle
#   ToggleForum try kiya jayega (user account se); na ho to bot ruk
#   jayega aur bolega ki group settings me Topics ON karo, phir dobara
#   /arrange chalao. Aadha kaam (kuch folders ban ke) nahi chhoda jayega.
# - DELETE KUCH NAHI: source group me kuch change nahi hoga, sirf DEST me
#   copy jayegi. /organize ki tarah hi, original safe rahega.
# - SEQUENCEWISE: har folder ke andar oldest-first order me copy hogi
#   (message ID ke hisaab se purani se nayi), taaki Chapter 1, 2, 3 ka
#   order folder ke andar sahi rahe. Folder ka order alphabetical hai
#   (deterministic, check karne me aasan).
# - Topic naam ka rule v56 /organize wala hi hai (_organize_topic_name):
#   "Maths Chapter 1.pdf" -> "Maths", "Physics Lecture 3.mp4" -> "Physics".
# - Invite link (t.me/+...) bhi chalega: _get_chat_maybe_invite se join
#   karke khola jayega. Best phir bhi t.me/c/... ya @username hi hai.
ARRANGE_STATES = {}  # uid -> {"step": "WAITING_SOURCE"/"WAITING_DEST", "src": ...}
ARRANGE_ACTIVE = {}  # uid -> asyncio.Event (cancel flag, /cancel se set hota hai)


def _arrange_parse_link(text):
    """Source/dest link parse karo. _clone_parse_link hi use karo — wo
    t.me/c/, @username, t.me/username aur t.me/+ invite sab deta hai."""
    return _clone_parse_link(text)


async def run_arrange(client, uid, src_chat, dest_chat, status_msg):
    """Source ki history -> naam se topic -> DEST me folder (ya reuse) ->
    har item ki COPY DEST folder me, oldest-first, sequencewise. NO DELETE."""
    userbot = await get_userbot(uid)
    if not userbot:
        await message_reply(status_msg, "❌ Pehle `/login` karo — source ki history aur "
                            "dest me folder, dono tumhare account se honge.")
        return
    cancel_event = asyncio.Event()
    ARRANGE_ACTIVE[uid] = cancel_event
    try:
        try:
            src = await _get_chat_maybe_invite(userbot, src_chat)
            src_title = getattr(src, "title", None) or str(src_chat)
            if getattr(src, "id", None):
                src_chat = src.id
        except Exception as e:
            await message_reply(status_msg, f"❌ Source group/channel nahi khul raha: {e}\n"
                                "Check karo ki tumhara account uska member hai.")
            return
        try:
            dest = await _get_chat_maybe_invite(userbot, dest_chat)
            dest_title = getattr(dest, "title", None) or str(dest_chat)
            if getattr(dest, "id", None):
                dest_chat = dest.id
        except Exception as e:
            await message_reply(status_msg, f"❌ Dest group nahi khul raha: {e}\n"
                                "Check karo ki tumhara account us group ka member hai.")
            return
        # DEST forum check: forum nahi hai to ToggleForum try karo, na ho to ruko.
        is_forum = getattr(dest, "is_forum", None)
        if is_forum is False:
            toggled = False
            try:
                from pyrogram import raw as _raw
                if hasattr(_raw.functions.channels, "ToggleForum"):
                    await userbot.invoke(_raw.functions.channels.ToggleForum(
                        channel=await userbot.resolve_peer(dest_chat), enabled=True))
                    dest2 = await userbot.get_chat(dest_chat)
                    toggled = getattr(dest2, "is_forum", False) is True
            except Exception as e:
                print(f"[ARRANGE] ToggleForum fail: {e}", flush=True)
            if not toggled:
                await message_reply(status_msg, "❌ **Dest group forum nahi hai!**\n"
                                    "Dest group ki Settings me jaake **Topics ON** karo, "
                                    "phir dobara `/arrange` chalao. Tab tak kuch copy nahi hua hai.")
                return
        await safe_edit(status_msg, f"🔍 **Arrange shuru:**\nSource: `{src_title}`\nDest: `{dest_title}`\nMessages nikal raha hun...")
        try:
            existing = await _clone_list_topics(userbot, dest_chat)
            topic_map = {ttitle.lower(): tid for tid, ttitle in (existing or [])}
        except Exception:
            topic_map = {}
        buckets = {}
        try:
            async for msg in userbot.get_chat_history(src_chat):
                if cancel_event.is_set():
                    break
                if not msg or getattr(msg, "empty", False):
                    continue
                info = _info_from_msg(msg, msg.id)
                if info is None:
                    continue
                # ARRANGE me source topic se koi matlab nahi — dest alag group
                # hai, isliye har item ko uske naam ke folder me copy karo,
                # chahe source me wo kisi bhi topic me ho.
                tname = _organize_topic_name(_organize_name_for_msg(msg, info))
                key = tname.lower()
                buckets.setdefault(key, {"name": tname, "msgs": []})["msgs"].append(msg)
        except Exception as e:
            await message_reply(status_msg, f"❌ History nahi padh paya: {e}")
            return
        total = sum(len(v["msgs"]) for v in buckets.values())
        await safe_edit(status_msg, f"📂 **Topics mile:** `{len(buckets)}`\n"
                        f"Items: `{total}` — dest me folders bana raha hun...")
        dest_peer = await userbot.resolve_peer(dest_chat)
        copied = failed = skipped = 0
        folder_report = []
        for key in sorted(buckets.keys()):
            if cancel_event.is_set():
                break
            b = buckets[key]
            tname = b["name"]
            dest_tid = topic_map.get(key)
            is_new = False
            if not dest_tid:
                try:
                    dest_tid = await _clone_create_topic(userbot, dest_chat, tname)
                except Exception as e:
                    print(f"[ARRANGE] folder fail '{tname}': {e}", flush=True)
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
            for m in reversed(b["msgs"]):  # oldest-first = sequencewise
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
        invite = None
        try:
            from pyrogram import raw as _raw
            inv = await userbot.invoke(_raw.functions.messages.ExportChatInvite(
                peer=dest_peer))
            invite = getattr(inv, "link", None)
        except Exception as e:
            print(f"[ARRANGE] invite link fail: {e}", flush=True)
        done_text = ("✅ **Arrange complete!**\n"
                     f"📦 Source: `{src_title}`\n"
                     f"📦 Dest: `{dest_title}`\n"
                     f"📂 Folders: `{len(buckets)}`\n"
                     f"✅ Copied: `{copied}` | ❌ Failed: `{failed}` | ⏭️ Skipped: `{skipped}`\n"
                     "🛡️ **Delete kuch nahi hua** — source group waisa ka waisa hai, "
                     "sirf dest me copy gayi hai.")
        if folder_report:
            done_text += "\n" + "\n".join(folder_report)
        if invite:
            done_text += f"\n🔗 Dest invite: {invite}"
        if cancel_event.is_set():
            done_text = "🛑 **Arrange stopped.**\n" + done_text
        await safe_edit(status_msg, done_text)
        await message_reply(status_msg, done_text)
    finally:
        ARRANGE_ACTIVE.pop(uid, None)


# --- v58 FBATCH: folderwise batch — /organize + /arrange ki jagah ek hi command ---
# User ne manga (2026-10-29):
# - "command ko fbatch name dedo, organise or arrange hata do, jb v
#   folderwise batch niklna hoga fbatch use kr lunga"
# - "isme destination m bss bot admin rhega to chalega n" -> HAAN, chalega,
#   isliye fbatch ka dest kaam BOT se hota hai: folder Bot API
#   createForumTopic se banega (bot ko dest me admin hona chahiye), upload
#   BOT client se hoga. Userbot ko dest me hona zaroori nahi hai.
#   Agar bot se fail ho to fallback me userbot se try kiya jayega.
#   Shart: bot se pehle se bane folders ki list nahi milti, isliye sirf
#   bot-admin wale dest me dobara fbatch chalane par duplicate folder ban
#   sakta hai. Bachne ke liye dest me login account ko sirf MEMBER bana
#   do (admin zaroori nahi) — tab folders reuse honge.
# - "jise upload show keta h waise isme v upload show krna chaiye" ->
#   har media file par private chat me per-file progress message banega,
#   jaise /batch me hota hai: pehle DOWNLOADING bar, phir UPLOADING bar.
# Flow: /fbatch -> SOURCE link -> DEST link -> process start.
# Baaki /arrange jaisa hi hai: naam se topic samjho, dest me folder, har
# folder ke andar oldest-first, sequencewise COPY, DELETE KUCH NAHI.
FBATCH_STATES = {}
FBATCH_ACTIVE = {}
FBATCH_TOPICS_FILE = "fbatch_topics.json"


def _fbatch_load_topics():
    try:
        data = _load_json(FBATCH_TOPICS_FILE, {})
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _fbatch_save_topics(mapping):
    try:
        _save_json(FBATCH_TOPICS_FILE, mapping)
    except Exception as e:
        print(f"[FBATCH] topics save fail: {e}", flush=True)


async def _fbatch_create_topic(bot_client, userbot, dest_id, title):
    tid = await _botapi_create_topic_for_chat(dest_id, title)
    if tid:
        return tid
    if userbot:
        try:
            tid = await _clone_create_topic(userbot, dest_id, title)
            if tid:
                return tid
        except Exception as e:
            print(f"[FBATCH] userbot topic fail '{title}': {e}", flush=True)
    return None


async def run_fbatch(client, uid, src_chat, dest_chat, status_msg):
    userbot = await get_userbot(uid)
    if not userbot:
        await message_reply(status_msg, "❌ Pehle `/login` karo — source ki history aur download tumhare account se honge.")
        return
    cancel_event = asyncio.Event()
    FBATCH_ACTIVE[uid] = cancel_event
    try:
        try:
            src = await _get_chat_maybe_invite(userbot, src_chat)
            src_title = getattr(src, "title", None) or str(src_chat)
            if getattr(src, "id", None):
                src_chat = src.id
        except Exception as e:
            await message_reply(status_msg, f"❌ Source group/channel nahi khul raha: {e}\nCheck karo ki tumhara account uska member hai.")
            return
        dest = None
        try:
            dest = await _get_chat_maybe_invite(userbot, dest_chat)
            dest_title = getattr(dest, "title", None) or str(dest_chat)
            if getattr(dest, "id", None):
                dest_chat = dest.id
        except Exception:
            dest_title = str(dest_chat)
            if isinstance(dest_chat, str) and dest_chat.lstrip("-").isdigit():
                dest_chat = int(dest_chat)
        is_forum = getattr(dest, "is_forum", None) if dest else None
        if is_forum is False:
            await message_reply(status_msg, "❌ **Dest group forum nahi hai!**\nDest group ki Settings me jaake **Topics ON** karo, phir dobara `/fbatch` chalao. Tab tak kuch copy nahi hua hai.")
            return
        await safe_edit(status_msg, f"🔍 **Fbatch shuru:**\nSource: `{src_title}`\nDest: `{dest_title}`\nMessages nikal raha hun...")
        topic_map = {}
        userbot_in_dest = False
        try:
            existing = await _clone_list_topics(userbot, dest_chat)
            if existing is not None:
                topic_map = {ttitle.lower(): tid for tid, ttitle in existing}
                userbot_in_dest = True
        except Exception:
            topic_map = {}
        saved_map = _fbatch_load_topics()
        dest_key = str(dest_chat)
        if dest_key in saved_map and isinstance(saved_map[dest_key], dict):
            for k, v in saved_map[dest_key].items():
                topic_map.setdefault(k.lower(), v)
        buckets = {}
        try:
            async for msg in userbot.get_chat_history(src_chat):
                if cancel_event.is_set():
                    break
                if not msg or getattr(msg, "empty", False):
                    continue
                info = _info_from_msg(msg, msg.id)
                if info is None:
                    continue
                tname = _organize_topic_name(_organize_name_for_msg(msg, info))
                key = tname.lower()
                buckets.setdefault(key, {"name": tname, "msgs": []})["msgs"].append(msg)
        except Exception as e:
            await message_reply(status_msg, f"❌ History nahi padh paya: {e}")
            return
        total = sum(len(v["msgs"]) for v in buckets.values())
        await safe_edit(status_msg, f"📂 **Topics mile:** `{len(buckets)}`\nItems: `{total}` — dest me folders bana raha hun...")
        dest_peer = None
        try:
            dest_peer = await client.resolve_peer(dest_chat)
        except Exception:
            try:
                dest_peer = await userbot.resolve_peer(dest_chat)
            except Exception as e:
                await message_reply(status_msg, f"❌ Dest group resolve nahi hua: {e}\nCheck karo ki bot dest me admin hai.")
                return
        copied = failed = skipped = 0
        folder_report = []
        for key in sorted(buckets.keys()):
            if cancel_event.is_set():
                break
            b = buckets[key]
            tname = b["name"]
            dest_tid = topic_map.get(key)
            is_new = False
            if not dest_tid:
                dest_tid = await _fbatch_create_topic(client, userbot, dest_chat, tname)
                if dest_tid:
                    topic_map[key] = dest_tid
                    is_new = True
                    saved_map.setdefault(dest_key, {})[key] = dest_tid
                    _fbatch_save_topics(saved_map)
            if not dest_tid:
                failed += len(b["msgs"])
                folder_report.append(f"• {tname}: ❌ folder nahi bana ({len(b['msgs'])} fail)")
                continue
            f_copied = f_failed = 0
            await safe_edit(status_msg, f"📂 **Folder:** `{tname}` ({'naya' if is_new else 'pehle se tha'})\nCopy ho raha hai... (total ab tak: ✅ {copied} | ❌ {failed})")
            for m in reversed(b["msgs"]):
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
                    sent = False
                    try:
                        from pyrogram import raw as _raw2
                        import random as _rand2
                        await client.invoke(_raw2.functions.messages.SendMessage(
                            peer=dest_peer, message=info["text"],
                            random_id=_rand2.randint(1, 2**63 - 1), top_msg_id=dest_tid))
                        sent = True
                    except Exception:
                        sent = await _clone_send_text(userbot, dest_peer, dest_tid, info["text"]) if userbot_in_dest else False
                    if sent:
                        copied += 1
                        f_copied += 1
                    else:
                        failed += 1
                        f_failed += 1
                    continue
                prog_msg = None
                try:
                    prog_msg = await status_msg.reply_text(
                        f"📥 Downloading • ID `{m.id}` • {info.get('file_name', 'file')} -> `{tname}`")
                except Exception:
                    prog_msg = None
                fp = await download_file(userbot, info, prog_msg, None, uid, client)
                if not fp:
                    failed += 1
                    f_failed += 1
                    if prog_msg:
                        try:
                            await safe_edit(prog_msg, f"❌ Download fail • ID `{m.id}`")
                        except Exception:
                            pass
                    if info.get("tmpdir"):
                        shutil.rmtree(info["tmpdir"], ignore_errors=True)
                    continue
                if prog_msg:
                    try:
                        await safe_edit(prog_msg, f"🚀 Uploading • ID `{m.id}` • {info.get('file_name', 'file')} -> `{tname}`")
                    except Exception:
                        pass
                ok = await _raw_send_to_topic(
                    client, dest_peer, dest_tid, info["kind"], file_path=fp,
                    caption=info.get("caption") or "", info=info, prog_msg=prog_msg)
                if not ok and userbot_in_dest:
                    if prog_msg:
                        try:
                            await safe_edit(prog_msg, f"🚀 Uploading (userbot) • ID `{m.id}`")
                        except Exception:
                            pass
                    ok = await _raw_send_to_topic(
                        userbot, dest_peer, dest_tid, info["kind"], file_path=fp,
                        caption=info.get("caption") or "", info=info, prog_msg=prog_msg)
                if info.get("tmpdir"):
                    shutil.rmtree(info["tmpdir"], ignore_errors=True)
                if ok:
                    copied += 1
                    f_copied += 1
                    if prog_msg:
                        try:
                            await safe_edit(prog_msg, f"✅ Copied • ID `{m.id}` • {info.get('file_name', 'file')} -> `{tname}`")
                        except Exception:
                            pass
                else:
                    failed += 1
                    f_failed += 1
                    if prog_msg:
                        try:
                            await safe_edit(prog_msg, f"❌ Upload fail • ID `{m.id}`")
                        except Exception:
                            pass
                await asyncio.sleep(2)
            folder_report.append(f"• {tname}: ✅ {f_copied} | ❌ {f_failed}")
            if cancel_event.is_set():
                break
        invite = None
        try:
            from pyrogram import raw as _raw
            inv = await client.invoke(_raw.functions.messages.ExportChatInvite(peer=dest_peer))
            invite = getattr(inv, "link", None)
        except Exception:
            pass
        done_text = ("✅ **Fbatch complete!**\n"
                     f"📦 Source: `{src_title}`\n"
                     f"📦 Dest: `{dest_title}`\n"
                     f"📂 Folders: `{len(buckets)}`\n"
                     f"✅ Copied: `{copied}` | ❌ Failed: `{failed}` | ⏭️ Skipped: `{skipped}`\n"
                     "🛡️ **Delete kuch nahi hua** — source waisa ka waisa hai, sirf dest me copy gayi hai.")
        if folder_report:
            done_text += "\n" + "\n".join(folder_report)
        if invite:
            done_text += f"\n🔗 Dest invite: {invite}"
        if not userbot_in_dest:
            done_text += ("\n⚠️ Dest me sirf bot admin tha, isliye pehle se bane folders ki list nahi mili — dobara fbatch chalane par duplicate folder ban sakta hai. Duplicate se bachne ke liye dest me login account ko sirf member bana do (admin zaroori nahi).")
        if cancel_event.is_set():
            done_text = "🛑 **Fbatch stopped.**\n" + done_text
        await safe_edit(status_msg, done_text)
        await message_reply(status_msg, done_text)
    finally:
        FBATCH_ACTIVE.pop(uid, None)


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
        _hist_n = 0
        try:
            async for msg in userbot.get_chat_history(src_chat):
                if cancel_event.is_set():
                    break
                if not msg or getattr(msg, "empty", False):
                    continue
                buckets.setdefault(_clone_topic_key(msg), []).append(msg)
                _hist_n += 1
                # v69 visibility: history padhte waqt chup mat raho — har
                # 50 messages par status update karo, taaki user ko pata
                # chale ki kuch padha ja raha hai, atak nahi gaya hai.
                if _hist_n % 50 == 0:
                    await safe_edit(status_msg,
                        f"🔍 **History padh raha hun...**\nMessages mile: `{_hist_n}` (ab tak)")
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
            _folder_msgs = list(reversed(msgs))  # oldest-first = same order
            for _fi, m in enumerate(_folder_msgs, 1):
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
                # v69 visibility: har media file par download aur upload
                # dono dikhao, taaki clone me ye pata chale ki kuch ho raha
                # hai ya nahi (pehle sirf folder naam dikhta tha).
                await safe_edit(status_msg,
                    f"📂 **Folder:** `{fname}`\n"
                    f"⬇️ **Download ho raha hai:** `{info.get('file_name') or m.id}` "
                    f"({_fi}/{len(_folder_msgs)})\n"
                    f"Total ab tak: ✅ {copied} | ❌ {failed}")
                fp = await download_file(userbot, info, None, None, uid, client)
                if not fp:
                    failed += 1
                    if info.get("tmpdir"):
                        shutil.rmtree(info["tmpdir"], ignore_errors=True)
                    continue
                await safe_edit(status_msg,
                    f"📂 **Folder:** `{fname}`\n"
                    f"⬆️ **Upload ho raha hai:** `{info.get('file_name') or m.id}` "
                    f"({_fi}/{len(_folder_msgs)})\n"
                    f"Total ab tak: ✅ {copied} | ❌ {failed}")
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
    "`/mkchannels` — Naam + DP + bio + 10 username se PUBLIC channels banao (naam/DP/bio ek hi, username alag)\n"
    "`/mychannels` — Tumhare banaye channels ki list\n"
    "`/sendall` — Ek message sab channels me bhejo (reply karke ya agla message bhej ke)\n"
    "`/addchannel` — Pehle se bana channel broadcast list me jodo\n"
    "`/sendallrange` — Pehli-aakhri link se 50 message tak sab channels me bhejo\n"
    "`/fbatch` — Folderwise batch: source se dest, folder + sequencewise copy (no delete, progress ke saath)\n"
    "`/resume` — Ruka hua batch wahi se dobara shuru karo (pehli file se nahi)\n"
    "`/settings` — Rename, caption, watermark\n"
    "`/plan` — Your plan\n"
    "`/buy` — Premium\n"
    "`/help` — Guide\n"
    "`/cancel` — Stop current task\n"
    "Send a link directly for a single file.\n\n"
    "**Owner:** `/premium <id> <amount> <unit>` • `/normal <id>` • `/unallow` • `/users`"
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
    "`/premium <user_id> <amount> <unit>` — Grant premium\n"
    "`/normal <user_id>` — Make normal (batch only)\n"
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



@app.on_message(filters.command("resume") & filters.private)
async def resume_start_handler(client, message):
    print(f"⚡ Processing /resume for user {message.chat.id}", flush=True)
    await _handle_resume(client, message.chat.id, message)


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
    if uid in ARRANGE_STATES:
        ARRANGE_STATES.pop(uid, None)
        hit = True
    ev = ARRANGE_ACTIVE.get(uid)
    if ev is not None:
        ev.set()
        hit = True
    if uid in FBATCH_STATES:
        FBATCH_STATES.pop(uid, None)
        hit = True
    if uid in MKCHANNELS_STATES:
        _mk_st = MKCHANNELS_STATES.pop(uid, None)
        if _mk_st and _mk_st.get("dp_path") and os.path.exists(_mk_st["dp_path"]):
            try:
                os.remove(_mk_st["dp_path"])
            except Exception:
                pass
        hit = True
    ev = MKCHANNELS_ACTIVE.get(uid)
    if ev is not None:
        ev.set()
        hit = True
    if uid in ADDCHANNEL_STATES:
        ADDCHANNEL_STATES.pop(uid, None)
        hit = True
    if uid in SENDALLRANGE_STATES:
        SENDALLRANGE_STATES.pop(uid, None)
        hit = True
    ev = SENDALLRANGE_ACTIVE.get(uid)
    if ev is not None:
        ev.set()
        hit = True
    if uid in SENDALL_STATES:
        SENDALL_STATES.pop(uid, None)
        hit = True
    ev = FBATCH_ACTIVE.get(uid)
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



@app.on_message(filters.command("premium") & filters.private)
async def premium_handler(client, message):
    """Owner kisi ko premium de: /premium <user_id> <amount> <unit>
    Unit: hour(s), day(s), month(s), year(s). Month = 30 din, year = 365 din.
    Purana form /premium <user_id> <days> bhi chalega (days maan ke)."""
    uid = message.chat.id
    if not _is_owner(uid):
        await message.reply_text("⛔ Owner only.")
        return
    parts = message.text.split()
    if len(parts) not in (3, 4):
        await message.reply_text(
            "Usage: `/premium <user_id> <amount> <unit>`\n"
            "Jaise: `/premium 123456789 1 month` • `/premium 123456789 2 hours` • "
            "`/premium 123456789 30 days` • `/premium 123456789 1 year`\n"
            "Unit: hour(s), day(s), month(s), year(s)")
        return
    if not parts[1].isdigit() or not parts[2].isdigit():
        await message.reply_text("❌ User ID aur amount dono number hone chahiye.")
        return
    target, amount = int(parts[1]), int(parts[2])
    if amount <= 0:
        await message.reply_text("❌ Amount 0 se zyada hona chahiye.")
        return
    if len(parts) == 3:
        unit, mult = "day", 86400
    else:
        unit = parts[3].strip().lower()
        mult = {"hour": 3600, "hours": 3600, "hr": 3600, "hrs": 3600,
                "day": 86400, "days": 86400,
                "month": 30 * 86400, "months": 30 * 86400,
                "year": 365 * 86400, "years": 365 * 86400}.get(unit)
        if not mult:
            await message.reply_text("❌ Unit hour, day, month ya year hona chahiye.")
            return
    add_secs = amount * mult
    if add_secs > 3650 * 86400:
        await message.reply_text("❌ Premium 3650 din se zyada nahi ho sakta.")
        return
    subs = _load_subs()
    now = time.time()
    cur = subs.get(str(target), {}).get("expiry", 0)
    new_exp = max(now, cur) + add_secs
    subs[str(target)] = {"type": "paid", "expiry": new_exp}
    _save_subs(subs)
    label = f"{amount} {unit}" if len(parts) == 4 else f"{amount} day(s)"
    card = await _premium_card(target, client)
    await message.reply_text(f"✅ Premium activate ho gaya ({label})!\n\n{card}")
    try:
        await client.send_message(
            target,
            f"🎉 Tumhe **{label}** ka premium access mil gaya hai!\n\n{card}\n\n"
            "`/login` karke shuru karo.",
            parse_mode=enums.ParseMode.MARKDOWN,
        )
    except Exception:
        pass


@app.on_message(filters.command("normal") & filters.private)
async def normal_handler(client, message):
    """Owner kisi premium user ko normal banaye: /normal <user_id>
    Paid subscription hatao, par allowed list me rakho, taaki user
    normal (batch-only) hi rahe, poori tarah blocked na ho."""
    uid = message.chat.id
    if not _is_owner(uid):
        await message.reply_text("⛔ Owner only.")
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.reply_text("Usage: `/normal <user_id>`")
        return
    target = int(parts[1])
    subs = _load_subs()
    subs.pop(str(target), None)
    _save_subs(subs)
    allowed = _load_allowed()
    allowed.add(target)
    _save_allowed(allowed)
    await message.reply_text(
        f"✅ `{target}` ab normal user hai.\n"
        "Wo /batch se save-restriction use kar sakta hai, par /public aur "
        "/mkchannels jaise premium commands use nahi kar payega.")


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
        "**VILLAN — Guide (sab commands ka use)**\n"
        "\n"
        "1. /login — Apne number se sign in karo (OTP aayega, phir 2FA ho to wo bhi). Bina login ke private groups/channels nahi khulenge.\n"
        "2. /logout — Session khatam karo.\n"
        "3. /batch — Bulk extraction: pehle pehli file ka link bhejo, phir aakhri file ka link bhejo. Ya seedha range bhejo: https://t.me/c/123456789/100-110\n"
        "   Files ek-ek karke, sequencewise aayengi, har file par download + upload progress dikhega.\n"
        "4. /public — Public channel ki fast copy: pehli link, phir aakhri link. Private link yahan nahi chalega, uske liye /batch use karo.\n"
        "5. /resume — Batch rukne par: batch rukte hi ek Resume Batch button wala message aayega, us button ko dabao ya /resume likho — batch wahi se shuru hoga jahan ruka tha, pehli file se nahi. Jo files pehle complete ho chuki hain (success ya fail, dono) wo dobara nahi aayengi, sirf aage ki bachi hui files aayengi. Agar beech me naya /batch chala diya, to resume latest batch ka hi hoga.\n"
        "6. /clone — Kisi group ke folders + saari messages ko naye PRIVATE group me copy karo: source group ka link bhejo, naya group tumhare account se banega.\n"
        "7. /create — Naya group banao: pehle group ka naam bhejo, phir topics ke naam ek hi message me (har naam ek line par, ya comma se) bhejo, un naam se folders ban jayenge.\n"
        "8a. /mkchannels — Public channels banao: /mkchannels likho, pehle NAAM bhejo (sab 10 me ek hi dikhega), phir DP bhejo ya skip karo (default rahega), phir BIO bhejo ya skip karo, phir 10 username ek hi message me bhejo (line ya comma se). Sirf username alag-alag hoga. Naam + DP + bio sab channels par ek saath lag jayenge. Channel tumhare login account se banega, aur har naye channel me bot ko bhi admin + Post Messages bana diya jayega, taaki baad me bot se bhi broadcast ho sake.\n"
        "8b. /mychannels — Tumhare banaye channels ki list dekho.\n"
        "8c. /sendall — Ek message sab channels me bhejo: kisi message (video / text / image) par reply karke /sendall likho, ya /sendall likh ke agla message bhejo. Pehle login account se try hoga, fail ho to bot se try hoga (bot dest me admin + Post Messages ON ho to).\n"
        "8d. /addchannel — Pehle se bana channel jodo: /addchannel likho, phir channel ka link / @username bhejo. Login account us channel ka admin hona chahiye, warna send fail hoga.\n"
        "8e. /sendallrange — 50 message tak ek saath sab channels me: /sendallrange likho, pehli message ka link bhejo, phir aakhri ka. Audio / video / text sab copy honge, sequencewise. Pehle login account se try hoga, fail ho to bot fallback try hoga (bot dest me admin ho to).\n"
        "8. /fbatch — Folderwise batch (folderwise kaam ke liye ek hi command hai): pehle SOURCE group/channel ka link bhejo, phir DEST group ka link bhejo. Bot naam padh ke topic samjhega, dest me us naam ka folder banayega, aur us naam wali saari cheezein (pdf / text / video / pic) us folder me sequencewise COPY karega. Delete kuch nahi hoga, source waisa ka waisa rahega. Har file par download + upload progress dikhega, jaise /batch me dikhta hai. Dest me sirf bot admin ho to bhi chalega, par duplicate folder se bachne ke liye dest me login account ko sirf member bana do. Dest group me Topics ON hona chahiye.\n"
        "9. /settings — Rename, caption template, word remove/replace, thumbnail, watermark, upload method badlo.\n"
        "10. /plan — Apna plan dekho. /buy — Premium kharido (payment manual UPI hai).\n"
        "11. /cancel — Chal raha kaam roko (batch, clone, create, fbatch — koi bhi). Batch rokne ke baad resume karne ke liye button ya /resume use karo.\n"
        "\n"
        "Seedha link bhejne par ek file ki copy hogi. Folderwise kaam ke liye sirf /fbatch hai.\n"
        "\n"
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
    ADDCHANNEL_STATES.pop(message.chat.id, None)
    SENDALLRANGE_STATES.pop(message.chat.id, None)
    MKCHANNELS_STATES.pop(message.chat.id, None)
    SENDALL_STATES.pop(message.chat.id, None)
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
    ADDCHANNEL_STATES.pop(message.chat.id, None)
    SENDALLRANGE_STATES.pop(message.chat.id, None)
    MKCHANNELS_STATES.pop(message.chat.id, None)
    SENDALL_STATES.pop(message.chat.id, None)
    USER_STATES.pop(message.chat.id, None)  # purana adhura batch state na atak jaye
    CREATE_STATES.pop(message.chat.id, None)  # v55: adhura /create state na atak jaye
    ARRANGE_STATES.pop(message.chat.id, None)
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
    ADDCHANNEL_STATES.pop(message.chat.id, None)
    SENDALLRANGE_STATES.pop(message.chat.id, None)
    MKCHANNELS_STATES.pop(message.chat.id, None)
    SENDALL_STATES.pop(message.chat.id, None)
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



@app.on_message(filters.command("mkchannels") & filters.private)
async def mkchannels_start_handler(client, message):
    if not await _has_joined(message.chat.id, force=True):
        await _send_join_wall(message)
        return
    if not _is_allowed(message.chat.id):
        await message.reply_text("⛔ Access denied.")
        return
    if not _is_premium(message.chat.id):
        await message.reply_text(
            "⛔ Ye command sirf premium members ke liye hai.\n"
            "Normal user /batch se save-restriction use kar sakta hai.")
        return
    print(f"⚡ Processing /mkchannels for user {message.chat.id}", flush=True)
    ADDCHANNEL_STATES.pop(message.chat.id, None)
    SENDALLRANGE_STATES.pop(message.chat.id, None)
    USER_STATES.pop(message.chat.id, None)
    CLONE_STATES.pop(message.chat.id, None)
    CREATE_STATES.pop(message.chat.id, None)
    PUBLIC_STATES.pop(message.chat.id, None)
    FBATCH_STATES.pop(message.chat.id, None)
    SENDALL_STATES.pop(message.chat.id, None)
    # Shortcut: /mkchannels <naam> likha ho to naam wahi maan lo, phir DP poochho (v64)
    args = message.command[1:] if len(message.command) > 1 else []
    if args:
        title = " ".join(args).strip()[:128]
        MKCHANNELS_STATES[message.chat.id] = {"step": "WAITING_DP", "title": title}
        await message.reply_text(
            f"**📢 VILLAN MKCHANNELS — Naam locked: `{title}`**\n"
            "**━━━━━━━━━━━━━━━**\n\n"
            "**STEP 2/4 — Ab channel ki DP bhejo (photo),**\n"
            "ya `skip` likho to default DP rahega (naam ka pehla akshar wala).\n"
            "DP milne ke baad bio poochhunga, phir username.\n\n"
            "**❌ Cancel:** `/cancel`",
            parse_mode=enums.ParseMode.MARKDOWN,
        )
        return
    MKCHANNELS_STATES[message.chat.id] = {"step": "WAITING_NAME"}
    await message.reply_text(
        "**📢 VILLAN MKCHANNELS — Public channels banao**\n"
        "**━━━━━━━━━━━━━━━**\n\n"
        "**STEP 1/4 — Pehle channel ka NAAM bhejo**\n"
        "Ye naam **sab 10 channels me ek hi dikhega**, jaise:\n"
        "`Dholu Bholu Kids`\n\n"
        "Uske baad STEP 2 me DP, STEP 3 me bio, STEP 4 me 10 username poochhunga,\n"
        "jo **alag-alag** honge aur wahi har channel ka link banenge.\n"
        "Jaise naam `Dholu Bholu Kids` + username `dholu1`, `dholu2` ho to\n"
        "dono channel ka naam `Dholu Bholu Kids` dikhega, link alag hoga.\n\n"
        "• Channel **tumhare login account se** banega (bot se channel\n"
        "  nahi banta, isliye `/login` zaroori hai), aur tum hi uske owner hoge.\n"
        "• Har channel ke beech **2 MIN ka gap** hai — 10 channel me ~18 min + create time lagega.\n"
        "**❌ Cancel:** `/cancel`",
        parse_mode=enums.ParseMode.MARKDOWN,
    )


@app.on_message(filters.command("mychannels") & filters.private)
async def mychannels_handler(client, message):
    if not await _has_joined(message.chat.id, force=True):
        await _send_join_wall(message)
        return
    if not _is_allowed(message.chat.id):
        await message.reply_text("⛔ Access denied.")
        return
    channels = _channels_load(message.chat.id)
    if not channels:
        await message.reply_text(
            "📢 Koi channel saved nahi hai.\n\nPehle `/mkchannels` se channels banao.")
        return
    txt = f"📢 **Tumhare channels: `{len(channels)}`**\n"
    for c in channels:
        txt += f"\n• {c.get('title', c.get('username', '?'))} — @{c.get('username', '?')} — {c.get('link', '')}"
    txt += ("\n\nIn sab me ek message bhejne ke liye: kisi message par reply "
            "karke `/sendall` likho, ya `/sendall` likh ke agla message bhejo.")
    await message.reply_text(txt, parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.command("sendall") & filters.private)
async def sendall_start_handler(client, message):
    if not await _has_joined(message.chat.id, force=True):
        await _send_join_wall(message)
        return
    if not _is_allowed(message.chat.id):
        await message.reply_text("⛔ Access denied.")
        return
    if not _is_premium(message.chat.id):
        await message.reply_text(
            "⛔ Ye command sirf premium members ke liye hai.\n"
            "Normal user /batch se save-restriction use kar sakta hai.")
        return
    print(f"⚡ Processing /sendall for user {message.chat.id}", flush=True)
    ADDCHANNEL_STATES.pop(message.chat.id, None)
    SENDALLRANGE_STATES.pop(message.chat.id, None)
    USER_STATES.pop(message.chat.id, None)
    CLONE_STATES.pop(message.chat.id, None)
    CREATE_STATES.pop(message.chat.id, None)
    PUBLIC_STATES.pop(message.chat.id, None)
    FBATCH_STATES.pop(message.chat.id, None)
    MKCHANNELS_STATES.pop(message.chat.id, None)
    if message.reply_to_message:
        status_msg = await message.reply_text(
            "📢 **Sendall Started!** Sab channels me ek-ek karke bhej raha hun...")
        await run_sendall(client, message.chat.id, message.reply_to_message, status_msg)
        return
    SENDALL_STATES[message.chat.id] = True
    await message.reply_text(
        "**📢 VILLAN SENDALL**\n"
        "**━━━━━━━━━━━━━━━**\n\n"
        "**Ab wo message bhejo jo sab channels me bhejna hai**\n"
        "(text / photo / video / document / animation).\n\n"
        "Bhejte hi wo tumhare saved saare channels me jayega.\n"
        "Channels dekhne ke liye: `/mychannels`\n\n"
        "**❌ Cancel:** `/cancel`",
        parse_mode=enums.ParseMode.MARKDOWN,
    )



@app.on_message(filters.command("addchannel") & filters.private)
async def addchannel_start_handler(client, message):
    if not await _has_joined(message.chat.id, force=True):
        await _send_join_wall(message)
        return
    if not _is_allowed(message.chat.id):
        await message.reply_text("⛔ Access denied.")
        return
    if not _is_premium(message.chat.id):
        await message.reply_text(
            "⛔ Ye command sirf premium members ke liye hai.\n"
            "Normal user /batch se save-restriction use kar sakta hai.")
        return
    print(f"⚡ Processing /addchannel for user {message.chat.id}", flush=True)
    USER_STATES.pop(message.chat.id, None)
    CLONE_STATES.pop(message.chat.id, None)
    CREATE_STATES.pop(message.chat.id, None)
    PUBLIC_STATES.pop(message.chat.id, None)
    FBATCH_STATES.pop(message.chat.id, None)
    MKCHANNELS_STATES.pop(message.chat.id, None)
    SENDALL_STATES.pop(message.chat.id, None)
    SENDALLRANGE_STATES.pop(message.chat.id, None)
    # Seedha /addchannel <link> bhi chalega
    args = message.command[1:] if len(message.command) > 1 else []
    if args:
        # Har arg ek alag channel ho sakta hai (space se alag), aur
        # har arg ke andar comma bhi ho sakta hai — dono ko todo.
        parts = []
        for _a in args:
            parts.extend(_add_parse_channels(_a))
        status_msg = await message.reply_text(
            "📢 **Addchannel Started!** Channel verify karke list me jod raha hun...")
        await run_addchannels(client, message.chat.id, parts, status_msg)
        return
    ADDCHANNEL_STATES[message.chat.id] = {"step": "WAITING_LINK"}
    await message.reply_text(
        "**📢 VILLAN ADDCHANNEL — Pehle se bana channel jodo**\n"
        "**━━━━━━━━━━━━━━━**\n\n"
        "**Ab channel ka link / username bhejo**\n"
        "(ek ya zyada, har ek ek nayi line par, ya comma se).\n"
        "Jaise:\n"
        "`https://t.me/mychannel`\n`@mychannel`\n`https://t.me/c/1234567890`\n\n"
        "• Channel **tumhare login account se verify** hoga.\n"
        "• Login account us channel ka **admin/creator** hona chahiye,\n"
        "  warna add to ho jayega par sendall ke time fail hoga —\n"
        "  isliye pehle admin bana lo, to behtar hai.\n"
        "• Ye channel `/mychannels` list me aa jayega, aur `/sendall` /\n"
        "  `/sendallrange` dono par kaam karega.\n\n"
        "**❌ Cancel:** `/cancel`",
        parse_mode=enums.ParseMode.MARKDOWN,
    )


@app.on_message(filters.command("sendallrange") & filters.private)
async def sendallrange_start_handler(client, message):
    if not await _has_joined(message.chat.id, force=True):
        await _send_join_wall(message)
        return
    if not _is_allowed(message.chat.id):
        await message.reply_text("⛔ Access denied.")
        return
    if not _is_premium(message.chat.id):
        await message.reply_text(
            "⛔ Ye command sirf premium members ke liye hai.\n"
            "Normal user /batch se save-restriction use kar sakta hai.")
        return
    print(f"⚡ Processing /sendallrange for user {message.chat.id}", flush=True)
    USER_STATES.pop(message.chat.id, None)
    CLONE_STATES.pop(message.chat.id, None)
    CREATE_STATES.pop(message.chat.id, None)
    PUBLIC_STATES.pop(message.chat.id, None)
    FBATCH_STATES.pop(message.chat.id, None)
    MKCHANNELS_STATES.pop(message.chat.id, None)
    SENDALL_STATES.pop(message.chat.id, None)
    ADDCHANNEL_STATES.pop(message.chat.id, None)
    SENDALLRANGE_STATES[message.chat.id] = {"step": "WAITING_FIRST"}
    await message.reply_text(
        "**📢 VILLAN SENDALLRANGE — 50 message tak, sab channels me**\n"
        "**━━━━━━━━━━━━━━━**\n\n"
        "**STEP 1/2 — Pehli message ka link bhejo**\n"
        "Jaise: `https://t.me/mychannel/100`\n\n"
        "Uske baad aakhri message ka link poochhunga.\n"
        "Range **max 50 message** hai — zyada ho to pehle 50 hi jayenge.\n"
        "Audio / video / text / photo — sab copy honge, sequencewise.\n"
        "Har send ke beech 2 sec ka gap hai, isliye 50 message * 10 channel\n"
        "ka kaam ~17 min tak chal sakta hai.\n\n"
        "**❌ Cancel:** `/cancel`",
        parse_mode=enums.ParseMode.MARKDOWN,
    )


@app.on_message(filters.command("fbatch") & filters.private)
async def fbatch_start_handler(client, message):
    if not await _has_joined(message.chat.id, force=True):
        await _send_join_wall(message)
        return
    if not _is_allowed(message.chat.id):
        await message.reply_text("⛔ Access denied.")
        return
    print(f"⚡ Processing /fbatch for user {message.chat.id}", flush=True)
    ADDCHANNEL_STATES.pop(message.chat.id, None)
    SENDALLRANGE_STATES.pop(message.chat.id, None)
    MKCHANNELS_STATES.pop(message.chat.id, None)
    SENDALL_STATES.pop(message.chat.id, None)
    USER_STATES.pop(message.chat.id, None)
    CLONE_STATES.pop(message.chat.id, None)
    CREATE_STATES.pop(message.chat.id, None)
    ORGANIZE_STATES.pop(message.chat.id, None)
    ARRANGE_STATES.pop(message.chat.id, None)
    PUBLIC_STATES.pop(message.chat.id, None)
    FBATCH_STATES[message.chat.id] = {"step": "WAITING_SOURCE"}
    await message.reply_text(
        "**📂 VILLAN FBATCH — folderwise batch, sequencewise, NO DELETE**\n"
        "**━━━━━━━━━━━━━━━**\n\n"
        "**Pehle SOURCE group/channel ka link bhejo**\n"
        "(jisme video / pdf / text / pic hain).\n\n"
        "Uske baad DEST group ka link poochhunga.\n\n"
        "Dest me **sirf bot admin** ho to bhi chalega.\n"
        "Har file par **download + upload progress** dikhega,\n"
        "jaise /batch me dikhta hai. **Delete kuch nahi hoga.**\n\n"
        "**❌ Cancel:** `/cancel`",
        parse_mode=enums.ParseMode.MARKDOWN,
    )


PUBLIC_STATES = {}  # uid -> {"step": "WAITING_FIRST_LINK"/"WAITING_LAST_LINK", ...}


@app.on_message(filters.command("public") & filters.private)
async def public_start_handler(client, message):
    if not await _has_joined(message.chat.id, force=True):
        await _send_join_wall(message)
        return
    if not _is_allowed(message.chat.id):
        await message.reply_text("⛔ Access denied.")
        return
    if not _is_premium(message.chat.id):
        await message.reply_text(
            "⛔ Ye command sirf premium members ke liye hai.\n"
            "Normal user /batch se save-restriction use kar sakta hai.")
        return
    print(f"⚡ Processing /public for user {message.chat.id}", flush=True)
    ADDCHANNEL_STATES.pop(message.chat.id, None)
    SENDALLRANGE_STATES.pop(message.chat.id, None)
    MKCHANNELS_STATES.pop(message.chat.id, None)
    SENDALL_STATES.pop(message.chat.id, None)
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
    if data == "resume_batch":
        try:
            await query.answer("▶️ Resuming batch...")
        except Exception:
            pass
        await _handle_resume(client, uid, query.message)
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
async def mkchannels_dp_handler(client, message):
    uid = message.chat.id
    st = MKCHANNELS_STATES.get(uid)
    if not st or st.get("step") != "WAITING_DP":
        return
    if not _is_allowed(uid):
        MKCHANNELS_STATES.pop(uid, None)
        await message.reply_text("⛔ Access denied.")
        return
    # Photo download karo, ek hi file 10 channels par lagegi
    try:
        dp_path = await message.download(file_name=os.path.join("downloads", f"mkdp_{uid}.jpg"))
    except Exception as e:
        await message.reply_text(f"❌ DP download nahi hui: {e}\nDobara photo bhejo, ya `skip` likho.")
        return
    MKCHANNELS_STATES[uid] = {"step": "WAITING_BIO", "title": st.get("title"), "dp_path": dp_path}
    await message.reply_text(
        "**✅ DP mil gayi!**\n\n"
        "**STEP 3/4 — Ab channel ki BIO bhejo**\n"
        "Ye bio **sab channels me ek hi** lagegi (max 255 chars).\n"
        "Jaise: `Dholu Bholu Kids — Learn, Play, Grow Together`\n"
        "Ya `skip` likho to default bio rahegi.\n\n"
        "**❌ Cancel:** `/cancel`",
        parse_mode=enums.ParseMode.MARKDOWN)


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
    (filters.photo | filters.video | filters.document | filters.animation | filters.audio | filters.sticker)
    & filters.private
)
async def sendall_media_handler(client, message):
    uid = message.chat.id
    if uid not in SENDALL_STATES:
        return
    if not await _has_joined(uid):
        await _send_join_wall(message)
        return
    if not _is_allowed(uid):
        SENDALL_STATES.pop(uid, None)
        await message.reply_text("⛔ Access denied.")
        return
    SENDALL_STATES.pop(uid, None)
    status_msg = await message.reply_text(
        "📢 **Sendall Started!** Sab channels me ek-ek karke bhej raha hun...")
    await run_sendall(client, uid, message, status_msg)


@app.on_message(
    filters.text
    & filters.private
    & ~filters.command(
        ["start", "batch", "cancel", "login", "logout", "settings",
         "allow", "unallow", "users", "paid", "plan", "help", "buy",
         "mkchannels", "mychannels", "sendall",
         "addchannel", "sendallrange"]
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

    # Mkchannels flow: pehle naam, phir username (v63 — naam ek hi, username alag)
    if user_id in MKCHANNELS_STATES:
        _mk_step = MKCHANNELS_STATES[user_id].get("step")
        if _mk_step == "WAITING_NAME":
            title = text.strip()[:128]
            if not title:
                await message.reply_text("❌ Naam khaali hai. Channel ka naam dobara bhejo, jaise `Dholu Bholu Kids`.")
                return
            MKCHANNELS_STATES[user_id] = {"step": "WAITING_DP", "title": title}
            await message.reply_text(
                f"**📢 Naam locked: `{title}`**\n\n"
                "**STEP 2/4 — Ab channel ki DP bhejo (photo),**\n"
                "ya `skip` likho to default DP rahega (naam ka pehla akshar wala).\n"
                "DP ke baad bio poochhunga, phir username.\n\n"
                "**❌ Cancel:** `/cancel`",
                parse_mode=enums.ParseMode.MARKDOWN,
            )
            return
        if _mk_step == "WAITING_DP":
            # Text aaya hai, photo nahi — sirf skip/default maano, warna dobara photo maango
            if text.strip().lower() in ("skip", "default", "no", "none"):
                MKCHANNELS_STATES[user_id] = {"step": "WAITING_BIO", "title": MKCHANNELS_STATES[user_id].get("title"), "dp_path": None}
                await message.reply_text(
                    "**STEP 3/4 — Ab channel ki BIO bhejo**\n"
                    "Ye bio **sab channels me ek hi** lagegi (max 255 chars).\n"
                    "Jaise: `Dholu Bholu Kids — Learn, Play, Grow Together`\n"
                    "Ya `skip` likho to default bio rahegi.\n\n"
                    "**❌ Cancel:** `/cancel`",
                    parse_mode=enums.ParseMode.MARKDOWN,
                )
                return
            await message.reply_text(
                "📸 DP ke liye **photo bhejo**, ya `skip` likho to default DP rahega.")
            return
        if _mk_step == "WAITING_BIO":
            if text.strip().lower() in ("skip", "default", "no", "none"):
                bio = MKCHANNELS_DEFAULT_BIO
            else:
                bio = text.strip()[:255]
            # v67 fix: title state se pehle nikaal lo, warna neeche reply
            # me {title} UnboundLocalError dega (v66 me yahi crash tha).
            title = MKCHANNELS_STATES[user_id].get("title")
            MKCHANNELS_STATES[user_id] = {"step": "WAITING_USERNAMES", "title": title, "bio": bio, "dp_path": MKCHANNELS_STATES[user_id].get("dp_path")}
            await message.reply_text(
                f"**📢 Bio locked.**\n\n"
                "**STEP 4/4 — Ab 10 username bhejo — ek hi message me,**\n"
                "har username ek nayi line par, ya comma se alag karke.\n"
                "Jaise:\n`myfirstchannel`\n`mysecondchannel`\n\n"
                f"• **Sab channels ka naam ek hi hoga:** `{title}`\n"
                "• Sirf **username alag-alag** hoga.\n"
                "• Ek run me **max 10** username, har channel ke beech **2 MIN ka gap** hai.\n\n"
                "**❌ Cancel:** `/cancel`",
                parse_mode=enums.ParseMode.MARKDOWN,
            )
            return
        _mk_title = MKCHANNELS_STATES[user_id].get("title")
        _mk_bio = MKCHANNELS_STATES[user_id].get("bio") or MKCHANNELS_DEFAULT_BIO
        _mk_dp = MKCHANNELS_STATES[user_id].get("dp_path")
        raw_names = _mk_split_raw(text)
        usernames = _mk_parse_usernames(text)
        if not usernames:
            await message.reply_text(
                "❌ Koi valid username nahi mila. Username dobara bhejo —\n"
                "5-32 chars, pehla char letter, sirf a-z / 0-9 / _.\n"
                "Jaise: `myfirstchannel`")
            return
        MKCHANNELS_STATES.pop(user_id, None)
        skipped = [n for n in raw_names if n not in usernames]
        if len(usernames) > MKCHANNELS_MAX:
            usernames = usernames[:MKCHANNELS_MAX]
        status_msg = await message.reply_text(
            f"📢 **Mkchannels Started!**\nUsernames: `{len(usernames)}` — ek-ek karke public channel bana raha hun...\n"
            f"⏱️ Har channel ke beech 2 min ka gap hai, isliye poora kaam ~`{(len(usernames) - 1) * 2}` min tak chal sakta hai. Beech me `/cancel` kar sakte ho.")
        await run_mkchannels(client, user_id, usernames, status_msg, title=_mk_title, bio=_mk_bio, dp_path=_mk_dp)
        if skipped:
            await message.reply_text(
                "⚠️ Ye username invalid the, isliye skip kiye:\n" +
                "\n".join(f"• @{s}" for s in skipped))
        if len(raw_names) > MKCHANNELS_MAX:
            await message.reply_text(
                f"⚠️ Sirf pehle `{MKCHANNELS_MAX}` username liye hain (limit). Baaki ke liye dobara `/mkchannels` karo.")
        return

    # Sendall flow (text message source): /sendall ke baad bheja gaya text
    if user_id in SENDALL_STATES:
        SENDALL_STATES.pop(user_id, None)
        status_msg = await message.reply_text(
            "📢 **Sendall Started!** Sab channels me ek-ek karke bhej raha hun...")
        await run_sendall(client, user_id, message, status_msg)
        return

    # Addchannel flow: channel link ka jawab aa raha ho
    if user_id in ADDCHANNEL_STATES:
        parts = _add_parse_channels(text)
        if not parts:
            await message.reply_text(
                "❌ Koi channel link nahi mila. Channel ka link / username dobara bhejo.")
            return
        ADDCHANNEL_STATES.pop(user_id, None)
        status_msg = await message.reply_text(
            "📢 **Addchannel Started!** Channel verify karke list me jod raha hun...")
        await run_addchannels(client, user_id, parts, status_msg)
        return

    # Sendallrange flow: pehli link, phir aakhri link
    if user_id in SENDALLRANGE_STATES:
        step = SENDALLRANGE_STATES[user_id].get("step")
        if step == "WAITING_FIRST":
            chat_id, start_id = parse_tg_link(text)
            if not chat_id or not start_id:
                await message.reply_text(
                    "❌ Invalid link. Pehli message ka link bhejo, jaise `https://t.me/mychannel/100`.")
                return
            SENDALLRANGE_STATES[user_id] = {"step": "WAITING_LAST", "chat_id": chat_id, "start_id": start_id}
            await message.reply_text(
                "**Sendallrange — Start locked.**\n\n"
                f"Start: `{start_id}`\n"
                "Ab **aakhri message ka link** bhejo.\n\n"
                "**❌ Cancel:** `/cancel`",
                parse_mode=enums.ParseMode.MARKDOWN,
            )
            return
        if step == "WAITING_LAST":
            chat_id, end_id = parse_tg_link(text)
            if not chat_id or not end_id:
                await message.reply_text(
                    "❌ Invalid link. Aakhri message ka link bhejo.")
                return
            state = SENDALLRANGE_STATES.pop(user_id)
            if state["chat_id"] != chat_id:
                await message.reply_text("❌ Dono link ek hi channel ke hone chahiye.")
                return
            start_id = state["start_id"]
            if start_id > end_id:
                start_id, end_id = end_id, start_id
            total = end_id - start_id + 1
            if total > SENDALLRANGE_MAX:
                end_id = start_id + SENDALLRANGE_MAX - 1
                total = SENDALLRANGE_MAX
                await message.reply_text(
                    f"⚠️ Range `{SENDALLRANGE_MAX}` se zyada tha, isliye sirf pehle `{SENDALLRANGE_MAX}` message bheje jayenge. Baaki ke liye dobara `/sendallrange` karo.")
            channels_n = len(_channels_load(user_id))
            status_msg = await message.reply_text(
                f"📢 **Sendallrange Started!**\nMessages: `{total}` • Channels: `{channels_n}` • Total send: `{total * channels_n}`\n"
                f"⏱️ Lagbhag `{total * channels_n * SENDALL_GAP // 60}` min tak chal sakta hai.")
            await run_sendallrange(client, user_id, chat_id, start_id, end_id, status_msg)
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

    # Arrange flow: pehle source link, phir dest link
    if user_id in ARRANGE_STATES:
        step = ARRANGE_STATES[user_id].get("step")
        if step == "WAITING_SOURCE":
            src_chat = _arrange_parse_link(text)
            if src_chat is None:
                await message.reply_text("❌ **Invalid link!** Source group/channel ka link bhejo "
                                         "(jaise `https://t.me/c/1234567890`, `@groupname`, "
                                         "ya invite link `https://t.me/+xxxx`).")
                return
            ARRANGE_STATES[user_id] = {"step": "WAITING_DEST", "src": src_chat}
            await message.reply_text(
                "**📂 Source link lock ho gaya.**\n\n"
                "**Ab DEST group ka link bhejo**\n"
                "(jisme folder bana ke bhejna hai).\n\n"
                "Dest group me **Topics ON** hona chahiye, aur bot +\n"
                "tumhara login account dono dest me admin hone chahiye.\n\n"
                "Dest link bhejte hi process start ho jayega,\n"
                "sequencewise copy hogi.\n\n"
                "**❌ Cancel:** `/cancel`",
                parse_mode=enums.ParseMode.MARKDOWN,
            )
            return
        if step == "WAITING_DEST":
            dest_chat = _arrange_parse_link(text)
            if dest_chat is None:
                await message.reply_text("❌ **Invalid link!** Dest group ka link bhejo "
                                         "(jaise `https://t.me/c/1234567890`, `@groupname`, "
                                         "ya invite link `https://t.me/+xxxx`).")
                return
            src_chat = ARRANGE_STATES[user_id].get("src")
            ARRANGE_STATES.pop(user_id, None)
            status_msg = await message.reply_text(
                "📂 **Arrange Started!** Source se dest tak, folder bana ke, "
                "sequencewise copy kar raha hun (delete nahi)..."
            )
            await run_arrange(client, user_id, src_chat, dest_chat, status_msg)
            return

    if user_id in FBATCH_STATES:
        step = FBATCH_STATES[user_id].get("step")
        if step == "WAITING_SOURCE":
            src_chat = _clone_parse_link(text)
            if src_chat is None:
                await message.reply_text("❌ **Invalid link!** Source group/channel ka link bhejo.")
                return
            FBATCH_STATES[user_id] = {"step": "WAITING_DEST", "src": src_chat}
            await message.reply_text(
                "**📂 Source link lock ho gaya.**\n\n"
                "**Ab DEST group ka link bhejo**\n"
                "Dest group me **Topics ON** hona chahiye, aur bot dest me admin hona chahiye.\n\n"
                "**❌ Cancel:** `/cancel`",
                parse_mode=enums.ParseMode.MARKDOWN,
            )
            return
        if step == "WAITING_DEST":
            dest_chat = _clone_parse_link(text)
            if dest_chat is None:
                await message.reply_text("❌ **Invalid link!** Dest group ka link bhejo.")
                return
            src_chat = FBATCH_STATES[user_id].get("src")
            FBATCH_STATES.pop(user_id, None)
            status_msg = await message.reply_text(
                "📂 **Fbatch Started!** Source se dest tak, folder bana ke, sequencewise copy kar raha hun (delete nahi, progress ke saath)..."
            )
            await run_fbatch(client, user_id, src_chat, dest_chat, status_msg)
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
# v62: Broadcast me sirf "BOT UPDATED" likhna kaafi nahi tha — user ne bola
# ki restart hone par ye bhi batana chahiye ki restart KYUN hua aur bot me
# KYA ADD hua hai. Isliye har version ke saath ye notes update karo, aur
# _broadcast_update() inhe hi bhejega. Notes HTML me hain (broadcast
# parse_mode=HTML hai), isliye < > mat likhna, sirf • wali lines.
UPDATE_NOTES = (
    "• /fbatch aur /public ka fix — pehle NameError aata tha, ab nahi\n"
    "• /mkchannels — 10 username se PUBLIC channel banao, har channel ke beech 2 min ka gap\n"
    "• /mychannels aur /sendall — ek message sab channels me bhejo\n"
    "• /addchannel — pehle se bana channel bhi broadcast list me jodo\n"
    "• /sendallrange — pehli-aakhri link se 50 message tak sab channels me ek saath\n"
    "• /mkchannels — ab 10 channels ka naam ek hi hoga, sirf username alag-alag hoga\n"
    "• /mkchannels — ab DP aur bio bhi poochhega, naam + DP + bio sab channels par ek saath lagega (DP na do to default rahega)\n"
    "• /sendall aur /sendallrange — ab bot fallback bhi hai, login account fail ho to bot admin se bhejega (Post Messages ON hona chahiye)\n"
    "• /mkchannels — har naye channel me bot bhi admin banega (Post Messages ON), taaki 1 account se sab par broadcast ho sake; /organize aur /arrange hata diye gaye, folderwise kaam sirf /fbatch se hoga\n"
    "• /mkchannels bio step ka crash fix — bio bhejte hi jo UnboundLocalError aata tha aur flow atak jata tha (isliye DP kabhi lagti hi nahi thi), wo theek hua\n"
    "• /clone list topics fix — deleted topic milne par poori folder list fail nahi hogi, deleted topics skip ho jayenge\n"
    "• Premium / Normal system — /public aur channel commands sirf premium members ke liye, normal users /batch use kar sakte hain; /clone me download/upload progress dikhega\n"
    "• /mkchannels bot admin fix — ab bot ko pehle channel me add karke phir admin banaya jayega, taaki bot admin fail na ho\n"
    "• /paid aur /allow commands hata diye gaye — premium ke liye /premium, normal ke liye /normal use hoga\n"
)
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
        "<b>\U0001f504 BOT RESTARTED — naya version deploy hua hai</b>\n\n"
        "Bot crash nahi hua hai. Naya update live karne ke liye bot ko "
        "restart kiya gaya hai, isliye chal raha batch ruk gaya hoga.\n\n"
        "<b>Is update me ye add hua hai:</b>\n"
        + UPDATE_NOTES +
        "\n\n<b>Agar tumhara batch ruk gaya ho to dobara /batch bhejo.</b>"
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
            BotCommand("mkchannels", "10 username se PUBLIC channels banao"),
            BotCommand("mychannels", "Tumhare channels ki list"),
            BotCommand("sendall", "Ek message sab channels me bhejo"),
            BotCommand("addchannel", "Pehle se bana channel jodo"),
            BotCommand("sendallrange", "50 message tak sab channels me bhejo"),
            BotCommand("fbatch", "Folderwise batch, sequencewise copy"),
            BotCommand("resume", "Ruka hua batch wahi se dobara shuru karo"),
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
