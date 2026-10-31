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
VERSION = "v78"

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
# v72 /free: ek ID par sirf ek baar, 10 messages ka trial.
FREE_FILE = os.path.join(DATA_DIR, "free_used.json")
FREE_LIMIT = 10
# v72 /ai: FREE stock Qwen 3.8-27B via IteraCompute (uncensored NAHI hai;
# uncensored hosted API paid hai). Free tier: 30M tokens/month, no card,
# shared queue, promotion 24 Nov 2026 tak. Key Koyeb env me, chat me nahi.
QWEN_API_KEY = os.environ.get("QWEN_API_KEY", "").strip()
QWEN_BASE_URL = os.environ.get("QWEN_BASE_URL", "https://api.iteracompute.com/v1").strip().rstrip("/")
QWEN_MODEL = os.environ.get("QWEN_MODEL", "qwen/qwen3.8-27b").strip()
# v72: Ek batch me max 5000 posts (user ne 5000 bola hai). Purana
# Koyeb env BATCH_LIMIT=1000 ho to bhi 5000 hi chalega — fixed hai.
BATCH_LIMIT = 5000
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
                  "user_settings.json", "user_topics.json", "channels.json", "free_used.json")
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
        "free_used.json": FREE_FILE,
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



def _free_load():
    data = _load_json(FREE_FILE, {})
    return data if isinstance(data, dict) else {}


def _free_save(data):
    _save_json(FREE_FILE, data)


def _free_remaining(uid):
    """Free trial user ke bache hue messages, ya None (trial user nahi)."""
    data = _free_load()
    entry = data.get(str(uid))
    if not isinstance(entry, dict):
        return None
    return int(entry.get("remaining", 0))


def _free_consume(uid, n):
    """Batch start hote hi quota kato. Khatam ho to allowed se bhi hatao
    (trial khatam), par free_used record rakho taaki /free dobara na chale."""
    data = _free_load()
    entry = data.get(str(uid))
    if not isinstance(entry, dict):
        return
    entry["remaining"] = max(0, int(entry.get("remaining", 0)) - int(n))
    data[str(uid)] = entry
    _free_save(data)
    if entry["remaining"] <= 0:
        allowed = _load_allowed()
        allowed.discard(uid)
        _save_allowed(allowed)


async def _qwen_chat(user_text, system_text="You are a helpful assistant. Answer in the user's language, short and clear."):
    """FREE Qwen 3.8-27B (stock) via IteraCompute, OpenAI-compatible API."""
    import aiohttp as _aio
    if not QWEN_API_KEY:
        raise RuntimeError("QWEN_API_KEY set nahi hai. iteracompute.com/free par free account banao, key banao, Koyeb env me QWEN_API_KEY daalo, phir dobara try karo. Key chat me kabhi mat bhejo.")
    payload = {"model": QWEN_MODEL,
               "messages": [{"role": "system", "content": system_text},
                            {"role": "user", "content": user_text}],
               "max_tokens": 1024, "temperature": 0.7}
    headers = {"Authorization": f"Bearer {QWEN_API_KEY}", "Content-Type": "application/json"}
    timeout = _aio.ClientTimeout(total=180)
    async with _aio.ClientSession(timeout=timeout) as session:
        async with session.post(f"{QWEN_BASE_URL}/chat/completions",
                                headers=headers, json=payload) as r:
            body = await r.text()
            if r.status == 429:
                raise RuntimeError("Qwen free limit/queue: 429 mila — mahine ke 30M free tokens khatam ho sakte hain, ya shared capacity me queue hai. Thodi der baad try karo.")
            if r.status != 200:
                raise RuntimeError(f"Qwen fail ({r.status}): {body[:500]}")
            data = json.loads(body)
    try:
        return data["choices"][0]["message"]["content"].strip()
    except Exception:
        raise RuntimeError(f"Qwen se jawab samajh nahi aaya: {body[:500]}")


async def _bot_make_admin(userbot, cid, bot_id, bot_username):
    """Channel me bot ko add + admin (Post + Edit Messages ON) karo.
    v70 ka invite-then-promote tareeka, ab /mkchannels aur /makeadmin
    dono isi helper se karte hain, taaki ek jagah fix dono jagah lage.
    Returns warn string ("" = success)."""
    if not bot_id:
        return "bot id nahi mila (get_me fail)"
    from pyrogram.types import ChatPrivileges
    _priv = ChatPrivileges(
        can_post_messages=True, can_edit_messages=True,
        can_delete_messages=False, can_manage_chat=False,
        can_restrict_members=False, can_promote_members=False,
        can_change_info=False, can_invite_users=False,
        can_pin_messages=False, can_manage_topics=False)
    try:
        await userbot.promote_chat_member(cid, bot_id, privileges=_priv)
        return ""
    except Exception as e1:
        try:
            await userbot.add_chat_members(cid, bot_id)
        except Exception:
            try:
                from pyrogram import raw as _raw
                _bot_peer = await userbot.resolve_peer(bot_username) if bot_username else await userbot.resolve_peer(bot_id)
                _chan_peer = await userbot.resolve_peer(cid)
                await userbot.invoke(_raw.functions.channels.InviteToChannel(
                    channel=_chan_peer, users=[_bot_peer]))
            except Exception:
                pass
        try:
            await userbot.promote_chat_member(cid, bot_id, privileges=_priv)
            return ""
        except Exception as e2:
            return f"bot admin fail: {e2} (pehle try: {e1})"


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
        _ext = {"photo": ".jpg", "voice": ".ogg", "video": ".mp4", "video_note": ".mp4",
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
            # v72 VIDEO-FORMAT FIX: video lecture ka format na badle.
            # (1) Bina extension wali video file ka mime octet-stream aa
            # jata tha — kind==video ho to video/mp4 force karo.
            # (2) Video jo DOCUMENT ki tarah bheji gayi thi (coaching
            # lectures aksar file ki tarah aate hain, .mp4 ke saath),
            # us par sirf Filename attribute lagta tha, isliye client
            # use playable video ki jagah generic file dikhata tha.
            # Ab document + video mime/extension par Video attribute
            # bhi lagao, taaki wo video ki tarah hi dikhe/play ho.
            _lower = fname.lower()
            _is_video_file = (kind == "video" or mime.startswith("video/")
                              or _lower.endswith((".mp4", ".mkv", ".mov", ".webm")))
            if kind == "video" and mime == "application/octet-stream":
                mime = "video/mp4"
            attrs = [raw.types.DocumentAttributeFilename(file_name=fname)]
            if kind == "video" or (_is_video_file and kind == "document"):
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
MKCHANNELS_DEFAULT_BIO = "ALL BRANCHES LATEST BATCHES JUST  DM AND JOIN UR BATCH :- @The_Og_VillaN"
# v72 DEFAULT DP: user ki di hui MR. VILLAIN logo photo. Ye file main.py
# ke saath hi deploy karni hai, naam "default_dp.jpg" rakh ke. DP skip
# karne par isi photo se channel ki DP lagegi (letter avatar nahi).
MKCHANNELS_DEFAULT_DP = "default_dp.jpg"


# v78 EMBEDDED DEFAULT DP: MR. VILLAIN logo (90KB jpg) code ke andar
# hi hai. Agar default_dp.jpg file deploy na hui ho, to _default_dp_path()
# isi se file bana lega — sirf main.py deploy karne par bhi default DP
# kaam karegi. Ye user ki di hui wahi logo photo hai.
_DEFAULT_DP_B64 = (
    "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"
    "WZ//2Q=="
)


def _default_dp_path():
    for cand in (os.path.join(os.path.dirname(os.path.abspath(__file__)), MKCHANNELS_DEFAULT_DP),
                 MKCHANNELS_DEFAULT_DP):
        if cand and os.path.exists(cand):
            return cand
    # v78: file deploy na hui ho to embedded logo se file bana lo.
    try:
        import base64 as _b64
        _out = os.path.join(os.path.dirname(os.path.abspath(__file__)), MKCHANNELS_DEFAULT_DP)
        with open(_out, "wb") as _f:
            _f.write(_b64.b64decode(_DEFAULT_DP_B64))
        if os.path.exists(_out):
            return _out
    except Exception as _e:
        print(f"[MKCHANNELS] embedded default DP likh nahi paya: {_e}", flush=True)
    return None
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
    bot_warn = await _bot_make_admin(userbot, cid, bot_id, bot_username)
    if bot_warn:
        print(f"[MKCHANNELS] bot admin fail @{username}: {bot_warn}", flush=True)
    return cid, f"https://t.me/{username}", bot_warn


async def run_mkchannels(client, uid, usernames, status_msg, title=None, bio=None, dp_path=None):
    _ev = asyncio.Event()
    MKCHANNELS_ACTIVE[uid] = _ev
    try:
        await _run_mkchannels_inner(client, uid, usernames, status_msg, _ev, title=title, bio=bio, dp_path=dp_path)
    finally:
        MKCHANNELS_ACTIVE.pop(uid, None)
        # DP temp file sirf is run ke liye thi, khatam hone par delete karo.
        # v72: default DP file (default_dp.jpg) ko KABHI delete mat karo,
        # warna agle run me default DP gayab ho jayegi.
        if dp_path and dp_path != _default_dp_path() and os.path.exists(dp_path):
            try:
                os.remove(dp_path)
            except Exception:
                pass


async def _run_mkchannels_inner(client, uid, usernames, status_msg, _ev, title=None, bio=None, dp_path=None):
    # v72: DP skip ki ho to default MR. VILLAIN logo wali DP lagao.
    _dp_is_default = False
    if not dp_path:
        _def_dp = _default_dp_path()
        if _def_dp:
            dp_path = _def_dp
            _dp_is_default = True
    userbot = await get_userbot(uid)
    if not userbot:
        await message_reply(status_msg, "❌ Pehle `/login` karo — public channel "
                            "tumhare account se banega, isliye login session zaroori hai.")
        return
    print(f"[MKCHANNELS] run start uid={uid} dp_path={dp_path} exists={os.path.exists(dp_path) if dp_path else False} size={(os.path.getsize(dp_path) if dp_path and os.path.exists(dp_path) else 0)}", flush=True)
    existing = _channels_load(uid)
    by_name = {c.get("username"): c for c in existing if isinstance(c, dict)}
    created, failed = [], []
    bot_admin_failed = []
    dp_failed = []
    # v74: skip DP par default file hi na mile to ye True rahega — pehle
    # ye chupchaap letter avatar ban jata tha, ab report me warning aayegi.
    dp_default_missing = (dp_path is None)
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
        if cid and dp_path and not os.path.exists(dp_path):
            dp_warn = " (DP file nahi mili)"
            print(f"[MKCHANNELS] DP file nahi mili @{username}: {dp_path}", flush=True)
        if cid and dp_path and os.path.exists(dp_path):
            # DP alag call se lagti hai (CreateChannel me photo nahi hoti).
            # DP fail ho to channel delete mat karo — channel + username
            # ban chuka hai, sirf DP ka warning report me de do.
            # v78: pehle normal set_chat_photo, fail ho to raw
            # channels.EditPhoto fallback, taaki method ka farq na roke.
            try:
                await userbot.set_chat_photo(cid, photo=dp_path)
                print(f"[MKCHANNELS] DP set ok @{username}", flush=True)
            except Exception as e:
                print(f"[MKCHANNELS] DP set fail @{username}: {e} — raw fallback try kar raha hun", flush=True)
                try:
                    from pyrogram import raw as _raw
                    _peer = await userbot.resolve_peer(cid)
                    _up = await userbot.save_file(dp_path)
                    await userbot.invoke(_raw.functions.channels.EditPhoto(
                        channel=_peer, photo=_raw.types.InputChatUploadedPhoto(file=_up)))
                    print(f"[MKCHANNELS] DP set ok via fallback @{username}", flush=True)
                except Exception as e2:
                    dp_warn = f" (DP fail: {e} / fallback fail: {e2})"
                    print(f"[MKCHANNELS] DP fallback bhi fail @{username}: {e2}", flush=True)
        if dp_warn:
            dp_failed.append((username, dp_warn))
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
            f"🖼️ DP: `{'default MR. VILLAIN logo' if _dp_is_default else ('tumhari photo' if dp_path else 'default (naam ka pehla akshar)')}` • 📝 Bio: `{((bio or MKCHANNELS_DEFAULT_BIO)[:80])}`")
    for c in created:
        done += f"\n• {c.get('title', c['username'])} — @{c['username']} — {c['link']}"
    if dp_default_missing:
        done += ("\n⚠️ Default DP file server par NAHI mili (default_dp.jpg main.py ke saath deploy nahi hui), "
                 "isliye in channels par letter avatar raha. File ko main.py ke saath hi deploy karo, phir dobara test karo.")
    if dp_failed:
        done += f"\n🖼️ DP fail hue: `{len(dp_failed)}` channels"
        for uname, err in dp_failed:
            done += f"\n• @{uname} — {err}"
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

async def _copy_to_channel(client, userbot, dest, src_bot, src_userbot, msg_id, public_source):
    """Ek message ko ek channel me bhejo — FAST copy/forward, download/upload NAHI.
    Public source ho to BOT copy pehle (bot public source padh sakta hai, aur
    dest me bot admin hai to userbot ki zaroorat nahi), phir userbot.
    Private source ho to pehle userbot, phir bot fallback (pehle jaisa).
    Returns (ok, via_bot)."""
    senders = []
    if public_source:
        senders = [(client, src_bot, True), (userbot, src_userbot, False)]
    else:
        senders = [(userbot, src_userbot, False), (client, src_bot, True)]
    for sender, src, via_bot in senders:
        if sender is None or src is None:
            continue
        try:
            await sender.copy_message(dest, src, msg_id)
            return True, via_bot
        except Exception:
            pass
        try:
            await sender.forward_messages(dest, src, [msg_id])
            return True, via_bot
        except Exception:
            continue
    return False, False


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
        # v72 FAST: source bot-chat me hai, isliye bot copy pehle (public fast
        # tareeka), fail ho to userbot. Download/upload kabhi nahi hota.
        ok, via_bot = await _copy_to_channel(
            client, userbot, dest, source_msg.chat.id, from_chat,
            source_msg.id, public_source=True)
        if ok:
            sent.append(uname)
            if via_bot:
                bot_via += 1
        else:
            failed.append((uname, "copy/forward fail (bot + userbot dono fail)"))
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
#   — ek message ka /sendall kaafi nahi, range ka batch chahiye (koi limit nahi).
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
#   phir aakhri message ka link. Range par koi limit nahi hai (v72 me 50 hataya gaya)
#   hai), zyada ho to pehle 50 hi jayenge, baaki ke liye dobara chalao.
#   Source public ya private channel/group ho sakta hai (userbot ko
#   uska access hona chahiye, jaise /batch me hota hai). Messages
#   oldest-first order me, har message ko har saved channel me COPY
#   kiya jata hai (copy fail ho to forward fallback), har send ke
#   beech SENDALL_GAP (2 sec) hai. Time estimate: messages * channels
#   * 2 sec — jaise jitne message * jitne channel = 500 send = ~17 min.
#   Empty/service message skip hote hain, report me bataye jayenge.
#   Cancel: SENDALLRANGE_ACTIVE event, /cancel se loop ruk jayega.
ADDCHANNEL_STATES = {}  # uid -> {"step": "WAITING_LINK"}
SENDALLRANGE_STATES = {}  # uid -> {"step": "WAITING_FIRST"/"WAITING_LAST", "chat_id", "start_id"}
SENDALLRANGE_ACTIVE = {}  # uid -> asyncio.Event (cancel flag)
# v72: SENDALLRANGE par koi limit nahi hai (pehle 50 tha, user ne hataya hai).
# Bada range = zyada time (har send ke beech 2 sec ka gap ab bhi hai, flood se bachne ke liye).


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
             "(koi limit nahi) kaam karega. List dekhne ke liye `/mychannels`.")
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
            # v72 FAST: public source (username link) ho to bot copy pehle,
            # private source ho to userbot pehle. Copy/forward = server-side
            # instant, koi download/upload nahi hota.
            _is_public_src = isinstance(src_chat, str)
            ok, via_bot = await _copy_to_channel(
                client, userbot, dest, src_chat, src_chat, m.id,
                public_source=_is_public_src)
            if ok:
                per_channel[str(dest)] = per_channel.get(str(dest), 0) + 1
                if via_bot:
                    bot_sends += 1
            else:
                failed_ch[str(dest)] = "copy/forward fail (bot + userbot dono fail)"
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


async def run_clone(client, uid, src_chat, status_msg, only_topic=None, range_start=None, range_end=None):
    """Poora clone: topics nikalo -> naya private group -> same folders ->
    har folder ki saari messages (oldest-first) copy karo. Sequential, ek-ek karke.
    v72: only_topic diya ho to sirf wahi folder nikalega, range_start/end
    diye hon to sirf us message-ID range ke messages nikalenge."""
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
        # v72 folder filter: naam galat ho to khaali group banane se pehle hi rok do
        if only_topic:
            if only_topic.lower() == "general":
                only_topic = "General"
            elif not topics:
                await message_reply(status_msg, "❌ Source group forum nahi hai, isliye folder filter nahi chalega. `/clone` dobara karo aur folder ke liye `all` likho.")
                return
            _match = ["General"] if only_topic == "General" else [tt for _tid, tt in topics if tt.lower() == only_topic.lower()]
            if not _match:
                _avail = ", ".join(f"`{tt}`" for _tid, tt in topics[:30])
                await message_reply(status_msg, f"❌ Folder `{only_topic}` source me nahi mila.\nMilne wale folders: {_avail}")
                return
            only_topic = _match[0]
        # STEP 1: saari history ek baar me group karo (newest-first aati hai)
        buckets = {}
        _hist_n = 0
        try:
            async for msg in userbot.get_chat_history(src_chat):
                if cancel_event.is_set():
                    break
                if not msg or getattr(msg, "empty", False):
                    continue
                # v72 filters: folder + message-ID range
                if range_start is not None and msg.id < range_start:
                    continue
                if range_end is not None and msg.id > range_end:
                    continue
                if only_topic:
                    _tid = _clone_topic_key(msg)
                    _tname = topic_titles.get(_tid, "General" if _tid == 1 else "")
                    if _tname.lower() != only_topic.lower():
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
                if only_topic and ttitle.lower() != only_topic.lower():
                    continue  # v72: sirf chuna hua folder hi dest me banao
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
        _filt = ""
        if only_topic:
            _filt += f"\n📂 Sirf folder: `{only_topic}`"
        if range_start is not None or range_end is not None:
            _filt += f"\n🔢 Range: `{range_start or 'shuru'}` - `{range_end or 'aakhri'}`"
        done_text = ("✅ **Clone complete!**\n"
                     f"📦 Naya group: `{src_title} Clone`{_filt}\n"
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
        "• Bulk batch, up to 5000 items\n"
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
    "`/sendallrange` — Pehli-aakhri link se koi limit nahi, sab channels me bhejo\n"
    "`/free` — Ek baar, 10 messages ka free trial\n"
    "`/ai` — Premium + owner, FREE Qwen se chat\n"
    "`/makeadmin` — Saved channels me bot ko admin banao\n"
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
    if uid in CLONE_STATES:
        CLONE_STATES.pop(uid, None)
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
    # v72: premium milne par free trial khatam (quota ab lagu nahi hoga)
    _fd = _free_load()
    if str(target) in _fd:
        _fd.pop(str(target), None)
        _free_save(_fd)
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
    # v72: normal milne par free trial khatam (normal quota-free hota hai)
    _fd = _free_load()
    if str(target) in _fd:
        _fd.pop(str(target), None)
        _free_save(_fd)
    allowed = _load_allowed()
    allowed.add(target)
    _save_allowed(allowed)
    await message.reply_text(
        f"✅ `{target}` ab normal user hai.\n"
        "Wo /batch se save-restriction use kar sakta hai, par /public aur "
        "/mkchannels jaise premium commands use nahi kar payega.")



# --- v72: /ai (FREE Qwen), /free (one-time 10-msg trial), /makeadmin ---
@app.on_message(filters.command("ai") & filters.private)
async def ai_handler(client, message):
    """Premium + owner AI chat — FREE stock Qwen 3.8-27B via IteraCompute.
    Normal users isko use nahi kar sakte. Video nahi banata, sirf text
    jawab deta hai (prompt idea, script, etc)."""
    uid = message.chat.id
    if not _is_owner(uid) and not _is_premium(uid):
        await message.reply_text(
            "⛔ Ye command sirf premium members ke liye hai.\n"
            "Normal user /batch se save-restriction use kar sakta hai.")
        return
    raw = message.text or ""
    q = raw.split(None, 1)[1].strip() if len(raw.split(None, 1)) > 1 else ""
    if not q:
        await message.reply_text("Sawal bhi likho, jaise: `/ai Dholu Bholu ke liye 5 video idea do`")
        return
    wait = await message.reply_text("💬 Qwen (FREE) soch raha hai...")
    try:
        ans = await _qwen_chat(q)
        await wait.edit_text(ans[:4000])
    except Exception as e:
        await wait.edit_text(f"❌ Qwen fail: `{str(e)[:800]}`", parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.command("free") & filters.private)
async def free_handler(client, message):
    """Ek ID par SIRF EK BAAR: 10 messages ka free trial.
    Pehle se allowed (normal/premium/owner) ho to /free ki zaroorat nahi.
    Dobara /free chalane par refuse hoga — record free_used.json me hai
    (backup channel me bhi jata hai, redeploy se reset nahi hoga)."""
    uid = message.chat.id
    if _is_owner(uid) or _is_premium(uid):
        await message.reply_text("✅ Tumhare paas pehle se premium access hai, /free ki zaroorat nahi hai.")
        return
    data = _free_load()
    if str(uid) in data:
        await message.reply_text(
            "⛔ Tum /free pehle hi use kar chuke ho — ye sirf ek baar milta hai.\n"
            "Aage ke liye owner se premium lo.")
        return
    if uid in _get_allowed():
        # Normal user pehle se hai — use trial me mat daalo, warna uska
        # normal access 10-message limit me aa jayega.
        await message.reply_text("✅ Tumhare paas pehle se access hai, /free ki zaroorat nahi hai.")
        return
    data[str(uid)] = {"remaining": FREE_LIMIT, "used": True}
    _free_save(data)
    allowed = _load_allowed()
    allowed.add(uid)
    _save_allowed(allowed)
    await message.reply_text(
        "🎉 **FREE trial mil gaya!**\n\n"
        f"• Sirf **{FREE_LIMIT} messages** nikal sakte ho (total, sab batches mila ke)\n"
        "• `/batch` me range 10 se zyada di to wo shuru hi nahi hoga — chhota range do\n"
        "• Ye **sirf ek baar** milta hai, dobara `/free` nahi chalega\n"
        "• Trial khatam hone par access band ho jayega, phir premium lena hoga\n\n"
        "Shuru karo: `/batch` likho, pehli link bhejo, phir aakhri link bhejo (10 tak).",
        parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.command("makeadmin") & filters.private)
async def makeadmin_handler(client, message):
    """Connected channels me bot ko add + admin banao.
    Bina argument: saved channels (mychannels wali list) sab par chalega.
    Argument ke saath: `/makeadmin @channel` ya link — sirf us channel par.
    Login account ko channel ka creator/admin hona chahiye (wo hi bot ko
    add kar sakta hai). Naye /mkchannels channels par yehi invite-then-
    promote tareeka pehle se lagta hai (v70/v72 helper)."""
    uid = message.chat.id
    if not await _has_joined(uid, force=True):
        await _send_join_wall(message)
        return
    if not _is_allowed(uid):
        await message.reply_text("⛔ Access denied.")
        return
    if not _is_premium(uid):
        await message.reply_text(
            "⛔ Ye command sirf premium members ke liye hai.\n"
            "Normal user /batch se save-restriction use kar sakta hai.")
        return
    userbot = await get_userbot(uid)
    if not userbot:
        await message.reply_text("❌ Pehle `/login` karo — bot ko channel me tumhara account hi add kar sakta hai.")
        return
    try:
        _me = await client.get_me()
        bot_id, bot_username = _me.id, getattr(_me, "username", None)
    except Exception:
        bot_id, bot_username = None, None
    args = (message.text or "").split(None, 1)
    targets = []
    if len(args) > 1 and args[1].strip():
        ident = _add_resolve_identifier(args[1].strip())
        if ident is None:
            await message.reply_text("❌ Channel link/username samajh nahi aaya. Jaise: `/makeadmin @mychannel`")
            return
        try:
            chat = await userbot.get_chat(ident)
            targets = [{"username": getattr(chat, "username", None), "chat_id": chat.id,
                        "title": getattr(chat, "title", None) or str(chat.id)}]
        except Exception as e:
            await message.reply_text(f"❌ Channel nahi khul raha: {e}")
            return
    else:
        targets = _channels_load(uid)
        if not targets:
            await message.reply_text("❌ Koi saved channel nahi hai. Pehle `/mkchannels` ya `/addchannel` karo, ya `/makeadmin @channel` likho.")
            return
    status_msg = await message.reply_text(f"🤖 **Makeadmin shuru:** `{len(targets)}` channels me bot ko admin bana raha hun...")
    done_ok, done_fail = [], []
    for ch in targets:
        cid = ch.get("chat_id")
        uname = ch.get("username") or ch.get("title") or str(cid)
        if not cid:
            done_fail.append((uname, "chat_id nahi hai"))
            continue
        warn = await _bot_make_admin(userbot, cid, bot_id, bot_username)
        if warn:
            done_fail.append((uname, warn))
        else:
            done_ok.append(uname)
            # Saved list me bot_admin flag update karo
            try:
                lst = _channels_load(uid)
                for e in lst:
                    if e.get("chat_id") == cid:
                        e["bot_admin"] = "yes"
                _channels_save(uid, lst)
            except Exception:
                pass
        await asyncio.sleep(1)
    done = (f"✅ **Makeadmin complete!**\n🤖 Bot admin ban gaya: `{len(done_ok)}`\n")
    for u in done_ok:
        done += f"• @{u}\n"
    if done_fail:
        done += f"❌ Fail hue: `{len(done_fail)}`\n"
        for u, e in done_fail:
            done += f"• @{u} — {e}\n"
    done += ("\nℹ️ Fail hone ka matlab aksar ye hai ki login account us channel ka "
             "creator/admin nahi hai, ya bot ko add karne ki permission nahi hai.")
    await status_msg.edit_text(done)


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
        "6. /clone — 3 step: source group ka link bhejo, phir kaunsa folder nikalna hai (naam ya all), phir kahan se kahan tak (range jaise 100-500 ya all). Naya PRIVATE group tumhare account se banega, download + upload har file par dikhega.\n"
        "7. /create — Naya group banao: pehle group ka naam bhejo, phir topics ke naam ek hi message me (har naam ek line par, ya comma se) bhejo, un naam se folders ban jayenge.\n"
        "8a. /mkchannels — Public channels banao: /mkchannels likho, pehle NAAM bhejo (sab 10 me ek hi dikhega), phir DP bhejo ya skip karo (default rahega), phir BIO bhejo ya skip karo, phir 10 username ek hi message me bhejo (line ya comma se). Sirf username alag-alag hoga. Naam + DP + bio sab channels par ek saath lag jayenge. Channel tumhare login account se banega, aur har naye channel me bot ko bhi admin + Post Messages bana diya jayega, taaki baad me bot se bhi broadcast ho sake.\n"
        "8b. /mychannels — Tumhare banaye channels ki list dekho.\n"
        "8c. /sendall — Ek message sab channels me bhejo: kisi message (video / text / image) par reply karke /sendall likho, ya /sendall likh ke agla message bhejo. Pehle login account se try hoga, fail ho to bot se try hoga (bot dest me admin + Post Messages ON ho to).\n"
        "8d. /addchannel — Pehle se bana channel jodo: /addchannel likho, phir channel ka link / @username bhejo. Login account us channel ka admin hona chahiye, warna send fail hoga.\n"
        "8e. /sendallrange — koi limit nahi, ek saath sab channels me: /sendallrange likho, pehli message ka link bhejo, phir aakhri ka. Audio / video / text sab copy honge, sequencewise, FAST copy se (public source par bot copy pehle).\n"
        "8f. /free — Ek ID par sirf ek baar, 10 messages ka trial.\n"
        "8g. /ai — Premium + owner, FREE Qwen 3.8-27B se chat (text only).\n"
        "8h. /makeadmin — Saved channels me bot ko add + admin banao (ya /makeadmin @channel).\n"
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
        "**STEP 1/3 — Source group ka link bhejo**\n"
        "(jiske folders clone karne hain).\n\n"
        "Uske baad poochhunga: (2) kaunsa folder nikalna hai, (3) kahan se kahan tak (message ID range).\n\n"
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
            "ya `skip` likho to default DP rahegi (MR. VILLAIN logo wali).\n"
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
        "**📢 VILLAN SENDALLRANGE — koi limit nahi, sab channels me**\n"
        "**━━━━━━━━━━━━━━━**\n\n"
        "**STEP 1/2 — Pehli message ka link bhejo**\n"
        "Jaise: `https://t.me/mychannel/100`\n\n"
        "Uske baad aakhri message ka link poochhunga.\n"
        "Range par **koi limit nahi** hai — zyada ho to pehle 50 hi jayenge.\n"
        "Audio / video / text / photo — sab copy honge, sequencewise.\n"
        "Har send ke beech 2 sec ka gap hai, isliye jitne message * jitne channel\n"
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
         "mkchannels", "mychannels", "sendall", "free", "ai", "makeadmin",
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

    # Clone flow v72: (1) link -> (2) folder -> (3) range -> phir clone
    if user_id in CLONE_STATES:
        _cstep = CLONE_STATES[user_id].get("step")
        if _cstep == "WAITING_LINK":
            src_chat = _clone_parse_link(text)
            if src_chat is None:
                await message.reply_text("❌ **Invalid link!** Source group ka link bhejo "
                                         "(jaise `https://t.me/c/1234567890` ya `@groupname`).")
                return
            CLONE_STATES[user_id] = {"step": "WAITING_FOLDER", "src": src_chat}
            await message.reply_text(
                "**📂 STEP 2/3 — Kaunsa folder nikalna hai?**\n\n"
                "Folder ka **naam** bhejo (jaise `Maths`), ya **`all`** likho to saare folders nikalenge.\n\n"
                "**❌ Cancel:** `/cancel`",
                parse_mode=enums.ParseMode.MARKDOWN,
            )
            return
        if _cstep == "WAITING_FOLDER":
            _folder = text.strip()[:100]
            if not _folder:
                await message.reply_text("❌ Folder ka naam khaali hai. Naam bhejo, ya `all` likho.")
                return
            only_topic = None if _folder.lower() in ("all", "sab", "all folders") else _folder
            CLONE_STATES[user_id] = {"step": "WAITING_RANGE", "src": CLONE_STATES[user_id].get("src"), "folder": only_topic}
            await message.reply_text(
                "**📂 STEP 3/3 — Kahan se kahan tak nikalna hai?**\n\n"
                "Message ID range bhejo, jaise **`100-500`**\n"
                "Ya **`all`** likho to poori history nikalegi.\n"
                "(Ek number, jaise `100`, ka matlab hai 100 se aakhri tak.)\n\n"
                "**❌ Cancel:** `/cancel`",
                parse_mode=enums.ParseMode.MARKDOWN,
            )
            return
        if _cstep == "WAITING_RANGE":
            _rt = text.strip().lower()
            range_start = range_end = None
            if _rt not in ("all", "sab", "poora", "full"):
                import re as _re
                _m = _re.fullmatch(r"(\d+)\s*-\s*(\d+)", _rt)
                _m1 = _re.fullmatch(r"(\d+)", _rt)
                if _m:
                    range_start, range_end = int(_m.group(1)), int(_m.group(2))
                    if range_start > range_end:
                        range_start, range_end = range_end, range_start
                elif _m1:
                    range_start = int(_m1.group(1))
                else:
                    await message.reply_text("❌ Range samajh nahi aaya. Jaise `100-500` bhejo, ya `all` likho.")
                    return
            _st = CLONE_STATES.pop(user_id)
            status_msg = await message.reply_text(
                "📂 **Clone Started!** Folders aur messages nikal raha hun (ek-ek karke, download + upload dikhega)..."
            )
            await run_clone(client, user_id, _st.get("src"), status_msg,
                            only_topic=_st.get("folder"), range_start=range_start, range_end=range_end)
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
                if _default_dp_path():
                    _ddp_warn = "\n\n✅ DP skip ki — **default MR. VILLAIN logo lagega**."
                else:
                    _ddp_warn = ("\n\n⚠️ **Default DP file server par nahi mili** — skip par letter avatar lagega. "
                                 "Logo wali DP chahiye to abhi photo bhejo, ya default_dp.jpg ko main.py ke saath deploy karo.")
                await message.reply_text(
                    "**STEP 3/4 — Ab channel ki BIO bhejo**\n" + _ddp_warn + "\n"
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
            _bio_skipped = text.strip().lower() in ("skip", "default", "no", "none")
            if _bio_skipped:
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
                f"• **Bio:** `{'default bio lagegi' if _bio_skipped else 'tumhari bio lagegi'}`\n"
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
            # v72: koi limit nahi — jitna range doge, utna jayega (time zyada lagega).
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
        _fr = _free_remaining(user_id)
        if _fr is not None and total_files > _fr:
            await message.reply_text(
                f"⛔ Free trial me sirf `{_fr}` messages bache hain, tumne `{total_files}` maange hain.\n"
                f"Range `{_fr}` tak rakho, ya premium lo.")
            return
        if _fr is not None:
            _free_consume(user_id, total_files)

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
        _fr = _free_remaining(user_id)
        if _fr is not None and total > _fr:
            await message.reply_text(
                f"⛔ Free trial me sirf `{_fr}` messages bache hain, tumne `{total}` maange hain.\n"
                f"Range `{_fr}` tak rakho, ya premium lo.")
            return
        if _fr is not None:
            _free_consume(user_id, total)

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
# v77: User ne bola deploy ke baad wala lamba broadcast BAND karo, sirf
# chhota "NEW UPDATE ADDED" message bhejo. Isliye UPDATE_NOTES ab bheja
# nahi jata — ye sirf code me record ke liye pada hai, broadcast me sirf
# neeche wala 2-line wala message jata hai.
UPDATE_NOTES = (
    "• /fbatch aur /public ka fix — pehle NameError aata tha, ab nahi\n"
    "• /mkchannels — 10 username se PUBLIC channel banao, har channel ke beech 2 min ka gap\n"
    "• /mychannels aur /sendall — ek message sab channels me bhejo\n"
    "• /addchannel — pehle se bana channel bhi broadcast list me jodo\n"
    "• /sendallrange — pehli-aakhri link se koi limit nahi, sab channels me ek saath\n"
    "• /mkchannels — ab 10 channels ka naam ek hi hoga, sirf username alag-alag hoga\n"
    "• /mkchannels — ab DP aur bio bhi poochhega, naam + DP + bio sab channels par ek saath lagega (DP na do to default rahega)\n"
    "• /sendall aur /sendallrange — ab bot fallback bhi hai, login account fail ho to bot admin se bhejega (Post Messages ON hona chahiye)\n"
    "• /mkchannels — har naye channel me bot bhi admin banega (Post Messages ON), taaki 1 account se sab par broadcast ho sake; /organize aur /arrange hata diye gaye, folderwise kaam sirf /fbatch se hoga\n"
    "• /mkchannels bio step ka crash fix — bio bhejte hi jo UnboundLocalError aata tha aur flow atak jata tha (isliye DP kabhi lagti hi nahi thi), wo theek hua\n"
    "• /clone list topics fix — deleted topic milne par poori folder list fail nahi hogi, deleted topics skip ho jayenge\n"
    "• Premium / Normal system — /public aur channel commands sirf premium members ke liye, normal users /batch use kar sakte hain; /clone me download/upload progress dikhega\n"
    "• /mkchannels bot admin fix — ab bot ko pehle channel me add karke phir admin banaya jayega, taaki bot admin fail na ho\n"
    "• /paid aur /allow commands hata diye gaye — premium ke liye /premium, normal ke liye /normal use hoga\n"
    "• /ai (FREE Qwen), /free (ek baar, 10 messages trial), /makeadmin, batch limit 5000, sendallrange limit hata, clone me folder + range, default DP + bio, video format fix\n"
    "• /ai ab premium users bhi use kar sakte hain, normal users nahi\n"
    "• /mkchannels DP fail ab chhupta nahi — report me DP fail ki line + default_dp.jpg missing ki warning aayegi\n"
    "• /mkchannels me DP aur bio dono poochhe jayenge, skip par tumhara diya hua default lagega (logo DP + default bio)\n"
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
    print(f"[BROADCAST] {len(users)} users ko chhota NEW UPDATE ADDED message bhej raha hun",
          flush=True)
    sent = failed = 0
    # v77: lamba changelog broadcast BAND — sirf ye chhota message jayega.
    text = "<b>🔄 NEW UPDATE ADDED</b>\n\nBot update ho gaya hai."
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
            BotCommand("sendallrange", "koi limit nahi, sab channels me bhejo"),
            BotCommand("free", "Ek baar, 10 messages ka free trial"),
            BotCommand("ai", "Premium + owner, FREE Qwen se chat"),
            BotCommand("makeadmin", "Saved channels me bot ko admin banao"),
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
    _ddp = _default_dp_path()
    print(f"👉 DEFAULT DP: {_ddp if _ddp else 'MISSING — default_dp.jpg main.py ke saath deploy nahi hui, skip par letter avatar lagega'}", flush=True)
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
