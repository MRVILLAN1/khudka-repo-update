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
# v94: reset-loop me Pyrogram har 5s "Send exception: Connection lost"
# (INFO) aur asyncio "socket.send() raised exception" (WARNING) se log
# bhar deta hai, asli [BATCH]/[DOWNLOAD] lines dab jati hain. Transport
# ka shor band karo — session reconnect ki INFO lines abhi bhi dikhengi.
logging.getLogger("pyrogram.connection.transport.tcp.tcp").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.ERROR)

# Code version - startup logs me dikhega taaki pata chale kaunsa code deploy hai
# v94 NET-RESILIENCE (2026-11-10, on v93 — baaki sab v93 jaisa hi):
# 2026-11-09 16:30 ek baar ka reset tha, 2026-11-10 07:53-07:56 lagatar
# loop tha: ConnectionResetError har ~5s, DC1 reconnect ke 4s baad phir
# reset. v93 me _fetch_part sirf FloodWait par retry karta tha — reset
# aate hi multi turant fail, classic bhi usi dead session par fail, file
# turant fail. v94 me: (1) part-level par reset par 2s/5s backoff + session
# evict + fresh session ke saath retry, (2) file-level par reset par 8s
# wait + ek baar dobara try, (3) socket.send / tcp Send-exception ka log
# shor band taaki [BATCH] lines dikhein. Ye Telegram/Koyeb ka network
# tootta hua rok nahi sakta — reset ab file ko turant nahi maarta, wo
# wait karke dobara try karta hai. Happy path, order, MULTI_PARTS=3,
# sequential delivery — sab v93 jaisa hi.
# v95 PIN-COPY (2026-11-10, on v94 — baaki sab v94 jaisa hi):
# User: source channel me jo video/message pinned hai, wo dest channel
# me bhi pinned ho jaye — public ho ya private, sab me.
# Kaise: har run shuru me source ke pinned IDs ek baar nikalo
# (search_messages, PINNED filter — Pyrogram 2.0.106 me
# get_pinned_messages NAHI hai, isliye yahi tareeka hai), phir har
# delivered file ke baad check karo: source ID pinned tha to dest me
# pin karo. Pin pehle userbot (login account, dest ka creator/admin)
# se, phir bot (Bot API) se. Pin fail ho to delivery fail NAHI hoti,
# sirf [PIN] log aayega. Scope: /sendall, /sendallrange, /fbatch,
# /clone — dest CHANNEL/group wale raste. /batch ki personal-chat
# delivery me pin nahi (wo channel nahi hai, status message pehle se
# pinned rehta hai). /organize-/arrange purane raste hain, pin wahan
# is version me nahi — folderwise kaam /fbatch se karo.
# Bot ko pin ka haq: _bot_make_admin me can_pin_messages True kiya,
# taaki aage se bane /mkchannels channels me bot bhi pin kar sake.
# Pehle se bane channels me bot ko pin tabhi milega jab /makeadmin
# dobara chalakar re-promote karo.
# v96 (2026-11-10, on v95 — baaki sab v95 jaisa hi):
# (1) /pin command: /pin -> group/channel link -> kya pin karna hai
# (message link / ID / plain text, ek line me ek) -> ek-ek karke pin.
# (2) /batch pin-copy ab dest channel par bhi: settings me chat_id se
# channel set ho to batch se nikli file, source me pinned thi to dest
# channel me bhi pin hogi. Personal chat (chat_id nahi set) me pin
# nahi — wahan status message pehle se pinned rehta hai.
# v97 STYLE (2026-11-18, on v96 — sirf STYLE badla hai, kaam wahi):
# User ne SRC EXTRACTOR 3 ka /start menu + SRC EXTRACTOR 1 ka
# "PRO TURBO TRANSFER PIPELINE" progress dikhakar manga: ye sab style
# apne bot me chahiye. Isliye /start menu aur batch status SRC style
# me kar diye — banner, status block, buttons, pipeline progress.
# BEHAVIOUR SAME: upload/download/order/caption/settings/pin — kuch
# nahi badla. Sirf dikhawa (text + buttons) badla hai. Buttons pehle
# wale hi kaam karte hain: Start = /batch wala flow, Buy VIP = owner
# se contact (buy_handler jaisa), Help = commands list, Logout =
# /logout jaisa. "Full Account Reset" button NAHI dala — wo session
# delete karta hai, uska Khudka me koi safe equivalent nahi; user
# mange to confirm step ke saath daalenge.
# v98 UNIFIED BATCH (2026-11-18, on v97):
# User: repo (v96, jo upload kiya — v96 se byte-identical, clean, koi
# stealer nahi) me sab mix ho gaya hai — start/batch/public/forward alag
# alag hain. Ab EK hi /batch command sab karega, khudse:
#   /batch -> Channel ID mango (-100 ID / @username / link / message link)
#   -> source khud pehchano: public (username) / private (-100) / folder
#      wala forum group -> forum ho to folder ka naam mango (ya all)
#   -> range mango (100-500 / 100 / all) -> run_batch chale.
# Public = bot fetch + fast file_id path (yehi /public wala kaam hai).
# Private = userbot fetch (/login zaroori). Folder = sirf us topic ke
# messages (reply_to_top_message_id filter, run_batch me only_topic).
# /public, /fbatch, /clone abhi bhi alag se kaam karte hain — unhe toda
# nahi hai, bas /batch ab unka kaam bhi khud kar lega. Upload/download/
# order/caption/settings/pin — kuch nahi badla.
# v99 (2026-11-18, on v98): /ai HATA DIYA — user ne manga.
# /ai handler + _qwen_chat + QWEN env vars + BotCommand menu + COMMANDS_TEXT
# + help 8g + dispatcher exclusion se "ai" — sab hata diya. Ab bot par
# /ai likhne par kuch nahi hoga (koi handler nahi). Koyeb env se
# QWEN_API_KEY bhi hata sakte ho (ab zaroorat nahi). Baaki sab v98 jaisa.
# v100 (2026-11-18, on v99):
# (1) /batch me /public + /clone bhi: folder wale group se /batch karo to
# bot poochhega EXTRACT ya CLONE — extract = batch jaisa (apne chat/dest
# me), clone = naya PRIVATE group banega, folders ke saath (run_clone).
# /public aur /clone likhne par bhi wahi unified /batch flow khulega
# (menu se hata diye, handler alias ke taur par rehte hain).
# (2) /sendall UNIFIED: ek message bhejo (sab channels me) YA pehli
# message ka link bhejo (range mode — phir aakhri link) — dono ek hi
# command me. /sendallrange menu se hata (handler alias rehta hai).
# (3) /makeadmin: pehle se admin wale channels SKIP honge, sirf baaki me
# bot admin banega (bot_admin flag bhi update hoga).
# (4) Fancy text (patch B) + /help me saare updates (patch B).
# v101 (2026-11-18, on v100):
# (1) /public, /clone, /sendallrange HATA DIYE — handler sirf ek chhota
# stub hai jo batata hai kaunsa command use karo (menu se pehle hi hata
# diye the). Ab extraction sirf /batch se, sending sirf /sendall se.
# (2) AUTO-DEST CHANNEL: /batch se kisi CHANNEL (public/private) se
# niklega to is user ke liye ek alag PRIVATE channel banega — ek ID ke
# liye EK hi channel (auto_dest.json me save, backup bhi hota hai, dobara
# nahi banega), bot usme AUTO-ADMIN hoga, batch wahi jayega (source
# jaisi hi sequence/content), aur source me pinned ho to wahan bhi pin
# hoga (pin-copy, dest channel hone se automatic). Folder group / clone
# mode par ye nahi lagta (wahan apna flow hai). /login na ho to auto
# channel nahi banega — batch personal chat me jayega (purana tareeka).
# Settings me chat_id set ho to bhi channel source par auto-dest hi chalega.
# v102 (2026-11-18, on v101): SRC EXTRACTOR 3 jaisa INTERFACE (style +
# flows, behaviour waisa hi):
# (1) /start menu SRC layout: User ka naam, FREE TIER (X/10 Used), Target,
# RAM Allocation Buffer (Turbo Mode, real MULTI_PARTS), "Apna operation
# niche diye gaye buttons se shuru karein", Full Account Reset button
# (confirm ke saath — session + settings + auto-dest record delete).
# (2) Start Forwarding Now -> mode-choice screen (Public / Private /
# Back to Main Menu). Public: login zaroori nahi, seedha channel ID.
# Private: session na ho to "Session Missing!" card. Mode sirf prompt
# badalta hai — unified /batch auto-detect waisa hi hai.
# (3) /setchannel (naya): destination set karo — channel/group ID bhejo
# ya "Save to My Private Chat" button. Settings chat_id hi save hota hai.
# (4) /help + COMMANDS_TEXT + menu me /setchannel.
# (5) "RAM Allocation Buffer" me koi fake number nahi — sirf Turbo Mode
# + asli parts/file; speed ka koi fake dawa nahi.
# v103 (2026-11-18, on v102): AUTO-DEST har source ke liye + stylish.
# (1) /batch dalte hi LINK maanga jata hai (prompt link-first; ID/username
# bhi chalte hain — parser waisa hi).
# (2) CHANNEL source (public/private) -> apna PRIVATE channel (v101).
# (3) GROUP source (public/private, folder ho ya plain) -> apna PRIVATE
# FORUM group (ek ID = ek group), folderwise copy — source jaisi folders
# banengi, files wahi jayengi; plain group ki files General me jayengi.
# (4) Ek ID par EK channel + EK group hi banega (auto_dest.json me do
# slot; v101 flat format channel slot samjha jata hai). Dono me bot
# AUTO-ADMIN hoga. Pin-copy dono me (channel: run_batch, group: run_clone).
# (5) run_clone ko dest_override mila — apna group reuse karta hai, pehle
# se bane folders dobara nahi banata; done text "Extract complete!" jab
# extract mode se chale. CLONE mode waisa hi (har baar naya group).
# (6) Stylish: mode-choice header + dest labels + clone/extract done
# fancy text me.
# v104 (2026-11-18, on v103): LIVE BUGFIX — do crash, dono v103 me:
# (1) /help MESSAGE_TOO_LONG — help_text 4096-char Telegram limit se
# bada ho gaya tha (har version ki NEW UPDATES lines judti gayi), to
# /help har baar fail hota tha. Ab help_handler _send_long_text se
# tukdo me bhejta hai (4000-char chunk, line boundary par).
# (2) NameError: PUBLIC_STATES is not defined — v101 me /public hatate
# waqt uska dict bhi hat gaya tha, par 11 jagah .pop()/in use baki tha:
# mode buttons (Public/Private) + /batch + dispatcher sab crash karte
# the. Dict wapas joda (stub flow dead hai, sirf cleanup ke liye).
# v105 (2026-11-18, on v104): AUDIT + SPEED.
# AUDIT (sab add kia hua check kiya — v94-v104): symtable scan se koi
# undefined name nahi mila (PUBLIC_STATES wala bug v104 me theek hai);
# 27 feature checks pass (pin-copy, /pin, unified batch, /ai hata, auto
# dest channel+group, /setchannel, mode screen, reset, chunked help).
# SPEED (Koyeb Eco ~5 MB/s cap waisa hi — code usse upar nahi ja sakta):
# (1) run_batch pacing DEST par based: 10s PRIVATE_GAP ab sirf tab jab
# dest APNA PERSONAL CHAT ho (DM me bulk bhejne par FloodWait lagta hai).
# Dest channel/group ho (auto-dest ya settings chat_id) to wahan normal
# FAST/SLOW gap (2-3s) — private batch auto-dest par ~3x tez.
# Personal-chat delivery bilkul waisi hi (10s gap, koi risk nahi).
# (2) run_clone + run_fbatch: har media file ke baad ka gap 2s -> 1s
# (userbot sends, FloodWait retry pehle se hai — backstop waisa hi).
# NAHI BADLA (jaan-boojh kar): MULTI_PARTS=3 (v93 safe level; env se 6
# tak badha sakte ho), FAST/SLOW_GAP defaults, transfer path (v88 me
# user ne download+upload maanga tha, instant copy band hai).
# v106 (2026-11-18, on v105): /buy DIRECT.
# /buy dabte hi "Owner se Baat Karo" button dikhega — dabate hi owner
# ka chat khul jayega, aur message pehle se likha hoga (fancy BOLD):
# "Hello brother, I want to buy premium SRC bot" — bas send dabana hai.
# /start menu ka "Buy VIP Premium Plan" button bhi wahi karta hai.
# (t.me/?text= link se text pre-filled aata hai — khud se kuch type
# nahi karna padta; payment manual UPI hi rahega, plan payment ke
# baad activate hoga.)
# v107 (2026-11-18, on v106): RESTART BROADCAST STYLISH.
# Deploy/restart par sab users ko jaane wala chhota message ab fancy
# + sundar hai (heading + "Kya naya hai" fancy bold me, version bhi
# likha hoga). Chhota hi hai (v77 rule — lamba changelog nahi jayega).
# Bullets me latest highlights hain — agle version me yahi line update
# karni hai.
# v108 (2026-11-18, on v107): UNLIMITED RANGE + SECOND LINK WAPAS.
# (1) Range UNLIMITED — BATCH_LIMIT (5000) wala check teeno jagah se
# hata (WAITING_RANGE, direct range-link shortcut, purana /public
# two-link flow). Free-trial limit (10 messages) waisa hi hai — wo
# paid model hai, range cap nahi.
# (2) Dusri LINK wapas — /batch me pehli link (start lock) ke baad
# dusri message link bhejo to uska message ID aakhri ID ban jayega
# (purana first-link/second-link flow). Galat source ki link ho to
# mana kar dega. Clone/group mode me bhi chalegi.
# (3) t.me/c/ 3-part topic link fix — _parse_channel_input pehle
# beech wala number (TOPIC id) start maan leta tha; ab AAKHRI number
# (message id) leta hai. Jaise t.me/c/4384345338/15668/17673 par ab
# start 17673 lock hoga, 15668 nahi.
# v109 (2026-11-18, on v108): DEST PROGRESS CARD.
# (1) Auto-dest (channel/group) me bhi per-file progress card — pehle
# sirf DM me dikhta tha (Preparing -> UPLOADING -> Done), dest me files
# chupchaap aa jati thi. Ab dest me bhi wahi card dikhega: live speed,
# bytes, %, ETA, Message ID — stylish fancy font me.
# (2) Card me batch counts bhi: File X/Y • ✅ ok • ❌ fail (pinned
# status me to tha hi, ab per-file card me bhi dikhega — DM + dest dono).
# (3) run_clone (auto forum group) me dest TOPIC me card: download bhi
# (speed ke saath) + upload bhi, usi folder ke topic me.
# Download DM chat me ab bhi SILENT hai (v90/v91 rule) — sirf DEST me
# dikhta hai. Numbers sab ASLI hain (progress_bar ke real bytes/speed),
# koi fake speed nahi.
# v110 (2026-11-18, on v109): ARROW PROGRESS CARD.
# User ne 20 designs me se ARROW (Design 14) chuna — progress_bar ab
# wahi design dikhayega, DM + dest dono me. Sab kuch BOLD hai (har line
# _fancy() se guzarti hai). Bar: ▶ filled, ▷ khaali (10 blocks).
# Numbers sab asli hain (bytes/speed/ETA/counts) — sirf look badla hai.
# v111 (2026-11-18, on v110): AUTO-DEST RESTORE FIX.
# BUG: restart par dusra group ban jata tha. Root cause — auto_dest.json
# BACKUP to hota tha (_BACKUP_KNOWN me tha, har save par backup channel
# me copy jati thi), par RESTORE ki list (stores) me tha hi nahi! Koyeb
# Eco par redeploy par local files mit jati hain, to mapping gayab ho
# jati thi aur agle batch par naya group ban jata tha (channel ka bhi
# wahi haal tha). Ab restore list me hai — restart ke baad wahi purana
# group/channel reuse hoga, naya nahi banega.
# (2) Group validation me ek retry: get_chat pehli baar transient
# wajah se fail ho to 3s ruk ke dobara try karo — tab bhi fail ho to hi
# mapping hatao aur naya banao. Isse startup ki chhoti gadbad par bhi
# duplicate group nahi banega.
# v112 (2026-11-18, on v111): DEST RULE (user).
# /settings me chat_id set ho to /batch WAHI jayega — auto-dest bilkul
# nahi chalega. Chat_id na ho to group source -> auto group, channel
# source -> auto channel (pehle wala rule). Pehle auto-dest settings
# chat_id ko override kar deta tha (run_batch me dest_override pehle
# chalta tha), isliye settings ID set hone par bhi batch auto group /
# channel me jata tha. Ab handler pehle hi settings ID ko dest bana
# deta hai. Group source + channel dest ho to flat run_batch (folderwise
# channel me nahi chalega); group dest ho to run_clone folderwise.
# Galat chat_id ho to batch shuru hone se pehle saaf error milega.
# Clone mode isse alag hai — wo hamesha naya group banata hai.
# v113 (2026-11-18, on v112): ANIMATED FULL-COVER PROGRESS CARD.
# Style (user ne choice mujh par chhodi — Typewriter + Dot Fill):
# header TYPEWRITER — SRC BOT BY MR V -> VI -> VIL -> VILL -> VILLA ->
# VILLAN, cursor ▌ ke saath; footer me VILLAN ke bagal wale black dots
# BLINK karte hain (● <-> ○, har second); cover lines (top/divider/
# bottom) ke right end par ➤ arrow hai (right face karke); right side
# KHULA hai (koi right border nahi). Frame har edit par agla animation
# frame dikhata hai (time-based), isliye card "move" karta hai. Box ki
# chaudaai body ki sabse lambi line se banti hai, isliye type karte waqt
# box hilta nahi. Sab kuch BOLD (har line _fancy() se). Numbers sab asli
# hain — sirf look badla hai.
# v114 (2026-11-18, on v113): CARD FIXES (user ke live card par).
# (1) Cover lines 2 chhoti: end ➤ arrow text se aage nikal jata tha
# (═ * (_w+2) ki wajah se), ab ═ * _w — arrow text ke right edge par.
# (2) "Kitna nikla / kitna bacha" ab saaf dikhega: per-file card ki File
# line me ⏳ remaining jud gaya (File X/Y • ✅ ok • ⏳ bacha • ❌ fail),
# aur DM wale pinned batch status me "⏳ Bacha: N" ki line jud gayi.
# v115 (2026-11-18, on v114): SMOOTH ANIMATION FIX.
# Blink/typewriter smooth nahi tha — wajah: (1) frame time-based tha
# (int(time.time()) % 6), par card edit sirf har 5s hota tha
# (PROGRESS_EVERY=5), to frame ulta chalta tha (V -> VILLAN -> VILLA...
# kyunki 5s ka gap mod 6 me -1 hota hai) aur blink bhi atak-atak ke
# badalta tha. (2) Ab frame har ASLI edit par ek kadam aage badhta hai
# (_ANIM_TICK counter) — V -> VI -> VIL -> ... hamesha seedha, koi skip
# ya ulta nahi; dots har 2 edit par flip (2s ●, 2s ○ — shant blink).
# (3) PROGRESS_EVERY 5 -> 1s, taaki card har second update ho aur
# animation dikhe (edit FloodWait aaye to safe_edit use skip kar deta
# hai, koi crash nahi). DM + dest dono ko ek hi call me wahi frame.
# v116 (2026-11-18, on v115): SCREENSHOT FIXES (user ke live card par).
# (1) Border wrap: cover lines (╔ + ═×W + ➤) phone par wrap ho jati
# thin — File line lambi hone se W=35 ho gaya tha, border 37 chars ka
# hokar wrap karta tha aur ➤ agle line par gir jata tha (sab idhar-udhar
# dikhta tha). Ab border max 30 ═ (kabhi wrap nahi hoga); header/footer
# bhi isi chaudaai par center honge.
# (2) DM stats: group source (run_clone) me per-file Arrow card sirf
# DEST topic me banta tha — DM me sirf plain status text tha, isliye bot
# chat me File/speed/⏳ bacha nahi dikhta tha. Ab run_clone me bhi DM ka
# per-file card (_dmprog) banta hai — download + upload dono ki progress
# DM + dest dono jagah wahi card dikhayegi (progress_bar ka dest_msg).
# v117 (2026-11-18, on v116): CARD CHHOTA — 5 LINES HATA DI.
# User ke live card par ab bhi bikhra hua lag raha tha (14 lines,
# border wrap + divider/footer/bottom). Hataya: (1) header ke neeche
# wala divider, (2) UPLOADING ke neeche blank line, (3) footer se pehle
# wala divider, (4) footer (VILLAN dots blink — iske saath blink bhi
# gaya; typewriter header ab bhi animate karta hai), (5) bottom border.
# Bacha: top border + header (typewriter) + 7 body lines = 9 lines.
# Right side aur neeche khula hai; border max 30 ═ (v116, no wrap).
# DM stats fix (v116 _dmprog) isme pehle se hai — deploy karne par DM
# me bhi wahi card + stats dikhenge.
# v118 (2026-11-18, on v117): SPEED FIX — card edits upload ko block kar
# rahe the. v115 me PROGRESS_EVERY 5s -> 1s hua (smooth animation ke
# liye) aur v116 me clone me DM card bhi jud gaya — matlab har second
# 2 message edits, aur Pyrogram upload agle chunk se PEHLE in edits ka
# wait karta tha (har edit ~200ms), isliye upload speed gir gayi thi
# (2.5 MB/s, Koyeb cap ~5 MB/s hote hue bhi). Ab beech wale edits
# background task me hote hain (_edit_async, ek message par ek hi edit
# in-flight) — upload kabhi edit ka wait nahi karta. Aakhri edit (100%)
# ab bhi await hota hai, taaki wo "✅ Sent" se pehle pakka lag jaye.
# Animation waisi hi smooth rahegi (1s tick, tick-based frame).
# v119 (2026-11-18, on v118): FOOTER DOTS WAPAS — user ne kaha neeche
# VILLAN wale dots bhi do-do badha do aur bari bari blink karao.
# Footer wapas aaya (v117 me hataya tha): har side 5 dots (3 ki jagah),
# ● ek-ek karke (bari bari) aage-peeche chalta hai — ping-pong
# (bahar -> andar -> bahar, koi jump nahi), dono side mirror me.
# Divider + footer wapas aaye (bottom border nahi) — card 11 lines.
# Typewriter header + smooth animation + speed fix (v118) waise hi hain.
# v120 (2026-11-18, on v119): USER KA PASTED CARD — user ne kaha phirse
# blunder ho gaya, maine thik kar diya, isko hi dekhlo (poora 14-line
# card paste kiya). Wapas aaya: header ke neeche divider, UPLOADING ke
# neeche blank line, footer ke baad bottom border. Border 24 ═ (user ke
# paste jaisi — 26 chars, phone par kabhi wrap nahi hogi). Footer dots
# 5-5 dono side + ping-pong blink (v119) waise hi. Typewriter header,
# smooth animation, speed fix (v118), DM card (v116) waise hi hain.
# v121 (2026-11-18, on v120): SPINNER HEADER — user ne kaha typing
# wala VILLAN animation change karke kuch aur kar do, apne hisab se.
# Typewriter (V -> VI -> ... -> VILLAN) hata diya; ab header me VILLAN
# ke pehle ek ghumta hua spinner hai (◐ -> ◑ -> ◒ -> ◓, har edit par
# ek kadam). Header ki lambai har frame me same rehti hai (spinner sirf
# glyph badalta hai), isliye card hilta nahi. Footer dots (5-5, ping-pong),
# 14-line card, border 24, speed fix (v118), DM card (v116) waise hi.
# v122 (2026-11-19, on v121): FAILING BATCH FIX — user ke log me msg
# 16-68 sab NO MEDIA/TEXT se fail ho rahe the, aur beech me EditMessage
# FloodWait (36s/60s/33s) aa raha tha. Wajah: inline path me har file par
# fetch se PEHLE hi DM + dest me "Preparing…" card banta tha (2 send +
# 2 delete har file par) — chahe file khaali hi kyun na ho. Khaali file
# par wo card bekaar rehta tha aur API call ki bheed se rate limit ka
# risk badhta tha. Ab fetch PEHLE hota hai; card sirf tab banta hai jab
# file asli ho (neeche "HAR FILE ke liye NAYA progress message" wala
# block). Fail hone wali file par ab sirf status update hota hai (3s
# throttle waise hi). NO MEDIA/TEXT khud code bug nahi hai — wo messages
# source me hi khaali/deleted hain.
# v123 (2026-11-19, on v122): (1) /batch HATA DIYA — batch ab sirf
# Start Forwarding se niklega (/start -> "Start Forwarding Now" button
# -> Public/Private mode -> link). /batch likhne par stub batayega.
# BotCommand menu se "batch" hata, COMMANDS_TEXT + /help + saare
# "/batch likho" wale messages Start Forwarding ki taraf badle.
# (2) DEST CHOICE — batch jab pehle se bane auto channel/group par
# jane wala ho to POOCHHEGA: "Purana wala" ya "Naya banao" (naya source
# ke naam se banega). Settings chat_id set ho to sawal nahi (v112 rule).
# Clone mode me sawal nahi (clone hamesha naya group banata hai).
# Choice WAITING_DEST_CHOICE state me save hoti hai; button dabane par
# _resume_dest_choice batch wahi se shuru karta hai.
# v124 (2026-11-19, on v123): /check DIAGNOSTIC — user 5968218636 ke
# batch me msg 6-369 sab NO MEDIA/TEXT se fail ho rahe the, jabki user
# kehta hai unme video hai. Bot ko message MIL raha hai par usme na
# media dikhta hai na text. /check <message link> ek message fetch karke
# batayega: source ka naam/type, message khaali hai ya nahi, service
# message hai ya nahi, media type, text/caption hai ya nahi, aur batch
# isko lega ya skip karega. Isse pata chalega bot galat chat padh raha
# hai ya message hi content-less aa raha hai. Saath me run_batch ab
# shuru hote hi log me source ka naam/ID/type likhega.
VERSION = "v124"
# v91: user ne kaha "waisa hi krdo" — matlab chat me Downloading ID kabhi
# nahi aana chahiye, jaise v51 se pehle tha. v90 ne pehli file ka prefetch
# download silent kiya tha, par prefetch fail hone par upload_one ka
# fallback download abhi bhi prog_msg par Downloading ID dikhata tha.
# v91 me fallback download bhi silent hai (prog_msg=None), upload ki
# progress abhi bhi prog_msg par UPLOADING ke roop me dikhegi. Download
# sirf LOG me dikhega, chat me kabhi nahi.
# v90 FIX (user: direct upload wapas, v1 se audit karke): v41-v89 tak audit
# me personal delivery hamesha topic copy se PEHLE thi — upload order kabhi
# toota hi nahi. Chat me DOWNLOAD dikhne ki wajah v51 ka first-file fix tha
# (pehli file ka download prog_msg par dikhane laga) + v87 ka transfer-skip.
# Isliye v90 me pehli file ka download bhi background jaisa silent hai:
# chat me sirf Preparing... phir UPLOADING dikhega, jaise video me hai.
# Download abhi bhi hota hai (protected ke liye zaroori hai), par uski
# progress sirf LOG me rahegi, chat me nahi. Speed ka farak nahi, sirf
# dikhne ka farak hai — download+upload ka time wahi lagega.
# v89 BUILD NOTE: v54 ko base banakar usme ab tak ke saare features merge
# kiye gaye hain (v55-v88 chain: mkchannels/sendall/clone/premium/free/ai/
# watchdog/DP-fix/direct-upload). v88 file ko base nahi banaya gaya, v54 par
# cumulative patch lagaya gaya hai — isliye code v88 jaisa hi nikla, kyunki
# v88 bhi isi chain se bana hai. Farak sirf version aur ye note hai.

# --- CONFIGURATION (Koyeb Environment Variables) ---
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "").strip()
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
SESSION_STRING = os.environ.get("SESSION_STRING", "").strip()
PORT = int(os.environ.get("PORT", 8000))

# (Sequential mode me istemal nahi hota; rakha hai taaki purana env var tootey nahi)
PARALLEL_TASKS = max(1, int(os.environ.get("PARALLEL_TASKS", "3")))
# Progress card kitne second me update ho (v115: 5 -> 1 — animation smooth
# chahiye to card har second update hona chahiye; edit FloodWait aaye to
# safe_edit use chupchaap skip kar deta hai, isliye koi crash nahi hoga).
PROGRESS_EVERY = float(os.environ.get("PROGRESS_EVERY", "1"))
# v115: animation tick — har ASLI progress edit par +1. Frame isi se
# chalta hai (time se nahi), isliye typewriter/blink hamesha aage badhta
# hai, kabhi skip ya ulta nahi.
_ANIM_TICK = [0]
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
# v103 AUTO-DEST: ek user ID -> do slot: channel + group (ek-ek hi banega).
AUTO_DEST_FILE = os.path.join(DATA_DIR, "auto_dest.json")
# v72 /free: ek ID par sirf ek baar, 10 messages ka trial.
FREE_FILE = os.path.join(DATA_DIR, "free_used.json")
FREE_LIMIT = 10
# v72: Ek batch me max 5000 posts tha (user ne 5000 bola tha).
# v108: range UNLIMITED kar diya — ye constant ab use nahi hota, sirf
# record ke liye pada hai (free-trial limit alag hai, wo waisa hi hai).
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
                  "user_settings.json", "user_topics.json", "channels.json", "free_used.json",
                  "auto_dest.json")
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
        # v111 FIX: auto_dest.json yahan MISSING tha — backup to hota tha
        # (_BACKUP_KNOWN me hai), par restore kabhi hota hi nahi tha,
        # isliye restart par mapping gayab ho jati thi aur naya group /
        # channel ban jata tha. Ab restore hoga — purana dest reuse hoga.
        "auto_dest.json": AUTO_DEST_FILE,
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

BOT_USERNAME = None  # v86: transfer fast path ke liye bot ka username (main() me set hota hai)

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
SETCHANNEL_STATES = {}  # v102: uid -> {"step": "WAITING_ID"}
PUBLIC_STATES = {}  # v104: /public hata diya gaya (v101) — sirf state cleanup ke liye bacha hai
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


async def _bot_make_admin(userbot, cid, bot_id, bot_username):
    """Channel me bot ko add + admin (Post + Edit Messages ON) karo.
    v70 ka invite-then-promote tareeka, ab /mkchannels aur /makeadmin
    dono isi helper se karte hain, taaki ek jagah fix dono jagah lage.
    Returns warn string ("" = success)."""
    if not bot_id:
        return "bot id nahi mila (get_me fail)"
    # v84: can_manage_topics Pyrogram 2.0.106 me maujood hi nahi hai —
    # isi ek galat kwarg se _bot_make_admin har baar TypeError se crash
    # hota tha. Sirf wahi fields rakho jo 2.0.106 jaanta hai.
    from pyrogram.types import ChatPrivileges
    try:
        _priv = ChatPrivileges(
            can_post_messages=True, can_edit_messages=True,
            can_delete_messages=False, can_manage_chat=False,
            can_restrict_members=False, can_promote_members=False,
            can_change_info=False, can_invite_users=False,
            can_pin_messages=True)  # v95: pin-copy ke liye bot ko bhi pin ka haq do
    except TypeError:
        _priv = ChatPrivileges(can_post_messages=True, can_edit_messages=True)
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
    """Parallel tasks me edit conflict/FloodWait aaye to silently skip.
    Returns True agar edit lag gaya, warna False."""
    try:
        await msg.edit_text(text, parse_mode=enums.ParseMode.MARKDOWN)
        return True
    except FloodWait:
        return False
    except Exception:
        return False


async def finish_status(status_msg, text):
    """Aakhri report SIRF EK BAAR dikhao (v80 fix).
    Pehle ye 2 baar aata tha: safe_edit se status message edit hota tha
    AUR message_reply se wahi text naya message ban ke aa jata tha.
    Ab edit lag gaya to naya message nahi bheja jayega; edit fail ho
    (message delete ho gaya, ya edit block ho gaya) tabhi ek naya
    message bheja jayega, taaki report kabhi gayab na ho."""
    if await safe_edit(status_msg, text):
        return
    await message_reply(status_msg, text)


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
    # v97 SRC style: wahi numbers (progress/bar/ok/fail/ETA), naya format.
    # Speed isme nahi dikhate — stats me speed track nahi hoti, jhoothi
    # speed likhna galat hoga.
    text = (
        "🔥 𝐏𝐑𝐎 𝐓𝐔𝐑𝐁𝐎 𝐓𝐑𝐀𝐍𝐒𝐅𝐄𝐑 𝐏𝐈𝐏𝐄𝐋𝐈𝐍𝐄 🔥\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"📊 Progress: `{done}/{total}`\n"
        f"{bar} `{pct:.0f}%`\n"
        f"✅ Transferred: `{ok}` | ❌ Failed: `{fail}`\n"
        f"⏳ Bacha: `{remaining}`\n"
        f"⏱️ Time Remaining: `{('~' + eta_str) if eta_str != '—' else '—'}`\n"
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


# v118: abhi edit ho rahe messages (id) — overlap rokne ke liye.
_EDITING = set()


async def _edit_async(msg, text):
    """progress_bar ka edit background me — upload/download iska wait na kare.
    Ek message par ek hi edit in-flight; dusra tick skip hoga (agla tick
    phir try karega). safe_edit khud exceptions pakadta hai, isliye task
    kabhi raise nahi karega."""
    _k = id(msg)
    if _k in _EDITING:
        return
    _EDITING.add(_k)
    try:
        await safe_edit(msg, text)
    finally:
        _EDITING.discard(_k)


async def progress_bar(
    current, total, message, start_time, last_update, action_title,
    stats=None, uid=None, bot_client=None, dest_msg=None
):
    """Premium style progress bar (screenshot wala design)."""
    now = time.time()
    if (now - last_update[0]) < PROGRESS_EVERY and current != total:
        return
    last_update[0] = now
    # v115 SMOOTH: har asli edit par tick +1 — frame hamesha agla aayega.
    _ANIM_TICK[0] += 1
    _tick = _ANIM_TICK[0]

    elapsed_time = max(now - start_time, 0.001)
    speed = current / elapsed_time
    percentage = (current * 100 / total) if total > 0 else 0

    # v113/v121: ANIMATED FRAME — Spinner header + footer dots (5 dots har
    # side, bari bari ping-pong blink). Poora 14-line card (user ke paste
    # jaisa). Har line _fancy() se guzarti hai, isliye saare akshar +
    # number bold dikhte hain. Numbers sab asli hain (bytes/speed/ETA/
    # counts).
    filled = int(percentage // 10)
    _bar = "▶" * filled + "▷" * (10 - filled)

    remaining_bytes = max(total - current, 0)
    time_left = round(remaining_bytes / speed) if speed > 0 else 0

    # Message ID nikalo (action_title me "ID `123`" format me hai)
    _mid = re.search(r"ID `(\d+)`", action_title or "")
    mid_str = _mid.group(1) if _mid else "—"

    # UPLOADING ya DOWNLOADING
    op = "UPLOADING" if "Upload" in (action_title or "") else "DOWNLOADING"

    _body = [
        _fancy(f"➤ {op} ➤ {percentage:.0f}%"),
        "",
        _bar,
        _fancy(f"➤ Size: {humanbytes(current)} / {humanbytes(total)}"),
        _fancy(f"➤ Speed: {humanbytes(speed)}/s"),
        _fancy(f"➤ ETA: {time_formatter(time_left * 1000)}"),
        _fancy(f"➤ ID: {mid_str}"),
    ]
    if stats is not None and getattr(stats, "total", 0):
        _cur = min(stats.done + 1, stats.total)
        # v114: bacha hua bhi dikhao — kitna nikla (✅ ok) / kitna bacha (⏳).
        _left = max(stats.total - stats.done, 0)
        _body.append(_fancy(
            f"➤ File: {_cur}/{stats.total} • ✅ {stats.ok} • ⏳ {_left} • ❌ {stats.fail}"))
    # Animation frame: tick-based — har edit par spinner ek kadam aage
    # (◐ -> ◑ -> ◒ -> ◓). DM + dest dono ko ek hi call me wahi frame
    # milta hai.
    _spin_frames = ["◐", "◑", "◒", "◓"]
    _si = _tick % len(_spin_frames)
    _header = _fancy(f"SRC BOT BY MR {_spin_frames[_si]} VILLAN")
    # v119: footer wapas — har side 5 dots (3 ki jagah), ● ek-ek karke
    # (bari bari) aage-peeche chalta hai (ping-pong: bahar -> andar ->
    # bahar, koi jump nahi). Dono side mirror me chalte hain.
    _ping = [0, 1, 2, 3, 4, 3, 2, 1]
    _p = _ping[_tick % len(_ping)]
    _ld = ["●" if i == _p else "○" for i in range(5)]
    _rd = ["●" if i == (4 - _p) else "○" for i in range(5)]
    _footer = _fancy(f"{' '.join(_ld)} VILLAN {' '.join(_rd)}")
    # v120: user ka pasted card — poora 14-line card wapas (header divider,
    # blank line, footer divider, bottom border). Border 24 ═ (user ke
    # paste jaisi — 26 chars, phone par kabhi wrap nahi hogi). Footer 5
    # dots + ping-pong blink (v119) waise hi.
    _w = max(len(x) for x in [_header] + _body + [_footer])
    _bw = min(_w, 24)
    _top = "╔" + "═" * _bw + "➤"
    _div = "╠" + "═" * _bw + "➤"
    _bot = "╚" + "═" * _bw + "➤"
    lines = [_top, "║ " + _header.center(_bw), _div]
    lines += ["║ " + x for x in _body]
    lines += [_div, "║ " + _footer.center(_bw), _bot]
    _text = "\n".join(lines)
    # v118 SPEED: beech wale edits background me — upload inka wait na kare.
    # Aakhri edit (100%) await karo, taaki wo "✅ Sent" se pehle pakka lage.
    if current == total:
        await safe_edit(message, _text)
        if dest_msg is not None:
            await safe_edit(dest_msg, _text)
    else:
        asyncio.create_task(_edit_async(message, _text))
        if dest_msg is not None:
            asyncio.create_task(_edit_async(dest_msg, _text))


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
        print(f"[BATCH FAIL] msg {msg_id}: OFFLINE (userbot/login session nahi hai)", flush=True)
        return "OFFLINE"
    if preloaded == "MISSING":
        print(f"[BATCH FAIL] msg {msg_id}: MISSING (source me delete/khaali/invalid ID hai)", flush=True)
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
                    print(f"[BATCH FAIL] msg {msg_id}: EMPTY (source me message khaali ya nahi mila)", flush=True)
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
                    "target_msg": target_msg,  # v98: topic filter ke liye
                    "delivered_fast": True}
        print(f"[BATCH FAIL] msg {msg_id}: NO MEDIA/TEXT (na media hai, na text hai)", flush=True)
        return None

    kind = target_msg.media.value
    media_obj = getattr(target_msg, kind, None)
    file_id = getattr(media_obj, "file_id", None) if media_obj else None
    file_size = getattr(media_obj, "file_size", 0) or 0
    if file_size > 2097152000:
        return {"kind": "too_big", "msg_id": msg_id,
                "target_msg": target_msg}  # v98: topic filter ke liye

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


async def download_file_classic(user_client, info, prog_msg, stats=None, uid=None, bot_client=None, dm_prog=None):
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
    # v116: DM card bhi wahi dikhaye (run_clone DM stats).
    if dm_prog is not None:
        try:
            await safe_edit(dm_prog, f"📥 Downloading • ID `{msg_id}` • {humanbytes(info.get('file_size', 0))}")
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
                f"📥 Downloading • ID `{msg_id}`", stats, uid, bot_client, dm_prog,
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
# v93: floor 3 — Koyeb env MULTI_PARTS=1 live tha, isliye multi band tha aur
# har file classic download par naya ExportAuthorization mangti thi (FloodWait).
# 3 parts = v47 wala safe level (6 se flood badhta hai), delivery order sequential hi rahega.
MULTI_PARTS = max(3, min(8, int(os.environ.get("MULTI_PARTS", "3"))))
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

# v94: network-reset pehchano — ye transient hai (FloodWait/406 jaisa
# permanent nahi), isliye is par retry + backoff banta hai.
_NET_RESET_MARKERS = (
    "connection lost", "connection reset", "broken pipe",
    "connection aborted", "timed out", "timeout", "connection refused",
)


def _is_net_reset_error(e):
    """Exception ya error-string network reset hai ya nahi."""
    if e is None:
        return False
    if isinstance(e, (ConnectionResetError, BrokenPipeError,
                      ConnectionAbortedError, TimeoutError,
                      asyncio.TimeoutError)):
        return True
    try:
        _t = str(e).lower()
    except Exception:
        return False
    return any(m in _t for m in _NET_RESET_MARKERS)


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


async def _download_multi(user_client, info, prog_msg, stats, uid, bot_client, dm_prog=None):
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
                    nonlocal session
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
                        except Exception as e:
                            # v94: ConnectionReset / BrokenPipe par turant
                            # fail mat karo — purani dead session hatao,
                            # 2s phir 5s ruk kar fresh session se retry.
                            if not _is_net_reset_error(e) or attempt == 2:
                                raise
                            _wait = 2 if attempt == 0 else 5
                            print(f"[NET] part {p} msg {msg_id}: reset "
                                  f"({e}), {_wait}s ruk kar fresh session "
                                  f"se retry {attempt + 1}/2", flush=True)
                            _evict_media_sessions(user_client, dc_id)
                            await asyncio.sleep(_wait)
                            session = await _get_media_session(
                                user_client, dc_id)
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
                            stats, uid, bot_client, dm_prog)
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
            if _is_net_reset_error(e):
                print(f"[NET] multi fail msg {msg_id}: {e} — session "
                      f"evicted, classic fallback", flush=True)
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


async def download_file(user_client, info, prog_msg, stats=None, uid=None, bot_client=None, dm_prog=None):
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
        if (user_client and info.get("target_msg")
                and info.get("file_id")
                and (info.get("file_size", 0) or 0) >= MULTI_MIN_SIZE):
            try:
                dl_path = await _download_multi(
                    user_client, info, prog_msg, stats, uid, bot_client, dm_prog)
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
            user_client, info, prog_msg, stats, uid, bot_client, dm_prog)

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
    # v94: network-reset par ek baar 8s wait + dobara try. v93 me sirf
    # FloodWait ko retry milta tha — reset par file turant fail hoti thi.
    # _attempt() multi + classic dono phir se try karta hai, aur multi
    # apne andar session evict karke fresh session lega. Reset agar
    # minutes tak chale to ye retry bhi fail hoga — file fail count hogi,
    # batch aage badhega, agla file phir try karega (hang nahi hoga).
    _err0 = info.get("download_error") or ""
    if _is_net_reset_error(_err0):
        print(f"[NET-RETRY] reset par file {msg_id}: 8s wait karke dobara "
              f"try (error: {_err0[:80]})", flush=True)
        if prog_msg is not None:
            try:
                await safe_edit(
                    prog_msg,
                    f"⏳ Connection reset — 8s wait, phir dobara try • ID `{msg_id}`")
            except Exception:
                pass
        _evict_media_sessions(user_client)
        await asyncio.sleep(8)
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
                             file_path=None, caption="", info=None, thumb=None, prog_msg=None,
                             stats=None, dm_prog=None):
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
                        f"🚀 Uploading • ID `{info.get('msg_id')}`", stats, None, None, dm_prog))
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
        _res = await client.invoke(raw.functions.messages.SendMedia(
            peer=peer,
            media=media,
            message=cap_text,
            entities=cap_entities,
            random_id=_rand.randint(1, 2**63 - 1),
            top_msg_id=topic_id,
        ))
        # v95: bheje gaye message ki id wapas do (pin copy ke liye).
        # Callers sirf truthiness check karte hain, isliye id (int) bhi
        # True jaisa hi kaam karega; id na mile to pehle jaisa True.
        _mid = _updates_msg_id(_res)
        return _mid if _mid else True
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
        _sent = await _send_by_file_id(client, dest_chat, kind, file_id, cap, info)
    except Exception as e:
        print(f"Direct fast path failed ({info['msg_id']}): {e}", flush=True)
        return False
    await _fast_group_copy(client, group_id, topic_id, kind, file_id, cap, info)
    print(f"⚡ INSTANT (direct) msg {info['msg_id']}", flush=True)
    # v96: sent id wapas do (batch->channel pin ke liye); na mile to True.
    return getattr(_sent, "id", None) or True


# Protected-content chats ("Restrict saving content" ON) jahan Telegram
# file reference re-send block kar deta hai — unke liye har file par transfer
# try karna bekar failed calls hain, isliye ek baar pata chalne ke baad skip.
_TRANSFER_BLOCKED_CHATS = set()


def _note_transfer_block(err, src_chat_id):
    es = str(err).upper()
    if src_chat_id and ("RESTRICT" in es or "FORWARD" in es):
        _TRANSFER_BLOCKED_CHATS.add(src_chat_id)
        print(f"[TRANSFER] chat {src_chat_id} protected hai — is chat ki "
              "baaki files seedha download se aayengi", flush=True)


async def _transfer_fast(client, userbot, dest_chat, group_id, topic_id,
                         kind, file_id, cap, info):
    """v86: TRANSFER FAST PATH wapas (v43 wali bot ki khasiyat, v44 me hati thi).
    Problem: userbot ka fetch kiya file_id BOT use nahi kar sakta (MEDIA_EMPTY),
    isliye har private file download+upload hoti thi (2x time).
    Trick: userbot wahi file_id BOT ke PM me bhej deta hai (server-side,
    instant — koi download nahi), phir bot us message ko user tak COPY kar
    deta hai (wo bhi server-side instant). Copy me bot ka apna file_id ban
    jata hai, jisse topic me bhi instant copy hota hai. Returns True/False.
    Fail ho to caller download+upload fallback par jayega — kuch regress nahi hoga."""
    if not BOT_USERNAME or userbot is None:
        return False
    # Protected chat me pehle hi block ho chuka hai to transfer skip karo
    src_chat = getattr(info.get("target_msg"), "chat", None)
    src_chat_id = getattr(src_chat, "id", None)
    if src_chat_id and src_chat_id in _TRANSFER_BLOCKED_CHATS:
        return False
    # 1) userbot file_id BOT ko PM kare (instant)
    try:
        sent = await _send_by_file_id(userbot, BOT_USERNAME, kind, file_id, cap, info)
    except Exception as e:
        print(f"Transfer send failed ({info['msg_id']}): {e}", flush=True)
        _note_transfer_block(e, src_chat_id)
        return False
    # Bot ke liye transfer chat = userbot wale account ki ID (private chat
    # har taraf se dusri party ki ID se jaani jaati hai)
    peer_uid = getattr(sent.from_user, "id", None) if sent.from_user else None
    if not peer_uid:
        try:
            peer_uid = (await userbot.get_me()).id
        except Exception:
            return False
    tmid = sent.id
    try:
        # 2) bot transfer message ko COPY karke asli destination de (instant)
        copied = await client.copy_message(dest_chat, peer_uid, tmid)
        # 3) topic me bhi — copy se mila BOT ka file_id use karke (instant)
        bot_fid = _sent_file_id(copied, kind)
        if group_id and topic_id and bot_fid:
            await _botapi_send_to_topic(
                kind, file_id=bot_fid, caption=cap,
                topic_id=topic_id, info=info)
        elif group_id and bot_fid:
            try:
                await _send_by_file_id(client, group_id, kind, bot_fid, cap, info)
            except Exception:
                pass
        print(f"⚡ INSTANT (transfer) msg {info['msg_id']}", flush=True)
        # v96: copied id wapas do (pin ke liye); na mile to True.
        return getattr(copied, "id", None) or True
    except Exception as e:
        print(f"Transfer copy failed ({info['msg_id']}): {e}", flush=True)
        _note_transfer_block(e, src_chat_id)
        return False
    finally:
        # 4) transfer message hata do — user ke chat me wo dikhna nahi chahiye
        try:
            await client.delete_messages(peer_uid, [tmid])
        except Exception:
            pass


async def upload_one(client, userbot, uid, dest_chat, topic_id, status_msg, info, stats=None, prog_msg=None, fast_only=False, dest_prog_msg=None):
    """Ek file deliver karo: personal (dest_chat) + group topic (LOG_GROUP_ID).
    FAST PATH: file_id se instant bhejo (no download/upload).
    SLOW PATH: download karo, settings lagao, phir upload karo.
    status_msg = pinned batch status (sirf _update_batch_status).
    prog_msg = alag per-file progress message (sirf progress_bar).
    dest_prog_msg = v109: dest (auto channel/group) me bhi wahi card.
    v96 PIN-COPY: info["pinned"] ho aur dest channel ho (settings chat_id
    se set, personal chat nahi) to delivered message dest me bhi pin hoga.
    Pin fail ho to delivery fail nahi hoti."""
    kind = info["kind"]
    s = get_settings(uid)
    group_id = LOG_GROUP_ID
    # v96: dest channel hai ya personal chat? chat_id set ho (uid se alag)
    # to channel/group, warna personal chat — pin sirf channel par.
    try:
        _dest_is_channel = int(dest_chat) != int(uid)
    except Exception:
        _dest_is_channel = False
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
            _sent_txt = await client.send_message(
                dest_chat, bold_text, parse_mode=enums.ParseMode.HTML)
            await _to_group_text(bold_text)
            info["delivered_fast"] = True
            if info.get("pinned") and _dest_is_channel and _sent_txt:
                if await _pin_delivered(userbot, client, dest_chat, _sent_txt.id):
                    info["pinned_done"] = True
            return True
        if kind == "too_big":
            print(f"[BATCH FAIL] msg {info['msg_id']}: TOO_BIG (2GB se badi file, skip hui)", flush=True)
            await client.send_message(
                dest_chat,
                f"⚠️ **Message {info['msg_id']}:** File 2GB se badi hai.",
                parse_mode=enums.ParseMode.MARKDOWN,
            )
            return False

        # === FAST PATH: file_id se INSTANT bhejo (sirf BOT ka apna file_id) ===
        # v86: transfer path WAPAS (v44 me hata tha, user ne wapas maanga —
        # yehi bot ki khasiyat thi). Userbot wale file_id par bot ka direct
        # send MEDIA_EMPTY par fail hota hai, isliye private source par pehle
        # transfer try hoga (neeche), fail ho to download+upload fallback.
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
                # v109: fast path me upload callback nahi chalta — dono
                # cards ko "Sent" par le jao, warna "Preparing…" atka rahega.
                _done = f"✅ {_fancy('Sent')} • ID `{info.get('msg_id')}`"
                if prog_msg is not None:
                    await safe_edit(prog_msg, _done)
                if dest_prog_msg is not None:
                    await safe_edit(dest_prog_msg, _done)
                if info.get("pinned") and _dest_is_channel and isinstance(ok, int):
                    if await _pin_delivered(userbot, client, dest_chat, ok):
                        info["pinned_done"] = True
                return True
            if fast_only:
                print(f"[PUBLIC] fast fail ({info['msg_id']}) — slow fallback",
                      flush=True)
            print(f"Fast path failed ({info['msg_id']}) — downloading...",
                  flush=True)
        # === v88: TRANSFER BAND — direct upload wapas ===
        # User ne video wala rasta maanga: instant copy nahi, balki
        # download+upload, aur uploading complete hote hi delivery.
        # Isliye transfer try hi nahi hoga, seedha slow path chalega.
        # (_transfer_fast code maujood hai, par v88 me call nahi hoga.)
        if False and (file_id and (fast_only or not _needs_file(s, info))
                and userbot is not None and userbot is not client):
            ok = await _transfer_fast(
                client, userbot, dest_chat, group_id, topic_id,
                kind, file_id, cap, info)
            if ok:
                info["delivered_fast"] = True
                return True
            print(f"Transfer fast failed ({info['msg_id']}) — downloading...",
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
            # v91: fallback download bhi silent — chat me Downloading ID nahi
            fp = await download_file(userbot, info, None, stats, uid, client)
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
                        "progress_args": pa + (title, stats, uid, client, dest_prog_msg)}
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
            # v109: photo/voice/video_note par upload callback nahi chalta
            # (progress sirf video/document/audio par) — dono cards ko
            # "Sent" par le jao, warna "Preparing…" atka rahega.
            if send_kind in ("photo", "voice", "video_note"):
                _done2 = f"✅ {_fancy('Sent')} • ID `{info.get('msg_id')}`"
                if prog_msg is not None:
                    await safe_edit(prog_msg, _done2)
                if dest_prog_msg is not None:
                    await safe_edit(dest_prog_msg, _done2)
        except FloodWait as e:
            print(f"[BATCH FAIL] msg {info.get('msg_id')}: Personal upload FloodWait {e.value}s", flush=True)
            await asyncio.sleep(e.value + 1)
            _clean()
            return False
        except Exception as e:
            print(f"Personal upload failed ({info.get('msg_id')}): {e}", flush=True)
            _clean()
            return False

        # v96 PIN-COPY (batch->channel): dest channel hai aur source me
        # pinned tha to dest me bhi pin karo. Personal chat me pin nahi.
        if info.get("pinned") and _dest_is_channel:
            try:
                if await _pin_delivered(userbot, client, dest_chat, sent_msg.id):
                    info["pinned_done"] = True
            except Exception:
                pass
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
        print(f"[BATCH FAIL] msg {info.get('msg_id')}: Upload FloodWait {e.value}s", flush=True)
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


def _updates_msg_id(res):
    """Raw Updates se bheje gaye message ki id nikalo (None = nahi mili)."""
    try:
        for u in getattr(res, "updates", []) or []:
            _m = getattr(u, "message", None)
            if _m is not None and getattr(_m, "id", None):
                return int(_m.id)
    except Exception:
        pass
    return None


async def _get_pinned_ids(fetch_client, src_chat):
    """Source chat ke pinned message IDs ka set lao (public/private dono).
    Pyrogram 2.0.106 me get_pinned_messages NAHI hai, isliye PINNED
    filter wali search use hoti hai. Fail ho to khaali set — delivery
    rukegi nahi, sirf pin copy skip hoga. Returns set[int]."""
    if not fetch_client or src_chat is None:
        return set()
    try:
        _ids = set()
        async for _m in fetch_client.search_messages(
                src_chat, filter=enums.MessagesFilter.PINNED, limit=100):
            if _m and getattr(_m, "id", None):
                _ids.add(int(_m.id))
        if _ids:
            print(f"[PIN] source pinned mile: {sorted(_ids)}", flush=True)
        return _ids
    except Exception as e:
        print(f"[PIN] source pinned list fail: {e}", flush=True)
        return set()


async def _pin_delivered(userbot, bot_client, dest_chat, sent_msg_id):
    """Dest me bheje gaye message ko pin karo — pehle userbot (login
    account, dest ka creator/admin), phir bot (Bot API). Pin fail ho to
    delivery fail NAHI hoti, sirf log me [PIN] fail aayega.
    Returns True/False."""
    if not sent_msg_id or not dest_chat:
        return False
    if userbot is not None:
        try:
            await userbot.pin_chat_message(
                dest_chat, sent_msg_id, disable_notification=True)
            print(f"[PIN] dest pin OK via userbot msg {sent_msg_id}", flush=True)
            return True
        except Exception as e:
            print(f"[PIN] userbot pin fail msg {sent_msg_id}: {e}", flush=True)
    if bot_client is not None:
        try:
            if await _pin_status(bot_client, dest_chat, sent_msg_id):
                print(f"[PIN] dest pin OK via bot msg {sent_msg_id}", flush=True)
                return True
        except Exception as e:
            print(f"[PIN] bot pin fail msg {sent_msg_id}: {e}", flush=True)
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
        await client.send_message(uid, "❌ Koi ruka hua batch nahi mila. Naya batch chalane ke liye /start karo aur 'Start Forwarding Now' dabao.")
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


def _parse_channel_input(text):
    """v98: /batch ke liye channel input parse karo.
    Accept: -100 ID, @username, t.me/username, t.me/c/ID, message link.
    Returns (chat_id, preset_start_id) ya (None, None).
    chat_id: str (public username) ya int (-100...)."""
    t = (text or "").strip()
    if not t:
        return None, None
    # t.me/c/... PEHLE check karo — parse_tg_link 'c' ko username samajh
    # leta hai, isliye private group link (t.me/c/ID) galat parse hota.
    # t.me/c/CHAT/TOPIC/MSG (topic link) me message ID AAKHRI number hai.
    m = re.search(r"t\.me/c/(\d+)((?:/\d+)*)", t)
    if m:
        _nums = [m.group(1)] + [x for x in m.group(2).split("/") if x]
        return int(f"-100{_nums[0]}"), (int(_nums[-1]) if len(_nums) > 1 else None)
    cid, mid = parse_tg_link(t)
    if cid and mid:
        return cid, mid
    m = re.search(r"t\.me/([a-zA-Z0-9_]+)", t)
    if m:
        return m.group(1), None
    m = re.fullmatch(r"@([a-zA-Z0-9_]{4,32})", t)
    if m:
        return m.group(1), None
    m = re.fullmatch(r"-100\d{5,}", t)
    if m:
        return int(t), None
    m = re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_]{3,31}", t)
    if m:
        return t, None
    return None, None


async def _resolve_batch_source(client, userbot, chat_id):
    """v98: source khud pehchano. Returns (chat, is_public, is_forum).
    chat None = khul hi nahi raha. Public = username (str), private =
    -100 (int). Forum = chat.is_forum."""
    is_public = isinstance(chat_id, str)
    chat = None
    try:
        chat = await client.get_chat(chat_id)
    except Exception:
        chat = None
    if chat is None and userbot is not None:
        try:
            chat = await userbot.get_chat(chat_id)
        except Exception:
            chat = None
    if chat is None:
        return None, is_public, False
    return chat, is_public, bool(getattr(chat, "is_forum", False))


def _parse_batch_range(text, preset_start=None):
    """v98: range text parse karo. '100-500' / '100' / 'all'.
    Returns (start, end, is_all) — 'all' par (preset or 1, None, True),
    galat input par (None, None, False)."""
    rt = (text or "").strip().lower()
    if rt in ("all", "sab", "poora", "full"):
        return (preset_start or 1), None, True
    m = re.fullmatch(r"(\d+)\s*-\s*(\d+)", rt)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        return (min(a, b), max(a, b), False)
    m = re.fullmatch(r"(\d+)", rt)
    if m:
        # Ek number: preset (message link se) ho to ye AAKHRI id hai,
        # warna ye START hai aur aakhri tak jayega (latest id chahiye).
        if preset_start:
            return (preset_start, int(m.group(1)), False)
        return (int(m.group(1)), None, True)
    return None, None, False


def _auto_dest_entry(data, uid):
    """v103: do slot — channel + group. v101 flat format (chat_id) ko
    channel slot samjho (backward compat)."""
    ent = data.get(str(uid)) or {}
    if "channel" in ent or "group" in ent:
        return ent
    if ent.get("chat_id"):
        return {"channel": {"chat_id": ent["chat_id"], "title": ent.get("title"), "source": ent.get("source")}}
    return {}


def _auto_dest_existing(uid, kind):
    """v123: auto-dest ka pehle se bana slot lao (kind: 'channel'/'group').
    Returns (chat_id, title) ya (None, None)."""
    try:
        data = _load_json(AUTO_DEST_FILE, {})
        ent = _auto_dest_entry(data, uid)
        slot = ent.get(kind) or {}
        if slot.get("chat_id"):
            return slot["chat_id"], slot.get("title") or "Saved"
    except Exception:
        pass
    return None, None


def _auto_dest_clear_slot(uid, kind):
    """v123: auto-dest ka slot mapping se hatao (naya banane ke liye).
    Purana channel/group DELETE nahi hota — sirf mapping hat-ti hai,
    isliye purana dest safe rehta hai."""
    try:
        data = _load_json(AUTO_DEST_FILE, {})
        ent = _auto_dest_entry(data, uid)
        if kind in ent:
            ent.pop(kind, None)
            data[str(uid)] = ent
            _save_json(AUTO_DEST_FILE, data)
            return True
    except Exception as e:
        print(f"[AUTO-DEST] clear slot fail: {e}", flush=True)
    return False


async def _get_or_create_auto_dest(client, userbot, uid, src_title):
    """v103: channel slot — ek ID ke liye EK hi auto dest channel.
    Pehli baar: source ke naam par naya PRIVATE channel banao (userbot
    creator hai), bot ko auto-admin banao, mapping save karo (backup bhi).
    Dobara: wahi channel reuse karo — naya nahi banega.
    Returns (chat_id, is_new, title) ya (None, False, None)."""
    from pyrogram import raw as _raw
    data = _load_json(AUTO_DEST_FILE, {})
    ent = _auto_dest_entry(data, uid)
    slot = ent.get("channel") or {}
    if slot.get("chat_id"):
        return slot["chat_id"], False, slot.get("title") or "Saved"
    title = f"{(src_title or 'VILLAN').strip()[:100]} • Saved"
    about = f"📂 Saved from {src_title or 'channel'} • Powered by VILLAN"[:255]
    try:
        upd = await userbot.invoke(_raw.functions.channels.CreateChannel(
            title=title, about=about,
            broadcast=True, megagroup=False, forum=False))
    except Exception as e:
        print(f"[AUTO-DEST] create fail for {uid}: {e}", flush=True)
        return None, False, None
    cid = None
    for ch in getattr(upd, "chats", []) or []:
        _id = getattr(ch, "id", None)
        if _id:
            cid = int(f"-100{_id}")
            break
    if not cid:
        return None, False, None
    ent["channel"] = {"chat_id": cid, "title": title, "source": src_title}
    data[str(uid)] = ent
    _save_json(AUTO_DEST_FILE, data)
    try:
        _me = await client.get_me()
        warn = await _bot_make_admin(userbot, cid, _me.id, getattr(_me, "username", None))
        if warn:
            print(f"[AUTO-DEST] bot admin warn: {warn}", flush=True)
    except Exception as e:
        print(f"[AUTO-DEST] bot admin fail: {e}", flush=True)
    return cid, True, title


async def _get_or_create_auto_group(client, userbot, uid, src_title):
    """v103: group slot — ek ID ke liye EK hi auto dest FORUM group.
    Pehli baar: source ke naam par naya PRIVATE forum group banao (userbot
    creator hai), bot ko auto-admin banao, mapping save karo (backup bhi).
    Dobara: wahi group reuse karo — pehle se bane folders dobara nahi
    banenge (run_clone naam se reuse karta hai). Agar saved group delete
    ho gaya ho to naya banega.
    Returns (chat_id, is_new, title) ya (None, False, None)."""
    data = _load_json(AUTO_DEST_FILE, {})
    ent = _auto_dest_entry(data, uid)
    slot = ent.get("group") or {}
    if slot.get("chat_id"):
        _ok = False
        for _att in range(2):
            try:
                await userbot.get_chat(slot["chat_id"])
                _ok = True
                break
            except Exception as e:
                if _att == 0:
                    # v111: pehli baar transient fail ho sakta hai
                    # (startup/network) — 3s ruk ke ek baar aur try karo.
                    print(f"[AUTO-GROUP] saved group check retry ({e})", flush=True)
                    await asyncio.sleep(3)
        if _ok:
            return slot["chat_id"], False, slot.get("title") or "Saved"
        print(f"[AUTO-GROUP] saved group {slot['chat_id']} nahi khul raha — naya banega", flush=True)
        ent.pop("group", None)
    title = f"{(src_title or 'VILLAN').strip()[:100]} • Saved"
    about = f"📂 Saved from {src_title or 'group'} • Powered by VILLAN"[:255]
    try:
        cid = await _clone_create_group(userbot, title, about=about)
    except Exception as e:
        print(f"[AUTO-GROUP] create fail for {uid}: {e}", flush=True)
        return None, False, None
    if not cid:
        return None, False, None
    ent["group"] = {"chat_id": cid, "title": title, "source": src_title}
    data[str(uid)] = ent
    _save_json(AUTO_DEST_FILE, data)
    try:
        _me = await client.get_me()
        warn = await _bot_make_admin(userbot, cid, _me.id, getattr(_me, "username", None))
        if warn:
            print(f"[AUTO-GROUP] bot admin warn: {warn}", flush=True)
    except Exception as e:
        print(f"[AUTO-GROUP] bot admin fail: {e}", flush=True)
    return cid, True, title


async def _resume_dest_choice(client, query, choice):
    """v123: dest-choice button (dest_old/dest_new) ka jawab — ruka hua
    batch wahi se shuru karo. ctx USER_STATES se aata hai (WAITING_RANGE
    ne save kiya tha). choice: 'old' (purana dest reuse) ya 'new' (slot
    hata ke source ke naam se naya dest banao)."""
    uid = query.from_user.id
    ctx = USER_STATES.get(uid) or {}
    if ctx.get("step") != "WAITING_DEST_CHOICE":
        try:
            await query.answer("Ye choice ab purani ho gayi — dobara batch shuru karo.", show_alert=True)
        except Exception:
            pass
        return
    USER_STATES.pop(uid, None)
    kind = ctx.get("kind")
    try:
        await query.answer()
    except Exception:
        pass
    try:
        await query.message.edit_text(
            "✅ Choice mil gayi — batch shuru ho raha hai…",
            parse_mode=enums.ParseMode.MARKDOWN)
    except Exception:
        pass
    if kind == "group":
        _src = ctx["src"]
        _ub = await get_userbot(uid)
        if _ub is None:
            await client.send_message(uid, "❌ Group se nikalne ke liye pehle `/login` karo.")
            return
        if choice == "new":
            _auto_dest_clear_slot(uid, "group")
            _gid, _gnew, _gtitle = await _get_or_create_auto_group(
                client, _ub, uid, ctx.get("name"))
            if not _gid:
                await client.send_message(uid, "❌ Apna group nahi ban paya — dobara try karo.")
                return
            _dest_note = " (bot admin ✅, folderwise — naya banaya)"
        else:
            _gid, _gnew, _gtitle = ctx["old_id"], False, ctx.get("old_title") or "Saved"
            _dest_note = " (bot admin ✅, folderwise — wahi purana)"
        status_msg = await client.send_message(
            uid,
            "🔥 𝐏𝐑𝐎 𝐓𝐔𝐑𝐁𝐎 𝐓𝐑𝐀𝐍𝐒𝐅𝐄𝐑 𝐏𝐈𝐏𝐄𝐋𝐈𝐍𝐄 🔥\n"
            f"📂 {_fancy('Dest Group')}: `{_gtitle}`{_dest_note}"
        )
        await run_clone(client, uid, _src, status_msg,
                        only_topic=ctx.get("fold"), range_start=ctx.get("rs"),
                        range_end=ctx.get("re"),
                        dest_override=_gid, dest_title=_gtitle, mode_label="Extract")
        return
    # kind == "channel"
    _src = ctx["src"]
    if choice == "new":
        _auto_dest_clear_slot(uid, "channel")
        _ub = await get_userbot(uid)
        if _ub is not None:
            _dest_override, _auto_new, _auto_title = await _get_or_create_auto_dest(
                client, _ub, uid, ctx.get("name"))
            if not _dest_override:
                await client.send_message(uid, "❌ Apna channel nahi ban paya — dobara try karo.")
                return
            _auto_line = (f"\n📂 {_fancy('Dest Channel')}: `{_auto_title}`"
                          " (bot admin ✅, pin-copy ON) — naya banaya")
        else:
            await client.send_message(uid, "❌ Auto channel ke liye `/login` karo.")
            return
    else:
        _dest_override = ctx["old_id"]
        _auto_title = ctx.get("old_title") or "Saved"
        _auto_line = (f"\n📂 {_fancy('Dest Channel')}: `{_auto_title}`"
                      " (bot admin ✅, pin-copy ON) — wahi purana")
    status_msg = await client.send_message(
        uid,
        f"🔥 𝐏𝐑𝐎 𝐓𝐔𝐑𝐁𝐎 𝐓𝐑𝐀𝐍𝐒𝐅𝐄𝐑 𝐏𝐈𝐏𝐄𝐋𝐈𝐍𝐄 🔥\nBatch started — `{ctx.get('total', 0)}` items.{_auto_line}"
    )
    await run_batch(client, uid, _src, ctx["start"], ctx["end"], status_msg,
                    public_fast=ctx.get("is_pub", False),
                    only_topic=ctx.get("only_topic"),
                    dest_override=_dest_override)


async def run_batch(client, owner_id, src_chat, start_id, end_id, status_msg, public_fast=False, only_topic=None, dest_override=None):
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

    # v124: batch shuru hote hi log me source ka naam/ID/type likho —
    # galat chat fetch ho rahi ho to turant pata chal jaye.
    try:
        _src_chat, _src_pub, _src_forum = await _resolve_batch_source(client, userbot, src_chat)
        if _src_chat is not None:
            _src_title = getattr(_src_chat, "title", None) or str(src_chat)
            _src_type = getattr(_src_chat, "type", None)
            _src_type = _src_type.value if _src_type is not None else "?"
            print(f"[BATCH] source: '{_src_title}' ({_src_type}" + (" forum" if _src_forum else "") + f") id={src_chat}", flush=True)
        else:
            print(f"[BATCH] source: {src_chat} (resolve nahi hua)", flush=True)
    except Exception as _e:
        print(f"[BATCH] source log fail: {_e}", flush=True)

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
    # Settings: agar user ne chat ID set kiya hai to files wahi jayengi.
    # v101: dest_override (auto-dest channel) ho to wahi pehle chalega.
    dest_chat = dest_override or get_settings(owner_id)["chat_id"] or owner_id
    # Log group me user ke naam ka folder (topic) banao
    topic_id = await get_or_create_topic(userbot, owner_id)
    # v50: user ne jo link diya, wo uske folder me record ho jaye
    await _archive_source_link(client, topic_id, src_chat, start_id, end_id, owner_id)
    # v96 PIN-COPY (batch->channel): source ke pinned IDs ek baar nikalo
    # (public/private dono). Fail ho to khaali set — pin skip, batch chalega.
    _pinned_ids = await _get_pinned_ids(fetch_client, src_chat)
    _pinned_n = 0
    # v98: folder filter par dusre folder ke messages skip honge.
    _skipped_n = 0
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
                # v88: purana pipeline wapas — private source ki agli file
                # background me download hoti rahegi jab current file upload
                # ho rahi hogi, taaki user ko video jaisa UPLOADING dikhe,
                # DOWNLOAD ka lamba wait na dikhe. (v87 me ye prefetch
                # transfer ke liye band kiya gaya tha, ab transfer band hai.)
                needs_dl = (
                    fetch_client is not client   # private source: upload ke liye file chahiye hi
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
        dest_prog_msg = None  # v109: dest (auto channel/group) ka progress card
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
                # v98 EARLY folder filter: chunk se message mil gaya ho to
                # topic yahin check karo — dusre folder ka ho to download
                # hi mat karo, seedha skip.
                if only_topic is not None and pre is not None and pre != "MISSING":
                    if getattr(pre, "reply_to_top_message_id", None) != only_topic:
                        if pending_task is not None and pending_mid == mid:
                            try:
                                pending_task.cancel()
                            except Exception:
                                pass
                            pending_task = None
                            if pending_holder is not None:
                                _cleanup_prepared(pending_holder)
                                _drop_holder(holders, pending_holder)
                                pending_holder = None
                        _skipped_n += 1
                        async with stats.lock:
                            stats.done += 1
                        _batch_save_progress(owner_id, src_chat, start_id, end_id, mid, public_fast)
                        await _update_batch_status(status_msg, stats)
                        continue
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
                    # v122: fetch PEHLE — khaali/deleted message par prog_msg
                    # mat banao. (pehle har file par DM + dest me "Preparing…"
                    # banta tha, chahe file fail hi kyun na ho — wo card
                    # bekaar rehta tha aur send/delete ki bheed se rate
                    # limit (FloodWait) ka risk badhta tha. Ab card sirf
                    # tab banta hai jab file asli ho — neeche "HAR FILE ke
                    # liye NAYA progress message" wala block banata hai.)
                    # v90: _prepare ko prog_msg mat do — download silent rahe
                    # (DM me DOWNLOAD nahi dikhna chahiye).
                    info = await _prepare(mid, pre, holder, None)
                    early_prog = False
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
                    print(f"[BATCH FAIL] msg {mid}: fetch se info nahi mila (reason upar wali [BATCH FAIL] line me hai)", flush=True)
                    async with stats.lock:
                        stats.done += 1
                        stats.fail += 1
                    _batch_save_progress(owner_id, src_chat, start_id, end_id, mid, public_fast)
                    await _update_batch_status(status_msg, stats)
                    continue
                # v98 LATE folder filter: chunk fail ho ya prefetch me file
                # pehle se prepare ho gayi ho to info ke target_msg se topic
                # check karo — dusre folder ka ho to bina deliver kiye skip.
                if only_topic is not None:
                    _mt = getattr(info.get("target_msg"), "reply_to_top_message_id", None)
                    if _mt != only_topic:
                        _skipped_n += 1
                        async with stats.lock:
                            stats.done += 1
                        if info.get("tmpdir"):
                            shutil.rmtree(info["tmpdir"], ignore_errors=True)
                        if 'holder' in locals():
                            _drop_holder(holders, holder)
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
                    # v109: dest (auto channel/group) me bhi progress card.
                    if dest_prog_msg:
                        try:
                            await dest_prog_msg.delete()
                        except Exception:
                            pass
                    dest_prog_msg = None
                    if dest_chat != owner_id:
                        try:
                            dest_prog_msg = await client.send_message(
                                dest_chat, "⏳ Preparing…",
                                parse_mode=enums.ParseMode.MARKDOWN)
                        except Exception:
                            dest_prog_msg = None
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
                    # v98: agli file dusre folder ki ho to prefetch hi mat karo.
                    if only_topic is not None and pre_n is not None and pre_n != "MISSING":
                        if getattr(pre_n, "reply_to_top_message_id", None) != only_topic:
                            continue
                    h = {}
                    holders.append(h)
                    pending_task = asyncio.create_task(_prepare(nxt, pre_n, h))
                    pending_mid = nxt
                    pending_holder = h
                # STEP 2: deliver karo (fast file_id ya download+upload), phir agli file
                # prog_msg = per-file progress (alag message), status_msg = pinned batch
                # v96: ye file source me pinned thi ya nahi — upload_one ko batao.
                if isinstance(info, dict):
                    info["pinned"] = mid in _pinned_ids
                ok_one = await upload_one(
                    client, fetch_client, owner_id, dest_chat, topic_id,
                    status_msg, info, stats, prog_msg,
                    fast_only=public_fast, dest_prog_msg=dest_prog_msg)
                # Memory: inline-prepared file ka holder ab zaroorat nahi
                if 'holder' in locals():
                    _drop_holder(holders, holder)
                async with stats.lock:
                    stats.done += 1
                    if ok_one:
                        stats.ok += 1
                    else:
                        stats.fail += 1
                if not ok_one:
                    print(f"[BATCH FAIL] msg {mid}: upload_one False (reason upar wali fail line me hai)", flush=True)
                if isinstance(info, dict) and info.get("pinned_done"):
                    _pinned_n += 1
                _batch_save_progress(owner_id, src_chat, start_id, end_id, mid, public_fast)
                # Pinned status live update (max 3 sec me 1 baar)
                await _update_batch_status(status_msg, stats)
                # v50 PACING: PRIVATE batch me 10s countdown gap, PUBLIC par nahi.
                # Countdown per-file progress message par dikhta hai (10..1).
                # Pinned status ko har second edit nahi karte (edit-flood se bachne).
                # v105 SPEED: 10s gap sirf tab jab dest APNA PERSONAL CHAT ho (DM
                # me bulk bhejne par FloodWait lagta hai). Dest channel/group ho
                # (auto-dest ya settings chat_id) to DM wala risk nahi — wahan
                # normal FAST/SLOW gap hi kaafi hai, batch tez chalega.
                if mid != ids[-1]:
                    if not is_public and not public_fast:
                        _kind = info.get("kind") if isinstance(info, dict) else None
                        if _kind == "text":
                            await asyncio.sleep(FAST_GAP)
                        elif dest_chat == owner_id:
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
        _done_text = ("✅ 𝐓𝐫𝐚𝐧𝐬𝐟𝐞𝐫 𝐂𝐨𝐦𝐩𝐥𝐞𝐭𝐞!\n"
                      f"📊 Total: `{total}` • ✅ Success: `{ok}` | ❌ Failed: `{fail}`")
        if _skipped_n:
            _done_text += f"\n⏭️ Skipped: `{_skipped_n}` (dusre folder ke)"
        if _pinned_n:
            _done_text += f"\n📌 Pinned copy hue: `{_pinned_n}`"
        await safe_edit(status_msg, _done_text)




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
        _res = await userbot.invoke(raw.functions.messages.SendMessage(
            peer=dest_peer, message=text,
            random_id=_rand.randint(1, 2**63 - 1), top_msg_id=topic_id))
        # v95: id wapas do (pin copy ke liye); na mile to pehle jaisa True.
        _mid = _updates_msg_id(_res)
        return _mid if _mid else True
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
    await finish_status(status_msg, done_text)



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
# v79: DEFAULT DP AUR DEFAULT BIO DONO HATA DIYE GAYE — user ne bola
# pehle jaise laga deta tha waise karo. Ab jo DP/bio user dega, wahi
# lagegi; skip par koi default nahi lagega (DP skip = letter avatar,
# bio skip = khaali about).




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


async def _mk_create_public_channel(userbot, username, title=None, bio=None):
    """Ek PUBLIC channel banao + username set karo.
    Title = user ka diya hua common naam (v63), na diya ho to username.
    Bio = user ka diya hua bio (v64), na diya ho to default bio.
    Telegram about max ~255 chars hai, isliye bio ko 255 par kaato.
    v83: v64 wala seedha flow wapas — sirf create + username, koi bot
    admin isme nahi. Bot admin DP lagne ke BAAD loop me alag se hoga,
    taaki DP creation ke turant baad, pehle jaise, lag sake.
    Returns (chat_id, link) ya (None, error_string)."""
    from pyrogram import raw
    title = (title or username).strip()[:128]
    about = (bio or "").strip()[:255]  # v79: koi default bio nahi
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
        return None, str(e)
    return cid, f"https://t.me/{username}"


async def run_mkchannels(client, uid, usernames, status_msg, title=None, bio=None, dp_path=None):
    _ev = asyncio.Event()
    MKCHANNELS_ACTIVE[uid] = _ev
    try:
        await _run_mkchannels_inner(client, uid, usernames, status_msg, _ev, title=title, bio=bio, dp_path=dp_path)
    finally:
        MKCHANNELS_ACTIVE.pop(uid, None)
        # DP temp file sirf is run ke liye thi, khatam hone par delete karo.
        if dp_path and os.path.exists(dp_path):
            try:
                os.remove(dp_path)
            except Exception:
                pass



async def _run_mkchannels_inner(client, uid, usernames, status_msg, _ev, title=None, bio=None, dp_path=None):
    # v79: koi default DP nahi — dp_path None ho to koi DP nahi lagegi.
    # Jo photo user ne bheji hai, wahi lagegi, pehle jaise.
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
            cid, res = await _mk_create_public_channel(userbot, username, title=title, bio=bio)
        except FloodWait as e:
            failed.append((username, f"FloodWait {e.value}s — baaki ruk gaye"))
            print(f"[MKCHANNELS] FloodWait {username}: {e.value}s", flush=True)
            break
        except Exception as e:
            cid, res = None, str(e)
        # v83: DP v64 wala seedha tareeka — create + username ke turant
        # baad, koi verify/fallback/second-pass nahi. DP fail ho to channel
        # delete mat karo, sirf warning report me de do.
        dp_warn = ""
        if cid and dp_path and os.path.exists(dp_path):
            try:
                await userbot.set_chat_photo(cid, photo=dp_path)
                print(f"[MKCHANNELS] DP set ok @{username}", flush=True)
            except Exception as e:
                dp_warn = f" (DP fail: {e})"
                print(f"[MKCHANNELS] DP set fail @{username}: {e}", flush=True)
        if dp_warn:
            dp_failed.append((username, dp_warn))
        # Bot admin DP ke BAAD (v83): pehle DP, phir admin — DP ka rasta
        # admin step se pehle hi poora ho jaye, bilkul v64 jaisa.
        bot_warn = ""
        if cid and bot_id:
            try:
                bot_warn = await _bot_make_admin(userbot, cid, bot_id, bot_username)
            except Exception as _bae:
                bot_warn = f"bot admin fail: {_bae}"
            if bot_warn:
                print(f"[MKCHANNELS] bot admin fail @{username}: {bot_warn}", flush=True)
        if cid:
            if bot_warn:
                bot_admin_failed.append((username, bot_warn))
            entry = {"username": username, "chat_id": cid, "title": (title or username),
                     "link": res, "dp": "yes" if (dp_path and not dp_warn) else ("fail" if dp_warn else "none"),
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
            f"🖼️ DP: `{'tumhari photo' if dp_path else 'koi DP nahi (letter avatar)'}` • 📝 Bio: `{(bio[:80] if bio else 'koi bio nahi')}`")
    for c in created:
        done += f"\n• {c.get('title', c['username'])} — @{c['username']} — {c['link']}"
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
    await finish_status(status_msg, done)


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
    Returns (ok, via_bot, sent_msg_id) — v95: pin copy ke liye sent id."""
    senders = []
    if public_source:
        senders = [(client, src_bot, True), (userbot, src_userbot, False)]
    else:
        senders = [(userbot, src_userbot, False), (client, src_bot, True)]
    for sender, src, via_bot in senders:
        if sender is None or src is None:
            continue
        try:
            _sent = await sender.copy_message(dest, src, msg_id)
            return True, via_bot, (getattr(_sent, "id", None) if _sent else None)
        except Exception:
            pass
        try:
            _sent = await sender.forward_messages(dest, src, [msg_id])
            if isinstance(_sent, (list, tuple)):
                _sent = _sent[0] if _sent else None
            return True, via_bot, (getattr(_sent, "id", None) if _sent else None)
        except Exception:
            continue
    return False, False, None


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
    # v95 PIN-COPY: agar user ne source channel se forward karke bheja hai
    # to usi original message ka pin check karo (bot-chat message ka pin
    # nahi — forward_from_chat / forward_from_message_id dekho).
    _pin_src_chat = None
    _pin_src_id = None
    _fwd_chat = getattr(source_msg, "forward_from_chat", None)
    if _fwd_chat is not None:
        _pin_src_chat = getattr(_fwd_chat, "id", None)
        _pin_src_id = getattr(source_msg, "forward_from_message_id", None)
    _pinned_ids = set()
    if _pin_src_chat and _pin_src_id:
        _pinned_ids = await _get_pinned_ids(userbot, _pin_src_chat)
    _should_pin = bool(_pin_src_id and _pin_src_id in _pinned_ids)
    _pinned_n = 0
    for ch in channels:
        dest = ch.get("chat_id")
        uname = ch.get("username", "?")
        if not dest:
            failed.append((uname, "chat_id nahi hai"))
            continue
        # v72 FAST: source bot-chat me hai, isliye bot copy pehle (public fast
        # tareeka), fail ho to userbot. Download/upload kabhi nahi hota.
        ok, via_bot, _sent_id = await _copy_to_channel(
            client, userbot, dest, source_msg.chat.id, from_chat,
            source_msg.id, public_source=True)
        if ok:
            sent.append(uname)
            if via_bot:
                bot_via += 1
            if _should_pin and _sent_id:
                if await _pin_delivered(userbot, client, dest, _sent_id):
                    _pinned_n += 1
        else:
            failed.append((uname, "copy/forward fail (bot + userbot dono fail)"))
        await asyncio.sleep(SENDALL_GAP)
    done = ("✅ 𝐒𝐞𝐧𝐝𝐚𝐥𝐥 𝐜𝐨𝐦𝐩𝐥𝐞𝐭𝐞!\n"
            f"📢 Bhej diya: `{len(sent)}` / `{len(channels)}` channels me")
    if bot_via:
        done += f"\n🤖 Bot se bheje: `{bot_via}` channels (login account fail hua, bot admin se kaam hua)"
    if _pinned_n:
        done += f"\n📌 Pinned copy hue: `{_pinned_n}` channels me"
    if sent:
        done += "\n" + "\n".join(f"• @{u}" for u in sent)
    if failed:
        done += f"\n❌ Fail hue: `{len(failed)}`"
        for uname, err in failed:
            done += f"\n• @{uname} — {err}"
    await finish_status(status_msg, done)



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
    await finish_status(status_msg, done)


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
    # v95 PIN-COPY: source ke pinned IDs ek baar nikalo (public/private dono).
    _pinned_ids = await _get_pinned_ids(userbot, src_chat)
    _pinned_n = 0
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
            ok, via_bot, _sent_id = await _copy_to_channel(
                client, userbot, dest, src_chat, src_chat, m.id,
                public_source=_is_public_src)
            if ok:
                per_channel[str(dest)] = per_channel.get(str(dest), 0) + 1
                if via_bot:
                    bot_sends += 1
                if m.id in _pinned_ids and _sent_id:
                    if await _pin_delivered(userbot, client, dest, _sent_id):
                        _pinned_n += 1
            else:
                failed_ch[str(dest)] = "copy/forward fail (bot + userbot dono fail)"
            await asyncio.sleep(SENDALL_GAP)
    done = ("✅ 𝐒𝐞𝐧𝐝𝐚𝐥𝐥𝐫𝐚𝐧𝐠𝐞 𝐜𝐨𝐦𝐩𝐥𝐞𝐭𝐞!\n"
            f"📝 Messages: `{len(usable)}` (skip hue empty: `{skipped}`)\n"
            f"📢 Channels: `{len(channels)}` • Total send: `{total_sends}`")
    if _pinned_n:
        done += f"\n📌 Pinned copy hue: `{_pinned_n}`"
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
    await finish_status(status_msg, done)


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
        await finish_status(status_msg, done_text)
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
        await finish_status(status_msg, done_text)
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
        # v95 PIN-COPY: source ke pinned IDs ek baar nikalo.
        _pinned_ids = await _get_pinned_ids(userbot, src_chat)
        _pinned_n = 0
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
                    _sent_id = None
                    try:
                        from pyrogram import raw as _raw2
                        import random as _rand2
                        _res2 = await client.invoke(_raw2.functions.messages.SendMessage(
                            peer=dest_peer, message=info["text"],
                            random_id=_rand2.randint(1, 2**63 - 1), top_msg_id=dest_tid))
                        sent = True
                        _sent_id = _updates_msg_id(_res2)
                    except Exception:
                        _sent2 = await _clone_send_text(userbot, dest_peer, dest_tid, info["text"]) if userbot_in_dest else False
                        sent = bool(_sent2)
                        _sent_id = _sent2 if isinstance(_sent2, int) else None
                    if sent:
                        copied += 1
                        f_copied += 1
                        if m.id in _pinned_ids and _sent_id:
                            if await _pin_delivered(userbot, client, dest_chat, _sent_id):
                                _pinned_n += 1
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
                    # v95 PIN-COPY: source me pinned tha to dest me bhi pin karo.
                    _sent_id = ok if isinstance(ok, int) else None
                    if m.id in _pinned_ids and _sent_id:
                        if await _pin_delivered(userbot, client, dest_chat, _sent_id):
                            _pinned_n += 1
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
                # v105 SPEED: gap 2s -> 1s (userbot sends, FloodWait retry backstop hai)
                await asyncio.sleep(1)
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
        if _pinned_n:
            done_text += f"\n📌 Pinned copy hue: `{_pinned_n}`"
        if folder_report:
            done_text += "\n" + "\n".join(folder_report)
        if invite:
            done_text += f"\n🔗 Dest invite: {invite}"
        if not userbot_in_dest:
            done_text += ("\n⚠️ Dest me sirf bot admin tha, isliye pehle se bane folders ki list nahi mili — dobara fbatch chalane par duplicate folder ban sakta hai. Duplicate se bachne ke liye dest me login account ko sirf member bana do (admin zaroori nahi).")
        if cancel_event.is_set():
            done_text = "🛑 **Fbatch stopped.**\n" + done_text
        await finish_status(status_msg, done_text)
    finally:
        FBATCH_ACTIVE.pop(uid, None)


async def run_clone(client, uid, src_chat, status_msg, only_topic=None, range_start=None, range_end=None, dest_override=None, dest_title=None, mode_label="Clone"):
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
        if dest_override:
            # v103: apna group reuse karo — naya mat banao.
            dest_id = dest_override
            await safe_edit(status_msg, f"📂 **Folders mile:** `{len(topic_titles) if topics else 1}`\n"
                            f"Messages: `{total_msgs}` — apna group use kar raha hun...")
        else:
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
            # v103: reuse wale group me pehle se bane folders dobara mat banao.
            _existing = {}
            if dest_override:
                try:
                    _dt = await _clone_list_topics(userbot, dest_id)
                    _existing = {t.lower(): tid for tid, t in (_dt or [])}
                except Exception:
                    _existing = {}
            for tid, ttitle in topics:
                if tid == 1:
                    continue
                if only_topic and ttitle.lower() != only_topic.lower():
                    continue  # v72: sirf chuna hua folder hi dest me banao
                if cancel_event.is_set():
                    break
                if ttitle.lower() in _existing:
                    dest_map[tid] = _existing[ttitle.lower()]
                    continue
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
        # v95 PIN-COPY: source ke pinned IDs ek baar nikalo.
        _pinned_ids = await _get_pinned_ids(userbot, src_chat)
        _pinned_n = 0
        copied = failed = skipped = 0
        # v109: dest topic card ke counts ke liye BatchStats.
        _cstats = BatchStats(total_msgs)
        _dprog = None  # v109: dest topic ka per-file progress card
        _dmprog = None  # v116: DM ka per-file progress card (wahi stats)
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
                    _sent_id = await _clone_send_text(userbot, dest_peer, dest_tid, info["text"])
                    if _sent_id:
                        copied += 1
                        if m.id in _pinned_ids and isinstance(_sent_id, int):
                            if await _pin_delivered(userbot, client, dest_id, _sent_id):
                                _pinned_n += 1
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
                # v109: dest topic me bhi progress card — download (speed)
                # + upload (speed) dono usi folder ke topic me dikhenge.
                _cstats.done = copied + failed + skipped
                _cstats.ok = copied
                _cstats.fail = failed
                if _dprog is not None:
                    try:
                        await _dprog.delete()
                    except Exception:
                        pass
                _dprog = None
                # v116: DM me bhi wahi card — bot chat me stats dikhenge
                # (pehle sirf dest topic me card tha, DM me sirf plain
                # status text tha). DM card ka lifecycle _dprog jaisa hi.
                if _dmprog is not None:
                    try:
                        await _dmprog.delete()
                    except Exception:
                        pass
                _dmprog = None
                try:
                    _dmprog = await client.send_message(
                        uid, "⏳ Preparing…",
                        parse_mode=enums.ParseMode.MARKDOWN)
                except Exception:
                    _dmprog = None
                try:
                    _dprog = await userbot.send_message(
                        dest_id, "⏳ Preparing…",
                        reply_to_message_id=dest_tid)
                except Exception:
                    _dprog = None
                fp = await download_file(userbot, info, _dprog, _cstats, uid, client, _dmprog)
                if not fp:
                    failed += 1
                    if _dprog is not None:
                        try:
                            await safe_edit(_dprog, f"❌ Download fail • ID `{m.id}`")
                        except Exception:
                            pass
                    if _dmprog is not None:
                        try:
                            await safe_edit(_dmprog, f"❌ Download fail • ID `{m.id}`")
                        except Exception:
                            pass
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
                    caption=info.get("caption") or "", info=info,
                    prog_msg=_dprog, stats=_cstats, dm_prog=_dmprog)
                if info.get("tmpdir"):
                    shutil.rmtree(info["tmpdir"], ignore_errors=True)
                if ok:
                    copied += 1
                    # v95 PIN-COPY: source me pinned tha to naye group me bhi pin karo.
                    _sent_id = ok if isinstance(ok, int) else None
                    if m.id in _pinned_ids and _sent_id:
                        if await _pin_delivered(userbot, client, dest_id, _sent_id):
                            _pinned_n += 1
                else:
                    failed += 1
                    if _dprog is not None:
                        try:
                            await safe_edit(_dprog, f"❌ Upload fail • ID `{m.id}`")
                        except Exception:
                            pass
                    if _dmprog is not None:
                        try:
                            await safe_edit(_dmprog, f"❌ Upload fail • ID `{m.id}`")
                        except Exception:
                            pass
                # v105 SPEED: gap 2s -> 1s (FloodWait retry pehle se hai — backstop waisa hi)
                await asyncio.sleep(1)
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
        _dest_label = dest_title if dest_override else f"{src_title} Clone"
        _dest_word = "Dest group" if dest_override else "Naya group"
        done_text = (f"✅ {_fancy(mode_label + ' complete!')}\n"
                     f"📦 {_dest_word}: `{_dest_label}`{_filt}\n"
                     f"📂 Folders: `{len(topic_titles) if topics else 1}`\n"
                     f"✅ Copied: `{copied}` | ❌ Failed: `{failed}` | ⏭️ Skipped: `{skipped}`")
        if _pinned_n:
            done_text += f"\n📌 Pinned copy hue: `{_pinned_n}`"
        if invite:
            done_text += f"\n🔗 Invite: {invite}"
        if cancel_event.is_set():
            done_text = "🛑 **Clone stopped.**\n" + done_text
        await finish_status(status_msg, done_text)
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


_FANCY_BOLD = str.maketrans(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
    "".join(chr(c) for c in range(0x1D400, 0x1D41A))
    + "".join(chr(c) for c in range(0x1D41A, 0x1D434))
    + "".join(chr(c) for c in range(0x1D7CE, 0x1D7D8)))


def _fancy(s):
    """v100: fancy bold text (mathematical bold Unicode) — headings ke liye.
    Sirf A-Z/a-z/0-9 badalte hain, baaki characters waisa hi rehta hai."""
    return (s or "").translate(_FANCY_BOLD)


_SESSION_MISSING_TEXT = (
    "❌ **Session Missing!**\n\n"
    "Restricted content extract karne ke liye aapko pehle apna Telegram account connect karna hoga.\n\n"
    "👉 Kripya pehle `/login` command par click karein aur apna session configure karein.\n"
    "Pehle `/setchannel` se destination set karein."
)


def _channel_id_prompt(mode_label):
    """v102: mode-choice ke baad channel ID maangne wala prompt."""
    return (
        f"✅ {mode_label} mode selected\n\n"
        "🔥 𝐏𝐑𝐎 𝐓𝐔𝐑𝐁𝐎 𝐓𝐑𝐀𝐍𝐒𝐅𝐄𝐑 𝐏𝐈𝐏𝐄𝐋𝐈𝐍𝐄 🔥\n\n"
        "**STEP 1/2 — Source ka link bhejo**\n\n"
        "Jaise:\n"
        "• Message link — `https://t.me/channelname/123`\n"
        "• Channel link — `https://t.me/channelname`\n"
        "• Group link — `https://t.me/c/1234567890/123`\n\n"
        "Ya Channel ID (`-1001234567890`) ya Username (`@channelname`) bhi chalega.\n\n"
        "Public ho ya private, folder wala group ho ya channel — bot khud pehchan lega.\n\n"
        "Cancel: `/cancel`"
    )


def _welcome_text(uid, name=None):
    has_session = uid in USER_CLIENTS or str(uid) in _load_json(SESSIONS_FILE, {})
    login = "✅ Connected" if has_session else "❌ Not Connected"
    _fr = _free_remaining(uid)
    if _is_owner(uid):
        plan = "👑 OWNER — Lifetime"
    elif _is_premium(uid):
        plan = "👑 VIP PREMIUM"
    elif _fr is not None:
        plan = f"🛡 FREE TIER ({FREE_LIMIT - _fr}/{FREE_LIMIT} Used)"
    else:
        plan = "🛡 FREE TIER"
    _uname = ""
    if name:
        _uname = "".join(c for c in str(name).strip() if c not in "*_[]()~`")[:32] or ""
    _user_line = f"👤 User: {_uname}" if _uname else f"👤 User: `{uid}`"
    try:
        _dest_id = get_settings(uid).get("chat_id")
    except Exception:
        _dest_id = None
    dest = f"`{_dest_id}`" if _dest_id else "Personal Chat"
    dest_id = f"`{_dest_id}`" if _dest_id else "Not Set"
    return (
        "🔥 𝐔𝐋𝐓𝐑𝐀 𝐇𝐈𝐆𝐇-𝐒𝐏𝐄𝐄𝐃 𝐑𝐄𝐒𝐓𝐑𝐈𝐂𝐓𝐄𝐃 𝐂𝐎𝐍𝐓𝐄𝐍𝐓 𝐄𝐗𝐓𝐑𝐀𝐂𝐓𝐎𝐑 🔥\n"
        "══════════════════════════════\n"
        f"{plan}\n\n"
        "⚡ Bina kisi restriction ke kisi bhi Private/Public Channel ki files download aur forward karein!\n"
        "🚀 Lightning-Fast Speed | Zero Master Bot Load | 100% Secure Sessions\n\n"
        f"{_user_line}\n"
        f"🔑 Session: {login} | 🎯 Target: ❌ Not Set\n"
        f"🌋 Destination: {dest}\n"
        f"🆔 Destination ID: {dest_id}\n"
        f"💾 RAM Allocation Buffer: Turbo Mode ({MULTI_PARTS} parts/file)\n\n"
        "📌 Apna operation niche diye gaye buttons se shuru karein:"
    )


def _welcome_kb():
    # v97 SRC style: Start = /batch flow, Buy VIP = owner contact,
    # Help = commands, Any Help = owner contact, Logout = /logout.
    # (Admin panel / disclaimer / warnings ab COMMANDS list me hain.)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 𝐒𝐭𝐚𝐫𝐭 𝐅𝐨𝐫𝐰𝐚𝐫𝐝𝐢𝐧𝐠 𝐍𝐨𝐰", callback_data="home_start_batch")],
        [InlineKeyboardButton("👑 𝐁𝐮𝐲 𝐕𝐈𝐏 𝐏𝐫𝐞𝐦𝐢𝐮𝐦 𝐏𝐥𝐚𝐧", callback_data="home_buy_vip"),
         InlineKeyboardButton("❓ Help & Guide", callback_data="home_commands")],
        [InlineKeyboardButton("🆘 Any Help", callback_data="home_any_help")],
        [InlineKeyboardButton("🔄 Full Account Reset", callback_data="home_reset"),
         InlineKeyboardButton("🚪 Logout", callback_data="home_logout")],
        [InlineKeyboardButton("❌ CLOSE", callback_data="home_close")],
    ])


def _page_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ BACK", callback_data="home_start"),
         InlineKeyboardButton("❌ CLOSE", callback_data="home_close")],
    ])


COMMANDS_TEXT = (
    "𝐕𝐈𝐋𝐋𝐀𝐍 — 𝐂𝐨𝐦𝐦𝐚𝐧𝐝𝐬\n\n"
    "`/login` — Sign in with your number\n"
    "`/logout` — Sign out\n"
    "`/batch` — Hata diya gaya hai, batch ab /start -> Start Forwarding Now se niklega\n"
    "`/public` — Hata diya gaya hai, /batch use karo\n"
    "`/clone` — Hata diya gaya hai, /batch use karo\n"
    "`/create` — Naya group + folders banao\n"
    "`/mkchannels` — Naam + DP + bio + 10 username se PUBLIC channels banao (naam/DP/bio ek hi, username alag)\n"
    "`/mychannels` — Tumhare banaye channels ki list\n"
    "`/sendall` — Ek message YA range (pehli link), sab channels me bhejo\n"
    "`/addchannel` — Pehle se bana channel broadcast list me jodo\n"
    "`/sendallrange` — Hata diya gaya hai, /sendall use karo\n"
    "`/free` — Ek baar, 10 messages ka free trial\n"
    "`/makeadmin` — Saved channels me bot ko admin banao (pehle se admin wale skip)\n"
    "`/pin` — Group/channel me messages pin karo (link/ID/text, comma se)\n"
    "`/fbatch` — Folderwise batch: source se dest, folder + sequencewise copy (no delete, progress ke saath)\n"
    "`/resume` — Ruka hua batch wahi se dobara shuru karo (pehli file se nahi)\n"
    "`/setchannel` — Destination set karo (channel ID ya Private Chat)\n"
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
    _name = None
    try:
        _name = (message.from_user.first_name or "").strip() or None
    except Exception:
        pass
    if START_IMAGE:
        try:
            await message.reply_photo(
                START_IMAGE, caption=_welcome_text(uid, _name),
                parse_mode=enums.ParseMode.MARKDOWN, reply_markup=_welcome_kb())
            return
        except Exception:
            pass
    await message.reply_text(
        _welcome_text(uid, _name), parse_mode=enums.ParseMode.MARKDOWN,
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
    _uname = ""
    try:
        _uname = (query.from_user.first_name or "").strip() if query.from_user else ""
    except Exception:
        pass
    if data == "home_joined":
        if await _has_joined(uid, force=True):
            await _edit_panel(query, _welcome_text(uid, _uname), _welcome_kb())
        else:
            try:
                await query.answer(
                    "❌ Abhi bhi join nahi dikh raha. Pehle channel join karo!",
                    show_alert=True)
            except Exception:
                pass
        return
    if data == "home_start":
        await _edit_panel(query, _welcome_text(uid, _uname), _welcome_kb())
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
    if data == "home_start_batch":
        # v102 SRC style: pehle mode chuno (Public/Private), phir channel ID.
        # Mode sirf prompt badalta hai — unified /batch auto-detect waisa hi.
        if not _is_allowed(uid):
            try:
                await query.answer("⛔ Access denied.", show_alert=True)
            except Exception:
                pass
            return
        await _edit_panel(query,
            f"🚀 {_fancy('START FORWARDING NOW')}\n\n"
            "Apne source ke hisaab se mode choose karein:\n"
            "🌐 **Public Forward** — public channel direct copy\n"
            "🔒 **Private Forward** — private/group/protected transfer\n\n"
            "Free trial: 10 messages (ek baar); uske baad /buy. Premium: unlimited.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🌐 Public", callback_data="home_mode_public"),
                 InlineKeyboardButton("🔒 Private", callback_data="home_mode_private")],
                [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="home_start")],
            ]))
        return
    if data in ("home_mode_public", "home_mode_private"):
        # v102: mode chuna — ab channel ID mango (wahi /batch flow).
        if not _is_allowed(uid):
            try:
                await query.answer("⛔ Access denied.", show_alert=True)
            except Exception:
                pass
            return
        _is_priv = (data == "home_mode_private")
        if _is_priv and not (uid in USER_CLIENTS or str(uid) in _load_json(SESSIONS_FILE, {})):
            await _edit_panel(query, _SESSION_MISSING_TEXT, _page_kb())
            return
        ADDCHANNEL_STATES.pop(uid, None)
        SENDALLRANGE_STATES.pop(uid, None)
        MKCHANNELS_STATES.pop(uid, None)
        SENDALL_STATES.pop(uid, None)
        SETTING_STATES.pop(uid, None)
        CLONE_STATES.pop(uid, None)
        PUBLIC_STATES.pop(uid, None)
        USER_STATES[uid] = {"step": "WAITING_CHANNEL_ID"}
        try:
            await query.message.reply_text(
                _channel_id_prompt("Private" if _is_priv else "Public"),
                parse_mode=enums.ParseMode.MARKDOWN,
            )
        except Exception:
            pass
        return
    if data == "home_buy_vip":
        # v106: Buy VIP = seedha owner ke chat me (message pre-filled).
        await _edit_panel(query,
            "👑 **VILLAN Premium**\n\n"
            "Neeche wale button se seedha owner ke chat me jao —\n"
            "message pehle se likha hoga, bas send dabana hai.\n\n"
            "Payment manual UPI se hoti hai; plan payment ke baad activate hoga.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("💬 Owner se Baat Karo", url=_buy_owner_url())],
                [InlineKeyboardButton("⬅️ Back", callback_data="home_start")],
            ]))
        return
    if data == "home_any_help":
        # v97: Any Help = owner se contact.
        await _edit_panel(query,
            "**🆘 Any Help**\n\n"
            "Koi problem ho to owner se contact karo:\n"
            f"`{OWNER_USERNAME}`\n\n"
            "Ya `/help` likho.",
            _page_kb())
        return
    if data == "home_reset":
        # v102: Full Account Reset — confirm ke baad hi delete hoga.
        await _edit_panel(query,
            "⚠️ **Full Account Reset?**\n\n"
            "Ye tumhara session (login) + settings (destination, caption, rename, thumbnail, watermark) + auto-dest channel record DELETE kar dega. Batch history/log waisa hi rahega.\n\n"
            "Pakka reset karna hai?",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Haan, Reset Karo", callback_data="home_reset_yes")],
                [InlineKeyboardButton("❌ Cancel", callback_data="home_start")],
            ]))
        return
    if data == "home_reset_yes":
        try:
            await _cleanup_login(uid)
            await _logout_user(uid)
            reset_settings(uid)
            _ad = _load_json(AUTO_DEST_FILE, {})
            _ad.pop(str(uid), None)
            _save_json(AUTO_DEST_FILE, _ad)
        except Exception:
            pass
        await _edit_panel(query,
            "✅ Account reset ho gaya.\nSession + settings delete ho gaye.\n/start se dobara shuru karo.",
            _page_kb())
        return
    if data == "home_logout":
        # v97: Logout = /logout wala kaam (session hatao).
        try:
            await _cleanup_login(uid)
            await _logout_user(uid)
        except Exception:
            pass
        await _edit_panel(query,
            "👋 Logged out. Use /login to sign in again.",
            _page_kb())
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
    if uid in PIN_STATES:
        PIN_STATES.pop(uid, None)
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



# --- v72: /free (one-time 10-msg trial), /makeadmin ---
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
        "• Batch me range 10 se zyada di to wo shuru hi nahi hoga — chhota range do\n"
        "• Ye **sirf ek baar** milta hai, dobara `/free` nahi chalega\n"
        "• Trial khatam hone par access band ho jayega, phir premium lena hoga\n\n"
        "Shuru karo: /start karo aur 'Start Forwarding Now' dabao, pehli link bhejo, phir aakhri link bhejo (10 tak).",
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
            "Normal user Start Forwarding se save-restriction use kar sakta hai.")
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
    status_msg = await message.reply_text(f"🤖 **Makeadmin shuru:** `{len(targets)}` channels me bot ko admin bana raha hun (pehle se admin wale skip honge)...")
    done_ok, done_fail, done_skip = [], [], []
    for ch in targets:
        cid = ch.get("chat_id")
        uname = ch.get("username") or ch.get("title") or str(cid)
        if not cid:
            done_fail.append((uname, "chat_id nahi hai"))
            continue
        # v100: pehle se admin ho to skip karo — dobara promote bekar hai.
        _already = False
        try:
            _mem = await client.get_chat_member(cid, bot_id)
            _already = getattr(_mem, "status", None) in ("administrator", "creator")
        except Exception:
            _already = False
        if _already:
            done_skip.append(uname)
            try:
                _lst = _channels_load(uid)
                for _e in _lst:
                    if _e.get("chat_id") == cid:
                        _e["bot_admin"] = "yes"
                _channels_save(uid, _lst)
            except Exception:
                pass
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
    done = (f"✅ 𝐌𝐚𝐤𝐞𝐚𝐝𝐦𝐢𝐧 𝐜𝐨𝐦𝐩𝐥𝐞𝐭𝐞!\n🤖 Bot admin ban gaya: `{len(done_ok)}`\n")
    if done_skip:
        done += f"⏭️ Pehle se admin (skip): `{len(done_skip)}`\n"
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


async def _send_long_text(message, text, parse_mode=None, limit=4000):
    """v104: lamba text (jaise /help) Telegram ki 4096-char limit me
    tukdo me bhejo — line boundary par todo, taaki Markdown na toote."""
    if len(text) <= limit:
        await message.reply_text(text, parse_mode=parse_mode)
        return
    _chunks = []
    _cur = ""
    for _line in text.split("\n"):
        if _cur and len(_cur) + len(_line) + 1 > limit:
            _chunks.append(_cur)
            _cur = _line
        else:
            _cur = _cur + "\n" + _line if _cur else _line
    if _cur:
        _chunks.append(_cur)
    for _c in _chunks:
        await message.reply_text(_c, parse_mode=parse_mode)


@app.on_message(filters.command("help") & filters.private)
async def help_handler(client, message):
    """Bot kaise use karein — poori guide."""
    help_text = (
        "𝐕𝐈𝐋𝐋𝐀𝐍 — 𝐆𝐮𝐢𝐝𝐞 (𝐬𝐚𝐛 𝐜𝐨𝐦𝐦𝐚𝐧𝐝𝐬 𝐤𝐚 𝐮𝐬𝐞)\n"
        "\n"
        "🆕 NEW UPDATES — kya naya hai, kaise use hoga:\n"
        "• /batch (UNIFIED) — Channel ID do, public/private/folder khud pehchan lega; folder group me `extract` ya `clone` chuno.\n"
        "• /sendall (UNIFIED) — Ek message bhejo ya pehli link bhejo (range) — dono ek hi command me.\n"
        "• /makeadmin — Pehle se admin wale channels skip honge, sirf baaki me admin banega.\n"
        "• /pin — Link/ID/text se pin karo, comma se ek saath bhi.\n"
        "• Pin-copy — Source me pinned ho to dest channel me bhi pin hoga (sendall/sendallrange/fbatch/clone/batch-channel).\n"
        "• Net-resilience — Connection reset par download retry (2s/5s) + file retry (8s).\n"
        "• /ai hata diya gaya hai.\n"
        "• v101 — /public, /clone, /sendallrange HATA DIYE (sirf /batch + /sendall); channel se batch niklega to apna alag channel banega (ek ID = ek channel), bot auto-admin, pin-copy.\n"
        "• v102 — SRC jaisa interface: /start me naam + FREE (X/10 Used) + Target + RAM Buffer; Start Forwarding Now se Public/Private mode chuno; /setchannel se destination set karo; Full Account Reset (confirm ke saath).\n"
        "• v103 — Auto-dest har source ke liye: channel -> apna channel, group -> apna group (folderwise); ek ID = ek channel + ek group, bot auto-admin; /batch link-first prompt; stylish fancy headings.\n"
        "• v104 — Bugfix: /help ab tukdo me aayega (lamba hone par bhi chalega); Public/Private mode buttons aur /batch ka crash theek hua.\n"
        "• v105 — Speed: auto-dest channel/group par private batch ka 10s gap hata (personal chat par waisa hi); clone/fbatch gap 2s -> 1s.\n"
        "• v106 — /buy direct: button dabate hi owner ka chat khulega, message pehle se likha hoga (bold me).\n"
        "• v107 — Restart message stylish: update ka message ab fancy + sundar aayega.\n"
        "• v108 — Range UNLIMITED (5000 wala cap hata); dusri link se end ID (pehli link phir dusri link wala flow wapas); topic link ka start fix.\n"
        "• v109 — Dest progress card: auto channel/group me bhi uploading + speed + File X/Y dikhega (stylish).\n"
        "• v110 — Progress card ARROW design (20 me se chuna): sab kuch bold, ▶/▷ bar.\n"
        "• v111 — Restart fix: auto-dest group/channel ab backup se wapas aayega, restart par dusra group nahi banega.\n"
        "• v112 — Dest rule: /settings me chat ID set ho to batch wahi jayega (auto-dest nahi chalega); na ho to group -> auto group, channel -> auto channel.\n"
        "• v113 — Progress card animated: SRC BOT BY MR VILLAN typewriter, blinking dots, right-end ➤ arrows (right side open).\n"
        "• v114 — Card fix: cover lines 2 chhoti (end ➤ ab text ke edge par), File line + status me ⏳ Bacha dikhega.\n"
        "• v115 — Smooth animation: typewriter/blink ab har edit par ek kadam aage (pehle ulta chalta tha), card har 1s update.\n"
        "• v116 — Card fix: border max 30 (phone par wrap nahi hoga), clone batch me DM me bhi wahi progress card + stats.\n"
        "• v117 — Card chhota: 5 lines hata di (divider/footer/bottom) — sirf top border + header + progress lines.\n"
        "• v118 — Speed fix: card edits ab background me (upload kabhi edit ka wait nahi karega), speed wapas ~4-5 MB/s.\n"
        "• v119 — Footer wapas: har side 5 dots, ● bari bari aage-peeche chalta hai (ping-pong blink).\n"
        "• v120 — Poora card wapas (14 lines, user ke paste jaisa): border 24, footer 5 dots + blink waise hi.\n"
        "• v121 — Header naya: typewriter ki jagah ghumta spinner (◐◑◒◓) VILLAN ke pehle.\n"
        "• v122 — Fail fix: khaali message par card nahi banega (fetch pehle), rate limit ka load kam.\n"
        "• v123 — /batch hata diya: batch ab sirf Start Forwarding se (button); pehle se auto channel/group ho to poochhega — purana wala ya naya banao (source ke naam se).\n"
        "• v124 — /check diagnostic: ek message ka link do, bot batayega usme use kya dikhta hai (video/photo/text/khaali) + batch shuru hote hi log me source ka naam aayega.\n"
        "\n"
        "1. /login — Apne number se sign in karo (OTP aayega, phir 2FA ho to wo bhi). Bina login ke private groups/channels nahi khulenge.\n"
        "2. /logout — Session khatam karo.\n"
        "3. Batch (Start Forwarding se) — /batch hata diya gaya hai. Batch chalane ke liye `/start` karo, **🚀 Start Forwarding Now** dabao, Public ya Private chuno, phir source ka LINK bhejo (message link, channel/group link — ya Channel ID / @username bhi chalega). Bot khud pehchan lega — public channel ho to FAST copy, private ho to userbot se, folder wala group ho to poochhega: `extract` ya `clone`. Phir folder ka naam ya `all`, phir range: `100-500` likho, dusri message ki LINK bhejo (uska ID aakhri ID ban jayega), ek number likho, ya `all` likho — range UNLIMITED hai, koi 5000 wali limit nahi. CHANNEL se niklega to tumhare liye ek alag PRIVATE channel banega; GROUP se niklega to ek alag PRIVATE group banega (folderwise — source jaisi hi folders banengi, files wahi jayengi, plain group ki files General me jayengi). Pehle se auto channel/group bana ho to bot POOCHHEGA — purana wala use karun ya source ke naam se naya banaun. Bot dono me auto-admin hoga, aur source me pinned ho to wahan bhi pin hoga. Auto-dest ke liye `/login` zaroori hai, warna personal chat me jayega. Files ek-ek karke, sequencewise aayengi, har file par download + upload progress dikhega.\n"
        "4. /public — Hata diya gaya hai. Sab kuch Start Forwarding se hota hai: /start karo, Start Forwarding Now dabao, Public chuno, public channel ID do, fast copy khud hoga.\n"
        "5. /resume — Batch rukne par: batch rukte hi ek Resume Batch button wala message aayega, us button ko dabao ya /resume likho — batch wahi se shuru hoga jahan ruka tha, pehli file se nahi. Jo files pehle complete ho chuki hain (success ya fail, dono) wo dobara nahi aayengi, sirf aage ki bachi hui files aayengi. Agar beech me naya batch chala diya, to resume latest batch ka hi hoga.\n"
        "6. /clone — Hata diya gaya hai. Sab kuch Start Forwarding se hota hai: folder wale group se Start Forwarding karo, phir `clone` chuno — naya PRIVATE group tumhare account se banega, folders ke saath.\n"
        "7. /create — Naya group banao: pehle group ka naam bhejo, phir topics ke naam ek hi message me (har naam ek line par, ya comma se) bhejo, un naam se folders ban jayenge.\n"
        "8a. /mkchannels — Public channels banao: /mkchannels likho, pehle NAAM bhejo (sab 10 me ek hi dikhega), phir DP photo bhejo ya skip karo (koi default DP nahi lagegi), phir BIO bhejo ya skip karo (koi default bio nahi lagegi), phir 10 username ek hi message me bhejo (line ya comma se). Sirf username alag-alag hoga. Naam + DP + bio sab channels par ek saath lag jayenge. Channel tumhare login account se banega, aur har naye channel me bot ko bhi admin + Post Messages bana diya jayega, taaki baad me bot se bhi broadcast ho sake.\n"
        "8b. /mychannels — Tumhare banaye channels ki list dekho.\n"
        "8c. /sendall — UNIFIED (ek message YA range): kisi message (video / text / image) par reply karke /sendall likho, ya /sendall likh ke agla message bhejo — sab channels me jayega. Ya pehli message ka link bhejo to RANGE mode khul jayega (phir aakhri link do) — /sendallrange ka kaam bhi yahi karta hai. Pehle login account se try hoga, fail ho to bot se try hoga (bot dest me admin + Post Messages ON ho to).\n"
        "8d. /addchannel — Pehle se bana channel jodo: /addchannel likho, phir channel ka link / @username bhejo. Login account us channel ka admin hona chahiye, warna send fail hoga.\n"
        "8e. /sendallrange — Hata diya gaya hai. /sendall use karo: /sendall likh ke pehli link bhejo to range mode khul jayega. Koi limit nahi, audio / video / text sab copy honge, sequencewise, FAST copy se (public source par bot copy pehle).\n"
        "8f. /free — Ek ID par sirf ek baar, 10 messages ka trial.\n"
        "8h. /makeadmin — Saved channels me bot ko add + admin banao (ya /makeadmin @channel). Pehle se admin wale channels SKIP honge, sirf baaki me admin banega.\n"
        "8i. /pin — Group/channel me messages pin karo: /pin likho, group link bhejo, phir link / ID / text bhejo (comma se ek saath bhi chalega). Text do to us text wale saare messages pin honge (max 10).\n"
        "8. /fbatch — Folderwise batch (folderwise kaam ke liye ek hi command hai): pehle SOURCE group/channel ka link bhejo, phir DEST group ka link bhejo. Bot naam padh ke topic samjhega, dest me us naam ka folder banayega, aur us naam wali saari cheezein (pdf / text / video / pic) us folder me sequencewise COPY karega. Delete kuch nahi hoga, source waisa ka waisa rahega. Har file par download + upload progress dikhega, jaise /batch me dikhta hai. Dest me sirf bot admin ho to bhi chalega, par duplicate folder se bachne ke liye dest me login account ko sirf member bana do. Dest group me Topics ON hona chahiye.\n"
        "9. /setchannel — Destination set karo: /setchannel likho, channel/group ID bhejo (jaise -100...), ya \"Save to My Private Chat\" button dabao. Wahi destination /settings wale chat_id me save hota hai.\n"
        "10. /settings — Rename, caption template, word remove/replace, thumbnail, watermark, upload method badlo.\n"
        "11. /plan — Apna plan dekho. /buy — Premium kharido: button dabate hi owner ka chat khul jayega, message pehle se likha hoga (bas send dabao). Payment manual UPI hai.\n"
        "12. /cancel — Chal raha kaam roko (batch, clone, create, fbatch — koi bhi). Batch rokne ke baad resume karne ke liye button ya /resume use karo.\n"
        "\n"
        "Seedha link bhejne par ek file ki copy hogi. Folderwise kaam /batch (folder group) ya /fbatch se hoga.\n"
        "\n"
        f"Owner: `{OWNER_USERNAME}`"
    )

    await _send_long_text(message, help_text, parse_mode=enums.ParseMode.MARKDOWN)


def _buy_owner_url():
    """v106: owner ke chat ka link, buy wala message pehle se likha hua
    (fancy BOLD — t.me ?text= me plain text hi jata hai, isliye Unicode
    bold use kiya hai)."""
    from urllib.parse import quote as _quote
    _uname = (OWNER_USERNAME or "").lstrip("@")
    _want = _fancy("Hello brother, I want to buy premium SRC bot")
    return f"https://t.me/{_uname}?text={_quote(_want)}"


@app.on_message(filters.command("buy") & filters.private)
async def buy_handler(client, message):
    """v106: /buy — seedha owner ke chat me le jao, message pre-filled."""
    await message.reply_text(
        "👑 **VILLAN Premium**\n\n"
        "Neeche wale button se seedha owner ke chat me jao —\n"
        "message pehle se likha hoga, bas send dabana hai.\n\n"
        "Payment manual UPI se hoti hai; plan payment ke baad activate hoga.",
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("💬 Owner se Baat Karo", url=_buy_owner_url())],
        ]),
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
    """v123: /batch HATA DIYA GAYA — batch ab sirf Start Forwarding se
    niklega (/start -> 'Start Forwarding Now' button)."""
    await message.reply_text(
        "❌ `/batch` hata diya gaya hai.\n\n"
        "Batch ab **Start Forwarding** se niklega:\n"
        "• `/start` karo\n"
        "• **🚀 Start Forwarding Now** dabao\n"
        "• Public ya Private chuno, phir source ka link bhejo.")


@app.on_message(filters.command("check") & filters.private)
async def check_handler(client, message):
    """v124: /check — ek message ka link do, bot batayega use kya dikhta hai.
    Batch me NO MEDIA/TEXT fail ho raha ho to pehle /check se dekho:
    video dikhta hai ya nahi, aur bot kaun si chat padh raha hai."""
    uid = message.chat.id
    if not await _has_joined(uid, force=True):
        await _send_join_wall(message)
        return
    if not _is_allowed(uid):
        await message.reply_text("⛔ Access denied.")
        return
    arg = (message.text or "").replace("/check", "", 1).strip()
    if not arg:
        await message.reply_text(
            "🔍 **/check ka use:**\n"
            "Ek message ka link bhejo, jaise:\n"
            "`/check https://t.me/c/1234567890/6`\n\n"
            "Bot batayega us message me use kya dikhta hai — "
            "video, photo, text, ya khaali.",
            parse_mode=enums.ParseMode.MARKDOWN)
        return
    chat_id, preset = _parse_channel_input(arg)
    if chat_id is None or not preset:
        await message.reply_text(
            "❌ Link samajh nahi aaya.\n"
            "Message ka poora link bhejo, jaise:\n"
            "`/check https://t.me/c/1234567890/6`",
            parse_mode=enums.ParseMode.MARKDOWN)
        return
    userbot = await get_userbot(uid)
    is_public = isinstance(chat_id, str)
    fetch_client = client if is_public else userbot
    if fetch_client is None:
        await message.reply_text("❌ Login required. Use /login first.")
        return
    # Source ka naam/type nikalo — yehi batayega bot kaun si chat padh raha hai.
    chat, _is_pub, is_forum = await _resolve_batch_source(client, userbot, chat_id)
    ctitle = getattr(chat, "title", None) or str(chat_id)
    ctitle = ctitle.replace("*", "").replace("_", "").replace("[", "").replace("]", "").replace("`", "")
    ctype = getattr(chat, "type", None)
    ctype = ctype.value if ctype is not None else "?"
    try:
        m = await fetch_client.get_messages(chat_id, preset)
    except Exception as e:
        await message.reply_text(f"❌ Fetch nahi hua: `{e}`")
        return
    if isinstance(m, list):
        m = m[0] if m else None
    if not m:
        await message.reply_text(f"❌ Message {preset} mila hi nahi.")
        return
    media = getattr(m, "media", None)
    media_s = media.value if media is not None else "—"
    has_text = bool(getattr(m, "text", None))
    has_cap = bool(getattr(m, "caption", None))
    is_empty = bool(getattr(m, "empty", False))
    is_service = getattr(m, "service", None) is not None
    would_take = bool(media) or has_text
    verdict = ("✅ Batch isko lega" if would_take
               else "❌ Batch isko SKIP karega (NO MEDIA/TEXT)")
    await message.reply_text(
        f"🔍 **/check result — msg {preset}**\n\n"
        f"**Source:** {ctitle}\n"
        f"**Type:** {ctype}" + (" (forum)" if is_forum else "") + "\n"
        f"**Chat ID:** `{chat_id}`\n\n"
        f"**Message khaali hai:** {'haan' if is_empty else 'nahi'}\n"
        f"**Service message hai:** {'haan' if is_service else 'nahi'}\n"
        f"**Media:** {media_s}\n"
        f"**Text hai:** {'haan' if has_text else 'nahi'}\n"
        f"**Caption hai:** {'haan' if has_cap else 'nahi'}\n\n"
        f"{verdict}",
        parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.command("clone") & filters.private)
async def clone_start_handler(client, message):
    """v101: /clone HATA DIYA GAYA — sab Start Forwarding se hota hai."""
    await message.reply_text(
        "❌ `/clone` hata diya gaya hai.\n"
        "Ab sab kuch Start Forwarding se hota hai — folder wale group se "
        "Start Forwarding karo, phir `clone` chuno.")


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
            "Normal user Start Forwarding se save-restriction use kar sakta hai.")
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
            "ya `skip` likho to koi DP nahi lagegi (letter avatar rahega).\n"
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


# --- v96 PIN: /pin — kisi group/channel me messages pin karo ---
# Flow: /pin -> group/channel link -> kya pin karna hai (ek line me ek)
# Har line ho sakti hai: message link (t.me/.../123), sirf ID (123),
# ya plain text (us text wala message dhoondh kar pin karega — sirf
# tab jab EXACTLY 1 match mile, warna fail list me batayega).
# v96 text-pin update: text do to us text wale saare messages pin honge
# (max 10) — sirf 1 match wali shart hata di.
# Pin pehle userbot (login account) se, phir bot se — _pin_delivered
# reuse. Pin fail = delivery fail nahi, report me aayega.
PIN_STATES = {}  # uid -> {"step": "WAITING_GROUP"/"WAITING_ITEMS", "group": chat_id, "title": str}


async def run_pin(client, uid, group_chat, status_msg, items):
    """items: [{"chat": chat_id|None, "msg_id": int|None, "text": str|None, "label": str}]"""
    userbot = await get_userbot(uid)
    if not userbot:
        await message_reply(status_msg, "❌ Pehle `/login` karo — pin tumhare account se hoga.")
        return
    pinned, failed = [], []
    for it in items:
        _chat = it.get("chat") or group_chat
        _mid = it.get("msg_id")
        _label = it.get("label") or str(_mid or it.get("text") or "?")
        if _mid is None and it.get("text"):
            # v96 text-pin: us text wale saare messages pin karo (max 10).
            # Pehle sirf exactly 1 match par pin hota tha — user ne manga
            # ki text do to wo bhi pin ho, isliye ab jitne mile sab pin honge.
            try:
                _found = []
                async for _m in userbot.search_messages(_chat, query=it["text"], limit=10):
                    if _m and getattr(_m, "id", None):
                        _found.append(int(_m.id))
                if not _found:
                    failed.append((_label, "text se koi message nahi mila"))
                    continue
                _ok_n = 0
                for _fid in _found:
                    if await _pin_delivered(userbot, client, _chat, _fid):
                        _ok_n += 1
                    await asyncio.sleep(1)
                if _ok_n:
                    pinned.append(f"{_label} ({_ok_n} mile)")
                else:
                    failed.append((_label, "pin fail — login account/bot ko pin ka haq nahi hai"))
                continue
            except Exception as e:
                failed.append((_label, str(e)))
                continue
        if _mid is None:
            failed.append((_label, "ID/link samajh nahi aaya"))
            continue
        if await _pin_delivered(userbot, client, _chat, int(_mid)):
            pinned.append(_label)
        else:
            failed.append((_label, "pin fail — login account/bot ko pin ka haq nahi hai"))
        await asyncio.sleep(1)
    done = ("✅ 𝐏𝐢𝐧 𝐜𝐨𝐦𝐩𝐥𝐞𝐭𝐞!\n"
            f"📌 Pinned: `{len(pinned)}` | ❌ Failed: `{len(failed)}`")
    if pinned:
        done += "\n" + "\n".join(f"• {x}" for x in pinned[:20])
    if failed:
        done += "\n❌ Fail hue:"
        for _lab, _err in failed[:20]:
            done += f"\n• {_lab} — {_err}"
    await finish_status(status_msg, done)


@app.on_message(filters.command("pin") & filters.private)
async def pin_start_handler(client, message):
    if not await _has_joined(message.chat.id, force=True):
        await _send_join_wall(message)
        return
    if not _is_allowed(message.chat.id):
        await message.reply_text("⛔ Access denied.")
        return
    print(f"📌 Processing /pin for user {message.chat.id}", flush=True)
    ADDCHANNEL_STATES.pop(message.chat.id, None)
    SENDALLRANGE_STATES.pop(message.chat.id, None)
    USER_STATES.pop(message.chat.id, None)
    CLONE_STATES.pop(message.chat.id, None)
    CREATE_STATES.pop(message.chat.id, None)
    PUBLIC_STATES.pop(message.chat.id, None)
    FBATCH_STATES.pop(message.chat.id, None)
    MKCHANNELS_STATES.pop(message.chat.id, None)
    SENDALL_STATES.pop(message.chat.id, None)
    PIN_STATES[message.chat.id] = {"step": "WAITING_GROUP"}
    await message.reply_text(
        "**📌 PIN — STEP 1/2**\n\n"
        "Kis **group/channel** me pin karna hai? Uska **link** bhejo.\n"
        "Jaise: `https://t.me/mychannel` ya `https://t.me/c/1234567890` ya `@mychannel`\n\n"
        "**❌ Cancel:** `/cancel`",
        parse_mode=enums.ParseMode.MARKDOWN,
    )


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
            "Normal user Start Forwarding se save-restriction use kar sakta hai.")
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
        "Ya **pehli message ka link** bhejo (range ke liye — phir aakhri link mangunga, /sendallrange ka kaam bhi yahi karta hai).\n\n"
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
            "Normal user Start Forwarding se save-restriction use kar sakta hai.")
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
    """v101: /sendallrange HATA DIYA GAYA — sab /sendall se hota hai."""
    await message.reply_text(
        "❌ `/sendallrange` hata diya gaya hai.\n"
        "Ab sab kuch `/sendall` se hota hai — `/sendall` likh ke pehli message "
        "ka link bhejo, phir aakhri link do.")


@app.on_message(filters.command("public") & filters.private)
async def public_start_handler(client, message):
    """v101: /public HATA DIYA GAYA — sab Start Forwarding se hota hai."""
    await message.reply_text(
        "❌ `/public` hata diya gaya hai.\n"
        "Ab sab kuch Start Forwarding se hota hai — /start karo, "
        "'Start Forwarding Now' dabao, Public chuno, channel ID do, "
        "bot khud pehchan lega (public ho to fast copy).")


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


@app.on_message(filters.command("setchannel") & filters.private)
async def setchannel_start_handler(client, message):
    """v102: destination set karo (SRC /setchannel jaisa).
    Channel/group ID bhejo, ya Private Chat button dabao.
    Settings chat_id hi save hota hai (wahi jo /settings me Set Chat ID karta hai)."""
    uid = message.chat.id
    if not await _has_joined(uid, force=True):
        await _send_join_wall(message)
        return
    if not _is_allowed(uid):
        await message.reply_text("⛔ Access denied.")
        return
    USER_STATES.pop(uid, None)
    SETTING_STATES.pop(uid, None)
    SETCHANNEL_STATES[uid] = {"step": "WAITING_ID"}
    await message.reply_text(
        "🎯 **Target Channel / Group ID Send Karein:**\n\n"
        "💡 Example: `-1001234567890`\n\n"
        "Ya neeche wale button se apne Private Chat me save karo.",
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("💾 Save to My Private Chat", callback_data="setchannel_private")],
            [InlineKeyboardButton("❌ Cancel", callback_data="setchannel_cancel")],
        ]),
    )


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
    if data in ("dest_old", "dest_new"):
        # v123: dest choice — purana dest ya naya banao.
        await _resume_dest_choice(client, query, "old" if data == "dest_old" else "new")
        return
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
    if data == "setchannel_private":
        # v102: destination = apna private chat (settings chat_id clear).
        try:
            await query.answer()
        except Exception:
            pass
        SETCHANNEL_STATES.pop(uid, None)
        update_setting(uid, "chat_id", None)
        await query.message.reply_text(
            "✅ **Destination set to Private Chat!**\n\n"
            "⚠️ Note: Telegram private chat (DM) mein bulk forwarding par 2 ghante ka FloodWait lag sakta hai. "
            "Bade batches ke liye Channel/Group banakar uski ID /setchannel se use karein.\n\n"
            "Ab /start karo aur 'Start Forwarding Now' dabao.",
            parse_mode=enums.ParseMode.MARKDOWN)
        return
    if data == "setchannel_cancel":
        try:
            await query.answer("Cancelled.")
        except Exception:
            pass
        SETCHANNEL_STATES.pop(uid, None)
        try:
            await query.message.delete()
        except Exception:
            pass
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
         "mkchannels", "mychannels", "sendall", "free", "makeadmin",
         "addchannel", "sendallrange", "public", "clone", "check"]
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
                "ya `skip` likho to koi DP nahi lagegi (letter avatar rahega).\n"
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
                    "**STEP 3/4 — Ab channel ki BIO bhejo**\n\nDP skip ki — koi DP nahi lagegi, letter avatar rahega.\n"
                    "Ye bio **sab channels me ek hi** lagegi (max 255 chars).\n"
                    "Jaise: `Dholu Bholu Kids — Learn, Play, Grow Together`\n"
                    "Ya `skip` likho to koi bio nahi lagegi.\n\n"
                    "**❌ Cancel:** `/cancel`",
                    parse_mode=enums.ParseMode.MARKDOWN,
                )
                return
            await message.reply_text(
                "📸 DP ke liye **photo bhejo**, ya `skip` likho to koi DP nahi lagegi.")
            return
        if _mk_step == "WAITING_BIO":
            _bio_skipped = text.strip().lower() in ("skip", "default", "no", "none")
            if _bio_skipped:
                bio = ""
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
                f"• **Bio:** `{'koi bio nahi' if _bio_skipped else 'tumhari bio lagegi'}`\n"
                "• Sirf **username alag-alag** hoga.\n"
                "• Ek run me **max 10** username, har channel ke beech **2 MIN ka gap** hai.\n\n"
                "**❌ Cancel:** `/cancel`",
                parse_mode=enums.ParseMode.MARKDOWN,
            )
            return
        _mk_title = MKCHANNELS_STATES[user_id].get("title")
        _mk_bio = MKCHANNELS_STATES[user_id].get("bio") or ""
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

    # Pin flow v96: (1) group link -> (2) pin karne wali list -> pin
    if user_id in PIN_STATES:
        _pstep = PIN_STATES[user_id].get("step")
        if _pstep == "WAITING_GROUP":
            _g = _clone_parse_link(text)
            if _g is None:
                _t = text.strip().lstrip("@")
                if _t.lstrip("-").isdigit():
                    _g = int(_t)
                elif re.fullmatch(r"[a-zA-Z0-9_]{4,32}", _t):
                    _g = _t
            if _g is None:
                await message.reply_text("❌ **Invalid link!** Group/channel ka link bhejo (jaise `https://t.me/mychannel` ya `@mychannel`).")
                return
            _ub = await get_userbot(user_id)
            if not _ub:
                PIN_STATES.pop(user_id, None)
                await message.reply_text("❌ Pehle `/login` karo — pin tumhare account se hoga.")
                return
            try:
                _ch = await _ub.get_chat(_g)
                _gid = _ch.id
                _title = getattr(_ch, "title", None) or str(_gid)
            except Exception as e:
                await message.reply_text(f"❌ Group/channel nahi khul raha: {e}\nCheck karo ki tumhara account uska member hai, phir link dobara bhejo.")
                return
            PIN_STATES[user_id] = {"step": "WAITING_ITEMS", "group": _gid, "title": _title}
            await message.reply_text(
                f"**📌 Group lock ho gaya:** `{_title}`\n\n"
                "**STEP 2/2 — Kya pin karna hai? Ek hi message me bhejo, comma se ya har line me ek.**\n\n"
                "Jaise: `123, 456, 789` ya link comma se, ya ek line me ek.\n\n"
                "Har item ho sakta hai:\n"
                "• Message link — jaise `https://t.me/mychannel/123`\n"
                "• Sirf ID — jaise `123`\n"
                "• Plain text — us text wale saare messages pin honge (max 10), jaise `Maths Chapter 1`\n\n"
                "**❌ Cancel:** `/cancel`",
                parse_mode=enums.ParseMode.MARKDOWN,
            )
            return
        if _pstep == "WAITING_ITEMS":
            _items = []
            # v96 fix: comma se bhi bhej sakte ho — "123, 456, 789" ya link
            # comma se, ya ek line me ek. Dono tareeke ek saath bhi chalenge.
            for _line in re.split(r"[,\n]+", text or ""):
                _line = _line.strip()
                if not _line:
                    continue
                _cid, _mid = parse_tg_link(_line)
                if _cid and _mid:
                    _items.append({"chat": _cid, "msg_id": _mid, "label": _line})
                elif _line.lstrip("-").isdigit():
                    _items.append({"chat": None, "msg_id": int(_line), "label": _line})
                else:
                    _items.append({"chat": None, "msg_id": None, "text": _line, "label": _line})
            if not _items:
                await message.reply_text("❌ Kuch samajh nahi aaya. Message link ya ID bhejo, comma ya line se alag karke.")
                return
            _st = PIN_STATES.pop(user_id)
            status_msg = await message.reply_text(
                f"📌 **Pin Started!** `{_st.get('title', _st.get('group'))}` me `{len(_items)}` items ek-ek karke pin kar raha hun...")
            await run_pin(client, user_id, _st.get("group"), status_msg, _items)
            return

    # Sendall flow v100 UNIFIED: /sendall ke baad bheja gaya message —
    # sirf message link ho to RANGE mode, warna ek message sab channels me.
    if user_id in SENDALL_STATES:
        _cid, _mid = parse_tg_link(text) if text else (None, None)
        if _cid and _mid and not message.media:
            SENDALL_STATES.pop(user_id, None)
            SENDALLRANGE_STATES[user_id] = {"step": "WAITING_LAST", "chat_id": _cid, "start_id": _mid}
            await message.reply_text(
                "**Sendallrange — Start locked.**\n\n"
                f"Start: `{_mid}`\n"
                "Ab **aakhri message ka link** bhejo.\n\n"
                "**❌ Cancel:** `/cancel`",
                parse_mode=enums.ParseMode.MARKDOWN,
            )
            return
        SENDALL_STATES.pop(user_id, None)
        status_msg = await message.reply_text(
            "📢 **Sendall Started!** Sab channels me ek-ek karke bhej raha hun...")
        await run_sendall(client, user_id, message, status_msg)
        return

    # Setchannel flow v102: destination channel/group ID ka jawab
    if user_id in SETCHANNEL_STATES:
        _cid = None
        _title = None
        _t = text.strip()
        if _t.lstrip("-").isdigit():
            _cid = int(_t)
        else:
            _ident = _add_resolve_identifier(_t)
            if _ident is not None:
                try:
                    _fc = await get_userbot(user_id) or client
                    _ch = await _fc.get_chat(_ident)
                    _cid = _ch.id
                    _title = getattr(_ch, "title", None)
                except Exception:
                    _cid = None
        if not _cid:
            await message.reply_text(
                "❌ ID samajh nahi aaya. Channel/group ID bhejo, jaise `-1001234567890`, ya /cancel likho.")
            return
        # Validate: chat khul raha hai ya nahi
        try:
            _fc = await get_userbot(user_id) or client
            _ch = await _fc.get_chat(_cid)
            _title = getattr(_ch, "title", None) or str(_cid)
        except Exception as e:
            await message.reply_text(f"❌ Channel/group nahi khul raha: {e}\nID check karo, ya pehle /login karo.")
            return
        SETCHANNEL_STATES.pop(user_id, None)
        update_setting(user_id, "chat_id", _cid)
        await message.reply_text(
            f"✅ **Destination set to {_title}!**\n"
            f"Destination ID: `{_cid}`\n\n"
            "Ab /batch ki files yahi jayengi.\n"
            "(Channel source par auto-dest channel chalega — group source ke liye ye destination use hoga.)",
            parse_mode=enums.ParseMode.MARKDOWN)
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
                "Private ke liye /start karo aur 'Start Forwarding Now' dabao, ya public channel ka link bhejo.")
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
        # v108: range UNLIMITED — BATCH_LIMIT check hata diya gaya hai.

        status_msg = await message.reply_text(
            f"Public fast started — `{total_files}` items."
        )
        await run_batch(client, user_id, chat_id, start_id, end_id, status_msg,
                        public_fast=True)
        return

    elif (
        user_id in USER_STATES
        and USER_STATES[user_id].get("step") == "WAITING_CHANNEL_ID"
    ):
        # v98 UNIFIED BATCH step 1: channel ID / username / link / message link.
        _cid, _preset = _parse_channel_input(text)
        if _cid is None:
            await message.reply_text(
                "❌ Channel ID samajh nahi aaya. Aise bhejo:\n"
                "`-1001234567890` ya `@channelname` ya `https://t.me/channelname`\n"
                "Message link bhi chalega: `https://t.me/channelname/123`")
            return
        _ub = await get_userbot(user_id)
        _chat, _is_pub, _is_forum = await _resolve_batch_source(client, _ub, _cid)
        if _chat is None:
            await message.reply_text(
                "❌ Channel/group nahi khul raha.\n"
                "• Public ho to bot ko group me add karo, ya link check karo\n"
                "• Private ho to pehle `/login` karo, aur account uska member hona chahiye.")
            return
        if not _is_pub and _ub is None:
            await message.reply_text("❌ Private source ke liye pehle `/login` karo.")
            return
        _st = {"step": "WAITING_RANGE", "chat_id": _cid, "is_public": _is_pub,
               "preset_start": _preset, "only_topic": None, "folder_name": None}
        _cname = getattr(_chat, "title", None) or str(_cid)
        _st["name"] = _cname
        _st["is_forum"] = _is_forum
        _st["is_group"] = getattr(_chat, "type", None) in ("group", "supergroup")
        _ctype = "Public" if _is_pub else "Private"
        if _is_forum:
            if _ub is None:
                await message.reply_text("❌ Folder wale group ke liye pehle `/login` karo.")
                return
            _topics = await _clone_list_topics(_ub, _cid)
            if _topics:
                _tnames = ", ".join(t for _, t in _topics[:20])
                _st["step"] = "WAITING_MODE"
                _st["topics"] = {t.lower(): tid for tid, t in _topics}
                _st["topic_names"] = _tnames
                USER_STATES[user_id] = _st
                await message.reply_text(
                    f"📂 **Folder wala group hai** (`{_cname}`). Folders: `{_tnames}`\n\n"
                    "**Kya karna hai?**\n"
                    "• `extract` — Files apne chat/dest me niklegi (batch jaisa)\n"
                    "• `clone` — Naya PRIVATE group banega, folders ke saath copy hoga\n\n"
                    "`extract` ya `clone` likho.\n\n"
                    "**❌ Cancel:** `/cancel`",
                    parse_mode=enums.ParseMode.MARKDOWN,
                )
                return
        USER_STATES[user_id] = _st
        _range_hint = f"Start locked: `{_preset}` — ab aakhri ID bhejo ya `all` likho.\n\n" if _preset else ""
        await message.reply_text(
            "**📦 Batch — STEP 2/2 — Range bhejo**\n\n"
            f"Source: `{_cname}` ({_ctype})\n"
            f"{_range_hint}"
            "Message ID range bhejo, jaise **`100-500`**\n"
            "Ya dusri message ki **link** bhejo — uska ID aakhri ID ban jayega.\n"
            "Ya **`all`** likho to poori history nikalegi (koi limit nahi).\n"
            "(Ek number, jaise `100`, ka matlab hai 100 se aakhri tak.)\n\n"
            "**❌ Cancel:** `/cancel`",
            parse_mode=enums.ParseMode.MARKDOWN,
        )
        return

    elif (
        user_id in USER_STATES
        and USER_STATES[user_id].get("step") == "WAITING_MODE"
    ):
        # v100: folder wale group me EXTRACT ya CLONE chuno.
        _m = text.strip().lower()
        if _m in ("extract", "e", "extract karo", "nikalo", "nikalna hai"):
            _mode = "extract"
        elif _m in ("clone", "c", "clone karo", "copy karo"):
            _mode = "clone"
        else:
            await message.reply_text("❌ `extract` ya `clone` likho.")
            return
        USER_STATES[user_id]["mode"] = _mode
        USER_STATES[user_id]["step"] = "WAITING_FOLDER"
        _tnames = USER_STATES[user_id].get("topic_names", "")
        await message.reply_text(
            f"📂 Folders: `{_tnames}`\n\n"
            "Kaunsa folder nikalna hai? Folder ka **naam** bhejo, ya **`all`** likho to saare folders nikalenge.\n\n"
            "**❌ Cancel:** `/cancel`",
            parse_mode=enums.ParseMode.MARKDOWN,
        )
        return

    elif (
        user_id in USER_STATES
        and USER_STATES[user_id].get("step") == "WAITING_FOLDER"
    ):
        # v98 UNIFIED BATCH step 1.5: folder chuno (sirf forum group par).
        _folder = text.strip()
        _topics = USER_STATES[user_id].get("topics") or {}
        if _folder.lower() in ("all", "sab", "all folders"):
            _only = None
        else:
            _only = _topics.get(_folder.lower())
            if _only is None:
                await message.reply_text(
                    "❌ Ye folder nahi mila. List me se naam bhejo, ya `all` likho.")
                return
        USER_STATES[user_id]["step"] = "WAITING_RANGE"
        USER_STATES[user_id]["only_topic"] = _only
        USER_STATES[user_id]["folder_name"] = _folder
        _preset = USER_STATES[user_id].get("preset_start")
        _range_hint = f"Start locked: `{_preset}` — ab aakhri ID bhejo ya `all` likho.\n\n" if _preset else ""
        await message.reply_text(
            "**📦 Batch — STEP 2/2 — Range bhejo**\n\n"
            f"{_range_hint}"
            "Message ID range bhejo, jaise **`100-500`**\n"
            "Ya dusri message ki **link** bhejo — uska ID aakhri ID ban jayega.\n"
            "Ya **`all`** likho to poori history nikalegi (koi limit nahi).\n"
            "(Ek number, jaise `100`, ka matlab hai 100 se aakhri tak.)\n\n"
            "**❌ Cancel:** `/cancel`",
            parse_mode=enums.ParseMode.MARKDOWN,
        )
        return

    elif (
        user_id in USER_STATES
        and USER_STATES[user_id].get("step") == "WAITING_DEST_CHOICE"
    ):
        # v123: dest choice ka intezar — button dabao (text se nahi).
        await message.reply_text(
            "👆 Upar wale buttons se chuno — **Purana wala** ya **Naya banao**.\n\n"
            "Cancel karna ho to `/cancel` likho.",
            parse_mode=enums.ParseMode.MARKDOWN)
        return

    elif (
        user_id in USER_STATES
        and USER_STATES[user_id].get("step") == "WAITING_RANGE"
    ):
        # v98 UNIFIED BATCH step 2: range -> run_batch (public/private/folder auto).
        _st = USER_STATES[user_id]
        # v108: dusri LINK bhi chalegi — pehli link (start lock) ke baad
        # dusri message link bhejo to uska message ID aakhri ID ban jayega
        # (purana first-link/second-link flow wapas). Galat source ki
        # link ho to mana kar dega.
        _link_mid = None
        if "t.me/" in (text or ""):
            _lc, _link_mid = _parse_channel_input(text)
            if _link_mid and isinstance(_lc, int) and isinstance(_st.get("chat_id"), int) and _lc != _st["chat_id"]:
                await message.reply_text("❌ Dusri link pehli wali source ki honi chahiye.")
                return
        if _link_mid:
            _start = _st.get("preset_start")
            _end = _link_mid
            _is_all = False
            if _start is None:
                _start, _end, _is_all = _link_mid, None, True
        else:
            _start, _end, _is_all = _parse_batch_range(text, _st.get("preset_start"))
        if _start is None:
            await message.reply_text(
                "❌ Range samajh nahi aaya. Jaise `100-500` bhejo, dusri link bhejo, ya `all` likho.")
            return
        _src = _st["chat_id"]
        _is_pub = _st.get("is_public", isinstance(_src, str))
        # v112 DEST RULE: /settings me chat_id set ho to batch WAHI
        # jayega — auto-dest (group/channel) bilkul nahi chalega. Chat_id
        # na ho to group source -> auto group, channel source -> auto
        # channel (pehle wala rule).
        _set_dest = get_settings(user_id).get("chat_id")
        if _st.get("mode") == "clone":
            # v100: CLONE mode — naya private group banega (run_clone).
            USER_STATES.pop(user_id, None)
            _fold = _st.get("folder_name")
            if (_fold or "").lower() in ("all", "sab", "all folders"):
                _fold = None
            _rt = (text or "").strip().lower()
            if _rt in ("all", "sab", "poora", "full"):
                _rs = _st.get("preset_start")
                _re = None
            elif _is_all:
                _rs, _re = _start, None
            else:
                _rs, _re = _start, _end
            status_msg = await message.reply_text(
                "🔥 𝐏𝐑𝐎 𝐓𝐔𝐑𝐁𝐎 𝐓𝐑𝐀𝐍𝐒𝐅𝐄𝐑 𝐏𝐈𝐏𝐄𝐋𝐈𝐍𝐄 🔥\nClone started — naya private group ban raha hai, folders ke saath..."
            )
            await run_clone(client, user_id, _src, status_msg,
                            only_topic=_fold, range_start=_rs, range_end=_re)
            return
        if _st.get("is_group") and _st.get("mode") != "clone":
            # v103: GROUP source — apna PRIVATE forum group (ek ID = ek group),
            # folderwise copy (run_clone, dest reuse). Range + folder filter lagte hain.
            USER_STATES.pop(user_id, None)
            _fold = _st.get("folder_name")
            if (_fold or "").lower() in ("all", "sab", "all folders"):
                _fold = None
            _rt = (text or "").strip().lower()
            if _rt in ("all", "sab", "poora", "full"):
                _rs = _st.get("preset_start")
                _re = None
            elif _is_all:
                _rs, _re = _start, None
            else:
                _rs, _re = _start, _end
            _ub3 = await get_userbot(user_id)
            if _ub3 is None:
                await message.reply_text("❌ Group se nikalne ke liye pehle `/login` karo.")
                return
            if _set_dest:
                # v112: settings chat_id set hai — wahi dest hai. Type
                # check karo: channel ho to folderwise nahi chalega, isliye
                # run_batch (flat) se wahan bhej do; group ho to run_clone
                # folderwise. ID hi galat ho to saaf error deke roko.
                _sd_type = None
                try:
                    _sd = await _ub3.get_chat(_set_dest)
                    _sd_type = str(getattr(_sd, "type", "")).lower()
                except Exception:
                    await message.reply_text(
                        "❌ Settings me jo chat ID hai wo khul nahi raha — ID galat hai ya tumhara account usme nahi hai.\n"
                        "/settings se sahi ID dalo, ya hata do (phir auto group chalega).")
                    return
                if "channel" in _sd_type:
                    _brs = _rs if _rs is not None else 1
                    _bre = _re
                    if _bre is None:
                        try:
                            async for _m in _ub3.get_chat_history(_src, limit=1):
                                _bre = _m.id
                                break
                        except Exception:
                            _bre = None
                        if not _bre:
                            await message.reply_text("❌ History nahi mili — source check karo, phir dobara Start Forwarding se karo.")
                            return
                    status_msg = await message.reply_text(
                        "🔥 𝐏𝐑𝐎 𝐓𝐔𝐑𝐁𝐎 𝐓𝐑𝐀𝐍𝐒𝐅𝐄𝐑 𝐏𝐈𝐏𝐄𝐋𝐈𝐍𝐄 🔥\n"
                        f"📂 {_fancy('Dest')}: `{_set_dest}` (settings chat ID — flat)"
                    )
                    await run_batch(client, user_id, _src, _brs, _bre, status_msg,
                                    public_fast=_is_pub,
                                    only_topic=_st.get("only_topic"),
                                    dest_override=_set_dest)
                    return
                _gid, _gnew, _gtitle = _set_dest, False, str(_set_dest)
                _dest_note = " (settings chat ID, folderwise)"
            else:
                # v123: pehle se auto group bana ho to POOCHO — purana
                # use karun ya naya banaun? (naya source ke naam se banega)
                _old_gid, _old_gtitle = _auto_dest_existing(user_id, "group")
                if _old_gid:
                    USER_STATES[user_id] = {
                        "step": "WAITING_DEST_CHOICE", "kind": "group",
                        "old_id": _old_gid, "old_title": _old_gtitle,
                        "src": _src, "is_pub": _is_pub,
                        "rs": _rs, "re": _re, "fold": _fold,
                        "name": _st.get("name"),
                    }
                    await message.reply_text(
                        "📂 **Destination pehle se hai**\n\n"
                        f"Tumhara group `{_old_gtitle}` pehle se bana hua hai.\n\n"
                        "**Files kahan upload karun?**\n"
                        "• 📂 **Purana wala** — files usi group me jayengi\n"
                        f"• 🆕 **Naya banao** — `{(_st.get('name') or 'VILLAN')} • Saved` naam se naya group banega\n\n"
                        "Chuno 👇",
                        parse_mode=enums.ParseMode.MARKDOWN,
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("📂 Purana wala", callback_data="dest_old"),
                             InlineKeyboardButton("🆕 Naya banao", callback_data="dest_new")],
                        ]),
                    )
                    return
                _gid, _gnew, _gtitle = await _get_or_create_auto_group(
                    client, _ub3, user_id, _st.get("name"))
                if not _gid:
                    await message.reply_text("❌ Apna group nahi ban paya — dobara try karo.")
                    return
                _dest_note = (" (bot admin ✅, folderwise)"
                              + (" — naya banaya" if _gnew else " — wahi purana"))
            status_msg = await message.reply_text(
                "🔥 𝐏𝐑𝐎 𝐓𝐔𝐑𝐁𝐎 𝐓𝐑𝐀𝐍𝐒𝐅𝐄𝐑 𝐏𝐈𝐏𝐄𝐋𝐈𝐍𝐄 🔥\n"
                f"📂 {_fancy('Dest Group')}: `{_gtitle}`{_dest_note}"
            )
            await run_clone(client, user_id, _src, status_msg,
                            only_topic=_fold, range_start=_rs, range_end=_re,
                            dest_override=_gid, dest_title=_gtitle, mode_label="Extract")
            return
        if _is_all:
            _fc = client if _is_pub else await get_userbot(user_id)
            if _fc is None:
                await message.reply_text("❌ Pehle `/login` karo.")
                return
            _end = None
            try:
                async for _m in _fc.get_chat_history(_src, limit=1):
                    _end = _m.id
                    break
            except Exception:
                _end = None
            if not _end:
                await message.reply_text("❌ History nahi mili — source check karo, phir dobara Start Forwarding se karo.")
                return
        if _end is None:
            _end = _start
        if _start > _end:
            _start, _end = _end, _start
        total_files = _end - _start + 1
        # v108: range UNLIMITED — BATCH_LIMIT check hata diya gaya hai.
        _fr = _free_remaining(user_id)
        if _fr is not None and total_files > _fr:
            await message.reply_text(
                f"⛔ Free trial me sirf `{_fr}` messages bache hain, tumne `{total_files}` maange hain.\n"
                f"Range `{_fr}` tak rakho, ya premium lo.")
            return
        if _fr is not None:
            _free_consume(user_id, total_files)
        # v101 AUTO-DEST: channel source ho (forum group ya clone nahi) to
        # is user ka apna alag channel (ek ID = ek channel) — pehli baar
        # banega, bot auto-admin, dobara wahi reuse hoga. Batch wahi jayega.
        _dest_override = None
        _auto_line = ""
        if not _st.get("is_group") and _st.get("mode") != "clone":
            if _set_dest:
                # v112: settings chat_id set hai — wahi dest, auto channel
                # bilkul nahi banega. Bot ko us chat me dekhna chahiye,
                # warna wahan bhej nahi payega — pehle hi check kar lo.
                try:
                    await client.get_chat(_set_dest)
                except Exception:
                    await message.reply_text(
                        "❌ Settings me jo chat ID hai wo khul nahi raha — bot usme nahi hai ya ID galat hai.\n"
                        "/settings se sahi ID dalo, ya hata do (phir auto channel chalega).")
                    return
                _dest_override = _set_dest
                _auto_line = (f"\n📂 {_fancy('Dest')}: `{_set_dest}`"
                              " (settings chat ID)")
            else:
                _ub2 = await get_userbot(user_id)
                if _ub2 is not None:
                    # v123: pehle se auto channel bana ho to POOCHO — purana
                    # use karun ya naya banaun? (naya source ke naam se banega)
                    _old_cid, _old_ctitle = _auto_dest_existing(user_id, "channel")
                    if _old_cid:
                        USER_STATES[user_id] = {
                            "step": "WAITING_DEST_CHOICE", "kind": "channel",
                            "old_id": _old_cid, "old_title": _old_ctitle,
                            "src": _src, "is_pub": _is_pub,
                            "start": _start, "end": _end,
                            "total": total_files,
                            "only_topic": _st.get("only_topic"),
                            "name": _st.get("name"),
                        }
                        await message.reply_text(
                            "📂 **Destination pehle se hai**\n\n"
                            f"Tumhara channel `{_old_ctitle}` pehle se bana hua hai.\n\n"
                            "**Files kahan upload karun?**\n"
                            "• 📂 **Purana wala** — files usi channel me jayengi\n"
                            f"• 🆕 **Naya banao** — `{(_st.get('name') or 'VILLAN')} • Saved` naam se naya channel banega\n\n"
                            "Chuno 👇",
                            parse_mode=enums.ParseMode.MARKDOWN,
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("📂 Purana wala", callback_data="dest_old"),
                                 InlineKeyboardButton("🆕 Naya banao", callback_data="dest_new")],
                            ]),
                        )
                        return
                    _dest_override, _auto_new, _auto_title = await _get_or_create_auto_dest(
                        client, _ub2, user_id, _st.get("name"))
                    if _dest_override:
                        _auto_line = (f"\n📂 {_fancy('Dest Channel')}: `{_auto_title}`"
                                      f" (bot admin ✅, pin-copy ON)"
                                      + (" — naya banaya" if _auto_new else " — wahi purana"))
                else:
                    _auto_line = ("\nℹ️ Auto channel ke liye `/login` karo — "
                                  "is baar personal chat me bhej raha hun.")
        USER_STATES.pop(user_id, None)
        status_msg = await message.reply_text(
            f"🔥 𝐏𝐑𝐎 𝐓𝐔𝐑𝐁𝐎 𝐓𝐑𝐀𝐍𝐒𝐅𝐄𝐑 𝐏𝐈𝐏𝐄𝐋𝐈𝐍𝐄 🔥\nBatch started — `{total_files}` items.{_auto_line}"
        )
        await run_batch(client, user_id, _src, _start, _end, status_msg,
                        public_fast=_is_pub, only_topic=_st.get("only_topic"),
                        dest_override=_dest_override)
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
        # v108: range UNLIMITED — BATCH_LIMIT check hata diya gaya hai.
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
    "• /mkchannels — ab DP aur bio bhi poochhega, naam + DP + bio sab channels par ek saath lagega\n"
    "• /sendall aur /sendallrange — ab bot fallback bhi hai, login account fail ho to bot admin se bhejega (Post Messages ON hona chahiye)\n"
    "• /mkchannels — har naye channel me bot bhi admin banega (Post Messages ON), taaki 1 account se sab par broadcast ho sake; /organize aur /arrange hata diye gaye, folderwise kaam sirf /fbatch se hoga\n"
    "• /mkchannels bio step ka crash fix — bio bhejte hi jo UnboundLocalError aata tha aur flow atak jata tha (isliye DP kabhi lagti hi nahi thi), wo theek hua\n"
    "• /clone list topics fix — deleted topic milne par poori folder list fail nahi hogi, deleted topics skip ho jayenge\n"
    "• Premium / Normal system — /public aur channel commands sirf premium members ke liye, normal users /batch use kar sakte hain; /clone me download/upload progress dikhega\n"
    "• /mkchannels bot admin fix — ab bot ko pehle channel me add karke phir admin banaya jayega, taaki bot admin fail na ho\n"
    "• /paid aur /allow commands hata diye gaye — premium ke liye /premium, normal ke liye /normal use hoga\n"
    "• /ai (FREE Qwen), /free (ek baar, 10 messages trial), /makeadmin, batch limit 5000, sendallrange limit hata, clone me folder + range, default DP + bio, video format fix\n"
    "• /ai ab premium users bhi use kar sakte hain, normal users nahi\n"
    "• /mkchannels DP fail ab chhupta nahi — report me DP fail ki line aayegi\n"
    "• /mkchannels default DP/bio hata diye — jo DP/bio doge wahi lagegi, skip par koi default nahi\n"
    "• /mkchannels me default DP/bio hata diye — jo DP/bio doge wahi lagegi\n"
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
    # v107: wahi chhota message, par stylish — fancy heading + version +
    # latest highlights (bullets har version me update karne hain).
    text = (
        f"🔥 {_fancy('VILLAN UPDATED')} 🔥\n\n"
        f"✨ Bot update ho gaya hai — {VERSION} live hai!\n\n"
        f"🆕 {_fancy('Kya naya hai:')}\n"
        "• /batch — channel + group dono me auto-dest\n"
        "• /buy — seedha owner se baat, message ready\n"
        "• Speed — auto-dest par tez batch\n\n"
        "👇 /start dabao aur try karo!"
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
async def _telegram_watchdog():
    """v85: Telegram connection ka watchdog — do baar aaye DC loop ka fix.

    Loop me process zinda rehta hai aur health server 200 deta hai, isliye
    Koyeb use healthy samajh ke restart nahi karta, aur bot tab tak dead
    rehta hai jab tak haath se restart/redeploy na karo. Ye watchdog har
    60 sec me bot client se get_me maangta hai; lagatar 5 baar fail ho
    (lagbhag 5 min tak Telegram dead) to process khud exit kar deta hai —
    Koyeb crashed container ko apne aap dobara start karta hai, naya
    session banta hai, loop toot jata hai. Startup ke pehle 3 min me
    exit nahi karta (backup restore + broadcast ka waqt).
    Batch par asar: dead session me batch waise bhi aage nahi badhta,
    isliye 5 min dead rehne ke baad restart hi sahi raasta hai."""
    await asyncio.sleep(180)
    _fails = 0
    while True:
        await asyncio.sleep(60)
        try:
            await asyncio.wait_for(app.get_me(), timeout=20)
            _fails = 0
        except Exception as _e:
            _fails += 1
            print(f"[WATCHDOG] Telegram check fail {_fails}/5: {_e}", flush=True)
            if _fails >= 5:
                print("[WATCHDOG] Telegram 5 min se dead hai — process exit kar raha hun, Koyeb fresh restart dega", flush=True)
                os._exit(1)


async def main():
    await start_web_server()

    print("🚀 Starting Bot Client...", flush=True)
    await app.start()
    asyncio.create_task(_telegram_watchdog())

    me = await app.get_me()
    # v86: transfer fast path ke liye bot ka username cache karo — userbot
    # private source wali files is username par PM karta hai (koi bhi account
    # bot ko PM kar sakta hai, group membership ki zaroorat nahi padti).
    global BOT_USERNAME
    BOT_USERNAME = me.username
    try:
        await app.set_bot_commands([
            BotCommand("start", "Bot start karo"),
            BotCommand("login", "Apne number se login"),
            BotCommand("logout", "Session khatam karo"),
            BotCommand("create", "Naya group + folders banao"),
            BotCommand("mkchannels", "10 username se PUBLIC channels banao"),
            BotCommand("mychannels", "Tumhare channels ki list"),
            BotCommand("sendall", "Ek message ya range, sab channels me bhejo"),
            BotCommand("addchannel", "Pehle se bana channel jodo"),
            BotCommand("pin", "Group/channel me messages pin karo"),
            BotCommand("free", "Ek baar, 10 messages ka free trial"),
            BotCommand("makeadmin", "Saved channels me bot ko admin banao"),
            BotCommand("fbatch", "Folderwise batch, sequencewise copy"),
            BotCommand("resume", "Ruka hua batch wahi se dobara shuru karo"),
            BotCommand("setchannel", "Destination set karo"),
            BotCommand("settings", "Customize panel"),
            BotCommand("plan", "Apna plan dekho"),
            BotCommand("buy", "Premium kharido"),
            BotCommand("help", "Usage guide"),
            BotCommand("cancel", "Chal raha task roko"),
            BotCommand("check", "Ek message check karo — video dikhta hai ya nahi"),
        ])
        print("👉 Bot command menu set ho gaya", flush=True)
    except Exception as e:
        print(f"[MENU] set_bot_commands fail: {e}", flush=True)
    print("========================================", flush=True)
    print(f"👉 BOT LISTENING AS: @{me.username}", flush=True)
    print(f"👉 VERSION: {VERSION}", flush=True)
    # v88: TRANSFER BAND — user ne video wala direct upload wapas maanga
    # (instant nahi, uploading complete hote hi delivery). Isliye startup
    # me transfer ON ka dawa nahi, direct upload mode dikhega.
    print("👉 DIRECT UPLOAD MODE: ON (transfer OFF) — private files download+upload se, uploading complete hote hi delivery", flush=True)
    print("👉 FAIL LOGGING: ON — har failed/skipped msg ID log me reason ke saath aayega", flush=True)
    print("👉 MULTI: cached media session — ExportAuthorization sirf pehli baar per DC, har file par nahi", flush=True)
    print("👉 MODE: sequential delivery + PIPELINED download (N upload + N+1 download)", flush=True)
    print(f"👉 OWNER_ID: {OWNER_ID if OWNER_ID else 'not set (open mode)'}", flush=True)
    print(f"👉 PACING: FAST_GAP={FAST_GAP}s SLOW_GAP={SLOW_GAP}s PRIVATE_GAP={PRIVATE_GAP}s "
          f"FETCH_CHUNK={FETCH_CHUNK}", flush=True)
    print(f"👉 MULTI_DL: {MULTI_PARTS} parts/file (ek file ke parts parallel, order sequential)", flush=True)
    print("👉 NET-RESILIENCE: ON — ConnectionReset par part retry (2s/5s) + file retry (8s), session evict + backoff, transport log shor OFF", flush=True)
    print("👉 PIN-COPY: ON — source pinned message dest channel me bhi pin hoga (sendall/sendallrange/fbatch/clone; public+private)", flush=True)
    print("👉 PIN-COMMAND: ON — /pin se kisi group/channel me messages pin karo; /batch me chat_id channel ho to pinned copy hoga", flush=True)
    print("👉 STYLE: SRC — /start menu + batch pipeline SRC style me", flush=True)
    print("👉 UNIFIED-BATCH: ON — /batch hi sab karega (channel ID -> public/private/folder auto -> range)", flush=True)
    print("👉 AI: OFF — /ai hata diya gaya hai", flush=True)
    print("👉 V100: /batch me public+clone, /sendall me range, /makeadmin skip-admin, fancy style", flush=True)
    print("👉 V101: /public+/clone+/sendallrange HATA DIYE; channel batch -> apna alag channel (ek ID = ek channel), bot auto-admin, pin-copy", flush=True)
    print("👉 V102: SRC-style interface — /start status lines, Public/Private mode screen, /setchannel, Full Account Reset (confirm)", flush=True)
    print("👉 V103: AUTO-DEST sabke liye — channel->channel, group->group (folderwise); ek ID = ek channel + ek group, bot auto-admin", flush=True)
    print("👉 V104: BUGFIX — /help chunked send (MESSAGE_TOO_LONG fix), PUBLIC_STATES dict wapas (mode buttons + /batch crash fix)", flush=True)
    print("👉 V105: SPEED — auto-dest par 10s gap nahi (sirf personal chat par), clone/fbatch gap 2s -> 1s; Koyeb ~5 MB/s cap waisa hi", flush=True)
    print("👉 V106: /buy DIRECT — owner chat link, message pre-filled (bold)", flush=True)
    print("👉 V107: BROADCAST stylish — restart message fancy + version + highlights", flush=True)
    print("👉 V108: RANGE UNLIMITED (5000 cap hata) + dusri link se end ID + topic link start fix", flush=True)
    print("👉 V109: DEST PROGRESS — auto channel/group me bhi uploading + speed + File X/Y card", flush=True)
    print("👉 V110: PROGRESS CARD ARROW design (sab bold, ▶/▷ bar)", flush=True)
    print("👉 V111: AUTO-DEST RESTORE FIX — restart par purana group/channel reuse hoga", flush=True)
    print("👉 V112: DEST RULE — settings chat ID set ho to batch wahi, warna auto group/channel", flush=True)
    print("👉 V113: ANIMATED PROGRESS CARD — typewriter VILLAN + blinking dots + right ➤", flush=True)
    print("👉 V114: CARD FIX — cover lines chhoti, File line + status me Bacha count", flush=True)
    print("👉 V115: SMOOTH ANIMATION — typewriter/blink har edit par ek kadam, card 1s update", flush=True)
    print("👉 V116: CARD FIX — border max 30 (no wrap), clone DM me bhi progress card", flush=True)
    print("👉 V117: CARD CHHOTA — 5 lines hata di, sirf top border + header + progress", flush=True)
    print("👉 V118: SPEED FIX — card edits background me, upload kabhi wait nahi karega", flush=True)
    print("👉 V119: FOOTER WAPAS — 5 dots har side, ● bari bari ping-pong blink", flush=True)
    print("👉 V120: USER PASTED CARD — poora 14-line card, border 24, footer 5 dots + blink", flush=True)
    print("👉 V121: SPINNER HEADER — typewriter ki jagah ◐◑◒◓ ghumta hai VILLAN ke pehle", flush=True)
    print("👉 V122: FAIL FIX — khaali message par card nahi banega, fetch pehle", flush=True)
    print("👉 V123: /batch HATA — batch sirf Start Forwarding se; pehle se auto dest ho to purana/naya poochhega", flush=True)
    print("👉 V124: /check DIAGNOSTIC — ek message link do, bot batayega use kya dikhta hai (media/text/khaali)", flush=True)
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
