import io
import os
import re
import sys
import json
import time
import copy
import shutil
import struct
import asyncio
import logging
import tempfile
import contextvars
from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.errors import (
    FloodWaitError, SessionPasswordNeededError, PhoneCodeInvalidError,
    PhoneCodeExpiredError, PhoneNumberInvalidError, PasswordHashInvalidError,
)
from telethon.extensions import markdown as tl_markdown
from telethon.tl.functions.channels import CreateChannelRequest
from telethon.tl.functions.messages import ToggleNoForwardsRequest
from telethon.tl.functions.bots import SetBotCommandsRequest
from telethon.tl.types import (
    MessageMediaWebPage, MessageMediaPoll, Channel, PeerChannel,
    BotCommand, BotCommandScopeDefault,
    DocumentAttributeFilename, DocumentAttributeVideo,
    PhotoSize, PhotoSizeProgressive, InputDocumentFileLocation,
)

try:
    # parallel multi-connection transfer (much faster up/downloads for large files)
    from FastTelethon import upload_file as fast_upload, download_file as fast_download
except Exception:
    fast_upload = None
    fast_download = None

VERSION = "v10"


def need(name):
    v = os.environ.get(name, "").strip()
    if not v:
        print(f"ERROR: environment variable '{name}' is missing. Add it in your hosting Variables/Secrets.")
        sys.exit(1)
    return v


try:
    api_id = int(need("API_ID"))
except ValueError:
    print("ERROR: API_ID must be a number (get it from my.telegram.org).")
    sys.exit(1)
api_hash = need("API_HASH")
bot_token = need("BOT_TOKEN")

RESTRICT = os.environ.get("RESTRICT", "1") == "1"            # channels created by /create are restricted
MAX_COUNT = int(os.environ.get("MAX_COUNT", "50"))           # max channels per /create
CREATE_DELAY = float(os.environ.get("CREATE_DELAY", "3"))    # wait between created channels
DELAY = float(os.environ.get("DELAY", "1.5"))                # wait between copied messages
EDIT_EVERY = float(os.environ.get("EDIT_EVERY", "4"))        # seconds between live status updates
PROGRESS_FILE = os.environ.get("PROGRESS_FILE", "progress.json")
SETTINGS_FILE = os.environ.get("SETTINGS_FILE", "settings.json")
SESSIONS_FILE = os.environ.get("SESSIONS_FILE", "sessions.json")
THUMB_DIR = os.environ.get("THUMB_DIR", "thumbs")
# Download temp files to the app's own (large, persistent) storage, NOT the system /tmp,
# which is tiny on many hosts and causes "No space left on device" for big videos.
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", os.path.join(os.getcwd(), "dl_tmp"))
try:
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
except Exception:
    DOWNLOAD_DIR = None  # fall back to the system temp dir if we can't create it
FAST_MIN = int(os.environ.get("FAST_UPLOAD_MB", "5")) * 1024 * 1024  # use parallel transfer above this size
# Parallel multi-connection transfer. Greatly speeds up uploads (single-connection uploads are slow).
# ON by default; set USE_FAST=0 to disable. Harmless connection noise is suppressed below.
USE_FAST = os.environ.get("USE_FAST", "1") == "1"
# PIPELINE=1: download the next file while uploading the current one (uses both directions, ~2x).
# PIPELINE=0 (default): strictly one at a time (download, then upload). Fewer simultaneous
# connections = more stable. Connection warnings are silenced either way.
PIPELINE = os.environ.get("PIPELINE", "0") == "1"
# Parallel DOWNLOAD via FastTelethon (on by default). With sequential mode (PIPELINE=0) only one
# transfer runs at a time, so the connection count stays low and stays stable.
USE_FAST_DL = os.environ.get("USE_FAST_DL", "1") == "1"


# ---- quiet the harmless asyncio/connection noise from parallel transfer senders ----
_orig_unraisablehook = sys.unraisablehook


def _quiet_unraisablehook(unraisable):
    exc = getattr(unraisable, "exc_value", None)
    if isinstance(exc, GeneratorExit):
        return
    if isinstance(exc, RuntimeError) and "GeneratorExit" in str(exc):
        return
    _orig_unraisablehook(unraisable)


sys.unraisablehook = _quiet_unraisablehook

# `bot` = the BotFather bot everyone chats with (receives commands, shows progress).
# Each user logs in with /login; their own Telegram account then does the real work.
bot = TelegramClient(StringSession(), api_id, api_hash)
bot.parse_mode = None  # bot messages are plain text


class BadInput(Exception):
    pass


# ----------------------------- per-user session -----------------------------
class UserSession:
    """Everything that belongs to one logged-in (or logging-in) user."""

    def __init__(self, uid):
        self.user_id = uid
        self.name = "there"
        self.client = None          # the user's own Telegram account client
        self.login_client = None     # temporary client used during /login
        self.dialogs_loaded = False
        self.chan_cache = {}        # {"all"/"admin": (timestamp, [(id, title), ...])}
        self.started_at = time.time()
        self.conv = {}
        self.job = {}
        self.reset_conv()
        self.reset_job()

    def reset_conv(self):
        self.conv.clear()
        self.conv.update(
            kind=None, step=None,
            src=None, src_id=None, src_title="", dst=None, dst_id=None, dst_title="",
            start=None, start_label="",
            rules=[], scope="both", link_rules=[],
            thumb=None, footer_text=None, footer_pos="append",
            name="", count=0,
            phone=None, phone_code_hash=None,
            pick_src=[], pick_src_all=[], pick_src_page=0,
            pick_dst=[], pick_dst_all=[], pick_dst_page=0,
        )

    def reset_job(self):
        self.job.clear()
        self.job.update(
            running=False, stop=False, kind=None, phase="idle", last_edit=0.0,
            started=0.0, src="", dst="", done=0, failed=0, skipped=0, bytes_done=0,
            cur_id=0, min_id=0, top_id=0, errors=[], flood_until=0.0,
            rules_n=0, scope="both", link_n=0, has_thumb=False, has_footer=False,
            name="", total=0, total_msgs=0, key="", last_result="", delay=DELAY,
            force_dl=False,
            dl_cur=0, dl_tot=0, dl_speed=0.0, dl_no=0,
            ul_cur=0, ul_tot=0, ul_speed=0.0, ul_no=0,
        )


sessions = {}
_cur = contextvars.ContextVar("current_session")


def S():
    """The session for the user currently being served."""
    return _cur.get()


def get_sess(uid):
    s = sessions.get(uid)
    if s is None:
        s = UserSession(uid)
        sessions[uid] = s
    return s


# ----------------------------- persistence -----------------------------
def jload(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def jsave(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception:
        pass


session_strings = jload(SESSIONS_FILE)   # {str(user_id): session_string}


def save_sessions():
    jsave(SESSIONS_FILE, session_strings)


def save_progress(key, last_id):
    data = jload(PROGRESS_FILE)
    data.setdefault(str(S().user_id), {})[key] = last_id
    jsave(PROGRESS_FILE, data)


def progress_map():
    return jload(PROGRESS_FILE).get(str(S().user_id), {})


def get_setting(name, default=None):
    return jload(SETTINGS_FILE).get(str(S().user_id), {}).get(name, default)


def set_setting(name, value):
    data = jload(SETTINGS_FILE)
    data.setdefault(str(S().user_id), {})[name] = value
    jsave(SETTINGS_FILE, data)


def get_delay():
    """Per-user delay (seconds) between copied messages; defaults to the DELAY env var."""
    try:
        v = float(get_setting("delay", DELAY))
    except Exception:
        v = DELAY
    return min(10.0, max(0.3, v))


# ----------------------------- small helpers -----------------------------
def fmt_duration(sec):
    sec = int(max(sec, 0))
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def fmt_size(n):
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024


def title_of(ent):
    return getattr(ent, "title", None) or getattr(ent, "first_name", None) or str(getattr(ent, "id", "?"))


def bar(pct, width=12):
    pct = min(100.0, max(0.0, pct))
    filled = int(round(pct / 100 * width))
    return "█" * filled + "░" * (width - filled)


async def say(text, **kw):
    return await bot.send_message(S().user_id, text, link_preview=False, **kw)


# ----------------------------- user client lifecycle -----------------------------
async def ensure_client(sess):
    """Returns a connected, authorized client for the user, or None if not logged in."""
    if sess.client is None:
        ss = session_strings.get(str(sess.user_id))
        if not ss:
            return None
        sess.client = TelegramClient(StringSession(ss), api_id, api_hash)
    try:
        if not sess.client.is_connected():
            await sess.client.connect()
        if not await sess.client.is_user_authorized():
            sess.client = None
            session_strings.pop(str(sess.user_id), None)
            save_sessions()
            return None
    except Exception:
        return None
    return sess.client


async def require_login(sess):
    c = await ensure_client(sess)
    if not c:
        await say("🔒 You need to log in first.\nSend /login to connect your Telegram account.")
        return False
    if sess.name == "there":
        try:
            me = await c.get_me()
            sess.name = me.first_name or "there"
        except Exception:
            pass
    return True


# ----------------------------- texts -----------------------------
WELCOME = (
    "👋 Welcome, {name}!\n\n"
    "I'm your Channel Tools Assistant. After you log in with your own Telegram account, I can:\n"
    "• Copy all content (videos, photos, files, text) from one channel to another, with extras: "
    "replace words, replace hidden hyperlink URLs, set a custom thumbnail, and add a caption / promo link.\n"
    "• Create several private channels with the same name in one go.\n\n"
    "First send /login to connect your account. Then send /copy or /create.\n"
    "Send /help for the full guide."
)

HELP = (
    "🤖 Channel Tools Assistant — Help\n\n"
    "Commands\n"
    "/login - Connect your Telegram account to the bot\n"
    "/logout - Disconnect your account\n"
    "/copy - Copy content from one channel to another (guided)\n"
    "/create - Create multiple private channels (guided)\n"
    "/status - Live progress (with speed) of your current task\n"
    "/speed - Set the delay between messages (e.g. /speed 0.8)\n"
    "/errors - Detailed list of recent errors\n"
    "/stop - Stop your current task (or discard a draft)\n"
    "/ping - Check that I'm online\n"
    "/about - About this assistant\n"
    "/help - Show this message\n\n"
    "Logging in\n"
    "1. Send /login.\n"
    "2. Send your phone number in international format (e.g. +919876543210).\n"
    "3. Enter the login code. Type it with spaces or dashes between the digits "
    "(e.g. 1 2 3 4 5) so Telegram doesn't block it.\n"
    "4. If you use two-step verification, enter your password.\n\n"
    "Copying content\n"
    "1. Send /copy.\n"
    "2. Pick the source channel from the buttons (or send an ID/@username, or forward a message).\n"
    "3. Pick the destination the same way (your admin channels are listed).\n"
    "4. Choose where to start: the beginning, a message ID, or resume.\n"
    "5. Open the Extras menu (all optional), then tap Done and Start.\n"
    "Shortcut: /copy <source> <destination> [message id]\n\n"
    "Extras (optional)\n"
    "• Replace words — in captions/text and/or file names. One rule per line: old => new. "
    "Leave the right side empty to remove a word. Case-insensitive.\n"
    "• Replace links — rewrite the hidden URL inside hyperlinks. One rule per line: "
    "old => new (e.g. t.me/oldchannel => t.me/newchannel).\n"
    "• Thumbnail — send a photo; it's used as the thumbnail for copied videos/documents "
    "(those files are re-uploaded, so they take longer).\n"
    "• Caption / promo link — extra text added to every caption. Use markdown for a "
    "clickable promo link: [Join here](https://t.me/yourchannel). Choose append or prepend.\n\n"
    "Creating channels\n"
    "Send /create, then the channel name and the number of channels. "
    "Shortcut: /create <name> <number>\n\n"
    "Channel IDs\n"
    "Private channel IDs start with -100 (for example -1001234567890). "
    "You can also forward any message from the channel to me.\n\n"
    "Good to know\n"
    "• Your account must be a member of the source and an admin of the destination.\n"
    "• Only one task runs at a time per user.\n"
    "• A stopped or failed copy can be resumed from where it left off.\n"
    "• Speed: use /speed to lower the delay (e.g. 0.8s). If Telegram pauses you "
    "(flood wait), /status shows a live countdown and the copy resumes automatically "
    "from the same message when the wait ends.\n"
    "• Protected/private sources: the bot detects these and goes straight to "
    "download-and-upload mode (no wasted attempt). /status shows live download/upload "
    "percentage and speed for the current file."
)

ABOUT = (
    f"ℹ️ Channel Tools Assistant {VERSION}\n\n"
    "Log in with your own Telegram account, then copy channel content with extras "
    "(replace words, replace hyperlink URLs, custom thumbnail, caption/promo link) "
    "and create multiple channels. Use /help for instructions."
)

GREETING_RE = re.compile(r"^(hi+|hello+|hey+|hola|namaste|good\s+(morning|afternoon|evening))\b", re.I)
THANKS_RE = re.compile(r"^(thanks|thank\s+you|thx|ty)\b", re.I)


# ----------------------------- channel references -----------------------------
def check_ref(token, label):
    t = token.strip()
    if re.fullmatch(r"-100\d{5,}", t):
        return int(t)
    if re.fullmatch(r"\d{5,}", t):
        raise BadInput(f"{label}: the ID '{t}' is missing the -100 prefix. Please send it as -100{t}.")
    if re.fullmatch(r"-\d+", t):
        raise BadInput(f"{label}: the ID '{t}' is not valid. Channel IDs start with -100, for example -100{t.lstrip('-')}.")
    if re.fullmatch(r"@[A-Za-z][A-Za-z0-9_]{3,31}", t):
        return t
    m = re.fullmatch(r"(?:https?://)?t\.me/([A-Za-z][A-Za-z0-9_]{3,31})/?", t)
    if m:
        return "@" + m.group(1)
    if "t.me/+" in t or "t.me/joinchat" in t or "t.me/c/" in t:
        raise BadInput(f"{label}: invite or message links can't be used here. Please send the numeric channel ID (-100...) or the @username.")
    raise BadInput(f"{label}: I couldn't understand '{t}'. Please send an @username or an ID that starts with -100.")


async def resolve(ref, label):
    sess = S()
    if not sess.dialogs_loaded:
        await sess.client.get_dialogs()
        sess.dialogs_loaded = True
    try:
        return await sess.client.get_entity(ref)
    except Exception as e:
        raise BadInput(
            f"{label} could not be found ({ref}).\n"
            "Please check that the ID/username is correct and that your account is a member of that channel.\n"
            f"Details: {e}"
        )


def fwd_channel_id(msg):
    """For a message forwarded from a channel, returns the channel ID in -100... form, else None."""
    f = getattr(msg, "fwd_from", None)
    pid = getattr(f, "from_id", None) if f else None
    if isinstance(pid, PeerChannel):
        return int("-100" + str(pid.channel_id))
    return None


# ----------------------------- text / entity utilities -----------------------------
def add_surrogate(text):
    """Python str -> UTF-16 units (Telegram entity offsets are UTF-16 based)."""
    return "".join(
        "".join(chr(u) for u in struct.unpack("<HH", ch.encode("utf-16le")))
        if 0x10000 <= ord(ch) <= 0x10FFFF else ch
        for ch in text
    )


def del_surrogate(text):
    return text.encode("utf-16", "surrogatepass").decode("utf-16")


def utf16_len(text):
    return len(add_surrogate(text or ""))


class Rules:
    """Compiled word-replacement rules (case-insensitive, longest match first)."""

    def __init__(self, pairs):
        self.pairs = [(o, n) for o, n in pairs]
        self.d_p = {o.lower(): n for o, n in self.pairs}
        self.pat_p = re.compile("|".join(re.escape(k) for k in sorted(self.d_p, key=len, reverse=True)), re.I)
        self.d_s = {add_surrogate(o).lower(): add_surrogate(n) for o, n in self.pairs}
        self.pat_s = re.compile("|".join(re.escape(k) for k in sorted(self.d_s, key=len, reverse=True)), re.I)

    def sub_plain(self, text):
        if not text:
            return text
        return self.pat_p.sub(lambda m: self.d_p.get(m.group(0).lower(), m.group(0)), text)


def parse_rules(text):
    """'old => new' per line. Returns (pairs, bad_lines)."""
    pairs, bad = [], []
    for ln in (text or "").splitlines():
        ln = ln.strip()
        if not ln:
            continue
        parts = re.split(r"\s*(?:=>|->|→)\s*", ln, maxsplit=1)
        if len(parts) != 2 or not parts[0].strip():
            bad.append(ln)
            continue
        pairs.append((parts[0].strip(), parts[1].strip()))
    return pairs, bad


def replace_in_text(raw_text, entities, rules):
    """Replaces words in a message text and keeps bold/links/etc. aligned.
    Returns (new_text, new_entities)."""
    ents = list(entities or [])
    if not rules or not raw_text:
        return raw_text or "", ents
    raw = add_surrogate(raw_text)
    repl = []
    for m in rules.pat_s.finditer(raw):
        new = rules.d_s.get(m.group(0).lower())
        if new is None or new == m.group(0):
            continue
        repl.append((m.start(), m.end(), new))
    if not repl:
        return raw_text, ents

    out, pos = [], 0
    for st, en, nt in repl:
        out.append(raw[pos:st])
        out.append(nt)
        pos = en
    out.append(raw[pos:])
    new_raw = "".join(out)

    def mp(x, is_end):
        shift = 0
        for st, en, nt in repl:
            if en <= x:
                shift += len(nt) - (en - st)
            elif st < x < en:
                return st + shift + (len(nt) if is_end else 0)
            else:
                break
        return x + shift

    new_ents = []
    for e in ents:
        ns, ne = mp(e.offset, False), mp(e.offset + e.length, True)
        if ne - ns <= 0:
            continue
        c = copy.copy(e)
        c.offset, c.length = ns, ne - ns
        new_ents.append(c)
    return del_surrogate(new_raw), new_ents


def apply_link_rules(entities, link_rules):
    """Rewrites the hidden URL inside hyperlink entities (e.g. [text](url))."""
    if not link_rules:
        return entities
    out = []
    for e in entities or []:
        c = copy.copy(e)
        url = getattr(c, "url", None)
        if url:
            c.url = link_rules.sub_plain(url)
        out.append(c)
    return out


def apply_footer(raw_text, entities, footer):
    """Adds an extra caption / promo text (with its own entities) to a message."""
    if not footer:
        return raw_text, list(entities or [])
    ftext, fents, pos = footer
    if not ftext:
        return raw_text, list(entities or [])
    text = raw_text or ""
    ents = [copy.copy(e) for e in (entities or [])]
    fe = [copy.copy(e) for e in (fents or [])]
    sep = "\n\n" if text.strip() else ""
    if pos == "prepend":
        new_text = ftext + sep + text
        shift = utf16_len(ftext + sep)
        for e in ents:
            e.offset += shift
        return new_text, fe + ents
    new_text = text + sep + ftext
    shift = utf16_len(text + sep)
    for e in fe:
        e.offset += shift
    return new_text, ents + fe


def rename_file(name, rules):
    """Applies the rules to a file name (extension is never changed)."""
    stem, ext = os.path.splitext(name)
    new = rules.sub_plain(stem)
    new = re.sub(r"[\\/\x00]", "_", new)
    new = re.sub(r"\s{2,}", " ", new).strip()
    if not new:
        new = "file"
    return new + ext


def normalize_thumb(src_path, dst_path):
    """Makes a Telegram-friendly jpg thumbnail (<=320px). Falls back to a plain copy."""
    try:
        from PIL import Image
        with Image.open(src_path) as im:
            im = im.convert("RGB")
            im.thumbnail((320, 320))
            im.save(dst_path, "JPEG", quality=70)
        return True
    except Exception:
        try:
            shutil.copyfile(src_path, dst_path)
            return True
        except Exception:
            return False


# ----------------------------- errors -----------------------------
def record_error(where, e):
    job = S().job
    name = e.__class__.__name__
    code = getattr(e, "code", None)
    detail = (str(e) or name).replace("\n", " ")
    job["errors"].append({"where": where, "name": name, "code": code, "detail": detail[:200]})
    if len(job["errors"]) > 500:
        del job["errors"][:100]


def err_lines(limit=5):
    job = S().job
    agg = {}
    for e in job["errors"]:
        k = (e["name"], e["code"])
        a = agg.setdefault(k, {"n": 0, "where": e["where"], "detail": e["detail"]})
        a["n"] += 1
        a["where"], a["detail"] = e["where"], e["detail"]
    lines = []
    for (name, code), a in sorted(agg.items(), key=lambda kv: -kv[1]["n"])[:limit]:
        c = f" (code {code})" if code else ""
        lines.append(f"• {name}{c} × {a['n']} — last at {a['where']}\n   {a['detail'][:90]}")
    return lines


# ----------------------------- status rendering -----------------------------
def render_job():
    job = S().job
    kind, phase = job["kind"], job["phase"]
    if kind is None:
        return "✅ No task is running. Send /copy or /create to start one."
    names = {"copy": "Copy", "create": "Channel creation"}
    head = {"running": f"⚙️ {names[kind]} in progress",
            "done": f"✅ {names[kind]} completed",
            "stopped": f"⏹ {names[kind]} stopped",
            "error": f"❌ {names[kind]} stopped because of an error"}[phase]
    elapsed = time.time() - job["started"]
    lines = [head]

    if kind == "copy":
        processed = job["done"] + job["skipped"] + job["failed"]
        total_c = job.get("total_msgs") or 0
        if total_c:
            pct = 100.0 if phase == "done" else min(100.0, processed / total_c * 100)
        else:
            span = max(job["top_id"] - job["min_id"], 1)
            pct = 100.0 if phase == "done" else min(100.0, max(0.0, (job["cur_id"] - job["min_id"]) / span * 100))
        lines.append(f"{job['src']} → {job['dst']}\n")
        lines.append(f"{bar(pct)} {pct:.0f}%")
        if total_c:
            lines.append(f"Content: {processed} / {total_c}")
        else:
            lines.append(f"Message ID: {job['cur_id']} / {job['top_id']}")
        lines.append(f"Copied: {job['done']} | Skipped: {job['skipped']} | Failed: {job['failed']}")
        # live transfer progress — download (of the next file) and upload (of the current) run together
        tc = f"/{total_c}" if total_c else ""
        if job["ul_tot"]:
            p = job["ul_cur"] / job["ul_tot"] * 100
            sp = f" • {fmt_size(job['ul_speed'])}/s" if job["ul_speed"] > 0 else ""
            lines.append(f"⬆️ Uploading #{job['ul_no']}{tc}: {p:.1f}% "
                         f"({fmt_size(job['ul_cur'])} / {fmt_size(job['ul_tot'])}){sp}")
        if job["dl_tot"]:
            p = job["dl_cur"] / job["dl_tot"] * 100
            sp = f" • {fmt_size(job['dl_speed'])}/s" if job["dl_speed"] > 0 else ""
            lines.append(f"⬇️ Downloading #{job['dl_no']}{tc}: {p:.1f}% "
                         f"({fmt_size(job['dl_cur'])} / {fmt_size(job['dl_tot'])}){sp}")
        if job["force_dl"]:
            # download/upload mode: the per-file speed above is the real one; show total moved
            if job["bytes_done"]:
                lines.append(f"Total moved: {fmt_size(job['bytes_done'])}")
        elif elapsed > 0:
            # server-side copy: message throughput
            lines.append(f"Speed: {job['done'] / (elapsed / 60):.0f} msg/min")
        eta = ""
        if phase == "running" and pct >= 2 and elapsed > 20:
            eta = f" | ETA: ~{fmt_duration(elapsed * (100 - pct) / pct)}"
        lines.append(f"Elapsed: {fmt_duration(elapsed)}{eta}")
        extras = []
        if job["rules_n"]:
            scope = {"both": "captions & file names", "text": "captions only", "names": "file names only"}[job["scope"]]
            extras.append(f"word rules: {job['rules_n']} ({scope})")
        if job["link_n"]:
            extras.append(f"link rules: {job['link_n']}")
        if job["has_thumb"]:
            extras.append("custom thumbnail")
        if job["has_footer"]:
            extras.append("caption/promo")
        if extras:
            lines.append("Extras: " + ", ".join(extras))
    else:
        total = max(job["total"], 1)
        pct = 100.0 if phase == "done" else job["done"] / total * 100
        lines.append(f"Name: {job['name']}\n")
        lines.append(f"{bar(pct)} {pct:.0f}%")
        lines.append(f"Created: {job['done']} / {job['total']}")
        lines.append(f"Elapsed: {fmt_duration(elapsed)}")

    if phase == "running" and job["flood_until"] > time.time():
        remaining = int(job["flood_until"] - time.time()) + 1
        lines.append(f"⏳ Paused by Telegram (flood wait): resuming in {remaining}s…")
        if kind == "copy":
            lines.append(f"   Will continue right after message ID {job['cur_id']}.")

    if job["errors"]:
        lines.append(f"\n⚠️ Errors: {len(job['errors'])}")
        lines.extend(err_lines())
        lines.append("Send /errors for the detailed list.")
    if kind == "copy" and phase in ("stopped", "error"):
        lines.append("\nRun /copy with the same source and destination and choose 'Resume' to continue.")
    if phase == "running":
        lines.append("\n/status for the latest progress • /stop to stop")
    return "\n".join(lines)


async def refresh(status, force=False):
    job = S().job
    now = time.time()
    if not force and now - job["last_edit"] < EDIT_EVERY:
        return
    job["last_edit"] = now
    try:
        await status.edit(render_job(), parse_mode=None)
    except Exception:
        pass


# ----------------------------- copy engine -----------------------------
def ent_kw(ents):
    return {"formatting_entities": ents} if ents else {"parse_mode": None}


def is_doc(m):
    return bool(getattr(m, "document", None))


def is_video(m):
    doc = getattr(m, "document", None)
    return bool(doc) and any(isinstance(a, DocumentAttributeVideo) for a in doc.attributes)


def doc_attrs(m, path):
    """Original document attributes, with the filename set to the (possibly renamed) file."""
    doc = getattr(m, "document", None)
    attrs = []
    for a in (doc.attributes if doc else []):
        if isinstance(a, DocumentAttributeFilename):
            continue
        if isinstance(a, DocumentAttributeVideo):
            a = copy.copy(a)
            a.supports_streaming = True  # keep the video playable while it loads
        attrs.append(a)
    attrs.append(DocumentAttributeFilename(os.path.basename(path)))
    return attrs, getattr(doc, "mime_type", None)


def _best_thumb_size(doc):
    """Largest real JPEG thumbnail of a document (PhotoSize or PhotoSizeProgressive)."""
    best, best_px = None, -1
    for t in (getattr(doc, "thumbs", None) or []):
        if isinstance(t, (PhotoSize, PhotoSizeProgressive)):
            px = (getattr(t, "w", 0) or 0) * (getattr(t, "h", 0) or 0)
            if px > best_px:
                best, best_px = t, px
    return best


def _save_thumb_bytes(data, dst):
    """Writes thumbnail bytes as a Telegram-friendly JPEG (<=320px, small)."""
    try:
        from PIL import Image
        with Image.open(io.BytesIO(data)) as im:
            im = im.convert("RGB")
            im.thumbnail((320, 320))
            im.save(dst, "JPEG", quality=85)
        return True
    except Exception:
        try:
            with open(dst, "wb") as f:
                f.write(data)
            return True
        except Exception:
            return False


async def _thumb_from_video(path, dst):
    """Grabs a frame from the video with ffmpeg (only if ffmpeg is installed on the host)."""
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg or not path or not os.path.exists(path):
        return False
    try:
        proc = await asyncio.create_subprocess_exec(
            ffmpeg, "-y", "-loglevel", "error", "-ss", "5", "-i", path,
            "-frames:v", "1", "-vf", "scale=320:-2", dst,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        await asyncio.wait_for(proc.wait(), timeout=60)
        return os.path.exists(dst) and os.path.getsize(dst) > 0
    except Exception:
        return False


async def try_src_thumb(client, m, video_path=None):
    """Gets a thumbnail for a re-uploaded video/document so it keeps its preview.
    Downloads the source thumbnail directly (Telethon's own helper crashes on the
    PhotoSizeProgressive format Telegram now uses), retries once with a fresh file
    reference, and falls back to a frame from the video if ffmpeg is available."""
    doc = getattr(m, "document", None)
    if not doc:
        return None
    dst = tempfile.mktemp(suffix=".jpg", dir=DOWNLOAD_DIR)
    reason = "source has no thumbnail"
    for attempt in range(2):
        size = _best_thumb_size(doc)
        if size is None:
            break
        try:
            loc = InputDocumentFileLocation(id=doc.id, access_hash=doc.access_hash,
                                            file_reference=doc.file_reference, thumb_size=size.type)
            data = await client.download_file(loc, bytes, dc_id=doc.dc_id)
            if data and _save_thumb_bytes(data, dst):
                return dst
            reason = "empty thumbnail"
        except FloodWaitError:
            raise
        except Exception as e:
            reason = f"{type(e).__name__}: {e}"
            if attempt == 0:
                # the file reference may have expired during a long download: refetch the message
                try:
                    fresh = await client.get_messages(m.peer_id, ids=m.id)
                    if fresh and getattr(fresh, "document", None):
                        doc = fresh.document
                        continue
                except Exception:
                    pass
            break
    if await _thumb_from_video(video_path, dst):
        return dst
    print(f"Thumbnail not copied for message {m.id}: {reason}")
    return None


def xfer_cb(prefix):
    """Progress callback that updates job['dl_*'] or job['ul_*'] with live bytes and speed."""
    job = S().job
    st = {"t": time.time(), "b": 0}

    def cb(cur, tot):
        now = time.time()
        job[prefix + "_cur"] = cur
        job[prefix + "_tot"] = tot or 0
        dt = now - st["t"]
        if dt >= 0.5:
            job[prefix + "_speed"] = max(0.0, cur - st["b"]) / dt
            st["t"] = now
            st["b"] = cur
    return cb


def reset_dir(prefix):
    job = S().job
    job[prefix + "_cur"] = 0
    job[prefix + "_tot"] = 0
    job[prefix + "_speed"] = 0.0


async def wait_flood(e):
    job = S().job
    job["flood_until"] = time.time() + e.seconds + 1
    await asyncio.sleep(e.seconds + 1)
    job["flood_until"] = 0.0


def make_captioner(opts):
    rules, scope = opts["rules"], opts["scope"]
    link_rules, footer = opts["link_rules"], opts["footer"]
    do_text = bool(rules) and scope in ("both", "text")

    def caption_of(m, add_foot):
        raw = m.raw_text or ""
        ents = list(m.entities or [])
        if do_text:
            raw, ents = replace_in_text(raw, ents, rules)
        if link_rules:
            ents = apply_link_rules(ents, link_rules)
        if add_foot:
            raw, ents = apply_footer(raw, ents, footer)
        return raw, ents
    return caption_of


async def download_one(client, m, tmp, idx, names, dcb):
    sub = os.path.join(tmp, str(idx))
    os.makedirs(sub, exist_ok=True)
    size = getattr(getattr(m, "file", None), "size", None) or 0
    # show the download is being attempted even before the first bytes arrive
    job = S().job
    job["dl_tot"] = size
    job["dl_cur"] = 0
    if USE_FAST_DL and fast_download is not None and is_doc(m) and size >= FAST_MIN:
        try:
            fname = names.get(m.id) or (getattr(m.file, "name", None) or f"file_{m.id}")
            path = os.path.join(sub, fname)
            with open(path, "wb") as out:
                await fast_download(client, m.document, out, progress_callback=dcb)
            return path
        except FloodWaitError:
            raise
        except Exception:
            pass  # fall back to the normal download
    if m.id in names:
        return await m.download_media(file=os.path.join(sub, names[m.id]), progress_callback=dcb)
    return await m.download_media(file=sub + os.sep, progress_callback=dcb)


async def prepare_group(group, opts):
    """Downloads whatever must be re-uploaded (nothing for a direct server-side copy).
    Runs ahead of sending so the next file downloads while the current one uploads."""
    client = S().client
    job = S().job
    rules, scope, thumb = opts["rules"], opts["scope"], opts["thumb"]
    do_names = bool(rules) and scope in ("both", "names")
    caption_of = make_captioner(opts)
    media_msgs = [m for m in group
                  if m.media and not isinstance(m.media, (MessageMediaWebPage, MessageMediaPoll))]

    if not media_msgs:
        return {"media_msgs": [], "caps": [caption_of(group[0], True)], "files": None,
                "names": {}, "reupload": set(), "tmp": None, "text_only": True}

    caps = [caption_of(m, i == 0) for i, m in enumerate(media_msgs)]
    names = {}
    if do_names:
        for m in media_msgs:
            old = getattr(m.file, "name", None) if m.file else None
            if old:
                new = rename_file(old, rules)
                if new != old:
                    names[m.id] = new
    if job.get("force_dl"):
        reupload = {m.id for m in media_msgs}
    else:
        reupload = {m.id for m in media_msgs if m.id in names or (thumb and is_doc(m))}

    tmp = None
    if reupload:
        dcb = xfer_cb("dl")
        try:
            while True:
                try:
                    tmp = tmp or tempfile.mkdtemp(prefix="cp_", dir=DOWNLOAD_DIR)
                    files = []
                    for i, m in enumerate(media_msgs):
                        files.append(await download_one(client, m, tmp, i, names, dcb)
                                     if m.id in reupload else m.media)
                    break
                except FloodWaitError as e:
                    await wait_flood(e)
        except BaseException:
            # a failed/cancelled download must not leave a partial file behind on disk
            if tmp:
                shutil.rmtree(tmp, ignore_errors=True)
            raise
        finally:
            reset_dir("dl")
    else:
        files = [m.media for m in media_msgs]
    return {"media_msgs": media_msgs, "caps": caps, "files": files,
            "names": names, "reupload": reupload, "tmp": tmp, "text_only": False}


async def do_upload(client, dst, prepared, opts):
    media_msgs, caps, files = prepared["media_msgs"], prepared["caps"], prepared["files"]
    thumb = opts["thumb"]
    ucb = xfer_cb("ul")
    if len(files) == 1:
        raw, ents = caps[0]
        m0, f0 = media_msgs[0], files[0]
        own_thumb = None
        thumb_file = None
        if is_doc(m0) and isinstance(f0, str):
            if thumb:
                thumb_file = thumb
            else:
                own_thumb = await try_src_thumb(client, m0, f0)
                thumb_file = own_thumb
        thumb_kw = {"thumb": thumb_file} if thumb_file else {}
        try:
            if (USE_FAST and fast_upload is not None and isinstance(f0, str)
                    and is_doc(m0) and os.path.getsize(f0) >= FAST_MIN):
                try:
                    with open(f0, "rb") as fh:
                        input_file = await fast_upload(client, fh, progress_callback=ucb)
                    attrs, mime = doc_attrs(m0, f0)
                    await client.send_file(dst, input_file, caption=raw, attributes=attrs,
                                           mime_type=mime, **ent_kw(ents), **thumb_kw)
                    return
                except FloodWaitError:
                    raise
                except Exception:
                    pass  # fall back to the normal single-connection upload
            extra = {"supports_streaming": True} if isinstance(f0, str) else {}
            await client.send_file(dst, f0, caption=raw, progress_callback=ucb,
                                   **ent_kw(ents), **thumb_kw, **extra)
        finally:
            if own_thumb and os.path.exists(own_thumb):
                try:
                    os.remove(own_thumb)
                except Exception:
                    pass
    else:
        streaming = any(isinstance(f, str) for f in files)
        extra = {"supports_streaming": True} if streaming else {}
        any_ents = any(e for _, e in caps)
        if any_ents:
            cap_list = [tl_markdown.unparse(r, e) if e else r for r, e in caps]
            kw = {}
        else:
            cap_list = [r for r, _ in caps]
            kw = {"parse_mode": None}
        await client.send_file(dst, files, caption=cap_list, progress_callback=ucb, **kw, **extra)


async def send_group(dst, group, prepared, opts):
    """Uploads/sends an already-prepared group. Returns (sent, error, extra_tmp_to_clean)."""
    client = S().client
    job = S().job
    extra_tmp = None
    try:
        if prepared["text_only"]:
            raw, ents = prepared["caps"][0]
            if not raw.strip():
                return False, None, None
            while True:
                try:
                    await client.send_message(dst, raw, link_preview=False, **ent_kw(ents))
                    return True, None, None
                except FloodWaitError as e:
                    await wait_flood(e)
        while True:
            try:
                await do_upload(client, dst, prepared, opts)
                return True, None, extra_tmp
            except FloodWaitError as e:
                await wait_flood(e)
            except Exception as e:
                # direct server-side copy failed (restricted source): download + re-upload once
                if not prepared["reupload"] and not job.get("force_dl"):
                    job["force_dl"] = True
                    try:
                        prepared = await prepare_group(group, opts)
                        extra_tmp = prepared.get("tmp")
                        continue
                    except Exception as e2:
                        return False, e2, extra_tmp
                return False, e, extra_tmp
    finally:
        reset_dir("ul")


def group_bytes(group):
    return sum((getattr(getattr(m, "file", None), "size", None) or 0) for m in group)


async def run_copy(src_ref, dst_ref, min_id, opts):
    sess = S()
    client = sess.client
    job = sess.job
    rules = opts["rules"]
    job.update(running=True, stop=False, kind="copy", phase="running", started=time.time(),
               last_edit=0.0, src="", dst="", done=0, failed=0, skipped=0, bytes_done=0,
               cur_id=min_id, min_id=min_id, top_id=min_id, errors=[], flood_until=0.0,
               rules_n=len(rules.pairs) if rules else 0, scope=opts["scope"],
               link_n=len(opts["link_rules"].pairs) if opts["link_rules"] else 0,
               has_thumb=bool(opts["thumb"]), has_footer=bool(opts["footer"]),
               delay=get_delay())
    status = None
    ticker_task = None

    async def progress_ticker():
        # keeps /status live: download/upload progress and the flood-wait countdown
        try:
            while job["running"]:
                force = (job["flood_until"] > time.time()
                         or job["dl_tot"] > 0 or job["ul_tot"] > 0)
                await refresh(status, force=force)
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            pass

    def clean_tmp(prepared):
        t = prepared.get("tmp") if isinstance(prepared, dict) else None
        if t:
            shutil.rmtree(t, ignore_errors=True)

    try:
        src = await resolve(src_ref, "Source channel")
        dst = await resolve(dst_ref, "Destination channel")
        job.update(src=title_of(src), dst=title_of(dst), key=f"{src.id}->{dst.id}")
        # protected/private source: skip the pointless direct-copy attempt, download+upload directly
        if getattr(src, "noforwards", False):
            job["force_dl"] = True
        try:
            job["total_msgs"] = getattr(await client.get_messages(src, limit=0), "total", 0) or 0
        except Exception:
            job["total_msgs"] = 0
        latest = await client.get_messages(src, limit=1)
        job["top_id"] = max(latest[0].id if latest else min_id, min_id)
        status = await say("▶️ Copy started…")
        await refresh(status, force=True)
        ticker_task = asyncio.create_task(progress_ticker())

        if PIPELINE:
            # overlap: a producer downloads the next group while the consumer uploads the current one
            queue = asyncio.Queue(maxsize=1)
            STOP = object()

            async def producer():
                group = []
                enq = [0]

                async def enqueue(g):
                    job["dl_no"] = enq[0] + 1
                    enq[0] += len(g)
                    try:
                        prepared = await prepare_group(g, opts)
                    except Exception as e:
                        prepared = {"__error__": e, "tmp": None}
                    await queue.put((g, prepared))

                try:
                    async for m in client.iter_messages(src, reverse=True, min_id=min_id):
                        if job["stop"]:
                            break
                        if m.action:
                            continue
                        if m.grouped_id and group and group[0].grouped_id == m.grouped_id:
                            group.append(m)
                            continue
                        if group:
                            await enqueue(group)
                        group = [m]
                    if group and not job["stop"]:
                        await enqueue(group)
                finally:
                    await queue.put(STOP)

            prod = asyncio.create_task(producer())
            try:
                while True:
                    item = await queue.get()
                    if item is STOP:
                        break
                    group, prepared = item
                    last_id = group[-1].id
                    if isinstance(prepared, dict) and "__error__" in prepared:
                        job["failed"] += len(group)
                        record_error(f"message ID {last_id}", prepared["__error__"])
                    else:
                        job["ul_no"] = job["done"] + job["skipped"] + job["failed"] + 1
                        sent, err, extra_tmp = await send_group(dst, group, prepared, opts)
                        clean_tmp(prepared)
                        if extra_tmp:
                            shutil.rmtree(extra_tmp, ignore_errors=True)
                        if err is not None:
                            job["failed"] += len(group)
                            record_error(f"message ID {last_id}", err)
                        elif sent:
                            job["done"] += len(group)
                            job["bytes_done"] += group_bytes(group)
                        else:
                            job["skipped"] += len(group)
                    job["cur_id"] = last_id
                    save_progress(job["key"], last_id)
                    if job["stop"]:
                        break
                    await asyncio.sleep(job["delay"])
                    await refresh(status)
            finally:
                prod.cancel()
                try:
                    await prod
                except BaseException:
                    pass
                while not queue.empty():
                    try:
                        it = queue.get_nowait()
                    except Exception:
                        break
                    if isinstance(it, tuple):
                        clean_tmp(it[1])
        else:
            # sequential: download a group fully, then upload it (one transfer at a time)
            group = []

            async def flush():
                nonlocal group
                if not group:
                    return
                job["dl_no"] = job["ul_no"] = job["done"] + job["skipped"] + job["failed"] + 1
                last_id = group[-1].id
                try:
                    prepared = await prepare_group(group, opts)
                except Exception as e:
                    prepared = {"__error__": e, "tmp": None}
                if isinstance(prepared, dict) and "__error__" in prepared:
                    job["failed"] += len(group)
                    record_error(f"message ID {last_id}", prepared["__error__"])
                else:
                    sent, err, extra_tmp = await send_group(dst, group, prepared, opts)
                    clean_tmp(prepared)
                    if extra_tmp:
                        shutil.rmtree(extra_tmp, ignore_errors=True)
                    if err is not None:
                        job["failed"] += len(group)
                        record_error(f"message ID {last_id}", err)
                    elif sent:
                        job["done"] += len(group)
                        job["bytes_done"] += group_bytes(group)
                    else:
                        job["skipped"] += len(group)
                job["cur_id"] = last_id
                save_progress(job["key"], last_id)
                group = []
                await asyncio.sleep(job["delay"])
                await refresh(status)

            async for m in client.iter_messages(src, reverse=True, min_id=min_id):
                if job["stop"]:
                    break
                if m.action:
                    continue
                if m.grouped_id and group and group[0].grouped_id == m.grouped_id:
                    group.append(m)
                    continue
                await flush()
                group = [m]
            if group and not job["stop"]:
                await flush()
        job["phase"] = "stopped" if job["stop"] else "done"
    except BadInput as e:
        job["phase"] = "error"
        record_error("setup", e)
        if status is None:
            await say(f"❌ {e}")
    except Exception as e:
        job["phase"] = "error"
        record_error(f"message ID {job['cur_id']}", e)
        if status is None:
            await say(f"❌ Something went wrong: {type(e).__name__}"
                      f"{' (code ' + str(e.code) + ')' if getattr(e, 'code', None) else ''}: {e}")
    finally:
        job["running"] = False
        job["flood_until"] = 0.0
        job["dl_tot"] = job["dl_cur"] = job["ul_tot"] = job["ul_cur"] = 0
        job["dl_speed"] = job["ul_speed"] = 0.0
        if ticker_task is not None:
            ticker_task.cancel()
        job["last_result"] = render_job()
        if status is not None:
            await refresh(status, force=True)
            if job["phase"] == "done" and job["done"] == 0 and job["failed"] == 0 and job["skipped"] == 0:
                await say(f"ℹ️ There was nothing new to copy after message ID {min_id}.\n"
                          "To copy from the very beginning, run /copy again and choose 'From the beginning'.")
            else:
                await say("🔔 Task finished. Send /status to see the summary again." if job["phase"] == "done"
                          else "🔔 Task stopped. Send /status for details.")


# ----------------------------- create engine -----------------------------
async def run_create(name, count):
    sess = S()
    client = sess.client
    job = sess.job
    job.update(running=True, stop=False, kind="create", phase="running", started=time.time(),
               last_edit=0.0, name=name, done=0, total=count, errors=[], flood_until=0.0,
               rules_n=0, link_n=0, has_thumb=False, has_footer=False)
    status = await say("▶️ Channel creation started…")
    await refresh(status, force=True)
    try:
        while job["done"] < count:
            if job["stop"]:
                job["phase"] = "stopped"
                break
            try:
                res = await client(CreateChannelRequest(title=name, about="", broadcast=True, megagroup=False))
                ch = res.chats[0]
                if RESTRICT:
                    await client(ToggleNoForwardsRequest(peer=ch, enabled=True))
                job["done"] += 1
                await refresh(status)
                await asyncio.sleep(CREATE_DELAY)
            except FloodWaitError as e:
                job["flood_until"] = time.time() + e.seconds + 1
                await refresh(status, force=True)
                await asyncio.sleep(e.seconds + 1)
                job["flood_until"] = 0.0
            except Exception as e:
                record_error(f"channel #{job['done'] + 1}", e)
                job["phase"] = "error"
                msg = str(e)
                if "CHANNELS_TOO_MUCH" in msg or "CHANNELS_ADMIN" in msg:
                    await say("ℹ️ Your account has reached Telegram's channel limit. Delete or leave some old channels and try again.")
                break
        else:
            job["phase"] = "done"
    finally:
        job["running"] = False
        job["flood_until"] = 0.0
        job["last_result"] = render_job()
        await refresh(status, force=True)
        await say("🔔 Task finished. Send /status for details.")


def build_opts(conv):
    rules = Rules(conv["rules"]) if conv["rules"] else None
    link_rules = Rules(conv["link_rules"]) if conv["link_rules"] else None
    footer = None
    if conv["footer_text"]:
        ftext, fents = tl_markdown.parse(conv["footer_text"])
        footer = (ftext, fents, conv["footer_pos"])
    return {"rules": rules, "scope": conv["scope"] or "both", "link_rules": link_rules,
            "thumb": conv["thumb"], "footer": footer}


def start_copy_job():
    sess = S()
    conv = sess.conv
    opts = build_opts(conv)
    args = (conv["src"], conv["dst"], conv["start"], opts)
    set_setting("last_src", {"ref": conv["src"], "title": conv["src_title"]})
    set_setting("last_dst", {"ref": conv["dst"], "title": conv["dst_title"]})
    sess.reset_conv()
    sess.job["running"] = True
    asyncio.create_task(run_copy(*args))


def start_create_job():
    sess = S()
    name, count = sess.conv["name"], sess.conv["count"]
    sess.reset_conv()
    sess.job["running"] = True
    asyncio.create_task(run_create(name, count))


# ----------------------------- guided copy flow -----------------------------
PICK_PAGE = 6


async def list_channels(admin_only):
    """Returns [(ref_id, title), ...] of the user's channels (cached ~2 min)."""
    sess = S()
    key = "admin" if admin_only else "all"
    now = time.time()
    cached = sess.chan_cache.get(key)
    if cached and now - cached[0] < 120:
        return cached[1]
    out = []
    try:
        async for d in sess.client.iter_dialogs():
            ent = d.entity
            if isinstance(ent, Channel):
                if admin_only and not (getattr(ent, "creator", False) or getattr(ent, "admin_rights", None)):
                    continue
                out.append((int("-100" + str(ent.id)), title_of(ent)))
                if len(out) >= 1000:
                    break
        sess.dialogs_loaded = True
    except Exception:
        pass
    sess.chan_cache[key] = (now, out)
    return out


def pick_rows(kind, items, page):
    start = page * PICK_PAGE
    rows = [[Button.inline(f"📢 {t[:40]}", f"pk:{kind}:{start + i}".encode())]
            for i, (cid, t) in enumerate(items[start:start + PICK_PAGE])]
    nav = []
    if page > 0:
        nav.append(Button.inline("◀️ Prev", f"pg:{kind}:{page - 1}".encode()))
    if start + PICK_PAGE < len(items):
        nav.append(Button.inline("Next ▶️", f"pg:{kind}:{page + 1}".encode()))
    if nav:
        rows.append(nav)
    return rows


def build_pick(kind, header):
    conv = S().conv
    items = conv.get(f"pick_{kind}") or []
    page = conv.get(f"pick_{kind}_page", 0)
    last = get_setting("last_src" if kind == "src" else "last_dst")
    rows = []
    if last:
        rows.append([Button.inline(f"♻️ Last: {last['title'][:28]}",
                                   b"src:last" if kind == "src" else b"dst:last")])
    rows += pick_rows(kind, items, page)
    rows.append([Button.inline("✖️ Cancel", b"cancel")])
    if items:
        tp = (len(items) + PICK_PAGE - 1) // PICK_PAGE
        tail = (f"\n\n👇 Tap a channel (page {page + 1}/{tp}). "
                "Lots of channels? Just type part of the name to search. "
                "You can also send a channel ID (-100…), an @username, or forward a message.")
    else:
        tail = ("\n\nType part of a channel's name to search, or send a channel ID (-100…) "
                "or @username, or forward a message from it.")
    return header + tail, rows


def src_header():
    return ("📥 Step 1 of 4 — Source channel (copy FROM)\n\n"
            "This can be someone else's channel — it does NOT have to be yours.\n"
            "• Pick a channel you've joined from the buttons below, or\n"
            "• Send a channel ID (private channels look like -1001234567890), or\n"
            "• For a public channel, send its @username or t.me link, or\n"
            "• Forward any message from the channel to me.")


def dst_header():
    return (f"✅ Source: {S().conv['src_title']}\n\n"
            "📤 Step 2 of 4 — Destination channel (copy TO)\n\n"
            "This must be YOUR channel (your account is an admin).\n"
            "Pick one below, or send its ID / @username, or forward a message from it.")


async def ask_src():
    conv = S().conv
    conv["step"] = "src"
    items = await list_channels(admin_only=False)
    conv["pick_src_all"] = items
    conv["pick_src"] = items
    conv["pick_src_page"] = 0
    text, rows = build_pick("src", src_header())
    await say(text, buttons=rows)


async def ask_dst():
    conv = S().conv
    conv["step"] = "dst"
    items = await list_channels(admin_only=True)
    conv["pick_dst_all"] = items
    conv["pick_dst"] = items
    conv["pick_dst_page"] = 0
    text, rows = build_pick("dst", dst_header())
    await say(text, buttons=rows)


async def handle_channel_text(kind, text, fwd):
    """Handles typed input at the source/destination step: forward, @username/ID, or a search query."""
    conv = S().conv
    label = "Source" if kind == "src" else "Destination"
    setfn = set_src if kind == "src" else set_dst
    if fwd:
        if await setfn(fwd):
            await copy_next()
        return
    tokens = text.split()
    ref = None
    if len(tokens) == 1:
        tok = tokens[0]
        try:
            ref = check_ref(tok, label)
        except BadInput as e:
            # if it clearly looks like an ID/username attempt, show the hint instead of searching
            if re.match(r"^(@|-|\d|https?://|t\.me)", tok):
                await say(f"❌ {e}\n\nYou can also type part of the channel's name to search for it.")
                return
            ref = None
    if ref is not None:
        if await setfn(ref):
            await copy_next()
        return
    # treat the text as a search over the user's channels
    q = text.strip().lower()
    all_items = conv.get(f"pick_{kind}_all") or []
    matches = [it for it in all_items if q in it[1].lower()] if q else all_items
    conv[f"pick_{kind}"] = matches
    conv[f"pick_{kind}_page"] = 0
    base = src_header() if kind == "src" else dst_header()
    if matches:
        header = base + f"\n\n🔍 {len(matches)} match(es) for “{text.strip()}”:"
        txt, rows = build_pick(kind, header)
        await say(txt, buttons=rows)
    else:
        await say(f"🔍 No channels matching “{text.strip()}”.\n"
                  "Try fewer letters, or send the @username / ID, or forward a message from the channel.")


async def ask_start():
    conv = S().conv
    conv["step"] = "start"
    key = f"{conv['src_id']}->{conv['dst_id']}"
    last_id = progress_map().get(key, 0)
    rows = [[Button.inline("⏮ From the beginning", b"start:begin")]]
    if last_id:
        rows.append([Button.inline(f"⏯ Resume (after message ID {last_id})", b"start:resume")])
    await say(f"✅ Destination: {conv['dst_title']}\n\n"
              "📍 Step 3 of 4 — Starting point\n\n"
              "Choose where to start, or send a message ID to start from that message.", buttons=rows)


async def show_extras():
    conv = S().conv
    conv["step"] = "extras"
    rules = conv["rules"] or []
    lr = conv["link_rules"] or []
    footer = conv["footer_text"]
    rows = [
        [Button.inline(("✅" if rules else "➕") + f" Replace words ({len(rules)})", b"ex:words")],
        [Button.inline(("✅" if lr else "➕") + f" Replace links ({len(lr)})", b"ex:links")],
        [Button.inline(("🖼 Thumbnail: set" if conv["thumb"] else "➕ Add thumbnail"), b"ex:thumb")],
        [Button.inline(("📝 Caption/promo: set" if footer else "➕ Add caption / promo link"), b"ex:cap")],
        [Button.inline("✅ Done — review & start", b"ex:done")],
        [Button.inline("✖️ Cancel", b"cancel")],
    ]
    await say("⚙️ Step 4 of 4 — Extras (all optional)\n\n"
              "Set any of these, then tap Done:\n"
              f"• Replace words (captions/file names): {len(rules)} rule(s)\n"
              f"• Replace hidden hyperlink URLs: {len(lr)} rule(s)\n"
              f"• Custom thumbnail: {'yes' if conv['thumb'] else 'no'}\n"
              f"• Extra caption / promo link: {'yes (' + conv['footer_pos'] + ')' if footer else 'no'}",
              buttons=rows)


async def ask_rules():
    conv = S().conv
    conv["step"] = "rules"
    rows = [[Button.inline("⏭ Clear (no word rules)", b"rules:skip")]]
    prev = get_setting("rules") or []
    if prev:
        rows.insert(0, [Button.inline(f"♻️ Use previous rules ({len(prev)})", b"rules:prev")])
    rows.append([Button.inline("⬅️ Back to extras", b"ex:back")])
    await say("✏️ Replace words\n\n"
              "I can replace words in captions/text and in file names.\n"
              "Send one rule per line like this:\n\n"
              "old word => new word\n\n"
              "Example:\nLecture => Class\n@oldchannel => @newchannel\n\n"
              "Matching ignores upper/lower case. Leave the right side empty to remove a word.",
              buttons=rows)


async def ask_scope():
    conv = S().conv
    conv["step"] = "scope"
    await say(f"✅ {len(conv['rules'])} word rule(s) saved.\n\nWhere should they be applied?",
              buttons=[[Button.inline("📝 Captions & file names", b"scope:both")],
                       [Button.inline("📝 Captions / text only", b"scope:text")],
                       [Button.inline("📁 File names only", b"scope:names")]])


async def ask_links():
    conv = S().conv
    conv["step"] = "links"
    rows = [[Button.inline("⏭ Clear (no link rules)", b"links:skip")]]
    prev = get_setting("link_rules") or []
    if prev:
        rows.insert(0, [Button.inline(f"♻️ Use previous link rules ({len(prev)})", b"links:prev")])
    rows.append([Button.inline("⬅️ Back to extras", b"ex:back")])
    await say("🔗 Replace links (hidden hyperlink URLs)\n\n"
              "This rewrites the URL hidden inside clickable text (e.g. [Join](https://t.me/old)).\n"
              "Send one rule per line:\n\n"
              "old => new\n\n"
              "Example:\nt.me/oldchannel => t.me/newchannel\nhttps://old.site => https://new.site\n\n"
              "Matching ignores upper/lower case.",
              buttons=rows)


async def ask_thumb():
    conv = S().conv
    conv["step"] = "thumb"
    rows = []
    saved = get_setting("thumb_path")
    if saved and os.path.exists(saved) and conv["thumb"] != saved:
        rows.append([Button.inline("♻️ Use last saved thumbnail", b"thumb:saved")])
    if conv["thumb"]:
        rows.append([Button.inline("🗑 Remove thumbnail", b"thumb:remove")])
    rows.append([Button.inline("⬅️ Back to extras", b"ex:back")])
    await say("🖼 Custom thumbnail\n\n"
              "Send me a photo to use as the thumbnail for copied videos and documents.\n"
              "Those files are re-uploaded (downloaded then sent again), so they take longer.\n"
              "Photos in the source are copied as-is (thumbnails don't apply to photos).",
              buttons=rows)


async def ask_caption():
    conv = S().conv
    conv["step"] = "caption"
    footer = conv["footer_text"]
    rows = []
    if footer:
        other = "prepend" if conv["footer_pos"] == "append" else "append"
        rows.append([Button.inline(f"↕️ Position: {conv['footer_pos']} (tap to {other})", b"cap:pos")])
        rows.append([Button.inline("🗑 Remove caption", b"cap:remove")])
    rows.append([Button.inline("⬅️ Back to extras", b"ex:back")])
    cur = f"\n\nCurrent ({conv['footer_pos']}):\n{footer}" if footer else ""
    await say("📝 Caption / promo link\n\n"
              "Send the extra text to add to every caption. For a clickable promo link use markdown:\n\n"
              "[Join our channel](https://t.me/yourchannel)\n\n"
              "You can mix normal text and links. Send new text to replace the current one." + cur,
              buttons=rows)


async def show_copy_confirm():
    conv = S().conv
    conv["step"] = "confirm"
    rules = conv["rules"] or []
    lr = conv["link_rules"] or []
    scope = {"both": "captions & file names", "text": "captions only", "names": "file names only"}.get(conv["scope"] or "both")
    lines = ["✅ Everything is ready.\n",
             f"Source: {conv['src_title']}",
             f"Destination: {conv['dst_title']}",
             f"Start: {conv['start_label']}"]
    if rules:
        lines.append(f"Word rules: {len(rules)} ({scope})")
        for o, n in rules[:6]:
            lines.append(f"   {o} → {n if n else '(remove)'}")
        if len(rules) > 6:
            lines.append(f"   …and {len(rules) - 6} more")
    if lr:
        lines.append(f"Link rules: {len(lr)}")
        for o, n in lr[:4]:
            lines.append(f"   {o} → {n}")
    if conv["thumb"]:
        lines.append("Custom thumbnail: on")
    if conv["footer_text"]:
        lines.append(f"Caption/promo ({conv['footer_pos']}): {conv['footer_text'][:60]}")
    if not (rules or lr or conv["thumb"] or conv["footer_text"]):
        lines.append("Extras: none")
    if (rules and conv["scope"] in ("both", "names")) or conv["thumb"]:
        lines.append("\nNote: renamed files and files with a custom thumbnail are downloaded and uploaded again, so they take longer.")
    await say("\n".join(lines), buttons=[[Button.inline("▶️ Start copying", b"go")],
                                         [Button.inline("⬅️ Back to extras", b"ex:back")],
                                         [Button.inline("✖️ Cancel", b"cancel")]])


async def copy_next():
    conv = S().conv
    if conv["src"] is None:
        await ask_src()
    elif conv["dst"] is None:
        await ask_dst()
    elif conv["start"] is None:
        await ask_start()
    else:
        await show_extras()


async def set_src(ref):
    conv = S().conv
    try:
        ent = await resolve(ref, "Source channel")
    except BadInput as e:
        conv["step"] = "src"
        await say(f"❌ {e}\n\nPlease send the source again, or /stop.")
        return False
    conv.update(src=ref, src_id=ent.id, src_title=title_of(ent))
    return True


async def set_dst(ref):
    sess = S()
    conv = sess.conv
    try:
        ent = await resolve(ref, "Destination channel")
        if ent.id == conv["src_id"]:
            raise BadInput("The source and destination are the same channel. Please choose a different destination.")
        if isinstance(ent, Channel) and ent.broadcast:
            try:
                perms = await sess.client.get_permissions(ent)
                if not (perms.is_creator or perms.is_admin):
                    raise BadInput("Your account is not an admin of the destination channel. "
                                   "Please make it an admin with permission to post, then send the destination again.")
            except BadInput:
                raise
            except Exception:
                pass
    except BadInput as e:
        conv["step"] = "dst"
        await say(f"❌ {e}\n\nPlease send the destination again, or /stop.")
        return False
    conv.update(dst=ref, dst_id=ent.id, dst_title=title_of(ent))
    return True


def set_start_from_id(n):
    conv = S().conv
    conv["start"] = max(0, n - 1)
    conv["start_label"] = f"message ID {n}" if n > 0 else "the beginning"


async def save_thumb_from(event):
    """Downloads a photo the user sent and stores it as their thumbnail."""
    sess = S()
    conv = sess.conv
    os.makedirs(THUMB_DIR, exist_ok=True)
    dst = os.path.join(THUMB_DIR, f"{sess.user_id}.jpg")
    tmpf = None
    try:
        tmpf = await event.message.download_media(file=tempfile.mktemp(suffix=".img", dir=DOWNLOAD_DIR))
        ok = normalize_thumb(tmpf, dst)
    except Exception:
        ok = False
    finally:
        if tmpf and os.path.exists(tmpf):
            try:
                os.remove(tmpf)
            except Exception:
                pass
    if ok:
        conv["thumb"] = dst
        set_setting("thumb_path", dst)
        await say("✅ Thumbnail saved.")
        await show_extras()
    else:
        await say("❌ I couldn't process that image. Please send a normal photo, or tap Back.")


# ----------------------------- guided create flow -----------------------------
async def ask_create_name():
    conv = S().conv
    conv["step"] = "c_name"
    await say("🆕 Step 1 of 2 — Channel name\n\nSend the name to use for all the channels (spaces are fine).\n"
              "Send /stop to cancel.")


async def ask_create_count():
    conv = S().conv
    conv["step"] = "c_count"
    await say(f"✅ Name: {conv['name']}\n\n🔢 Step 2 of 2 — How many?\n\n"
              f"Send the number of channels to create (1 to {MAX_COUNT}).")


async def show_create_confirm():
    conv = S().conv
    conv["step"] = "c_confirm"
    await say(f"✅ Ready.\n\nName: {conv['name']}\nCount: {conv['count']}\n"
              f"Restricted (no forwarding): {'yes' if RESTRICT else 'no'}",
              buttons=[[Button.inline("▶️ Create channels", b"go")], [Button.inline("✖️ Cancel", b"cancel")]])


# ----------------------------- login flow -----------------------------
async def _finish_login(sess):
    sess.client = sess.login_client
    sess.login_client = None
    sess.dialogs_loaded = False
    session_strings[str(sess.user_id)] = sess.client.session.save()
    save_sessions()
    try:
        me = await sess.client.get_me()
        sess.name = me.first_name or "there"
        who = me.first_name or "your account"
    except Exception:
        who = "your account"
    sess.reset_conv()
    await say(f"✅ Logged in as {who}.\n\nYou can now use /copy and /create. Send /help for the guide.")


async def login_phone(text):
    sess = S()
    conv = sess.conv
    phone = re.sub(r"[^\d+]", "", text)
    if not re.fullmatch(r"\+?\d{7,15}", phone):
        await say("❌ That doesn't look like a valid phone number.\nSend it in international format, e.g. +919876543210, or /stop.")
        return
    try:
        sess.login_client = TelegramClient(StringSession(), api_id, api_hash)
        await sess.login_client.connect()
        sent = await sess.login_client.send_code_request(phone)
    except PhoneNumberInvalidError:
        await say("❌ Telegram says that phone number is invalid. Send a valid one, or /stop.")
        return
    except FloodWaitError as e:
        await say(f"⏳ Too many attempts. Telegram asked to wait {fmt_duration(e.seconds)}. Try /login later.")
        sess.reset_conv()
        return
    except Exception as e:
        await say(f"❌ Couldn't send the code: {e}\n\nCheck the number and send /login to try again.")
        sess.reset_conv()
        return
    conv["phone"] = phone
    conv["phone_code_hash"] = sent.phone_code_hash
    conv["step"] = "login_code"
    await say("📩 I sent a login code to your Telegram app.\n\n"
              "Type the code here, but put spaces or dashes between the digits so Telegram doesn't block it.\n"
              "For example: 1 2 3 4 5  or  1-2-3-4-5\n\n"
              "Send /stop to cancel.")


async def login_code(text):
    sess = S()
    conv = sess.conv
    code = re.sub(r"\D", "", text)
    if not code:
        await say("Please send the digits of the code (spaces or dashes between them are fine), or /stop.")
        return
    try:
        await sess.login_client.sign_in(phone=conv["phone"], code=code, phone_code_hash=conv["phone_code_hash"])
    except SessionPasswordNeededError:
        conv["step"] = "login_password"
        await say("🔒 Your account uses two-step verification.\nSend your password here to finish logging in, or /stop.")
        return
    except PhoneCodeInvalidError:
        await say("❌ That code is wrong. Type it again (spaces/dashes between digits are fine), or /stop.")
        return
    except PhoneCodeExpiredError:
        await say("❌ The code expired. Send /login to get a new one.")
        sess.reset_conv()
        return
    except Exception as e:
        await say(f"❌ Login failed: {e}\n\nSend /login to try again.")
        sess.reset_conv()
        return
    await _finish_login(sess)


async def login_password(text):
    sess = S()
    try:
        await sess.login_client.sign_in(password=text.strip())
    except PasswordHashInvalidError:
        await say("❌ Wrong password. Send it again, or /stop.")
        return
    except Exception as e:
        await say(f"❌ Login failed: {e}\n\nSend /login to try again.")
        sess.reset_conv()
        return
    await _finish_login(sess)


# ----------------------------- commands -----------------------------
async def cmd_start(arg):
    S().reset_conv()
    await say(WELCOME.format(name=S().name))


async def cmd_help(arg):
    await say(HELP)


async def cmd_about(arg):
    await say(ABOUT)


async def cmd_ping(arg):
    await say(f"🏓 Pong! I'm online.\nUptime: {fmt_duration(time.time() - S().started_at)}")


async def cmd_login(arg):
    sess = S()
    if sess.job["running"]:
        await say("⚠️ A task is running. Send /stop first if you want to switch accounts.")
        return
    if await ensure_client(sess):
        try:
            me = await sess.client.get_me()
            who = me.first_name or "your account"
        except Exception:
            who = "your account"
        await say(f"✅ You're already logged in as {who}.\nSend /logout first if you want to use a different account.")
        return
    sess.reset_conv()
    sess.conv["kind"] = "login"
    sess.conv["step"] = "login_phone"
    await say("🔐 Let's connect your Telegram account.\n\n"
              "Send your phone number in international format, e.g. +919876543210.\n\n"
              "⚠️ Logging in lets this bot act on your behalf (copy content, create channels). "
              "Only do this with a bot you trust. Send /stop to cancel.")


async def cmd_logout(arg):
    sess = S()
    if sess.job["running"]:
        await say("⚠️ A task is running. Send /stop first.")
        return
    c = await ensure_client(sess)
    if not c and str(sess.user_id) not in session_strings:
        await say("You're not logged in. Send /login to connect an account.")
        return
    try:
        if c:
            await c.log_out()
    except Exception:
        pass
    try:
        if sess.client and sess.client.is_connected():
            await sess.client.disconnect()
    except Exception:
        pass
    sess.client = None
    sess.dialogs_loaded = False
    sess.name = "there"
    session_strings.pop(str(sess.user_id), None)
    save_sessions()
    sess.reset_conv()
    await say("👋 Logged out. Your session has been removed from the bot.\nSend /login to connect again.")


async def cmd_speed(arg):
    arg = arg.strip().replace(",", ".")
    if not arg:
        await say(f"⚡ Delay between copied messages: {get_delay()}s.\n\n"
                  "Change it with: /speed <seconds>  (e.g. /speed 0.8)\n"
                  "Lower = faster, but too low makes Telegram pause you (flood wait).\n"
                  "Allowed range: 0.3 – 10 seconds. Applies to your next copy.")
        return
    try:
        v = float(arg)
    except ValueError:
        await say("❌ Please send a number, e.g. /speed 0.8")
        return
    if not (0.3 <= v <= 10):
        await say("❌ Please choose a value between 0.3 and 10 seconds.")
        return
    set_setting("delay", v)
    note = "\n⚠️ That's quite fast — if you start seeing flood waits, raise it a little." if v < 1 else ""
    await say(f"✅ Delay set to {v}s between messages." + note +
              ("\n(A copy is running now — this takes effect on your next copy.)" if S().job["running"] else ""))


async def cmd_status(arg):
    sess = S()
    if sess.job["running"] or sess.job["kind"]:
        await say(render_job())
    elif sess.conv["kind"]:
        await say("📝 A request is in progress. Please continue with the last question, or send /stop to discard it.")
    else:
        await say("✅ No task is running. Send /copy or /create to start one.")


async def cmd_errors(arg):
    errs = S().job["errors"]
    if not errs:
        await say("✅ No errors recorded.")
        return
    lines = [f"⚠️ {len(errs)} error(s) recorded. Showing the latest {min(len(errs), 15)}:\n"]
    for i, e in enumerate(errs[-15:], start=len(errs) - min(len(errs), 15) + 1):
        c = f" (code {e['code']})" if e["code"] else ""
        lines.append(f"#{i} {e['where']}\n   {e['name']}{c}\n   {e['detail'][:150]}")
    await say("\n".join(lines))


async def cmd_stop(arg):
    sess = S()
    if sess.job["running"]:
        sess.job["stop"] = True
        await say("🛑 Stopping after the current item…")
    elif sess.conv["kind"]:
        sess.reset_conv()
        await say("Cancelled. Send /copy or /create whenever you're ready.")
    else:
        await say("There is nothing to stop right now.")


async def cmd_copy(arg):
    sess = S()
    if sess.job["running"]:
        await say("⚠️ A task is already running. Send /status to see it or /stop to stop it.")
        return
    if not await require_login(sess):
        return
    sess.reset_conv()
    conv = sess.conv
    conv["kind"] = "copy"
    parts = arg.split()
    if len(parts) > 3:
        await say("❌ Too many values. Use: /copy <source> <destination> [message id]")
        return
    if len(parts) >= 1:
        try:
            ref = check_ref(parts[0], "Source")
        except BadInput as e:
            await say(f"❌ {e}")
            return await copy_next()
        if not await set_src(ref):
            return
    if len(parts) >= 2 and conv["src"] is not None:
        try:
            ref = check_ref(parts[1], "Destination")
        except BadInput as e:
            await say(f"❌ {e}")
            return await copy_next()
        if not await set_dst(ref):
            return
    if len(parts) == 3:
        if parts[2].isdigit():
            set_start_from_id(int(parts[2]))
        else:
            await say(f"❌ '{parts[2]}' must be a number (message ID).")
    await copy_next()


async def cmd_create(arg):
    sess = S()
    if sess.job["running"]:
        await say("⚠️ A task is already running. Send /status to see it or /stop to stop it.")
        return
    if not await require_login(sess):
        return
    sess.reset_conv()
    conv = sess.conv
    conv["kind"] = "create"
    parts = arg.rsplit(None, 1)
    if len(parts) == 2 and parts[1].isdigit():
        n = int(parts[1])
        if 1 <= n <= MAX_COUNT and len(parts[0].strip()) <= 128:
            conv.update(name=parts[0].strip(), count=n)
            return await show_create_confirm()
        await say(f"❌ The number must be between 1 and {MAX_COUNT} and the name at most 128 characters.")
    elif arg:
        conv["name"] = arg.strip()[:128]
        return await ask_create_count()
    await ask_create_name()


COMMANDS = {
    "/start": cmd_start, "/help": cmd_help, "/login": cmd_login, "/logout": cmd_logout,
    "/copy": cmd_copy, "/create": cmd_create, "/status": cmd_status, "/errors": cmd_errors,
    "/speed": cmd_speed, "/stop": cmd_stop, "/cancel": cmd_stop, "/ping": cmd_ping, "/about": cmd_about,
}


# ----------------------------- free text -----------------------------
async def handle_text(event, text):
    sess = S()
    conv = sess.conv
    step = conv["step"]
    fwd = fwd_channel_id(event.message)

    if step == "login_phone":
        await login_phone(text)
    elif step == "login_code":
        await login_code(text)
    elif step == "login_password":
        await login_password(text)
    elif step == "thumb":
        msg = event.message
        is_image = bool(msg.photo) or (msg.document and (getattr(msg.document, "mime_type", "") or "").startswith("image/"))
        if is_image:
            await save_thumb_from(event)
        else:
            await say("Please send a photo to use as the thumbnail, or tap Back.")
    elif step == "caption":
        if not text.strip():
            await say("Please send some text for the caption, or tap Back.")
            return
        conv["footer_text"] = text.strip()
        set_setting("footer_text", conv["footer_text"])
        await say("✅ Caption/promo text saved.")
        await ask_caption()
    elif step == "src":
        await handle_channel_text("src", text, fwd)
    elif step == "dst":
        await handle_channel_text("dst", text, fwd)
    elif step == "start":
        if text.isdigit():
            set_start_from_id(int(text))
            await copy_next()
        else:
            await say("Please choose an option using the buttons above, or send a message ID (a number).")
    elif step == "rules":
        pairs, bad = parse_rules(text)
        if bad:
            await say("❌ I couldn't read these line(s):\n" + "\n".join(f"• {b}" for b in bad[:8]) +
                      "\n\nUse the format: old word => new word\nSend all rules again, or tap Back.")
            return
        if not pairs:
            await say("I couldn't find any rule. Use the format: old word => new word, one per line.")
            return
        if len(pairs) > 100:
            await say("❌ Please send at most 100 rules.")
            return
        conv["rules"] = pairs
        set_setting("rules", pairs)
        await ask_scope()
    elif step == "links":
        pairs, bad = parse_rules(text)
        if bad:
            await say("❌ I couldn't read these line(s):\n" + "\n".join(f"• {b}" for b in bad[:8]) +
                      "\n\nUse the format: old => new\nSend all rules again, or tap Back.")
            return
        if not pairs:
            await say("I couldn't find any rule. Use the format: old => new, one per line.")
            return
        if len(pairs) > 100:
            await say("❌ Please send at most 100 rules.")
            return
        conv["link_rules"] = pairs
        set_setting("link_rules", pairs)
        await say(f"✅ {len(pairs)} link rule(s) saved.")
        await show_extras()
    elif step in ("scope", "extras", "confirm", "c_confirm"):
        await say("Please choose an option using the buttons above, or send /stop to cancel.")
    elif step == "c_name":
        name = text.strip()
        if not name or len(name) > 128:
            await say("❌ Please send a name between 1 and 128 characters.")
            return
        conv["name"] = name
        await ask_create_count()
    elif step == "c_count":
        if text.isdigit() and 1 <= int(text) <= MAX_COUNT:
            conv["count"] = int(text)
            await show_create_confirm()
        else:
            await say(f"❌ Please send a number between 1 and {MAX_COUNT}.")
    else:
        if GREETING_RE.match(text):
            await say(f"Hello, {sess.name}! 👋 How can I help you today?\n"
                      "Send /login to connect your account, then /copy or /create. Send /help for all commands.")
        elif THANKS_RE.match(text):
            await say("You're welcome! Let me know if you need anything else.")
        else:
            await say("I can copy content between channels and create channels on your account.\n"
                      "Send /login to connect, then /copy or /create, or /help for all commands.")


async def handle_message(event):
    text = (event.raw_text or "").strip()
    if text.startswith("/"):
        parts = text.split(None, 1)
        cmd = parts[0].lower().split("@")[0]
        arg = parts[1].strip() if len(parts) > 1 else ""
        fn = COMMANDS.get(cmd)
        if fn:
            await fn(arg)
        else:
            await say("Sorry, I don't recognise that command. Send /help to see what I can do.")
        return
    await handle_text(event, text)


@bot.on(events.NewMessage(incoming=True))
async def on_message(event):
    if not event.is_private:
        return
    sess = get_sess(event.sender_id)
    _cur.set(sess)
    if sess.name == "there":
        try:
            sender = await event.get_sender()
            sess.name = getattr(sender, "first_name", None) or "there"
        except Exception:
            pass
    try:
        await handle_message(event)
    except Exception as e:
        print(f"Handler error: {e}")
        try:
            await say(f"⚠️ Something went wrong: {e}")
        except Exception:
            pass


@bot.on(events.CallbackQuery)
async def on_button(event):
    sess = get_sess(event.sender_id)
    _cur.set(sess)
    data = (event.data or b"").decode()
    conv = sess.conv
    step = conv["step"]

    async def expired():
        await event.answer("This request has expired. Please start again.", alert=True)

    try:
        if data == "cancel":
            await event.answer()
            sess.reset_conv()
            await event.edit("Cancelled. Send /copy or /create whenever you're ready.")
        elif data == "src:last" and step == "src":
            last = get_setting("last_src")
            if not last:
                return await expired()
            await event.answer()
            await event.edit(f"Source selected: {last['title']}")
            if await set_src(last["ref"]):
                await copy_next()
        elif data == "dst:last" and step == "dst":
            last = get_setting("last_dst")
            if not last:
                return await expired()
            await event.answer()
            await event.edit(f"Destination selected: {last['title']}")
            if await set_dst(last["ref"]):
                await copy_next()
        elif data.startswith("pg:"):
            try:
                _, kind, page = data.split(":")
            except ValueError:
                return await expired()
            conv[f"pick_{kind}_page"] = int(page)
            text, rows = build_pick(kind, src_header() if kind == "src" else dst_header())
            await event.answer()
            try:
                await event.edit(text, buttons=rows)
            except Exception:
                pass
        elif data.startswith("pk:"):
            try:
                _, kind, idx = data.split(":")
                idx = int(idx)
            except ValueError:
                return await expired()
            items = conv.get(f"pick_{kind}") or []
            if idx >= len(items) or step != kind:
                return await expired()
            cid, title = items[idx]
            await event.answer()
            await event.edit(f"Selected: {title}")
            if kind == "src":
                if await set_src(cid):
                    await copy_next()
            else:
                if await set_dst(cid):
                    await copy_next()
        elif data in ("start:begin", "start:resume") and step == "start":
            await event.answer()
            if data == "start:begin":
                conv.update(start=0, start_label="the beginning")
            else:
                key = f"{conv['src_id']}->{conv['dst_id']}"
                last_id = progress_map().get(key, 0)
                conv.update(start=last_id, start_label=f"resuming after message ID {last_id}")
            await event.edit(f"Start: {conv['start_label']}")
            await copy_next()
        # ----- extras hub -----
        elif data == "ex:words":
            await event.answer()
            await ask_rules()
        elif data == "ex:links":
            await event.answer()
            await ask_links()
        elif data == "ex:thumb":
            await event.answer()
            await ask_thumb()
        elif data == "ex:cap":
            await event.answer()
            await ask_caption()
        elif data == "ex:back":
            await event.answer()
            await show_extras()
        elif data == "ex:done":
            await event.answer()
            await show_copy_confirm()
        # ----- word rules -----
        elif data in ("rules:skip", "rules:prev") and step == "rules":
            await event.answer()
            if data == "rules:skip":
                conv["rules"] = []
                await show_extras()
            else:
                conv["rules"] = [tuple(p) for p in (get_setting("rules") or [])]
                await ask_scope()
        elif data.startswith("scope:") and step == "scope":
            await event.answer()
            conv["scope"] = data.split(":", 1)[1]
            await show_extras()
        # ----- link rules -----
        elif data in ("links:skip", "links:prev") and step == "links":
            await event.answer()
            if data == "links:skip":
                conv["link_rules"] = []
            else:
                conv["link_rules"] = [tuple(p) for p in (get_setting("link_rules") or [])]
            await show_extras()
        # ----- thumbnail -----
        elif data == "thumb:saved" and step == "thumb":
            await event.answer()
            saved = get_setting("thumb_path")
            if saved and os.path.exists(saved):
                conv["thumb"] = saved
                await show_extras()
            else:
                await say("No saved thumbnail found. Please send a photo.")
        elif data == "thumb:remove" and step == "thumb":
            await event.answer()
            conv["thumb"] = None
            await show_extras()
        # ----- caption / promo -----
        elif data == "cap:pos" and step == "caption":
            await event.answer()
            conv["footer_pos"] = "prepend" if conv["footer_pos"] == "append" else "append"
            await ask_caption()
        elif data == "cap:remove" and step == "caption":
            await event.answer()
            conv["footer_text"] = None
            await show_extras()
        # ----- start -----
        elif data == "go" and step in ("confirm", "c_confirm"):
            if sess.job["running"]:
                await event.answer("Another task is already running.", alert=True)
                return
            await event.answer()
            await event.edit("▶️ Started. I'll show live progress below. Use /status any time.")
            if step == "confirm":
                start_copy_job()
            else:
                start_create_job()
        else:
            await expired()
    except Exception as e:
        print(f"Button error: {e}")
        await say(f"⚠️ Something went wrong: {e}")


# ----------------------------- startup -----------------------------
def _quiet_exception_handler(loop, context):
    # hide harmless asyncio noise from abandoned network coroutines
    msg = context.get("message", "")
    if "Task was destroyed but it is pending" in msg:
        return
    if isinstance(context.get("exception"), GeneratorExit):
        return
    loop.default_exception_handler(context)


async def main():
    # hide Telethon's transient connection/auth warnings (they are retried automatically)
    logging.getLogger("telethon").setLevel(logging.ERROR)
    # clear any leftover temp download files from a previous run so they don't fill the disk
    if DOWNLOAD_DIR and os.path.isdir(DOWNLOAD_DIR):
        for name in os.listdir(DOWNLOAD_DIR):
            shutil.rmtree(os.path.join(DOWNLOAD_DIR, name), ignore_errors=True)
    try:
        bot.loop.set_exception_handler(_quiet_exception_handler)
    except Exception:
        pass
    try:
        await bot.start(bot_token=bot_token)
    except Exception as e:
        print(f"ERROR: BOT_TOKEN looks invalid: {e}")
        sys.exit(1)
    bme = await bot.get_me()
    try:
        await bot(SetBotCommandsRequest(
            scope=BotCommandScopeDefault(), lang_code="",
            commands=[
                BotCommand("start", "Welcome message"),
                BotCommand("login", "Connect your Telegram account"),
                BotCommand("logout", "Disconnect your account"),
                BotCommand("copy", "Copy a channel (guided)"),
                BotCommand("create", "Create multiple channels"),
                BotCommand("status", "Live progress"),
                BotCommand("speed", "Set delay between messages"),
                BotCommand("errors", "Detailed error list"),
                BotCommand("stop", "Stop the current task"),
                BotCommand("ping", "Check that the bot is online"),
                BotCommand("about", "About this assistant"),
                BotCommand("help", "How to use this assistant"),
            ]))
    except Exception as e:
        print(f"Could not set the command menu: {e}")

    print(f"Channel Tools Assistant {VERSION} is running. Bot: @{bme.username}")
    print(f"Logged-in users remembered: {len(session_strings)}")
    await bot.run_until_disconnected()


if __name__ == "__main__":
    bot.loop.run_until_complete(main())
