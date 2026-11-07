import os
import re
import sys
import json
import time
import copy
import shutil
import struct
import asyncio
import tempfile
from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from telethon.extensions import markdown as tl_markdown
from telethon.tl.functions.channels import CreateChannelRequest
from telethon.tl.functions.messages import ToggleNoForwardsRequest
from telethon.tl.functions.bots import SetBotCommandsRequest
from telethon.tl.types import (
    MessageMediaWebPage, MessageMediaPoll, Channel, PeerChannel,
    BotCommand, BotCommandScopeDefault,
)

VERSION = "v7"


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
session = need("SESSION_STRING")
bot_token = need("BOT_TOKEN")

RESTRICT = os.environ.get("RESTRICT", "1") == "1"            # channels created by /create are restricted
MAX_COUNT = int(os.environ.get("MAX_COUNT", "50"))           # max channels per /create
CREATE_DELAY = float(os.environ.get("CREATE_DELAY", "3"))    # wait between created channels
DELAY = float(os.environ.get("DELAY", "1.5"))                # wait between copied messages
EDIT_EVERY = float(os.environ.get("EDIT_EVERY", "4"))        # seconds between live status updates
PROGRESS_FILE = os.environ.get("PROGRESS_FILE", "progress.json")
SETTINGS_FILE = os.environ.get("SETTINGS_FILE", "settings.json")

# `client` = your own Telegram account (does the real work: creating channels, copying content)
# `bot`    = the BotFather bot you chat with (receives commands and shows progress)
client = TelegramClient(StringSession(session), api_id, api_hash)
bot = TelegramClient(StringSession(), api_id, api_hash)
bot.parse_mode = None  # bot messages are plain text

my_id = None
my_name = "there"
started_at = time.time()
dialogs_loaded = False


class BadInput(Exception):
    pass


# ----------------------------- state -----------------------------
conv = {}


def reset_conv():
    conv.clear()
    conv.update(kind=None, step=None,
                src=None, src_id=None, src_title="", dst=None, dst_id=None, dst_title="",
                start=None, start_label="", rules=None, scope=None,
                name="", count=0)


reset_conv()

job = {"running": False, "stop": False, "kind": None, "phase": "idle", "last_edit": 0.0,
       "started": 0.0, "src": "", "dst": "", "done": 0, "failed": 0, "skipped": 0,
       "cur_id": 0, "min_id": 0, "top_id": 0, "errors": [], "flood_until": 0.0,
       "rules_n": 0, "scope": "both", "name": "", "total": 0, "key": "", "last_result": ""}


# ----------------------------- small helpers -----------------------------
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


def save_progress(key, last_id):
    data = jload(PROGRESS_FILE)
    data[key] = last_id
    jsave(PROGRESS_FILE, data)


def get_setting(name, default=None):
    return jload(SETTINGS_FILE).get(name, default)


def set_setting(name, value):
    data = jload(SETTINGS_FILE)
    data[name] = value
    jsave(SETTINGS_FILE, data)


def fmt_duration(sec):
    sec = int(max(sec, 0))
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def title_of(ent):
    return getattr(ent, "title", None) or getattr(ent, "first_name", None) or str(getattr(ent, "id", "?"))


def bar(pct, width=12):
    pct = min(100.0, max(0.0, pct))
    filled = int(round(pct / 100 * width))
    return "█" * filled + "░" * (width - filled)


async def say(text, **kw):
    return await bot.send_message(my_id, text, link_preview=False, **kw)


# ----------------------------- texts -----------------------------
WELCOME = (
    "👋 Welcome, {name}!\n\n"
    "I'm your Channel Tools Assistant. I can:\n"
    "• Copy all content (videos, photos, files, text) from one channel to another, "
    "optionally replacing words in captions and file names.\n"
    "• Create several private channels with the same name in one go.\n\n"
    "Send /copy or /create to begin. I'll guide you step by step.\n"
    "Send /help for the full guide."
)

HELP = (
    "🤖 Channel Tools Assistant — Help\n\n"
    "Commands\n"
    "/copy - Copy content from one channel to another (guided)\n"
    "/create - Create multiple private channels (guided)\n"
    "/status - Live progress of the current task\n"
    "/errors - Detailed list of recent errors\n"
    "/stop - Stop the current task (or discard a draft)\n"
    "/ping - Check that I'm online\n"
    "/about - About this assistant\n"
    "/help - Show this message\n\n"
    "Copying content\n"
    "1. Send /copy.\n"
    "2. Send the source channel (ID, @username, or forward any message from it).\n"
    "3. Send the destination channel the same way.\n"
    "4. Choose where to start: the beginning, a message ID, or resume the last run.\n"
    "5. Optionally add word replacements, then tap Start.\n"
    "Shortcut: /copy <source> <destination> [message id]\n\n"
    "Replacing words (optional)\n"
    "Send one rule per line in the format: old word => new word\n"
    "Example:\n"
    "Lecture => Class\n"
    "@oldchannel => @newchannel\n"
    "Leave the right side empty to remove a word. Matching ignores upper/lower case. "
    "Rules can be applied to captions and text, to file names, or both. "
    "File extensions are never changed. "
    "Renaming a file means it must be downloaded and uploaded again, so those files take longer.\n\n"
    "Creating channels\n"
    "Send /create, then the channel name and the number of channels. "
    "Shortcut: /create <name> <number>\n\n"
    "Channel IDs\n"
    "Private channel IDs start with -100 (for example -1001234567890). "
    "You can also simply forward any message from the channel to me and I'll read the ID.\n\n"
    "Good to know\n"
    "• Your account must be a member of the source and an admin of the destination.\n"
    "• Only one task runs at a time.\n"
    "• A stopped or failed copy can be resumed from where it left off.\n"
    "• Errors are shown in /status together with their error name and code.\n"
    "• This assistant works only for its owner."
)

ABOUT = (
    f"ℹ️ Channel Tools Assistant {VERSION}\n\n"
    "Copies channel content with optional word replacement (captions and file names) "
    "and creates multiple channels. Use /help for instructions."
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
    global dialogs_loaded
    if not dialogs_loaded:
        await client.get_dialogs()
        dialogs_loaded = True
    try:
        return await client.get_entity(ref)
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


# ----------------------------- word replacement -----------------------------
def add_surrogate(text):
    """Python str -> UTF-16 units (Telegram entity offsets are UTF-16 based)."""
    return "".join(
        "".join(chr(u) for u in struct.unpack("<HH", ch.encode("utf-16le")))
        if 0x10000 <= ord(ch) <= 0x10FFFF else ch
        for ch in text
    )


def del_surrogate(text):
    return text.encode("utf-16", "surrogatepass").decode("utf-16")


class Rules:
    """Compiled word-replacement rules (case-insensitive, longest match first)."""

    def __init__(self, pairs):
        self.pairs = [(o, n) for o, n in pairs]
        self.d_p = {o.lower(): n for o, n in self.pairs}
        self.pat_p = re.compile("|".join(re.escape(k) for k in sorted(self.d_p, key=len, reverse=True)), re.I)
        self.d_s = {add_surrogate(o).lower(): add_surrogate(n) for o, n in self.pairs}
        self.pat_s = re.compile("|".join(re.escape(k) for k in sorted(self.d_s, key=len, reverse=True)), re.I)


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


def rename_file(name, rules):
    """Applies the rules to a file name (extension is never changed)."""
    stem, ext = os.path.splitext(name)
    new = rules.pat_p.sub(lambda m: rules.d_p.get(m.group(0).lower(), m.group(0)), stem)
    new = re.sub(r"[\\/\x00]", "_", new)
    new = re.sub(r"\s{2,}", " ", new).strip()
    if not new:
        new = "file"
    return new + ext


# ----------------------------- errors -----------------------------
def record_error(where, e):
    name = e.__class__.__name__
    code = getattr(e, "code", None)
    detail = (str(e) or name).replace("\n", " ")
    job["errors"].append({"where": where, "name": name, "code": code, "detail": detail[:200]})
    if len(job["errors"]) > 500:
        del job["errors"][:100]


def err_lines(limit=5):
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
        total = max(job["top_id"] - job["min_id"], 1)
        pct = 100.0 if phase == "done" else min(100.0, max(0.0, (job["cur_id"] - job["min_id"]) / total * 100))
        lines.append(f"{job['src']} → {job['dst']}\n")
        lines.append(f"{bar(pct)} {pct:.0f}%")
        lines.append(f"Message ID: {job['cur_id']} / {job['top_id']}")
        lines.append(f"Copied: {job['done']} | Skipped: {job['skipped']} | Failed: {job['failed']}")
        eta = ""
        if phase == "running" and pct >= 2 and elapsed > 20:
            eta = f" | ETA: ~{fmt_duration(elapsed * (100 - pct) / pct)}"
        lines.append(f"Elapsed: {fmt_duration(elapsed)}{eta}")
        if job["rules_n"]:
            scope = {"both": "captions & file names", "text": "captions only", "names": "file names only"}[job["scope"]]
            lines.append(f"Replace rules: {job['rules_n']} ({scope})")
    else:
        total = max(job["total"], 1)
        pct = 100.0 if phase == "done" else job["done"] / total * 100
        lines.append(f"Name: {job['name']}\n")
        lines.append(f"{bar(pct)} {pct:.0f}%")
        lines.append(f"Created: {job['done']} / {job['total']}")
        lines.append(f"Elapsed: {fmt_duration(elapsed)}")

    if phase == "running" and job["flood_until"] > time.time():
        lines.append(f"⏳ Telegram asked me to wait {int(job['flood_until'] - time.time())}s (flood wait)")

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


async def send_item(dst, msgs, rules, scope):
    """Sends one message (or one album). Returns True if something was sent."""
    media_msgs = [m for m in msgs
                  if m.media and not isinstance(m.media, (MessageMediaWebPage, MessageMediaPoll))]
    do_text = bool(rules) and scope in ("both", "text")
    do_names = bool(rules) and scope in ("both", "names")

    def caption_of(m):
        if do_text:
            return replace_in_text(m.raw_text or "", m.entities, rules)
        return m.raw_text or "", list(m.entities or [])

    if not media_msgs:
        raw, ents = caption_of(msgs[0])
        if raw.strip():
            await client.send_message(dst, raw, link_preview=False, **ent_kw(ents))
            return True
        return False

    caps = [caption_of(m) for m in media_msgs]
    names = {}
    if do_names:
        for m in media_msgs:
            old = getattr(m.file, "name", None) if m.file else None
            if old:
                new = rename_file(old, rules)
                if new != old:
                    names[m.id] = new

    async def upload(files, streaming):
        extra = {"supports_streaming": True} if streaming else {}
        if len(files) == 1:
            raw, ents = caps[0]
            await client.send_file(dst, files[0], caption=raw, **ent_kw(ents), **extra)
        else:
            any_ents = any(e for _, e in caps)
            if any_ents:
                cap_list = [tl_markdown.unparse(r, e) if e else r for r, e in caps]
                kw = {}
            else:
                cap_list = [r for r, _ in caps]
                kw = {"parse_mode": None}
            await client.send_file(dst, files, caption=cap_list, **kw, **extra)

    tmp = None

    async def download(i, m, rename):
        nonlocal tmp
        if tmp is None:
            tmp = tempfile.mkdtemp(prefix="cp_")
        sub = os.path.join(tmp, str(i))
        os.makedirs(sub, exist_ok=True)
        if rename and m.id in names:
            return await m.download_media(file=os.path.join(sub, names[m.id]))
        return await m.download_media(file=sub + os.sep)

    try:
        try:
            files = []
            for i, m in enumerate(media_msgs):
                files.append(await download(i, m, True) if m.id in names else m.media)
            await upload(files, streaming=bool(names))
            return True
        except FloodWaitError:
            raise
        except Exception:
            pass  # fall back to a full download and re-upload
        files = [await download(i, m, True) for i, m in enumerate(media_msgs)]
        await upload(files, streaming=True)
        return True
    finally:
        if tmp:
            shutil.rmtree(tmp, ignore_errors=True)


async def safe_send(dst, group, rules, scope):
    """Returns (sent: bool, error: Exception or None). Waits out FloodWait automatically."""
    while True:
        try:
            return await send_item(dst, group, rules, scope), None
        except FloodWaitError as e:
            job["flood_until"] = time.time() + e.seconds + 1
            await asyncio.sleep(e.seconds + 1)
            job["flood_until"] = 0.0
        except Exception as e:
            return False, e


async def run_copy(src_ref, dst_ref, min_id, rules, scope):
    job.update(running=True, stop=False, kind="copy", phase="running", started=time.time(),
               last_edit=0.0, src="", dst="", done=0, failed=0, skipped=0, cur_id=min_id,
               min_id=min_id, top_id=min_id, errors=[], flood_until=0.0,
               rules_n=len(rules.pairs) if rules else 0, scope=scope)
    status = None
    try:
        src = await resolve(src_ref, "Source channel")
        dst = await resolve(dst_ref, "Destination channel")
        job.update(src=title_of(src), dst=title_of(dst), key=f"{src.id}->{dst.id}")
        latest = await client.get_messages(src, limit=1)
        job["top_id"] = max(latest[0].id if latest else min_id, min_id)
        status = await say("▶️ Copy started…")
        await refresh(status, force=True)

        group = []

        async def flush():
            nonlocal group
            if not group:
                return
            sent, err = await safe_send(dst, group, rules, scope)
            last_id = group[-1].id
            if err is not None:
                job["failed"] += len(group)
                record_error(f"message ID {last_id}", err)
            elif sent:
                job["done"] += len(group)
            else:
                job["skipped"] += len(group)
            job["cur_id"] = last_id
            save_progress(job["key"], last_id)
            group = []
            await asyncio.sleep(DELAY)
            await refresh(status)

        async for m in client.iter_messages(src, reverse=True, min_id=min_id):
            if job["stop"]:
                break
            if m.action:
                continue
            if m.grouped_id:
                if group and group[0].grouped_id == m.grouped_id:
                    group.append(m)
                    continue
                await flush()
                group = [m]
            else:
                await flush()
                group = [m]
                await flush()
        if not job["stop"]:
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
    job.update(running=True, stop=False, kind="create", phase="running", started=time.time(),
               last_edit=0.0, name=name, done=0, total=count, errors=[], flood_until=0.0, rules_n=0)
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


def start_copy_job():
    pairs = conv["rules"] or []
    rules = Rules(pairs) if pairs else None
    scope = conv["scope"] or "both"
    args = (conv["src"], conv["dst"], conv["start"], rules, scope)
    set_setting("last_src", {"ref": conv["src"], "title": conv["src_title"]})
    set_setting("last_dst", {"ref": conv["dst"], "title": conv["dst_title"]})
    reset_conv()
    job["running"] = True
    asyncio.create_task(run_copy(*args))


def start_create_job():
    name, count = conv["name"], conv["count"]
    reset_conv()
    job["running"] = True
    asyncio.create_task(run_create(name, count))


# ----------------------------- guided flows -----------------------------
async def ask_src():
    conv["step"] = "src"
    last = get_setting("last_src")
    buttons = [[Button.inline(f"Use last source: {last['title'][:35]}", b"src:last")]] if last else None
    await say("📥 Step 1 of 4 — Source channel\n\n"
              "Send the ID or @username of the channel to copy FROM.\n"
              "• Private channel IDs start with -100 (for example -1001234567890).\n"
              "• Or forward any message from that channel to me.\n\n"
              "Send /stop to cancel.", buttons=buttons)


async def ask_dst():
    conv["step"] = "dst"
    last = get_setting("last_dst")
    buttons = [[Button.inline(f"Use last destination: {last['title'][:30]}", b"dst:last")]] if last else None
    await say(f"✅ Source: {conv['src_title']}\n\n"
              "📤 Step 2 of 4 — Destination channel\n\n"
              "Send the ID or @username of the channel to copy TO, or forward any message from it.\n"
              "Your account must be an admin there.", buttons=buttons)


async def ask_start():
    conv["step"] = "start"
    key = f"{conv['src_id']}->{conv['dst_id']}"
    last_id = jload(PROGRESS_FILE).get(key, 0)
    rows = [[Button.inline("⏮ From the beginning", b"start:begin")]]
    if last_id:
        rows.append([Button.inline(f"⏯ Resume (after message ID {last_id})", b"start:resume")])
    await say(f"✅ Destination: {conv['dst_title']}\n\n"
              "📍 Step 3 of 4 — Starting point\n\n"
              "Choose where to start, or send a message ID to start from that message.", buttons=rows)


async def ask_rules():
    conv["step"] = "rules"
    rows = [[Button.inline("⏭ Skip (no changes)", b"rules:skip")]]
    prev = get_setting("rules") or []
    if prev:
        rows.insert(0, [Button.inline(f"♻️ Use previous rules ({len(prev)})", b"rules:prev")])
    await say("✏️ Step 4 of 4 — Replace words (optional)\n\n"
              "I can replace words in captions/text and in file names while copying.\n"
              "Send one rule per line like this:\n\n"
              "old word => new word\n\n"
              "Example:\nLecture => Class\n@oldchannel => @newchannel\n\n"
              "Matching ignores upper/lower case. Leave the right side empty to remove a word. "
              "You can send many rules in one message.", buttons=rows)


async def ask_scope():
    conv["step"] = "scope"
    await say(f"✅ {len(conv['rules'])} rule(s) saved.\n\nWhere should they be applied?",
              buttons=[[Button.inline("📝 Captions & file names", b"scope:both")],
                       [Button.inline("📝 Captions / text only", b"scope:text")],
                       [Button.inline("📁 File names only", b"scope:names")]])


async def show_copy_confirm():
    conv["step"] = "confirm"
    rules = conv["rules"] or []
    scope = {"both": "captions & file names", "text": "captions only", "names": "file names only"}.get(conv["scope"] or "both")
    lines = ["✅ Everything is ready.\n",
             f"Source: {conv['src_title']}",
             f"Destination: {conv['dst_title']}",
             f"Start: {conv['start_label']}"]
    if rules:
        lines.append(f"Replace rules: {len(rules)} ({scope})")
        for o, n in rules[:8]:
            lines.append(f"   {o} → {n if n else '(remove)'}")
        if len(rules) > 8:
            lines.append(f"   …and {len(rules) - 8} more")
    else:
        lines.append("Replace rules: none")
    if rules and conv["scope"] in ("both", "names"):
        lines.append("\nNote: renamed files are downloaded and uploaded again, so they take longer.")
    await say("\n".join(lines), buttons=[[Button.inline("▶️ Start copying", b"go")],
                                         [Button.inline("✖️ Cancel", b"cancel")]])


async def copy_next():
    if conv["src"] is None:
        await ask_src()
    elif conv["dst"] is None:
        await ask_dst()
    elif conv["start"] is None:
        await ask_start()
    elif conv["rules"] is None:
        await ask_rules()
    elif conv["rules"] and conv["scope"] is None:
        await ask_scope()
    else:
        await show_copy_confirm()


async def set_src(ref):
    try:
        ent = await resolve(ref, "Source channel")
    except BadInput as e:
        conv["step"] = "src"
        await say(f"❌ {e}\n\nPlease send the source again, or /stop.")
        return False
    conv.update(src=ref, src_id=ent.id, src_title=title_of(ent))
    return True


async def set_dst(ref):
    try:
        ent = await resolve(ref, "Destination channel")
        if ent.id == conv["src_id"]:
            raise BadInput("The source and destination are the same channel. Please choose a different destination.")
        if isinstance(ent, Channel) and ent.broadcast:
            try:
                perms = await client.get_permissions(ent)
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
    conv["start"] = max(0, n - 1)
    conv["start_label"] = f"message ID {n}" if n > 0 else "the beginning"


async def ask_create_name():
    conv["step"] = "c_name"
    await say("🆕 Step 1 of 2 — Channel name\n\nSend the name to use for all the channels (spaces are fine).\n"
              "Send /stop to cancel.")


async def ask_create_count():
    conv["step"] = "c_count"
    await say(f"✅ Name: {conv['name']}\n\n🔢 Step 2 of 2 — How many?\n\n"
              f"Send the number of channels to create (1 to {MAX_COUNT}).")


async def show_create_confirm():
    conv["step"] = "c_confirm"
    await say(f"✅ Ready.\n\nName: {conv['name']}\nCount: {conv['count']}\n"
              f"Restricted (no forwarding): {'yes' if RESTRICT else 'no'}",
              buttons=[[Button.inline("▶️ Create channels", b"go")], [Button.inline("✖️ Cancel", b"cancel")]])


# ----------------------------- commands -----------------------------
async def cmd_start(arg):
    reset_conv()
    await say(WELCOME.format(name=my_name))


async def cmd_help(arg):
    await say(HELP)


async def cmd_about(arg):
    await say(ABOUT)


async def cmd_ping(arg):
    await say(f"🏓 Pong! I'm online.\nUptime: {fmt_duration(time.time() - started_at)}")


async def cmd_status(arg):
    if job["running"] or job["kind"]:
        await say(render_job())
    elif conv["kind"]:
        await say("📝 A request is in progress. Please continue with the last question, or send /stop to discard it.")
    else:
        await say("✅ No task is running. Send /copy or /create to start one.")


async def cmd_errors(arg):
    errs = job["errors"]
    if not errs:
        await say("✅ No errors recorded.")
        return
    lines = [f"⚠️ {len(errs)} error(s) recorded. Showing the latest {min(len(errs), 15)}:\n"]
    for i, e in enumerate(errs[-15:], start=len(errs) - min(len(errs), 15) + 1):
        c = f" (code {e['code']})" if e["code"] else ""
        lines.append(f"#{i} {e['where']}\n   {e['name']}{c}\n   {e['detail'][:150]}")
    await say("\n".join(lines))


async def cmd_stop(arg):
    if job["running"]:
        job["stop"] = True
        await say("🛑 Stopping after the current item…")
    elif conv["kind"]:
        reset_conv()
        await say("Cancelled. Send /copy or /create whenever you're ready.")
    else:
        await say("There is nothing to stop right now.")


async def cmd_copy(arg):
    if job["running"]:
        await say("⚠️ A task is already running. Send /status to see it or /stop to stop it.")
        return
    reset_conv()
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
    if job["running"]:
        await say("⚠️ A task is already running. Send /status to see it or /stop to stop it.")
        return
    reset_conv()
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
    "/start": cmd_start, "/help": cmd_help, "/copy": cmd_copy, "/create": cmd_create,
    "/status": cmd_status, "/errors": cmd_errors, "/stop": cmd_stop, "/cancel": cmd_stop,
    "/ping": cmd_ping, "/about": cmd_about,
}


# ----------------------------- free text -----------------------------
async def handle_text(event, text):
    step = conv["step"]
    fwd = fwd_channel_id(event.message)

    if step == "src":
        try:
            ref = fwd or check_ref(text.split()[0] if text.split() else "", "Source")
        except BadInput as e:
            await say(f"❌ {e}\n\nSend the channel ID (-100...) or @username, forward a message from it, or /stop.")
            return
        if await set_src(ref):
            await copy_next()
    elif step == "dst":
        try:
            ref = fwd or check_ref(text.split()[0] if text.split() else "", "Destination")
        except BadInput as e:
            await say(f"❌ {e}\n\nSend the channel ID (-100...) or @username, forward a message from it, or /stop.")
            return
        if await set_dst(ref):
            await copy_next()
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
                      "\n\nUse the format: old word => new word\nSend all rules again, or tap Skip.")
            return
        if not pairs:
            await say("I couldn't find any rule. Use the format: old word => new word, one per line, or tap Skip.")
            return
        if len(pairs) > 100:
            await say("❌ Please send at most 100 rules.")
            return
        conv["rules"] = pairs
        set_setting("rules", pairs)
        await copy_next()
    elif step in ("scope", "confirm", "c_confirm"):
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
            await say(f"Hello, {my_name}! 👋 How can I help you today?\nSend /copy to copy a channel, /create to create channels, or /help.")
        elif THANKS_RE.match(text):
            await say("You're welcome! Let me know if you need anything else.")
        else:
            await say("I can copy content between channels and create channels.\nSend /copy or /create to begin, or /help for all commands.")


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
    if event.sender_id != my_id:
        await event.respond(
            "🔒 This is a private assistant and can only be used by its owner.\n\n"
            f"Your Telegram ID: {event.sender_id}\n"
            "If this is your own bot and you still see this message, your SESSION_STRING belongs to a "
            "different account than the one you are chatting from. Generate a new one with that account.",
            parse_mode=None)
        return
    try:
        await handle_message(event)
    except Exception as e:
        print(f"Handler error: {e}")
        await say(f"⚠️ Something went wrong: {e}")


@bot.on(events.CallbackQuery)
async def on_button(event):
    if event.sender_id != my_id:
        await event.answer("This assistant is private.", alert=True)
        return
    data = (event.data or b"").decode()
    step = conv["step"]

    async def expired():
        await event.answer("This request has expired. Please start again.", alert=True)

    try:
        if data == "cancel":
            await event.answer()
            reset_conv()
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
        elif data in ("start:begin", "start:resume") and step == "start":
            await event.answer()
            if data == "start:begin":
                conv.update(start=0, start_label="the beginning")
            else:
                key = f"{conv['src_id']}->{conv['dst_id']}"
                last_id = jload(PROGRESS_FILE).get(key, 0)
                conv.update(start=last_id, start_label=f"resuming after message ID {last_id}")
            await event.edit(f"Start: {conv['start_label']}")
            await copy_next()
        elif data in ("rules:skip", "rules:prev") and step == "rules":
            await event.answer()
            if data == "rules:skip":
                conv["rules"] = []
                await event.edit("Word replacement: skipped")
            else:
                prev = [tuple(p) for p in (get_setting("rules") or [])]
                conv["rules"] = prev
                await event.edit(f"Using the previous {len(prev)} rule(s)")
            await copy_next()
        elif data.startswith("scope:") and step == "scope":
            await event.answer()
            conv["scope"] = data.split(":", 1)[1]
            await event.edit("Rules applied to: " + {"both": "captions & file names", "text": "captions only",
                                                      "names": "file names only"}[conv["scope"]])
            await copy_next()
        elif data == "go" and step in ("confirm", "c_confirm"):
            if job["running"]:
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
async def main():
    global my_id, my_name
    await client.connect()
    if not await client.is_user_authorized():
        print("ERROR: SESSION_STRING is invalid or expired. Generate a new one with generate_session.py.")
        sys.exit(1)
    me = await client.get_me()
    my_id = me.id
    my_name = me.first_name or "there"

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
                BotCommand("copy", "Copy a channel (guided)"),
                BotCommand("create", "Create multiple channels"),
                BotCommand("status", "Live progress"),
                BotCommand("errors", "Detailed error list"),
                BotCommand("stop", "Stop the current task"),
                BotCommand("ping", "Check that the bot is online"),
                BotCommand("about", "About this assistant"),
                BotCommand("help", "How to use this assistant"),
            ]))
    except Exception as e:
        print(f"Could not set the command menu: {e}")

    print(f"Channel Tools Assistant {VERSION} is running. Account: {my_name} | Bot: @{bme.username}")
    print(f"Owner account ID: {my_id}")
    try:
        await bot.send_message(my_id, "✅ Channel Tools Assistant is online. Send /copy or /create to begin, or /help.",
                               parse_mode=None)
    except Exception as e:
        print(f"Startup message not sent (open the bot and press Start): {e}")

    await asyncio.gather(client.run_until_disconnected(), bot.run_until_disconnected())


client.loop.run_until_complete(main())
