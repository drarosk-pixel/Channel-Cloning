# Channel-Cloning

A multi-user Telegram assistant bot. Users chat with one bot (created via
[@BotFather](https://t.me/BotFather)) and log in with their **own** Telegram
account using `/login`. The bot then works on each user's behalf to:

- **Copy** all content (videos, photos, files, text) from one channel to another,
  with optional extras:
  - **Replace words** in captions/text and/or file names.
  - **Replace links** — rewrite the hidden URL inside hyperlinks (e.g.
    `t.me/oldchannel` → `t.me/newchannel`).
  - **Custom thumbnail** — upload a photo to use as the thumbnail for copied
    videos/documents.
  - **Caption / promo link** — add extra text (and a clickable promo link) to
    every caption, appended or prepended.
  - **Live speed** — `/status` shows transfer speed and messages/min with a
    progress bar and ETA.
- **Create** several private channels with the same name in one go.

Earlier versions needed a single `SESSION_STRING` so only the owner could use the
bot. This version removes that: anyone can log in and use it with their own account.

## Setup

1. Get `API_ID` and `API_HASH` from [my.telegram.org](https://my.telegram.org).
2. Create a bot with [@BotFather](https://t.me/BotFather) and get its `BOT_TOKEN`.
3. Set the environment variables and run the worker.

### Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `API_ID` | yes | — | From my.telegram.org (used to log in every user) |
| `API_HASH` | yes | — | From my.telegram.org |
| `BOT_TOKEN` | yes | — | From @BotFather |
| `RESTRICT` | no | `1` | Created channels block forwarding |
| `MAX_COUNT` | no | `50` | Max channels per `/create` |
| `CREATE_DELAY` | no | `3` | Seconds between created channels |
| `DELAY` | no | `1.5` | Seconds between copied messages |
| `EDIT_EVERY` | no | `4` | Seconds between live status updates |
| `SESSIONS_FILE` | no | `sessions.json` | Where logged-in sessions are stored |
| `DL_SESSIONS_FILE` | no | `dl_sessions.json` | Where download-account sessions are stored (see Download account) |
| `THUMB_DIR` | no | `thumbs` | Folder for per-user custom thumbnails |
| `DOWNLOAD_DIR` | no | `./dl_tmp` | Where big files are downloaded temporarily. Keep it on large storage, not `/tmp` |
| `USE_FAST` | no | `1` | Parallel multi-connection **upload** (fast uploads). Set `0` to disable |
| `USE_FAST_DL` | no | `1` | Parallel **download** via FastTelethon. Set `0` for plain download |
| `USE_FAST_DL2` | no | `1` | Same, but for the separate download account (its speed limits are its own) |
| `USE_DL_ACCOUNT` | no | `1` | `0` keeps the download account logged in but stops using it, so the two accounts' download speeds can be compared without logging out |
| `STALL_SECONDS` | no | `180` | Abort and retry a transfer if it makes no progress for this long (dead connection) |
| `PIPELINE` | no | `0` | `1` = overlap download+upload (~2x, more connections). `0` = one at a time (stable) |
| `FAST_UPLOAD_MB` | no | `5` | When `USE_FAST=1`, files larger than this use parallel transfer |
| `FT_CONNECTIONS` | no | `8` | Parallel connections per transfer (lower = more stable on small hosts) |
| `PROGRESS_FILE` | no | `progress.json` | Per-user copy progress |
| `SETTINGS_FILE` | no | `settings.json` | Per-user saved settings |

`SESSION_STRING` is no longer used.

### Run

```bash
pip install -r requirements.txt
python bot.py
```

Then open your bot in Telegram, press **Start**, and send `/login`.

## Logging in

1. Send `/login`.
2. Send your phone number in international format, e.g. `+919876543210`.
3. Enter the login code Telegram sends you. **Type it with spaces or dashes
   between the digits** (e.g. `1 2 3 4 5`) so Telegram doesn't block the code.
4. If you use two-step verification, enter your password.

Logging in lets the bot act on your behalf, so only use a bot you trust.

## Download account (second account for downloading)

Telegram applies its download speed limit **per account**. A throttled account
downloads at roughly 1 MB/s no matter what the code does, and parallel download
on such an account just collects `GetFileRequest` flood waits.

`/download_account` logs in a second account that does only the downloading.
Each file is then fetched by that account and uploaded by the main account, so a
copy runs at the faster account's download speed. Nothing else changes: the
source/destination pickers, topic handling, extras, progress and resume all still
use the main account.

- The download account must be a **member of every source channel** you copy
  from — it is the one fetching the files. It never uploads, posts or creates
  anything.
- If it cannot see a source channel, or cannot fetch one particular message, the
  main account downloads that file as before and `/status` says so. A copy never
  fails or skips content because of this.
- `/download_account` again shows which account is in use (and whether it is
  Premium); `/remove_download_account` goes back to one account.
- To compare the two accounts' download speed, set `USE_DL_ACCOUNT=0` and restart:
  the login is kept but the main account does the downloading. Telegram rate-limits
  logins, so this avoids logging out and back in for a measurement.
- Message objects can't be shared between accounts (file references and access
  hashes belong to the account that fetched them), so the download account
  re-fetches each message by ID from its own view of the channel.

## Commands

`/login`, `/logout`, `/download_account`, `/remove_download_account`, `/copy`,
`/create`, `/status`, `/errors`, `/stop`, `/ping`, `/about`, `/help`.

Shortcuts: `/copy <source> <destination> [message id]` and `/create <name> <number>`.

## Privacy note

Logged-in session strings are stored in `SESSIONS_FILE`, and download-account
sessions in `DL_SESSIONS_FILE`. A session string grants full access to that
account, so keep both files private and use persistent storage on your host so
users stay logged in across restarts.
