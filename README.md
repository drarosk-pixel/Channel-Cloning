# Channel-Cloning

A multi-user Telegram assistant bot. Users chat with one bot (created via
[@BotFather](https://t.me/BotFather)) and log in with their **own** Telegram
account using `/login`. The bot then works on each user's behalf to:

- **Copy** all content (videos, photos, files, text) from one channel to another,
  optionally replacing words in captions and file names.
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

## Commands

`/login`, `/logout`, `/copy`, `/create`, `/status`, `/errors`, `/stop`,
`/ping`, `/about`, `/help`.

Shortcuts: `/copy <source> <destination> [message id]` and `/create <name> <number>`.

## Privacy note

Logged-in session strings are stored in `SESSIONS_FILE`. A session string grants
full access to that account, so keep the file private and use persistent storage
on your host so users stay logged in across restarts.
