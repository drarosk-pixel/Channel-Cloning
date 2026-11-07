CHANNEL TOOLS ASSISTANT v8

Everyone chats with one normal Telegram bot (created with @BotFather). Each user
logs in with their OWN Telegram account using /login, and from then on the bot
works on that user's behalf. There is no shared SESSION_STRING anymore — the bot
is multi-user.

ENVIRONMENT VARIABLES
  Required: API_ID, API_HASH, BOT_TOKEN
  Optional: RESTRICT (1), MAX_COUNT (50), CREATE_DELAY (3), DELAY (1.5), EDIT_EVERY (4)
            PROGRESS_FILE, SETTINGS_FILE, SESSIONS_FILE (storage paths)

  The same API_ID / API_HASH (from my.telegram.org) is used to log in every user.
  SESSION_STRING is no longer required.

START: python bot.py (worker). Then open your bot in Telegram, press Start, and
send /login to connect your account. After that, send /help.

COMMANDS
  /start     Welcome message
  /help      Full guide
  /login     Connect your Telegram account to the bot (phone -> code -> optional 2FA)
  /logout    Disconnect your account and remove its saved session
  /copy      Copy a channel to another channel (guided: source, destination,
             starting point and optional word replacement)
  /create    Create multiple private channels (guided)
  /status    Live progress bar, counts, ETA and errors (your own task)
  /errors    Detailed list of your recent errors (error name + code + message ID)
  /stop      Stop your current task (or discard a draft)
  /ping, /about

SHORTCUTS
  /copy <source> <destination> [message id]
  /create <name> <number>

LOGGING IN
  1. Send /login.
  2. Send your phone number in international format, e.g. +919876543210.
  3. Enter the login code Telegram sends you. Type it with spaces or dashes
     between the digits (e.g. 1 2 3 4 5) so Telegram does not block the code.
  4. If you have two-step verification, enter your password.
  Logging in lets the bot act on your behalf, so only use a bot you trust.

WORD REPLACEMENT (during /copy)
  One rule per line:   old word => new word
  - Works on captions/text and/or file names (you choose).
  - Case-insensitive. Empty right side removes the word.
  - File extensions are never changed.
  - Renamed files are downloaded and uploaded again, so they take longer.
  - Previous rules can be reused with one tap.

TIPS
  - You can forward any message from a channel instead of typing its ID.
  - A stopped or failed copy can be resumed (choose "Resume" at the start step).
  - Each user has their own tasks, progress and saved settings.

STORAGE / PRIVACY
  Logged-in session strings are stored in SESSIONS_FILE (default sessions.json).
  A session string grants full access to that account, so keep this file private
  and use persistent storage on your host so users stay logged in across restarts.
