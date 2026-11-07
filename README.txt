CHANNEL TOOLS ASSISTANT v7

You chat with a normal Telegram bot (created with @BotFather). Your own account
(SESSION_STRING) does the real work behind the scenes. Only that account can use the bot.

ENVIRONMENT VARIABLES
  Required: API_ID, API_HASH, SESSION_STRING, BOT_TOKEN
  Optional: RESTRICT (1), MAX_COUNT (50), CREATE_DELAY (3), DELAY (1.5), EDIT_EVERY (4)

START: python bot.py (worker). Then open your bot in Telegram, press Start, send /help.

COMMANDS
  /start     Welcome message
  /help      Full guide
  /copy      Copy a channel to another channel (guided: the bot asks for source,
             destination, starting point and optional word replacement)
  /create    Create multiple private channels (guided)
  /status    Live progress bar, counts, ETA and errors
  /errors    Detailed list of recent errors (error name + code + message ID)
  /stop      Stop the current task (or discard a draft)
  /ping, /about

SHORTCUTS
  /copy <source> <destination> [message id]
  /create <name> <number>

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
