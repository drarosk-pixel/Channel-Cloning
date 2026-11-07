# Do not run this on your hosting. Run it once on your laptop or in Termux.
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

api_id = int(input("API_ID: "))
api_hash = input("API_HASH: ").strip()

with TelegramClient(StringSession(), api_id, api_hash) as client:
    print("\nThis is your SESSION_STRING (never share it with anyone):\n")
    print(client.session.save())
