# fullwhssstask bot — Render deployment

A Telegram relay bot (Bot API + Telethon user account) packaged for Render as a
**Background Worker** with a persistent disk.

## What changed vs the original script

- **Secrets** (`BOT_TOKEN`, `API_ID`, `API_HASH`, `PHONE`, `ADMIN_CHAT_ID`,
  `TELETHON_SESSION_B64`) are now read from environment variables.
- All persistent JSON files (`balances.json`, `wallets.json`, `invites.json`,
  `users.json`, `history.json`, `queue.json`) are written to `DATA_DIR`
  (defaults to `/var/data` on Render).
- The Telethon `.session` file lives in `DATA_DIR` and can be pre-baked from a
  base64 env var so the worker can boot without an interactive code prompt.
- A friendly error message is shown if the MTProto login fails on first boot
  (instead of a silent crash).

Nothing was removed — every command, callback, queue behaviour, watchdog,
reward calculation, BEP20 wallet flow, admin tools, test-mode simulator, and
button relay logic is preserved.

## File layout

```
fullwhssstask.py     # bot code (entry point)
requirements.txt     # python-telegram-bot, telethon, Pillow
render.yaml          # Render service definition
runtime.txt          # Python version pin
.gitignore           # exclude .session and JSON state
```

## One-time: bake the Telethon session

Telethon's `.session` is created on the first interactive login, which a
headless worker can't do. Generate it locally and ship it as an env var.

```bash
# 1. Clone the repo, install requirements
pip install -r requirements.txt

# 2. Run the script once locally with TEST_MODE=true so it doesn't need a session
TEST_MODE=true BOT_TOKEN=xxx API_ID=xxx API_HASH=xxx PHONE=+xxx \
  python fullwhssstask.py
# (Ctrl+C after the menu appears)

# 3. In a separate Python shell, create the session interactively
python -c "
from telethon import TelegramClient
import asyncio
async def go():
    c = TelegramClient('relay_session', 37721239, 'fb79a93b97dbc31fc97fa33ae3df9f59')
    await c.start(phone='+251909259439')
    print('logged in')
    await c.disconnect()
asyncio.run(go())
"

# 4. Base64-encode the resulting .session file
base64 -w0 relay_session.session > relay_session.session.b64
cat relay_session.session.b64
```

Paste that long base64 string into the `TELETHON_SESSION_B64` env var on
Render. The worker decodes it into `/var/data/relay_session.session` on boot.

## Render setup

1. Push this folder to a GitHub repo.
2. Render dashboard → **New +** → **Blueprint** → point at the repo.
   Render reads `render.yaml` and provisions:
   - a **Background Worker** (`starter` plan, $7/mo, no sleep)
   - a **Persistent Disk** mounted at `/var/data` (1 GB, $1/mo)
3. In the service's **Environment** tab, fill in the `sync: false` vars:
   - `BOT_TOKEN`
   - `API_ID`
   - `API_HASH`
   - `PHONE`
   - `ADMIN_CHAT_ID`
   - `TELETHON_SESSION_B64` (from the step above)
4. **Manual deploy** once env vars are set.

## Free vs paid

| Plan         | Behaviour                                                                |
|--------------|--------------------------------------------------------------------------|
| Free         | Sleeps after 15 min idle, cold-start wipe of local disk. **Not usable** for this bot. |
| Starter $7   | Always on, persistent disk survives deploys, recommended.                 |

## Local quick test (no Telegram)

```bash
TEST_MODE=true BOT_TOKEN=000:fake API_ID=1 API_HASH=fake PHONE=+10000000000 \
  python fullwhssstask.py
```

The simulator inside the script walks the full flow with fake provider
messages, so you can exercise the queue, watchdogs, balance, and wallet paths
without touching Telegram.
