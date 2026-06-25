# pip install python-telegram-bot telethon
import asyncio
import base64
import io
import json
import os
import re
import logging
import time
from telethon import TelegramClient, events
from telethon.tl.functions.messages import GetBotCallbackAnswerRequest
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

# -------------------------------------------------------------------
#  Render-friendly configuration
#  - Secrets come from environment variables
#  - All persistent files live under DATA_DIR (use a Render Disk)
#  - Telethon session can be supplied pre-baked as base64 via env
# -------------------------------------------------------------------
def _env(name, default=None, required=False, cast=str):
    val = os.environ.get(name, default)
    if required and (val is None or val == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    if val is None:
        return None
    try:
        return cast(val)
    except Exception:
        raise RuntimeError(f"Environment variable {name} has an invalid value")

def _env_bool(name, default=False):
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")

# Data directory – on Render, mount a Persistent Disk at /var/data
DATA_DIR = _env("DATA_DIR", "/var/data" if os.path.isdir("/var/data") else ".")

# Make sure the directory exists so JSON writes don't crash
try:
    os.makedirs(DATA_DIR, exist_ok=True)
except Exception:
    # fall back to local dir if a permission issue shows up
    DATA_DIR = "."
    os.makedirs(DATA_DIR, exist_ok=True)

CONFIG = {
    "BOT_TOKEN":       _env("BOT_TOKEN", required=True),
    "API_ID":          _env("API_ID", required=True, cast=int),
    "API_HASH":        _env("API_HASH", required=True),
    "PHONE":           _env("PHONE", required=True),
    "SESSION_NAME":    _env("SESSION_NAME", "relay_session"),
    "PROV_BOT":        _env("PROV_BOT", "WStaskbot"),
    "TIMEOUT_SEC":     _env("TIMEOUT_SEC", 90, cast=int),
    "ADMIN_CHAT_ID":   _env("ADMIN_CHAT_ID", 8453713398, cast=int),
    "REWARD_PER_MESSAGE": _env("REWARD_PER_MESSAGE", 0.02, cast=float),
    "MIN_WITHDRAW":    _env("MIN_WITHDRAW", 0.5, cast=float),
    "BALANCE_FILE":    os.path.join(DATA_DIR, "balances.json"),
    "WALLET_FILE":     os.path.join(DATA_DIR, "wallets.json"),
    "INVITE_FILE":     os.path.join(DATA_DIR, "invites.json"),
    "USERS_FILE":      os.path.join(DATA_DIR, "users.json"),
    "HISTORY_FILE":    os.path.join(DATA_DIR, "history.json"),
    "QUEUE_FILE":      os.path.join(DATA_DIR, "queue.json"),
    "SESSION_FILE":    os.path.join(DATA_DIR, _env("SESSION_NAME", "relay_session") + ".session"),
    "RETRY_MAX":       _env("RETRY_MAX", 3, cast=int),
    "BOT_USERNAME":    _env("BOT_USERNAME", "Whstask_bot"),
    "TEST_MODE":       _env_bool("TEST_MODE", False),
    # Optional: pre-baked Telethon session as base64 string
    "TELETHON_SESSION_B64": _env("TELETHON_SESSION_B64", None),
}

# -------------------------------------------------------------------
#  Custom cancellable queue with persistence & reserve_uid
# -------------------------------------------------------------------
class CancellableQueue:
    def __init__(self):
        self._queue = asyncio.Queue()
        self._items = {}
        self._counter = 0
        self._loaded = False

    def reserve_uid(self) -> int:
        self._counter += 1
        return self._counter

    async def put(self, item: dict, uid: int = None) -> int:
        if uid is None:
            uid = self.reserve_uid()
        item["_queue_id"] = uid
        item["_cancelled"] = False
        self._items[uid] = item
        await self._queue.put(uid)
        self._save()
        return uid

    async def get(self) -> dict:
        while True:
            uid = await self._queue.get()
            item = self._items.pop(uid, None)
            if item is None:
                continue
            if item.get("_cancelled"):
                continue
            self._save()
            return item

    def cancel_by_user(self, chat_id: int) -> list:
        cancelled_ids = []
        for uid, item in list(self._items.items()):
            if item.get("chat_id") == chat_id and not item.get("_cancelled"):
                item["_cancelled"] = True
                cancelled_ids.append(uid)
        if cancelled_ids:
            self._save()
        return cancelled_ids

    def cancel_by_uid(self, uid: int):
        if uid in self._items:
            self._items[uid]["_cancelled"] = True
            self._save()

    def has_pending(self, chat_id: int) -> bool:
        for item in self._items.values():
            if item.get("chat_id") == chat_id and not item.get("_cancelled"):
                return True
        return False

    def qsize(self) -> int:
        return sum(1 for item in self._items.values() if not item["_cancelled"])

    async def task_done(self):
        self._queue.task_done()

    def _save(self):
        data = []
        for uid, item in self._items.items():
            if not item.get("_cancelled"):
                data.append({
                    "_queue_id": item["_queue_id"],
                    "chat_id": item["chat_id"],
                    "number": item["number"],
                    "_notif_msg_id": item.get("_notif_msg_id")
                })
        save_json(CONFIG["QUEUE_FILE"], {"counter": self._counter, "items": data})

    def load(self):
        if self._loaded:
            return
        self._loaded = True
        saved = load_json(CONFIG["QUEUE_FILE"])
        self._counter = saved.get("counter", 0)
        for item_data in saved.get("items", []):
            uid = item_data["_queue_id"]
            item = {
                "chat_id": item_data["chat_id"],
                "number": item_data["number"],
                "_queue_id": uid,
                "_cancelled": False,
                "_notif_msg_id": item_data.get("_notif_msg_id")
            }
            self._items[uid] = item
            self._queue.put_nowait(uid)
        if self._items:
            log.info(f"Loaded {len(self._items)} queued requests from disk.")

request_queue = CancellableQueue()
active_request = None
callback_store = {}
cb_counter = 0
stop_event = asyncio.Event()
user_keyboard_msg = {}       # user_id -> message_id of the keyboard update message
last_relayed = {}            # user_id -> msg_map from the last completed task
user_warning_msg = {}        # user_id -> message_id of the "already busy" warning

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger("RelayBot")
client = TelegramClient(CONFIG["SESSION_FILE"], CONFIG["API_ID"], CONFIG["API_HASH"])

# -------------------------------------------------------------------
#  Persistent storage
# -------------------------------------------------------------------
def load_json(path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

balances = load_json(CONFIG["BALANCE_FILE"])
wallets = load_json(CONFIG["WALLET_FILE"])
invites = load_json(CONFIG["INVITE_FILE"])
users = load_json(CONFIG["USERS_FILE"])
history = load_json(CONFIG["HISTORY_FILE"])

# -------------------------------------------------------------------
#  Telethon session bootstrap
#  - If TELETHON_SESSION_B64 is provided, decode it into the session file
#  - Otherwise the first local run will need an interactive login
# -------------------------------------------------------------------
def materialize_telethon_session():
    b64 = CONFIG.get("TELETHON_SESSION_B64")
    if not b64:
        return
    try:
        raw = base64.b64decode(b64)
        with open(CONFIG["SESSION_FILE"], "wb") as f:
            f.write(raw)
        log.info(f"✅ Telethon session materialised at {CONFIG['SESSION_FILE']}")
    except Exception as e:
        log.error(f"Failed to materialise Telethon session: {e}")

def add_user(uid: int, bot=None):
    key = str(uid)
    is_new = key not in users
    if is_new:
        users[key] = True
        save_json(CONFIG["USERS_FILE"], users)
        if bot:
            asyncio.create_task(notify_admin_new_user(uid, bot))

async def notify_admin_new_user(uid: int, bot):
    try:
        user_info = await bot.get_chat(uid)
        username = f"@{user_info.username}" if user_info.username else "no username"
        name = user_info.full_name
        text = f"🆕 New user started the bot!\nID: {uid}\nName: {name}\nUsername: {username}"
        await bot.send_message(CONFIG["ADMIN_CHAT_ID"], text)
    except Exception:
        await bot.send_message(CONFIG["ADMIN_CHAT_ID"], f"🆕 New user (ID: {uid}) started the bot.")

def record_transaction(uid: int, amount: float, txn_type: str):
    key = str(uid)
    entry = {
        "type": txn_type,
        "amount": amount,
        "time": int(time.time())
    }
    if key not in history:
        history[key] = []
    history[key].append(entry)
    if len(history[key]) > 50:
        history[key] = history[key][-50:]
    save_json(CONFIG["HISTORY_FILE"], history)

def get_balance(uid: int) -> float:
    return balances.get(str(uid), 0.0)

def add_balance(uid: int, amount: float, txn_type: str = None):
    key = str(uid)
    balances[key] = balances.get(key, 0.0) + amount
    save_json(CONFIG["BALANCE_FILE"], balances)
    if txn_type:
        record_transaction(uid, amount, txn_type)

def withdraw_balance(uid: int, amount: float) -> bool:
    key = str(uid)
    if balances.get(key, 0.0) >= amount:
        balances[key] -= amount
        save_json(CONFIG["BALANCE_FILE"], balances)
        record_transaction(uid, -amount, "withdraw")
        return True
    return False

def get_wallet(uid: int) -> str:
    return wallets.get(str(uid), "")

def set_wallet(uid: int, address: str):
    wallets[str(uid)] = address
    save_json(CONFIG["WALLET_FILE"], wallets)

# -------------------------------------------------------------------
#  Helper to check if a user is blocked (active or queued)
# -------------------------------------------------------------------
def user_has_pending_request(uid: int) -> bool:
    if active_request and active_request.get("active") and active_request["chat_id"] == uid:
        return True
    return request_queue.has_pending(uid)

# -------------------------------------------------------------------
#  Helper to delete a warning message if it exists
# -------------------------------------------------------------------
async def clear_warning_message(chat_id: int, bot):
    msg_id = user_warning_msg.pop(chat_id, None)
    if msg_id:
        try:
            await bot.delete_message(chat_id, msg_id)
        except Exception:
            pass

# -------------------------------------------------------------------
#  Button mapping
# -------------------------------------------------------------------
def encode_buttons(prov_msg):
    global cb_counter
    if not prov_msg.buttons:
        return None
    keyboard = []
    for row_idx, row in enumerate(prov_msg.buttons):
        api_row = []
        for btn_idx, btn in enumerate(row):
            try:
                if hasattr(btn, 'data') and btn.data:
                    cb_id = f"pb_{cb_counter}"
                    btn_text = getattr(btn, 'text', f"Btn {cb_counter}")
                    callback_store[cb_id] = {
                        "msg_id": prov_msg.id,
                        "row": row_idx,
                        "col": btn_idx,
                        "data": btn.data,
                        "text": btn_text
                    }
                    api_row.append(InlineKeyboardButton(btn_text, callback_data=cb_id))
                    cb_counter += 1
                elif hasattr(btn, 'url') and btn.url:
                    btn_text = getattr(btn, 'text', 'Link')
                    api_row.append(InlineKeyboardButton(btn_text, url=btn.url))
            except Exception:
                continue
        if api_row:
            keyboard.append(api_row)
    return InlineKeyboardMarkup(keyboard) if keyboard else None

# -------------------------------------------------------------------
#  Reward calculation & caption replacement
# -------------------------------------------------------------------
REWARD_PATTERN = re.compile(r"Total successfully sent:\s*(\d+)", re.IGNORECASE)

def compute_reward(text):
    match = REWARD_PATTERN.search(text)
    if not match:
        return 0.0
    return int(match.group(1)) * CONFIG["REWARD_PER_MESSAGE"]

def replace_earnings(text, reward):
    lines = text.split('\n')
    new_lines = []
    for line in lines:
        if "earnings for this task" in line.lower() or "💵 Earnings" in line:
            continue
        new_lines.append(line)
    new_lines.append(f"💵 Earnings for this task: {reward:.4f} USD")
    return '\n'.join(new_lines)

def get_processed_caption(raw):
    reward = compute_reward(raw)
    return replace_earnings(raw, reward)

def should_show_own_reward(text):
    """Return True if the message contains the sent count (so we should always show our reward)."""
    if not text:
        return False
    return "Total successfully sent" in text or "Sending Task Completed" in text

# -------------------------------------------------------------------
#  Relaying – always show our own reward, add balance only on final
# -------------------------------------------------------------------
async def relay_message(msg, chat_id, bot, edit_user_msg_id=None, edit_was_photo=False, task_completed=False):
    raw_caption = msg.text or msg.raw_text or ""
    # Always show our own reward if the message contains the sent count
    if should_show_own_reward(raw_caption):
        caption = get_processed_caption(raw_caption)
        if task_completed:
            reward = compute_reward(raw_caption)
            if reward > 0:
                add_balance(chat_id, reward, "task")
                log.info(f"Added ${reward:.4f} to user {chat_id}")
    else:
        caption = raw_caption

    reply_markup = encode_buttons(msg)
    was_photo = bool(msg.photo)

    if edit_user_msg_id is not None and was_photo != edit_was_photo:
        edit_user_msg_id = None

    try:
        if edit_user_msg_id:
            if was_photo:
                try:
                    await bot.edit_message_caption(
                        chat_id=chat_id,
                        message_id=edit_user_msg_id,
                        caption=caption,
                        reply_markup=reply_markup
                    )
                    return True, edit_user_msg_id
                except Exception as edit_err:
                    if "Message is not modified" in str(edit_err):
                        return True, edit_user_msg_id
                    log.warning(f"Caption edit failed, sending new: {edit_err}")
                    edit_user_msg_id = None
            else:
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=edit_user_msg_id,
                        text=caption,
                        reply_markup=reply_markup
                    )
                    return False, edit_user_msg_id
                except Exception as edit_err:
                    if "Message is not modified" in str(edit_err):
                        return False, edit_user_msg_id
                    log.warning(f"Text edit failed, sending new: {edit_err}")
                    edit_user_msg_id = None

        if was_photo:
            buf = io.BytesIO()
            await msg.download_media(file=buf)
            buf.seek(0)
            sent = await bot.send_photo(chat_id, buf, caption=caption, reply_markup=reply_markup)
            return True, sent.message_id
        elif msg.media:
            buf = io.BytesIO()
            await msg.download_media(file=buf)
            buf.seek(0)
            sent = await bot.send_document(chat_id, buf, caption=caption, reply_markup=reply_markup)
            return False, sent.message_id
        else:
            sent = await bot.send_message(chat_id, caption, reply_markup=reply_markup)
            return False, sent.message_id

    except Exception as e:
        log.error(f"Relay error: {e}")
        await bot.send_message(chat_id, f"Error: {e}")
        return False, None

# -------------------------------------------------------------------
#  Provider cancel click helper (used on timeout)
# -------------------------------------------------------------------
async def click_provider_cancel(req):
    for entry in callback_store.values():
        if "cancel" in entry.get("text", "").lower():
            try:
                await client(GetBotCallbackAnswerRequest(
                    peer=CONFIG["PROV_BOT"],
                    msg_id=entry["msg_id"],
                    data=entry["data"]
                ))
                log.info("Provider cancel button clicked on timeout.")
                return True
            except Exception as e:
                log.warning(f"Failed to click provider cancel button: {e}")
    return False

# -------------------------------------------------------------------
#  Delete all relayed messages from the user's chat
# -------------------------------------------------------------------
async def delete_relayed_messages(msg_map, bot, chat_id):
    if not msg_map:
        return
    for info in msg_map.values():
        msg_id = info.get("msg_id")
        if msg_id:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=msg_id)
            except Exception:
                pass

# -------------------------------------------------------------------
#  Cancel helpers
# -------------------------------------------------------------------
async def timeout_handler(req):
    if req and req.get("active") and not req.get("was_cancelled_by_provider"):
        await click_provider_cancel(req)
        await delete_relayed_messages(req.get("msg_map"), req["bot"], req["chat_id"])
        await req["bot"].send_message(req["chat_id"], "⏱️ Timeout – request cancelled.")
    if req and req.get("active"):
        req["completed"] = True
        req["was_cancelled"] = True
        req["completed_event"].set()

async def cancel_user_request(user_id, bot):
    global active_request, last_relayed

    if active_request and active_request.get("active") and active_request["chat_id"] == user_id:
        cur = active_request
        await delete_relayed_messages(cur.get("msg_map"), cur["bot"], cur["chat_id"])
        cur["completed"] = True
        cur["was_cancelled"] = True
        cur["completed_event"].set()
        await clear_warning_message(user_id, bot)
        await update_user_keyboard(user_id, bot, with_cancel=False, text="Select an action from the menu.")
        return None

    cancelled_ids = request_queue.cancel_by_user(user_id)
    if cancelled_ids:
        for uid in cancelled_ids:
            item = request_queue._items.get(uid)
            if item and item.get("_notif_msg_id"):
                try:
                    await bot.delete_message(chat_id=item["chat_id"], message_id=item["_notif_msg_id"])
                except Exception:
                    pass
        await clear_warning_message(user_id, bot)
        await update_user_keyboard(user_id, bot, with_cancel=False, text="Select an action from the menu.")
        return None

    return "⚠️ No active or queued request to cancel."

# -------------------------------------------------------------------
#  Dynamic keyboard management
# -------------------------------------------------------------------
def get_keyboard(with_cancel=False):
    if with_cancel:
        return ReplyKeyboardMarkup(
            [
                [KeyboardButton("➕ Add"), KeyboardButton("❌ Cancel")],
                [KeyboardButton("💰 Balance"), KeyboardButton("🏦 Withdraw")],
                [KeyboardButton("❓ FAQ"), KeyboardButton("📨 Invite")],
                [KeyboardButton("⚙️ Setup"), KeyboardButton("📊 History")]
            ],
            resize_keyboard=True, one_time_keyboard=False
        )
    else:
        return ReplyKeyboardMarkup(
            [
                [KeyboardButton("➕ Add")],
                [KeyboardButton("💰 Balance"), KeyboardButton("🏦 Withdraw")],
                [KeyboardButton("❓ FAQ"), KeyboardButton("📨 Invite")],
                [KeyboardButton("⚙️ Setup"), KeyboardButton("📊 History")]
            ],
            resize_keyboard=True, one_time_keyboard=False
        )

async def update_user_keyboard(chat_id, bot, with_cancel, text="⏳"):
    global user_keyboard_msg
    old_msg_id = user_keyboard_msg.pop(chat_id, None)
    if old_msg_id:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=old_msg_id)
        except Exception:
            pass
    msg = await bot.send_message(chat_id, text, reply_markup=get_keyboard(with_cancel))
    user_keyboard_msg[chat_id] = msg.message_id

# -------------------------------------------------------------------
#  Provider listeners – with pattern‑based announcement filter
# -------------------------------------------------------------------
def is_task_completion(text):
    if not text:
        return False
    return "Authorization failed" in text or "Sending Task Completed" in text

def is_task_message(msg, request_number):
    text = (msg.text or "").lower()

    if request_number and request_number in text:
        return True

    if msg.buttons:
        for row in msg.buttons:
            for btn in row:
                if hasattr(btn, 'data') and btn.data:
                    data = btn.data
                    if data in (b"biz", b"pers", b"code_done", b"refresh") or data.startswith(b"limit_"):
                        return True
                if hasattr(btn, 'text'):
                    btn_text = btn.text.lower()
                    if "i've entered the code" in btn_text or "refresh" in btn_text:
                        return True

    if "total successfully sent" in text or "sending task completed" in text:
        return True

    if "authorization failed" in text or "authorization login" in text:
        return True

    return False

@client.on(events.NewMessage(chats=CONFIG["PROV_BOT"]))
async def prov_listener(event):
    global active_request
    if not active_request or not active_request.get("active"):
        return
    msg = event.message
    req = active_request
    if req.get("was_cancelled_by_provider"):
        return

    if req.get("watchdog") and not req["watchdog"].done():
        req["watchdog"].cancel()
    async def watchdog():
        await asyncio.sleep(CONFIG["TIMEOUT_SEC"])
        await timeout_handler(req)
    req["watchdog"] = asyncio.create_task(watchdog())

    if not is_task_message(msg, req["number"]):
        log.info("Blocked announcement from provider.")
        return

    task_done = is_task_completion(msg.text)
    was_photo, user_msg_id = await relay_message(
        msg, req["chat_id"], req["bot"],
        task_completed=task_done
    )
    if user_msg_id:
        req["msg_map"][msg.id] = {"msg_id": user_msg_id, "is_photo": was_photo}
        req["last_prov_msg_id"] = msg.id

    if task_done:
        req["completed"] = True
        req["completed_event"].set()

@client.on(events.MessageEdited(chats=CONFIG["PROV_BOT"]))
async def prov_edit_listener(event):
    global active_request
    if not active_request or not active_request.get("active"):
        return
    msg = event.message
    req = active_request
    if req.get("was_cancelled_by_provider"):
        return

    if req.get("watchdog") and not req["watchdog"].done():
        req["watchdog"].cancel()
    async def watchdog():
        await asyncio.sleep(CONFIG["TIMEOUT_SEC"])
        await timeout_handler(req)
    req["watchdog"] = asyncio.create_task(watchdog())

    if not is_task_message(msg, req["number"]):
        log.info("Blocked announcement edit from provider.")
        return

    task_done = is_task_completion(msg.text)
    was_photo = bool(msg.photo)

    prev_info = req["msg_map"].get(msg.id)
    edit_user_msg_id = prev_info["msg_id"] if prev_info else None
    edit_was_photo = prev_info["is_photo"] if prev_info else None

    was_photo_result, user_msg_id = await relay_message(
        msg, req["chat_id"], req["bot"],
        edit_user_msg_id=edit_user_msg_id,
        edit_was_photo=edit_was_photo,
        task_completed=task_done
    )

    if user_msg_id:
        req["msg_map"][msg.id] = {"msg_id": user_msg_id, "is_photo": was_photo_result}
        req["last_prov_msg_id"] = msg.id

    if task_done:
        req["completed"] = True
        req["completed_event"].set()

# -------------------------------------------------------------------
#  Main queue processor (with auto‑retry)
# -------------------------------------------------------------------
async def process_request(app):
    global active_request, callback_store, last_relayed
    while not stop_event.is_set():
        queue_item = await request_queue.get()
        completed_event = asyncio.Event()
        chat_id = queue_item["chat_id"]
        active_request = {
            "active": True,
            "completed": False,
            "chat_id": chat_id,
            "bot": app.bot,
            "number": queue_item["number"],
            "watchdog": None,
            "msg_map": {},
            "last_prov_msg_id": None,
            "was_cancelled": False,
            "was_cancelled_by_provider": False,
            "completed_event": completed_event,
            "_queue_uid": queue_item.get("_queue_id"),
            "_notif_msg_id": queue_item.get("_notif_msg_id")
        }
        callback_store.clear()
        cur = active_request

        # --- Remove the inline Cancel button silently ---
        notif_msg = cur.get("_notif_msg_id")
        if notif_msg:
            try:
                await app.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=notif_msg,
                    text="⚙️ Processing your request…",
                    reply_markup=None
                )
            except Exception as e:
                if "Message is not modified" in str(e):
                    pass
                else:
                    try:
                        await app.bot.delete_message(chat_id=chat_id, message_id=notif_msg)
                    except Exception:
                        pass
                    try:
                        new_msg = await app.bot.send_message(chat_id, "⚙️ Processing your request…")
                        cur["_notif_msg_id"] = new_msg.message_id
                    except Exception:
                        pass
        # Hide the persistent Cancel button
        await update_user_keyboard(chat_id, app.bot, with_cancel=False, text="⚙️ Processing your request…")

        # Auto‑retry sending the number
        retry_count = 0
        sent_successfully = False
        while retry_count < CONFIG["RETRY_MAX"] and not sent_successfully:
            try:
                if not CONFIG["TEST_MODE"]:
                    await client.send_message(CONFIG["PROV_BOT"], queue_item["number"])
                sent_successfully = True
            except Exception as e:
                log.error(f"Send attempt {retry_count+1} failed: {e}")
                retry_count += 1
                if retry_count < CONFIG["RETRY_MAX"]:
                    await asyncio.sleep(2)
                else:
                    await app.bot.send_message(chat_id, "❌ Unable to reach the service. Your request has been cancelled.")
                    cur["completed"] = True
                    cur["was_cancelled"] = True
                    cur["completed_event"].set()
                    break

        if not sent_successfully:
            pass
        else:
            if not CONFIG["TEST_MODE"]:
                async def watchdog():
                    await asyncio.sleep(CONFIG["TIMEOUT_SEC"])
                    await timeout_handler(cur)
                cur["watchdog"] = asyncio.create_task(watchdog())
                await completed_event.wait()
            else:
                log.info("Test mode: simulating provider for request")
                sim_task = asyncio.create_task(simulate_provider_task(cur, app.bot))
                async def watchdog():
                    await asyncio.sleep(CONFIG["TIMEOUT_SEC"])
                    if not cur["completed"]:
                        sim_task.cancel()
                        await timeout_handler(cur)
                cur["watchdog"] = asyncio.create_task(watchdog())
                await completed_event.wait()
                if not sim_task.done():
                    sim_task.cancel()

        # Cleanup
        if cur.get("watchdog") and not cur["watchdog"].done():
            cur["watchdog"].cancel()
        cur["active"] = False

        # Delete warning message when request finishes (either success or cancel)
        await clear_warning_message(chat_id, app.bot)

        if cur.get("was_cancelled"):
            await delete_relayed_messages(cur.get("msg_map"), app.bot, chat_id)
        else:
            last_relayed[chat_id] = cur.get("msg_map", {})

        notif_msg = cur.get("_notif_msg_id")
        if notif_msg:
            try:
                await app.bot.delete_message(chat_id=chat_id, message_id=notif_msg)
            except Exception:
                pass

        if cur.get("was_cancelled_by_provider") or cur.get("was_cancelled"):
            await update_user_keyboard(chat_id, app.bot, with_cancel=False, text="Select an action from the menu.")
        else:
            await update_user_keyboard(chat_id, app.bot, with_cancel=False, text="Select an action from the menu.")
        active_request = None
        callback_store.clear()
        await request_queue.task_done()
        log.info("Request finished – ready for next")

# -------------------------------------------------------------------
#  /start – with referral reward and user tracking
# -------------------------------------------------------------------
async def handle_start(update: Update, context):
    user = update.effective_user
    uid = user.id
    add_user(uid, context.bot)

    if context.args and context.args[0].isdigit():
        ref_id = int(context.args[0])
        if ref_id != uid:
            invitee_key = str(uid)
            if invitee_key not in invites:
                invites[invitee_key] = str(ref_id)
                save_json(CONFIG["INVITE_FILE"], invites)
                add_balance(ref_id, 0.05, "referral")
                log.info(f"Referral reward $0.05 to user {ref_id} from user {uid}")
                try:
                    await context.bot.send_message(
                        ref_id,
                        "🎉 Someone joined using your invite link! You received $0.05."
                    )
                except Exception:
                    pass

    await update.message.reply_text(
        "🤖 Welcome! Use the buttons below to manage your account.",
        reply_markup=get_keyboard(with_cancel=False)
    )

# -------------------------------------------------------------------
#  Handle replies from persistent keyboard – improved deletion
# -------------------------------------------------------------------
async def handle_menu_text(update: Update, context):
    text = update.message.text
    user = update.effective_user
    uid = user.id
    chat_id = update.effective_chat.id
    bot = context.bot

    try:
        await update.message.delete()
    except Exception:
        pass

    old_keyboard_msg = user_keyboard_msg.pop(chat_id, None)
    if old_keyboard_msg:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=old_keyboard_msg)
        except Exception:
            pass

    prompt_msg_id = context.user_data.pop("prompt_msg_id", None)
    if prompt_msg_id:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=prompt_msg_id)
        except Exception:
            pass

    prev_msg_id = context.user_data.pop("last_bot_msg_id", None)
    if prev_msg_id:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=prev_msg_id)
        except Exception:
            pass

    old_relayed = last_relayed.pop(chat_id, None)
    if old_relayed:
        await delete_relayed_messages(old_relayed, bot, chat_id)

    context.user_data["awaiting_number"] = False
    context.user_data["awaiting_wallet"] = False
    context.user_data.pop("wallet_type", None)

    if text == "➕ Add":
        if user_has_pending_request(uid):
            sent = await update.message.reply_text("⚠️ You already have a request in progress or queued.")
            user_warning_msg[uid] = sent.message_id
            return

        context.user_data["awaiting_number"] = True
        sent = await update.message.reply_text(
            "📱 Please send the number with the country code. 🌍📞\n"

            "Example: 📝\n"
            "• Ethiopia: +251972XXXX 🇪🇹\n"
            "• USA: +123564XXXXX 🇺🇸\n"
            "• India: +916271XXXXX 🇮🇳\n"

            "Format: +[country code][number] ✍️🔢"
        )
        context.user_data["prompt_msg_id"] = sent.message_id
        return

    if text == "❌ Cancel":
        msg = await cancel_user_request(uid, bot)
        if msg:
            await update.message.reply_text(msg)
        return

    sent_msg = None
    if text == "💰 Balance":
        bal = get_balance(uid)
        sent_msg = await update.message.reply_text(f"💰 Your balance: ${bal:.4f}")
        await update_user_keyboard(chat_id, bot, with_cancel=False, text="Select an action from the menu.")
    elif text == "🏦 Withdraw":
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("BEP20 (BSC)", callback_data="withdraw_method_BEP20")],
            [InlineKeyboardButton("🔙 Back", callback_data="setup_cancel")]
        ])
        sent_msg = await update.message.reply_text(
            "🏦 Choose withdrawal method:",
            reply_markup=keyboard
        )
        context.user_data["prompt_msg_id"] = sent_msg.message_id
        return
    elif text == "❓ FAQ":
        faq_text = (
            "❓ FAQ 🍖🔥\n\n"
            "• How to earn? Send number or use ➕ button. Reward: $0.015 per message sent. 🌾\n"
            "• Withdraw: Use 🏦 Withdraw after setting wallet. 💰\n"
            "• Support: Contact @vx_lecter or call +251909259439. 📞\n"
            "• About requests: Processed only for 90 seconds. If time passes, request cancelled. ⏳\n"
            "• For each referral, you'll get $0.05. 🎁"
        )
        try:
            sent_msg = await update.message.reply_text(faq_text, parse_mode="HTML")
        except Exception as e:
            log.error(f"FAQ send error: {e}")
            sent_msg = await update.message.reply_text(faq_text)
        await update_user_keyboard(chat_id, bot, with_cancel=False, text="Select an action from the menu.")
    elif text == "📨 Invite":
        invited_count = sum(1 for v in invites.values() if v == str(uid))
        bot_username = CONFIG["BOT_USERNAME"]
        ref_link = f"https://t.me/{bot_username}?start={uid}"
        invite_text = (
            f"📨 Invite friends and earn $0.05 per referral!\n\n"
            f"👥 You have invited: {invited_count} user{'s' if invited_count != 1 else ''}\n\n"
            f"Your referral link:\n`{ref_link}`\n\nShare this link to earn bonuses!"
        )
        sent_msg = await update.message.reply_text(invite_text, parse_mode="Markdown")
        await update_user_keyboard(chat_id, bot, with_cancel=False, text="Select an action from the menu.")
    elif text == "⚙️ Setup":
        # Show current wallet if set (BEP20 only), otherwise show selection
        wallet = get_wallet(uid)
        if wallet:
            if wallet.startswith("BEP20:"):
                w_address = wallet[6:]
                w_type = "BEP20"
            else:
                w_type = "Unknown"
                w_address = wallet
            review_text = f"⚙️ Your current wallet:\nType: {w_type}\nAddress: {w_address}\n\nChoose an action:"
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Change", callback_data="setup_change")],
                [InlineKeyboardButton("🔙 Back", callback_data="setup_cancel")]
            ])
            sent_msg = await update.message.reply_text(review_text, reply_markup=keyboard)
        else:
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("BEP20 (BSC)", callback_data="wallet_type_BEP20")],
                [InlineKeyboardButton("🔙 Back", callback_data="setup_cancel")]
            ])
            sent_msg = await update.message.reply_text("⚙️ Choose your wallet type:", reply_markup=keyboard)
        context.user_data["prompt_msg_id"] = sent_msg.message_id
        return
    elif text == "📊 History":
        user_history = history.get(str(uid), [])
        if not user_history:
            sent_msg = await update.message.reply_text("📊 You have no earning history yet.")
        else:
            total_task = sum(t["amount"] for t in user_history if t["type"] == "task")
            total_referral = sum(t["amount"] for t in user_history if t["type"] == "referral")
            total_withdraw = sum(t["amount"] for t in user_history if t["type"] == "withdraw")
            lines = [
                "📊 <b>Earning History</b>",
                f"• From tasks: ${total_task:.4f}",
                f"• From referrals: ${total_referral:.4f}",
                f"• Total withdrawn: ${abs(total_withdraw):.4f}",
                "───────────────",
                "<b>Recent transactions:</b>"
            ]
            for txn in user_history[-10:]:
                timestamp = txn.get("time", 0)
                date_str = time.strftime("%d/%m/%Y %H:%M", time.gmtime(timestamp)) if timestamp else "?"
                if txn["type"] == "task":
                    t_type = "🟢 Task"
                elif txn["type"] == "referral":
                    t_type = "🔵 Referral"
                elif txn["type"] == "withdraw":
                    t_type = "🔴 Withdraw"
                else:
                    t_type = txn["type"]
                lines.append(f"{t_type} ${txn['amount']:+.4f} - {date_str}")
            sent_msg = await update.message.reply_text("\n".join(lines), parse_mode="HTML")
        await update_user_keyboard(chat_id, bot, with_cancel=False, text="Select an action from the menu.")

    if sent_msg:
        context.user_data["last_bot_msg_id"] = sent_msg.message_id

# -------------------------------------------------------------------
#  Setup Change callback – go to wallet type selection (BEP20 only)
# -------------------------------------------------------------------
async def setup_change_callback(update: Update, context):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("⚙️ Choose new wallet type:", reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("BEP20 (BSC)", callback_data="wallet_type_BEP20")],
        [InlineKeyboardButton("🔙 Back", callback_data="setup_cancel")]
    ]))
    context.user_data["prompt_msg_id"] = query.message.message_id
    context.user_data["awaiting_wallet"] = False
    context.user_data.pop("wallet_type", None)

# -------------------------------------------------------------------
#  Withdrawal method selection callback – only BEP20, with minimum check
# -------------------------------------------------------------------
async def withdraw_method_callback(update: Update, context):
    query = update.callback_query
    await query.answer()
    data = query.data

    if not data.startswith("withdraw_method_"):
        return

    method = data[len("withdraw_method_"):]   # "BEP20"
    user_id = update.effective_user.id
    user = update.effective_user
    bot = context.bot

    wallet = get_wallet(user_id)

    if not wallet.startswith("BEP20:"):
        await query.edit_message_text("⚠️ You haven't set a BEP20 wallet. Use ⚙️ Setup first.")
        await update_user_keyboard(query.message.chat_id, bot, with_cancel=False, text="Select an action from the menu.")
        return
    w_address = wallet[6:]
    w_type = "BEP20"

    bal = get_balance(user_id)
    if bal <= 0:
        await query.edit_message_text("Your balance is zero, please complete more tasks and earn 💸💰 📈.")
        await update_user_keyboard(query.message.chat_id, bot, with_cancel=False, text="Select an action from the menu.")
        return

    if bal < CONFIG["MIN_WITHDRAW"]:
        await query.edit_message_text(f"⚠️ Minimum withdrawal is ${CONFIG['MIN_WITHDRAW']:.2f}. Keep earning!")
        await update_user_keyboard(query.message.chat_id, bot, with_cancel=False, text="Select an action from the menu.")
        return

    amount_str = f"{bal:.4f}"
    callback_data = f"done_withdraw_{user_id}_{amount_str}"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Done", callback_data=callback_data)]
    ])
    admin_msg = (f"🏦 Withdraw request\nUser: {user_id}\n"
                 f"Type: {w_type}\n"
                 f"Address: {w_address}\n"
                 f"Amount: ${amount_str}\n"
                 f"Username: @{user.username or 'N/A'}")
    try:
        await bot.send_message(CONFIG["ADMIN_CHAT_ID"], admin_msg, reply_markup=keyboard)
        withdraw_balance(user_id, bal)
        await query.edit_message_text(f"✅ Withdraw request for ${amount_str} sent. It will be processed in 12 hours.")
    except Exception as e:
        log.error(f"Admin notify failed: {e}")
        await query.edit_message_text("❌ Error sending request. Contact support.")

    await update_user_keyboard(query.message.chat_id, bot, with_cancel=False, text="Select an action from the menu.")

# -------------------------------------------------------------------
#  Wallet type selection callback – only BEP20
# -------------------------------------------------------------------
async def wallet_type_callback(update: Update, context):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "wallet_type_BEP20":
        wallet_type = "BEP20"
        prompt_text = "📝 Please send your BEP20 (BSC) USDT address.\nIt should start with '0x' and be 42 characters long.\nExample: 0x1234567890abcdef1234567890abcdef12345678"
    else:
        return

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back", callback_data="wallet_back")]
    ])
    await query.edit_message_text(prompt_text, reply_markup=keyboard)

    context.user_data["wallet_type"] = wallet_type
    context.user_data["awaiting_wallet"] = True
    context.user_data["prompt_msg_id"] = query.message.message_id

# -------------------------------------------------------------------
#  Back button handlers
# -------------------------------------------------------------------
async def setup_cancel_callback(update: Update, context):
    query = update.callback_query
    await query.answer()
    await query.delete_message()
    await update_user_keyboard(query.message.chat_id, context.bot, with_cancel=False, text="Select an action from the menu.")

async def wallet_back_callback(update: Update, context):
    query = update.callback_query
    await query.answer()
    context.user_data["awaiting_wallet"] = False
    context.user_data.pop("wallet_type", None)
    await query.delete_message()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("BEP20 (BSC)", callback_data="wallet_type_BEP20")],
        [InlineKeyboardButton("🔙 Back", callback_data="setup_cancel")]
    ])
    sent_msg = await query.message.reply_text(
        "⚙️ Choose your wallet type:",
        reply_markup=keyboard
    )
    context.user_data["prompt_msg_id"] = sent_msg.message_id
    await update_user_keyboard(query.message.chat_id, context.bot, with_cancel=False, text="Select an action from the menu.")

# -------------------------------------------------------------------
#  Combined text input handler (wallet setup & number input)
# -------------------------------------------------------------------
async def handle_text_input(update: Update, context):
    text = update.message.text.strip()
    menu_buttons = ["➕ Add", "❌ Cancel", "💰 Balance", "🏦 Withdraw", "❓ FAQ", "📨 Invite", "⚙️ Setup", "📊 History"]

    if text in menu_buttons:
        await handle_menu_text(update, context)
        return

    if context.user_data.get("awaiting_number"):
        number = text
        if not re.match(r'^\+?\d+$', number):
            await update.message.reply_text(
                "❌ Invalid format. Please send the number with country code, e.g., +25197*******."
            )
            return

        uid = update.effective_user.id

        if user_has_pending_request(uid):
            sent = await update.message.reply_text("⚠️ You already have a request in progress or queued.")
            user_warning_msg[uid] = sent.message_id
            context.user_data["awaiting_number"] = False
            return

        prompt_msg_id = context.user_data.pop("prompt_msg_id", None)
        if prompt_msg_id:
            try:
                await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=prompt_msg_id)
            except Exception:
                pass
        try:
            await update.message.delete()
        except Exception:
            pass

        context.user_data["awaiting_number"] = False
        add_user(uid, context.bot)

        queue_uid = request_queue.reserve_uid()
        pos = request_queue.qsize() + 1
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_order_{queue_uid}")]
        ])
        sent_msg = await update.message.reply_text(
            f"✅ Order sent (position #{pos}). Please wait...",
            reply_markup=keyboard
        )
        await request_queue.put({
            "chat_id": uid,
            "number": number,
            "_notif_msg_id": sent_msg.message_id
        }, uid=queue_uid)
        await update_user_keyboard(update.effective_chat.id, context.bot, with_cancel=True, text="📦 Order queued. Cancel button added.")
        return

    if context.user_data.get("awaiting_wallet"):
        wallet_type = context.user_data.get("wallet_type")
        if not wallet_type:
            await update.message.reply_text("⚠️ Please choose a wallet type first using ⚙️ Setup.")
            return

        raw = text
        if wallet_type == "BEP20":
            if re.match(r'^0x[a-fA-F0-9]{40}$', raw):
                formatted = f"BEP20:{raw}"
            else:
                await update.message.reply_text("❌ Invalid BEP20 address. It must start with '0x' and be 42 characters long.")
                return
        else:
            return

        prompt_msg_id = context.user_data.pop("prompt_msg_id", None)
        if prompt_msg_id:
            try:
                await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=prompt_msg_id)
            except Exception:
                pass
        uid = update.effective_user.id
        set_wallet(uid, formatted)
        context.user_data["awaiting_wallet"] = False
        context.user_data.pop("wallet_type", None)
        await update.message.reply_text("✅ Wallet address saved successfully!")
        await update_user_keyboard(update.effective_chat.id, context.bot, with_cancel=False, text="Select an action from the menu.")
        return

    await update.message.reply_text("Please use the menu buttons or /add [number].")

# -------------------------------------------------------------------
#  Commands: /add, /cancel, /status, /queue, /promo, /help, /msg, /stats
# -------------------------------------------------------------------
async def handle_add(update: Update, context):
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /add [number]")
        return
    number = context.args[0].strip()

    uid = update.effective_user.id

    if user_has_pending_request(uid):
        sent = await update.message.reply_text("⚠️ You already have a request in progress or queued.")
        user_warning_msg[uid] = sent.message_id
        return

    add_user(uid, context.bot)

    queue_uid = request_queue.reserve_uid()
    pos = request_queue.qsize() + 1
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_order_{queue_uid}")]
    ])
    sent_msg = await update.message.reply_text(
        f"✅ Order sent (position #{pos}). Please wait...",
        reply_markup=keyboard
    )
    await request_queue.put({
        "chat_id": uid,
        "number": number,
        "_notif_msg_id": sent_msg.message_id
    }, uid=queue_uid)
    await update_user_keyboard(uid, context.bot, with_cancel=True, text="📦 Order queued.")

async def handle_cancel(update: Update, context):
    user_id = update.effective_user.id
    bot = context.bot
    msg = await cancel_user_request(user_id, bot)
    if msg:
        await update.message.reply_text(msg)

async def handle_status(update: Update, context):
    if update.effective_user.id != CONFIG["ADMIN_CHAT_ID"]:
        await update.message.reply_text("⛔ Admin only.")
        return
    if active_request and active_request.get("active"):
        await update.message.reply_text(f"🔵 Active request: {active_request['number']} for user {active_request['chat_id']}")
    else:
        await update.message.reply_text("No active request.")

async def handle_queue(update: Update, context):
    if update.effective_user.id != CONFIG["ADMIN_CHAT_ID"]:
        await update.message.reply_text("⛔ Admin only.")
        return
    size = request_queue.qsize()
    await update.message.reply_text(f"📋 Queue length: {size}")

async def handle_promo(update: Update, context):
    if update.effective_user.id != CONFIG["ADMIN_CHAT_ID"]:
        await update.message.reply_text("⛔ Admin only.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /promo <message text>")
        return

    announcement = " ".join(context.args)

    all_user_ids = list(users.keys())
    for uid in list(balances.keys()) + list(wallets.keys()):
        if uid not in all_user_ids:
            all_user_ids.append(uid)
    for item in request_queue._items.values():
        if not item.get("_cancelled"):
            uid = str(item.get("chat_id"))
            if uid not in all_user_ids:
                all_user_ids.append(uid)

    all_user_ids = [uid for uid in all_user_ids if uid != str(CONFIG["ADMIN_CHAT_ID"])]

    if not all_user_ids:
        await update.message.reply_text("No users found to send the announcement.")
        return

    sent_count = 0
    fail_count = 0
    for user_id in all_user_ids:
        try:
            await context.bot.send_message(int(user_id), f"📢 Announcement:\n\n{announcement}")
            sent_count += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            log.warning(f"Failed to send promo to {user_id}: {e}")
            fail_count += 1

    await update.message.reply_text(
        f"✅ Announcement sent to {sent_count}/{len(all_user_ids)} users."
        + (f" ({fail_count} failed)" if fail_count else "")
    )

async def handle_help(update: Update, context):
    help_text = (
        "🤖 <b>Available Commands:</b>\n"
        "/start – Show the main menu\n"
        "/add [number] – Submit a new task\n"
        "/cancel – Cancel your current or queued request\n"
        "/status – (Admin) Check active request\n"
        "/queue – (Admin) Check queue length\n"
        "/promo [text] – (Admin) Send announcement to all users\n"
        "/help – Show this message\n\n"
        "<b>Menu Buttons:</b>\n"
        "➕ Add – Start a new task\n"
        "💰 Balance – Check your balance\n"
        "🏦 Withdraw – Withdraw earnings (BEP20 only)\n"
        "❓ FAQ – Frequently asked questions\n"
        "📨 Invite – Get your referral link\n"
        "⚙️ Setup – Set or change your BEP20 wallet address\n"
        "📊 History – View earning & withdrawal history\n"
    )
    await update.message.reply_text(help_text, parse_mode="HTML")

async def handle_msg(update: Update, context):
    if update.effective_user.id != CONFIG["ADMIN_CHAT_ID"]:
        await update.message.reply_text("⛔ Admin only.")
        return

    if not context.args or len(context.args) < 2:
        await update.message.reply_text("Usage: /msg <user_id> <message>")
        return

    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Invalid user ID.")
        return

    text_to_send = " ".join(context.args[1:])

    try:
        await context.bot.send_message(target_id, f"📢 Message from admin:\n\n{text_to_send}")
        await update.message.reply_text(f"✅ Message sent to user {target_id}.")
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to send message: {e}")

async def handle_stats(update: Update, context):
    if update.effective_user.id != CONFIG["ADMIN_CHAT_ID"]:
        await update.message.reply_text("⛔ Admin only.")
        return

    total_users = len(users)
    total_tasks = 0
    total_referrals = 0
    total_withdraw = 0
    total_earned = 0

    for uid, txns in history.items():
        for txn in txns:
            if txn["type"] == "task":
                total_tasks += 1
                total_earned += txn["amount"]
            elif txn["type"] == "referral":
                total_referrals += 1
                total_earned += txn["amount"]
            elif txn["type"] == "withdraw":
                total_withdraw += abs(txn["amount"])

    stats_text = (
        "📊 <b>Bot Statistics</b>\n"
        f"👥 Total users: {total_users}\n"
        f"🟢 Tasks completed: {total_tasks}\n"
        f"🔵 Referrals: {total_referrals}\n"
        f"💰 Total earned: ${total_earned:.4f}\n"
        f"🔴 Total withdrawn: ${total_withdraw:.4f}\n"
        f"💵 Current balance (all users): ${sum(balances.values()):.4f}"
    )
    await update.message.reply_text(stats_text, parse_mode="HTML")

# -------------------------------------------------------------------
#  Inline button handlers (relayed buttons + custom cancel)
# -------------------------------------------------------------------
async def cancel_order_callback(update: Update, context):
    query = update.callback_query
    await query.answer()
    data = query.data
    if not data.startswith("cancel_order_"):
        return
    try:
        queue_uid = int(data.split("_")[-1])
    except ValueError:
        await query.edit_message_text("Invalid cancel request.")
        return

    user_id = update.effective_user.id
    bot = context.bot
    global active_request

    if active_request and active_request.get("active") and active_request.get("_queue_uid") == queue_uid:
        if active_request["chat_id"] != user_id:
            await query.answer("You can only cancel your own request.", show_alert=True)
            return
        cur = active_request
        await delete_relayed_messages(cur.get("msg_map"), bot, cur["chat_id"])
        cur["completed"] = True
        cur["was_cancelled"] = True
        cur["completed_event"].set()
        cur["_notif_msg_id"] = None
        await clear_warning_message(user_id, bot)
        await query.edit_message_text("🛑 Order cancelled.")
        await update_user_keyboard(user_id, bot, with_cancel=False, text="Select an action from the menu.")
        return

    request_queue.cancel_by_uid(queue_uid)
    await clear_warning_message(user_id, bot)
    await query.edit_message_text("🛑 Order cancelled.")
    has_pending = any(not i.get("_cancelled") and i.get("chat_id") == user_id
                      for i in request_queue._items.values())
    if not has_pending and (not active_request or active_request.get("chat_id") != user_id):
        await update_user_keyboard(user_id, bot, with_cancel=False, text="Select an action from the menu.")

async def done_withdraw_callback(update: Update, context):
    query = update.callback_query
    await query.answer()
    data = query.data
    if not data.startswith("done_withdraw_"):
        return

    if update.effective_user.id != CONFIG["ADMIN_CHAT_ID"]:
        await query.answer("You are not authorized.", show_alert=True)
        return

    try:
        parts = data[len("done_withdraw_"):].split("_", 1)
        user_id = int(parts[0])
        amount_str = parts[1]
        amount = float(amount_str)
    except Exception:
        await query.edit_message_text("Invalid withdrawal data.")
        return

    original_text = query.message.text
    new_text = original_text + "\n\n✅ Paid"
    await query.edit_message_text(new_text)

    try:
        await context.bot.send_message(
            user_id,
            f"✅ Your withdrawal of ${amount_str} has been processed."
        )
    except Exception as e:
        log.warning(f"Could not notify user {user_id} about withdrawal: {e}")

async def handle_click(cb_id, query):
    global active_request
    if not active_request or not active_request.get("active"):
        await query.edit_message_text("No active request.")
        return

    if CONFIG["TEST_MODE"]:
        await simulate_provider_callback(cb_id, query, active_request)
        return

    if cb_id not in callback_store:
        await query.edit_message_text("Invalid button")
        return
    entry = callback_store[cb_id]

    if "cancel" in entry.get("text", "").lower():
        cur = active_request
        try:
            await client(GetBotCallbackAnswerRequest(
                peer=CONFIG["PROV_BOT"], msg_id=entry["msg_id"], data=entry["data"]))
        except Exception as e:
            log.warning(f"Provider cancel click failed: {e}")

        if cur and cur.get("active"):
            await delete_relayed_messages(cur.get("msg_map"), query._bot, cur["chat_id"])
            cur["was_cancelled_by_provider"] = True
            cur["completed"] = True
            cur["was_cancelled"] = True
            cur["completed_event"].set()
        await clear_warning_message(cur["chat_id"], query._bot)
        await query.edit_message_text("🛑 Order cancelled.")
        return

    try:
        await client(GetBotCallbackAnswerRequest(
            peer=CONFIG["PROV_BOT"], msg_id=entry["msg_id"], data=entry["data"]))
    except Exception as e:
        await query.edit_message_text(f"Error: {e}")

async def button_callback(update: Update, context):
    query = update.callback_query
    await query.answer()
    if query.data.startswith("pb_"):
        await handle_click(query.data, query)

# -------------------------------------------------------------------
#  Test mode simulation helpers (unchanged)
# -------------------------------------------------------------------
SIM_ID_COUNTER = 1000

def sim_msg_id():
    global SIM_ID_COUNTER
    SIM_ID_COUNTER += 1
    return SIM_ID_COUNTER

class SimButton:
    def __init__(self, text, data=None, url=None):
        self.text = text
        self.data = data
        self.url = url

class SimMessage:
    def __init__(self, mid, text, photo=False, media=False, buttons=None):
        self.id = mid
        self.text = text
        self.raw_text = text
        self.photo = photo
        self.media = media
        self._buttons = buttons or []

    @property
    def buttons(self):
        return self._buttons

    async def download_media(self, file):
        from PIL import Image
        img = Image.new('RGB', (100, 100), color='black')
        img.save(file, format='JPEG')

def make_sim_buttons(button_list):
    rows = []
    for row in button_list:
        btn_row = []
        for b in row:
            btn = SimButton(b["text"], b.get("data"), b.get("url"))
            btn_row.append(btn)
        rows.append(btn_row)
    return rows

async def relay_sim_message(fake_msg, chat_id, bot, req, task_completed=False):
    mid = fake_msg.get("id", sim_msg_id())
    text = fake_msg.get("text", "")
    photo = fake_msg.get("photo", False)
    media = fake_msg.get("media", False)
    button_defs = fake_msg.get("buttons", [])
    buttons = make_sim_buttons(button_defs) if button_defs else []

    sim = SimMessage(mid, text, photo=photo, media=media, buttons=buttons)
    was_photo, user_msg_id = await relay_message(sim, chat_id, bot, task_completed=task_completed)
    if user_msg_id:
        req["msg_map"][mid] = {"msg_id": user_msg_id, "is_photo": was_photo}
        req["last_prov_msg_id"] = mid

async def simulate_provider_task(req, bot):
    chat_id = req["chat_id"]
    number = req["number"]

    msg1_id = sim_msg_id()
    await relay_sim_message({
        "id": msg1_id,
        "text": f"Phone Number: {number}\nAccount Type: ?\nChoose:",
        "buttons": [
            [{"text": "Business", "data": b"biz"}, {"text": "Personal", "data": b"pers"}]
        ]
    }, chat_id, bot, req)

    req["stage_event"] = asyncio.Event()
    await req["stage_event"].wait()
    req["stage_event"].clear()

    req["stage_event"] = asyncio.Event()
    await req["stage_event"].wait()
    req["stage_event"].clear()

    code_msg_id = sim_msg_id()
    await relay_sim_message({
        "id": code_msg_id,
        "text": f"🔢 Please enter this code on the other platform: ABC-{number[-4:]}\nThen press the button below.",
        "buttons": [[{"text": "✅ I've entered the code", "data": b"code_done"}]]
    }, chat_id, bot, req)
    req["last_prov_msg_id"] = code_msg_id
    req["stage_event"] = asyncio.Event()
    await req["stage_event"].wait()
    req["stage_event"].clear()

    photo_msg_id = sim_msg_id()
    await relay_sim_message({
        "id": photo_msg_id,
        "photo": True,
        "text": "✅ Task completed",
        "buttons": []
    }, chat_id, bot, req)

    stats_msg_id = sim_msg_id()
    stats_text = (
        "✅ Sending Task Completed\n"
        "--------------------\n"
        "251921390749\n"
        "--------------------\n"
        "🎉 Great job! Your sending task is now fully completed.\n"
        "📨 Total successfully sent: 1\n"
        "💵 Earnings for this task: 0.0150 USD\n"
        "⏱ Please keep your account online...\n"
        "Status: Online"
    )
    await relay_sim_message({
        "id": stats_msg_id,
        "photo": False,
        "text": stats_text,
        "buttons": [[{"text": "🔄 Refresh", "data": b"refresh"}]]
    }, chat_id, bot, req, task_completed=True)
    req["last_prov_msg_id"] = stats_msg_id
    req["last_stats_text"] = stats_text
    req["last_status"] = "Online"

    req["completed"] = True
    req["completed_event"].set()

async def simulate_provider_callback(cb_id, query, req):
    if cb_id not in callback_store:
        await query.edit_message_text("Invalid button")
        return

    entry = callback_store[cb_id]
    btn_text = entry["text"].lower()
    data_bytes = entry.get("data")
    data_str = data_bytes.decode() if data_bytes else ""
    chat_id = req["chat_id"]
    bot = req["bot"]
    prov_msg_id = entry["msg_id"]

    def signal_stage():
        if req.get("stage_event"):
            req["stage_event"].set()

    try:
        if btn_text in ["business", "personal"]:
            new_text = f"Phone Number: {req['number']}\nAccount Type: {btn_text.capitalize()}\n\nSelect limit:"
            new_buttons = [
                [{"text": "1", "data": b"limit_1"}, {"text": "3", "data": b"limit_3"}, {"text": "5", "data": b"limit_5"}],
                [{"text": "7", "data": b"limit_7"}, {"text": "10", "data": b"limit_10"}, {"text": "20", "data": b"limit_20"}]
            ]
            await edit_sim_message(prov_msg_id, new_text, new_buttons, chat_id, bot, req)
            signal_stage()
            return

        if data_str.startswith("limit_"):
            limit = data_str.split("_")[1]
            new_text = f"Phone Number: {req['number']}\nAccount Type: Business\nLimit: {limit}\n\nSending code..."
            try:
                await edit_sim_message(prov_msg_id, new_text, [], chat_id, bot, req)
            except Exception as e:
                log.error(f"Limit edit failed: {e}")
            finally:
                signal_stage()
            return

        if "i've entered the code" in btn_text or data_str == "code_done":
            signal_stage()
            await query.edit_message_text("✅ Code accepted.")
            return

        if btn_text == "🔄 refresh":
            old_text = req.get("last_stats_text", "")
            match = re.search(r"Total successfully sent:\s*(\d+)", old_text)
            new_count = int(match.group(1)) + 1 if match else 1

            old_status = req.get("last_status", "Online")
            new_status = "Offline" if old_status == "Online" else "Online"

            new_text = (
                "✅ Sending Task Completed\n"
                "--------------------\n"
                "251921390749\n"
                "--------------------\n"
                "🎉 Great job! Your sending task is now fully completed.\n"
                f"📨 Total successfully sent: {new_count}\n"
                f"💵 Earnings for this task: {new_count * CONFIG['REWARD_PER_MESSAGE']:.4f} USD\n"
                "⏱ Please keep your account online...\n"
                f"Status: {new_status}"
            )
            req["last_stats_text"] = new_text
            req["last_status"] = new_status

            prev_info = req["msg_map"].get(prov_msg_id)
            if prev_info and not prev_info["is_photo"]:
                sim = SimMessage(prov_msg_id, new_text,
                                 buttons=make_sim_buttons([[{"text": "🔄 Refresh", "data": b"refresh"}]]))
                await relay_message(sim, chat_id, bot,
                                    edit_user_msg_id=prev_info["msg_id"],
                                    edit_was_photo=False,
                                    task_completed=True)
            else:
                await relay_sim_message({"id": prov_msg_id, "text": new_text,
                                         "buttons": [[{"text": "🔄 Refresh", "data": b"refresh"}]]},
                                        chat_id, bot, req, task_completed=True)
            return

    except Exception as e:
        log.error(f"Callback error ({btn_text}): {e}")
        await query.edit_message_text(f"❌ Error: {e}")
        signal_stage()

async def edit_sim_message(prov_msg_id, new_text, new_buttons, chat_id, bot, req):
    sim = SimMessage(prov_msg_id, new_text, buttons=make_sim_buttons(new_buttons) if new_buttons else [])
    prev_info = req["msg_map"].get(prov_msg_id)
    if prev_info:
        edit_id = prev_info["msg_id"]
        was_photo_prev = prev_info["is_photo"]
        was_photo, user_msg_id = await relay_message(sim, chat_id, bot,
                                                     edit_user_msg_id=edit_id,
                                                     edit_was_photo=was_photo_prev)
        if user_msg_id:
            req["msg_map"][prov_msg_id] = {"msg_id": user_msg_id, "is_photo": was_photo}
    else:
        await relay_sim_message({"id": prov_msg_id, "text": new_text, "buttons": new_buttons}, chat_id, bot, req)

# -------------------------------------------------------------------
#  Startup
# -------------------------------------------------------------------
async def main():
    # Materialise Telethon session from env if provided
    materialize_telethon_session()

    app = Application.builder().token(CONFIG["BOT_TOKEN"]).build()

    request_queue.load()

    app.add_handler(CommandHandler("start", handle_start))
    app.add_handler(CommandHandler("add", handle_add))
    app.add_handler(CommandHandler("cancel", handle_cancel))
    app.add_handler(CommandHandler("status", handle_status))
    app.add_handler(CommandHandler("queue", handle_queue))
    app.add_handler(CommandHandler("promo", handle_promo))
    app.add_handler(CommandHandler("help", handle_help))
    app.add_handler(CommandHandler("msg", handle_msg))
    app.add_handler(CommandHandler("stats", handle_stats))
    app.add_handler(CallbackQueryHandler(cancel_order_callback, pattern=r"^cancel_order_"))
    app.add_handler(CallbackQueryHandler(done_withdraw_callback, pattern=r"^done_withdraw_"))
    app.add_handler(CallbackQueryHandler(withdraw_method_callback, pattern=r"^withdraw_method_"))
    app.add_handler(CallbackQueryHandler(wallet_type_callback, pattern=r"^wallet_type_"))
    app.add_handler(CallbackQueryHandler(setup_change_callback, pattern=r"^setup_change$"))
    app.add_handler(CallbackQueryHandler(setup_cancel_callback, pattern=r"^setup_cancel$"))
    app.add_handler(CallbackQueryHandler(wallet_back_callback, pattern=r"^wallet_back$"))
    app.add_handler(CallbackQueryHandler(button_callback, pattern=r"^pb_"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))

    if not CONFIG["TEST_MODE"]:
        try:
            await client.start(phone=CONFIG["PHONE"])
            log.info("✅ MTProto Connected")
        except Exception as e:
            log.error(f"❌ MTProto start failed: {e}")
            log.error("If this is the first run, generate a session locally and set TELETHON_SESSION_B64.")
            raise
    else:
        log.info("🔧 Test mode enabled – no MTProto account needed")

    asyncio.create_task(process_request(app))

    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    log.info("🤖 Bot running – /start for menu, use ➕ Add or /add <number> to begin")

    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        log.info("Stopped")
    finally:
        await app.updater.stop()
        await app.stop()
        if not CONFIG["TEST_MODE"]:
            await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
