"""
bot.py  –  Telegram Channel Forwarder Bot
==========================================
Workflows:
  /login          – connect Telethon user account (phone → OTP → optional 2FA)
  /logout         – revoke session
  /status         – check login status
  /addpipeline    – set up a source → destination → topic mapping
  /mypipelines    – list, pause/resume, delete pipelines
  /cancel         – cancel any ongoing operation

On startup all active pipelines are resumed automatically.
"""

import asyncio
import os
import random
import re
from typing import Optional

import aiohttp
from aiohttp import web

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
)
from telegram.constants import ParseMode
from telegram.error import RetryAfter, TimedOut, NetworkError
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ConversationHandler, CallbackQueryHandler,
    ContextTypes, filters,
)
from telegram.request import HTTPXRequest

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    PhoneNumberInvalidError,
    PasswordHashInvalidError,
)
from telethon.tl.functions.messages import GetForumTopicsRequest

from db import (
    get_user, set_user_field, set_user_fields,
    get_pipelines, add_pipeline, delete_pipeline,
    toggle_pipeline, count_pipelines, close_db,
    MAX_PIPELINES,
)
import listener

# ======================== ENV ============================
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID    = os.getenv("TELEGRAM_API_ID")
API_HASH  = os.getenv("TELEGRAM_API_HASH")

if not all([BOT_TOKEN, API_ID, API_HASH]):
    raise RuntimeError("Missing env vars: BOT_TOKEN, TELEGRAM_API_ID, TELEGRAM_API_HASH")

API_ID = int(API_ID)

PING_PORT = int(os.getenv("PORT", "10000"))

# =================== CONV STATES =========================
LOGIN_PHONE, LOGIN_OTP, LOGIN_2FA = range(3)

# /addpipeline states
(
    AP_SOURCE,
    AP_DESTINATION,
    AP_VERIFY_ADMIN,
    AP_GENERAL_TOPIC,
    AP_FORMS_TOPIC,
) = range(5, 10)

# =================== LOGIN STATE =========================
LOGIN_STATE: dict[str, dict] = {}


# ── Helpers ───────────────────────────────────────────────────────

def escape_md(text: str) -> str:
    special = r"\_*[]()~`>#+-=|{}.!"
    return re.sub(f"([{re.escape(special)}])", r"\\\1", str(text))


async def safe_send(coro, retries: int = 6):
    for attempt in range(retries):
        try:
            return await coro
        except RetryAfter as e:
            wait = e.retry_after + random.uniform(5, 15)
            print(f"  ⚠️  Rate-limit: sleeping {wait:.1f}s")
            await asyncio.sleep(wait)
        except (TimedOut, NetworkError) as e:
            wait = (2 ** attempt) + random.uniform(1, 5)
            print(f"  ⚠️  Network error ({e}), retry {attempt+1}/{retries} in {wait:.1f}s")
            await asyncio.sleep(wait)
        except Exception:
            raise
    raise RuntimeError(f"safe_send failed after {retries} attempts")


async def get_client(user_id: str) -> Optional[TelegramClient]:
    user_doc    = await get_user(user_id)
    session_str = user_doc.get("session_string", "")
    client      = TelegramClient(StringSession(session_str), API_ID, API_HASH)
    await client.connect()
    return client


async def save_session(user_id: str, client: TelegramClient):
    await set_user_field(user_id, "session_string", client.session.save())


async def _cleanup_login_state(user_id: str):
    state = LOGIN_STATE.pop(user_id, None)
    if state:
        client = state.get("data", {}).get("client")
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass


# ── /start ────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    first = update.effective_user.first_name or "there"
    await update.message.reply_text(
        f"👋 *Welcome, {first}!*\n\n"
        "I forward messages from private channels to your group topics.\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "🔐 *Account*\n"
        "• /login — connect your Telegram account\n"
        "• /logout — revoke your session\n"
        "• /status — check login status\n\n"
        "🔁 *Forwarder*\n"
        "• /addpipeline — add a source → destination pipeline\n"
        "• /mypipelines — manage your pipelines\n\n"
        "⚙️ *Other*\n"
        "• /cancel — cancel any ongoing operation\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "👉 New here? Start with /login to connect your account.",
        parse_mode=ParseMode.MARKDOWN,
    )


# ── /cancel ───────────────────────────────────────────────────────

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    context.user_data.clear()
    await _cleanup_login_state(user_id)
    await update.message.reply_text("❌ Cancelled.")
    return ConversationHandler.END


# ── /logout ───────────────────────────────────────────────────────

async def logout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id  = str(update.effective_user.id)
    user_doc = await get_user(user_id)
    if not user_doc.get("session_string"):
        await update.message.reply_text("❌ You are not logged in.")
        return
    try:
        client = await get_client(user_id)
        if await client.is_user_authorized():
            await client.log_out()
        else:
            await client.disconnect()
    except Exception:
        pass
    await set_user_field(user_id, "session_string", "")
    await _cleanup_login_state(user_id)
    # Stop their listener too
    await listener.stop_user_listener(user_id)
    await update.message.reply_text(
        "✅ Logged out successfully.\n"
        "Your session has been revoked.\n\n"
        "Use /login to log in again."
    )


# ── /status ───────────────────────────────────────────────────────

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id  = str(update.effective_user.id)
    user_doc = await get_user(user_id)
    if user_doc.get("session_string"):
        try:
            client = await get_client(user_id)
            if await client.is_user_authorized():
                me = await client.get_me()
                await client.disconnect()
                count = await count_pipelines(user_id)
                await update.message.reply_text(
                    f"✅ Logged in as *{me.first_name}* (@{me.username})\n"
                    f"📋 Active pipelines: *{count}* / {MAX_PIPELINES}",
                    parse_mode=ParseMode.MARKDOWN,
                )
                return
            await client.disconnect()
        except Exception:
            pass
    await update.message.reply_text("❌ Not logged in. Use /login to authenticate.")


# ================================================================
# LOGIN CONVERSATION
# ================================================================

async def login_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id  = str(update.effective_user.id)
    user_doc = await get_user(user_id)
    if user_doc.get("session_string"):
        try:
            client = await get_client(user_id)
            if await client.is_user_authorized():
                await client.disconnect()
                await update.message.reply_text(
                    "✅ You are already logged in.\n\nUse /status to check."
                )
                return ConversationHandler.END
            await client.disconnect()
        except Exception:
            pass
    await _cleanup_login_state(user_id)
    LOGIN_STATE[user_id] = {"step": "WAITING_PHONE", "data": {}}
    await update.message.reply_text(
        "📱 *Login — Step 1 of 3*\n\n"
        "Send your phone number with country code.\n\n"
        "📎 Example: `+919876543210`\n\n"
        "Or /cancel to abort.",
        parse_mode=ParseMode.MARKDOWN,
    )
    return LOGIN_PHONE


async def login_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    phone   = update.message.text.strip().replace(" ", "")
    if not re.match(r"^\+\d{7,15}$", phone):
        await update.message.reply_text("❌ Invalid format. Use +1234567890.")
        return LOGIN_PHONE
    await update.message.reply_text("📩 Sending OTP…")
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()
    try:
        sent = await client.send_code_request(phone)
        LOGIN_STATE[user_id] = {
            "step": "WAITING_CODE",
            "data": {"client": client, "phone": phone, "hash": sent.phone_code_hash},
        }
        await set_user_field(user_id, "phone_number", phone)
        await update.message.reply_text(
            "📲 *Login — Step 2 of 3*\n\n"
            "A verification code was sent to your Telegram app.\n\n"
            "Send it here (spaces are fine):\n`1 2 3 4 5` or `12345`\n\n"
            "Or /cancel to abort.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return LOGIN_OTP
    except PhoneNumberInvalidError:
        await client.disconnect()
        del LOGIN_STATE[user_id]
        await update.message.reply_text("❌ Phone number is invalid. Try again.")
        return LOGIN_PHONE
    except Exception as e:
        await client.disconnect()
        del LOGIN_STATE[user_id]
        await update.message.reply_text(f"❌ Failed to send OTP: {e}\n\nTry /login again.")
        return ConversationHandler.END


async def login_otp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    otp     = update.message.text.strip().replace(" ", "")
    if not otp.isdigit():
        await update.message.reply_text("❌ Send only the numeric code.")
        return LOGIN_OTP
    state = LOGIN_STATE.get(user_id)
    if not state or state["step"] != "WAITING_CODE":
        await update.message.reply_text("❌ Session lost. Please /login again.")
        return ConversationHandler.END
    client     = state["data"]["client"]
    phone      = state["data"]["phone"]
    phone_hash = state["data"]["hash"]
    try:
        await client.sign_in(phone, otp, phone_code_hash=phone_hash)
        await _finalize_login(update, client, user_id, context)
        return ConversationHandler.END
    except PhoneCodeInvalidError:
        await update.message.reply_text("❌ OTP is incorrect. Try again.")
        return LOGIN_OTP
    except PhoneCodeExpiredError:
        await client.disconnect()
        del LOGIN_STATE[user_id]
        await update.message.reply_text("❌ OTP expired. Please /login again.")
        return ConversationHandler.END
    except SessionPasswordNeededError:
        LOGIN_STATE[user_id]["step"] = "WAITING_PASSWORD"
        await update.message.reply_text(
            "🔐 *Login — Step 3 of 3*\n\n"
            "Two-step verification is enabled.\n\n"
            "Please send your 2FA password.\n\n"
            "Or /cancel to abort.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return LOGIN_2FA
    except Exception as e:
        await client.disconnect()
        del LOGIN_STATE[user_id]
        await update.message.reply_text(f"❌ Error: {e}\n\nPlease /login again.")
        return ConversationHandler.END


async def login_2fa(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id  = str(update.effective_user.id)
    password = update.message.text.strip()
    state    = LOGIN_STATE.get(user_id)
    if not state or state["step"] != "WAITING_PASSWORD":
        await update.message.reply_text("❌ Session lost. Please /login again.")
        return ConversationHandler.END
    client = state["data"]["client"]
    try:
        await client.sign_in(password=password)
        await _finalize_login(update, client, user_id, context)
        return ConversationHandler.END
    except PasswordHashInvalidError:
        await update.message.reply_text("❌ Wrong password. Try again.")
        return LOGIN_2FA
    except Exception as e:
        await client.disconnect()
        del LOGIN_STATE[user_id]
        await update.message.reply_text(f"❌ 2FA error: {e}\n\nPlease /login again.")
        return ConversationHandler.END


async def _finalize_login(
    update: Update,
    client: TelegramClient,
    user_id: str,
    context: ContextTypes.DEFAULT_TYPE,
):
    try:
        me = await client.get_me()
        await save_session(user_id, client)
        await client.disconnect()
        LOGIN_STATE.pop(user_id, None)
        await update.message.reply_text(
            f"✅ *Logged in successfully!*\n\n"
            f"👤 Name: {me.first_name}\n"
            f"🔖 Username: @{me.username}\n\n"
            "Your session is saved. Use /addpipeline to set up forwarding!\n\n"
            "_If you ever get an auth error, use /logout then /login again._",
            parse_mode=ParseMode.MARKDOWN,
        )
        # Start listener for this user (may have existing pipelines)
        bot = context.application.bot
        await listener.start_user_listener(user_id, bot)
    except Exception as e:
        LOGIN_STATE.pop(user_id, None)
        await update.message.reply_text(f"❌ Error finalising login: {e}")


# ================================================================
# /addpipeline CONVERSATION
# ================================================================

async def addpipeline_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id  = str(update.effective_user.id)
    user_doc = await get_user(user_id)

    if not user_doc.get("session_string"):
        await update.message.reply_text(
            "❌ *Not logged in.*\n\nUse /login first.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return ConversationHandler.END

    count = await count_pipelines(user_id)
    if count >= MAX_PIPELINES:
        await update.message.reply_text(
            f"❌ You already have *{MAX_PIPELINES}* pipelines (the maximum).\n\n"
            "Use /mypipelines to delete one first.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return ConversationHandler.END

    context.user_data.clear()
    context.user_data["ap_user_id"] = user_id

    await update.message.reply_text(
        "🔁 *Add Pipeline — Step 1 of 4*\n\n"
        "Forward any message from your *SOURCE* private channel or group "
        "to this chat.\n\n"
        "This lets me detect the channel ID automatically.\n\n"
        "Or /cancel to abort.",
        parse_mode=ParseMode.MARKDOWN,
    )
    return AP_SOURCE


async def ap_source(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Receive a forwarded message from the source channel."""
    msg = update.message

    # Try telegram's forward_origin first, then legacy forward_from_chat
    fwd_chat = None
    if hasattr(msg, "forward_origin") and msg.forward_origin:
        fwd_chat = getattr(msg.forward_origin, "chat", None)
    if fwd_chat is None:
        fwd_chat = getattr(msg, "forward_from_chat", None)

    if fwd_chat is None:
        await msg.reply_text(
            "❌ That doesn't look like a forwarded message.\n\n"
            "Please *forward* a message from your source channel/group.\n\n"
            "Or /cancel to abort.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return AP_SOURCE

    raw_id    = fwd_chat.id
    chat_type = fwd_chat.type  # "channel" | "supergroup" | "group"

    if chat_type in ("channel", "supergroup"):
        abs_str     = str(abs(raw_id))
        chat_id_int = int("-" + ("100" + abs_str if not abs_str.startswith("100") else abs_str))
    else:
        chat_id_int = raw_id

    title = (
        getattr(fwd_chat, "title", None)
        or getattr(fwd_chat, "username", None)
        or str(chat_id_int)
    )[:60]

    context.user_data["source_chat_id"] = chat_id_int
    context.user_data["source_title"]   = title

    await msg.reply_text(
        f"✅ Source set: *{escape_md(title)}*\n`{chat_id_int}`\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "🔁 *Add Pipeline — Step 2 of 4*\n\n"
        "Now forward any message from your *DESTINATION* group "
        "(the group where I should send messages).\n\n"
        "Make sure I am already an *admin* in that group!\n\n"
        "Or /cancel to abort.",
        parse_mode=ParseMode.MARKDOWN,
    )
    return AP_DESTINATION


async def ap_destination(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Receive a forwarded message from the destination group."""
    msg = update.message

    fwd_chat = None
    if hasattr(msg, "forward_origin") and msg.forward_origin:
        fwd_chat = getattr(msg.forward_origin, "chat", None)
    if fwd_chat is None:
        fwd_chat = getattr(msg, "forward_from_chat", None)

    if fwd_chat is None:
        await msg.reply_text(
            "❌ That doesn't look like a forwarded message.\n\n"
            "Please *forward* a message from your destination group.\n\n"
            "Or /cancel to abort.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return AP_DESTINATION

    raw_id    = fwd_chat.id
    chat_type = fwd_chat.type

    if chat_type in ("channel", "supergroup"):
        abs_str     = str(abs(raw_id))
        chat_id_int = int("-" + ("100" + abs_str if not abs_str.startswith("100") else abs_str))
    else:
        chat_id_int = raw_id

    title = (
        getattr(fwd_chat, "title", None)
        or getattr(fwd_chat, "username", None)
        or str(chat_id_int)
    )[:60]

    context.user_data["destination_chat_id"] = chat_id_int
    context.user_data["destination_title"]   = title

    # Verify bot is admin in destination
    await msg.reply_text("🔍 Verifying bot admin status…")
    try:
        bot_member = await context.bot.get_chat_member(chat_id_int, context.bot.id)
        if bot_member.status not in ("administrator", "creator"):
            await msg.reply_text(
                "❌ I am *not an admin* in that group.\n\n"
                "Please add me as admin with *Post Messages* permission, then try again.\n\n"
                "Or /cancel to abort.",
                parse_mode=ParseMode.MARKDOWN,
            )
            return AP_DESTINATION
    except Exception as e:
        await msg.reply_text(
            f"❌ Could not verify my admin status: `{e}`\n\n"
            "Make sure I am an admin in the group and try again.\n\n"
            "Or /cancel to abort.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return AP_DESTINATION

    # Fetch topics using Telethon
    await msg.reply_text("📋 Fetching topics from the group…")
    user_id = context.user_data["ap_user_id"]
    try:
        client = await get_client(user_id)
        if not await client.is_user_authorized():
            await client.disconnect()
            await msg.reply_text("❌ Session expired. Please /login again.")
            return ConversationHandler.END

        result = await client(GetForumTopicsRequest(
            channel=chat_id_int,
            offset_date=0,
            offset_id=0,
            offset_topic=0,
            limit=100,
        ))
        await client.disconnect()

        topics = result.topics  # list of ForumTopic
        if not topics:
            await msg.reply_text(
                "❌ No topics found in that group.\n\n"
                "Make sure the group has *Topics* enabled.\n\n"
                "Or /cancel to abort.",
                parse_mode=ParseMode.MARKDOWN,
            )
            return AP_DESTINATION

        # Store topics list for later steps
        context.user_data["topics"] = [
            {"id": t.id, "title": t.title} for t in topics
        ]

    except Exception as e:
        await msg.reply_text(
            f"❌ Could not fetch topics: `{e}`\n\n"
            "Make sure the group has Topics enabled and I am a member.\n\n"
            "Or /cancel to abort.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return AP_DESTINATION

    # Show topic picker for general messages
    topics     = context.user_data["topics"]
    lines      = [
        f"✅ Destination set: *{escape_md(title)}*\n\n",
        "━━━━━━━━━━━━━━━━━━\n",
        "🔁 *Add Pipeline — Step 3 of 4*\n\n",
        "Which topic should *all messages* go to?\n\n",
    ]
    for i, t in enumerate(topics, 1):
        lines.append(f"*{i}.* {t['title']}")
    lines.append("\n\nReply with the *number* of your choice. Or /cancel to abort.")

    await msg.reply_text("".join(lines), parse_mode=ParseMode.MARKDOWN)
    return AP_GENERAL_TOPIC


async def ap_general_topic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User picks the general topic."""
    raw    = update.message.text.strip()
    topics = context.user_data.get("topics", [])

    if not raw.isdigit() or not (1 <= int(raw) <= len(topics)):
        await update.message.reply_text(
            f"❌ Please reply with a number between *1* and *{len(topics)}*.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return AP_GENERAL_TOPIC

    chosen = topics[int(raw) - 1]
    context.user_data["general_topic_id"]   = chosen["id"]
    context.user_data["general_topic_name"] = chosen["title"]

    # Build topic list again for forms picker (exclude already chosen)
    lines = [
        f"✅ General topic set: *{escape_md(chosen['title'])}*\n\n",
        "━━━━━━━━━━━━━━━━━━\n",
        "🔁 *Add Pipeline — Step 4 of 4 (optional)*\n\n",
        "Should *Google Form links* go to a separate topic?\n\n",
        "Pick a topic number, or send *0* to skip:\n\n",
    ]
    for i, t in enumerate(topics, 1):
        lines.append(f"*{i}.* {t['title']}")
    lines.append("\n\nOr /cancel to abort.")

    await update.message.reply_text("".join(lines), parse_mode=ParseMode.MARKDOWN)
    return AP_FORMS_TOPIC


async def ap_forms_topic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User picks the forms topic (or 0 to skip), then pipeline is saved."""
    raw    = update.message.text.strip()
    topics = context.user_data.get("topics", [])

    if not raw.isdigit() or not (0 <= int(raw) <= len(topics)):
        await update.message.reply_text(
            f"❌ Reply with *0* to skip, or a number between *1* and *{len(topics)}*.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return AP_FORMS_TOPIC

    num = int(raw)
    if num == 0:
        forms_topic_id   = None
        forms_topic_name = None
        forms_label      = "_not set_"
    else:
        chosen           = topics[num - 1]
        forms_topic_id   = chosen["id"]
        forms_topic_name = chosen["title"]
        forms_label      = f"*{escape_md(chosen['title'])}*"

    context.user_data["forms_topic_id"]   = forms_topic_id
    context.user_data["forms_topic_name"] = forms_topic_name

    # Save pipeline to MongoDB
    user_id = context.user_data["ap_user_id"]
    try:
        pipeline_id = await add_pipeline(
            user_id              = user_id,
            source_chat_id       = context.user_data["source_chat_id"],
            source_title         = context.user_data["source_title"],
            destination_chat_id  = context.user_data["destination_chat_id"],
            destination_title    = context.user_data["destination_title"],
            general_topic_id     = context.user_data["general_topic_id"],
            general_topic_name   = context.user_data["general_topic_name"],
            forms_topic_id       = forms_topic_id,
            forms_topic_name     = forms_topic_name,
        )
    except ValueError as e:
        await update.message.reply_text(f"❌ {e}")
        return ConversationHandler.END
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to save pipeline: {e}")
        return ConversationHandler.END

    await update.message.reply_text(
        "✅ *Pipeline saved!*\n\n"
        f"📥 Source: *{escape_md(context.user_data['source_title'])}*\n"
        f"📤 Destination: *{escape_md(context.user_data['destination_title'])}*\n"
        f"📌 General topic: *{escape_md(context.user_data['general_topic_name'])}*\n"
        f"📝 Forms topic: {forms_label}\n\n"
        "🟢 Forwarding is now *active*!\n\n"
        "Use /mypipelines to manage your pipelines.",
        parse_mode=ParseMode.MARKDOWN,
    )

    # Reload listener to include this new source
    bot = context.application.bot
    await listener.reload_user_listener(user_id, bot)

    return ConversationHandler.END


# ================================================================
# /mypipelines — list + manage
# ================================================================

_PL_PREFIX  = "pl:"      # view a pipeline
_DEL_PREFIX = "pldel:"   # delete a pipeline
_TOG_PREFIX = "pltog:"   # toggle pause/resume
_BACK_CB    = "plback"   # back to list


def _pipeline_list_keyboard(pipelines: list) -> InlineKeyboardMarkup:
    buttons = []
    for p in pipelines:
        status = "🟢" if p.get("is_active") else "🔴"
        label  = f"{status} {p['source_title']} → {p['destination_title']}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"{_PL_PREFIX}{p['_id']}")])
    return InlineKeyboardMarkup(buttons)


def _pipeline_detail_keyboard(pipeline_id: str, is_active: bool) -> InlineKeyboardMarkup:
    toggle_label = "⏸ Pause" if is_active else "▶️ Resume"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(toggle_label, callback_data=f"{_TOG_PREFIX}{pipeline_id}")],
        [InlineKeyboardButton("🗑 Delete",   callback_data=f"{_DEL_PREFIX}{pipeline_id}")],
        [InlineKeyboardButton("⬅️ Back",     callback_data=_BACK_CB)],
    ])


async def mypipelines(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id   = str(update.effective_user.id)
    pipelines = await get_pipelines(user_id)
    if not pipelines:
        await update.message.reply_text(
            "📋 *Your Pipelines*\n\nYou have no pipelines yet.\n\n"
            "Use /addpipeline to create one.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    await update.message.reply_text(
        f"📋 *Your Pipelines* ({len(pipelines)}/{MAX_PIPELINES})\n\n"
        "Tap a pipeline to manage it.\n"
        "🟢 = active  |  🔴 = paused",
        reply_markup=_pipeline_list_keyboard(pipelines),
        parse_mode=ParseMode.MARKDOWN,
    )


async def pipelines_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    user_id = str(update.effective_user.id)
    data    = query.data

    # ── Back to list ──────────────────────────────────────────────
    if data == _BACK_CB:
        pipelines = await get_pipelines(user_id)
        if not pipelines:
            await query.edit_message_text(
                "📋 *Your Pipelines*\n\nNo pipelines. Use /addpipeline to create one.",
                parse_mode=ParseMode.MARKDOWN,
            )
            return
        await query.edit_message_text(
            f"📋 *Your Pipelines* ({len(pipelines)}/{MAX_PIPELINES})\n\n"
            "Tap a pipeline to manage it.\n"
            "🟢 = active  |  🔴 = paused",
            reply_markup=_pipeline_list_keyboard(pipelines),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # ── View detail ───────────────────────────────────────────────
    if data.startswith(_PL_PREFIX):
        pid      = data[len(_PL_PREFIX):]
        from db import get_pipeline
        pipeline = await get_pipeline(pid)
        if not pipeline:
            await query.edit_message_text("❌ Pipeline not found.")
            return
        status     = "🟢 Active" if pipeline.get("is_active") else "🔴 Paused"
        forms_line = (
            f"📝 Forms topic: *{escape_md(pipeline['forms_topic_name'])}*\n"
            if pipeline.get("forms_topic_id")
            else "📝 Forms topic: _not set_\n"
        )
        await query.edit_message_text(
            f"🔁 *Pipeline Details*\n\n"
            f"📥 Source: *{escape_md(pipeline['source_title'])}*\n"
            f"📤 Destination: *{escape_md(pipeline['destination_title'])}*\n"
            f"📌 General topic: *{escape_md(pipeline['general_topic_name'])}*\n"
            f"{forms_line}"
            f"Status: {status}",
            reply_markup=_pipeline_detail_keyboard(pid, pipeline.get("is_active", True)),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # ── Toggle pause/resume ───────────────────────────────────────
    if data.startswith(_TOG_PREFIX):
        pid      = data[len(_TOG_PREFIX):]
        from db import get_pipeline
        pipeline = await get_pipeline(pid)
        if not pipeline:
            await query.edit_message_text("❌ Pipeline not found.")
            return
        new_state = not pipeline.get("is_active", True)
        await toggle_pipeline(pid, new_state)
        action = "resumed 🟢" if new_state else "paused 🔴"
        await query.answer(f"Pipeline {action}!", show_alert=False)
        # Reload listener to reflect new state
        await listener.reload_user_listener(user_id, context.application.bot)
        # Refresh detail view
        pipeline["is_active"] = new_state
        status     = "🟢 Active" if new_state else "🔴 Paused"
        forms_line = (
            f"📝 Forms topic: *{escape_md(pipeline['forms_topic_name'])}*\n"
            if pipeline.get("forms_topic_id")
            else "📝 Forms topic: _not set_\n"
        )
        await query.edit_message_text(
            f"🔁 *Pipeline Details*\n\n"
            f"📥 Source: *{escape_md(pipeline['source_title'])}*\n"
            f"📤 Destination: *{escape_md(pipeline['destination_title'])}*\n"
            f"📌 General topic: *{escape_md(pipeline['general_topic_name'])}*\n"
            f"{forms_line}"
            f"Status: {status}",
            reply_markup=_pipeline_detail_keyboard(pid, new_state),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # ── Delete ────────────────────────────────────────────────────
    if data.startswith(_DEL_PREFIX):
        pid = data[len(_DEL_PREFIX):]
        await delete_pipeline(pid)
        await listener.reload_user_listener(user_id, context.application.bot)
        pipelines = await get_pipelines(user_id)
        if not pipelines:
            await query.edit_message_text(
                "🗑 Pipeline deleted.\n\n"
                "📋 No more pipelines. Use /addpipeline to create one.",
                parse_mode=ParseMode.MARKDOWN,
            )
            return
        await query.edit_message_text(
            f"🗑 Pipeline deleted.\n\n"
            f"📋 *Your Pipelines* ({len(pipelines)}/{MAX_PIPELINES})\n\n"
            "Tap a pipeline to manage it.",
            reply_markup=_pipeline_list_keyboard(pipelines),
            parse_mode=ParseMode.MARKDOWN,
        )
        return


# ===================== SELF-PING =========================

async def health_handler(request):
    return web.Response(text="OK")


async def start_ping_server():
    app_web = web.Application()
    app_web.router.add_get("/",       health_handler)
    app_web.router.add_get("/health", health_handler)
    runner = web.AppRunner(app_web)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PING_PORT)
    await site.start()
    print(f"🌐 Health server running on port {PING_PORT}")


async def self_ping_loop():
    if not RENDER_URL:
        print("⚠️  RENDER_EXTERNAL_URL not set — self-ping disabled.")
        return
    url = RENDER_URL.rstrip("/") + "/health"
    print(f"🔁 Self-ping → {url} every {PING_INTERVAL}s")
    async with aiohttp.ClientSession() as session:
        while True:
            await asyncio.sleep(PING_INTERVAL)
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    print(f"  🏓 Ping → {resp.status}")
            except Exception as e:
                print(f"  ⚠️  Ping failed: {e}")


# ======================== MAIN ============================

def main():
    import httpx
    try:
        httpx.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook",
            params={"drop_pending_updates": True},
            timeout=10,
        )
        print("✅ Webhook cleared.")
    except Exception as e:
        print(f"⚠️  Could not clear webhook: {e}")

    async def post_init(application):
        await start_ping_server()
        application.bot_data["ping_task"] = asyncio.create_task(self_ping_loop())
        # Resume all active pipelines from MongoDB
        bot = application.bot
        await listener.start_all_listeners(bot)

    async def post_shutdown(application):
        ping_task = application.bot_data.get("ping_task")
        if ping_task and not ping_task.done():
            ping_task.cancel()
            try:
                await ping_task
            except asyncio.CancelledError:
                pass
        try:
            await close_db()
            print("✅ DB closed.")
        except Exception as e:
            print(f"⚠️  DB close error: {e}")

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .request(HTTPXRequest(
            connection_pool_size = 8,
            read_timeout         = 60,
            write_timeout        = 60,
            connect_timeout      = 30,
            pool_timeout         = 30,
        ))
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    # ── Login conversation ──────────────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("login", login_start)],
        states={
            LOGIN_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, login_phone)],
            LOGIN_OTP:   [MessageHandler(filters.TEXT & ~filters.COMMAND, login_otp)],
            LOGIN_2FA:   [MessageHandler(filters.TEXT & ~filters.COMMAND, login_2fa)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    ))

    # ── Add pipeline conversation ───────────────────────────────────
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("addpipeline", addpipeline_start)],
        states={
            AP_SOURCE: [MessageHandler(
                filters.FORWARDED & ~filters.COMMAND, ap_source
            )],
            AP_DESTINATION: [MessageHandler(
                filters.FORWARDED & ~filters.COMMAND, ap_destination
            )],
            AP_GENERAL_TOPIC: [MessageHandler(
                filters.TEXT & ~filters.COMMAND, ap_general_topic
            )],
            AP_FORMS_TOPIC: [MessageHandler(
                filters.TEXT & ~filters.COMMAND, ap_forms_topic
            )],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    ))

    # ── Pipeline management (inline buttons) ────────────────────────
    app.add_handler(CallbackQueryHandler(
        pipelines_callback,
        pattern=f"^({_PL_PREFIX}|{_DEL_PREFIX}|{_TOG_PREFIX}|{_BACK_CB})",
    ))

    # ── Standalone commands ─────────────────────────────────────────
    app.add_handler(CommandHandler("start",        start))
    app.add_handler(CommandHandler("status",       status))
    app.add_handler(CommandHandler("logout",       logout))
    app.add_handler(CommandHandler("mypipelines",  mypipelines))
    app.add_handler(CommandHandler("cancel",       cancel))

    print("🤖 Forwarder bot is running…")
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
