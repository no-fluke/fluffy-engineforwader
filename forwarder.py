"""
forwarder.py  –  Classify a message and copy it to the correct topic
=====================================================================
Called by listener.py for every new message in a source chat.

The BOT account does all sending — the user's Telethon client is used
only to *read* the source channel (which the bot cannot join directly).

Routing logic:
  - Contains "docs.google.com/forms" AND forms_topic_id set → forms topic
  - Everything else                                         → general topic
"""

import asyncio
import io
import random

from telethon.errors import FloodWaitError
from telethon.tl.types import (
    MessageMediaPhoto,
    MessageMediaDocument,
    MessageMediaWebPage,
)

from telegram.error import RetryAfter, TelegramError

# ── Delay: 60–100 seconds ─────────────────────────────────────────
DELAY_MIN = 60
DELAY_MAX = 100

GOOGLE_FORMS_DOMAIN = "docs.google.com/forms"


def _is_google_form(text: str) -> bool:
    return GOOGLE_FORMS_DOMAIN in (text or "")


def _pick_topic(message_text: str, pipeline: dict) -> int:
    if _is_google_form(message_text) and pipeline.get("forms_topic_id"):
        return pipeline["forms_topic_id"]
    return pipeline["general_topic_id"]


async def _bot_send(coro, retries: int = 5):
    """Retry wrapper for python-telegram-bot calls."""
    for attempt in range(retries):
        try:
            return await coro
        except RetryAfter as e:
            wait = e.retry_after + random.uniform(5, 15)
            print(f"  ⚠️  Bot rate-limit: sleeping {wait:.1f}s")
            await asyncio.sleep(wait)
        except TelegramError as e:
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt + random.uniform(1, 3))
            else:
                raise
    raise RuntimeError(f"_bot_send failed after {retries} attempts")


async def handle(bot, client, telethon_message, pipeline: dict):
    """
    bot              : python-telegram-bot Bot — does ALL sending
    client           : the already-connected Telethon client (read-only use)
    telethon_message : Telethon Message object
    pipeline         : pipeline doc from MongoDB
    """
    # ── 0. Skip PDFs ─────────────────────────────────────────────
    media_obj = telethon_message.media
    if media_obj and hasattr(media_obj, "document"):
        for attr in getattr(media_obj.document, "attributes", []):
            if hasattr(attr, "file_name") and (attr.file_name or "").lower().endswith(".pdf"):
                print(f"  ⏭️  msg #{telethon_message.id} is a PDF — skipping")
                return

    # ── 1. Random delay ───────────────────────────────────────────
    delay = random.randint(DELAY_MIN, DELAY_MAX)
    print(
        f"  ⏳  [{pipeline['source_title']}] msg #{telethon_message.id} "
        f"— waiting {delay}s before forwarding"
    )
    await asyncio.sleep(delay)

    # ── 2. Route ─────────────────────────────────────────────────
    text        = telethon_message.message or ""
    thread_id   = _pick_topic(text, pipeline)
    topic_label = (
        pipeline.get("forms_topic_name", "Forms")
        if (_is_google_form(text) and pipeline.get("forms_topic_id"))
        else pipeline.get("general_topic_name", "General")
    )
    dest_chat_id = pipeline["destination_chat_id"]

    print(
        f"  📤  [{pipeline['source_title']}] → "
        f"[{pipeline['destination_title']}] topic={topic_label} "
        f"msg_id={telethon_message.id}"
    )

    # ── 3. Decide what to send ────────────────────────────────────
    media = telethon_message.media

    # Web-page previews are auto-generated from the URL — treat as text only
    if isinstance(media, MessageMediaWebPage):
        media = None

    try:
        if media is None:
            # ── Text only ────────────────────────────────────────
            if not text:
                print(f"  ⚠️  msg #{telethon_message.id} has no text and no media — skipping")
                return
            await _bot_send(
                bot.send_message(
                    chat_id=dest_chat_id,
                    text=text,
                    message_thread_id=thread_id,
                )
            )

        elif isinstance(media, MessageMediaPhoto):
            # ── Photo ────────────────────────────────────────────
            file_bytes = await client.download_media(media, bytes)
            await _bot_send(
                bot.send_photo(
                    chat_id=dest_chat_id,
                    photo=io.BytesIO(file_bytes),
                    caption=text or None,
                    message_thread_id=thread_id,
                )
            )

        elif isinstance(media, MessageMediaDocument):
            # ── Document / video / audio / sticker / gif ─────────
            file_bytes = await client.download_media(media, bytes)
            buf        = io.BytesIO(file_bytes)

            # Preserve filename
            filename = None
            for attr in getattr(media.document, "attributes", []):
                if hasattr(attr, "file_name") and attr.file_name:
                    filename = attr.file_name
                    break
            if filename:
                buf.name = filename

            mime = getattr(media.document, "mime_type", "") or ""

            if mime.startswith("video/"):
                await _bot_send(
                    bot.send_video(
                        chat_id=dest_chat_id,
                        video=buf,
                        caption=text or None,
                        message_thread_id=thread_id,
                    )
                )
            elif mime.startswith("audio/"):
                await _bot_send(
                    bot.send_audio(
                        chat_id=dest_chat_id,
                        audio=buf,
                        caption=text or None,
                        message_thread_id=thread_id,
                    )
                )
            else:
                await _bot_send(
                    bot.send_document(
                        chat_id=dest_chat_id,
                        document=buf,
                        caption=text or None,
                        message_thread_id=thread_id,
                    )
                )

        else:
            # ── Unknown media type — fall back to text if available ──
            if text:
                await _bot_send(
                    bot.send_message(
                        chat_id=dest_chat_id,
                        text=text,
                        message_thread_id=thread_id,
                    )
                )
            else:
                print(f"  ⚠️  msg #{telethon_message.id} unsupported media type — skipping")
                return

        print(f"  ✅  Bot delivered to topic '{topic_label}' (thread {thread_id})")

    except RetryAfter as e:
        wait = e.retry_after + 10
        print(f"  ⚠️  FloodWait: {e.retry_after}s — sleeping {wait}s then dropping msg")
        await asyncio.sleep(wait)
        print(f"  ⚠️  msg #{telethon_message.id} dropped after flood wait (will not retry)")

    except TelegramError as e:
        print(
            f"  ❌  Bot failed to deliver msg #{telethon_message.id} "
            f"from {pipeline['source_title']}: {e}"
        )

    except FloodWaitError as e:
        wait = e.seconds + 10
        print(f"  ⚠️  Telethon FloodWait (download): {e.seconds}s — sleeping {wait}s")
        await asyncio.sleep(wait)
        print(f"  ⚠️  msg #{telethon_message.id} dropped after flood wait")

    except Exception as e:
        print(
            f"  ❌  Unexpected error for msg #{telethon_message.id} "
            f"from {pipeline['source_title']}: {e}"
        )
