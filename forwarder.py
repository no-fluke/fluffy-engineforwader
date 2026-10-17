"""
forwarder.py  –  Classify a message and copy it to the correct topic
=====================================================================
Called by listener.py for every new message in a source chat.

Uses the SAME Telethon client that the listener already has open —
no new sessions, no extra logins, no risk to the user account.

Routing logic:
  - Contains "docs.google.com/forms" AND forms_topic_id set → forms topic
  - Everything else                                         → general topic
"""

import asyncio
import io
import random

from telethon.errors import (
    FloodWaitError,
    ChatWriteForbiddenError,
    UserBannedInChannelError,
    ChannelPrivateError,
)
from telethon.tl.types import MessageMediaWebPage

# ── Delay: 1–2 minutes (60–120 seconds) ──────────────────────────
DELAY_MIN = 60
DELAY_MAX = 100

GOOGLE_FORMS_DOMAIN = "docs.google.com/forms"


def _is_google_form(text: str) -> bool:
    return GOOGLE_FORMS_DOMAIN in (text or "")


def _pick_topic(message_text: str, pipeline: dict) -> int:
    if _is_google_form(message_text) and pipeline.get("forms_topic_id"):
        return pipeline["forms_topic_id"]
    return pipeline["general_topic_id"]


async def handle(bot, client, telethon_message, pipeline: dict):
    """
    bot              : python-telegram-bot Bot (unused here, kept for API compat)
    client           : the already-connected Telethon client from listener.py
    telethon_message : Telethon Message object
    pipeline         : pipeline doc from MongoDB
    """
    # ── 0. Skip PDFs ─────────────────────────────────────────────
    media_check = telethon_message.media
    if media_check and hasattr(media_check, "document"):
        for attr in getattr(media_check.document, "attributes", []):
            if hasattr(attr, "file_name") and (attr.file_name or "").lower().endswith(".pdf"):
                print(f"  ⏭️  msg #{telethon_message.id} is a PDF — skipping")
                return

    # ── 1. Random delay (1–2 minutes) ────────────────────────────
    delay = random.randint(DELAY_MIN, DELAY_MAX)
    print(
        f"  ⏳  [{pipeline['source_title']}] msg #{telethon_message.id} "
        f"— waiting {delay}s ({delay//60}m {delay%60}s) before forwarding"
    )
    await asyncio.sleep(delay)

    # ── 2. Route ─────────────────────────────────────────────────
    text        = telethon_message.text or getattr(telethon_message, "caption", "") or ""
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

    # ── 3. Try native forward first (no re-upload, safer) ────────
    try:
        await client.forward_messages(
            dest_chat_id,
            telethon_message,
            reply_to=thread_id,
        )
        print(f"  ✅  Forwarded natively to topic '{topic_label}' (thread {thread_id})")
        return

    except FloodWaitError as e:
        wait = e.seconds + 10
        print(f"  ⚠️  FloodWait on forward: Telegram demands {e.seconds}s — sleeping {wait}s")
        await asyncio.sleep(wait)
        # Fall through to re-upload fallback after flood wait

    except (ChatWriteForbiddenError, UserBannedInChannelError, ChannelPrivateError) as e:
        print(f"  ❌  Cannot write to destination ({type(e).__name__}) — skipping")
        return

    except Exception:
        # Source restricts forwarding — fall through to download+reupload
        pass

    # ── 4. Fallback: download → re-upload ────────────────────────
    try:
        media = telethon_message.media

        # Web-page previews are auto-generated from the URL — skip
        if isinstance(media, MessageMediaWebPage):
            media = None

        if media:
            file_bytes = await client.download_media(media, bytes)
            buf        = io.BytesIO(file_bytes)

            # Preserve original filename for documents
            filename = None
            if hasattr(media, "document"):
                for attr in getattr(media.document, "attributes", []):
                    if hasattr(attr, "file_name"):
                        filename = attr.file_name
                        break
            if filename:
                buf.name = filename

            await client.send_file(
                dest_chat_id,
                buf,
                caption=text or None,
                reply_to=thread_id,
            )
        else:
            if text:
                await client.send_message(
                    dest_chat_id,
                    text,
                    reply_to=thread_id,
                )
            else:
                print(f"  ⚠️  msg #{telethon_message.id} has no text and no media — skipping")
                return

        print(f"  ✅  Delivered (re-upload) to topic '{topic_label}' (thread {thread_id})")

    except FloodWaitError as e:
        wait = e.seconds + 10
        print(f"  ⚠️  FloodWait on send: Telegram demands {e.seconds}s — sleeping {wait}s")
        await asyncio.sleep(wait)
        print(f"  ⚠️  msg #{telethon_message.id} dropped after flood wait (will not retry)")

    except (ChatWriteForbiddenError, UserBannedInChannelError) as e:
        print(f"  ❌  Banned/forbidden in destination ({type(e).__name__}) — skipping msg")

    except Exception as e:
        print(
            f"  ❌  Failed to forward msg #{telethon_message.id} "
            f"from {pipeline['source_title']}: {e}"
        )
