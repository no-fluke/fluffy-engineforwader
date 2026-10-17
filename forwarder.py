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

from telethon.tl.types import MessageMediaWebPage

DELAY_MIN = 3
DELAY_MAX = 8

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
    # ── 1. Random delay ───────────────────────────────────────────
    delay = random.randint(DELAY_MIN, DELAY_MAX)
    print(
        f"  ⏳  [{pipeline['source_title']}] msg #{telethon_message.id} "
        f"— waiting {delay}s before forwarding"
    )
    await asyncio.sleep(delay)

    # ── 2. Route ──────────────────────────────────────────────────
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

    # ── 3. Send via the listener's existing client ─────────────────
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

        print(f"  ✅  Delivered to topic '{topic_label}' (thread {thread_id})")

    except Exception as e:
        print(
            f"  ❌  Failed to forward msg #{telethon_message.id} "
            f"from {pipeline['source_title']}: {e}"
        )
