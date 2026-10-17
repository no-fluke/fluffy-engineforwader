"""
forwarder.py  –  Classify a message and copy it to the correct topic
=====================================================================
Called by listener.py every time a new message arrives in a source chat.

Routing logic:
  - Contains "docs.google.com/forms"  AND forms_topic_id is set  → forms topic
  - Everything else                                               → general topic

Uses copy_message (not forward) so there is no "Forwarded from X" label.
Applies a random 20-50 s delay before sending to reduce ban risk.
"""

import asyncio
import random

from telegram import Bot
from telegram.error import RetryAfter, TimedOut, NetworkError

DELAY_MIN = 20
DELAY_MAX = 50

GOOGLE_FORMS_DOMAIN = "docs.google.com/forms"


def _is_google_form(text: str) -> bool:
    return GOOGLE_FORMS_DOMAIN in (text or "")


def _pick_topic(message_text: str, pipeline: dict) -> int:
    """Return the thread_id (topic) this message should go to."""
    if _is_google_form(message_text) and pipeline.get("forms_topic_id"):
        return pipeline["forms_topic_id"]
    return pipeline["general_topic_id"]


async def _safe_send(coro, retries: int = 5):
    """Retry wrapper identical to the one in bot.py."""
    for attempt in range(retries):
        try:
            return await coro
        except RetryAfter as e:
            wait = e.retry_after + random.uniform(5, 15)
            print(f"  ⚠️  FloodWait: sleeping {wait:.1f}s")
            await asyncio.sleep(wait)
        except (TimedOut, NetworkError) as e:
            wait = (2 ** attempt) + random.uniform(1, 5)
            print(f"  ⚠️  Network error ({e}), retry {attempt+1}/{retries} in {wait:.1f}s")
            await asyncio.sleep(wait)
        except Exception:
            raise
    raise RuntimeError(f"_safe_send failed after {retries} attempts")


async def handle(bot: Bot, telethon_message, pipeline: dict):
    """
    Main entry point called by listener.py for every new message.

    telethon_message : a Telethon Message object
    pipeline         : a pipeline doc from MongoDB
    """
    # ── 1. Random human-like delay ────────────────────────────────
    delay = random.randint(DELAY_MIN, DELAY_MAX)
    print(
        f"  ⏳  [{pipeline['source_title']}] msg #{telethon_message.id} "
        f"— waiting {delay}s before forwarding"
    )
    await asyncio.sleep(delay)

    # ── 2. Get message text for routing ──────────────────────────
    text = telethon_message.text or getattr(telethon_message, "caption", "") or ""

    # ── 3. Pick destination topic ─────────────────────────────────
    thread_id = _pick_topic(text, pipeline)
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

    # ── 4. Copy message (not forward) to correct topic ────────────
    try:
        await _safe_send(
            bot.copy_message(
                chat_id=dest_chat_id,
                from_chat_id=pipeline["source_chat_id"],
                message_id=telethon_message.id,
                message_thread_id=thread_id,
            )
        )
        print(f"  ✅  Delivered to topic '{topic_label}' (thread {thread_id})")

    except Exception as e:
        print(
            f"  ❌  Failed to copy msg #{telethon_message.id} "
            f"from {pipeline['source_title']}: {e}"
        )
