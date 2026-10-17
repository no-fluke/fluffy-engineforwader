"""
listener.py  –  Telethon live listener
=======================================
One Telethon client per user.
Each client watches ALL of that user's source chats in a single
NewMessage handler using an O(1) lookup dict.

Public API (called from bot.py):
  await start_user_listener(user_id, bot)
  await stop_user_listener(user_id)
  await reload_user_listener(user_id, bot)   ← call after add/delete pipeline
  await start_all_listeners(bot)             ← called once on bot startup
"""

import asyncio
import os

from telethon import TelegramClient, events
from telethon.sessions import StringSession

import forwarder
from db import get_user, get_pipelines, get_all_active_pipelines, set_user_field

API_ID   = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "")

# ── In-memory registry ────────────────────────────────────────────
# { user_id: {"client": TelegramClient, "task": asyncio.Task, "lookup": {source_chat_id: pipeline}} }
_registry: dict[str, dict] = {}


# ── Internal helpers ──────────────────────────────────────────────

async def _build_client(user_id: str) -> TelegramClient | None:
    """Create and connect a Telethon client for this user."""
    user_doc    = await get_user(user_id)
    session_str = user_doc.get("session_string", "")
    if not session_str:
        print(f"  [listener] No session for user {user_id} — skipping")
        return None

    client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
    await client.connect()

    if not await client.is_user_authorized():
        print(f"  [listener] Session expired for user {user_id} — skipping")
        await client.disconnect()
        return None

    return client


def _make_handler(lookup: dict, bot):
    """
    Return a Telethon NewMessage handler that routes messages
    using the lookup dict: { source_chat_id: pipeline_doc }
    """
    async def _on_message(event):
        chat_id  = event.chat_id
        pipeline = lookup.get(chat_id)
        if pipeline is None or not pipeline.get("is_active"):
            return
        # Fire-and-forget so the listener loop is never blocked
        asyncio.create_task(
            forwarder.handle(bot, event.message, pipeline)
        )

    return _on_message


async def _run_listener(user_id: str, bot):
    """
    Long-running coroutine for one user.
    Builds the client, registers ONE handler for ALL sources,
    then keeps the client connected until cancelled.
    """
    pipelines = await get_pipelines(user_id)
    active    = [p for p in pipelines if p.get("is_active")]

    if not active:
        print(f"  [listener] user {user_id}: no active pipelines — idle")
        # Stay alive so reload_user_listener can restart properly
        try:
            await asyncio.Future()   # sleep forever until cancelled
        except asyncio.CancelledError:
            pass
        return

    client = await _build_client(user_id)
    if client is None:
        return

    # Store client in registry so reload can disconnect it
    if user_id in _registry:
        _registry[user_id]["client"] = client

    # Build O(1) lookup dict
    lookup: dict[int, dict] = {
        p["source_chat_id"]: p for p in active
    }

    source_ids = list(lookup.keys())
    print(
        f"  [listener] user {user_id}: watching "
        f"{len(source_ids)} source(s): {source_ids}"
    )

    handler = _make_handler(lookup, bot)
    client.add_event_handler(
        handler,
        events.NewMessage(chats=source_ids),
    )

    try:
        await client.run_until_disconnected()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"  [listener] user {user_id} client error: {e}")
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass
        print(f"  [listener] user {user_id}: client disconnected")


# ── Public API ────────────────────────────────────────────────────

async def start_user_listener(user_id: str, bot):
    """Start a background listener task for this user."""
    if user_id in _registry:
        print(f"  [listener] user {user_id}: already running — skipping")
        return

    task = asyncio.create_task(_run_listener(user_id, bot))
    _registry[user_id] = {"client": None, "task": task}

    def _on_done(t):
        # Auto-clean registry when task ends unexpectedly
        _registry.pop(user_id, None)
        if not t.cancelled() and t.exception():
            print(f"  [listener] user {user_id} task died: {t.exception()}")

    task.add_done_callback(_on_done)
    print(f"  [listener] user {user_id}: listener started")


async def stop_user_listener(user_id: str):
    """Cancel and clean up a user's listener task."""
    entry = _registry.pop(user_id, None)
    if entry is None:
        return

    # Disconnect the Telethon client first so run_until_disconnected returns
    client = entry.get("client")
    if client:
        try:
            await client.disconnect()
        except Exception:
            pass

    task = entry.get("task")
    if task and not task.done():
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    print(f"  [listener] user {user_id}: listener stopped")


async def reload_user_listener(user_id: str, bot):
    """
    Stop then restart a user's listener.
    Call this after adding or deleting a pipeline.
    """
    print(f"  [listener] user {user_id}: reloading…")
    await stop_user_listener(user_id)
    await asyncio.sleep(1)   # brief pause before reconnect
    await start_user_listener(user_id, bot)


async def start_all_listeners(bot):
    """
    Called once when the bot starts.
    Loads every active pipeline from MongoDB and starts one
    listener task per unique user_id.
    """
    all_pipelines = await get_all_active_pipelines()
    user_ids = list({p["user_id"] for p in all_pipelines})

    print(f"  [listener] startup: found {len(user_ids)} user(s) with active pipelines")
    for uid in user_ids:
        await start_user_listener(uid, bot)
