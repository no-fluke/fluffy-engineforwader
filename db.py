"""
db.py  –  MongoDB helpers for the Forwarder Bot
================================================
Collections:
  users         – Telethon session strings + phone
  pipelines     – source → destination + topic mapping (max 20 per user)
  allowed_users – user IDs permitted to use the bot (managed by owner)
"""

import os
from motor.motor_asyncio import AsyncIOMotorClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("MONGO_DB_NAME", "forwarder_bot")

_client = None
_db     = None

MAX_PIPELINES = 20


def get_db():
    global _client, _db
    if _db is None:
        _client = AsyncIOMotorClient(MONGO_URI)
        _db     = _client[DB_NAME]
    return _db


# ── User helpers ──────────────────────────────────────────────────

async def get_user(user_id: str) -> dict:
    doc = await get_db().users.find_one({"_id": user_id})
    return doc or {}


async def set_user_field(user_id: str, field: str, value):
    await get_db().users.update_one(
        {"_id": user_id}, {"$set": {field: value}}, upsert=True
    )


async def set_user_fields(user_id: str, fields: dict):
    await get_db().users.update_one(
        {"_id": user_id}, {"$set": fields}, upsert=True
    )


# ── Allowed-users helpers (owner-managed whitelist) ───────────────

async def is_allowed(user_id: str) -> bool:
    """Return True if this user_id is in the whitelist."""
    doc = await get_db().allowed_users.find_one({"_id": user_id})
    return doc is not None


async def add_allowed_user(user_id: str, added_by: str) -> bool:
    """
    Add user_id to the whitelist.
    Returns True if newly added, False if already present.
    """
    existing = await get_db().allowed_users.find_one({"_id": user_id})
    if existing:
        return False
    await get_db().allowed_users.insert_one({
        "_id":      user_id,
        "added_by": added_by,
    })
    return True


async def remove_allowed_user(user_id: str) -> bool:
    """
    Remove user_id from the whitelist.
    Returns True if removed, False if they weren't in the list.
    """
    result = await get_db().allowed_users.delete_one({"_id": user_id})
    return result.deleted_count > 0


async def list_allowed_users() -> list[dict]:
    """Return all whitelisted user docs."""
    cursor = get_db().allowed_users.find({})
    return await cursor.to_list(length=None)


# ── Pipeline helpers ──────────────────────────────────────────────

async def get_pipelines(user_id: str, active_only: bool = False) -> list:
    """Return pipelines for a user. Pass active_only=True to get only active ones."""
    query = {"user_id": user_id}
    if active_only:
        query["is_active"] = True
    cursor = get_db().pipelines.find(query)
    return await cursor.to_list(length=MAX_PIPELINES)


async def get_pipeline(pipeline_id: str) -> dict | None:
    """Return a single pipeline by its _id string."""
    from bson import ObjectId
    return await get_db().pipelines.find_one({"_id": ObjectId(pipeline_id)})


async def get_all_active_pipelines() -> list:
    """Return every active pipeline across all users — used on startup."""
    cursor = get_db().pipelines.find({"is_active": True})
    return await cursor.to_list(length=None)


async def count_pipelines(user_id: str) -> int:
    return await get_db().pipelines.count_documents({"user_id": user_id})


async def add_pipeline(
    user_id: str,
    source_chat_id: int,
    source_title: str,
    destination_chat_id: int,
    destination_title: str,
    general_topic_id: int,
    general_topic_name: str,
    forms_topic_id: int | None = None,
    forms_topic_name: str | None = None,
) -> str:
    """
    Insert a new pipeline doc and return its inserted _id as string.
    Raises ValueError if the user already has MAX_PIPELINES.
    """
    count = await count_pipelines(user_id)
    if count >= MAX_PIPELINES:
        raise ValueError(f"You already have {MAX_PIPELINES} pipelines (the maximum).")

    doc = {
        "user_id":              user_id,
        "source_chat_id":       source_chat_id,
        "source_title":         source_title,
        "destination_chat_id":  destination_chat_id,
        "destination_title":    destination_title,
        "general_topic_id":     general_topic_id,
        "general_topic_name":   general_topic_name,
        "forms_topic_id":       forms_topic_id,
        "forms_topic_name":     forms_topic_name,
        "is_active":            True,
    }
    result = await get_db().pipelines.insert_one(doc)
    return str(result.inserted_id)


async def delete_pipeline(pipeline_id: str):
    """Permanently delete a pipeline by its _id string."""
    from bson import ObjectId
    await get_db().pipelines.delete_one({"_id": ObjectId(pipeline_id)})


async def toggle_pipeline(pipeline_id: str, active: bool):
    """Pause or resume a pipeline without deleting it."""
    from bson import ObjectId
    await get_db().pipelines.update_one(
        {"_id": ObjectId(pipeline_id)},
        {"$set": {"is_active": active}},
    )


# ── Cleanup ───────────────────────────────────────────────────────

async def close_db():
    global _client, _db
    if _client:
        _client.close()
        _client = None
        _db     = None
