"""S3 operations using aioboto3 (async). Session download, attachment and profile photo upload."""

from __future__ import annotations

import logging

lg = logging.getLogger(__name__)

SESSION_PREFIX = "sessions"
PROFILE_PHOTOS_PREFIX = "profile-photos"
CHATS_ATTACHMENTS_PREFIX = "chats"

# async def upload_telethon_session_file(
# filepath: str,
# bucket: str,
# peer_id: int,
# region: str,
# *,
# access_key: str | None = None,
# secret_key: str | None = None,
# ) -> str:
# """Upload a Telethon session file to S3. Returns s3://bucket/key."""
# key = session_s3_key(peer_id)
# session = aioboto3.Session()
# extra = {}
# if access_key and secret_key:
# extra["aws_access_key_id"] = access_key
# extra["aws_secret_access_key"] = secret_key
# async with session.client("s3", region_name=region, **extra) as s3:
# await s3.upload_file(filepath, bucket, key)
# lg.info("Uploaded Telethon session for peer_id=%s to s3://%s/%s", peer_id, bucket, key)
# return f"s3://{bucket}/{key}"
