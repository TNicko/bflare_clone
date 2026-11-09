"""Object-store backends for embedding shards.

A ``Sink`` is the minimal blob interface the writer/reader need: put bytes, get bytes, list keys. Two
implementations:

- ``S3Sink``   - real store (prod + dev read from shared S3). Uses synchronous boto3, matching the sync
  backfill/indexer services. Supports a custom ``endpoint_url`` for MinIO/dev.
- ``LocalSink`` - a local directory, for tests and offline development without S3.

Keeping IO behind this protocol means the writer/reader code is identical across S3, MinIO, and local disk.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Iterator


class Sink(Protocol):
    """Minimal blob store: byte put/get, key listing, and deletes."""

    def put_bytes(self, key: str, data: bytes) -> None: ...

    def get_bytes(self, key: str) -> bytes: ...

    def download_to_path(self, key: str, path: Path) -> int:
        """Stream ``key`` onto ``path`` (parents created). Returns bytes written."""
        ...

    def object_size(self, key: str) -> int:
        """Remote/local byte size of ``key`` (HeadObject / ``stat``)."""
        ...

    def list_keys(self, prefix: str) -> Iterator[str]: ...

    def delete_key(self, key: str) -> None: ...

    def delete_keys(self, keys: list[str]) -> int: ...

    def delete_prefix(self, prefix: str) -> int: ...


class S3Sink:
    """S3-backed sink (synchronous boto3). Pass ``endpoint_url`` for MinIO / S3-compatible dev stores."""

    def __init__(
        self,
        bucket: str,
        *,
        region: str,
        access_key: str | None = None,
        secret_key: str | None = None,
        endpoint_url: str | None = None,
        connect_timeout: float = 5.0,
        read_timeout: float = 30.0,
    ) -> None:
        import boto3
        from botocore.config import Config

        self._bucket = bucket
        session_kwargs: dict[str, object] = {"region_name": region}
        if access_key and secret_key:
            session_kwargs["aws_access_key_id"] = access_key
            session_kwargs["aws_secret_access_key"] = secret_key
        client_kwargs = dict(session_kwargs)
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
        # Default boto3 timeouts are long enough that a stalled archive GET can
        # pin an API request (and uvicorn --reload) for many minutes.
        client_kwargs["config"] = Config(
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            retries={"max_attempts": 2, "mode": "standard"},
        )
        self._client = boto3.client("s3", **client_kwargs)

    def put_bytes(self, key: str, data: bytes) -> None:
        self._client.put_object(Bucket=self._bucket, Key=key, Body=data)

    def get_bytes(self, key: str) -> bytes:
        resp = self._client.get_object(Bucket=self._bucket, Key=key)
        data: bytes = resp["Body"].read()
        return data

    def download_to_path(self, key: str, path: Path) -> int:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".partial")
        self._client.download_file(self._bucket, key, str(tmp))
        size = tmp.stat().st_size
        tmp.replace(path)
        return size

    def object_size(self, key: str) -> int:
        head = self._client.head_object(Bucket=self._bucket, Key=key)
        return int(head["ContentLength"])

    def list_keys(self, prefix: str) -> Iterator[str]:
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield obj["Key"]

    def delete_key(self, key: str) -> None:
        self._client.delete_object(Bucket=self._bucket, Key=key)

    def delete_keys(self, keys: list[str]) -> int:
        deleted = 0
        batch: list[dict[str, str]] = []
        for key in keys:
            batch.append({"Key": key})
            if len(batch) == 1000:  # S3 delete_objects caps at 1000 keys per call
                self._client.delete_objects(Bucket=self._bucket, Delete={"Objects": batch})
                deleted += len(batch)
                batch = []
        if batch:
            self._client.delete_objects(Bucket=self._bucket, Delete={"Objects": batch})
            deleted += len(batch)
        return deleted

    def delete_prefix(self, prefix: str) -> int:
        return self.delete_keys(list(self.list_keys(prefix)))


class LocalSink:
    """Filesystem-backed sink rooted at ``base_dir``. Keys map to relative paths. For tests / offline dev."""

    def __init__(self, base_dir: str | Path) -> None:
        self._base = Path(base_dir)

    def _path(self, key: str) -> Path:
        return self._base / key

    def put_bytes(self, key: str, data: bytes) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def get_bytes(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def download_to_path(self, key: str, path: Path) -> int:
        import shutil

        src = self._path(key)
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, path)
        return path.stat().st_size

    def object_size(self, key: str) -> int:
        return self._path(key).stat().st_size

    def list_keys(self, prefix: str) -> Iterator[str]:
        root = self._path(prefix)
        if not root.exists():
            return
        for path in sorted(root.rglob("*")):
            if path.is_file():
                yield str(path.relative_to(self._base))

    def delete_key(self, key: str) -> None:
        self._path(key).unlink(missing_ok=True)

    def delete_keys(self, keys: list[str]) -> int:
        deleted = 0
        for key in keys:
            path = self._path(key)
            if path.exists():
                path.unlink()
                deleted += 1
        return deleted

    def delete_prefix(self, prefix: str) -> int:
        return self.delete_keys(list(self.list_keys(prefix)))
