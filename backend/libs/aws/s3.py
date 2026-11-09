from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import aioboto3

from utils.request_spans import timed

lg = logging.getLogger(__name__)


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Parse ``s3://bucket/key`` into ``(bucket, key)``."""
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri!r}")
    rest = uri[5:]
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise ValueError(f"Invalid S3 URI: {uri!r}")
    return bucket, key


def make_s3_uri(bucket: str, key: str) -> str:
    """Build ``s3://bucket/key`` from bucket and key."""
    return f"s3://{bucket}/{key}"


def s3_configured(bucket: str, region: str) -> bool:
    """Return True when both bucket and region are non-empty."""
    return bool(bucket.strip() and region.strip())


async def object_exists(
    *,
    bucket: str,
    key: str,
    region: str,
    access_key: str | None = None,
    secret_key: str | None = None,
) -> bool:
    """Return True when an object exists at ``bucket/key``."""
    async with s3_client(region, access_key=access_key, secret_key=secret_key) as client:
        try:
            await client.head_object(Bucket=bucket, Key=key)
            return True
        except client.exceptions.ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code")
            if error_code in ("404", "NoSuchKey", "NotFound"):
                return False
            raise


@asynccontextmanager
async def s3_client(
    region: str,
    *,
    access_key: str | None = None,
    secret_key: str | None = None,
) -> AsyncIterator[Any]:
    """Open an aioboto3 S3 client (aioboto3 stubs omit async context manager types)."""
    if not region.strip():
        raise ValueError("AWS region is required for S3 client (set AWS_REGION)")
    session = aioboto3.Session()
    if access_key and secret_key:
        client_cm = session.client(
            "s3",
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
    else:
        client_cm = session.client("s3", region_name=region)
    async with cast(Any, client_cm) as client:
        yield client


async def upload_file(
    *,
    bucket: str,
    key: str,
    filepath: str | Path,
    region: str,
    content_type: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
) -> str:
    """Upload a local file to S3. Returns ``s3://bucket/key`` URI."""
    path = Path(filepath)
    if not path.is_file():
        raise FileNotFoundError(f"S3 upload file not found: {path}")
    extra_args = {"ContentType": content_type} if content_type else None
    async with s3_client(region, access_key=access_key, secret_key=secret_key) as client:
        with path.open("rb") as f:
            if extra_args:
                await client.upload_fileobj(f, bucket, key, ExtraArgs=extra_args)
            else:
                await client.upload_fileobj(f, bucket, key)
    return make_s3_uri(bucket, key)


async def download_s3_object_to_path(
    uri: str,
    destination: Path,
    *,
    region: str,
    client: Any | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
) -> None:
    """Stream an S3 object to a local file.

    Pass ``client`` to reuse an open aioboto3 S3 client (avoids per-file session setup).
    """
    bucket, key = parse_s3_uri(uri)
    destination.parent.mkdir(parents=True, exist_ok=True)

    async def _write(s3: Any) -> None:
        response = await s3.get_object(Bucket=bucket, Key=key)
        body = response["Body"]
        with destination.open("wb") as f:
            async for chunk in body.iter_chunks():
                f.write(chunk)

    if client is not None:
        await _write(client)
        return
    async with s3_client(region, access_key=access_key, secret_key=secret_key) as s3:
        await _write(s3)


async def download_s3_object_bytes(
    uri: str,
    *,
    region: str,
    client: Any | None = None,
) -> bytes:
    """Read an S3 object into memory.

    Pass ``client`` to reuse an open aioboto3 S3 client (avoids per-file session setup).
    """
    bucket, key = parse_s3_uri(uri)

    async def _read(s3: Any) -> bytes:
        response = await s3.get_object(Bucket=bucket, Key=key)
        return await response["Body"].read()

    if client is not None:
        return await _read(client)
    async with s3_client(region) as s3:
        return await _read(s3)


async def presign_s3_uri(
    uri: str,
    *,
    region: str,
    expires_in: int = 21_600,
) -> str:
    """Return a time-limited HTTPS URL for an ``s3://`` URI."""
    urls = await presign_s3_uris([uri], region=region, expires_in=expires_in)
    return urls[0]


async def presign_s3_uris(
    uris: list[str],
    *,
    region: str,
    expires_in: int = 21_600,
) -> list[str]:
    """Presign many ``s3://`` URIs with a single S3 client (list endpoints).

    Duplicate URIs share one signature so callers can pass a page of repeats
    (e.g. image search copies) without minting N distinct URLs — identical HTTPS
    URLs let the browser cache collapse egress.
    """
    if not uris:
        return []
    async with timed("s3", "presign"):
        async with s3_client(region) as client:
            cache: dict[str, str] = {}
            out: list[str] = []
            for uri in uris:
                signed = cache.get(uri)
                if signed is None:
                    bucket, key = parse_s3_uri(uri)
                    signed = await client.generate_presigned_url(
                        "get_object",
                        Params={"Bucket": bucket, "Key": key},
                        ExpiresIn=expires_in,
                    )
                    cache[uri] = signed
                out.append(signed)
            return out


async def presign_s3_uri_map(
    uris: list[str],
    *,
    region: str,
    expires_in: int = 21_600,
) -> dict[str, str]:
    """Presign unique ``s3://`` URIs; return ``{uri: https_url}``."""
    unique = list(dict.fromkeys(uri for uri in uris if uri.startswith("s3://")))
    if not unique:
        return {}
    urls = await presign_s3_uris(unique, region=region, expires_in=expires_in)
    return dict(zip(unique, urls, strict=True))


async def presign_harvest_urls_in_json(value: Any, *, region: str) -> Any:
    """Recursively add ``harvest.url`` presigned HTTPS links for ``harvest.s3_uri`` values."""
    if isinstance(value, dict):
        out = dict(value)
        harvest = out.get("harvest")
        if isinstance(harvest, dict):
            uri = harvest.get("s3_uri")
            if isinstance(uri, str) and uri.startswith("s3://"):
                harvest_out = dict(harvest)
                harvest_out["url"] = await presign_s3_uri(uri, region=region)
                out["harvest"] = harvest_out
        for key, child in out.items():
            if key == "harvest":
                continue
            out[key] = await presign_harvest_urls_in_json(child, region=region)
        return out
    if isinstance(value, list):
        return [await presign_harvest_urls_in_json(item, region=region) for item in value]
    return value


async def delete_object(
    *,
    bucket: str,
    key: str,
    region: str,
    access_key: str | None = None,
    secret_key: str | None = None,
) -> None:
    """Delete an S3 object (no-op if the object is already absent)."""
    async with s3_client(region, access_key=access_key, secret_key=secret_key) as client:
        await client.delete_object(Bucket=bucket, Key=key)
    lg.info("Deleted s3://%s/%s", bucket, key)
