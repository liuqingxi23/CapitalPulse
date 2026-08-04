"""Shared HTTP client with retry logic, timeout, and GBK decode support."""

import asyncio
import logging
from typing import Optional

import httpx

from config import DEFAULT_HEADERS, MAX_RETRIES, REQUEST_TIMEOUT, RETRY_DELAY

logger = logging.getLogger(__name__)

# Module-level async client (created lazily)
_client: Optional[httpx.AsyncClient] = None


async def get_client() -> httpx.AsyncClient:
    """Get or create the shared async HTTP client."""
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            timeout=httpx.Timeout(REQUEST_TIMEOUT),
            headers=DEFAULT_HEADERS,
            follow_redirects=True,
        )
    return _client


async def close_client() -> None:
    """Close the shared async HTTP client."""
    global _client
    if _client is not None and not _client.is_closed:
        await _client.aclose()
        _client = None


def _decode_bytes(data: bytes) -> str:
    """
    Decode response bytes trying multiple encodings.
    Priority: gbk → utf-8 → gb2312 → gb18030 → latin-1
    """
    encodings = ["gbk", "utf-8", "gb2312", "gb18030", "latin-1"]
    for enc in encodings:
        try:
            text = data.decode(enc)
            # If the text contains Chinese characters, it's likely correct
            if any("\u4e00" <= ch <= "\u9fff" for ch in text):
                return text
        except (UnicodeDecodeError, LookupError):
            continue

    # Final fallback
    return data.decode("latin-1")


async def safe_fetch(
    url: str,
    *,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    force_gbk: bool = False,
) -> Optional[str]:
    """
    Fetch text from a URL with retry and timeout.

    Args:
        url: The URL to fetch.
        params: Query parameters.
        headers: Extra headers to merge with defaults.
        force_gbk: If True, try GBK decoding first.

    Returns:
        Decoded text or None on failure.
    """
    client = await get_client()
    merged_headers = {**DEFAULT_HEADERS}
    if headers:
        merged_headers.update(headers)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = await client.get(url, params=params, headers=merged_headers)

            if resp.status_code == 200:
                if force_gbk:
                    return _decode_bytes(resp.content)
                else:
                    # Try to detect encoding from Content-Type
                    content_type = resp.headers.get("content-type", "")
                    charset = ""
                    if "charset=" in content_type:
                        charset = content_type.split("charset=")[1].strip().lower()

                    if charset in ("gbk", "gb2312", "gb18030"):
                        return _decode_bytes(resp.content)

                    # Try UTF-8 first, fallback to GBK decode chain
                    try:
                        return resp.content.decode("utf-8")
                    except UnicodeDecodeError:
                        return _decode_bytes(resp.content)

            logger.warning(
                "[http_client] HTTP %s for %s (attempt %d/%d)",
                resp.status_code, url, attempt, MAX_RETRIES,
            )

        except httpx.HTTPError as e:
            logger.warning(
                "[http_client] Request error for %s (attempt %d/%d): %s",
                url, attempt, MAX_RETRIES, str(e),
            )

        if attempt < MAX_RETRIES:
            await asyncio.sleep(RETRY_DELAY * attempt / 1000.0)

    return None


async def safe_fetch_json(
    url: str,
    *,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
) -> Optional[dict]:
    """
    Fetch JSON from a URL with retry and timeout.

    Args:
        url: The URL to fetch.
        params: Query parameters.
        headers: Extra headers to merge with defaults.

    Returns:
        Parsed JSON dict or None on failure.
    """
    text = await safe_fetch(url, params=params, headers=headers)
    if text is None:
        return None

    try:
        import json
        return json.loads(text)
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning("[http_client] Failed to parse JSON: %s", str(e))
        return None
