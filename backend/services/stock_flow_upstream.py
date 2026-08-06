"""EastMoney upstream requests for searchable stock-level fund flows."""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Any

from config import (
    EASTMONEY_SECTOR_CAPITAL_FLOW_FALLBACK_URL,
    EASTMONEY_SECTOR_CAPITAL_FLOW_URL,
    EASTMONEY_STOCK_FLOW_SNAPSHOT_FALLBACK_URL,
    EASTMONEY_STOCK_FLOW_SNAPSHOT_URL,
    EASTMONEY_STOCK_SEARCH_URL,
)
from services.sector_flow_upstream import parse_minute_flows
from utils.http_client import safe_fetch
from utils.sector_selection import as_float

logger = logging.getLogger(__name__)

SEARCH_TOKEN = "D43BF722C8E33BDC906FB84D85E326E8"
QUOTE_ID_PATTERN = re.compile(r"^[01]\.\d{6}$")
STOCK_FLOW_FIELDS = "f12,f14,f62,f66,f72,f78,f84,f124"


def parse_stock_search_items(items: list[Any]) -> list[dict[str, str]]:
    """Keep A-share results and expose the exact quote id used upstream."""
    results: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in items:
        if not isinstance(item, dict) or str(item.get("Classify") or "") != "AStock":
            continue
        quote_id = str(item.get("QuoteID") or "").strip()
        code = str(item.get("Code") or "").strip()
        name = str(item.get("Name") or "").strip()
        if (
            not QUOTE_ID_PATTERN.fullmatch(quote_id)
            or not code.isdigit()
            or len(code) != 6
            or not name
            or quote_id in seen
        ):
            continue
        seen.add(quote_id)
        results.append({
            "quote_id": quote_id,
            "code": code,
            "name": name,
            "market_name": str(item.get("SecurityTypeName") or "A股"),
            "pinyin": str(item.get("PinYin") or ""),
        })
    return results


async def search_stocks(keyword: str, limit: int = 8) -> list[dict[str, str]] | None:
    """Search A-share stocks by code, Chinese name, or pinyin."""
    text = await safe_fetch(
        EASTMONEY_STOCK_SEARCH_URL,
        params={
            "input": keyword,
            "type": "14",
            "token": SEARCH_TOKEN,
            "count": str(limit),
        },
        headers={"Referer": "https://quote.eastmoney.com/"},
    )
    if not text:
        return None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    items = (payload.get("QuotationCodeTable") or {}).get("Data") or []
    return parse_stock_search_items(items)[:limit]


async def fetch_stock_minute_data(
    quote_id: str,
    limit: int = 240,
) -> dict[str, Any] | None:
    """Fetch authentic cumulative minute history for pre-subscription backfill."""
    if not QUOTE_ID_PATTERN.fullmatch(quote_id):
        return None
    params = {
        "lmt": str(limit),
        "klt": "1",
        "secid": quote_id,
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56",
        "_": str(int(time.time() * 1000)),
    }
    text = await safe_fetch(
        EASTMONEY_SECTOR_CAPITAL_FLOW_URL,
        params=params,
        headers={"Referer": "https://data.eastmoney.com/"},
    )
    if not text:
        logger.warning("[stock-flow] Minute history primary upstream unavailable for %s", quote_id)
        text = await safe_fetch(
            EASTMONEY_SECTOR_CAPITAL_FLOW_FALLBACK_URL,
            params=params,
            headers={"Referer": "https://data.eastmoney.com/"},
        )
    if not text:
        return None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    upstream_data = payload.get("data") or {}
    flows = parse_minute_flows(upstream_data.get("klines") or [])
    return {
        "quote_id": quote_id,
        "code": str(upstream_data.get("code") or quote_id.split(".", 1)[1]),
        "name": str(upstream_data.get("name") or ""),
        "interval": "1m",
        "value_type": "cumulative",
        "count": len(flows),
        "flows": flows,
    }


async def fetch_stock_flow_snapshot(quote_id: str) -> dict[str, Any] | None:
    """Fetch the latest second-level cumulative fund-flow snapshot."""
    if not QUOTE_ID_PATTERN.fullmatch(quote_id):
        return None
    params = {
        "secids": quote_id,
        "fields": STOCK_FLOW_FIELDS,
        "np": "1",
        "fltt": "2",
        "invt": "2",
        "ut": "7eea3edcaed734bea9telecast",
        "_": str(int(time.time() * 1000)),
    }
    text = await safe_fetch(
        EASTMONEY_STOCK_FLOW_SNAPSHOT_URL,
        params=params,
        headers={"Referer": "https://data.eastmoney.com/"},
    )
    if not text:
        text = await safe_fetch(
            EASTMONEY_STOCK_FLOW_SNAPSHOT_FALLBACK_URL,
            params=params,
            headers={"Referer": "https://data.eastmoney.com/"},
        )
    if not text:
        return None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    diff = (payload.get("data") or {}).get("diff") or []
    data = diff[0] if diff and isinstance(diff[0], dict) else {}
    try:
        source_time = int(data.get("f124") or 0)
    except (TypeError, ValueError):
        source_time = 0
    code = str(data.get("f12") or quote_id.split(".", 1)[1])
    return {
        "quote_id": quote_id,
        "code": code,
        "name": str(data.get("f14") or ""),
        "source_time": source_time,
        "main_net": as_float(data.get("f62")),
        "super_large_net": as_float(data.get("f66")),
        "large_net": as_float(data.get("f72")),
        "mid_net": as_float(data.get("f78")),
        "small_net": as_float(data.get("f84")),
    }
