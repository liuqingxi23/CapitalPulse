"""EastMoney upstream requests used by the real-time sector-flow dashboard."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from config import (
    EASTMONEY_PUSH_URL,
    EASTMONEY_SECTOR_CAPITAL_FLOW_FALLBACK_URL,
    EASTMONEY_SECTOR_CAPITAL_FLOW_URL,
    EASTMONEY_SECTOR_DAILY_FLOW_URL,
    EASTMONEY_SECTOR_URL,
)
from utils.http_client import safe_fetch

logger = logging.getLogger(__name__)


def build_eastmoney_params(overrides: dict[str, Any]) -> dict[str, Any]:
    """Add the request tokens required by EastMoney push endpoints."""
    return {
        "ut": "7eea3edcaed734bea9telecast",
        "np": "1",
        "fltt": "2",
        "invt": "2",
        "_": str(int(time.time() * 1000)),
        **overrides,
    }


def _number(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def parse_minute_flows(klines: list[Any]) -> list[dict[str, Any]]:
    """Parse cumulative minute flows returned by EastMoney."""
    flows: list[dict[str, Any]] = []
    for kline in klines:
        if not isinstance(kline, str):
            continue
        parts = kline.split(",")
        if len(parts) < 6 or not parts[0]:
            continue
        flows.append({
            "time": parts[0],
            "main_net": _number(parts[1]),
            "small_net": _number(parts[2]),
            "mid_net": _number(parts[3]),
            "large_net": _number(parts[4]),
            "super_large_net": _number(parts[5]),
        })
    return flows


def parse_daily_flows(klines: list[Any]) -> list[dict[str, Any]]:
    """Parse one net-flow value for each trading day."""
    return [
        {"trade_date": flow.pop("time"), **flow}
        for flow in parse_minute_flows(klines)
    ]


async def fetch_sector_minute_data(
    normalized_code: str,
    limit: int = 240,
) -> dict[str, Any] | None:
    """Fetch one sector's cumulative minute history."""
    params = {
        "lmt": str(limit),
        "klt": "1",
        "secid": f"90.{normalized_code}",
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
        logger.warning(
            "[sector-flow] Minute history primary upstream unavailable for %s",
            normalized_code,
        )
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
        "code": normalized_code,
        "name": str(upstream_data.get("name") or ""),
        "interval": "1m",
        "value_type": "cumulative",
        "count": len(flows),
        "flows": flows,
    }


async def fetch_sector_daily_data(
    normalized_code: str,
    limit: int = 30,
) -> dict[str, Any] | None:
    """Fetch daily net flows from the dedicated historical day-kline endpoint."""
    params = {
        "lmt": str(limit),
        "klt": "101",
        "secid": f"90.{normalized_code}",
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56",
        "_": str(int(time.time() * 1000)),
    }
    text = await safe_fetch(
        EASTMONEY_SECTOR_DAILY_FLOW_URL,
        params=params,
        headers={"Referer": "https://data.eastmoney.com/"},
    )
    if not text:
        logger.warning(
            "[sector-flow] Daily history upstream unavailable for %s",
            normalized_code,
        )
        return None

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    upstream_data = payload.get("data") or {}
    flows = parse_daily_flows(upstream_data.get("klines") or [])
    return {
        "code": normalized_code,
        "name": str(upstream_data.get("name") or ""),
        "interval": "1d",
        "value_type": "daily_net",
        "count": len(flows),
        "flows": flows,
    }


async def _fetch_industry_sectors_from(url: str) -> list[dict[str, Any]] | None:
    """Fetch every page of EastMoney's industry sector list."""
    page_size = 100
    page = 1
    all_items: list[dict[str, Any]] = []
    while True:
        params = build_eastmoney_params({
            "dpt": "wz.zhyj",
            "Ession": "",
            "fs": "m:90+t:2+f:!50",
            "fields": "f3,f12,f14,f20",
            "pn": str(page),
            "pz": str(page_size),
            "po": "1",
            "fid": "f3",
        })
        text = await safe_fetch(
            url,
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
        items = upstream_data.get("diff") or []
        if not items:
            return None
        all_items.extend(item for item in items if isinstance(item, dict))
        try:
            total = int(upstream_data.get("total") or len(all_items))
        except (TypeError, ValueError):
            total = len(all_items)
        if len(all_items) >= total or len(items) < page_size:
            break
        page += 1
        if page > 20:
            logger.warning("[sector-flow] Refusing to fetch more than 20 sector pages")
            return None

    sectors: list[dict[str, Any]] = []
    seen_codes: set[str] = set()
    for item in all_items:
        try:
            code = str(item.get("f12") or "").upper()
            if not code or code in seen_codes:
                continue
            seen_codes.add(code)
            sectors.append({
                "code": code,
                "name": str(item.get("f14") or ""),
                "change_percent": round(float(item.get("f3") or 0), 2),
                "market_cap": float(item.get("f20") or 0),
                "sector_type": "industry",
            })
        except (TypeError, ValueError):
            continue
    return sectors or None


async def fetch_all_industry_sectors() -> list[dict[str, Any]] | None:
    """Fetch all industry sectors, falling back to the non-delay endpoint."""
    sectors = await _fetch_industry_sectors_from(EASTMONEY_SECTOR_URL)
    if sectors is None:
        logger.warning("[sector-flow] Industry list primary upstream unavailable")
        sectors = await _fetch_industry_sectors_from(EASTMONEY_PUSH_URL)
    return sectors
