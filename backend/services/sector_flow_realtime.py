"""Persistent three-second sector capital-flow snapshots and live broadcasts."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import sqlite3
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect

from config import (
    EASTMONEY_SECTOR_FLOW_SNAPSHOT_FALLBACK_URL,
    EASTMONEY_SECTOR_FLOW_SNAPSHOT_URL,
)
from services.sector_flow_upstream import (
    build_eastmoney_params,
    fetch_all_industry_sectors,
    fetch_sector_daily_data,
    fetch_sector_minute_data,
)
from utils.http_client import safe_fetch
from utils.sector_selection import (
    as_float,
    filter_second_level_industries,
    select_top_sectors,
)

logger = logging.getLogger(__name__)

CST = timezone(timedelta(hours=8))
FLOW_COLUMNS = (
    "main_net",
    "small_net",
    "mid_net",
    "large_net",
    "super_large_net",
)
FLOW_FIELDS = {
    "main_net": "f62",
    "super_large_net": "f66",
    "large_net": "f72",
    "mid_net": "f78",
    "small_net": "f84",
}
MORNING_START = time(9, 30)
MORNING_END = time(11, 30)
AFTERNOON_START = time(13, 0)
AFTERNOON_END = time(15, 0)
DEFAULT_DB_PATH = Path(__file__).resolve().parents[1] / "data" / "sector_flow_realtime.sqlite3"
DETAIL_PAGE_SIZE = 6


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def market_status_at(value: datetime) -> str:
    """Return the A-share session state for a CST-aware datetime."""
    if value.weekday() >= 5:
        return "closed"
    current = value.timetz().replace(tzinfo=None)
    if current < MORNING_START:
        return "preopen"
    if MORNING_START <= current <= MORNING_END:
        return "open"
    if MORNING_END < current < AFTERNOON_START:
        return "lunch"
    if AFTERNOON_START <= current <= AFTERNOON_END:
        return "open"
    return "closed"


def parse_snapshot_items(items: list[Any]) -> list[dict[str, Any]]:
    """Parse EastMoney's latest cumulative flow snapshots."""
    flows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        code = str(item.get("f12") or "").strip().upper()
        try:
            source_time = int(item.get("f124") or 0)
        except (TypeError, ValueError):
            source_time = 0
        if not code or source_time <= 0:
            continue
        flow = {
            "sector_code": code,
            "sector_name": str(item.get("f14") or ""),
            "source_time": source_time,
        }
        for column, field in FLOW_FIELDS.items():
            flow[column] = as_float(item.get(field))
        flows.append(flow)
    return flows


def minute_source_time(value: Any, trade_date: date) -> int | None:
    """Convert an upstream minute label to a valid CST trading timestamp."""
    try:
        parsed = datetime.strptime(str(value), "%Y-%m-%d %H:%M").replace(tzinfo=CST)
    except (TypeError, ValueError):
        return None
    if parsed.date() != trade_date:
        return None
    current = parsed.timetz().replace(tzinfo=None)
    if (
        MORNING_START <= current <= MORNING_END
        or AFTERNOON_START <= current <= AFTERNOON_END
    ):
        return int(parsed.timestamp())
    return None


class SectorFlowRealtimeService:
    """One global collector shared by every API and WebSocket client."""

    def __init__(self) -> None:
        self.enabled = _env_bool("SECTOR_FLOW_ENABLED", True)
        self.poll_seconds = max(1.0, float(os.getenv("SECTOR_FLOW_POLL_SECONDS", "3")))
        self.retention_days = max(1, int(os.getenv("SECTOR_FLOW_RETENTION_DAYS", "30")))
        self.db_path = Path(os.getenv("SECTOR_FLOW_DB_PATH", str(DEFAULT_DB_PATH)))
        self._connection: sqlite3.Connection | None = None
        self._task: asyncio.Task[None] | None = None
        self._backfill_task: asyncio.Task[None] | None = None
        self._backfill_date: date | None = None
        self._backfill_status = "idle"
        self._backfill_inserted_points = 0
        self._backfill_error: str | None = None
        self._backfill_retry_at = 0.0
        self._daily_history_lock = asyncio.Lock()
        self._daily_refresh_date: date | None = None
        self._daily_refresh_retry_at = 0.0
        self._clients: set[WebSocket] = set()
        self._selection: list[dict[str, Any]] = []
        self._selection_date: date | None = None
        self._latest: dict[str, dict[str, Any]] = {}
        self._last_signature: tuple[Any, ...] | None = None
        self._status = "closed"
        self._last_source_time: int | None = None
        self._last_received_at: str | None = None
        self._last_error: str | None = None

    @property
    def connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("sector-flow database is not open")
        return self._connection

    @property
    def ready(self) -> bool:
        return self._connection is not None

    async def start(self) -> None:
        if not self.enabled:
            logger.info("[sector-flow] Disabled by SECTOR_FLOW_ENABLED")
            return
        if self._task is not None and not self._task.done():
            return
        self._open_database()
        self._cleanup_old_data(datetime.now(CST).date())
        self._task = asyncio.create_task(self._poll_loop(), name="sector-flow-poller")
        logger.info(
            "[sector-flow] Collector started (%.1fs, db=%s)",
            self.poll_seconds,
            self.db_path,
        )

    async def stop(self) -> None:
        if self._task is not None and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._task = None
        if self._backfill_task is not None and not self._backfill_task.done():
            self._backfill_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._backfill_task
        self._backfill_task = None
        for client in list(self._clients):
            with contextlib.suppress(Exception):
                await client.close()
        self._clients.clear()
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def _open_database(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.db_path, check_same_thread=False)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS sector_flow_selection (
                trade_date TEXT NOT NULL,
                rank INTEGER NOT NULL,
                sector_code TEXT NOT NULL,
                sector_name TEXT NOT NULL,
                market_cap REAL NOT NULL,
                selected_at TEXT NOT NULL,
                PRIMARY KEY (trade_date, sector_code)
            );

            CREATE TABLE IF NOT EXISTS sector_flow_snapshot (
                trade_date TEXT NOT NULL,
                source_time INTEGER NOT NULL,
                sector_code TEXT NOT NULL,
                sector_name TEXT NOT NULL,
                main_net REAL NOT NULL,
                small_net REAL NOT NULL,
                mid_net REAL NOT NULL,
                large_net REAL NOT NULL,
                super_large_net REAL NOT NULL,
                received_at TEXT NOT NULL,
                granularity TEXT NOT NULL DEFAULT 'realtime',
                PRIMARY KEY (trade_date, source_time, sector_code)
            );

            CREATE INDEX IF NOT EXISTS idx_sector_flow_snapshot_time
                ON sector_flow_snapshot (trade_date, source_time);
            CREATE INDEX IF NOT EXISTS idx_sector_flow_snapshot_sector
                ON sector_flow_snapshot (trade_date, sector_code, source_time);

            CREATE TABLE IF NOT EXISTS sector_flow_daily (
                trade_date TEXT NOT NULL,
                sector_code TEXT NOT NULL,
                sector_name TEXT NOT NULL,
                main_net REAL NOT NULL,
                super_large_net REAL NOT NULL DEFAULT 0,
                large_net REAL NOT NULL DEFAULT 0,
                mid_net REAL NOT NULL DEFAULT 0,
                small_net REAL NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (trade_date, sector_code)
            );

            CREATE INDEX IF NOT EXISTS idx_sector_flow_daily_sector_date
                ON sector_flow_daily (sector_code, trade_date);
            """
        )
        columns = {
            str(row[1])
            for row in self.connection.execute(
                "PRAGMA table_info(sector_flow_snapshot)"
            ).fetchall()
        }
        if "granularity" not in columns:
            self.connection.execute(
                "ALTER TABLE sector_flow_snapshot "
                "ADD COLUMN granularity TEXT NOT NULL DEFAULT 'realtime'"
            )
        daily_columns = {
            str(row[1])
            for row in self.connection.execute(
                "PRAGMA table_info(sector_flow_daily)"
            ).fetchall()
        }
        daily_schema_extended = False
        for column in (
            "super_large_net",
            "large_net",
            "mid_net",
            "small_net",
        ):
            if column not in daily_columns:
                daily_schema_extended = True
                self.connection.execute(
                    f"ALTER TABLE sector_flow_daily "
                    f"ADD COLUMN {column} REAL NOT NULL DEFAULT 0"
                )
        if daily_schema_extended:
            self.connection.execute("DELETE FROM sector_flow_daily")
        self.connection.commit()

    def _load_selection(self, trade_date: date) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT rank, sector_code, sector_name, market_cap
            FROM sector_flow_selection
            WHERE trade_date = ?
            ORDER BY rank
            """,
            (trade_date.isoformat(),),
        ).fetchall()
        return [
            {
                "rank": int(row[0]),
                "code": str(row[1]),
                "name": str(row[2]),
                "market_cap": float(row[3]),
                "sector_type": "industry",
            }
            for row in rows
        ]

    def _save_selection(
        self,
        trade_date: date,
        sectors: list[dict[str, Any]],
    ) -> None:
        selected_at = datetime.now(CST).isoformat(timespec="seconds")
        self.connection.executemany(
            """
            INSERT INTO sector_flow_selection
                (trade_date, rank, sector_code, sector_name, market_cap, selected_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (trade_date, sector_code) DO UPDATE SET
                rank = excluded.rank,
                sector_name = excluded.sector_name,
                market_cap = excluded.market_cap,
                selected_at = excluded.selected_at
            """,
            [
                (
                    trade_date.isoformat(),
                    rank,
                    sector["code"],
                    str(sector.get("name") or ""),
                    as_float(sector.get("market_cap")),
                    selected_at,
                )
                for rank, sector in enumerate(sectors, start=1)
            ],
        )
        self.connection.commit()

    def _reset_backfill_for_date(self, trade_date: date) -> None:
        if self._backfill_date == trade_date:
            return
        self._backfill_date = trade_date
        self._backfill_status = "idle"
        self._backfill_inserted_points = 0
        self._backfill_error = None
        self._backfill_retry_at = 0.0

    async def _ensure_selection(self, trade_date: date) -> bool:
        if self._selection_date == trade_date and len(self._selection) == 30:
            return True

        self._cleanup_old_data(trade_date)
        stored = self._load_selection(trade_date)
        if len(stored) == 30:
            self._selection = stored
            self._selection_date = trade_date
            self._reset_backfill_for_date(trade_date)
            self._load_latest(trade_date)
            await self.broadcast(self.snapshot_message())
            return True

        sectors = await fetch_all_industry_sectors()
        if sectors is None:
            await self._set_status("error", "无法获取行业板块列表")
            return False
        candidates = filter_second_level_industries([
            {**sector, "sector_type": "industry"}
            for sector in sectors
        ])
        selected = select_top_sectors(candidates, 30)
        if len(selected) != 30:
            await self._set_status(
                "error",
                f"仅获取到 {len(selected)} 个申万二级行业，无法选出 Top 30",
            )
            return False

        self._save_selection(trade_date, selected)
        self._selection = [
            {**sector, "rank": rank}
            for rank, sector in enumerate(selected, start=1)
        ]
        self._selection_date = trade_date
        self._reset_backfill_for_date(trade_date)
        self._latest.clear()
        self._last_signature = None
        self._cleanup_old_data(trade_date)
        await self.broadcast(self.snapshot_message())
        return True

    def _cleanup_daily_cache(self) -> None:
        self.connection.execute(
            """
            DELETE FROM sector_flow_daily
            WHERE rowid IN (
                SELECT rowid
                FROM (
                    SELECT rowid,
                           ROW_NUMBER() OVER (
                               PARTITION BY sector_code
                               ORDER BY trade_date DESC
                           ) AS row_number
                    FROM sector_flow_daily
                )
                WHERE row_number > 30
            )
            """
        )
        self.connection.execute(
            """
            DELETE FROM sector_flow_daily
            WHERE sector_code NOT IN (
                SELECT DISTINCT sector_code FROM sector_flow_selection
            )
            """
        )

    def _cleanup_old_data(self, today: date) -> None:
        cutoff = (today - timedelta(days=self.retention_days)).isoformat()
        self.connection.execute(
            "DELETE FROM sector_flow_snapshot WHERE trade_date < ?",
            (cutoff,),
        )
        self.connection.execute(
            "DELETE FROM sector_flow_selection WHERE trade_date < ?",
            (cutoff,),
        )
        self._cleanup_daily_cache()
        self.connection.commit()

    def _load_latest(self, trade_date: date) -> None:
        rows = self.connection.execute(
            """
            SELECT snapshot.source_time, snapshot.sector_code,
                   snapshot.sector_name, snapshot.main_net, snapshot.small_net,
                   snapshot.mid_net, snapshot.large_net,
                   snapshot.super_large_net, snapshot.received_at,
                   snapshot.granularity
            FROM sector_flow_snapshot AS snapshot
            INNER JOIN (
                SELECT sector_code, MAX(source_time) AS source_time
                FROM sector_flow_snapshot
                WHERE trade_date = ?
                GROUP BY sector_code
            ) AS latest
                ON latest.sector_code = snapshot.sector_code
               AND latest.source_time = snapshot.source_time
            WHERE snapshot.trade_date = ?
            """,
            (trade_date.isoformat(), trade_date.isoformat()),
        ).fetchall()
        if not rows:
            self._latest.clear()
            return
        self._latest = {
            str(row[1]): {
                "source_time": int(row[0]),
                "sector_code": str(row[1]),
                "sector_name": str(row[2]),
                "main_net": float(row[3]),
                "small_net": float(row[4]),
                "mid_net": float(row[5]),
                "large_net": float(row[6]),
                "super_large_net": float(row[7]),
                "received_at": str(row[8]),
                "granularity": str(row[9]),
            }
            for row in rows
        }
        latest_row = max(rows, key=lambda row: int(row[0]))
        self._last_source_time = int(latest_row[0])
        self._last_received_at = str(latest_row[8])

    async def _fetch_snapshot(self) -> list[dict[str, Any]] | None:
        secids = ",".join(f"90.{sector['code']}" for sector in self._selection)
        params = build_eastmoney_params({
            "secids": secids,
            "fields": "f12,f14,f62,f66,f72,f78,f84,f124",
        })
        text = await safe_fetch(
            EASTMONEY_SECTOR_FLOW_SNAPSHOT_URL,
            params=params,
            headers={"Referer": "https://data.eastmoney.com/"},
        )
        if not text:
            text = await safe_fetch(
                EASTMONEY_SECTOR_FLOW_SNAPSHOT_FALLBACK_URL,
                params=params,
                headers={"Referer": "https://data.eastmoney.com/"},
            )
        if not text:
            return None
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return None
        return parse_snapshot_items((payload.get("data") or {}).get("diff") or [])

    def _schedule_minute_backfill(
        self,
        now: datetime,
        monotonic_now: float,
    ) -> None:
        if (
            now.weekday() >= 5
            or now.timetz().replace(tzinfo=None) <= MORNING_START
            or self._selection_date != now.date()
            or len(self._selection) != 30
            or self._backfill_status == "complete"
            or monotonic_now < self._backfill_retry_at
            or (
                self._backfill_task is not None
                and not self._backfill_task.done()
            )
        ):
            return
        self._backfill_status = "running"
        self._backfill_error = None
        self._backfill_task = asyncio.create_task(
            self._run_minute_backfill(now),
            name=f"sector-flow-backfill-{now.date().isoformat()}",
        )

    async def _run_minute_backfill(self, now: datetime) -> None:
        """Fill any empty trading-minute buckets without blocking live polling."""
        await self.broadcast({"type": "status", "data": self.status_data()})
        semaphore = asyncio.Semaphore(5)

        async def fetch(sector: dict[str, Any]) -> tuple[dict[str, Any], Any]:
            async with semaphore:
                data = await fetch_sector_minute_data(str(sector["code"]), 240)
                return sector, data

        try:
            results = await asyncio.gather(*(fetch(sector) for sector in self._selection))
            failures: list[str] = []
            candidates: list[dict[str, Any]] = []
            upper_bound = int(now.timestamp())
            for sector, data in results:
                if data is None:
                    failures.append(str(sector["code"]))
                    continue
                sector_name = str(data.get("name") or sector.get("name") or "")
                for flow in data.get("flows") or []:
                    source_time = minute_source_time(flow.get("time"), now.date())
                    if source_time is None or source_time > upper_bound:
                        continue
                    candidates.append({
                        "source_time": source_time,
                        "sector_code": str(sector["code"]),
                        "sector_name": sector_name,
                        **{
                            column: as_float(flow.get(column))
                            for column in FLOW_COLUMNS
                        },
                    })

            received_at = datetime.now(CST).isoformat(timespec="milliseconds")
            inserted = self._persist_minute_backfill(
                now.date(),
                candidates,
                received_at,
            )
            self._load_latest(now.date())
            self._backfill_inserted_points += inserted
            if failures:
                self._backfill_status = "error"
                self._backfill_error = (
                    f"{len(failures)} 个板块分钟历史补全失败，将自动重试"
                )
                self._backfill_retry_at = asyncio.get_running_loop().time() + 60.0
            else:
                self._backfill_status = "complete"
                self._backfill_error = None

            if inserted:
                await self.broadcast({
                    "type": "history_backfill",
                    "data": {
                        "trade_date": now.date().isoformat(),
                        "inserted_points": inserted,
                    },
                })
            await self.broadcast({"type": "status", "data": self.status_data()})
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("[sector-flow] Minute history backfill failed")
            self._backfill_status = "error"
            self._backfill_error = str(exc)
            self._backfill_retry_at = asyncio.get_running_loop().time() + 60.0
            await self.broadcast({"type": "status", "data": self.status_data()})

    def _persist_minute_backfill(
        self,
        trade_date: date,
        flows: list[dict[str, Any]],
        received_at: str,
    ) -> int:
        """Insert one point only for minute buckets with no real-time snapshot."""
        occupied_rows = self.connection.execute(
            """
            SELECT DISTINCT sector_code, CAST(source_time / 60 AS INTEGER)
            FROM sector_flow_snapshot
            WHERE trade_date = ?
            """,
            (trade_date.isoformat(),),
        ).fetchall()
        occupied = {(str(row[0]), int(row[1])) for row in occupied_rows}
        rows: list[tuple[Any, ...]] = []
        for flow in sorted(
            flows,
            key=lambda item: (int(item["source_time"]), str(item["sector_code"])),
        ):
            key = (str(flow["sector_code"]), int(flow["source_time"]) // 60)
            if key in occupied:
                continue
            occupied.add(key)
            rows.append((
                trade_date.isoformat(),
                int(flow["source_time"]),
                str(flow["sector_code"]),
                str(flow["sector_name"]),
                *(as_float(flow[column]) for column in FLOW_COLUMNS),
                received_at,
            ))
        if not rows:
            return 0

        before = self.connection.total_changes
        self.connection.executemany(
            """
            INSERT INTO sector_flow_snapshot
                (trade_date, source_time, sector_code, sector_name, main_net,
                 small_net, mid_net, large_net, super_large_net, received_at,
                 granularity)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'minute_backfill')
            ON CONFLICT (trade_date, source_time, sector_code) DO NOTHING
            """,
            rows,
        )
        self.connection.commit()
        return self.connection.total_changes - before

    def _persist_snapshot(
        self,
        trade_date: date,
        flows: list[dict[str, Any]],
        received_at: str,
    ) -> None:
        self.connection.executemany(
            """
            INSERT INTO sector_flow_snapshot
                (trade_date, source_time, sector_code, sector_name, main_net,
                 small_net, mid_net, large_net, super_large_net, received_at,
                 granularity)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'realtime')
            ON CONFLICT (trade_date, source_time, sector_code) DO UPDATE SET
                sector_name = excluded.sector_name,
                main_net = excluded.main_net,
                small_net = excluded.small_net,
                mid_net = excluded.mid_net,
                large_net = excluded.large_net,
                super_large_net = excluded.super_large_net,
                received_at = excluded.received_at,
                granularity = 'realtime'
            """,
            [
                (
                    trade_date.isoformat(),
                    int(flow["source_time"]),
                    flow["sector_code"],
                    flow["sector_name"],
                    *(as_float(flow[column]) for column in FLOW_COLUMNS),
                    received_at,
                )
                for flow in flows
            ],
        )
        self.connection.commit()

    async def collect_once(self, now: datetime | None = None) -> bool:
        """Fetch, persist, and then broadcast one batch. Exposed for tests."""
        current = now or datetime.now(CST)
        if not await self._ensure_selection(current.date()):
            return False
        flows = await self._fetch_snapshot()
        if not flows:
            await self._set_status("error", "板块资金快照请求失败")
            return False

        selected_codes = {sector["code"] for sector in self._selection}
        flows = [flow for flow in flows if flow["sector_code"] in selected_codes]
        if not flows:
            await self._set_status("error", "板块资金快照为空")
            return False

        valid_flows = [
            flow
            for flow in flows
            if datetime.fromtimestamp(int(flow["source_time"]), CST).date() == current.date()
        ]
        if not valid_flows:
            await self._set_status("stale", "上游尚未提供当天板块资金快照")
            return False

        received_at = datetime.now(CST).isoformat(timespec="milliseconds")
        signature = tuple(
            (
                flow["sector_code"],
                flow["source_time"],
                *(flow[column] for column in FLOW_COLUMNS),
            )
            for flow in sorted(valid_flows, key=lambda item: item["sector_code"])
        )
        if signature == self._last_signature:
            source_time = max(int(flow["source_time"]) for flow in valid_flows)
            delayed = current.timestamp() - source_time > max(10.0, self.poll_seconds * 3)
            await self._set_status(
                "stale" if delayed else "open",
                "上游板块资金快照更新延迟" if delayed else None,
            )
            return False

        # Persist first so reconnecting clients can always backfill a broadcast.
        self._persist_snapshot(current.date(), valid_flows, received_at)
        broadcast_flows = [
            {**flow, "granularity": "realtime"}
            for flow in valid_flows
        ]
        for flow in broadcast_flows:
            self._latest[flow["sector_code"]] = {**flow, "received_at": received_at}
        self._last_signature = signature
        self._last_source_time = max(int(flow["source_time"]) for flow in valid_flows)
        self._last_received_at = received_at
        delayed = current.timestamp() - self._last_source_time > max(
            10.0,
            self.poll_seconds * 3,
        )
        self._last_error = "上游板块资金快照更新延迟" if delayed else None
        self._status = "stale" if delayed else "open"

        await self.broadcast({
            "type": "update",
            "data": {
                "source_time": self._last_source_time,
                "received_at": received_at,
                "complete": len(valid_flows) == len(self._selection),
                "flows": broadcast_flows,
            },
        })
        return True

    async def _refresh_daily_cache_after_close(
        self,
        now: datetime,
        monotonic_now: float,
    ) -> None:
        if (
            now.weekday() >= 5
            or now.timetz().replace(tzinfo=None) <= AFTERNOON_END
            or self._selection_date != now.date()
            or self._daily_refresh_date == now.date()
            or monotonic_now < self._daily_refresh_retry_at
        ):
            return
        try:
            result = await self.daily_history_data(now.date(), now=now)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[sector-flow] Daily cache refresh after close failed")
            self._daily_refresh_retry_at = monotonic_now + 300.0
            return
        failures = result["failed_codes"] or result["refresh_failed_codes"]
        if failures:
            logger.warning(
                "[sector-flow] Daily cache refresh incomplete for %d sectors",
                len(failures),
            )
            self._daily_refresh_retry_at = monotonic_now + 300.0
            return
        self._daily_refresh_date = now.date()
        self._daily_refresh_retry_at = 0.0

    async def _poll_loop(self) -> None:
        loop = asyncio.get_running_loop()
        next_tick = loop.time()
        consecutive_errors = 0
        while True:
            now = datetime.now(CST)
            session_status = market_status_at(now)
            if now.weekday() < 5 and self._selection_date != now.date():
                await self._ensure_selection(now.date())
            self._schedule_minute_backfill(now, loop.time())
            await self._refresh_daily_cache_after_close(now, loop.time())

            if session_status == "open":
                try:
                    await self.collect_once(now)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.exception("[sector-flow] Poll failed")
                    await self._set_status("error", str(exc))

                if self._status == "error":
                    consecutive_errors += 1
                    delay = min(
                        self.poll_seconds * (2 ** min(consecutive_errors - 1, 5)),
                        60.0,
                    )
                    next_tick = loop.time() + delay
                else:
                    consecutive_errors = 0
                    next_tick = max(next_tick + self.poll_seconds, loop.time())
                await asyncio.sleep(max(0.0, next_tick - loop.time()))
            else:
                consecutive_errors = 0
                next_tick = loop.time()
                await self._set_status(session_status, None)
                await asyncio.sleep(15.0)

    async def _set_status(self, status: str, error: str | None) -> None:
        changed = status != self._status or error != self._last_error
        self._status = status
        self._last_error = error
        if changed:
            await self.broadcast({"type": "status", "data": self.status_data()})

    def status_data(self) -> dict[str, Any]:
        return {
            "market_status": self._status,
            "last_source_time": self._last_source_time,
            "last_received_at": self._last_received_at,
            "selected_count": len(self._selection),
            "last_error": self._last_error,
            "poll_seconds": self.poll_seconds,
            "backfill_status": self._backfill_status,
            "backfill_inserted_points": self._backfill_inserted_points,
            "backfill_error": self._backfill_error,
        }

    def selection_data(self, top: int = 30) -> list[dict[str, Any]]:
        return [
            {
                "rank": int(sector["rank"]),
                "sector_code": sector["code"],
                "sector_name": str(sector.get("name") or ""),
                "market_cap": as_float(sector.get("market_cap")),
            }
            for sector in self._selection[:top]
        ]

    def snapshot_message(self) -> dict[str, Any]:
        ordered_codes = [sector["code"] for sector in self._selection]
        return {
            "type": "snapshot",
            "data": {
                "selection": self.selection_data(30),
                "flows": [
                    self._latest[code]
                    for code in ordered_codes
                    if code in self._latest
                ],
                "status": self.status_data(),
            },
        }

    def history_data(self, trade_date: date, top: int) -> dict[str, Any]:
        selection = self._load_selection(trade_date)[:top]
        selection_by_code = {sector["code"]: sector for sector in selection}
        series = {
            code: {
                "rank": int(sector["rank"]),
                "sector_code": code,
                "sector_name": sector["name"],
                "points": [],
            }
            for code, sector in selection_by_code.items()
        }
        if selection_by_code:
            placeholders = ",".join("?" for _ in selection_by_code)
            rows = self.connection.execute(
                f"""
                SELECT source_time, sector_code, main_net
                FROM sector_flow_snapshot
                WHERE trade_date = ? AND sector_code IN ({placeholders})
                ORDER BY source_time, sector_code
                """,
                (trade_date.isoformat(), *selection_by_code.keys()),
            ).fetchall()
            for source_time, code, main_net in rows:
                if code in series:
                    series[code]["points"].append([int(source_time), float(main_net)])

        latest: list[dict[str, Any]] = []
        if selection_by_code:
            placeholders = ",".join("?" for _ in selection_by_code)
            latest_rows = self.connection.execute(
                f"""
                SELECT snapshot.source_time, snapshot.sector_code,
                       snapshot.sector_name, snapshot.main_net,
                       snapshot.small_net, snapshot.mid_net,
                       snapshot.large_net, snapshot.super_large_net,
                       snapshot.received_at, snapshot.granularity
                FROM sector_flow_snapshot AS snapshot
                INNER JOIN (
                    SELECT sector_code, MAX(source_time) AS source_time
                    FROM sector_flow_snapshot
                    WHERE trade_date = ?
                      AND sector_code IN ({placeholders})
                    GROUP BY sector_code
                ) AS latest_per_sector
                    ON latest_per_sector.sector_code = snapshot.sector_code
                   AND latest_per_sector.source_time = snapshot.source_time
                WHERE snapshot.trade_date = ?
                ORDER BY snapshot.sector_code
                """,
                (
                    trade_date.isoformat(),
                    *selection_by_code.keys(),
                    trade_date.isoformat(),
                ),
            ).fetchall()
            latest = [
                {
                    "source_time": int(row[0]),
                    "sector_code": str(row[1]),
                    "sector_name": str(row[2]),
                    "main_net": float(row[3]),
                    "small_net": float(row[4]),
                    "mid_net": float(row[5]),
                    "large_net": float(row[6]),
                    "super_large_net": float(row[7]),
                    "received_at": str(row[8]),
                    "granularity": str(row[9]),
                }
                for row in latest_rows
            ]

        return {
            "trade_date": trade_date.isoformat(),
            "selection": [
                {
                    "rank": int(sector["rank"]),
                    "sector_code": sector["code"],
                    "sector_name": sector["name"],
                    "market_cap": as_float(sector["market_cap"]),
                }
                for sector in selection
            ],
            "series": sorted(series.values(), key=lambda item: item["rank"]),
            "latest": latest,
            "status": self.status_data(),
        }

    def detail_history_data(self, trade_date: date, page: int) -> dict[str, Any]:
        selection = self._load_selection(trade_date)
        total_items = len(selection)
        total_pages = (total_items + DETAIL_PAGE_SIZE - 1) // DETAIL_PAGE_SIZE
        start = (page - 1) * DETAIL_PAGE_SIZE
        page_selection = selection[start:start + DETAIL_PAGE_SIZE]
        selection_by_code = {sector["code"]: sector for sector in page_selection}
        series = {
            code: {
                "rank": int(sector["rank"]),
                "sector_code": code,
                "sector_name": sector["name"],
                "points": [],
            }
            for code, sector in selection_by_code.items()
        }

        if selection_by_code:
            placeholders = ",".join("?" for _ in selection_by_code)
            rows = self.connection.execute(
                f"""
                SELECT source_time, sector_code, main_net, super_large_net,
                       large_net, mid_net, small_net
                FROM sector_flow_snapshot
                WHERE trade_date = ? AND sector_code IN ({placeholders})
                ORDER BY source_time, sector_code
                """,
                (trade_date.isoformat(), *selection_by_code.keys()),
            ).fetchall()
            for source_time, code, main_net, super_large_net, large_net, mid_net, small_net in rows:
                if code in series:
                    series[code]["points"].append([
                        int(source_time),
                        float(main_net),
                        float(super_large_net),
                        float(large_net),
                        float(mid_net),
                        float(small_net),
                    ])

        return {
            "trade_date": trade_date.isoformat(),
            "page": page,
            "page_size": DETAIL_PAGE_SIZE,
            "total_items": total_items,
            "total_pages": total_pages,
            "series": sorted(series.values(), key=lambda item: item["rank"]),
        }

    @staticmethod
    def _expected_daily_trade_date(trade_date: date, now: datetime) -> date:
        expected = now.date()
        if now.weekday() >= 5 or now.timetz().replace(tzinfo=None) < MORNING_START:
            expected -= timedelta(days=1)
        while expected.weekday() >= 5:
            expected -= timedelta(days=1)
        expected = min(expected, trade_date)
        while expected.weekday() >= 5:
            expected -= timedelta(days=1)
        return expected

    def _load_daily_cache(
        self,
        selection: list[dict[str, Any]],
        trade_date: date,
        days: int,
    ) -> tuple[dict[str, list[list[Any]]], dict[str, datetime]]:
        codes = [str(sector["code"]) for sector in selection]
        if not codes:
            return {}, {}
        placeholders = ",".join("?" for _ in codes)
        rows = self.connection.execute(
            f"""
            SELECT sector_code, trade_date, main_net, super_large_net,
                   large_net, mid_net, small_net, updated_at
            FROM sector_flow_daily
            WHERE sector_code IN ({placeholders}) AND trade_date <= ?
            ORDER BY sector_code, trade_date
            """,
            (*codes, trade_date.isoformat()),
        ).fetchall()
        points_by_code: dict[str, list[list[Any]]] = {code: [] for code in codes}
        synced_at_by_code: dict[str, datetime] = {}
        for (
            code_value,
            date_value,
            main_net,
            super_large_net,
            large_net,
            mid_net,
            small_net,
            updated_at,
        ) in rows:
            code = str(code_value)
            points_by_code.setdefault(code, []).append([
                str(date_value),
                float(main_net),
                float(super_large_net),
                float(large_net),
                float(mid_net),
                float(small_net),
            ])
            try:
                synced_at = datetime.fromisoformat(str(updated_at))
                if synced_at.tzinfo is None:
                    synced_at = synced_at.replace(tzinfo=CST)
                previous = synced_at_by_code.get(code)
                if previous is None or synced_at > previous:
                    synced_at_by_code[code] = synced_at
            except ValueError:
                pass
        return {
            code: points[-days:]
            for code, points in points_by_code.items()
        }, synced_at_by_code

    def _daily_cache_needs_refresh(
        self,
        points: list[list[Any]],
        synced_at: datetime | None,
        trade_date: date,
        days: int,
        now: datetime,
    ) -> bool:
        if not points:
            return True
        synced_at_cst = synced_at.astimezone(CST) if synced_at else None
        checked_today = bool(
            synced_at_cst and synced_at_cst.date() == now.date()
        )
        latest_date = date.fromisoformat(str(points[-1][0]))
        expected_date = self._expected_daily_trade_date(trade_date, now)

        if len(points) < days and not checked_today:
            return True
        if latest_date < expected_date:
            if expected_date == now.date():
                open_at = datetime.combine(now.date(), MORNING_START, tzinfo=CST)
                return synced_at_cst is None or synced_at_cst < open_at
            return not checked_today
        if expected_date == now.date() and now.timetz().replace(tzinfo=None) > AFTERNOON_END:
            close_at = datetime.combine(now.date(), AFTERNOON_END, tzinfo=CST)
            return synced_at_cst is None or synced_at_cst <= close_at
        return False

    def _persist_daily_data(
        self,
        sector: dict[str, Any],
        data: dict[str, Any],
        updated_at: str,
        mutable_trade_date: date | None,
    ) -> None:
        rows: list[tuple[Any, ...]] = []
        mutable_row: tuple[float, float, float, float, float, str, str] | None = None
        for flow in data.get("flows") or []:
            trade_date_value = str(flow.get("trade_date") or "")
            try:
                date.fromisoformat(trade_date_value)
            except ValueError:
                continue
            rows.append((
                trade_date_value,
                str(sector["code"]),
                str(data.get("name") or sector.get("name") or ""),
                as_float(flow.get("main_net")),
                as_float(flow.get("super_large_net")),
                as_float(flow.get("large_net")),
                as_float(flow.get("mid_net")),
                as_float(flow.get("small_net")),
                updated_at,
            ))
            if mutable_trade_date and trade_date_value == mutable_trade_date.isoformat():
                mutable_row = (
                    as_float(flow.get("main_net")),
                    as_float(flow.get("super_large_net")),
                    as_float(flow.get("large_net")),
                    as_float(flow.get("mid_net")),
                    as_float(flow.get("small_net")),
                    trade_date_value,
                    str(sector["code"]),
                )
        if not rows:
            return
        self.connection.executemany(
            """
            INSERT INTO sector_flow_daily
                (trade_date, sector_code, sector_name, main_net,
                 super_large_net, large_net, mid_net, small_net, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (trade_date, sector_code) DO UPDATE SET
                sector_name = excluded.sector_name,
                updated_at = excluded.updated_at
            """,
            rows,
        )
        if mutable_row:
            self.connection.execute(
                """
                UPDATE sector_flow_daily
                SET main_net = ?, super_large_net = ?, large_net = ?,
                    mid_net = ?, small_net = ?
                WHERE trade_date = ? AND sector_code = ?
                """,
                mutable_row,
            )

    async def daily_history_data(
        self,
        trade_date: date,
        top: int = 30,
        days: int = 30,
        now: datetime | None = None,
        page: int | None = None,
    ) -> dict[str, Any]:
        async with self._daily_history_lock:
            return await self._daily_history_data(trade_date, top, days, now, page)

    async def _daily_history_data(
        self,
        trade_date: date,
        top: int,
        days: int,
        now: datetime | None,
        page: int | None,
    ) -> dict[str, Any]:
        current_time = now or datetime.now(CST)
        row = self.connection.execute(
            "SELECT MAX(trade_date) FROM sector_flow_selection WHERE trade_date <= ?",
            (trade_date.isoformat(),),
        ).fetchone()
        selection_date = date.fromisoformat(str(row[0])) if row and row[0] else None
        full_selection = self._load_selection(selection_date)[:top] if selection_date else []
        total_items = len(full_selection)
        total_pages = max(1, (total_items + DETAIL_PAGE_SIZE - 1) // DETAIL_PAGE_SIZE)
        selection = full_selection
        if page is not None:
            start = (page - 1) * DETAIL_PAGE_SIZE
            selection = full_selection[start:start + DETAIL_PAGE_SIZE]
        cached_points, synced_at_by_code = self._load_daily_cache(
            selection,
            trade_date,
            days,
        )
        pending = [
            sector
            for sector in selection
            if self._daily_cache_needs_refresh(
                cached_points.get(str(sector["code"]), []),
                synced_at_by_code.get(str(sector["code"])),
                trade_date,
                days,
                current_time,
            )
        ]
        semaphore = asyncio.Semaphore(2)

        async def fetch(sector: dict[str, Any]) -> tuple[dict[str, Any], Any]:
            async with semaphore:
                data = await fetch_sector_daily_data(str(sector["code"]), days)
                return sector, data

        data_by_code: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
        for attempt in range(1, 4):
            if not pending:
                break
            results = await asyncio.gather(*(fetch(sector) for sector in pending))
            pending = []
            for sector, data in results:
                code = str(sector["code"])
                if data is None:
                    pending.append(sector)
                else:
                    data_by_code[code] = (sector, data)
            if pending and attempt < 3:
                logger.warning(
                    "[sector-flow] Retrying daily history for %d sectors (round %d/3)",
                    len(pending),
                    attempt + 1,
                )
                await asyncio.sleep(float(attempt))

        updated_at = current_time.isoformat(timespec="seconds")
        mutable_trade_date = (
            current_time.date()
            if trade_date >= current_time.date()
            and current_time.timetz().replace(tzinfo=None) > AFTERNOON_END
            else None
        )
        for sector, data in data_by_code.values():
            self._persist_daily_data(
                sector,
                data,
                updated_at,
                mutable_trade_date,
            )
        if data_by_code:
            self._cleanup_daily_cache()
            self.connection.commit()

        cached_points, _ = self._load_daily_cache(selection, trade_date, days)

        series: list[dict[str, Any]] = []
        failed_codes: list[str] = []
        for sector in selection:
            code = str(sector["code"])
            points = cached_points.get(code, [])
            if not points:
                failed_codes.append(code)
            series.append({
                "rank": int(sector["rank"]),
                "sector_code": code,
                "sector_name": str(sector.get("name") or ""),
                "points": points,
            })

        return {
            "selection_date": selection_date.isoformat() if selection_date else None,
            "interval": "1d",
            "value_type": "daily_net",
            "days": days,
            "page": page,
            "page_size": DETAIL_PAGE_SIZE,
            "total_items": total_items,
            "total_pages": total_pages,
            "failed_codes": failed_codes,
            "refresh_failed_codes": [str(sector["code"]) for sector in pending],
            "series": series,
        }

    async def broadcast(self, message: dict[str, Any]) -> None:
        if not self._clients:
            return

        async def send(client: WebSocket) -> tuple[WebSocket, bool]:
            try:
                await asyncio.wait_for(client.send_json(message), timeout=5.0)
                return client, True
            except Exception:
                return client, False

        results = await asyncio.gather(*(send(client) for client in list(self._clients)))
        for client, success in results:
            if not success:
                self._clients.discard(client)

    async def websocket_handler(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self._clients.add(websocket)
        try:
            await websocket.send_json(self.snapshot_message())
            while True:
                try:
                    await asyncio.wait_for(websocket.receive_text(), timeout=15.0)
                except asyncio.TimeoutError:
                    await websocket.send_json({
                        "type": "heartbeat",
                        "data": self.status_data(),
                    })
        except WebSocketDisconnect:
            pass
        except Exception:
            logger.debug("[sector-flow] WebSocket disconnected", exc_info=True)
        finally:
            self._clients.discard(websocket)


sector_flow_service = SectorFlowRealtimeService()
