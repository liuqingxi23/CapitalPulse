"""Dynamically subscribed, persistent stock-level fund-flow snapshots."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import sqlite3
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect

from services.sector_flow_realtime import (
    CST,
    DEFAULT_DB_PATH,
    FLOW_COLUMNS,
    MORNING_START,
    market_status_at,
    minute_source_time,
)
from services.stock_flow_upstream import (
    QUOTE_ID_PATTERN,
    fetch_stock_flow_snapshot,
    fetch_stock_minute_data,
)
from utils.sector_selection import as_float

logger = logging.getLogger(__name__)


class StockFlowRealtimeService:
    """Share one three-second collector for each stock that has live clients."""

    def __init__(self) -> None:
        self.poll_seconds = max(1.0, float(os.getenv("STOCK_FLOW_POLL_SECONDS", "3")))
        self.retention_days = max(1, int(os.getenv("SECTOR_FLOW_RETENTION_DAYS", "30")))
        self.db_path = Path(os.getenv("SECTOR_FLOW_DB_PATH", str(DEFAULT_DB_PATH)))
        self._connection: sqlite3.Connection | None = None
        self.runtime_id = ""
        self._clients: dict[str, set[WebSocket]] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._backfill_tasks: dict[str, asyncio.Task[None]] = {}
        self._stock_meta: dict[str, dict[str, str]] = {}
        self._last_signatures: dict[str, tuple[Any, ...]] = {}
        self._backfill_locks: dict[str, asyncio.Lock] = {}
        self._backfilled_minutes: dict[tuple[date, str], int] = {}

    @property
    def ready(self) -> bool:
        return self._connection is not None

    @property
    def connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("stock-flow database is not open")
        return self._connection

    async def start(self) -> None:
        if self.ready:
            return
        self.runtime_id = uuid.uuid4().hex
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.db_path, check_same_thread=False)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS stock_flow_snapshot (
                trade_date TEXT NOT NULL,
                source_time INTEGER NOT NULL,
                quote_id TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                stock_name TEXT NOT NULL,
                main_net REAL NOT NULL,
                small_net REAL NOT NULL,
                mid_net REAL NOT NULL,
                large_net REAL NOT NULL,
                super_large_net REAL NOT NULL,
                received_at TEXT NOT NULL,
                granularity TEXT NOT NULL DEFAULT 'realtime',
                PRIMARY KEY (trade_date, source_time, quote_id)
            );

            CREATE INDEX IF NOT EXISTS idx_stock_flow_snapshot_quote_time
                ON stock_flow_snapshot (trade_date, quote_id, source_time);
            """
        )
        columns = {
            str(row[1])
            for row in self.connection.execute("PRAGMA table_info(stock_flow_snapshot)")
        }
        if "granularity" not in columns:
            self.connection.execute(
                "ALTER TABLE stock_flow_snapshot "
                "ADD COLUMN granularity TEXT NOT NULL DEFAULT 'realtime'"
            )
            self.connection.commit()
        self.cleanup_old_data(datetime.now(CST).date())

    async def stop(self) -> None:
        tasks = [*self._tasks.values(), *self._backfill_tasks.values()]
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._tasks.clear()
        self._backfill_tasks.clear()
        self._backfill_locks.clear()
        self._backfilled_minutes.clear()
        for clients in self._clients.values():
            for client in clients:
                with contextlib.suppress(Exception):
                    await client.close()
        self._clients.clear()
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def cleanup_old_data(self, today: date) -> None:
        cutoff = (today - timedelta(days=self.retention_days)).isoformat()
        self.connection.execute(
            "DELETE FROM stock_flow_snapshot WHERE trade_date < ?",
            (cutoff,),
        )
        self.connection.commit()

    def _persist_snapshot(self, trade_date: date, flow: dict[str, Any]) -> None:
        self.connection.execute(
            """
            INSERT INTO stock_flow_snapshot
                (trade_date, source_time, quote_id, stock_code, stock_name,
                 main_net, small_net, mid_net, large_net, super_large_net,
                 received_at, granularity)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'realtime')
            ON CONFLICT (trade_date, source_time, quote_id) DO UPDATE SET
                stock_code = excluded.stock_code,
                stock_name = excluded.stock_name,
                main_net = excluded.main_net,
                small_net = excluded.small_net,
                mid_net = excluded.mid_net,
                large_net = excluded.large_net,
                super_large_net = excluded.super_large_net,
                received_at = excluded.received_at,
                granularity = 'realtime'
            """,
            (
                trade_date.isoformat(),
                int(flow["source_time"]),
                str(flow["quote_id"]),
                str(flow["code"]),
                str(flow["name"]),
                *(as_float(flow[column]) for column in FLOW_COLUMNS),
                str(flow["received_at"]),
            ),
        )
        self.connection.commit()

    def _persist_minute_backfill(
        self,
        trade_date: date,
        quote_id: str,
        flows: list[dict[str, Any]],
        received_at: str,
    ) -> int:
        """Insert authentic historical points only into empty minute buckets."""
        occupied_rows = self.connection.execute(
            """
            SELECT DISTINCT CAST(source_time / 60 AS INTEGER)
            FROM stock_flow_snapshot
            WHERE trade_date = ? AND quote_id = ?
            """,
            (trade_date.isoformat(), quote_id),
        ).fetchall()
        occupied = {int(row[0]) for row in occupied_rows}
        rows: list[tuple[Any, ...]] = []
        for flow in sorted(flows, key=lambda item: int(item["source_time"])):
            minute_bucket = int(flow["source_time"]) // 60
            if minute_bucket in occupied:
                continue
            occupied.add(minute_bucket)
            rows.append((
                trade_date.isoformat(),
                int(flow["source_time"]),
                quote_id,
                str(flow["code"]),
                str(flow["name"]),
                *(as_float(flow[column]) for column in FLOW_COLUMNS),
                received_at,
            ))
        if not rows:
            return 0
        before = self.connection.total_changes
        self.connection.executemany(
            """
            INSERT INTO stock_flow_snapshot
                (trade_date, source_time, quote_id, stock_code, stock_name,
                 main_net, small_net, mid_net, large_net, super_large_net,
                 received_at, granularity)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'minute_backfill')
            ON CONFLICT (trade_date, source_time, quote_id) DO NOTHING
            """,
            rows,
        )
        self.connection.commit()
        return self.connection.total_changes - before

    async def backfill_history(
        self,
        quote_id: str,
        code: str = "",
        name: str = "",
        now: datetime | None = None,
    ) -> int:
        """Backfill today's minute history before continuing with live seconds."""
        current = now or datetime.now(CST)
        if (
            current.weekday() >= 5
            or current.timetz().replace(tzinfo=None) <= MORNING_START
        ):
            return 0
        lock = self._backfill_locks.setdefault(quote_id, asyncio.Lock())
        async with lock:
            key = (current.date(), quote_id)
            current_minute = int(current.timestamp()) // 60
            if self._backfilled_minutes.get(key) == current_minute:
                return 0
            data = await fetch_stock_minute_data(quote_id, 240)
            if data is None:
                return 0
            meta = self._stock_meta.get(quote_id, {})
            stock_code = str(data.get("code") or code or meta.get("code") or "")
            stock_name = str(data.get("name") or name or meta.get("name") or "")
            upper_bound = int(current.timestamp())
            candidates: list[dict[str, Any]] = []
            for flow in data.get("flows") or []:
                source_time = minute_source_time(flow.get("time"), current.date())
                if source_time is None or source_time > upper_bound:
                    continue
                candidates.append({
                    "source_time": source_time,
                    "code": stock_code,
                    "name": stock_name,
                    **{
                        column: as_float(flow.get(column))
                        for column in FLOW_COLUMNS
                    },
                })
            received_at = datetime.now(CST).isoformat(timespec="milliseconds")
            inserted = self._persist_minute_backfill(
                current.date(),
                quote_id,
                candidates,
                received_at,
            )
            self._backfilled_minutes[key] = current_minute
            if inserted:
                logger.info(
                    "[stock-flow] Backfilled %d minute points for %s",
                    inserted,
                    quote_id,
                )
            return inserted

    async def _run_backfill_and_broadcast(
        self,
        quote_id: str,
        code: str,
        name: str,
    ) -> None:
        """Backfill without delaying the first live snapshot or collector."""
        try:
            inserted = await self.backfill_history(quote_id, code, name)
            if inserted and self._clients.get(quote_id):
                await self._broadcast(quote_id, {
                    "type": "snapshot",
                    "data": self.history_data(
                        datetime.now(CST).date(),
                        quote_id,
                        code,
                        name,
                    ),
                })
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[stock-flow] History backfill failed for %s", quote_id)
        finally:
            self._backfill_tasks.pop(quote_id, None)

    def history_data(
        self,
        trade_date: date,
        quote_id: str,
        code: str = "",
        name: str = "",
    ) -> dict[str, Any]:
        rows = self.connection.execute(
            """
            SELECT source_time, stock_code, stock_name, main_net,
                   super_large_net, large_net, mid_net, small_net
            FROM stock_flow_snapshot
            WHERE trade_date = ? AND quote_id = ?
            ORDER BY source_time
            """,
            (trade_date.isoformat(), quote_id),
        ).fetchall()
        if rows:
            code = str(rows[-1][1])
            name = str(rows[-1][2])
        return {
            "runtime_id": self.runtime_id,
            "trade_date": trade_date.isoformat(),
            "stock": {"quote_id": quote_id, "code": code, "name": name},
            "points": [
                [
                    int(row[0]),
                    float(row[3]),
                    float(row[4]),
                    float(row[5]),
                    float(row[6]),
                    float(row[7]),
                ]
                for row in rows
            ],
            "poll_seconds": self.poll_seconds,
            "market_status": market_status_at(datetime.now(CST)),
        }

    async def collect_once(
        self,
        quote_id: str,
        now: datetime | None = None,
    ) -> bool:
        current = now or datetime.now(CST)
        data = await fetch_stock_flow_snapshot(quote_id)
        if not data or int(data.get("source_time") or 0) <= 0:
            return False
        source_time = int(data["source_time"])
        if datetime.fromtimestamp(source_time, CST).date() != current.date():
            return False
        meta = self._stock_meta.get(quote_id, {})
        data["code"] = str(data.get("code") or meta.get("code") or "")
        data["name"] = str(data.get("name") or meta.get("name") or "")
        data["received_at"] = datetime.now(CST).isoformat(timespec="milliseconds")
        signature = (
            source_time,
            *(as_float(data[column]) for column in FLOW_COLUMNS),
        )
        if signature == self._last_signatures.get(quote_id):
            return False
        self._persist_snapshot(current.date(), data)
        self._last_signatures[quote_id] = signature
        await self._broadcast(quote_id, {"type": "update", "data": data})
        return True

    async def _broadcast(self, quote_id: str, message: dict[str, Any]) -> None:
        clients = list(self._clients.get(quote_id, set()))
        if not clients:
            return

        async def send(client: WebSocket) -> tuple[WebSocket, bool]:
            try:
                await asyncio.wait_for(client.send_json(message), timeout=5.0)
                return client, True
            except Exception:
                return client, False

        results = await asyncio.gather(*(send(client) for client in clients))
        for client, success in results:
            if not success:
                self._clients.get(quote_id, set()).discard(client)

    async def _collector_loop(self, quote_id: str) -> None:
        try:
            while self._clients.get(quote_id):
                now = datetime.now(CST)
                status = market_status_at(now)
                if status == "open":
                    try:
                        await self.collect_once(quote_id, now)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.exception("[stock-flow] Poll failed for %s", quote_id)
                    await asyncio.sleep(self.poll_seconds)
                else:
                    await self._broadcast(
                        quote_id,
                        {"type": "status", "data": {"market_status": status}},
                    )
                    await asyncio.sleep(15.0)
        finally:
            self._tasks.pop(quote_id, None)

    async def websocket_handler(
        self,
        websocket: WebSocket,
        quote_id: str,
        code: str,
        name: str,
    ) -> None:
        if not self.ready or not QUOTE_ID_PATTERN.fullmatch(quote_id):
            await websocket.close(code=1008)
            return
        await websocket.accept()
        clients = self._clients.setdefault(quote_id, set())
        clients.add(websocket)
        self._stock_meta[quote_id] = {"code": code, "name": name}
        await websocket.send_json({
            "type": "snapshot",
            "data": self.history_data(datetime.now(CST).date(), quote_id, code, name),
        })
        if quote_id not in self._tasks:
            self._tasks[quote_id] = asyncio.create_task(
                self._collector_loop(quote_id),
                name=f"stock-flow-{quote_id}",
            )
        if quote_id not in self._backfill_tasks:
            self._backfill_tasks[quote_id] = asyncio.create_task(
                self._run_backfill_and_broadcast(quote_id, code, name),
                name=f"stock-flow-backfill-{quote_id}",
            )
        try:
            while True:
                try:
                    await asyncio.wait_for(websocket.receive_text(), timeout=15.0)
                except asyncio.TimeoutError:
                    await websocket.send_json({
                        "type": "heartbeat",
                        "data": {"market_status": market_status_at(datetime.now(CST))},
                    })
        except WebSocketDisconnect:
            pass
        except Exception:
            logger.debug("[stock-flow] WebSocket disconnected", exc_info=True)
        finally:
            clients.discard(websocket)
            if not clients:
                self._clients.pop(quote_id, None)
                task = self._tasks.get(quote_id)
                if task and not task.done():
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task


stock_flow_service = StockFlowRealtimeService()
