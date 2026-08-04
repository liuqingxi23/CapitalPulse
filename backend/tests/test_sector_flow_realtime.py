import json
import sqlite3
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

from services.sector_flow_realtime import (
    CST,
    SectorFlowRealtimeService,
    market_status_at,
    minute_source_time,
    parse_snapshot_items,
)


def make_sectors(count: int = 30) -> list[dict]:
    return [
        {
            "code": f"BK{index:04d}",
            "name": f"行业{index}",
            "market_cap": float(10_000 - index),
            "sector_type": "industry",
        }
        for index in range(1, count + 1)
    ]


def make_flow(code: str, source_time: int, main_net: float = 100.0) -> dict:
    return {
        "sector_code": code,
        "sector_name": f"行业{code}",
        "source_time": source_time,
        "main_net": main_net,
        "small_net": -10.0,
        "mid_net": -20.0,
        "large_net": 60.0,
        "super_large_net": 40.0,
    }


class FakeWebSocket:
    def __init__(self, fails: bool = False) -> None:
        self.fails = fails
        self.messages: list[dict] = []

    async def send_json(self, message: dict) -> None:
        if self.fails:
            raise ConnectionError("client disconnected")
        self.messages.append(message)


class SectorFlowParsingTests(unittest.TestCase):
    def test_market_session_boundaries_and_weekend(self):
        self.assertEqual(market_status_at(datetime(2026, 7, 27, 9, 29, tzinfo=CST)), "preopen")
        self.assertEqual(market_status_at(datetime(2026, 7, 27, 9, 30, tzinfo=CST)), "open")
        self.assertEqual(market_status_at(datetime(2026, 7, 27, 11, 30, tzinfo=CST)), "open")
        self.assertEqual(market_status_at(datetime(2026, 7, 27, 11, 31, tzinfo=CST)), "lunch")
        self.assertEqual(market_status_at(datetime(2026, 7, 27, 13, 0, tzinfo=CST)), "open")
        self.assertEqual(market_status_at(datetime(2026, 7, 27, 15, 1, tzinfo=CST)), "closed")
        self.assertEqual(market_status_at(datetime(2026, 7, 26, 10, 0, tzinfo=CST)), "closed")

    def test_daily_expected_date_switches_to_today_at_market_open(self):
        trade_date = date(2026, 7, 27)

        self.assertEqual(
            SectorFlowRealtimeService._expected_daily_trade_date(
                trade_date,
                datetime(2026, 7, 27, 9, 29, tzinfo=CST),
            ),
            date(2026, 7, 24),
        )
        self.assertEqual(
            SectorFlowRealtimeService._expected_daily_trade_date(
                trade_date,
                datetime(2026, 7, 27, 9, 30, tzinfo=CST),
            ),
            trade_date,
        )
        service = SectorFlowRealtimeService()
        previous_points = [["2026-07-24", 100.0, 40.0, 60.0, -20.0, -10.0]]
        self.assertTrue(service._daily_cache_needs_refresh(
            previous_points,
            datetime(2026, 7, 27, 9, 0, tzinfo=CST),
            trade_date,
            1,
            datetime(2026, 7, 27, 9, 30, tzinfo=CST),
        ))

    def test_parses_all_fields_and_defaults_missing_values(self):
        parsed = parse_snapshot_items([
            {
                "f12": "bk0001",
                "f14": "行业一",
                "f62": "12.5",
                "f66": 4,
                "f72": None,
                "f78": "-2.5",
                "f84": "-10",
                "f124": "1785115800",
            },
            {"f12": "BK0002", "f124": 0},
            "invalid",
        ])

        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0]["sector_code"], "BK0001")
        self.assertEqual(parsed[0]["main_net"], 12.5)
        self.assertEqual(parsed[0]["large_net"], 0.0)
        self.assertEqual(parsed[0]["small_net"], -10.0)

    def test_parses_only_current_trade_date_session_minutes(self):
        trade_date = date(2026, 7, 27)

        self.assertEqual(
            minute_source_time("2026-07-27 09:30", trade_date),
            int(datetime(2026, 7, 27, 9, 30, tzinfo=CST).timestamp()),
        )
        self.assertIsNone(minute_source_time("2026-07-27 12:00", trade_date))
        self.assertIsNone(minute_source_time("2026-07-26 10:00", trade_date))
        self.assertIsNone(minute_source_time("invalid", trade_date))


class SectorFlowDatabaseTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.service = SectorFlowRealtimeService()
        self.service.db_path = Path(self.temp_dir.name) / "sector-flow.sqlite3"
        self.service._open_database()

    def tearDown(self):
        if self.service.ready:
            self.service.connection.close()
            self.service._connection = None
        self.temp_dir.cleanup()

    async def test_daily_selection_is_reused_without_refetching(self):
        trade_date = date(2026, 7, 27)
        self.service._save_selection(trade_date, make_sectors())

        with (
            patch(
                "services.sector_flow_realtime.fetch_all_industry_sectors",
                new=AsyncMock(),
            ) as get_sectors,
            patch.object(self.service, "broadcast", new=AsyncMock()),
        ):
            result = await self.service._ensure_selection(trade_date)

        self.assertTrue(result)
        self.assertEqual(len(self.service._selection), 30)
        self.assertEqual(self.service._selection[0]["rank"], 1)
        get_sectors.assert_not_awaited()

    def test_upsert_revision_history_order_and_top_cropping(self):
        trade_date = date(2026, 7, 27)
        self.service._save_selection(trade_date, make_sectors())
        first_time = int(datetime(2026, 7, 27, 9, 30, tzinfo=CST).timestamp())
        second_time = first_time + 3

        self.service._persist_snapshot(
            trade_date,
            [
                make_flow("BK0001", first_time, 100),
                make_flow("BK0011", first_time, 200),
            ],
            "2026-07-27T09:30:00+08:00",
        )
        self.service._persist_snapshot(
            trade_date,
            [
                make_flow("BK0001", first_time, 110),
                make_flow("BK0001", second_time, 120),
            ],
            "2026-07-27T09:30:03+08:00",
        )

        rows = self.service.connection.execute(
            "SELECT source_time, main_net FROM sector_flow_snapshot "
            "WHERE sector_code = 'BK0001' ORDER BY source_time"
        ).fetchall()
        history = self.service.history_data(trade_date, 10)
        full_history = self.service.history_data(trade_date, 30)

        self.assertEqual(rows, [(first_time, 110.0), (second_time, 120.0)])
        self.assertEqual(len(history["selection"]), 10)
        self.assertEqual(history["series"][0]["points"], [[first_time, 110.0], [second_time, 120.0]])
        self.assertNotIn("BK0011", {series["sector_code"] for series in history["series"]})
        self.assertEqual(
            {flow["sector_code"] for flow in full_history["latest"]},
            {"BK0001", "BK0011"},
        )

    def test_detail_history_paginates_six_sectors_and_returns_all_metrics(self):
        trade_date = date(2026, 7, 27)
        self.service._save_selection(trade_date, make_sectors())
        first_time = int(datetime(2026, 7, 27, 9, 30, tzinfo=CST).timestamp())
        second_time = first_time + 3
        self.service._persist_snapshot(
            trade_date,
            [
                make_flow("BK0007", second_time, 777),
                make_flow("BK0007", first_time, 700),
                make_flow("BK0030", first_time, 3000),
            ],
            "2026-07-27T09:30:03+08:00",
        )

        first_page = self.service.detail_history_data(trade_date, 1)
        second_page = self.service.detail_history_data(trade_date, 2)
        last_page = self.service.detail_history_data(trade_date, 5)

        self.assertEqual(first_page["page_size"], 6)
        self.assertEqual(first_page["total_items"], 30)
        self.assertEqual(first_page["total_pages"], 5)
        self.assertEqual(
            [series["sector_code"] for series in first_page["series"]],
            [f"BK{index:04d}" for index in range(1, 7)],
        )
        self.assertEqual(second_page["series"][0]["sector_code"], "BK0007")
        self.assertEqual(
            second_page["series"][0]["points"],
            [
                [first_time, 700.0, 40.0, 60.0, -20.0, -10.0],
                [second_time, 777.0, 40.0, 60.0, -20.0, -10.0],
            ],
        )
        self.assertEqual(
            [series["rank"] for series in last_page["series"]],
            list(range(25, 31)),
        )

    async def test_daily_history_uses_latest_selection_and_daily_upstream(self):
        selection_date = date(2026, 7, 27)
        self.service._save_selection(selection_date, make_sectors())

        async def daily_data(code, limit):
            self.assertEqual(limit, 30)
            return {
                "code": code,
                "name": f"行业{code}",
                "interval": "1d",
                "value_type": "daily_net",
                "flows": [
                    {
                        "trade_date": "2026-07-24",
                        "main_net": 100,
                        "super_large_net": 40,
                        "large_net": 60,
                        "mid_net": -20,
                        "small_net": -10,
                    },
                    {
                        "trade_date": "2026-07-25",
                        "main_net": 200,
                        "super_large_net": 80,
                        "large_net": 120,
                        "mid_net": -30,
                        "small_net": -20,
                    },
                ],
            }

        with patch(
            "services.sector_flow_realtime.fetch_sector_daily_data",
            new=AsyncMock(side_effect=daily_data),
        ) as fetch:
            result = await self.service.daily_history_data(date(2026, 7, 28))

        self.assertEqual(fetch.await_count, 30)
        self.assertEqual(result["selection_date"], "2026-07-27")
        self.assertEqual(result["interval"], "1d")
        self.assertEqual(result["value_type"], "daily_net")
        self.assertEqual(result["total_items"], 30)
        self.assertEqual(result["failed_codes"], [])
        self.assertEqual(
            result["series"][0]["points"],
            [
                ["2026-07-24", 100.0, 40.0, 60.0, -20.0, -10.0],
                ["2026-07-25", 200.0, 80.0, 120.0, -30.0, -20.0],
            ],
        )

    async def test_daily_history_paginates_six_sectors(self):
        selection_date = date(2026, 7, 27)
        self.service._save_selection(selection_date, make_sectors())

        async def daily_data(code, _limit):
            return {
                "code": code,
                "name": code,
                "flows": [{"trade_date": "2026-07-25", "main_net": 100}],
            }

        with patch(
            "services.sector_flow_realtime.fetch_sector_daily_data",
            new=AsyncMock(side_effect=daily_data),
        ) as fetch:
            result = await self.service.daily_history_data(
                selection_date,
                page=2,
            )

        self.assertEqual(fetch.await_count, 6)
        self.assertEqual(result["page"], 2)
        self.assertEqual(result["page_size"], 6)
        self.assertEqual(result["total_items"], 30)
        self.assertEqual(result["total_pages"], 5)
        self.assertEqual(
            [series["sector_code"] for series in result["series"]],
            [f"BK{index:04d}" for index in range(7, 13)],
        )

    async def test_daily_history_retries_only_failed_sectors(self):
        selection_date = date(2026, 7, 27)
        self.service._save_selection(selection_date, make_sectors())
        attempts: dict[str, int] = {}

        async def flaky_daily_data(code, _limit):
            attempts[code] = attempts.get(code, 0) + 1
            if attempts[code] == 1:
                return None
            return {
                "code": code,
                "name": code,
                "flows": [{"trade_date": "2026-07-25", "main_net": 100}],
            }

        with (
            patch(
                "services.sector_flow_realtime.fetch_sector_daily_data",
                new=AsyncMock(side_effect=flaky_daily_data),
            ) as fetch,
            patch("services.sector_flow_realtime.asyncio.sleep", new=AsyncMock()),
        ):
            result = await self.service.daily_history_data(selection_date)

        self.assertEqual(fetch.await_count, 60)
        self.assertEqual(result["failed_codes"], [])
        self.assertTrue(all(count == 2 for count in attempts.values()))

    async def test_daily_history_reuses_sqlite_cache(self):
        selection_date = date(2026, 7, 27)
        now = datetime(2026, 7, 27, 15, 10, tzinfo=CST)
        self.service._save_selection(selection_date, make_sectors())

        async def daily_data(code, _limit):
            return {
                "code": code,
                "name": code,
                "flows": [
                    {"trade_date": "2026-07-27", "main_net": 100},
                ],
            }

        with patch(
            "services.sector_flow_realtime.fetch_sector_daily_data",
            new=AsyncMock(side_effect=daily_data),
        ) as fetch:
            first = await self.service.daily_history_data(selection_date, now=now)
            db_path = self.service.db_path
            self.service.connection.close()
            self.service._connection = None
            self.service = SectorFlowRealtimeService()
            self.service.db_path = db_path
            self.service._open_database()
            second = await self.service.daily_history_data(selection_date, now=now)

        self.assertEqual(fetch.await_count, 30)
        self.assertEqual(first["series"], second["series"])
        self.assertEqual(second["failed_codes"], [])
        cached = self.service.connection.execute(
            "SELECT COUNT(*) FROM sector_flow_daily"
        ).fetchone()
        self.assertEqual(cached, (30,))

    async def test_daily_history_refreshes_current_day_after_close(self):
        selection_date = date(2026, 7, 27)
        before_close = datetime(2026, 7, 27, 14, 0, tzinfo=CST)
        after_close = datetime(2026, 7, 27, 15, 10, tzinfo=CST)
        self.service._save_selection(selection_date, make_sectors())
        value = 100
        historical_value = 50

        async def daily_data(code, _limit):
            return {
                "code": code,
                "name": code,
                "flows": [
                    {"trade_date": "2026-07-24", "main_net": historical_value},
                    {"trade_date": "2026-07-27", "main_net": value},
                ],
            }

        with patch(
            "services.sector_flow_realtime.fetch_sector_daily_data",
            new=AsyncMock(side_effect=daily_data),
        ) as fetch:
            await self.service.daily_history_data(selection_date, now=before_close)
            value = 200
            historical_value = 999
            result = await self.service.daily_history_data(selection_date, now=after_close)

        self.assertEqual(fetch.await_count, 60)
        self.assertEqual(
            result["series"][0]["points"],
            [
                ["2026-07-24", 50.0, 0.0, 0.0, 0.0, 0.0],
                ["2026-07-27", 200.0, 0.0, 0.0, 0.0, 0.0],
            ],
        )

    async def test_daily_cache_refresh_runs_once_after_close(self):
        trade_date = date(2026, 7, 27)
        before_close = datetime(2026, 7, 27, 14, 59, tzinfo=CST)
        after_close = datetime(2026, 7, 27, 15, 1, tzinfo=CST)
        self.service._selection_date = trade_date
        result = {"failed_codes": [], "refresh_failed_codes": []}

        with patch.object(
            self.service,
            "daily_history_data",
            new=AsyncMock(return_value=result),
        ) as refresh:
            await self.service._refresh_daily_cache_after_close(before_close, 100.0)
            await self.service._refresh_daily_cache_after_close(after_close, 200.0)
            await self.service._refresh_daily_cache_after_close(after_close, 300.0)

        refresh.assert_awaited_once_with(trade_date, now=after_close)
        self.assertEqual(self.service._daily_refresh_date, trade_date)

    async def test_daily_cache_refresh_failure_uses_five_minute_backoff(self):
        trade_date = date(2026, 7, 27)
        after_close = datetime(2026, 7, 27, 15, 1, tzinfo=CST)
        self.service._selection_date = trade_date
        result = {"failed_codes": [], "refresh_failed_codes": ["BK0001"]}

        with patch.object(
            self.service,
            "daily_history_data",
            new=AsyncMock(return_value=result),
        ) as refresh:
            await self.service._refresh_daily_cache_after_close(after_close, 1000.0)
            await self.service._refresh_daily_cache_after_close(after_close, 1100.0)
            await self.service._refresh_daily_cache_after_close(after_close, 1301.0)

        self.assertEqual(refresh.await_count, 2)
        self.assertIsNone(self.service._daily_refresh_date)

    def test_retention_cleanup_removes_only_expired_dates(self):
        self.service.retention_days = 30
        old_date = date(2026, 6, 26)
        boundary_date = date(2026, 6, 27)
        self.service._save_selection(old_date, make_sectors())
        self.service._save_selection(boundary_date, make_sectors())
        self.service._cleanup_old_data(date(2026, 7, 27))

        remaining = self.service.connection.execute(
            "SELECT DISTINCT trade_date FROM sector_flow_selection ORDER BY trade_date"
        ).fetchall()
        self.assertEqual(remaining, [("2026-06-27",)])

    def test_daily_cache_cleanup_keeps_latest_30_rows_and_removes_orphans(self):
        today = date(2026, 7, 27)
        self.service._save_selection(today, make_sectors())
        rows = [
            (
                (today - timedelta(days=offset)).isoformat(),
                "BK0001",
                "BK0001",
                float(offset),
                "2026-07-27T15:10:00+08:00",
            )
            for offset in range(35)
        ]
        rows.append((
            today.isoformat(),
            "ORPHAN",
            "ORPHAN",
            1.0,
            "2026-07-27T15:10:00+08:00",
        ))
        self.service.connection.executemany(
            """
            INSERT INTO sector_flow_daily
                (trade_date, sector_code, sector_name, main_net, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.service.connection.commit()

        self.service._cleanup_old_data(today)

        retained = self.service.connection.execute(
            "SELECT trade_date FROM sector_flow_daily "
            "WHERE sector_code = 'BK0001' ORDER BY trade_date"
        ).fetchall()
        orphan_count = self.service.connection.execute(
            "SELECT COUNT(*) FROM sector_flow_daily WHERE sector_code = 'ORPHAN'"
        ).fetchone()
        self.assertEqual(len(retained), 30)
        self.assertEqual(retained[0], ((today - timedelta(days=29)).isoformat(),))
        self.assertEqual(orphan_count, (0,))

    async def test_minute_backfill_fills_empty_minutes_without_overwriting_seconds(self):
        current = datetime(2026, 7, 27, 10, 0, tzinfo=CST)
        sectors = make_sectors()
        self.service._save_selection(current.date(), sectors)
        self.service._selection = [
            {**sector, "rank": rank}
            for rank, sector in enumerate(sectors, start=1)
        ]
        self.service._selection_date = current.date()
        self.service._reset_backfill_for_date(current.date())

        realtime_time = int(datetime(2026, 7, 27, 9, 31, 20, tzinfo=CST).timestamp())
        self.service._persist_snapshot(
            current.date(),
            [make_flow("BK0001", realtime_time, 999)],
            "2026-07-27T09:31:20+08:00",
        )

        async def minute_data(code, _limit):
            return {
                "code": code,
                "name": f"行业{code}",
                "flows": [
                    {
                        "time": "2026-07-27 09:30",
                        "main_net": 100,
                        "small_net": -10,
                        "mid_net": -20,
                        "large_net": 60,
                        "super_large_net": 40,
                    },
                    {
                        "time": "2026-07-27 09:31",
                        "main_net": 110,
                        "small_net": -11,
                        "mid_net": -21,
                        "large_net": 61,
                        "super_large_net": 41,
                    },
                ],
            }

        with (
            patch(
                "services.sector_flow_realtime.fetch_sector_minute_data",
                new=AsyncMock(side_effect=minute_data),
            ) as fetch,
            patch.object(self.service, "broadcast", new=AsyncMock()) as broadcast,
        ):
            await self.service._run_minute_backfill(current)

        rows = self.service.connection.execute(
            """
            SELECT sector_code, source_time, main_net, granularity
            FROM sector_flow_snapshot
            ORDER BY sector_code, source_time
            """
        ).fetchall()
        bk1_rows = [row for row in rows if row[0] == "BK0001"]

        self.assertEqual(fetch.await_count, 30)
        self.assertEqual(len(rows), 60)
        self.assertEqual(
            [row[3] for row in bk1_rows],
            ["minute_backfill", "realtime"],
        )
        self.assertEqual(bk1_rows[-1][2], 999.0)
        self.assertEqual(self.service._backfill_status, "complete")
        self.assertEqual(self.service._backfill_inserted_points, 59)
        self.assertTrue(any(
            call.args[0]["type"] == "history_backfill"
            for call in broadcast.await_args_list
        ))

    def test_open_database_migrates_legacy_snapshot_table(self):
        legacy_path = Path(self.temp_dir.name) / "legacy.sqlite3"
        legacy = sqlite3.connect(legacy_path)
        legacy.execute(
            """
            CREATE TABLE sector_flow_snapshot (
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
                PRIMARY KEY (trade_date, source_time, sector_code)
            )
            """
        )
        legacy.execute(
            """
            CREATE TABLE sector_flow_daily (
                trade_date TEXT NOT NULL,
                sector_code TEXT NOT NULL,
                sector_name TEXT NOT NULL,
                main_net REAL NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (trade_date, sector_code)
            )
            """
        )
        legacy.execute(
            """
            INSERT INTO sector_flow_daily
                (trade_date, sector_code, sector_name, main_net, updated_at)
            VALUES ('2026-07-27', 'BK0001', '行业1', 100, '2026-07-27T15:10:00+08:00')
            """
        )
        legacy.commit()
        legacy.close()

        migrated = SectorFlowRealtimeService()
        migrated.db_path = legacy_path
        migrated._open_database()
        try:
            columns = {
                row[1]
                for row in migrated.connection.execute(
                    "PRAGMA table_info(sector_flow_snapshot)"
                ).fetchall()
            }
            daily_columns = {
                row[1]
                for row in migrated.connection.execute(
                    "PRAGMA table_info(sector_flow_daily)"
                ).fetchall()
            }
            daily_count = migrated.connection.execute(
                "SELECT COUNT(*) FROM sector_flow_daily"
            ).fetchone()[0]
        finally:
            migrated.connection.close()
            migrated._connection = None

        self.assertIn("granularity", columns)
        self.assertTrue({
            "super_large_net",
            "large_net",
            "mid_net",
            "small_net",
        }.issubset(daily_columns))
        self.assertEqual(daily_count, 0)

    async def test_collect_persists_before_broadcast_and_skips_duplicate(self):
        current = datetime(2026, 7, 27, 10, 0, tzinfo=CST)
        source_time = int(current.timestamp())
        self.service._selection = [
            {**sector, "rank": rank}
            for rank, sector in enumerate(make_sectors(), start=1)
        ]
        self.service._selection_date = current.date()
        flow = make_flow("BK0001", source_time)

        async def assert_persisted(message):
            count = self.service.connection.execute(
                "SELECT COUNT(*) FROM sector_flow_snapshot"
            ).fetchone()[0]
            self.assertEqual(count, 1)
            self.assertEqual(message["type"], "update")

        with (
            patch.object(self.service, "_fetch_snapshot", new=AsyncMock(return_value=[flow])),
            patch.object(self.service, "broadcast", new=AsyncMock(side_effect=assert_persisted)) as broadcast,
        ):
            self.assertTrue(await self.service.collect_once(current))
            self.assertFalse(await self.service.collect_once(current))

        self.assertEqual(broadcast.await_count, 1)
        self.assertEqual(
            self.service.connection.execute(
                "SELECT COUNT(*) FROM sector_flow_snapshot"
            ).fetchone()[0],
            1,
        )

    async def test_stale_snapshot_is_not_persisted(self):
        current = datetime(2026, 7, 27, 10, 0, tzinfo=CST)
        stale_time = int(datetime(2026, 7, 26, 10, 0, tzinfo=CST).timestamp())
        self.service._selection = [
            {**sector, "rank": rank}
            for rank, sector in enumerate(make_sectors(), start=1)
        ]
        self.service._selection_date = current.date()

        with (
            patch.object(
                self.service,
                "_fetch_snapshot",
                new=AsyncMock(return_value=[make_flow("BK0001", stale_time)]),
            ),
            patch.object(self.service, "broadcast", new=AsyncMock()),
        ):
            self.assertFalse(await self.service.collect_once(current))

        self.assertEqual(self.service.status_data()["market_status"], "stale")
        self.assertEqual(
            self.service.connection.execute(
                "SELECT COUNT(*) FROM sector_flow_snapshot"
            ).fetchone()[0],
            0,
        )

    async def test_primary_failure_uses_delay_endpoint(self):
        self.service._selection = [
            {**sector, "rank": rank}
            for rank, sector in enumerate(make_sectors(), start=1)
        ]
        payload = json.dumps({
            "data": {
                "diff": [{
                    "f12": "BK0001",
                    "f14": "行业一",
                    "f62": 1,
                    "f66": 2,
                    "f72": 3,
                    "f78": 4,
                    "f84": 5,
                    "f124": 1785115800,
                }]
            }
        })
        fetch = AsyncMock(side_effect=[None, payload])

        with patch("services.sector_flow_realtime.safe_fetch", new=fetch):
            result = await self.service._fetch_snapshot()

        self.assertEqual(fetch.await_count, 2)
        self.assertEqual(result[0]["sector_code"], "BK0001")

    async def test_failed_websocket_is_isolated(self):
        healthy = FakeWebSocket()
        failed = FakeWebSocket(fails=True)
        self.service._clients.update({healthy, failed})

        await self.service.broadcast({"type": "status", "data": {}})

        self.assertEqual(healthy.messages, [{"type": "status", "data": {}}])
        self.assertIn(healthy, self.service._clients)
        self.assertNotIn(failed, self.service._clients)


if __name__ == "__main__":
    unittest.main()
