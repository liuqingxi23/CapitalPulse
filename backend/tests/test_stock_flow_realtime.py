import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

from services.sector_flow_realtime import CST
from services.stock_flow_realtime import StockFlowRealtimeService


def make_stock_flow(source_time: int) -> dict:
    return {
        "quote_id": "1.600519",
        "code": "600519",
        "name": "贵州茅台",
        "source_time": source_time,
        "main_net": 100.0,
        "small_net": -80.0,
        "mid_net": -20.0,
        "large_net": 60.0,
        "super_large_net": 40.0,
    }


class StockFlowRealtimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.service = StockFlowRealtimeService()
        self.service.db_path = Path(self.temp_dir.name) / "stock-flow.sqlite3"
        await self.service.start()

    async def asyncTearDown(self):
        await self.service.stop()
        self.temp_dir.cleanup()

    async def test_collect_persists_and_broadcasts_five_cumulative_fields(self):
        current = datetime(2026, 8, 5, 10, 0, tzinfo=CST)
        flow = make_stock_flow(int(current.timestamp()))
        self.service._stock_meta["1.600519"] = {
            "code": "600519",
            "name": "贵州茅台",
        }

        with (
            patch(
                "services.stock_flow_realtime.fetch_stock_flow_snapshot",
                new=AsyncMock(return_value=flow),
            ),
            patch.object(self.service, "_broadcast", new=AsyncMock()) as broadcast,
        ):
            self.assertTrue(await self.service.collect_once("1.600519", current))
            self.assertFalse(await self.service.collect_once("1.600519", current))

        history = self.service.history_data(current.date(), "1.600519")
        self.assertEqual(history["stock"]["name"], "贵州茅台")
        self.assertEqual(history["points"], [[
            int(current.timestamp()),
            100.0,
            40.0,
            60.0,
            -20.0,
            -80.0,
        ]])
        broadcast.assert_awaited_once()
        self.assertEqual(broadcast.await_args.args[1]["type"], "update")

    async def test_runtime_id_changes_after_backend_service_restart(self):
        first_runtime_id = self.service.runtime_id
        self.assertTrue(first_runtime_id)

        await self.service.stop()
        await self.service.start()

        self.assertTrue(self.service.runtime_id)
        self.assertNotEqual(self.service.runtime_id, first_runtime_id)

    async def test_rejects_stale_snapshot(self):
        current = datetime(2026, 8, 5, 10, 0, tzinfo=CST)
        stale = make_stock_flow(int(datetime(2026, 8, 4, 15, 0, tzinfo=CST).timestamp()))
        with patch(
            "services.stock_flow_realtime.fetch_stock_flow_snapshot",
            new=AsyncMock(return_value=stale),
        ):
            self.assertFalse(await self.service.collect_once("1.600519", current))

        count = self.service.connection.execute(
            "SELECT COUNT(*) FROM stock_flow_snapshot"
        ).fetchone()[0]
        self.assertEqual(count, 0)

    async def test_backfill_adds_prior_minutes_without_replacing_live_seconds(self):
        current = datetime(2026, 8, 5, 10, 30, 20, tzinfo=CST)
        realtime = {
            **make_stock_flow(
                int(datetime(2026, 8, 5, 10, 0, 5, tzinfo=CST).timestamp())
            ),
            "received_at": "2026-08-05T10:00:05+08:00",
        }
        self.service._persist_snapshot(current.date(), realtime)
        minute_data = {
            "quote_id": "1.600519",
            "code": "600519",
            "name": "贵州茅台",
            "flows": [
                {
                    "time": "2026-08-05 09:30",
                    "main_net": 10,
                    "small_net": -8,
                    "mid_net": -2,
                    "large_net": 6,
                    "super_large_net": 4,
                },
                {
                    "time": "2026-08-05 10:00",
                    "main_net": 90,
                    "small_net": -70,
                    "mid_net": -20,
                    "large_net": 50,
                    "super_large_net": 40,
                },
                {
                    "time": "2026-08-05 10:31",
                    "main_net": 120,
                    "small_net": -90,
                    "mid_net": -30,
                    "large_net": 70,
                    "super_large_net": 50,
                },
            ],
        }
        fetch = AsyncMock(return_value=minute_data)
        with patch(
            "services.stock_flow_realtime.fetch_stock_minute_data",
            new=fetch,
        ):
            self.assertEqual(
                await self.service.backfill_history(
                    "1.600519", "600519", "贵州茅台", current
                ),
                1,
            )
            self.assertEqual(
                await self.service.backfill_history(
                    "1.600519", "600519", "贵州茅台", current
                ),
                0,
            )

        rows = self.service.connection.execute(
            """
            SELECT source_time, granularity
            FROM stock_flow_snapshot
            WHERE trade_date = ? AND quote_id = ?
            ORDER BY source_time
            """,
            (current.date().isoformat(), "1.600519"),
        ).fetchall()
        self.assertEqual(rows, [
            (
                int(datetime(2026, 8, 5, 9, 30, tzinfo=CST).timestamp()),
                "minute_backfill",
            ),
            (realtime["source_time"], "realtime"),
        ])
        fetch.assert_awaited_once_with("1.600519", 240)

    def test_cleanup_removes_only_expired_stock_snapshots(self):
        self.service.retention_days = 30
        old_flow = {
            **make_stock_flow(1),
            "received_at": "2026-06-26T10:00:00+08:00",
        }
        boundary_flow = {
            **make_stock_flow(2),
            "received_at": "2026-06-27T10:00:00+08:00",
        }
        self.service._persist_snapshot(date(2026, 6, 26), old_flow)
        self.service._persist_snapshot(date(2026, 6, 27), boundary_flow)

        self.service.cleanup_old_data(date(2026, 7, 27))

        dates = self.service.connection.execute(
            "SELECT DISTINCT trade_date FROM stock_flow_snapshot ORDER BY trade_date"
        ).fetchall()
        self.assertEqual(dates, [("2026-06-27",)])


if __name__ == "__main__":
    unittest.main()
