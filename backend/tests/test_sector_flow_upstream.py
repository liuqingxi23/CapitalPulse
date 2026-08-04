import json
import unittest
from unittest.mock import AsyncMock, patch

from services.sector_flow_upstream import (
    fetch_sector_daily_data,
    fetch_sector_minute_data,
    parse_daily_flows,
    parse_minute_flows,
)


class SectorFlowUpstreamTests(unittest.IsolatedAsyncioTestCase):
    def test_parse_minute_flows(self):
        result = parse_minute_flows([
            "2026-07-20 11:24,1180348891,-339167661,-841320868,765447547,414901344",
            None,
            "invalid",
        ])
        self.assertEqual(result, [{
            "time": "2026-07-20 11:24",
            "main_net": 1180348891.0,
            "small_net": -339167661.0,
            "mid_net": -841320868.0,
            "large_net": 765447547.0,
            "super_large_net": 414901344.0,
        }])

    async def test_fetches_minute_history_with_market_90_secid(self):
        payload = json.dumps({
            "data": {
                "name": "测试行业",
                "klines": ["2026-07-20 11:25,1,2,3,4,5"],
            }
        })
        fetch = AsyncMock(return_value=payload)
        with patch("services.sector_flow_upstream.safe_fetch", fetch):
            result = await fetch_sector_minute_data("BK0477", 20)

        self.assertEqual(result["count"], 1)
        self.assertEqual(fetch.await_args.kwargs["params"]["secid"], "90.BK0477")
        self.assertEqual(fetch.await_args.kwargs["params"]["klt"], "1")

    def test_parse_daily_flows(self):
        result = parse_daily_flows([
            "2026-08-01,100,20,30,40,50",
            "invalid",
        ])
        self.assertEqual(result, [{
            "trade_date": "2026-08-01",
            "main_net": 100.0,
            "small_net": 20.0,
            "mid_net": 30.0,
            "large_net": 40.0,
            "super_large_net": 50.0,
        }])

    async def test_fetches_daily_history_from_dedicated_daykline_endpoint(self):
        payload = json.dumps({
            "data": {
                "name": "测试行业",
                "klines": ["2026-08-01,100,20,30,40,50"],
            }
        })
        fetch = AsyncMock(return_value=payload)
        with patch("services.sector_flow_upstream.safe_fetch", fetch):
            result = await fetch_sector_daily_data("BK1036", 30)

        self.assertEqual(result["interval"], "1d")
        self.assertEqual(result["value_type"], "daily_net")
        self.assertEqual(result["count"], 1)
        self.assertIn("daykline/get", fetch.await_args.args[0])
        self.assertEqual(fetch.await_args.kwargs["params"]["klt"], "101")
        self.assertEqual(fetch.await_args.kwargs["params"]["lmt"], "30")

    async def test_minute_history_uses_fallback(self):
        payload = json.dumps({
            "data": {"name": "测试行业", "klines": ["2026-07-21 15:00,1,2,3,4,5"]}
        })
        fetch = AsyncMock(side_effect=[None, payload])
        with patch("services.sector_flow_upstream.safe_fetch", fetch):
            result = await fetch_sector_minute_data("BK1036")

        self.assertEqual(result["count"], 1)
        self.assertEqual(fetch.await_count, 2)
        self.assertIn("push2.eastmoney.com", fetch.await_args_list[0].args[0])
        self.assertIn("push2delay.eastmoney.com", fetch.await_args_list[1].args[0])


if __name__ == "__main__":
    unittest.main()
