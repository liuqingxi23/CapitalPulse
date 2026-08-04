import json
import unittest
from unittest.mock import AsyncMock, patch

from services.sector_flow_upstream import _fetch_industry_sectors_from


def make_item(code: str, name: str) -> dict:
    return {
        "f3": 1.0,
        "f12": code,
        "f14": name,
        "f20": 1000,
        "f104": 1,
        "f105": 2,
        "f106": 3,
        "f107": 4,
        "f128": "lead",
        "f140": "000001",
        "f136": 2.0,
    }


class SectorFlowPaginationTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetches_every_upstream_page(self):
        first_page = [make_item(f"BK{i:04d}", f"行业{i}") for i in range(100)]
        second_page = [make_item("BK0100", "行业100")]
        fetch = AsyncMock(side_effect=[
            json.dumps({"data": {"total": 101, "diff": first_page}}),
            json.dumps({"data": {"total": 101, "diff": second_page}}),
        ])

        with patch("services.sector_flow_upstream.safe_fetch", fetch):
            sectors = await _fetch_industry_sectors_from("https://example.test")

        self.assertEqual(len(sectors or []), 101)
        self.assertEqual(fetch.await_count, 2)
        self.assertEqual(fetch.await_args_list[0].kwargs["params"]["pn"], "1")
        self.assertEqual(fetch.await_args_list[1].kwargs["params"]["pn"], "2")


if __name__ == "__main__":
    unittest.main()
