"""Configuration for the real-time sector capital-flow service."""

HOST = "0.0.0.0"
PORT = 8000

REQUEST_TIMEOUT = 10.0
MAX_RETRIES = 2
RETRY_DELAY = 500

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Referer": "https://data.eastmoney.com/",
}

EASTMONEY_SECTOR_URL = "https://push2delay.eastmoney.com/api/qt/clist/get"
EASTMONEY_PUSH_URL = "https://push2.eastmoney.com/api/qt/clist/get"
EASTMONEY_SECTOR_CAPITAL_FLOW_URL = (
    "https://push2.eastmoney.com/api/qt/stock/fflow/kline/get"
)
EASTMONEY_SECTOR_CAPITAL_FLOW_FALLBACK_URL = (
    "https://push2delay.eastmoney.com/api/qt/stock/fflow/kline/get"
)
EASTMONEY_SECTOR_DAILY_FLOW_URL = (
    "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
)
EASTMONEY_SECTOR_FLOW_SNAPSHOT_URL = (
    "https://push2.eastmoney.com/api/qt/ulist.np/get"
)
EASTMONEY_SECTOR_FLOW_SNAPSHOT_FALLBACK_URL = (
    "https://push2delay.eastmoney.com/api/qt/ulist.np/get"
)
