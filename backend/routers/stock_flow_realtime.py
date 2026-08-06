"""REST endpoints for stock search and persisted second-level fund flows."""

from datetime import date, datetime

from fastapi import APIRouter, Query

from services.sector_flow_realtime import CST
from services.stock_flow_realtime import stock_flow_service
from services.stock_flow_upstream import QUOTE_ID_PATTERN, search_stocks

router = APIRouter()
DEFAULT_STOCK = {
    "quote_id": "0.000001",
    "code": "000001",
    "name": "平安银行",
    "market_name": "深A",
    "pinyin": "PAYH",
}


@router.get("/stock-flow/session")
async def get_stock_flow_session():
    if not stock_flow_service.ready:
        return {"code": 503, "msg": "stock-flow collector is not ready", "data": None}
    return {
        "code": 200,
        "msg": "success",
        "data": {
            "runtime_id": stock_flow_service.runtime_id,
            "default_stock": DEFAULT_STOCK,
        },
    }


@router.get("/stock-flow/search")
async def search_stock_flow_symbols(
    q: str = Query(..., min_length=1, max_length=30, description="Stock code or name"),
):
    keyword = q.strip()
    if not keyword:
        return {"code": 400, "msg": "search keyword is required", "data": []}
    results = await search_stocks(keyword)
    if results is None:
        return {"code": 502, "msg": "stock search upstream is unavailable", "data": []}
    return {"code": 200, "msg": "success", "data": results}


@router.get("/stock-flow/history")
async def get_stock_flow_history(
    quote_id: str = Query(..., description="EastMoney quote id, for example 1.600519"),
    trade_date: date | None = Query(None, description="CST trade date"),
):
    if not QUOTE_ID_PATTERN.fullmatch(quote_id):
        return {"code": 400, "msg": "invalid quote_id", "data": None}
    if not stock_flow_service.ready:
        return {"code": 503, "msg": "stock-flow collector is not ready", "data": None}
    target = trade_date or datetime.now(CST).date()
    return {
        "code": 200,
        "msg": "success",
        "data": stock_flow_service.history_data(target, quote_id),
    }
