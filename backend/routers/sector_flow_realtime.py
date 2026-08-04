"""REST endpoints for the persistent real-time sector-flow collector."""

from datetime import date, datetime

from fastapi import APIRouter, Query

from services.sector_flow_realtime import CST, sector_flow_service

router = APIRouter()


@router.get("/sector-flow/history")
async def get_sector_flow_history(
    trade_date: date | None = Query(None, description="CST trade date"),
    top: int = Query(10, description="Fixed market-cap ranks to return"),
):
    if top not in (10, 30):
        return {"code": 400, "msg": "top must be 10 or 30", "data": None}
    if not sector_flow_service.enabled:
        return {"code": 503, "msg": "sector-flow collector is disabled", "data": None}
    if not sector_flow_service.ready:
        return {"code": 503, "msg": "sector-flow collector is not ready", "data": None}
    target = trade_date or datetime.now(CST).date()
    return {
        "code": 200,
        "msg": "success",
        "data": sector_flow_service.history_data(target, top),
    }


@router.get("/sector-flow/detail-history")
async def get_sector_flow_detail_history(
    trade_date: date | None = Query(None, description="CST trade date"),
    page: int = Query(1, ge=1, description="Six-sector detail page"),
):
    if not sector_flow_service.enabled:
        return {"code": 503, "msg": "sector-flow collector is disabled", "data": None}
    if not sector_flow_service.ready:
        return {"code": 503, "msg": "sector-flow collector is not ready", "data": None}
    target = trade_date or datetime.now(CST).date()
    return {
        "code": 200,
        "msg": "success",
        "data": sector_flow_service.detail_history_data(target, page),
    }


@router.get("/sector-flow/daily-history")
async def get_sector_flow_daily_history(
    trade_date: date | None = Query(None, description="CST trade date"),
    top: int = Query(30, description="Fixed market-cap ranks to return"),
    days: int = Query(30, description="Daily trading records to return"),
    page: int = Query(1, ge=1, description="Six-sector daily history page"),
):
    if top != 30 or days != 30:
        return {"code": 400, "msg": "top and days must both be 30", "data": None}
    if not sector_flow_service.enabled:
        return {"code": 503, "msg": "sector-flow collector is disabled", "data": None}
    if not sector_flow_service.ready:
        return {"code": 503, "msg": "sector-flow collector is not ready", "data": None}
    target = trade_date or datetime.now(CST).date()
    data = await sector_flow_service.daily_history_data(
        target,
        top,
        days,
        page=page,
    )
    if not data["series"]:
        return {"code": 503, "msg": "daily sector selection is not ready", "data": data}
    if len(data["failed_codes"]) == len(data["series"]):
        return {"code": 502, "msg": "daily fund-flow upstream is unavailable", "data": data}
    return {"code": 200, "msg": "success", "data": data}


@router.get("/sector-flow/status")
async def get_sector_flow_status():
    return {
        "code": 200,
        "msg": "success",
        "data": {
            "enabled": sector_flow_service.enabled,
            **sector_flow_service.status_data(),
        },
    }
