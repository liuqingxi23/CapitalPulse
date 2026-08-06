"""FastAPI application entry point."""

import logging
import sys
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware

from routers import sector_flow_realtime, stock_flow_realtime
from services.sector_flow_realtime import sector_flow_service
from services.stock_flow_realtime import stock_flow_service
from utils.http_client import close_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: startup and shutdown."""
    logger.info("backend starting up...")
    await sector_flow_service.start()
    await stock_flow_service.start()
    yield
    logger.info("backend shutting down...")
    await stock_flow_service.stop()
    await sector_flow_service.stop()
    await close_client()


# Create FastAPI app
app = FastAPI(
    title="Vane Sector Flow API",
    description="Real-time A-share sector capital-flow dashboard API.",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware - allow all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(sector_flow_realtime.router, prefix="/api", tags=["Real-time Sector Flow"])
app.include_router(stock_flow_realtime.router, prefix="/api", tags=["Real-time Stock Flow"])


# Health check endpoint
@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {"code": 200, "msg": "ok", "data": {"status": "healthy"}}


@app.websocket("/ws/sector-flow")
async def ws_sector_flow(websocket: WebSocket):
    """WebSocket endpoint for persistent sector-flow snapshots."""
    await sector_flow_service.websocket_handler(websocket)


@app.websocket("/ws/stock-flow")
async def stock_flow_ws(
    websocket: WebSocket,
    quote_id: str,
    code: str = "",
    name: str = "",
):
    """Subscribe to one stock's persistent second-level fund-flow snapshots."""
    await stock_flow_service.websocket_handler(websocket, quote_id, code, name)


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        log_level="info",
        reload=False,
    )
