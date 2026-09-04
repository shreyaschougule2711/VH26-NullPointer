import asyncio
from typing import Set
from fastapi import WebSocket, WebSocketDisconnect
from datetime import datetime, timezone
from services.metrics_service.aggregator import MetricsAggregator
from services.common.logger import get_logger

logger = get_logger("WebSocketServer")


class ConnectionManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(f"WebSocket client connected. Total clients: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"WebSocket client disconnected. Remaining clients: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        if not self.active_connections:
            return
        to_remove = set()
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                to_remove.add(connection)
        for dead in to_remove:
            self.disconnect(dead)


manager = ConnectionManager()


async def start_broadcasting_loop(aggregator: MetricsAggregator):
    while True:
        try:
            metrics_snapshot = aggregator.snapshot()
            payload = {
                "event": "METRICS_UPDATE",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "metrics": metrics_snapshot
            }
            await manager.broadcast(payload)
        except Exception as e:
            logger.error(f"Error broadcasting WebSocket metrics: {e}")
        await asyncio.sleep(0.5)
