import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from services.metrics_service.aggregator import MetricsAggregator
from services.metrics_service.ws_server import manager, start_broadcasting_loop
from services.common.logger import get_logger

logger = get_logger("MetricsServiceMain")

aggregator = MetricsAggregator()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await aggregator.start()
    asyncio.create_task(start_broadcasting_loop(aggregator))
    logger.info("Metrics Telemetry Service & Unified Event Bus Relay running.")
    yield
    await aggregator.stop()
    logger.info("Metrics Service stopped.")


app = FastAPI(title="AEOP Metrics Telemetry Service", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    return {
        "status": "UP",
        "service": "metrics-service",
        "activeWsClients": len(manager.active_connections)
    }


@app.get("/api/metrics/current")
async def get_current_metrics():
    return aggregator.snapshot()


@app.post("/api/bus/publish")
async def publish_event(request: Request):
    payload = await request.json()
    topic = payload.get("topic")
    value = payload.get("value")

    if topic == "raw-events" and value:
        aggregator.ingest_raw_event(value)
    elif topic == "metrics-events" and value:
        await aggregator.process_metric("metrics-events", value)

    return {"status": "ACK", "topic": topic}


@app.websocket("/ws/metrics")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
