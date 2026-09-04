import asyncio
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from services.common.models import EventModel, MetricTelemetryModel
from services.common.kafka_producer import AEOKafkaProducer
from services.common.kafka_consumer import AEOKafkaConsumer
from services.haeo.pressure_monitor import PressureMonitor
from services.haeo.edf_scheduler import EDFScheduler
from services.haeo.rule_engine import RuleEngine
from services.haeo.decision_engine import DecisionEngine
from services.common.logger import get_logger

logger = get_logger("HAEOOrchestratorService")

pressure_monitor = PressureMonitor()
edf_scheduler = EDFScheduler()
rule_engine = RuleEngine()
decision_engine = DecisionEngine(pressure_monitor, edf_scheduler, rule_engine)

producer = AEOKafkaProducer()
raw_consumer = AEOKafkaConsumer(
    topics=["raw-events"],
    group_id=os.getenv("HAEO_CONSUMER_GROUP", "haeo-orchestrator-group"),
    auto_offset_reset="latest"
)
telemetry_consumer = AEOKafkaConsumer(
    topics=["metrics-events"],
    group_id="haeo-telemetry-feedback-group",
    auto_offset_reset="latest"
)


async def handle_raw_event(topic: str, payload: dict):
    try:
        event = EventModel(**payload)
        outcome, target_topic, routing_meta = decision_engine.evaluate_and_route(event)

        event.routingMeta = routing_meta

        # Publish routed event to target topic
        await producer.send(
            topic=target_topic,
            value=event.model_dump(),
            key=event.id
        )

        # Emit decision telemetry
        telemetry = MetricTelemetryModel(
            metricId=f"dec_{event.id}",
            sourceService="haeo-orchestrator",
            metricType="DECISION_TELEMETRY",
            data={
                "eventId": event.id,
                "eventType": event.eventType.value,
                "decisionOutcome": outcome.value,
                "targetTopic": target_topic,
                "effectivePriorityScore": routing_meta.effectivePriorityScore,
                "pressureScore": routing_meta.systemPressureAtRouting,
                "delayMs": routing_meta.orchestrationDelayMs
            }
        )
        await producer.send(
            topic="metrics-events",
            value=telemetry.model_dump(),
            key=telemetry.metricId
        )

    except Exception as e:
        logger.error(f"Error orchestrating raw event: {e}")


async def handle_telemetry_event(topic: str, payload: dict):
    try:
        if payload.get("metricType") == "WORKER_EXECUTION":
            data = payload.get("data", {})
            q_len = data.get("currentQueueOccupancy", 0)
            worker_util = data.get("workerUtilization", 0.1)
            avg_lat = data.get("avgLatencyMs", 10.0)
            pressure_monitor.update_telemetry(q_len, worker_util, avg_lat)
    except Exception as e:
        logger.error(f"Error parsing telemetry feedback: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await producer.start()
    await raw_consumer.start(handle_raw_event)
    await telemetry_consumer.start(handle_telemetry_event)
    logger.info("Hybrid Adaptive Event Orchestrator (HAEO) initialized & listening.")
    yield
    await raw_consumer.stop()
    await telemetry_consumer.stop()
    await producer.stop()
    logger.info("HAEO Service shutdown complete.")


app = FastAPI(title="AEOP Hybrid Adaptive Event Orchestrator", lifespan=lifespan)


@app.get("/health")
async def health():
    score = pressure_monitor.compute_pressure_score()
    level = pressure_monitor.get_pressure_level(score)
    return {
        "status": "UP",
        "service": "haeo-orchestrator",
        "currentPressureScore": score,
        "currentPressureLevel": level.value
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
