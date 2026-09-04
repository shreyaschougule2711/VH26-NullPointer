import asyncio
import time
import uuid
from services.common.models import MetricTelemetryModel
from services.common.kafka_producer import AEOKafkaProducer
from services.common.kafka_consumer import AEOKafkaConsumer
from services.common.logger import get_logger

logger = get_logger("DeferredWorker")


class DeferredWorker:
    def __init__(self, producer: AEOKafkaProducer):
        self.producer = producer
        self.processed_count = 0
        self.consumer = AEOKafkaConsumer(
            topics=["deferred-events"],
            group_id="worker-deferred-group",
            auto_offset_reset="latest"
        )

    async def start(self):
        await self.consumer.start(self.process_event)
        logger.info("Deferred Worker active on 'deferred-events'.")

    async def process_event(self, topic: str, payload: dict):
        start_time = time.time()
        # Rate-limited processing sleep
        await asyncio.sleep(0.03)

        latency_ms = (time.time() - start_time) * 1000.0
        self.processed_count += 1

        telemetry = MetricTelemetryModel(
            metricId=f"m_def_{uuid.uuid4().hex[:6]}",
            sourceService="worker-deferred",
            metricType="WORKER_EXECUTION",
            data={
                "workerType": "DEFERRED",
                "processedCount": 1,
                "latencyMs": round(latency_ms, 2),
                "avgLatencyMs": round(latency_ms, 2),
                "workerUtilization": 0.35,
                "currentQueueOccupancy": 80
            }
        )
        await self.producer.send(topic="metrics-events", value=telemetry.model_dump())

    async def stop(self):
        await self.consumer.stop()
