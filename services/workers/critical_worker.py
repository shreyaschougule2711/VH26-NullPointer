import asyncio
import time
import uuid
from services.common.models import MetricTelemetryModel
from services.common.kafka_producer import AEOKafkaProducer
from services.common.kafka_consumer import AEOKafkaConsumer
from services.common.logger import get_logger

logger = get_logger("CriticalWorker")


class CriticalWorker:
    def __init__(self, producer: AEOKafkaProducer):
        self.producer = producer
        self.processed_count = 0
        self.total_latency_ms = 0.0
        self.consumer = AEOKafkaConsumer(
            topics=["critical-events"],
            group_id="worker-critical-group",
            auto_offset_reset="latest"
        )

    async def start(self):
        await self.consumer.start(self.process_event)
        logger.info("Critical Worker active on 'critical-events'.")

    async def process_event(self, topic: str, payload: dict):
        start_time = time.time()
        
        # Simulate processing logic (5-15ms execution)
        await asyncio.sleep(0.01)

        latency_ms = (time.time() - start_time) * 1000.0
        self.processed_count += 1
        self.total_latency_ms += latency_ms

        avg_lat = round(self.total_latency_ms / max(1, self.processed_count), 2)

        telemetry = MetricTelemetryModel(
            metricId=f"m_crit_{uuid.uuid4().hex[:6]}",
            sourceService="worker-critical",
            metricType="WORKER_EXECUTION",
            data={
                "workerType": "CRITICAL",
                "processedCount": 1,
                "latencyMs": round(latency_ms, 2),
                "avgLatencyMs": avg_lat,
                "workerUtilization": 0.85,
                "currentQueueOccupancy": 15
            }
        )
        await self.producer.send(topic="metrics-events", value=telemetry.model_dump())

    async def stop(self):
        await self.consumer.stop()
