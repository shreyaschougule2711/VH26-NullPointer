import asyncio
import time
import uuid
from services.common.models import MetricTelemetryModel
from services.common.kafka_producer import AEOKafkaProducer
from services.common.kafka_consumer import AEOKafkaConsumer
from services.common.logger import get_logger

logger = get_logger("DLQWorker")


class DLQWorker:
    def __init__(self, producer: AEOKafkaProducer):
        self.producer = producer
        self.shed_count = 0
        self.consumer = AEOKafkaConsumer(
            topics=["dlq-events"],
            group_id="worker-dlq-group",
            auto_offset_reset="latest"
        )

    async def start(self):
        await self.consumer.start(self.process_event)
        logger.info("DLQ Worker active on 'dlq-events'.")

    async def process_event(self, topic: str, payload: dict):
        self.shed_count += 1
        event_id = payload.get("id", "unknown")
        event_type = payload.get("eventType", "unknown")
        logger.info(f"Audit log: Shed event {event_id} ({event_type}) stored in DLQ.")

        telemetry = MetricTelemetryModel(
            metricId=f"m_dlq_{uuid.uuid4().hex[:6]}",
            sourceService="worker-dlq",
            metricType="WORKER_EXECUTION",
            data={
                "workerType": "DLQ",
                "processedCount": 1,
                "droppedEvents": 1,
                "workerUtilization": 0.05,
                "currentQueueOccupancy": 5
            }
        )
        await self.producer.send(topic="metrics-events", value=telemetry.model_dump())

    async def stop(self):
        await self.consumer.stop()
