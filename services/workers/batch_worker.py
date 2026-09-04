import asyncio
import time
import uuid
from typing import List, Dict, Any
from services.common.models import MetricTelemetryModel
from services.common.kafka_producer import AEOKafkaProducer
from services.common.kafka_consumer import AEOKafkaConsumer
from services.common.logger import get_logger

logger = get_logger("BatchWorker")


class BatchWorker:
    def __init__(self, producer: AEOKafkaProducer, batch_size: int = 15, max_window_ms: float = 200.0):
        self.producer = producer
        self.batch_size = batch_size
        self.max_window_ms = max_window_ms
        self.buffer: List[Dict[str, Any]] = []
        self.processed_count = 0
        self.last_flush_time = time.time()
        self.consumer = AEOKafkaConsumer(
            topics=["batch-events"],
            group_id="worker-batch-group",
            auto_offset_reset="latest"
        )

    async def start(self):
        await self.consumer.start(self.on_message)
        asyncio.create_task(self._flush_loop())
        logger.info("Batch Worker active on 'batch-events'.")

    async def on_message(self, topic: str, payload: dict):
        self.buffer.append(payload)
        if len(self.buffer) >= self.batch_size:
            await self._flush_batch()

    async def _flush_loop(self):
        while self.consumer.is_running:
            await asyncio.sleep(self.max_window_ms / 1000.0)
            if self.buffer:
                await self._flush_batch()

    async def _flush_batch(self):
        if not self.buffer:
            return

        batch_to_process = self.buffer[:]
        self.buffer.clear()
        self.last_flush_time = time.time()

        start_time = time.time()
        # Simulate micro-batch vector processing
        await asyncio.sleep(0.02)
        latency_ms = (time.time() - start_time) * 1000.0

        count = len(batch_to_process)
        self.processed_count += count

        telemetry = MetricTelemetryModel(
            metricId=f"m_batch_{uuid.uuid4().hex[:6]}",
            sourceService="worker-batch",
            metricType="WORKER_EXECUTION",
            data={
                "workerType": "BATCH",
                "processedCount": count,
                "batchSize": count,
                "latencyMs": round(latency_ms, 2),
                "avgLatencyMs": round(latency_ms / count, 2),
                "workerUtilization": 0.60,
                "currentQueueOccupancy": 40
            }
        )
        await self.producer.send(topic="metrics-events", value=telemetry.model_dump())

    async def stop(self):
        await self._flush_batch()
        await self.consumer.stop()
