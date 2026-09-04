import asyncio
import json
import os
from typing import List, Callable, Awaitable, Optional, Dict, Any
from services.common.logger import get_logger
from services.common.kafka_producer import get_mock_queue

logger = get_logger("KafkaConsumer")


class AEOKafkaConsumer:
    def __init__(
        self,
        topics: List[str],
        group_id: str,
        bootstrap_servers: Optional[str] = None,
        auto_offset_reset: str = "latest"
    ):
        self.topics = topics
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.auto_offset_reset = auto_offset_reset
        self.consumer = None
        self.use_mock = False
        self.is_running = False

    async def start(self, message_handler: Callable[[str, Dict[str, Any]], Awaitable[None]]):
        self.is_running = True
        try:
            from aiokafka import AIOKafkaConsumer
            self.consumer = AIOKafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset=self.auto_offset_reset,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                session_timeout_ms=10000,
                heartbeat_interval_ms=3000
            )
            await self.consumer.start()
            logger.info(f"AEOKafkaConsumer group '{self.group_id}' subscribed to topics {self.topics}")
            
            # Start consumption task
            asyncio.create_task(self._consume_loop(message_handler))
        except Exception as e:
            logger.warning(f"Could not connect real Kafka consumer group '{self.group_id}' ({e}). Falling back to mock bus.")
            self.use_mock = True
            asyncio.create_task(self._consume_mock_loop(message_handler))

    async def _consume_loop(self, message_handler: Callable[[str, Dict[str, Any]], Awaitable[None]]):
        try:
            async for msg in self.consumer:
                if not self.is_running:
                    break
                try:
                    await message_handler(msg.topic, msg.value)
                except Exception as ex:
                    logger.error(f"Error in consumer message handler for topic {msg.topic}: {ex}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Kafka consume loop error: {e}")

    async def _consume_mock_loop(self, message_handler: Callable[[str, Dict[str, Any]], Awaitable[None]]):
        while self.is_running:
            for topic in self.topics:
                queue = get_mock_queue(topic)
                while not queue.empty():
                    item = await queue.get()
                    try:
                        await message_handler(topic, item["value"])
                    except Exception as ex:
                        logger.error(f"Error handling mock message on topic {topic}: {ex}")
                    queue.task_done()
            await asyncio.sleep(0.01)

    async def stop(self):
        self.is_running = False
        if self.consumer and not self.use_mock:
            try:
                await self.consumer.stop()
                logger.info(f"Consumer group '{self.group_id}' stopped.")
            except Exception as e:
                logger.error(f"Error stopping consumer: {e}")
