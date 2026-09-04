import asyncio
import json
import os
import requests
from typing import Dict, Any, Optional
from services.common.logger import get_logger

logger = get_logger("KafkaProducer")

MOCK_EVENT_BUS: Dict[str, asyncio.Queue] = {}


def get_mock_queue(topic: str) -> asyncio.Queue:
    if topic not in MOCK_EVENT_BUS:
        MOCK_EVENT_BUS[topic] = asyncio.Queue()
    return MOCK_EVENT_BUS[topic]


class AEOKafkaProducer:
    def __init__(self, bootstrap_servers: Optional[str] = None):
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.producer = None
        self.use_mock = False
        self.bus_url = "http://localhost:8000/api/bus/publish"

    async def start(self):
        try:
            from aiokafka import AIOKafkaProducer
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                request_timeout_ms=1000
            )
            await self.producer.start()
            logger.info(f"AEOKafkaProducer connected to Kafka broker at {self.bootstrap_servers}")
        except Exception as e:
            logger.warning(f"Could not connect to real Kafka broker ({e}). Falling back to HTTP Bus Relay / in-memory bus.")
            self.use_mock = True

    async def send(self, topic: str, value: Dict[str, Any], key: Optional[str] = None):
        if self.use_mock or self.producer is None:
            # 1. Local queue for same-process subscribers
            queue = get_mock_queue(topic)
            await queue.put({"key": key, "value": value})

            # 2. HTTP Bus Relay to Metrics Service if running out-of-process
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    lambda: requests.post(
                        self.bus_url,
                        json={"topic": topic, "key": key, "value": value},
                        timeout=0.2
                    )
                )
            except Exception:
                pass
        else:
            try:
                await self.producer.send_and_wait(topic, value=value, key=key)
            except Exception as e:
                logger.error(f"Error publishing message to Kafka topic {topic}: {e}")
                queue = get_mock_queue(topic)
                await queue.put({"key": key, "value": value})

    async def stop(self):
        if self.producer and not self.use_mock:
            try:
                await self.producer.stop()
                logger.info("AEOKafkaProducer stopped cleanly.")
            except Exception as e:
                logger.error(f"Error stopping producer: {e}")
