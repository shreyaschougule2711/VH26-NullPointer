import asyncio
import uuid
import random
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from services.common.models import EventModel, EventType, PriorityLevel, SimulationConfigModel
from services.common.kafka_producer import AEOKafkaProducer
from services.common.logger import get_logger

logger = get_logger("TrafficGenerator")

EVENT_SPECS = {
    EventType.PAYMENT: {"priority": PriorityLevel.CRITICAL, "deadline": 100},
    EventType.ORDER: {"priority": PriorityLevel.HIGH, "deadline": 200},
    EventType.REFUND: {"priority": PriorityLevel.HIGH, "deadline": 150},
    EventType.INVENTORY: {"priority": PriorityLevel.MEDIUM, "deadline": 2000},
    EventType.NOTIFICATION: {"priority": PriorityLevel.LOW, "deadline": 5000},
    EventType.CLICK: {"priority": PriorityLevel.TRIVIAL, "deadline": 10000},
    EventType.LOG: {"priority": PriorityLevel.TRIVIAL, "deadline": 30000},
}


class TrafficGenerator:
    def __init__(self, producer: AEOKafkaProducer):
        self.producer = producer
        self.is_running = False
        self.config: Optional[SimulationConfigModel] = None
        self.start_time: float = 0.0
        self.total_generated: int = 0
        self.current_eps: float = 0.0
        self._task: Optional[asyncio.Task] = None

    async def start(self, config: SimulationConfigModel):
        if self.is_running:
            await self.stop()

        self.config = config
        self.is_running = True
        self.start_time = time.time()
        self.total_generated = 0
        self.current_eps = float(config.baseEps)
        
        logger.info(f"Starting traffic simulation scenario '{config.scenario}' with base EPS {config.baseEps}")
        self._task = asyncio.create_task(self._generator_loop())

    async def stop(self):
        self.is_running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info(f"Traffic simulation stopped. Total generated: {self.total_generated}")

    def _calculate_current_eps(self) -> float:
        if not self.config:
            return 0.0

        elapsed = time.time() - self.start_time
        base = self.config.baseEps
        multiplier = self.config.spikeMultiplier
        peak_target = base * multiplier

        ramp_up = self.config.rampUpSeconds
        peak_dur = self.config.peakDurationSeconds
        ramp_down = self.config.rampDownSeconds

        if ramp_up > 0 and elapsed < ramp_up:
            # Linear ramp up
            progress = elapsed / ramp_up
            return base + (peak_target - base) * progress
        elif elapsed < (ramp_up + peak_dur):
            # Peak duration
            return peak_target
        elif ramp_down > 0 and elapsed < (ramp_up + peak_dur + ramp_down):
            # Linear ramp down
            down_elapsed = elapsed - (ramp_up + peak_dur)
            progress = down_elapsed / ramp_down
            return peak_target - (peak_target - base) * progress
        else:
            # Post scenario baseline
            return float(base)

    def _pick_event_type(self) -> EventType:
        dist = self.config.eventDistribution if self.config else {}
        if not dist:
            return random.choice(list(EventType))
        
        types = list(dist.keys())
        weights = list(dist.values())
        chosen_str = random.choices(types, weights=weights, k=1)[0]
        return EventType(chosen_str)

    def _build_event(self, event_type: EventType) -> EventModel:
        spec = EVENT_SPECS.get(event_type, {"priority": PriorityLevel.MEDIUM, "deadline": 1000})
        
        payload = {}
        if event_type == EventType.PAYMENT:
            payload = {"userId": f"usr_{random.randint(1000, 9999)}", "amount": round(random.uniform(10.0, 1000.0), 2), "currency": "INR"}
        elif event_type == EventType.ORDER:
            payload = {"orderId": f"ord_{uuid.uuid4().hex[:8]}", "itemCount": random.randint(1, 5)}
        elif event_type == EventType.REFUND:
            payload = {"refundId": f"ref_{uuid.uuid4().hex[:8]}", "reason": "Customer cancellation"}
        elif event_type == EventType.INVENTORY:
            payload = {"sku": f"SKU-{random.randint(100, 999)}", "stockDelta": -1}
        elif event_type == EventType.NOTIFICATION:
            payload = {"channel": random.choice(["SMS", "PUSH", "EMAIL"]), "recipient": f"user_{random.randint(100,999)}"}
        elif event_type == EventType.CLICK:
            payload = {"page": "/product/detail", "x": random.randint(0, 1920), "y": random.randint(0, 1080)}
        else:
            payload = {"level": "INFO", "msg": "Synthetic log entry"}

        return EventModel(
            id=str(uuid.uuid4()),
            eventType=event_type,
            priority=spec["priority"],
            arrivalTime=datetime.now(timezone.utc).isoformat(),
            deadline=spec["deadline"],
            retryCount=0,
            payload=payload
        )

    async def _generator_loop(self):
        try:
            while self.is_running:
                target_eps = self._calculate_current_eps()
                self.current_eps = target_eps

                if target_eps <= 0:
                    await asyncio.sleep(0.1)
                    continue

                interval = 1.0 / target_eps
                batch_size = max(1, int(target_eps / 50))  # Produce in micro-batches if EPS is high

                for _ in range(batch_size):
                    event_type = self._pick_event_type()
                    event = self._build_event(event_type)
                    await self.producer.send(
                        topic="raw-events",
                        value=event.model_dump(),
                        key=event.id
                    )
                    self.total_generated += 1

                await asyncio.sleep(interval * batch_size)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in traffic generator loop: {e}")
