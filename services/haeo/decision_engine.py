import time
from typing import Tuple
from services.common.models import EventModel, EventType, DecisionOutcome, PressureLevel, RoutingMeta
from services.haeo.pressure_monitor import PressureMonitor
from services.haeo.edf_scheduler import EDFScheduler
from services.haeo.rule_engine import RuleEngine
from services.common.logger import get_logger

logger = get_logger("DecisionEngine")

TOPIC_MAP = {
    DecisionOutcome.PROCESS: "critical-events",
    DecisionOutcome.BATCH: "batch-events",
    DecisionOutcome.DEFER: "deferred-events",
    DecisionOutcome.SHED: "dlq-events",
    DecisionOutcome.RETRY: "dlq-events"
}


class DecisionEngine:
    def __init__(self, pressure_monitor: PressureMonitor, edf_scheduler: EDFScheduler, rule_engine: RuleEngine):
        self.pressure_monitor = pressure_monitor
        self.edf_scheduler = edf_scheduler
        self.rule_engine = rule_engine

    def evaluate_and_route(self, event: EventModel) -> Tuple[DecisionOutcome, str, RoutingMeta]:
        start_time = time.time()

        # 1. Compute pressure score & level
        pressure_score = self.pressure_monitor.compute_pressure_score()
        pressure_level = self.pressure_monitor.get_pressure_level(pressure_score)

        # 2. Compute effective priority & EDF score
        priority_score = self.edf_scheduler.compute_effective_priority(event)

        # 3. Determine initial decision outcome based on Pressure Matrix & Priority
        proposed_outcome = self._select_outcome(event.eventType, pressure_level, priority_score)

        # 4. Validate decision through Business SLA Rule Engine
        final_outcome = self.rule_engine.validate_decision(event.eventType, proposed_outcome)

        # 5. Map outcome to target Kafka topic
        target_topic = TOPIC_MAP.get(final_outcome, "critical-events")

        delay_ms = round((time.time() - start_time) * 1000.0, 3)

        routing_meta = RoutingMeta(
            effectivePriorityScore=priority_score,
            systemPressureAtRouting=pressure_score,
            decisionOutcome=final_outcome,
            targetTopic=target_topic,
            orchestrationDelayMs=delay_ms
        )

        return final_outcome, target_topic, routing_meta

    def _select_outcome(self, event_type: EventType, pressure_level: PressureLevel, priority_score: float) -> DecisionOutcome:
        # High priority override: Any event with Priority Score >= 120 gets instant PROCESS
        if priority_score >= 120.0 and event_type in [EventType.PAYMENT, EventType.ORDER, EventType.REFUND]:
            return DecisionOutcome.PROCESS

        if pressure_level == PressureLevel.LOW:
            return DecisionOutcome.PROCESS

        elif pressure_level == PressureLevel.MEDIUM:
            if event_type in [EventType.PAYMENT, EventType.ORDER, EventType.REFUND]:
                return DecisionOutcome.PROCESS
            elif event_type in [EventType.INVENTORY, EventType.NOTIFICATION]:
                return DecisionOutcome.BATCH
            else:  # CLICK, LOG
                return DecisionOutcome.DEFER

        elif pressure_level == PressureLevel.HIGH:
            if event_type in [EventType.PAYMENT, EventType.ORDER, EventType.REFUND]:
                return DecisionOutcome.PROCESS
            elif event_type == EventType.INVENTORY:
                return DecisionOutcome.BATCH
            elif event_type == EventType.NOTIFICATION:
                return DecisionOutcome.DEFER
            elif event_type == EventType.CLICK:
                return DecisionOutcome.DEFER
            else:  # LOG
                return DecisionOutcome.SHED

        else:  # PressureLevel.CRITICAL
            if event_type in [EventType.PAYMENT, EventType.ORDER, EventType.REFUND]:
                return DecisionOutcome.PROCESS
            elif event_type in [EventType.INVENTORY, EventType.NOTIFICATION]:
                return DecisionOutcome.DEFER
            else:  # CLICK, LOG
                return DecisionOutcome.SHED
