import time
import random
from typing import Dict, Any, List
from services.common.kafka_consumer import AEOKafkaConsumer
from services.common.models import EventModel, DecisionOutcome, EventType
from services.haeo.pressure_monitor import PressureMonitor
from services.haeo.edf_scheduler import EDFScheduler
from services.haeo.rule_engine import RuleEngine
from services.haeo.decision_engine import DecisionEngine
from services.common.logger import get_logger

logger = get_logger("MetricsAggregator")


class MetricsAggregator:
    def __init__(self):
        self.decisions_counter = {"PROCESS": 0, "BATCH": 0, "DEFER": 0, "SHED": 0, "RETRY": 0}
        self.total_processed_events = 0
        self.total_dropped_events = 0
        self.latencies_window = [12.5, 14.2, 18.0, 11.4, 15.6]
        self.latest_pressure_score = 0.15
        self.latest_pressure_level = "LOW"
        self.worker_utilization = {
            "criticalWorker": 0.35,
            "batchWorker": 0.25,
            "deferredWorker": 0.15,
            "dlqWorker": 0.05
        }
        self.queue_occupancy = {
            "raw": 0,
            "critical": 0,
            "batch": 0,
            "deferred": 0,
            "dlq": 0
        }
        self.recent_decisions: List[Dict[str, Any]] = []
        self.last_reset_time = time.time()

        # Embedded HAEO Engine for unified out-of-process simulation support
        self.pressure_monitor = PressureMonitor()
        self.edf_scheduler = EDFScheduler()
        self.rule_engine = RuleEngine()
        self.decision_engine = DecisionEngine(self.pressure_monitor, self.edf_scheduler, self.rule_engine)

        self.consumer = AEOKafkaConsumer(
            topics=["metrics-events"],
            group_id="metrics-aggregator-group",
            auto_offset_reset="latest"
        )

    async def start(self):
        await self.consumer.start(self.process_metric)
        logger.info("Metrics Aggregator started listening to 'metrics-events'.")

    def ingest_raw_event(self, raw_payload: dict):
        try:
            event = EventModel(**raw_payload)

            # 1. Update queue depth based on ingest rate
            self.queue_occupancy["raw"] = min(10000, self.queue_occupancy["raw"] + 1)
            self.pressure_monitor.update_telemetry(
                queue_len=self.queue_occupancy["raw"],
                worker_util=self.worker_utilization["criticalWorker"],
                avg_lat_ms=self.latencies_window[-1] if self.latencies_window else 15.0
            )

            # 2. Evaluate HAEO decision
            outcome, target_topic, routing_meta = self.decision_engine.evaluate_and_route(event)

            # 3. Update internal counters & telemetry state
            self.decisions_counter[outcome.value] = self.decisions_counter.get(outcome.value, 0) + 1
            self.total_processed_events += 1

            if outcome == DecisionOutcome.SHED:
                self.total_dropped_events += 1
                self.queue_occupancy["dlq"] += 1
            elif outcome == DecisionOutcome.PROCESS:
                self.queue_occupancy["critical"] = max(0, self.queue_occupancy["critical"] + 1)
            elif outcome == DecisionOutcome.BATCH:
                self.queue_occupancy["batch"] = max(0, self.queue_occupancy["batch"] + 1)
            elif outcome == DecisionOutcome.DEFER:
                self.queue_occupancy["deferred"] = max(0, self.queue_occupancy["deferred"] + 1)

            # 4. Simulate worker execution latency
            sim_lat = round(random.uniform(5.0, 35.0), 2)
            self.latencies_window.append(sim_lat)
            if len(self.latencies_window) > 100:
                self.latencies_window.pop(0)

            # 5. Pressure update
            self.latest_pressure_score = routing_meta.systemPressureAtRouting
            if self.latest_pressure_score < 0.35:
                self.latest_pressure_level = "LOW"
                self.worker_utilization = {"criticalWorker": 0.45, "batchWorker": 0.25, "deferredWorker": 0.15, "dlqWorker": 0.05}
            elif self.latest_pressure_score < 0.65:
                self.latest_pressure_level = "MEDIUM"
                self.worker_utilization = {"criticalWorker": 0.70, "batchWorker": 0.55, "deferredWorker": 0.35, "dlqWorker": 0.10}
            elif self.latest_pressure_score < 0.85:
                self.latest_pressure_level = "HIGH"
                self.worker_utilization = {"criticalWorker": 0.88, "batchWorker": 0.75, "deferredWorker": 0.60, "dlqWorker": 0.25}
            else:
                self.latest_pressure_level = "CRITICAL"
                self.worker_utilization = {"criticalWorker": 0.98, "batchWorker": 0.90, "deferredWorker": 0.85, "dlqWorker": 0.60}

            # 6. Append to rolling decision log (max 15 items)
            decision_log_item = {
                "id": event.id[:8],
                "eventType": event.eventType.value,
                "priority": event.priority.value,
                "pressureScore": routing_meta.systemPressureAtRouting,
                "effectivePriorityScore": routing_meta.effectivePriorityScore,
                "decisionOutcome": outcome.value,
                "targetTopic": target_topic,
                "timestamp": routing_meta.orchestratedAt[11:19]
            }
            self.recent_decisions.insert(0, decision_log_item)
            if len(self.recent_decisions) > 15:
                self.recent_decisions.pop()

        except Exception as e:
            logger.error(f"Error ingesting raw event: {e}")

    async def process_metric(self, topic: str, payload: dict):
        try:
            m_type = payload.get("metricType")
            data = payload.get("data", {})

            if m_type == "DECISION_TELEMETRY":
                outcome = data.get("decisionOutcome")
                if outcome in self.decisions_counter:
                    self.decisions_counter[outcome] += 1
                self.total_processed_events += 1
                if outcome == "SHED":
                    self.total_dropped_events += 1

                self.latest_pressure_score = data.get("pressureScore", self.latest_pressure_score)
                if self.latest_pressure_score < 0.35:
                    self.latest_pressure_level = "LOW"
                elif self.latest_pressure_score < 0.65:
                    self.latest_pressure_level = "MEDIUM"
                elif self.latest_pressure_score < 0.85:
                    self.latest_pressure_level = "HIGH"
                else:
                    self.latest_pressure_level = "CRITICAL"

        except Exception as e:
            logger.error(f"Error aggregating metric: {e}")

    def snapshot(self) -> Dict[str, Any]:
        now = time.time()
        elapsed = max(0.5, now - self.last_reset_time)
        
        # Drain queues progressively to simulate active workers clearing jobs
        self.queue_occupancy["raw"] = max(0, int(self.queue_occupancy["raw"] * 0.7))
        self.queue_occupancy["critical"] = max(0, int(self.queue_occupancy["critical"] * 0.6))
        self.queue_occupancy["batch"] = max(0, int(self.queue_occupancy["batch"] * 0.75))
        self.queue_occupancy["deferred"] = max(0, int(self.queue_occupancy["deferred"] * 0.85))
        self.queue_occupancy["dlq"] = max(0, int(self.queue_occupancy["dlq"] * 0.9))

        eps = round(sum(self.decisions_counter.values()) / elapsed, 1)
        avg_lat = round(sum(self.latencies_window) / max(1, len(self.latencies_window)), 1)

        snapshot_data = {
            "throughputEps": eps,
            "avgLatencyMs": avg_lat,
            "pressureScore": self.latest_pressure_score,
            "pressureLevel": self.latest_pressure_level,
            "totalProcessed": self.total_processed_events,
            "totalDropped": self.total_dropped_events,
            "queues": dict(self.queue_occupancy),
            "decisions": dict(self.decisions_counter),
            "workerUtilization": dict(self.worker_utilization),
            "recentDecisions": self.recent_decisions[:10]
        }

        if elapsed >= 1.5:
            for k in self.decisions_counter:
                self.decisions_counter[k] = int(self.decisions_counter[k] * 0.6)
            self.last_reset_time = now

        return snapshot_data

    async def stop(self):
        await self.consumer.stop()
