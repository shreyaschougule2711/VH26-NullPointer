import pytest
from datetime import datetime, timezone
from services.haeo.pressure_monitor import PressureMonitor
from services.haeo.edf_scheduler import EDFScheduler
from services.haeo.rule_engine import RuleEngine
from services.haeo.decision_engine import DecisionEngine
from services.common.models import EventModel, EventType, PriorityLevel, DecisionOutcome, PressureLevel


def test_payment_never_drops():
    pm = PressureMonitor()
    pm.update_telemetry(queue_len=10000, worker_util=1.0, avg_lat_ms=500.0)  # Critical pressure
    
    edf = EDFScheduler()
    re = RuleEngine()
    de = DecisionEngine(pm, edf, re)

    event = EventModel(
        id="pay_99",
        eventType=EventType.PAYMENT,
        priority=PriorityLevel.CRITICAL,
        arrivalTime=datetime.now(timezone.utc).isoformat(),
        deadline=100
    )

    outcome, topic, meta = de.evaluate_and_route(event)
    assert outcome == DecisionOutcome.PROCESS
    assert topic == "critical-events"


def test_click_shed_under_critical_pressure():
    pm = PressureMonitor()
    pm.update_telemetry(queue_len=10000, worker_util=1.0, avg_lat_ms=500.0)
    
    edf = EDFScheduler()
    re = RuleEngine()
    de = DecisionEngine(pm, edf, re)

    event = EventModel(
        id="clk_1",
        eventType=EventType.CLICK,
        priority=PriorityLevel.TRIVIAL,
        arrivalTime=datetime.now(timezone.utc).isoformat(),
        deadline=10000
    )

    outcome, topic, meta = de.evaluate_and_route(event)
    assert outcome == DecisionOutcome.SHED
    assert topic == "dlq-events"
