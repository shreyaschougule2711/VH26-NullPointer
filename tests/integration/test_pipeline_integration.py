import pytest
import asyncio
from datetime import datetime, timezone
from services.common.models import EventModel, EventType, PriorityLevel
from services.haeo.pressure_monitor import PressureMonitor
from services.haeo.edf_scheduler import EDFScheduler
from services.haeo.rule_engine import RuleEngine
from services.haeo.decision_engine import DecisionEngine


@pytest.mark.asyncio
async def test_end_to_end_orchestration_flow():
    pm = PressureMonitor()
    edf = EDFScheduler()
    re = RuleEngine()
    engine = DecisionEngine(pm, edf, re)

    events = [
        EventModel(id="1", eventType=EventType.PAYMENT, priority=PriorityLevel.CRITICAL, arrivalTime=datetime.now(timezone.utc).isoformat(), deadline=100),
        EventModel(id="2", eventType=EventType.INVENTORY, priority=PriorityLevel.MEDIUM, arrivalTime=datetime.now(timezone.utc).isoformat(), deadline=2000),
        EventModel(id="3", eventType=EventType.LOG, priority=PriorityLevel.TRIVIAL, arrivalTime=datetime.now(timezone.utc).isoformat(), deadline=30000),
    ]

    results = []
    for ev in events:
        outcome, topic, meta = engine.evaluate_and_route(ev)
        results.append((ev.eventType, outcome, topic))

    assert results[0] == (EventType.PAYMENT, "PROCESS", "critical-events")
    assert results[1][2] in ["critical-events", "batch-events"]
