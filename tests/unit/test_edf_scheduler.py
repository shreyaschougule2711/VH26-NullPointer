import pytest
from datetime import datetime, timezone
from services.haeo.edf_scheduler import EDFScheduler
from services.common.models import EventModel, EventType, PriorityLevel


def test_payment_priority_score():
    scheduler = EDFScheduler()
    event = EventModel(
        id="test_1",
        eventType=EventType.PAYMENT,
        priority=PriorityLevel.CRITICAL,
        arrivalTime=datetime.now(timezone.utc).isoformat(),
        deadline=100
    )
    score = scheduler.compute_effective_priority(event)
    # Payment base priority is 100.0
    assert score >= 100.0


def test_log_vs_payment_score():
    scheduler = EDFScheduler()
    now_iso = datetime.now(timezone.utc).isoformat()
    payment = EventModel(
        id="p1",
        eventType=EventType.PAYMENT,
        priority=PriorityLevel.CRITICAL,
        arrivalTime=now_iso,
        deadline=100
    )
    log_event = EventModel(
        id="l1",
        eventType=EventType.LOG,
        priority=PriorityLevel.TRIVIAL,
        arrivalTime=now_iso,
        deadline=30000
    )
    assert scheduler.compute_effective_priority(payment) > scheduler.compute_effective_priority(log_event)
