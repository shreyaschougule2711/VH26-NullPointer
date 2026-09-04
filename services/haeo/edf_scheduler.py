import time
from datetime import datetime, timezone
from services.common.models import EventModel, EventType
from services.common.logger import get_logger

logger = get_logger("EDFScheduler")

BASE_PRIORITIES = {
    EventType.PAYMENT: 100.0,
    EventType.REFUND: 90.0,
    EventType.ORDER: 80.0,
    EventType.INVENTORY: 50.0,
    EventType.NOTIFICATION: 30.0,
    EventType.CLICK: 10.0,
    EventType.LOG: 5.0
}


class EDFScheduler:
    def __init__(self, alpha: float = 2.5, beta: float = 40.0, max_deadline_ms: float = 10000.0):
        self.alpha = alpha  # Aging multiplier
        self.beta = beta    # Urgency multiplier
        self.max_deadline_ms = max_deadline_ms

    def compute_effective_priority(self, event: EventModel) -> float:
        base = BASE_PRIORITIES.get(event.eventType, 10.0)

        # Compute aging (waiting time in seconds)
        try:
            arr_dt = datetime.fromisoformat(event.arrivalTime.replace("Z", "+00:00"))
            now_dt = datetime.now(timezone.utc)
            waiting_time_sec = max(0.0, (now_dt - arr_dt).total_seconds())
        except Exception:
            waiting_time_sec = 0.0

        aging_score = self.alpha * waiting_time_sec

        # Compute EDF urgency factor
        # Deadline is in ms from arrival
        deadline_window_sec = event.deadline / 1000.0
        time_remaining_sec = max(0.0, deadline_window_sec - waiting_time_sec)
        
        urgency_ratio = max(0.0, 1.0 - (time_remaining_sec / (self.max_deadline_ms / 1000.0)))
        urgency_score = self.beta * urgency_ratio

        total_score = base + aging_score + urgency_score
        return float(round(total_score, 2))
