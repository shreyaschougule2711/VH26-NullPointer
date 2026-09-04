from enum import Enum
from typing import Dict, Any, Optional
from datetime import datetime, timezone
from pydantic import BaseModel, Field


class EventType(str, Enum):
    PAYMENT = "PAYMENT"
    ORDER = "ORDER"
    REFUND = "REFUND"
    INVENTORY = "INVENTORY"
    NOTIFICATION = "NOTIFICATION"
    CLICK = "CLICK"
    LOG = "LOG"


class PriorityLevel(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    TRIVIAL = "TRIVIAL"


class DecisionOutcome(str, Enum):
    PROCESS = "PROCESS"
    BATCH = "BATCH"
    DEFER = "DEFER"
    SHED = "SHED"
    RETRY = "RETRY"


class PressureLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class RoutingMeta(BaseModel):
    effectivePriorityScore: float = 0.0
    systemPressureAtRouting: float = 0.0
    decisionOutcome: DecisionOutcome = DecisionOutcome.PROCESS
    targetTopic: str = "critical-events"
    orchestratedAt: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    orchestrationDelayMs: float = 0.0


class EventModel(BaseModel):
    id: str
    eventType: EventType
    priority: PriorityLevel
    arrivalTime: str
    deadline: int  # deadline in ms from arrival
    retryCount: int = 0
    payload: Dict[str, Any] = Field(default_factory=dict)
    routingMeta: Optional[RoutingMeta] = None


class MetricTelemetryModel(BaseModel):
    metricId: str
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    sourceService: str
    metricType: str  # WORKER_EXECUTION, DECISION_TELEMETRY, PRESSURE_TELEMETRY
    data: Dict[str, Any]


class SimulationConfigModel(BaseModel):
    scenario: str = "NORMAL"
    baseEps: int = 50
    spikeMultiplier: float = 1.0
    rampUpSeconds: int = 0
    peakDurationSeconds: int = 60
    rampDownSeconds: int = 0
    eventDistribution: Dict[str, float] = Field(default_factory=dict)
