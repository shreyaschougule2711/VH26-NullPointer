import psutil
import time
from typing import Dict, Any
from services.common.models import PressureLevel
from services.common.logger import get_logger

logger = get_logger("PressureMonitor")


class PressureMonitor:
    def __init__(self):
        self.weights = {
            "queueOccupancy": 0.35,
            "workerUtilization": 0.25,
            "latency": 0.25,
            "cpu": 0.15
        }
        self.thresholds = {
            "low": 0.35,
            "medium": 0.65,
            "high": 0.85
        }
        self.max_queue_capacity = 10000
        self.target_max_latency_ms = 100.0

        # State cache updated by telemetry feedback
        self.current_queue_length = 0
        self.worker_utilization = 0.1
        self.avg_latency_ms = 10.0
        self.last_update_time = time.time()

    def update_telemetry(self, queue_len: int, worker_util: float, avg_lat_ms: float):
        self.current_queue_length = max(0, queue_len)
        self.worker_utilization = max(0.0, min(1.0, worker_util))
        self.avg_latency_ms = max(0.0, avg_lat_ms)
        self.last_update_time = time.time()

    def compute_pressure_score(self) -> float:
        # Queue occupancy ratio
        q_ratio = min(1.0, self.current_queue_length / self.max_queue_capacity)
        
        # Worker utilization ratio
        u_ratio = self.worker_utilization
        
        # Latency pressure ratio
        l_ratio = min(1.0, self.avg_latency_ms / self.target_max_latency_ms)
        
        # System CPU ratio
        try:
            c_ratio = psutil.cpu_percent(interval=None) / 100.0
        except Exception:
            c_ratio = 0.2

        w = self.weights
        pressure_score = (
            w["queueOccupancy"] * q_ratio +
            w["workerUtilization"] * u_ratio +
            w["latency"] * l_ratio +
            w["cpu"] * c_ratio
        )
        return float(round(max(0.0, min(1.0, pressure_score)), 4))

    def get_pressure_level(self, score: float) -> PressureLevel:
        if score < self.thresholds["low"]:
            return PressureLevel.LOW
        elif score < self.thresholds["medium"]:
            return PressureLevel.MEDIUM
        elif score < self.thresholds["high"]:
            return PressureLevel.HIGH
        else:
            return PressureLevel.CRITICAL
