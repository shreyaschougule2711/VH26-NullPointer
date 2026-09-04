import pytest
from services.haeo.pressure_monitor import PressureMonitor
from services.common.models import PressureLevel


def test_pressure_calculation_low():
    monitor = PressureMonitor()
    monitor.update_telemetry(queue_len=100, worker_util=0.1, avg_lat_ms=10.0)
    score = monitor.compute_pressure_score()
    level = monitor.get_pressure_level(score)
    assert score < 0.35
    assert level == PressureLevel.LOW


def test_pressure_calculation_critical():
    monitor = PressureMonitor()
    monitor.update_telemetry(queue_len=10000, worker_util=1.0, avg_lat_ms=500.0)
    score = monitor.compute_pressure_score()
    level = monitor.get_pressure_level(score)
    assert score >= 0.85
    assert level == PressureLevel.CRITICAL
