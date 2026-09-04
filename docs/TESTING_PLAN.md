# Comprehensive Testing & Verification Plan

## 1. Testing Strategy Overview

The verification matrix for AEOP covers three layers: Unit Testing, Integration Testing with Kafka, and Load/Stress Scenario Verification.

```
       ┌────────────────────────┐
       │   Scenario / Load      │  (Flash Sale, Black Friday, Spike Tests)
       │    Stress Tests        │
       └───────────┬────────────┘
                   │
       ┌───────────┴────────────┐
       │   Integration Tests    │  (Kafka Topic Routing, Worker ACKs)
       └───────────┬────────────┘
                   │
       ┌───────────┴────────────┐
       │      Unit Tests        │  (Pressure Formula, EDF Score, Rule Engine)
       └────────────────────────┘
```

---

## 2. Unit Testing Suite (`tests/unit/`)

### 2.1 Core Decision Engine Tests (`test_decision_engine.py`)
- **Test Case 1.1**: Verify `PAYMENT` event is ALWAYS assigned to `PROCESS` regardless of Pressure Score (0.0 to 1.0).
- **Test Case 1.2**: Verify `ORDER` event receives max 100ms artificial delay under `HIGH` pressure but is NEVER dropped.
- **Test Case 1.3**: Verify `CLICK` event is routed to `PROCESS` under `LOW` pressure, `DEFER` under `MEDIUM/HIGH`, and `SHED` under `CRITICAL` pressure.
- **Test Case 1.4**: Verify EDF Aging calculation score increases strictly monotonically with elapsed time.

### 2.2 Pressure Evaluator Tests (`test_pressure_evaluator.py`)
- **Test Case 2.1**: Calculate composite pressure score given sample inputs:
  - Queue Occupancy = 0.8, Worker Util = 0.9, Latency = 150ms (target 100ms), CPU = 70%.
  - Verify result matches expected formula output ($P = 0.805 \rightarrow \text{HIGH}$).

---

## 3. Integration Testing Suite (`tests/integration/`)

- **Kafka Ingest to HAEO Routing Test**:
  1. Produce 100 mixed events (`PAYMENT`, `LOG`, `CLICK`) to `raw-events`.
  2. Start HAEO engine in test mode with artificially injected `HIGH` pressure state.
  3. Assert `critical-events` contains 100% of `PAYMENT` events.
  4. Assert `dlq-events` or `deferred-events` contains `LOG` and `CLICK` events.

---

## 4. Load & Stress Simulation Scenarios

| Test Scenario | Config Profile | Target EPS | Verification Benchmark |
| :--- | :--- | :--- | :--- |
| **Baseline Normal** | `NORMAL` | 100 eps | 0 dropped events, Avg Latency < 15ms, Pressure LOW |
| **Flash Sale Spike**| `FLASH_SALE` | 2,000 eps | 0 Payment/Order drops, Clicks/Logs shed, Latency < 100ms |
| **Sustained Burst** | `RANDOM_BURST`| 1,500 eps peak | Adaptive batching activates, Batch size scales up to 25 |
| **Worker Failure** | `HIGH_PRESSURE` | 1,000 eps | Worker failure triggers Pressure CRITICAL; HAEO sheds non-critical traffic |

---

## 5. Automated Verification Script (`scripts/run_tests.sh`)

```bash
#!/bin/bash
set -e

echo "=== Running Unit Tests ==="
pytest tests/unit --cov=services

echo "=== Running Kafka Integration Tests ==="
pytest tests/integration

echo "=== System Health Check Complete ==="
```

---
