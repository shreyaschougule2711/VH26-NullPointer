# Configuration & System Parameters Guide

## 1. Environment Variables Matrix

| Component | Variable Name | Default Value | Description |
| :--- | :--- | :--- | :--- |
| **Global** | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker bootstrap address |
| **Global** | `LOG_LEVEL` | `INFO` | Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| **Simulator**| `SIMULATOR_PORT` | `8001` | Traffic simulator API internal port |
| **HAEO** | `HAEO_CONSUMER_GROUP` | `haeo-orchestrator-group` | Kafka consumer group ID for HAEO |
| **HAEO** | `MAX_QUEUE_CAPACITY` | `10000` | Max raw queue capacity threshold for pressure calc |
| **HAEO** | `TARGET_LATENCY_MS` | `100.0` | Target max latency baseline |
| **Workers** | `CRITICAL_WORKER_CONCURRENCY` | `10` | Concurrency thread/task count for critical worker |
| **Workers** | `BATCH_WORKER_WINDOW_MS` | `200` | Batch aggregation window in milliseconds |
| **Workers** | `BATCH_WORKER_MAX_SIZE` | `25` | Max size of a single micro-batch |
| **Metrics** | `METRICS_WS_PORT` | `8000` | FastAPI / WebSocket server port |

---

## 2. Dynamic Business Rules Specification (`config/haeo_rules.json`)

```json
{
  "version": "1.0.0",
  "pressureFormula": {
    "weights": {
      "queueOccupancy": 0.35,
      "workerUtilization": 0.25,
      "latency": 0.25,
      "cpu": 0.15
    },
    "thresholds": {
      "low": 0.35,
      "medium": 0.65,
      "high": 0.85
    }
  },
  "edfRules": {
    "agingMultiplierAlpha": 2.5,
    "urgencyMultiplierBeta": 40.0
  },
  "eventSlaRules": {
    "PAYMENT": {
      "basePriority": 100,
      "canDrop": false,
      "canBatch": false,
      "maxDelayMs": 0,
      "defaultTopic": "critical-events"
    },
    "ORDER": {
      "basePriority": 80,
      "canDrop": false,
      "canBatch": false,
      "maxDelayMs": 100,
      "defaultTopic": "critical-events"
    },
    "REFUND": {
      "basePriority": 90,
      "canDrop": false,
      "canBatch": false,
      "maxDelayMs": 50,
      "defaultTopic": "critical-events"
    },
    "INVENTORY": {
      "basePriority": 50,
      "canDrop": false,
      "canBatch": true,
      "maxDelayMs": 2000,
      "defaultTopic": "batch-events"
    },
    "NOTIFICATION": {
      "basePriority": 30,
      "canDrop": true,
      "canBatch": true,
      "maxDelayMs": 5000,
      "defaultTopic": "batch-events"
    },
    "CLICK": {
      "basePriority": 10,
      "canDrop": true,
      "canBatch": true,
      "maxDelayMs": 10000,
      "defaultTopic": "deferred-events"
    },
    "LOG": {
      "basePriority": 5,
      "canDrop": true,
      "canBatch": true,
      "maxDelayMs": 30000,
      "defaultTopic": "deferred-events"
    }
  }
}
```

---
