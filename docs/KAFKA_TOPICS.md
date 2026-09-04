# Kafka Topic Definitions & Event Schemas

## 1. Kafka Topic Topology Specification

| Topic Name | Retention Period | Partitions | Keying Strategy | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| `raw-events` | 1 hour | 6 | `eventId` (Round-Robin) | High-throughput ingest queue from Traffic Simulator |
| `critical-events` | 6 hours | 6 | `eventId` | High-priority processing stream (Payments, Orders, Refunds) |
| `batch-events` | 6 hours | 3 | `eventType` | Micro-batching stream (Inventory updates, Notifications) |
| `deferred-events` | 12 hours | 3 | `eventType` | Rate-limited background queue (Clicks, Logs) |
| `dlq-events` | 24 hours | 2 | `eventId` | Shed events audit stream & failed processing retry queue |
| `metrics-events` | 1 hour | 2 | `serviceId` | Telemetry event bus emitted by Orchestrator and Workers |

---

## 2. Event Payload Schemas

### 2.1 Ingest Event Schema (`raw-events`)

Every message in Kafka is JSON-encoded.

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "IngestEvent",
  "type": "object",
  "required": ["id", "eventType", "priority", "arrivalTime", "deadline", "retryCount", "payload"],
  "properties": {
    "id": {
      "type": "string",
      "format": "uuid",
      "example": "e4f8b91a-289b-4c55-90df-8b3fa47d1001"
    },
    "eventType": {
      "type": "string",
      "enum": ["PAYMENT", "ORDER", "REFUND", "INVENTORY", "NOTIFICATION", "CLICK", "LOG"],
      "example": "PAYMENT"
    },
    "priority": {
      "type": "string",
      "enum": ["CRITICAL", "HIGH", "MEDIUM", "LOW", "TRIVIAL"],
      "example": "CRITICAL"
    },
    "arrivalTime": {
      "type": "string",
      "format": "date-time",
      "example": "2026-09-04T15:45:00.123456Z"
    },
    "deadline": {
      "type": "integer",
      "description": "Deadline window in milliseconds from arrival time",
      "example": 200
    },
    "retryCount": {
      "type": "integer",
      "minimum": 0,
      "example": 0
    },
    "payload": {
      "type": "object",
      "additionalProperties": true,
      "example": {
        "userId": "usr_99182",
        "amount": 499.99,
        "currency": "INR"
      }
    }
  }
}
```

---

### 2.2 Routed Event Schema (`critical-events`, `batch-events`, `deferred-events`, `dlq-events`)

Extends the Ingest Event Schema with HAEO routing metadata:

```json
{
  "id": "e4f8b91a-289b-4c55-90df-8b3fa47d1001",
  "eventType": "PAYMENT",
  "priority": "CRITICAL",
  "arrivalTime": "2026-09-04T15:45:00.123456Z",
  "deadline": 200,
  "retryCount": 0,
  "payload": {
    "userId": "usr_99182",
    "amount": 499.99
  },
  "routingMeta": {
    "effectivePriorityScore": 142.5,
    "systemPressureAtRouting": 0.42,
    "decisionOutcome": "PROCESS",
    "targetTopic": "critical-events",
    "orchestratedAt": "2026-09-04T15:45:00.128100Z",
    "orchestrationDelayMs": 4.64
  }
}
```

---

### 2.3 Telemetry Metric Schema (`metrics-events`)

Emitted by workers and HAEO to report system telemetry:

```json
{
  "metricId": "m_8829102",
  "timestamp": "2026-09-04T15:45:01.000000Z",
  "sourceService": "worker-critical-1",
  "metricType": "WORKER_EXECUTION",
  "data": {
    "processedCount": 150,
    "failedCount": 0,
    "avgLatencyMs": 12.4,
    "p95LatencyMs": 18.2,
    "workerUtilization": 0.78,
    "currentQueueOccupancy": 45
  }
}
```

---
