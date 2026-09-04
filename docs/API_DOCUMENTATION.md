# REST & WebSocket API Documentation

## 1. REST API Specification (FastAPI Control Plane)

Base URL: `http://localhost:8000/api/v1`

---

### 1.1 Traffic Simulation Control

#### `POST /simulation/start`
Starts or reconfigures the traffic simulator.

**Request Body:**
```json
{
  "scenario": "FLASH_SALE",
  "baseEps": 100,
  "spikeMultiplier": 10,
  "rampUpSeconds": 15,
  "peakDurationSeconds": 45,
  "rampDownSeconds": 15,
  "eventDistribution": {
    "PAYMENT": 0.20,
    "ORDER": 0.25,
    "REFUND": 0.05,
    "INVENTORY": 0.15,
    "NOTIFICATION": 0.15,
    "CLICK": 0.10,
    "LOG": 0.10
  }
}
```

**Response (200 OK):**
```json
{
  "status": "SUCCESS",
  "message": "Traffic simulator started with scenario 'FLASH_SALE'",
  "simulationId": "sim_881920",
  "targetEps": 1000
}
```

---

#### `POST /simulation/stop`
Stops the active simulation immediately.

**Response (200 OK):**
```json
{
  "status": "STOPPED",
  "simulationId": "sim_881920",
  "totalEventsGenerated": 45200
}
```

---

#### `GET /simulation/status`
Returns current simulator state.

**Response (200 OK):**
```json
{
  "isRunning": true,
  "activeScenario": "FLASH_SALE",
  "currentEps": 850,
  "elapsedSeconds": 24,
  "totalGenerated": 20400
}
```

---

### 1.2 Orchestrator Rules & Dynamic Configuration

#### `GET /haeo/config`
Retrieves current business SLA rules, pressure weights, and thresholds.

---

#### `PUT /haeo/config`
Dynamically updates pressure weights or rule parameters without service restart.

**Request Body:**
```json
{
  "pressureWeights": {
    "queueOccupancy": 0.40,
    "workerUtilization": 0.20,
    "latency": 0.25,
    "cpu": 0.15
  },
  "agingMultiplier": 3.0
}
```

---

## 2. WebSocket Protocol Specification

Endpoint: `ws://localhost:8000/ws/metrics`

### Connection Handshake & Protocol
Upon connection, the server streams real-time metrics every 500ms in JSON format.

```json
{
  "event": "METRICS_UPDATE",
  "timestamp": "2026-09-04T15:45:05.500000Z",
  "metrics": {
    "throughputEps": 942.5,
    "avgLatencyMs": 34.2,
    "pressureScore": 0.74,
    "pressureLevel": "HIGH",
    "queues": {
      "raw": 450,
      "critical": 12,
      "batch": 180,
      "deferred": 890,
      "dlq": 34
    },
    "decisionsLast5s": {
      "PROCESS": 450,
      "BATCH": 320,
      "DEFER": 120,
      "SHED": 40,
      "RETRY": 0
    },
    "workerUtilization": {
      "criticalWorker": 0.85,
      "batchWorker": 0.62,
      "deferredWorker": 0.30,
      "dlqWorker": 0.05
    }
  }
}
```

---
