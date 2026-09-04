"""
AEOP — Adaptive Event Orchestration Platform
Unified Server: Traffic Generator + HAEO Decision Engine + Workers + Metrics + WebSocket

All components run in a single async process for reliable local execution.
Configuration loaded from config/haeo_rules.json and config/traffic_profiles.json.
Nothing is hardcoded — all parameters are driven by config files.
"""

import asyncio
import json
import math
import os
import random
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ═══════════════════════════════════════════════════════════════════════
# CONFIGURATION LOADER
# ═══════════════════════════════════════════════════════════════════════

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _load_json(path: str, default: dict = None) -> dict:
    if os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    return default or {}


RULES = _load_json(os.path.join(BASE_DIR, "config", "haeo_rules.json"))
PROFILES = _load_json(os.path.join(BASE_DIR, "config", "traffic_profiles.json")).get(
    "scenarios", {}
)

# Extract sub-configs once
_PF = RULES.get("pressureFormula", {})
_PW = _PF.get("weights", {"queueOccupancy": 0.40, "workerUtilization": 0.35, "latency": 0.25})
_PT = _PF.get("thresholds", {"low": 0.30, "medium": 0.55, "high": 0.75})
_WC = RULES.get("workerConfig", {})
_SLA = RULES.get("eventSlaRules", {})
_EDF = RULES.get("edfRules", {})


# ═══════════════════════════════════════════════════════════════════════
# REQUEST / RESPONSE MODELS
# ═══════════════════════════════════════════════════════════════════════


class SimRequest(BaseModel):
    scenario: str = "NORMAL"
    baseEps: int = 1000
    spikeMultiplier: float = 10.0
    spikeStartSec: float = 10.0
    spikeDurationSec: float = 20.0
    rampDownSec: float = 5.0
    pattern: str = "SUDDEN_SPIKE"
    eventDistribution: Optional[Dict[str, float]] = None


# ═══════════════════════════════════════════════════════════════════════
# PRESSURE CALCULATOR
# ═══════════════════════════════════════════════════════════════════════


class PressureCalculator:
    """Computes composite system pressure P = Wq·Q + Wu·U + Wl·L"""

    def __init__(self):
        self.wq = _PW["queueOccupancy"]
        self.wu = _PW["workerUtilization"]
        self.wl = _PW["latency"]
        self.t_low = _PT["low"]
        self.t_med = _PT["medium"]
        self.t_high = _PT["high"]
        self.max_q = max(1, _WC.get("maxQueueCapacity", 5000))
        self.target_lat = max(1, _WC.get("targetLatencyMs", 200))

    def compute(self, queue_size: int, worker_util: float, avg_latency: float):
        q = min(1.0, queue_size / self.max_q)
        u = min(1.0, max(0.0, worker_util))
        l = min(1.0, avg_latency / self.target_lat)
        score = round(min(1.0, max(0.0, self.wq * q + self.wu * u + self.wl * l)), 4)

        if score < self.t_low:
            level = "LOW"
        elif score < self.t_med:
            level = "MEDIUM"
        elif score < self.t_high:
            level = "HIGH"
        else:
            level = "CRITICAL"
        return level, score


# ═══════════════════════════════════════════════════════════════════════
# HAEO DECISION ENGINE
# ═══════════════════════════════════════════════════════════════════════


class DecisionEngine:
    """
    Adaptive routing decisions based on:
      - Event SLA rules (priority, can_drop, can_batch)
      - Current system pressure level
      - Event aging (EDF-inspired starvation prevention)
    """

    def __init__(self):
        self.sla = _SLA
        self.aging_threshold = _EDF.get("agingThresholdSec", 30)

    def decide(self, event_type: str, pressure: str, waiting_sec: float = 0.0):
        rule = self.sla.get(event_type, {})
        priority = rule.get("basePriority", 10)
        can_drop = rule.get("canDrop", True)
        can_batch = rule.get("canBatch", True)

        # ── Aging: prevent starvation of long-waiting events ──
        if waiting_sec > self.aging_threshold:
            return "PROCESS", "Aging Priority Boost"

        # ── Critical events (PAYMENT, ORDER, REFUND): always process ──
        if not can_drop and not can_batch:
            return "PROCESS", "Critical Event"

        # ── LOW pressure: stream everything through ──
        if pressure == "LOW":
            return "PROCESS", "Low Pressure"

        # ── MEDIUM pressure: gentle optimisation ──
        if pressure == "MEDIUM":
            if can_batch and priority <= 50:
                return "BATCH", "Adaptive Batch"
            return "PROCESS", "Medium Priority"

        # ── HIGH pressure: batch + defer + selective shedding ──
        if pressure == "HIGH":
            if can_drop and priority <= 5:       # LOG only
                return "DROP", "Load Shedding"
            if can_batch:
                return "BATCH", "Adaptive Batch"
            if priority <= 30:
                return "DEFER", "Queue Full"
            return "PROCESS", "Priority Process"

        # ── CRITICAL pressure: aggressive optimisation ──
        if can_drop and priority <= 5:           # LOG → drop
            return "DROP", "Load Shedding"
        if can_batch:                            # INVENTORY, NOTIFICATION → batch
            return "BATCH", "Emergency Batch"
        if can_drop and priority <= 10:          # CLICK → defer
            return "DEFER", "Queue Full"
        if priority <= 30:
            return "DEFER", "Critical Defer"
        return "PROCESS", "Critical Priority"


# ═══════════════════════════════════════════════════════════════════════
# SIMULATION ENGINE (Traffic Gen + Workers + Metrics)
# ═══════════════════════════════════════════════════════════════════════


class SimulationEngine:
    """Orchestrates the entire simulation lifecycle in-process."""

    TICK_DT = 0.05          # 50 ms per tick → 20 ticks/sec
    CHART_TICKS = 10        # record chart point every 10 ticks (500 ms)
    MAX_CHART_PTS = 300     # sliding window size for time-series

    def __init__(self):
        self.pressure = PressureCalculator()
        self.decision = DecisionEngine()
        self.running = False
        self._task: Optional[asyncio.Task] = None
        self._reset()

    # ────────── State Reset ──────────

    def _reset(self):
        self.start_time = 0.0
        self.config: Optional[SimRequest] = None

        # Queues
        self.q_raw = 0
        self.q_critical = 0
        self.q_batch = 0
        self.q_deferred = 0

        # Worker capacities (from config)
        self.cap_haeo = _WC.get("haeoClassificationRate", 15000)
        self.cap_critical = _WC.get("criticalProcessingRate", 10000)
        self.cap_batch = _WC.get("batchProcessingRate", 5000)
        self.cap_deferred = _WC.get("deferredProcessingRate", 2000)
        self.base_latency = _WC.get("baseLatencyMs", 15)

        # Counters
        self.total_gen = 0
        self.total_processed = 0
        self.total_batched = 0
        self.total_deferred_proc = 0
        self.total_dropped = 0
        self.critical_lost = 0

        # Live gauges
        self.cur_eps = 0.0
        self.cur_pressure = "LOW"
        self.cur_pscore = 0.0
        self.cur_latency = float(self.base_latency)
        self.peak_latency = float(self.base_latency)
        self.peak_queue = 0
        self.peak_eps = 0

        # Worker utilisation
        self.w_util = {"critical": 0.0, "batch": 0.0, "deferred": 0.0}

        # Decision totals
        self.dec_total = {"PROCESS": 0, "BATCH": 0, "DEFER": 0, "DROP": 0}

        # Recent decisions ring buffer
        self.recent_dec: deque = deque(maxlen=100)

        # Time-series buffers
        self.ts_t: List[float] = []
        self.ts_eps: List[float] = []
        self.ts_lat: List[float] = []
        self.ts_qr: List[int] = []
        self.ts_qc: List[int] = []
        self.ts_qb: List[int] = []
        self.ts_qd: List[int] = []
        self.ts_p: List[float] = []

        self._tick = 0
        self._sec_gen = 0
        self._sec_proc = 0
        self._last_sec = 0.0
        self._actual_eps = 0.0

    # ────────── Start / Stop ──────────

    async def start(self, req: SimRequest):
        if self.running:
            self.stop()
            await asyncio.sleep(0.1)

        self._reset()
        self.config = req

        # Merge scenario profile if available
        if req.scenario in PROFILES:
            p = PROFILES[req.scenario]
            if not req.eventDistribution:
                req.eventDistribution = p.get("eventDistribution")
            for key in ("spikeMultiplier", "spikeStartSec", "spikeDurationSec", "rampDownSec", "pattern"):
                if key in p:
                    setattr(req, key, p[key])

        self.running = True
        self.start_time = time.time()
        self._task = asyncio.create_task(self._loop())

    def stop(self):
        self.running = False
        if self._task and not self._task.done():
            self._task.cancel()

    # ────────── Traffic Pattern Calculator ──────────

    def _target_eps(self, t: float) -> float:
        c = self.config
        if c.pattern == "SUDDEN_SPIKE":
            if t < c.spikeStartSec:
                return c.baseEps
            elif t < c.spikeStartSec + c.spikeDurationSec:
                return c.baseEps * c.spikeMultiplier
            elif t < c.spikeStartSec + c.spikeDurationSec + c.rampDownSec:
                frac = (t - c.spikeStartSec - c.spikeDurationSec) / max(0.1, c.rampDownSec)
                return c.baseEps * c.spikeMultiplier * (1 - frac) + c.baseEps * frac
            return c.baseEps

        elif c.pattern == "GRADUAL_SPIKE":
            if t < c.spikeStartSec:
                return c.baseEps
            half = c.spikeDurationSec / 2
            peak = c.baseEps * c.spikeMultiplier
            if t < c.spikeStartSec + half:
                frac = (t - c.spikeStartSec) / max(0.1, half)
                return c.baseEps + (peak - c.baseEps) * frac
            elif t < c.spikeStartSec + c.spikeDurationSec:
                frac = (t - c.spikeStartSec - half) / max(0.1, half)
                return peak - (peak - c.baseEps) * frac
            elif t < c.spikeStartSec + c.spikeDurationSec + c.rampDownSec:
                frac = (t - c.spikeStartSec - c.spikeDurationSec) / max(0.1, c.rampDownSec)
                return c.baseEps * (1 + (c.spikeMultiplier - 1) * 0.1 * (1 - frac))
            return c.baseEps

        elif c.pattern == "RANDOM_BURST":
            if random.random() < 0.05:
                return c.baseEps * c.spikeMultiplier * random.uniform(0.4, 1.0)
            return c.baseEps * random.uniform(0.8, 1.2)

        return c.baseEps  # NORMAL

    # ────────── Event Type Picker ──────────

    def _pick_type(self) -> str:
        dist = self.config.eventDistribution or {
            "PAYMENT": 0.20, "ORDER": 0.20, "CLICK": 0.25,
            "INVENTORY": 0.15, "LOG": 0.10, "NOTIFICATION": 0.10,
        }
        types = list(dist.keys())
        weights = [dist[t] for t in types]
        return random.choices(types, weights=weights, k=1)[0]

    # ────────── Main Simulation Loop ──────────

    async def _loop(self):
        dt = self.TICK_DT
        try:
            while self.running:
                elapsed = time.time() - self.start_time
                target = self._target_eps(elapsed)
                self.cur_eps = target
                self.peak_eps = max(self.peak_eps, target)

                n_events = max(0, int(target * dt + random.uniform(-0.5, 0.5)))
                self.total_gen += n_events
                self._sec_gen += n_events
                self.q_raw += n_events

                # ── HAEO Classification (rate-limited) ──
                classify_budget = int(self.cap_haeo * dt)
                classified = min(self.q_raw, classify_budget)
                self.q_raw = max(0, self.q_raw - classified)

                for _ in range(classified):
                    etype = self._pick_type()
                    total_q = self.q_raw + self.q_critical + self.q_batch + self.q_deferred
                    waiting = random.uniform(0, min(5, total_q / 500)) if total_q > 50 else 0

                    outcome, reason = self.decision.decide(etype, self.cur_pressure, waiting)

                    if outcome == "PROCESS":
                        self.q_critical += 1
                    elif outcome == "BATCH":
                        self.q_batch += 1
                    elif outcome == "DEFER":
                        self.q_deferred += 1
                    else:  # DROP
                        self.total_dropped += 1

                    self.dec_total[outcome] += 1

                    # Log decision
                    self.recent_dec.appendleft({
                        "time": datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3],
                        "elapsed": round(elapsed, 1),
                        "eventType": etype,
                        "queue": total_q,
                        "pressure": self.cur_pressure,
                        "pScore": self.cur_pscore,
                        "decision": outcome,
                        "reason": reason,
                        "latency": round(self.cur_latency, 1),
                    })

                # ── Worker Processing ──
                c_proc = min(self.q_critical, int(self.cap_critical * dt))
                self.q_critical = max(0, self.q_critical - c_proc)
                self.total_processed += c_proc
                self._sec_proc += c_proc

                b_proc = min(self.q_batch, int(self.cap_batch * dt))
                self.q_batch = max(0, self.q_batch - b_proc)
                self.total_batched += b_proc

                d_proc = min(self.q_deferred, int(self.cap_deferred * dt))
                self.q_deferred = max(0, self.q_deferred - d_proc)
                self.total_deferred_proc += d_proc

                # ── Worker Utilisation ──
                self.w_util["critical"] = min(1.0, c_proc / max(1, self.cap_critical * dt)) if (self.q_critical > 0 or c_proc > 0) else max(0, self.w_util["critical"] * 0.85)
                self.w_util["batch"] = min(1.0, b_proc / max(1, self.cap_batch * dt)) if (self.q_batch > 0 or b_proc > 0) else max(0, self.w_util["batch"] * 0.85)
                self.w_util["deferred"] = min(1.0, d_proc / max(1, self.cap_deferred * dt)) if (self.q_deferred > 0 or d_proc > 0) else max(0, self.w_util["deferred"] * 0.85)

                # ── Latency Model ──
                total_q = self.q_raw + self.q_critical + self.q_batch + self.q_deferred
                qf = min(1.0, total_q / self.pressure.max_q)
                uf = max(self.w_util.values()) if self.w_util else 0
                self.cur_latency = round(min(300, self.base_latency * (1 + qf * 6 + uf ** 3 * 14)), 1)
                self.peak_latency = max(self.peak_latency, self.cur_latency)
                self.peak_queue = max(self.peak_queue, total_q)

                # ── Pressure Recalculation ──
                overall_util = sum(self.w_util.values()) / 3
                self.cur_pressure, self.cur_pscore = self.pressure.compute(
                    total_q, overall_util, self.cur_latency
                )

                # ── Actual EPS calculation (per second) ──
                if elapsed - self._last_sec >= 1.0:
                    self._actual_eps = self._sec_gen
                    self._sec_gen = 0
                    self._sec_proc = 0
                    self._last_sec = elapsed

                # ── Record Time-Series ──
                self._tick += 1
                if self._tick % self.CHART_TICKS == 0:
                    self.ts_t.append(round(elapsed, 1))
                    self.ts_eps.append(round(target))
                    self.ts_lat.append(round(self.cur_latency, 1))
                    self.ts_qr.append(self.q_raw)
                    self.ts_qc.append(self.q_critical)
                    self.ts_qb.append(self.q_batch)
                    self.ts_qd.append(self.q_deferred)
                    self.ts_p.append(self.cur_pscore)

                    if len(self.ts_t) > self.MAX_CHART_PTS:
                        for arr in (self.ts_t, self.ts_eps, self.ts_lat, self.ts_qr,
                                    self.ts_qc, self.ts_qb, self.ts_qd, self.ts_p):
                            arr.pop(0)

                await asyncio.sleep(dt)

        except asyncio.CancelledError:
            pass
        finally:
            self.running = False

    # ────────── Snapshot for WebSocket / REST ──────────

    def snapshot(self) -> dict:
        total_decided = max(1, sum(self.dec_total.values()))
        total_q = self.q_raw + self.q_critical + self.q_batch + self.q_deferred
        elapsed = round(time.time() - self.start_time, 1) if self.start_time else 0

        return {
            "running": self.running,
            "elapsed": elapsed,
            "currentEps": round(self.cur_eps),
            "actualEps": round(self._actual_eps),
            "pressureLevel": self.cur_pressure,
            "pressureScore": self.cur_pscore,
            "avgLatencyMs": self.cur_latency,
            "totalGenerated": self.total_gen,
            "totalProcessed": self.total_processed,
            "totalBatched": self.total_batched,
            "totalDeferredProcessed": self.total_deferred_proc,
            "totalDropped": self.total_dropped,
            "criticalLost": self.critical_lost,
            "peakLatency": self.peak_latency,
            "peakQueue": self.peak_queue,
            "peakEps": round(self.peak_eps),
            "queues": {
                "raw": self.q_raw,
                "critical": self.q_critical,
                "batch": self.q_batch,
                "deferred": self.q_deferred,
                "total": total_q,
            },
            "workerUtilization": {
                k: round(v * 100, 1) for k, v in self.w_util.items()
            },
            "decisions": dict(self.dec_total),
            "decisionPct": {
                k: round(v / total_decided * 100, 1) for k, v in self.dec_total.items()
            },
            "recentDecisions": list(self.recent_dec)[:30],
            "timeSeries": {
                "timestamps": list(self.ts_t[-80:]),
                "throughput": list(self.ts_eps[-80:]),
                "latency": list(self.ts_lat[-80:]),
                "queueRaw": list(self.ts_qr[-80:]),
                "queueCritical": list(self.ts_qc[-80:]),
                "queueBatch": list(self.ts_qb[-80:]),
                "queueDeferred": list(self.ts_qd[-80:]),
                "pressure": list(self.ts_p[-80:]),
            },
        }


# ═══════════════════════════════════════════════════════════════════════
# WEBSOCKET CONNECTION MANAGER
# ═══════════════════════════════════════════════════════════════════════


class WSManager:
    def __init__(self):
        self.active: Set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.add(ws)

    def disconnect(self, ws: WebSocket):
        self.active.discard(ws)

    async def broadcast(self, data: dict):
        dead: Set[WebSocket] = set()
        for ws in self.active:
            try:
                await ws.send_json(data)
            except Exception:
                dead.add(ws)
        self.active -= dead


# ═══════════════════════════════════════════════════════════════════════
# FASTAPI APPLICATION
# ═══════════════════════════════════════════════════════════════════════

engine = SimulationEngine()
ws_mgr = WSManager()


async def _broadcast_loop():
    """Push metrics snapshot to all connected WebSocket clients every 500ms."""
    while True:
        if ws_mgr.active:
            await ws_mgr.broadcast({"event": "METRICS_UPDATE", "data": engine.snapshot()})
        await asyncio.sleep(0.5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_broadcast_loop())
    yield
    task.cancel()


app = FastAPI(title="AEOP Unified Server", version="2.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    return {"status": "UP", "running": engine.running}


@app.get("/api/scenarios")
async def list_scenarios():
    return {k: {"label": v.get("label", k), "description": v.get("description", "")} for k, v in PROFILES.items()}


@app.get("/api/scenario/{name}")
async def get_scenario(name: str):
    if name in PROFILES:
        return PROFILES[name]
    return {"error": "Scenario not found"}


@app.post("/api/simulation/start")
async def start_simulation(req: SimRequest):
    await engine.start(req)
    return {"status": "STARTED", "scenario": req.scenario, "baseEps": req.baseEps}


@app.post("/api/simulation/stop")
async def stop_simulation():
    engine.stop()
    return {
        "status": "STOPPED",
        "summary": {
            "totalGenerated": engine.total_gen,
            "totalProcessed": engine.total_processed,
            "totalBatched": engine.total_batched,
            "totalDeferred": engine.total_deferred_proc,
            "totalDropped": engine.total_dropped,
            "criticalLost": engine.critical_lost,
            "peakLatency": engine.peak_latency,
            "peakQueue": engine.peak_queue,
            "peakEps": round(engine.peak_eps),
        },
    }


@app.get("/api/simulation/status")
async def simulation_status():
    return engine.snapshot()


@app.get("/api/rules")
async def get_rules():
    return RULES


@app.websocket("/ws/metrics")
async def websocket_endpoint(ws: WebSocket):
    await ws_mgr.connect(ws)
    try:
        while True:
            await ws.receive_text()  # keep-alive
    except WebSocketDisconnect:
        ws_mgr.disconnect(ws)
    except Exception:
        ws_mgr.disconnect(ws)


# ═══════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn

    print("\n+--------------------------------------------------------------+")
    print("|  AEOP - Adaptive Event Orchestration Platform  v2.0         |")
    print("|  Unified Server: API + WebSocket + Simulation Engine        |")
    print("|  Listening on http://0.0.0.0:8000                           |")
    print("+--------------------------------------------------------------+\n")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
