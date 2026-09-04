from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from services.common.models import SimulationConfigModel
from services.common.kafka_producer import AEOKafkaProducer
from services.simulator.generator import TrafficGenerator
from services.simulator.config import load_traffic_profiles
from services.common.logger import get_logger

logger = get_logger("SimulatorService")

producer = AEOKafkaProducer()
generator = TrafficGenerator(producer)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await producer.start()
    logger.info("Traffic Simulator Service started.")
    yield
    await generator.stop()
    await producer.stop()
    logger.info("Traffic Simulator Service stopped.")


app = FastAPI(title="AEOP Traffic Simulator Service", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    return {"status": "UP", "service": "traffic-simulator"}


@app.get("/api/simulation/scenarios")
async def get_scenarios():
    return load_traffic_profiles()


@app.get("/api/simulation/status")
async def get_status():
    return {
        "isRunning": generator.is_running,
        "activeScenario": generator.config.scenario if generator.config else None,
        "currentEps": round(generator.current_eps, 2),
        "totalGenerated": generator.total_generated
    }


@app.post("/api/simulation/start")
async def start_simulation(config: SimulationConfigModel):
    scenarios = load_traffic_profiles()
    if config.scenario in scenarios:
        profile = scenarios[config.scenario]
        config.baseEps = config.baseEps or profile.get("baseEps", 50)
        config.spikeMultiplier = profile.get("spikeMultiplier", 1.0)
        config.rampUpSeconds = profile.get("rampUpSeconds", 0)
        config.peakDurationSeconds = profile.get("peakDurationSeconds", 60)
        config.rampDownSeconds = profile.get("rampDownSeconds", 0)
        if not config.eventDistribution:
            config.eventDistribution = profile.get("eventDistribution", {})

    await generator.start(config)
    return {
        "status": "SUCCESS",
        "message": f"Simulation scenario '{config.scenario}' started.",
        "config": config
    }


@app.post("/api/simulation/stop")
async def stop_simulation():
    await generator.stop()
    return {
        "status": "STOPPED",
        "totalGenerated": generator.total_generated
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
