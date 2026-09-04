import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from services.common.kafka_producer import AEOKafkaProducer
from services.workers.critical_worker import CriticalWorker
from services.workers.batch_worker import BatchWorker
from services.workers.deferred_worker import DeferredWorker
from services.workers.dlq_worker import DLQWorker
from services.common.logger import get_logger

logger = get_logger("WorkerPoolService")

producer = AEOKafkaProducer()
critical_worker = CriticalWorker(producer)
batch_worker = BatchWorker(producer)
deferred_worker = DeferredWorker(producer)
dlq_worker = DLQWorker(producer)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await producer.start()
    await critical_worker.start()
    await batch_worker.start()
    await deferred_worker.start()
    await dlq_worker.start()
    logger.info("All independent worker microservices started.")
    yield
    await critical_worker.stop()
    await batch_worker.stop()
    await deferred_worker.stop()
    await dlq_worker.stop()
    await producer.stop()
    logger.info("Worker pool stopped cleanly.")


app = FastAPI(title="AEOP Worker Pool Service", lifespan=lifespan)


@app.get("/health")
async def health():
    return {
        "status": "UP",
        "service": "worker-pool",
        "criticalCount": critical_worker.processed_count,
        "batchCount": batch_worker.processed_count,
        "deferredCount": deferred_worker.processed_count,
        "dlqCount": dlq_worker.shed_count
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)
