# Containerization & Docker Setup Guide

## 1. Multi-Container Services Architecture

The AEOP environment consists of 8 interconnected container services orchestrated via Docker Compose:

1. **`zookeeper`**: Coordinates Kafka cluster metadata.
2. **`kafka`**: Apache Kafka message broker handling streaming topics.
3. **`simulator`**: Python AsyncIO Traffic Generator.
4. **`haeo`**: Hybrid Adaptive Event Orchestrator engine.
5. **`workers`**: Multi-instance container pool running `critical_worker`, `batch_worker`, `deferred_worker`, and `dlq_worker`.
6. **`metrics-service`**: Telemetry aggregator and FastAPI WebSocket server.
7. **`frontend`**: React Vite application served via Nginx.
8. **`kafka-ui`**: Web UI monitoring Kafka topics, consumer groups, and messages.

---

## 2. Docker Compose Infrastructure File (`docker-compose.yml`)

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: aeop-zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: aeop-kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: aeop-kafka-ui
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092

  metrics-service:
    build:
      context: .
      dockerfile: services/metrics_service/Dockerfile
    container_name: aeop-metrics-service
    ports:
      - "8000:8000"
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
    depends_on:
      - kafka

  simulator:
    build:
      context: .
      dockerfile: services/simulator/Dockerfile
    container_name: aeop-simulator
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
    depends_on:
      - kafka

  haeo:
    build:
      context: .
      dockerfile: services/haeo/Dockerfile
    container_name: aeop-haeo
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
    depends_on:
      - kafka

  workers:
    build:
      context: .
      dockerfile: services/workers/Dockerfile
    container_name: aeop-workers
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
    depends_on:
      - kafka

  frontend:
    build:
      context: ./frontend
      dockerfile: Dockerfile
    container_name: aeop-frontend
    ports:
      - "3000:80"
    depends_on:
      - metrics-service
```

---

## 3. Launch & Inspection Commands

```bash
# Build and run containers in detached mode
docker-compose up --build -d

# View service logs in real time
docker-compose logs -f haeo metrics-service

# Check health status of all containers
docker-compose ps

# Stop all containers
docker-compose down -v
```

---
