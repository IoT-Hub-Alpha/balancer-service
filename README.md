# Balancer Service

## Overview

The **Balancer Service** is a FastAPI-based service responsible for:

- Monitoring service health (CPU, HTTP health endpoints)
- Monitoring Kafka consumer lag
- Dynamically scaling Docker containers based on load
- Providing system-wide health and status endpoints

It is part of the **IoT Hub Alpha** microservices architecture.

---

## Features

- 🔍 Health monitoring for services via HTTP and Docker stats
- 📊 Kafka lag monitoring per consumer group
- ⚖️ Dynamic scaling (spin up new containers)
- 🐳 Docker integration (clone running containers)
- 🧠 Config-driven behavior via environment variables
- 🚀 FastAPI endpoints for observability and control

---

## Project Structure

app/
├── main.py
├── balancer.py
├── health_monitor.py
├── kafka_monitor.py
└── services/
    ├── config.py
    ├── helpers.py
    └── service_list.json

---

## Endpoints

### GET /health
Returns health status of PostgreSQL, Kafka, and services.

### GET /status
Returns current balancing state.

### GET /add_container?service_name=...
Manually start a new container instance.

---

## Configuration

Environment variables control behavior:

- BALANCER_SERVICES
- BALANCER_GROUPS
- BALANCER_TOPICS
- BALANCER_CPU_THRESHOLD
- BALANCER_LAG_THRESHOLD
- BALANCER_MAX_CONTAINERS
- BALANCER_CHECK_TIME_S
- BALANCER_LOAD_TIME_TILL_START_S
- BALANCER_START_UP_INTERVAL

---

## Running Locally

pip install -r requirements.txt
python -m main

---

## Docker

docker build -t balancer-service .
docker run -p 8055:8055 balancer-service

---

## Testing

pytest --cov=app --cov-report=term-missing

---

## Notes

- Kafka retry logic may return None on retries
- Scaling assumes container cloning works
- Internal service usage (no auth)

---

## License

MIT
