import threading
import uvicorn
import docker
from fastapi import FastAPI
from contextlib import asynccontextmanager
from services.config import Config
from health_monitor import HealthMonitor
from kafka_monitor import KafkaMonitor
from fastapi.middleware.cors import CORSMiddleware
from balancer import Balancer
from services.helpers import load_json_services


bootstrap_server = Config.construct_kafka_service()
kafka_monitor = KafkaMonitor(bootstrap_server)
config = Config.constuct_config()
client = docker.DockerClient(base_url="unix:///var/run/docker.sock")
health_monitor = HealthMonitor(config, client)
balancer = Balancer(health_monitor, kafka_monitor, config, client)
service_list = load_json_services()

@asynccontextmanager
async def lifespan(app: FastAPI):
    thread = threading.Thread(target=balancer.run, daemon=True)
    thread.start()
    yield


app = FastAPI(lifespan=lifespan)

# allow everyone, since its dev, why the hell not, right???
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    status = []
    for db, dns in config.dbs.items():
        result = health_monitor.check_postgres(dns)
        status.append({"service": db, "status": result[0], "msg": result[1]})
    
    kafka = kafka_monitor.check_kafka()
    status.append({"service": "kafka", "status": kafka[0], "msg": kafka[1]})
    
    for service in service_list:
        health_monitor.check_service_health(service, status)
    
    return status

@app.get("/add_container")
async def add_container(service_name: str | None = None):
    if not service_name:
        return {"msg": "Must have container name"}
    
    service = [x for x in config.services if x.service == service_name]
    if not service:
        return {"msg": "Service not found"}
    
    balancer.start_new_instance(service[0])
    return {"msg": f"container {service_name} started"}

@app.get("/status")
def status():
    result = [{"service": service.service, "containers": service.containers_num, "total_lag": service.lag, "cpu_usage": service.cpu_usage} for service in config.services]
    return result


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8055)