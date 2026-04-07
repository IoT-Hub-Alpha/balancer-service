from services.config import Config
from time import sleep
import logging
import requests

from typing import Tuple

import psycopg2
from psycopg2 import OperationalError
from confluent_kafka import Producer, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)

class HealthMonitor:
    def __init__(self, config: Config, client):
        self.client = client
        self.config = config
        self.logging = logging.getLogger(__name__)
        
    def get_health(self, container_name):
        for container in self.client.containers.list():
            if container_name == container.name:
                stats = self._get_cpu(container)
                return {"health": True, "cpu": stats}
        self.logging.warning(f"{container_name} not found")
        return {"health": False, "cpu": -1}
    
    def _get_cpu(self, container):
        stats = container.stats(stream=False)
        cpu_stats = stats.get("cpu_stats") or {}
        precpu_stats = stats.get("precpu_stats") or {}
        cpu_usage = cpu_stats.get("cpu_usage") or {}
        precpu_usage = precpu_stats.get("cpu_usage") or {}
        
        percpu_usage = cpu_usage.get("percpu_usage") or []
        cpu_count = len(percpu_usage) if percpu_usage else 1
        cpu_total = cpu_usage.get("total_usage", 0) or 0
        precpu_total = precpu_usage.get("total_usage", 0) or 0
        system_total = cpu_stats.get("system_cpu_usage", 0) or 0
        presystem_total = precpu_stats.get("system_cpu_usage", 0) or 0
        
        cpu_percent = 0.0
        cpu_delta = cpu_total - precpu_total
        system_delta = system_total - presystem_total
        if system_delta > 0 and cpu_delta > 0:
            cpu_percent = (cpu_delta / system_delta) * cpu_count * 100.0
            
        return round(cpu_percent, 2)
    
    def check_service_health(self, service, status) -> None:
        try:
            response = requests.get(
            f"http://{service['service']}/health",
                headers={"X-Internal-Service": "balancer"}
            )
            if service['name'] == "mqtt-ingest":
                data = response.json()
                if data.get('mqtt_connected'):
                    status.append({"service": "mosquitto", "status": 200 , "msg": "Mosqitto online"})
                else:
                    status.append({"service": "mosquitto", "status": 400 , "msg": "Mosqitto offline"})
            status.append({"service": service['name'], "status": response.status_code == 200, "msg": response.status_code})
        except requests.exceptions.ConnectionError as e:
            status.append({"service": service['name'], "status": False, "msg": 'Health not responding'})
    
    def check_postgres(self, dns) -> Tuple[bool, str]:
        conn = None
        try:
            conn = psycopg2.connect(dns)
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                result = cur.fetchone()

            if result and result[0] == 1:
                return True, "Postgres is healthy"
            return False, "Postgres returned unexpected result"

        except OperationalError as e:
            return False, f"Postgres connection failed: {e}"
        except Exception as e:
            return False, f"Postgres healthcheck error: {e}"
        finally:
            if conn is not None:
                conn.close()