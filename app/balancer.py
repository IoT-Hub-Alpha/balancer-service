from time import monotonic, time
from services.config import Config, Services
from health_monitor import HealthMonitor
from kafka_monitor import KafkaMonitor
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)

class Balancer():
    def __init__(self, health_monitor: HealthMonitor, kafka_monitor: KafkaMonitor, config: Config, client):
        self.health = health_monitor
        self.kafka = kafka_monitor
        self.config = config
        self.client = client
        self._is_running = True
        self._last_check = monotonic()
        self.logger = logging.getLogger(__name__)
        
    def run(self):
        while self._is_running:
            
            if monotonic() - self._last_check > self.config.check_time:
                for service in self.config.services:
                    health = self.health.get_health(service.service)
                    kafka = self.kafka.get_group_lag(
                    group_id=service.group_id,
                    topic=service.topic,
                )
                    self._check_all(health, kafka, service)
                self._last_check = monotonic()

    def start_new_instance(self, service: Services):
        source = self.client.containers.get(service.service)
        attrs = source.attrs

        image = attrs["Config"]["Image"]
        command = attrs["Config"]["Cmd"]
        env = attrs["Config"]["Env"]

        networks = attrs["NetworkSettings"]["Networks"]
        network_name = list(networks.keys())[0]

        ports = None

        binds = attrs["HostConfig"].get("Binds", [])

        volumes = {}
        if binds:
            for bind in binds:
                parts = bind.split(":")
                host_path = parts[0]
                container_path = parts[1]
                mode = parts[2] if len(parts) > 2 else "rw"

                volumes[host_path] = {
                    "bind": container_path,
                    "mode": mode,
                }

        restart_policy = attrs["HostConfig"].get("RestartPolicy", {})

        new_name = f"{service.service}_clone_{int(time())}"

        container = self.client.containers.run(
            image=image,
            command=command,
            environment=env,
            name=new_name,
            detach=True,
            network=network_name,
            volumes=volumes if volumes else None,
            restart_policy=restart_policy,
            ports=ports,
        )
        service.containers_num += 1
        self.logger.info(f"Started clone: {container.name}")
             
    def _check_all(self, health, kafka, service: Services) -> None:
        current_time = monotonic()
        service.lag = kafka.get("total_lag", 0) if kafka else 0
        service.cpu_usage = health['cpu'] if health else 0
        last_trigger = monotonic() if not service.last_trigger else service.last_trigger
        if current_time - last_trigger > self.config.load_time_till_start_s:
            if service.containers_num < self.config.max_containers:
                if current_time - service.cooldown > self.config.start_up_interval_s:
                    self.logger.info(f"{service.service} exceeded timer, starting new service to compensate....")
                    self.start_new_instance(service)
                    service.cooldown = current_time
                    service.last_trigger = current_time
                    return
            
        if health and health['cpu'] > self.config.cpu_threshold:
            self.logger.info(f"{service.service} exceeded cpu threshold!")
            return
        
        if kafka and kafka.get("total_lag", 0) > self.config.lag_threshold:
            self.logger.info(f"{service.service} exceeded kafka lag threshold!")
            return
        
        self.logger.info(f"No triggers: {kafka} ==== {health}")
        service.last_trigger = current_time
            