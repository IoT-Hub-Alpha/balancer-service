import os
from pydantic import BaseModel, ValidationError
from dotenv import load_dotenv
from logging import getLogger
from time import monotonic
from .helpers import construct_posgres_dns

load_dotenv()
logging = getLogger(__name__)

class ConfigError(Exception):
    pass

class Services(BaseModel):
    service: str
    group_id: str
    topic: str
    containers_num: int = 1
    last_trigger: float | None = None
    lag: int = 0
    cpu_usage: float = 0.0
    cooldown: float = 0

class Config(BaseModel):
    services: list[Services]
    cpu_threshold: int
    max_containers: int
    load_time_till_start_s: float
    start_up_interval_s: float
    check_time: int
    lag_threshold: int
    dbs: dict[str, str]
    
    @staticmethod
    def construct_kafka_service():
        bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        return bootstrap_server
    
    @staticmethod
    def constuct_config():
        
        try:
            service_names = os.getenv("BALANCER_SERVICES", "").split(",")
            service_group = os.getenv("BALANCER_GROUPS", "").split(",")
            service_topic = os.getenv("BALANCER_TOPICS", "").split(",")
            load_time_till_start_s = float(os.getenv("BALANCER_LOAD_TIME_TILL_START_S", 20))
            start_up_interval_s = float(os.getenv("BALANCER_START_UP_INTERVAL", 20))
            max_containers = int(os.getenv("BALANCER_MAX_CONTAINERS", 3))
            cpu_threshold = int(os.getenv("BALANCER_CPU_THRESHOLD", 70))
            check_time = int(os.getenv("BALANCER_CHECK_TIME_S", 10))
            lag_threshold = int(os.getenv("BALANCER_LAG_THRESHOLD", 10))
            dbs = construct_posgres_dns()
        except ValueError as e:
            logging.error(f"Something went wrong with env {str(e)}")
            raise ConfigError(f"Something went wrong with env {str(e)}")
        
        services = [Services(service=service_names[idx], group_id=service_group[idx], topic=service_topic[idx]) for idx, value in enumerate(service_names)]
        
        try:
            config = Config(
                services=services,
                max_containers=max_containers,
                load_time_till_start_s=load_time_till_start_s,
                start_up_interval_s=start_up_interval_s,
                cpu_threshold=cpu_threshold,
                check_time=check_time,
                lag_threshold=lag_threshold,
                dbs=dbs)
        except ValidationError as e:
            logging.error(f"Something went wrong with env {str(e)}")
            raise ConfigError(f"Something went wrong with env {str(e)}")
        
        return config