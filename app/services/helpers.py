import os
from dotenv import load_dotenv
import json
from pathlib import Path

load_dotenv()

def construct_posgres_dns():
    DB_NAME = str(os.getenv("DB_NAME", "iot_hub_alpha_db"))
    DB_USER = str(os.getenv("DB_USER", "postgres"))
    DB_PASSWORD = str(os.getenv("DB_PASSWORD", "postgres"))
    DB_HOST = str(os.getenv("DB_HOST", "db"))
    DB_PORT = str(os.getenv("DB_PORT", 5432))
    DB_CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT", 10))
    
    TELEMETRY_DB_NAME = str(os.getenv("TELEMETRY_DB_NAME", "iot_hub_telemetry_db"))
    TELEMETRY_DB_USER = str(os.getenv("TELEMETRY_DB_USER", "postgres"))
    TELEMETRY_DB_PASSWORD = str(os.getenv("TELEMETRY_DB_PASSWORD", "postgres"))
    TELEMETRY_DB_HOST = str(os.getenv("TELEMETRY_DB_HOST", "telemetry-db"))
    TELEMETRY_DB_PORT = str(os.getenv("TELEMETRY_DB_PORT", 5432))
    TELEMETRY_DB_CONNECT_TIMEOUT = int(os.getenv("TELEMETRY_DB_CONNECT_TIMEOUT", 10))
    
    POSTGRES_DSN = f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT} connect_timeout={DB_CONNECT_TIMEOUT}"
    TELEMETRY_POSTGRES_DNS = f"dbname={TELEMETRY_DB_NAME} user={TELEMETRY_DB_USER} password={TELEMETRY_DB_PASSWORD} host={TELEMETRY_DB_HOST} port={TELEMETRY_DB_PORT} connect_timeout={TELEMETRY_DB_CONNECT_TIMEOUT}"
    
    return {
        "db": POSTGRES_DSN,
        "telem-db": TELEMETRY_POSTGRES_DNS
    }
    
def load_json_services():
    BASE_DIR = Path(__file__).resolve().parent
    file_path = BASE_DIR / "service_list.json"
    
    with open(file_path) as f:
        data = json.load(f)
    return data