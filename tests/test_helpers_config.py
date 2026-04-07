import os

import pytest

from services.config import Config, ConfigError
from services import helpers


def test_construct_postgres_dns_defaults(monkeypatch):
    for key in [
        'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT', 'DB_CONNECT_TIMEOUT',
        'TELEMETRY_DB_NAME', 'TELEMETRY_DB_USER', 'TELEMETRY_DB_PASSWORD',
        'TELEMETRY_DB_HOST', 'TELEMETRY_DB_PORT', 'TELEMETRY_DB_CONNECT_TIMEOUT',
    ]:
        monkeypatch.delenv(key, raising=False)

    dns = helpers.construct_posgres_dns()

    assert 'dbname=iot_hub_alpha_db' in dns['db']
    assert 'host=db' in dns['db']
    assert 'dbname=iot_hub_telemetry_db' in dns['telem-db']
    assert 'host=telemetry-db' in dns['telem-db']


def test_construct_postgres_dns_env_override(monkeypatch):
    monkeypatch.setenv('DB_NAME', 'main_db')
    monkeypatch.setenv('DB_USER', 'alice')
    monkeypatch.setenv('DB_PASSWORD', 'secret')
    monkeypatch.setenv('DB_HOST', 'postgres-host')
    monkeypatch.setenv('DB_PORT', '5439')
    monkeypatch.setenv('DB_CONNECT_TIMEOUT', '22')

    dns = helpers.construct_posgres_dns()

    assert dns['db'] == (
        'dbname=main_db user=alice password=secret '
        'host=postgres-host port=5439 connect_timeout=22'
    )


def test_load_json_services_reads_uploaded_file():
    data = helpers.load_json_services()
    assert isinstance(data, list)
    assert data[0]['name'] == 'mqtt-ingest'
    assert any(item['name'] == 'http-ingest' for item in data)


def test_construct_kafka_service_default_and_override(monkeypatch):
    monkeypatch.delenv('KAFKA_BOOTSTRAP_SERVERS', raising=False)
    assert Config.construct_kafka_service() == 'kafka:9092'

    monkeypatch.setenv('KAFKA_BOOTSTRAP_SERVERS', 'broker:29092')
    assert Config.construct_kafka_service() == 'broker:29092'


def test_construct_config_happy_path(monkeypatch):
    monkeypatch.setenv('BALANCER_SERVICES', 'svc-a,svc-b')
    monkeypatch.setenv('BALANCER_GROUPS', 'group-a,group-b')
    monkeypatch.setenv('BALANCER_TOPICS', 'topic-a,topic-b')
    monkeypatch.setenv('BALANCER_LOAD_TIME_TILL_START_S', '15')
    monkeypatch.setenv('BALANCER_START_UP_INTERVAL', '12')
    monkeypatch.setenv('BALANCER_MAX_CONTAINERS', '5')
    monkeypatch.setenv('BALANCER_CPU_THRESHOLD', '77')
    monkeypatch.setenv('BALANCER_CHECK_TIME_S', '4')
    monkeypatch.setenv('BALANCER_LAG_THRESHOLD', '99')
    import services.config as svc_config
    monkeypatch.setattr(svc_config, 'construct_posgres_dns', lambda: {'db': 'dsn'})

    config = Config.constuct_config()

    assert [svc.service for svc in config.services] == ['svc-a', 'svc-b']
    assert config.max_containers == 5
    assert config.cpu_threshold == 77
    assert config.dbs == {'db': 'dsn'}


def test_construct_config_bad_numeric_env_raises(monkeypatch):
    monkeypatch.setenv('BALANCER_SERVICES', 'svc-a')
    monkeypatch.setenv('BALANCER_GROUPS', 'group-a')
    monkeypatch.setenv('BALANCER_TOPICS', 'topic-a')
    monkeypatch.setenv('BALANCER_MAX_CONTAINERS', 'not-an-int')

    with pytest.raises(ConfigError):
        Config.constuct_config()
