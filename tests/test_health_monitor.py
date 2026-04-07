from types import SimpleNamespace

import pytest
import requests

import health_monitor
from health_monitor import HealthMonitor


class DummyContainer:
    def __init__(self, name, stats_payload=None):
        self.name = name
        self._stats_payload = stats_payload or {}

    def stats(self, stream=False):
        return self._stats_payload


class DummyConn:
    def __init__(self, fetchone_value=(1,)):
        self.fetchone_value = fetchone_value
        self.closed = False
        self.executed = []

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql):
        self.executed.append(sql)

    def fetchone(self):
        return self.fetchone_value

    def close(self):
        self.closed = True


@pytest.fixture
def monitor():
    client = SimpleNamespace(containers=SimpleNamespace(list=lambda: []))
    return HealthMonitor(config=SimpleNamespace(), client=client)


def test_get_health_found_and_not_found(monitor):
    payload = {
        'cpu_stats': {'cpu_usage': {'percpu_usage': [1, 2], 'total_usage': 60}, 'system_cpu_usage': 200},
        'precpu_stats': {'cpu_usage': {'total_usage': 20}, 'system_cpu_usage': 100},
    }
    found = DummyContainer('svc-a', payload)
    monitor.client.containers.list = lambda: [found]

    assert monitor.get_health('svc-a') == {'health': True, 'cpu': 80.0}
    assert monitor.get_health('svc-missing') == {'health': False, 'cpu': -1}


def test_get_cpu_zero_when_no_delta(monitor):
    container = DummyContainer('svc', {'cpu_stats': {}, 'precpu_stats': {}})
    assert monitor._get_cpu(container) == 0.0


def test_check_service_health_regular_service(monkeypatch, monitor):
    class Resp:
        status_code = 200

    monkeypatch.setattr(health_monitor.requests, 'get', lambda *a, **k: Resp())
    status = []

    monitor.check_service_health({'name': 'db-writer', 'service': 'db-writer:8033'}, status)

    assert status == [{'service': 'db-writer', 'status': True, 'msg': 200}]


def test_check_service_health_mqtt_connected_and_disconnected(monkeypatch, monitor):
    class RespTrue:
        status_code = 200
        def json(self):
            return {'mqtt_connected': True}

    class RespFalse:
        status_code = 503
        def json(self):
            return {'mqtt_connected': False}

    status = []
    monkeypatch.setattr(health_monitor.requests, 'get', lambda *a, **k: RespTrue())
    monitor.check_service_health({'name': 'mqtt-ingest', 'service': 'mqtt-ingest:8000'}, status)
    assert status[0] == {'service': 'mosquitto', 'status': 200, 'msg': 'Mosqitto online'}
    assert status[1] == {'service': 'mqtt-ingest', 'status': True, 'msg': 200}

    status = []
    monkeypatch.setattr(health_monitor.requests, 'get', lambda *a, **k: RespFalse())
    monitor.check_service_health({'name': 'mqtt-ingest', 'service': 'mqtt-ingest:8000'}, status)
    assert status[0] == {'service': 'mosquitto', 'status': 400, 'msg': 'Mosqitto offline'}
    assert status[1] == {'service': 'mqtt-ingest', 'status': False, 'msg': 503}


def test_check_service_health_connection_error(monkeypatch, monitor):
    def boom(*args, **kwargs):
        raise requests.exceptions.ConnectionError('down')

    monkeypatch.setattr(health_monitor.requests, 'get', boom)
    status = []
    monitor.check_service_health({'name': 'api-rule', 'service': 'rule-api:8013'}, status)
    assert status == [{'service': 'api-rule', 'status': False, 'msg': 'Health not responding'}]


def test_check_postgres_success_and_unexpected(monkeypatch, monitor):
    conn = DummyConn((1,))
    monkeypatch.setattr(health_monitor.psycopg2, 'connect', lambda dns: conn)

    ok, msg = monitor.check_postgres('dsn')
    assert (ok, msg) == (True, 'Postgres is healthy')
    assert conn.closed is True
    assert conn.executed == ['SELECT 1;']

    conn2 = DummyConn((2,))
    monkeypatch.setattr(health_monitor.psycopg2, 'connect', lambda dns: conn2)
    ok, msg = monitor.check_postgres('dsn')
    assert (ok, msg) == (False, 'Postgres returned unexpected result')
    assert conn2.closed is True


def test_check_postgres_operational_and_generic_errors(monkeypatch, monitor):
    op_err = health_monitor.OperationalError('bad conn')
    monkeypatch.setattr(health_monitor.psycopg2, 'connect', lambda dns: (_ for _ in ()).throw(op_err))
    ok, msg = monitor.check_postgres('dsn')
    assert ok is False
    assert 'Postgres connection failed' in msg

    monkeypatch.setattr(health_monitor.psycopg2, 'connect', lambda dns: (_ for _ in ()).throw(RuntimeError('boom')))
    ok, msg = monitor.check_postgres('dsn')
    assert ok is False
    assert 'Postgres healthcheck error' in msg
