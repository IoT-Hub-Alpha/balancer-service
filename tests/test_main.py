import importlib
from types import SimpleNamespace

from fastapi.testclient import TestClient


def test_main_endpoints(monkeypatch):
    monkeypatch.setenv('BALANCER_SERVICES', 'svc-a,svc-b')
    monkeypatch.setenv('BALANCER_GROUPS', 'group-a,group-b')
    monkeypatch.setenv('BALANCER_TOPICS', 'topic-a,topic-b')
    monkeypatch.setenv('BALANCER_MAX_CONTAINERS', '3')
    monkeypatch.setenv('BALANCER_CPU_THRESHOLD', '70')
    monkeypatch.setenv('BALANCER_CHECK_TIME_S', '1')
    monkeypatch.setenv('BALANCER_LAG_THRESHOLD', '10')

    import services.config as svc_config
    monkeypatch.setattr(svc_config.Config, 'construct_kafka_service', staticmethod(lambda: 'broker:9092'))
    monkeypatch.setattr(svc_config.Config, 'constuct_config', staticmethod(lambda: svc_config.Config(
        services=[
            svc_config.Services(service='svc-a', group_id='group-a', topic='topic-a', containers_num=2, lag=8, cpu_usage=44.5),
            svc_config.Services(service='svc-b', group_id='group-b', topic='topic-b', containers_num=1, lag=0, cpu_usage=2.5),
        ],
        cpu_threshold=70,
        max_containers=3,
        load_time_till_start_s=10,
        start_up_interval_s=5,
        check_time=1,
        lag_threshold=10,
        dbs={'db': 'dsn-main', 'telem-db': 'dsn-telem'},
    )))

    import health_monitor
    import kafka_monitor
    import balancer
    import docker
    import services.helpers

    monkeypatch.setattr(health_monitor.HealthMonitor, 'check_postgres', lambda self, dsn: (True, f'ok {dsn}'))
    monkeypatch.setattr(health_monitor.HealthMonitor, 'check_service_health', lambda self, service, status: status.append({'service': service['name'], 'status': True, 'msg': 'ok'}))
    monkeypatch.setattr(kafka_monitor.KafkaMonitor, 'check_kafka', lambda self: (True, 'kafka ok'))

    started = []
    monkeypatch.setattr(balancer.Balancer, 'run', lambda self: None)
    monkeypatch.setattr(balancer.Balancer, 'start_new_instance', lambda self, service: started.append(service.service))
    monkeypatch.setattr(docker, 'DockerClient', lambda *a, **k: SimpleNamespace())
    monkeypatch.setattr(services.helpers, 'load_json_services', lambda: [{'name': 'mqtt-ingest', 'service': 'mqtt-ingest:8000'}])

    main = importlib.import_module('main')
    main = importlib.reload(main)

    with TestClient(main.app) as client:
        r = client.get('/health')
        assert r.status_code == 200
        data = r.json()
        assert {'service': 'kafka', 'status': True, 'msg': 'kafka ok'} in data
        assert {'service': 'mqtt-ingest', 'status': True, 'msg': 'ok'} in data

        r = client.get('/add_container')
        assert r.json() == {'msg': 'Must have container name'}

        r = client.get('/add_container', params={'service_name': 'missing'})
        assert r.json() == {'msg': 'Service not found'}

        r = client.get('/add_container', params={'service_name': 'svc-a'})
        assert r.json() == {'msg': 'container svc-a started'}
        assert started == ['svc-a']

        r = client.get('/status')
        assert r.json() == [
            {'service': 'svc-a', 'containers': 2, 'total_lag': 8, 'cpu_usage': 44.5},
            {'service': 'svc-b', 'containers': 1, 'total_lag': 0, 'cpu_usage': 2.5},
        ]
