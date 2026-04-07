from types import SimpleNamespace

import balancer as balancer_module
from balancer import Balancer
from services.config import Config, Services


def make_config(service=None):
    return Config(
        services=[service or Services(service='svc-a', group_id='group-a', topic='topic-a')],
        cpu_threshold=70,
        max_containers=3,
        load_time_till_start_s=10,
        start_up_interval_s=5,
        check_time=2,
        lag_threshold=11,
        dbs={'db': 'dsn'},
    )


def test_start_new_instance_copies_container_config(monkeypatch):
    service = Services(service='svc-a', group_id='group-a', topic='topic-a')
    config = make_config(service)

    source = SimpleNamespace(attrs={
        'Config': {'Image': 'img:1', 'Cmd': ['python', 'app.py'], 'Env': ['A=1']},
        'NetworkSettings': {'Networks': {'mynet': {}}},
        'HostConfig': {'Binds': ['/host:/container:ro'], 'RestartPolicy': {'Name': 'always'}},
    })

    run_calls = {}

    class ContainersAPI:
        def get(self, name):
            assert name == 'svc-a'
            return source

        def run(self, **kwargs):
            run_calls.update(kwargs)
            return SimpleNamespace(name=kwargs['name'])

    client = SimpleNamespace(containers=ContainersAPI())
    bal = Balancer(SimpleNamespace(), SimpleNamespace(), config, client)
    monkeypatch.setattr(balancer_module, 'time', lambda: 1234.9)

    bal.start_new_instance(service)

    assert service.containers_num == 2
    assert run_calls['image'] == 'img:1'
    assert run_calls['command'] == ['python', 'app.py']
    assert run_calls['environment'] == ['A=1']
    assert run_calls['network'] == 'mynet'
    assert run_calls['restart_policy'] == {'Name': 'always'}
    assert run_calls['volumes'] == {'/host': {'bind': '/container', 'mode': 'ro'}}
    assert run_calls['name'] == 'svc-a_clone_1234'


def test_check_all_starts_instance_when_timer_and_cooldown_allow(monkeypatch):
    service = Services(service='svc-a', group_id='group-a', topic='topic-a', last_trigger=1, cooldown=0)
    config = make_config(service)
    bal = Balancer(SimpleNamespace(), SimpleNamespace(), config, client=SimpleNamespace())
    started = []
    bal.start_new_instance = lambda svc: started.append(svc.service)
    monkeypatch.setattr(balancer_module, 'monotonic', lambda: 20)

    bal._check_all({'cpu': 12}, {'total_lag': 3}, service)

    assert started == ['svc-a']
    assert service.cooldown == 20
    assert service.last_trigger == 20
    assert service.lag == 3
    assert service.cpu_usage == 12


def test_check_all_cpu_lag_and_no_trigger_paths(monkeypatch):
    service = Services(service='svc-a', group_id='group-a', topic='topic-a', last_trigger=19, cooldown=0)
    config = make_config(service)
    bal = Balancer(SimpleNamespace(), SimpleNamespace(), config, client=SimpleNamespace())
    monkeypatch.setattr(balancer_module, 'monotonic', lambda: 20)

    bal._check_all({'cpu': 80}, {'total_lag': 3}, service)
    assert service.last_trigger == 19  # unchanged on early return

    bal._check_all({'cpu': 12}, {'total_lag': 20}, service)
    assert service.last_trigger == 19

    bal._check_all({'cpu': 12}, {'total_lag': 3}, service)
    assert service.last_trigger == 20


def test_run_checks_services_when_interval_elapsed(monkeypatch):
    service = Services(service='svc-a', group_id='group-a', topic='topic-a')
    config = make_config(service)
    health = SimpleNamespace(get_health=lambda name: {'cpu': 1})
    kafka = SimpleNamespace(get_group_lag=lambda group_id, topic: {'total_lag': 2})
    bal = Balancer(health, kafka, config, client=SimpleNamespace())
    bal._last_check = 0

    values = iter([5, 6, 7])
    monkeypatch.setattr(balancer_module, 'monotonic', lambda: next(values))

    called = []
    def fake_check_all(health_data, kafka_data, svc):
        called.append((health_data, kafka_data, svc.service))
        bal._is_running = False

    bal._check_all = fake_check_all
    bal.run()

    assert called == [({'cpu': 1}, {'total_lag': 2}, 'svc-a')]
