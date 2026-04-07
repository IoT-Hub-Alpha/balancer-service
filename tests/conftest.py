import importlib
import importlib.util
import sys
import types
from pathlib import Path

import pytest

ROOT_DIR = Path(__file__).resolve().parents[1]
APP_DIR = ROOT_DIR / "app"

if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

# Stub external deps that are not installed in the execution environment.
if 'confluent_kafka' not in sys.modules:
    ck = types.ModuleType('confluent_kafka')

    class KafkaException(Exception):
        pass

    class KafkaError(Exception):
        pass

    class TopicPartition:
        def __init__(self, topic, partition, offset=None):
            self.topic = topic
            self.partition = partition
            self.offset = offset

    class Producer:
        def __init__(self, *args, **kwargs):
            pass

        def list_topics(self, timeout=None):
            return types.SimpleNamespace(brokers={1: object()})

    class Consumer:
        def __init__(self, *args, **kwargs):
            pass

    ck.Consumer = Consumer
    ck.TopicPartition = TopicPartition
    ck.OFFSET_INVALID = -1001
    ck.Producer = Producer
    ck.KafkaException = KafkaException
    ck.KafkaError = KafkaError
    sys.modules['confluent_kafka'] = ck

if 'psycopg2' not in sys.modules:
    psy = types.ModuleType('psycopg2')

    class OperationalError(Exception):
        pass

    def connect(*args, **kwargs):
        raise OperationalError('stub connect called unexpectedly')

    psy.OperationalError = OperationalError
    psy.connect = connect
    sys.modules['psycopg2'] = psy

if 'docker' not in sys.modules:
    docker_mod = types.ModuleType('docker')

    class DockerClient:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    docker_mod.DockerClient = DockerClient
    sys.modules['docker'] = docker_mod

# Create/load a synthetic "services" package so imports like
# "from services.config import Config" work with the uploaded flat files.
if 'services' not in sys.modules:
    pkg = types.ModuleType('services')
    pkg.__path__ = [str(APP_DIR)]
    sys.modules['services'] = pkg


def _load_services_submodule(name: str, filename: str):
    full_name = f'services.{name}'
    if full_name in sys.modules:
        return sys.modules[full_name]
    spec = importlib.util.spec_from_file_location(full_name, APP_DIR / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[full_name] = module
    spec.loader.exec_module(module)
    setattr(sys.modules['services'], name, module)
    return module


_load_services_submodule('helpers', 'services/helpers.py')
_load_services_submodule('config', 'services/config.py')


@pytest.fixture
def reload_module():
    def _reload(name: str):
        if name in sys.modules:
            return importlib.reload(sys.modules[name])
        return importlib.import_module(name)
    return _reload
