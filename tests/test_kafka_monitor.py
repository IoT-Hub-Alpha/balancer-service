from types import SimpleNamespace

import pytest

import kafka_monitor
from kafka_monitor import KafkaMonitor


class FakeConsumer:
    def __init__(self, config=None, metadata=None, committed=None, watermark_map=None):
        self.config = config or {}
        self.metadata = metadata
        self._committed = committed or []
        self.watermark_map = watermark_map or {}
        self.closed = False

    def list_topics(self, topic=None, timeout=10):
        return self.metadata

    def committed(self, tps, timeout=10):
        return self._committed

    def get_watermark_offsets(self, tp, timeout=10, cached=False):
        return self.watermark_map[(tp.topic, tp.partition)]

    def close(self):
        self.closed = True


class FakeProducer:
    def __init__(self, metadata=None, exc=None, *args, **kwargs):
        self.metadata = metadata
        self.exc = exc

    def list_topics(self, timeout=3):
        if self.exc:
            raise self.exc
        return self.metadata


class TopicMeta:
    def __init__(self, partitions, error=None):
        self.partitions = partitions
        self.error = error


def test_get_topic_size_and_backlog():
    monitor = KafkaMonitor('broker:9092')
    md = SimpleNamespace(topics={'telemetry.raw': SimpleNamespace(partitions={0: object(), 1: object()})})
    consumer = FakeConsumer(metadata=md, watermark_map={('telemetry.raw', 0): (0, 5), ('telemetry.raw', 1): (0, 7)})

    assert monitor.get_topic_size('telemetry.raw', consumer) == 12

    monitor.get_topic_size = lambda topic, consumer: {'telemetry.raw': 20, 'telemetry.clean': 7, 'telemetry.dlq': 4}[topic]
    assert monitor.calculate_validator_est_backlog(object()) == 9

    monitor.get_topic_size = lambda topic, consumer: {'telemetry.raw': 3, 'telemetry.clean': 7, 'telemetry.dlq': 4}[topic]
    assert monitor.calculate_validator_est_backlog(object()) == 0


def test_check_kafka_success_no_brokers_and_exceptions(monkeypatch):
    monitor = KafkaMonitor('broker:9092')

    monkeypatch.setattr(kafka_monitor, 'Producer', lambda conf: FakeProducer(metadata=SimpleNamespace(brokers={1: object(), 2: object()})))
    ok, msg = monitor.check_kafka()
    assert ok is True
    assert 'brokers found: 2' in msg

    monkeypatch.setattr(kafka_monitor, 'Producer', lambda conf: FakeProducer(metadata=SimpleNamespace(brokers={})))
    assert monitor.check_kafka() == (False, 'Kafka metadata returned no brokers')

    monkeypatch.setattr(kafka_monitor, 'Producer', lambda conf: FakeProducer(exc=kafka_monitor.KafkaException('kapow')))
    ok, msg = monitor.check_kafka()
    assert ok is False and 'Kafka connection failed' in msg

    monkeypatch.setattr(kafka_monitor, 'Producer', lambda conf: FakeProducer(exc=RuntimeError('boom')))
    ok, msg = monitor.check_kafka()
    assert ok is False and 'Kafka healthcheck error' in msg


def test_get_group_lag_regular_and_validator(monkeypatch):
    monitor = KafkaMonitor('broker:9092')
    topic_md = TopicMeta(partitions={0: object(), 1: object()})
    metadata = SimpleNamespace(topics={'topic-a': topic_md})
    committed = [
        SimpleNamespace(topic='topic-a', partition=0, offset=3),
        SimpleNamespace(topic='topic-a', partition=1, offset=kafka_monitor.OFFSET_INVALID),
    ]
    watermark_map = {('topic-a', 0): (0, 10), ('topic-a', 1): (0, 9)}

    created = []

    def fake_consumer(conf):
        c = FakeConsumer(conf, metadata=metadata, committed=committed, watermark_map=watermark_map)
        created.append(c)
        return c

    monkeypatch.setattr(kafka_monitor, 'Consumer', fake_consumer)
    result = monitor.get_group_lag('db-reader', 'topic-a')
    assert result['group_id'] == 'db-reader'
    assert result['total_lag'] == 7
    assert result['partitions'][1]['lag'] is None
    assert created[-1].closed is True

    monitor.calculate_validator_est_backlog = lambda consumer: 42
    created.clear()
    result = monitor.get_group_lag('telemetry-validator', 'topic-a')
    assert result['total_lag'] == 42
    assert created[-1].closed is True


def test_get_group_lag_raises_after_max_attempts(monkeypatch):
    monitor = KafkaMonitor('broker:9092')
    monitor.max_attempts = 0
    metadata = SimpleNamespace(topics={})

    created = []

    def fake_consumer(conf):
        c = FakeConsumer(conf, metadata=metadata)
        created.append(c)
        return c

    monkeypatch.setattr(kafka_monitor, 'Consumer', fake_consumer)
    monkeypatch.setattr(kafka_monitor, 'sleep', lambda *_: None)

    with pytest.raises(RuntimeError, match='Excedded max attempts'):
        monitor.get_group_lag('db-reader', 'missing-topic', attempts=1)

    assert created[-1].closed is True
