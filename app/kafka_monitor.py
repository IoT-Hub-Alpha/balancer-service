from confluent_kafka import Consumer, TopicPartition, OFFSET_INVALID, Producer, KafkaException, KafkaError
import logging
from typing import Tuple
from time import sleep

logging.basicConfig(
    level=logging.INFO,  # 👈 THIS is the important part
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)

class KafkaMonitor:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.max_attempts = 5
        self.logger = logging.getLogger(__name__)

    def get_topic_size(self, topic: str, consumer) -> int:

        md = consumer.list_topics(topic, timeout=10)
        partitions = md.topics[topic].partitions.keys()

        total = 0

        for p in partitions:
            tp = TopicPartition(topic, p)
            low, high = consumer.get_watermark_offsets(tp, timeout=10)
            total += high  # 👈 this is "end offset"

        return total

    def calculate_validator_est_backlog(self, consumer):
        raw_total = self.get_topic_size("telemetry.raw", consumer)
        clean_total = self.get_topic_size("telemetry.clean", consumer)
        dlq_total = self.get_topic_size("telemetry.dlq", consumer)

        validator_backlog = raw_total - (clean_total + dlq_total)
        validator_backlog = max(0, validator_backlog)
        
        return validator_backlog

    def check_kafka(self) -> Tuple[bool, str]:
        try:
            producer = Producer(
                {
                    "bootstrap.servers": self.bootstrap_servers,
                    "socket.timeout.ms": 3000,
                    "message.timeout.ms": 3000,
                }
            )

            metadata = producer.list_topics(timeout=3)

            if metadata.brokers:
                return True, f"Kafka is healthy, brokers found: {len(metadata.brokers)}"
            return False, "Kafka metadata returned no brokers"

        except KafkaException as e:
            return False, f"Kafka connection failed: {e}"
        except Exception as e:
            return False, f"Kafka healthcheck error: {e}"

    def get_group_lag(self, group_id: str, topic: str, attempts=0) -> dict:
        try:
            consumer = Consumer({
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": f"{group_id}-lag-monitor",
                "enable.auto.commit": False,
            })
            
            md = consumer.list_topics(topic=topic, timeout=10)
            topic_md = md.topics.get(topic)

            if topic_md is None:
                raise RuntimeError(f"Topic {topic!r} not found in metadata")

            if getattr(topic_md, "error", None) is not None and topic_md.error is not None:
                raise RuntimeError(f"Topic metadata error for {topic}: {topic_md.error}")

            partitions = sorted(topic_md.partitions.keys())
            tps = [TopicPartition(topic, p) for p in partitions]

            committed = consumer.committed(tps, timeout=10)

            results = []
            total_lag = 0

            for tp in committed:
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(tp.topic, tp.partition),
                    timeout=10,
                    cached=False,
                )

                committed_offset = tp.offset
                if tp.offset == OFFSET_INVALID:
                    lag = None
                else:
                    lag = high - tp.offset
                    total_lag += lag



                results.append({
                    "topic": tp.topic,
                    "partition": tp.partition,
                    "committed_offset": committed_offset,
                    "log_end_offset": high,
                    "lag": lag,
                })

            
            if "validator" in group_id:
                total_lag = self.calculate_validator_est_backlog(consumer)
                
            return {
                "group_id": group_id,
                "topic": topic,
                "total_lag": total_lag,
                "partitions": results,
            }

        except (RuntimeError, KafkaException) as e:
            if attempts > self.max_attempts:
                self.logger.error("Max attempts excceeded, stopping run")
                raise RuntimeError("Excedded max attempts")
            
            self.logger.error(f"Attempt {attempts} failed, retrying in {10*(1+attempts)} seconds")
            sleep(10*(1*attempts))
            self.get_group_lag(group_id, topic, attempts = attempts + 1)
            
        
        finally:
            consumer.close()