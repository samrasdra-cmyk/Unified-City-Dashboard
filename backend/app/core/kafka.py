import logging

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from app.core.config import get_settings

logger = logging.getLogger("kafka")
settings = get_settings()

_producer: Producer | None = None


def _get_kafka_config() -> dict:
    """Return Kafka client config."""
    return settings.kafka_client_config.copy()


def get_producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = Producer(_get_kafka_config())
    return _producer


def ensure_topics() -> None:
    """Create the required Kafka topics if they don't already exist."""
    admin = AdminClient(_get_kafka_config())
    topics = [
        settings.KAFKA_TOPIC_TRAFFIC,
        settings.KAFKA_TOPIC_AIR,
        settings.KAFKA_TOPIC_TRANSIT,
        settings.KAFKA_TOPIC_WASTE,
        settings.KAFKA_TOPIC_TEMPERATURE,
    ]
    existing = admin.list_topics(timeout=10).topics.keys()
    new_topics = [
        NewTopic(t, num_partitions=1, replication_factor=1)
        for t in topics
        if t not in existing
    ]
    if new_topics:
        fs = admin.create_topics(new_topics)
        for topic, f in fs.items():
            try:
                f.result()
                logger.info("Created Kafka topic %s", topic)
            except Exception as e:
                logger.warning("Could not create topic %s: %s", topic, e)
