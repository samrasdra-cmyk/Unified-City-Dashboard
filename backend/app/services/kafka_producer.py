import logging

import orjson
from confluent_kafka import Producer

from app.core.kafka import get_producer

logger = logging.getLogger("kafka_producer")


def _delivery_report(err, msg):
    if err is not None:
        logger.warning("Kafka delivery failed for %s: %s", msg.topic(), err)


def publish(topic: str, payload: dict) -> None:
    producer: Producer = get_producer()
    try:
        producer.produce(
            topic,
            value=orjson.dumps(payload),
            callback=_delivery_report,
        )
        producer.poll(0)
    except BufferError:
        logger.warning("Kafka producer queue full, flushing...")
        producer.flush(2)
    except Exception as e:  # noqa: BLE001
        # Never let a Kafka hiccup crash the ingestion pipeline
        logger.error("Failed to publish to %s: %s", topic, e)
