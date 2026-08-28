"""
Stream processing layer: consumes raw domain topics, computes rolling
windowed aggregates (avg speed / congestion, avg AQI, transit on-time %,
avg waste fill), persists raw + aggregated records to PostgreSQL/PostGIS,
and writes the latest snapshot into Redis (TTL configurable) for
low-latency dashboard reads.

Structured as a single asyncio consumer loop per topic so it can be lifted
into a real Flink/Spark job later without touching the schema contract.
"""

import asyncio
import logging
from collections import deque
from datetime import datetime, timezone

import orjson
from confluent_kafka import Consumer
from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from app.core.config import get_settings
from app.core.db import AsyncSessionLocal
from app.core.redis import redis_client
from app.models.orm import AirQualityReading, TemperatureReading, TrafficReading, TransitPosition, WasteReading

logger = logging.getLogger("kafka_consumer")
settings = get_settings()

WINDOW_SIZE = 200  # rolling window of most recent records per domain, per topic

_windows = {
    "traffic": deque(maxlen=WINDOW_SIZE),
    "air": deque(maxlen=WINDOW_SIZE),
    "transit": deque(maxlen=WINDOW_SIZE),
    "waste": deque(maxlen=WINDOW_SIZE),
    "temperature": deque(maxlen=WINDOW_SIZE),
}


def _make_consumer(group_suffix: str) -> Consumer:
    config = {
        **settings.kafka_client_config,
        "group.id": f"{settings.KAFKA_CONSUMER_GROUP}-{group_suffix}",
        "auto.offset.reset": "latest",
    }
    return Consumer(config)


async def _update_snapshot():
    """Recompute the aggregated snapshot from in-memory windows and cache it in Redis."""
    traffic = _windows["traffic"]
    air = _windows["air"]
    transit = _windows["transit"]
    waste = _windows["waste"]
    temperature = _windows["temperature"]

    avg_speed = sum(r["current_speed_kmh"] for r in traffic) / len(traffic) if traffic else None
    avg_aqi = sum(r["aqi"] for r in air) / len(air) if air else None
    on_time_pct = (
        100 * sum(1 for r in transit if r["on_time"]) / len(transit) if transit else None
    )
    avg_waste = sum(r["fill_level_pct"] for r in waste) / len(waste) if waste else None
    avg_thermal = (
        sum(r["thermal_comfort"] for r in temperature) / len(temperature) if temperature else None
    )
    avg_feels = (
        sum(r["feels_like"] for r in temperature) / len(temperature) if temperature else None
    )

    snapshot = {
        "avg_speed_kmh": round(avg_speed, 1) if avg_speed is not None else None,
        "avg_aqi": round(avg_aqi, 2) if avg_aqi is not None else None,
        "transit_on_time_pct": round(on_time_pct, 1) if on_time_pct is not None else None,
        "avg_waste_fill_pct": round(avg_waste, 1) if avg_waste is not None else None,
        "thermal_comfort": round(avg_thermal, 1) if avg_thermal is not None else None,
        "feels_like": round(avg_feels, 1) if avg_feels is not None else None,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    await redis_client.set(
        "dashboard:snapshot", orjson.dumps(snapshot), ex=settings.REDIS_SNAPSHOT_TTL_SECONDS
    )


async def _persist_traffic(record: dict):
    async with AsyncSessionLocal() as session:
        row = TrafficReading(
            segment_id=record["segment_id"],
            location=from_shape(Point(record["lng"], record["lat"]), srid=4326),
            current_speed_kmh=record["current_speed_kmh"],
            free_flow_speed_kmh=record["free_flow_speed_kmh"],
            congestion_index=record["congestion_index"],
            source=record["source"],
        )
        session.add(row)
        await session.commit()


async def _persist_air(record: dict):
    async with AsyncSessionLocal() as session:
        row = AirQualityReading(
            station_id=record["station_id"],
            location=from_shape(Point(record["lng"], record["lat"]), srid=4326),
            aqi=record["aqi"],
            pm2_5=record["pm2_5"],
            pm10=record["pm10"],
            no2=record["no2"],
            o3=record["o3"],
            source=record["source"],
        )
        session.add(row)
        await session.commit()


async def _persist_transit(record: dict):
    async with AsyncSessionLocal() as session:
        row = TransitPosition(
            vehicle_id=record["vehicle_id"],
            route_id=record["route_id"],
            location=from_shape(Point(record["lng"], record["lat"]), srid=4326),
            speed_kmh=record["speed_kmh"],
            on_time=record["on_time"],
            source=record["source"],
        )
        session.add(row)
        await session.commit()


async def _persist_waste(record: dict):
    async with AsyncSessionLocal() as session:
        row = WasteReading(
            bin_id=record["bin_id"],
            location=from_shape(Point(record["lng"], record["lat"]), srid=4326),
            fill_level_pct=record["fill_level_pct"],
            source=record["source"],
        )
        session.add(row)
        await session.commit()


async def _persist_temperature(record: dict):
    async with AsyncSessionLocal() as session:
        row = TemperatureReading(
            point_id=record["point_id"],
            location=from_shape(Point(record["lng"], record["lat"]), srid=4326),
            thermal_comfort=record["thermal_comfort"],
            feels_like=record["feels_like"],
            heat_index=record["heat_index"],
            humidity=record["humidity"],
            source=record["source"],
        )
        session.add(row)
        await session.commit()


PERSIST_FN = {
    "traffic": _persist_traffic,
    "air": _persist_air,
    "transit": _persist_transit,
    "waste": _persist_waste,
    "temperature": _persist_temperature,
}


async def _consume_topic(domain: str, topic: str):
    consumer = _make_consumer(domain)
    consumer.subscribe([topic])
    logger.info("Subscribed consumer to topic %s (domain=%s)", topic, domain)

    try:
        while True:
            msg = await asyncio.to_thread(consumer.poll, 1.0)
            if msg is None:
                continue
            if msg.error():
                logger.warning("Kafka consumer error on %s: %s", topic, msg.error())
                continue

            try:
                record = orjson.loads(msg.value())
            except Exception as e:  # noqa: BLE001
                logger.error("Failed to decode message on %s: %s", topic, e)
                continue

            _windows[domain].append(record)

            try:
                await PERSIST_FN[domain](record)
            except Exception as e:  # noqa: BLE001
                # DB hiccups must never crash the consumer loop
                logger.error("Failed to persist %s record: %s", domain, e)

            await _update_snapshot()
    finally:
        consumer.close()


async def start_kafka_consumers():
    await asyncio.gather(
        _consume_topic("traffic", settings.KAFKA_TOPIC_TRAFFIC),
        _consume_topic("air", settings.KAFKA_TOPIC_AIR),
        _consume_topic("transit", settings.KAFKA_TOPIC_TRANSIT),
        _consume_topic("waste", settings.KAFKA_TOPIC_WASTE),
        _consume_topic("temperature", settings.KAFKA_TOPIC_TEMPERATURE),
    )
