"""
Standalone fallback simulator.

Runs independently of the FastAPI backend and pushes mock data to the exact
same Kafka topics / JSON schema used by the real API adapter
(backend/app/services/api_adapter.py). Only intended to run when
API_ADAPTER_ENABLED=False, e.g. for local development without any API keys,
or for load-testing the pipeline without hitting rate limits.

Usage:
    python data_simulator.py
"""

import os
import random
import time
from datetime import datetime, timezone

import orjson
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC_TRAFFIC = os.environ.get("KAFKA_TOPIC_TRAFFIC", "traffic.speed")
TOPIC_AIR = os.environ.get("KAFKA_TOPIC_AIR", "air.quality")
TOPIC_TRANSIT = os.environ.get("KAFKA_TOPIC_TRANSIT", "transit.gps")
TOPIC_WASTE = os.environ.get("KAFKA_TOPIC_WASTE", "waste.level")

CITY_LAT = float(os.environ.get("CITY_CENTER_LAT", 52.5200))
CITY_LNG = float(os.environ.get("CITY_CENTER_LNG", 13.4050))
RADIUS = float(os.environ.get("GRID_RADIUS_DEG", 0.03))

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def publish(topic: str, payload: dict) -> None:
    try:
        producer.produce(topic, value=orjson.dumps(payload))
        producer.poll(0)
    except BufferError:
        producer.flush(2)


def rand_point():
    return (
        round(CITY_LAT + random.uniform(-RADIUS, RADIUS), 5),
        round(CITY_LNG + random.uniform(-RADIUS, RADIUS), 5),
    )


def gen_traffic():
    lat, lng = rand_point()
    free_flow = round(random.uniform(35, 60), 1)
    current = round(free_flow * random.uniform(0.3, 1.05), 1)
    congestion = max(0.0, min(1.0, 1 - (current / free_flow)))
    return {
        "segment_id": f"sim-{lat}-{lng}",
        "lat": lat,
        "lng": lng,
        "current_speed_kmh": current,
        "free_flow_speed_kmh": free_flow,
        "congestion_index": round(congestion, 3),
        "source": "simulator",
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }


def gen_air(idx: int):
    lat, lng = rand_point()
    return {
        "station_id": f"station-{idx}",
        "lat": lat,
        "lng": lng,
        "aqi": random.randint(1, 5),
        "pm2_5": round(random.uniform(2, 55), 1),
        "pm10": round(random.uniform(5, 90), 1),
        "no2": round(random.uniform(5, 80), 1),
        "o3": round(random.uniform(10, 100), 1),
        "source": "simulator",
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }


def gen_transit(idx: int):
    lat, lng = rand_point()
    return {
        "vehicle_id": f"bus-{idx}",
        "route_id": f"route-{idx % 4}",
        "lat": lat,
        "lng": lng,
        "speed_kmh": round(random.uniform(0, 45), 1),
        "on_time": random.random() > 0.15,
        "source": "simulator",
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }


def gen_waste(idx: int):
    lat, lng = rand_point()
    return {
        "bin_id": f"bin-{idx}",
        "lat": lat,
        "lng": lng,
        "fill_level_pct": round(random.uniform(0, 100), 1),
        "source": "simulator",
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }


def main():
    print(f"⚠️  Standalone simulator started. Publishing to {KAFKA_BOOTSTRAP_SERVERS}")
    tick = 0
    while True:
        for _ in range(20):
            publish(TOPIC_TRAFFIC, gen_traffic())
        for i in range(8):
            publish(TOPIC_AIR, gen_air(i))
        for i in range(12):
            publish(TOPIC_TRANSIT, gen_transit(i))
        if tick % 5 == 0:
            for i in range(10):
                publish(TOPIC_WASTE, gen_waste(i))

        producer.flush(1)
        tick += 1
        time.sleep(10)


if __name__ == "__main__":
    main()
