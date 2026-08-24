"""
The 'Crown Jewel' Hybrid Real/Online ingestion layer.

Polls real external APIs (TomTom Traffic, OpenWeatherMap Air Pollution,
GTFS-RT stub for transit) on independent schedules. Each domain has its own
CircuitBreaker: after N consecutive failures it trips to UNHEALTHY and the
domain silently falls back to the local simulator generator until a
successful real call resets it.

Both paths (real + simulated) publish to the *same* Kafka topics using the
*same* JSON schema, so downstream consumers are origin-agnostic.
"""

import asyncio
import logging
import random
import time
import uuid
from datetime import datetime, timezone

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from app.core.config import get_settings
from app.models.schemas import AirQualityRecord, TrafficRecord, TransitRecord, WasteRecord
from app.services.kafka_producer import publish

logger = logging.getLogger("api_adapter")
settings = get_settings()


# --------------------------------------------------------------------------- #
# Circuit Breaker
# --------------------------------------------------------------------------- #
class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int, reset_seconds: int):
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_seconds = reset_seconds
        self.consecutive_failures = 0
        self.tripped_at: float | None = None
        self.last_success: datetime | None = None

    @property
    def is_open(self) -> bool:
        """True == circuit OPEN == fall back to simulator."""
        if self.tripped_at is None:
            return False
        if time.time() - self.tripped_at > self.reset_seconds:
            logger.info("Circuit breaker for %s attempting reset (half-open)", self.name)
            return False
        return True

    def record_success(self):
        self.consecutive_failures = 0
        self.tripped_at = None
        self.last_success = datetime.now(timezone.utc)

    def record_failure(self):
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.failure_threshold and self.tripped_at is None:
            self.tripped_at = time.time()
            logger.warning(
                "Circuit breaker TRIPPED for %s after %s consecutive failures. "
                "Falling back to simulator.",
                self.name,
                self.consecutive_failures,
            )

    def status(self) -> str:
        if self.is_open:
            return "RED"
        if self.consecutive_failures > 0:
            return "YELLOW"
        return "GREEN"


traffic_breaker = CircuitBreaker("tomtom_traffic", settings.CIRCUIT_BREAKER_FAILURE_THRESHOLD, settings.CIRCUIT_BREAKER_RESET_SECONDS)
air_breaker = CircuitBreaker("openweather_air", settings.CIRCUIT_BREAKER_FAILURE_THRESHOLD, settings.CIRCUIT_BREAKER_RESET_SECONDS)
transit_breaker = CircuitBreaker("gtfs_rt_transit", settings.CIRCUIT_BREAKER_FAILURE_THRESHOLD, settings.CIRCUIT_BREAKER_RESET_SECONDS)

BREAKERS = {
    "traffic": traffic_breaker,
    "air": air_breaker,
    "transit": transit_breaker,
}


def get_connector_statuses() -> list[dict]:
    return [
        {
            "name": name,
            "status": breaker.status(),
            "last_success": breaker.last_success.isoformat() if breaker.last_success else None,
            "consecutive_failures": breaker.consecutive_failures,
            "using_fallback": breaker.is_open or not settings.effective_api_adapter_enabled,
        }
        for name, breaker in BREAKERS.items()
    ]


def _grid_points() -> list[tuple[float, float]]:
    """Generate a simple lat/lng grid around the configured city center."""
    lat0, lng0 = settings.CITY_CENTER_LAT, settings.CITY_CENTER_LNG
    r, step = settings.GRID_RADIUS_DEG, settings.GRID_STEP_DEG
    points = []
    lat = lat0 - r
    while lat <= lat0 + r:
        lng = lng0 - r
        while lng <= lng0 + r:
            points.append((round(lat, 5), round(lng, 5)))
            lng += step
        lat += step
    return points


# --------------------------------------------------------------------------- #
# TomTom Traffic
# --------------------------------------------------------------------------- #
@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TransportError)),
)
async def _fetch_tomtom_segment(client: httpx.AsyncClient, lat: float, lng: float) -> dict:
    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
    params = {"point": f"{lat},{lng}", "key": settings.TOMTOM_API_KEY}
    resp = await client.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _simulate_traffic_point(lat: float, lng: float) -> TrafficRecord:
    free_flow = round(random.uniform(35, 60), 1)
    current = round(free_flow * random.uniform(0.3, 1.05), 1)
    congestion = max(0.0, min(1.0, 1 - (current / free_flow)))
    return TrafficRecord(
        segment_id=f"sim-{lat}-{lng}",
        lat=lat,
        lng=lng,
        current_speed_kmh=current,
        free_flow_speed_kmh=free_flow,
        congestion_index=round(congestion, 3),
        source="simulator",
        recorded_at=datetime.now(timezone.utc),
    )


async def poll_traffic_once(client: httpx.AsyncClient) -> None:
    points = _grid_points()
    use_simulator = not settings.effective_api_adapter_enabled or traffic_breaker.is_open

    for lat, lng in points:
        if use_simulator:
            record = _simulate_traffic_point(lat, lng)
        else:
            try:
                data = await _fetch_tomtom_segment(client, lat, lng)
                seg = data["flowSegmentData"]
                current = float(seg["currentSpeed"])
                free_flow = float(seg["freeFlowSpeed"])
                congestion = max(0.0, min(1.0, 1 - (current / free_flow))) if free_flow else 0.0
                record = TrafficRecord(
                    segment_id=f"tomtom-{lat}-{lng}",
                    lat=lat,
                    lng=lng,
                    current_speed_kmh=current,
                    free_flow_speed_kmh=free_flow,
                    congestion_index=round(congestion, 3),
                    source="tomtom",
                    recorded_at=datetime.now(timezone.utc),
                )
                traffic_breaker.record_success()
            except Exception as e:  # noqa: BLE001
                logger.warning("TomTom fetch failed for (%s, %s): %s", lat, lng, e)
                traffic_breaker.record_failure()
                record = _simulate_traffic_point(lat, lng)

        publish(settings.KAFKA_TOPIC_TRAFFIC, record.model_dump(mode="json"))


# --------------------------------------------------------------------------- #
# OpenWeatherMap Air Pollution
# --------------------------------------------------------------------------- #
@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TransportError)),
)
async def _fetch_openweather_air(client: httpx.AsyncClient, lat: float, lng: float) -> dict:
    url = "https://api.openweathermap.org/data/2.5/air_pollution"
    params = {"lat": lat, "lon": lng, "appid": settings.OPENWEATHER_API_KEY}
    resp = await client.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _simulate_air_point(station_id: str, lat: float, lng: float) -> AirQualityRecord:
    return AirQualityRecord(
        station_id=station_id,
        lat=lat,
        lng=lng,
        aqi=random.randint(1, 5),
        pm2_5=round(random.uniform(2, 55), 1),
        pm10=round(random.uniform(5, 90), 1),
        no2=round(random.uniform(5, 80), 1),
        o3=round(random.uniform(10, 100), 1),
        source="simulator",
        recorded_at=datetime.now(timezone.utc),
    )


async def poll_air_quality_once(client: httpx.AsyncClient) -> None:
    # Air quality is sampled at a coarser set of "station" points (subset of grid)
    points = _grid_points()[::4] or [(settings.CITY_CENTER_LAT, settings.CITY_CENTER_LNG)]
    use_simulator = not settings.effective_api_adapter_enabled or air_breaker.is_open

    for idx, (lat, lng) in enumerate(points):
        station_id = f"station-{idx}"
        if use_simulator:
            record = _simulate_air_point(station_id, lat, lng)
        else:
            try:
                data = await _fetch_openweather_air(client, lat, lng)
                item = data["list"][0]
                components = item["components"]
                record = AirQualityRecord(
                    station_id=station_id,
                    lat=lat,
                    lng=lng,
                    aqi=int(item["main"]["aqi"]),
                    pm2_5=float(components.get("pm2_5", 0)),
                    pm10=float(components.get("pm10", 0)),
                    no2=float(components.get("no2", 0)),
                    o3=float(components.get("o3", 0)),
                    source="openweathermap",
                    recorded_at=datetime.now(timezone.utc),
                )
                air_breaker.record_success()
            except Exception as e:  # noqa: BLE001
                logger.warning("OpenWeather fetch failed for (%s, %s): %s", lat, lng, e)
                air_breaker.record_failure()
                record = _simulate_air_point(station_id, lat, lng)

        publish(settings.KAFKA_TOPIC_AIR, record.model_dump(mode="json"))


# --------------------------------------------------------------------------- #
# Public Transport (GTFS-RT)
#
# NOTE: Setting up a real GTFS-RT feed requires a city-specific static GTFS
# schedule + a vehicle-positions.pb realtime endpoint. For this MVP we stub
# realistic mock bus GPS data. To wire up a real feed later:
#
#   from google.transit import gtfs_realtime_pb2
#   import httpx
#   resp = await client.get(settings.GTFS_RT_URL)
#   feed = gtfs_realtime_pb2.FeedMessage()
#   feed.ParseFromString(resp.content)
#   for entity in feed.entity:
#       vp = entity.vehicle
#       ... map vp.position.latitude / longitude / speed into TransitRecord
# --------------------------------------------------------------------------- #
_BUS_STATE: dict[str, tuple[float, float]] = {}


def _simulate_transit_point(vehicle_id: str, route_id: str) -> TransitRecord:
    lat0, lng0 = settings.CITY_CENTER_LAT, settings.CITY_CENTER_LNG
    r = settings.GRID_RADIUS_DEG
    last = _BUS_STATE.get(vehicle_id)
    if last is None:
        lat, lng = lat0 + random.uniform(-r, r), lng0 + random.uniform(-r, r)
    else:
        lat = max(lat0 - r, min(lat0 + r, last[0] + random.uniform(-0.002, 0.002)))
        lng = max(lng0 - r, min(lng0 + r, last[1] + random.uniform(-0.002, 0.002)))
    _BUS_STATE[vehicle_id] = (lat, lng)

    return TransitRecord(
        vehicle_id=vehicle_id,
        route_id=route_id,
        lat=round(lat, 5),
        lng=round(lng, 5),
        speed_kmh=round(random.uniform(0, 45), 1),
        on_time=random.random() > 0.15,
        source="simulator",
        recorded_at=datetime.now(timezone.utc),
    )


async def poll_transit_once(client: httpx.AsyncClient) -> None:
    # GTFS_RT_URL not configured -> always stubbed for MVP (documented above).
    for i in range(12):
        vehicle_id = f"bus-{i}"
        route_id = f"route-{i % 4}"
        record = _simulate_transit_point(vehicle_id, route_id)
        publish(settings.KAFKA_TOPIC_TRANSIT, record.model_dump(mode="json"))
    transit_breaker.record_success()


# --------------------------------------------------------------------------- #
# Waste sensors (always simulated for MVP; same pattern applies for a real feed)
# --------------------------------------------------------------------------- #
async def poll_waste_once(client: httpx.AsyncClient) -> None:
    points = _grid_points()[::6]
    for idx, (lat, lng) in enumerate(points):
        record = WasteRecord(
            bin_id=f"bin-{idx}",
            lat=lat,
            lng=lng,
            fill_level_pct=round(random.uniform(0, 100), 1),
            source="simulator",
            recorded_at=datetime.now(timezone.utc),
        )
        publish(settings.KAFKA_TOPIC_WASTE, record.model_dump(mode="json"))


# --------------------------------------------------------------------------- #
# Background poller loops
# --------------------------------------------------------------------------- #
async def _loop(name: str, interval: int, fn, client: httpx.AsyncClient):
    while True:
        try:
            await fn(client)
        except Exception as e:  # noqa: BLE001
            logger.error("Unhandled error in %s poll loop: %s", name, e)
        await asyncio.sleep(interval)


async def start_api_adapter():
    if not settings.effective_api_adapter_enabled:
        logger.warning(
            "⚠️  No API Keys found (or API_ADAPTER_ENABLED=False). "
            "Falling back to Simulator for all domains."
        )
    else:
        logger.info("✅ API Adapter enabled. Polling TomTom / OpenWeatherMap for real data.")

    async with httpx.AsyncClient() as client:
        await asyncio.gather(
            _loop("traffic", settings.TRAFFIC_POLL_INTERVAL, poll_traffic_once, client),
            _loop("air", settings.AIR_POLL_INTERVAL, poll_air_quality_once, client),
            _loop("transit", settings.TRANSIT_POLL_INTERVAL, poll_transit_once, client),
            _loop("waste", settings.WASTE_POLL_INTERVAL, poll_waste_once, client),
        )
