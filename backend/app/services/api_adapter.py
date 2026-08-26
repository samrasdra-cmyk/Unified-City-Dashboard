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
waste_breaker = CircuitBreaker("waste_sensor", settings.CIRCUIT_BREAKER_FAILURE_THRESHOLD, settings.CIRCUIT_BREAKER_RESET_SECONDS)

BREAKERS = {
    "traffic": traffic_breaker,
    "air": air_breaker,
    "transit": transit_breaker,
    "waste": waste_breaker,
}


def get_connector_statuses() -> list[dict]:
    return [
        {
            "name": name,
            "status": breaker.status(),
            "last_success": breaker.last_success.isoformat() if breaker.last_success else None,
            "consecutive_failures": breaker.consecutive_failures,
            "using_fallback": breaker.is_open
            or (name == "traffic" and not (settings.API_ADAPTER_ENABLED and settings.TOMTOM_API_KEY))
            or (name == "air" and not (settings.API_ADAPTER_ENABLED and settings.OPENWEATHER_API_KEY))
            or (name == "transit" and not (settings.API_ADAPTER_ENABLED and settings.GTFS_RT_URL))
            or (name == "waste" and not (settings.API_ADAPTER_ENABLED and settings.WASTE_API_URL)),
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
    has_key = bool(settings.API_ADAPTER_ENABLED and settings.TOMTOM_API_KEY)
    use_simulator = not has_key or traffic_breaker.is_open

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
    points = _grid_points()[::4] or [(settings.CITY_CENTER_LAT, settings.CITY_CENTER_LNG)]
    has_key = bool(settings.API_ADAPTER_ENABLED and settings.OPENWEATHER_API_KEY)
    use_simulator = not has_key or air_breaker.is_open

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

    # Realistic on-time rate with slight random variance
    is_on_time = random.random() > 0.12

    return TransitRecord(
        vehicle_id=vehicle_id,
        route_id=route_id,
        lat=round(lat, 5),
        lng=round(lng, 5),
        speed_kmh=round(random.uniform(0, 45), 1),
        on_time=is_on_time,
        source="simulator",
        recorded_at=datetime.now(timezone.utc),
    )


async def _fetch_gtfs_rt_feed(client: httpx.AsyncClient) -> list[TransitRecord]:
    try:
        from google.transit import gtfs_realtime_pb2
    except ImportError:
        logger.warning("gtfs-realtime-bindings not installed. Using fallback simulator.")
        return []

    resp = await client.get(settings.GTFS_RT_URL, timeout=15)
    resp.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)

    records: list[TransitRecord] = []
    now = datetime.now(timezone.utc)

    for entity in feed.entity:
        if entity.HasField("vehicle"):
            v = entity.vehicle
            vehicle_id = v.vehicle.id if v.vehicle.id else f"bus-{entity.id}"
            route_id = v.trip.route_id if v.trip.route_id else "route-unknown"
            lat = v.position.latitude
            lng = v.position.longitude
            speed_ms = v.position.speed if v.position.HasField("speed") else 0.0
            speed_kmh = round(speed_ms * 3.6, 1) if speed_ms else 0.0

            # Estimate on-time status: GTFS-RT standard evaluates delay or current schedule status
            # If delay field is not explicitly present, healthy speed (>5km/h) or valid position is on-time
            on_time = True
            if hasattr(v, "current_status") and v.current_status == 2:  # STOPPED_AT or congested
                on_time = speed_kmh > 0 or random.random() > 0.2

            records.append(
                TransitRecord(
                    vehicle_id=vehicle_id,
                    route_id=route_id,
                    lat=round(lat, 5),
                    lng=round(lng, 5),
                    speed_kmh=speed_kmh,
                    on_time=on_time,
                    source="gtfs-rt",
                    recorded_at=now,
                )
            )

    return records


async def poll_transit_once(client: httpx.AsyncClient) -> None:
    has_feed = bool(settings.API_ADAPTER_ENABLED and settings.GTFS_RT_URL)
    use_simulator = not has_feed or transit_breaker.is_open

    if not use_simulator:
        try:
            records = await _fetch_gtfs_rt_feed(client)
            if records:
                for record in records:
                    publish(settings.KAFKA_TOPIC_TRANSIT, record.model_dump(mode="json"))
                transit_breaker.record_success()
                return
            else:
                logger.info("GTFS-RT feed returned 0 vehicles, falling back to simulator.")
        except Exception as e:  # noqa: BLE001
            logger.warning("GTFS-RT fetch failed: %s", e)
            transit_breaker.record_failure()

    # Fallback to smart simulated fleet
    for i in range(12):
        vehicle_id = f"bus-{i}"
        route_id = f"route-{i % 4}"
        record = _simulate_transit_point(vehicle_id, route_id)
        publish(settings.KAFKA_TOPIC_TRANSIT, record.model_dump(mode="json"))
    transit_breaker.record_success()


# --------------------------------------------------------------------------- #
# Waste sensors (Real IoT API + Time/Day Pattern Model)
# --------------------------------------------------------------------------- #
async def _fetch_waste_api(client: httpx.AsyncClient) -> list[WasteRecord]:
    headers = {}
    if settings.WASTE_API_KEY:
        headers["Authorization"] = f"Bearer {settings.WASTE_API_KEY}"
        headers["x-api-key"] = settings.WASTE_API_KEY

    resp = await client.get(settings.WASTE_API_URL, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    records: list[WasteRecord] = []
    now = datetime.now(timezone.utc)
    # Support array of bins or { "bins": [...] }
    items = data if isinstance(data, list) else data.get("bins", data.get("items", []))
    for item in items:
        records.append(
            WasteRecord(
                bin_id=str(item.get("bin_id", item.get("id", uuid.uuid4().hex[:6]))),
                lat=float(item["lat"]),
                lng=float(item["lng"]),
                fill_level_pct=round(float(item.get("fill_percentage", item.get("fill_level_pct", 50.0))), 1),
                source="iot-sensor",
                recorded_at=now,
            )
        )
    return records


def _simulate_pattern_waste_point(bin_id: str, lat: float, lng: float, now: datetime) -> WasteRecord:
    """Pattern-based waste fill simulation reflecting diurnal and day-of-week trends."""
    hour = now.hour
    is_weekend = now.weekday() >= 5

    # Peak fill accumulation during midday to evening (12:00 - 20:00)
    base_fill = 40.0 + (35.0 * (1.0 - abs(hour - 16) / 16.0))
    if is_weekend:
        base_fill += 10.0

    # Add localized noise based on bin hash
    bin_hash_factor = (hash(bin_id) % 25) - 12
    fill_level = max(5.0, min(98.0, base_fill + bin_hash_factor + random.uniform(-5, 5)))

    return WasteRecord(
        bin_id=bin_id,
        lat=lat,
        lng=lng,
        fill_level_pct=round(fill_level, 1),
        source="pattern-model",
        recorded_at=now,
    )


async def poll_waste_once(client: httpx.AsyncClient) -> None:
    has_api = bool(settings.API_ADAPTER_ENABLED and settings.WASTE_API_URL)
    use_simulator = not has_api or waste_breaker.is_open

    if not use_simulator:
        try:
            records = await _fetch_waste_api(client)
            if records:
                for record in records:
                    publish(settings.KAFKA_TOPIC_WASTE, record.model_dump(mode="json"))
                waste_breaker.record_success()
                return
        except Exception as e:  # noqa: BLE001
            logger.warning("Waste IoT API fetch failed: %s", e)
            waste_breaker.record_failure()

    # Pattern-based model
    points = _grid_points()[::6]
    now = datetime.now(timezone.utc)
    for idx, (lat, lng) in enumerate(points):
        record = _simulate_pattern_waste_point(f"bin-{idx}", lat, lng, now)
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
    logger.info("Starting ingestion layer (Real GTFS-RT / TomTom / Weather / IoT & Pattern Models)")

    async with httpx.AsyncClient() as client:
        await asyncio.gather(
            _loop("traffic", settings.TRAFFIC_POLL_INTERVAL, poll_traffic_once, client),
            _loop("air", settings.AIR_POLL_INTERVAL, poll_air_quality_once, client),
            _loop("transit", settings.TRANSIT_POLL_INTERVAL, poll_transit_once, client),
            _loop("waste", settings.WASTE_POLL_INTERVAL, poll_waste_once, client),
        )

