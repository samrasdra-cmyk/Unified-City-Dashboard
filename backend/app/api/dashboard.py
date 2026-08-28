import logging
from datetime import datetime, timedelta, timezone

import orjson
from fastapi import APIRouter, Query
from redis.exceptions import RedisError
from sqlalchemy import select, text

from app.core.db import AsyncSessionLocal
from app.core.redis import redis_client
from app.models.orm import AirQualityReading, TemperatureReading, TrafficReading
from app.models.schemas import (
    DashboardSnapshot,
    PopulationSimulationRequest,
    PopulationSimulationResponse,
)
from app.services.simulation_stub import simulate_population_increase

logger = logging.getLogger("dashboard_api")
router = APIRouter(prefix="/api/v1/dashboard", tags=["dashboard"])


async def _get_cached_snapshot() -> DashboardSnapshot:
    """Return a cache snapshot without making Redis availability a UI outage."""
    try:
        raw = await redis_client.get("dashboard:snapshot")
    except RedisError as exc:
        logger.warning("Redis is unavailable; returning an empty dashboard snapshot: %s", exc)
        return DashboardSnapshot()

    if raw is None:
        return DashboardSnapshot()
    return DashboardSnapshot(**orjson.loads(raw))


@router.get("/latest", response_model=DashboardSnapshot)
async def get_latest():
    return await _get_cached_snapshot()


@router.get("/heatmap")
async def get_heatmap(
    type: str = Query(..., pattern="^(traffic|air|temperature)$"),
    bbox: str | None = Query(None, description="minLng,minLat,maxLng,maxLat"),
    limit: int = 500,
):
    """Return a GeoJSON FeatureCollection of the most recent readings from PostGIS."""
    try:
        async with AsyncSessionLocal() as session:
            if type == "traffic":
                stmt = (
                    select(
                        TrafficReading.segment_id,
                        TrafficReading.current_speed_kmh,
                        TrafficReading.congestion_index,
                        TrafficReading.recorded_at,
                        TrafficReading.location.ST_AsGeoJSON().label("geom"),
                    )
                    .order_by(TrafficReading.recorded_at.desc())
                    .limit(limit)
                )
            elif type == "air":
                stmt = (
                    select(
                        AirQualityReading.station_id,
                        AirQualityReading.aqi,
                        AirQualityReading.pm2_5,
                        AirQualityReading.recorded_at,
                        AirQualityReading.location.ST_AsGeoJSON().label("geom"),
                    )
                    .order_by(AirQualityReading.recorded_at.desc())
                    .limit(limit)
                )
            else:
                stmt = (
                    select(
                        TemperatureReading.point_id,
                        TemperatureReading.thermal_comfort,
                        TemperatureReading.feels_like,
                        TemperatureReading.heat_index,
                        TemperatureReading.humidity,
                        TemperatureReading.recorded_at,
                        TemperatureReading.location.ST_AsGeoJSON().label("geom"),
                    )
                    .order_by(TemperatureReading.recorded_at.desc())
                    .limit(limit)
                )

            result = await session.execute(stmt)
            rows = result.mappings().all()

        features = []
        for row in rows:
            geom = orjson.loads(row["geom"]) if row["geom"] else None
            props = {k: v for k, v in row.items() if k != "geom"}
            if isinstance(props.get("recorded_at"), datetime):
                props["recorded_at"] = props["recorded_at"].isoformat()
            features.append({"type": "Feature", "geometry": geom, "properties": props})

        return {"type": "FeatureCollection", "features": features}
    except Exception as exc:
        logger.error("Error retrieving %s heatmap data: %s", type, exc)
        return {"type": "FeatureCollection", "features": []}


@router.get("/history")
async def get_history(
    metric: str = Query(..., pattern="^(aqi|traffic|temperature)$"),
    hours: int = Query(24, ge=1, le=168),
):
    """Time-bucketed averages over the requested window, for the trend chart."""
    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    async with AsyncSessionLocal() as session:
        if metric == "aqi":
            stmt = text(
                """
                SELECT date_trunc('minute', recorded_at) AS bucket,
                       avg(aqi) AS value
                FROM air_quality_readings
                WHERE recorded_at >= :since
                GROUP BY bucket
                ORDER BY bucket
                """
            )
        elif metric == "traffic":
            stmt = text(
                """
                SELECT date_trunc('minute', recorded_at) AS bucket,
                       avg(current_speed_kmh) AS value
                FROM traffic_readings
                WHERE recorded_at >= :since
                GROUP BY bucket
                ORDER BY bucket
                """
            )
        else:
            stmt = text(
                """
                SELECT date_trunc('minute', recorded_at) AS bucket,
                       avg(thermal_comfort) AS value
                FROM temperature_readings
                WHERE recorded_at >= :since
                GROUP BY bucket
                ORDER BY bucket
                """
            )
        result = await session.execute(stmt, {"since": since})
        rows = result.mappings().all()

    return [
        {"timestamp": row["bucket"].isoformat(), "value": round(float(row["value"]), 2)}
        for row in rows
        if row["value"] is not None
    ]


@router.post("/simulate/population", response_model=PopulationSimulationResponse)
async def simulate_population(payload: PopulationSimulationRequest):
    snapshot = await _get_cached_snapshot()
    avg_speed = snapshot.avg_speed_kmh

    # Rough congestion index estimate from avg speed if we don't have one directly
    current_congestion = 0.3
    if avg_speed:
        current_congestion = max(0.0, min(1.0, 1 - (avg_speed / 50)))

    result = simulate_population_increase(payload.increase_percent, current_congestion)
    logger.info(
        "Population simulation stub run: +%s%% -> congestion=%s (placeholder for future ABM)",
        payload.increase_percent,
        result.projected_avg_congestion_index,
    )
    return result
