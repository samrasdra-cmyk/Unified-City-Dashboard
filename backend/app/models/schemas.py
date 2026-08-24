from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class TrafficRecord(BaseModel):
    segment_id: str
    lat: float
    lng: float
    current_speed_kmh: float
    free_flow_speed_kmh: float
    congestion_index: float = Field(description="0 = free flow, 1 = fully congested")
    source: Literal["tomtom", "simulator"]
    recorded_at: datetime


class AirQualityRecord(BaseModel):
    station_id: str
    lat: float
    lng: float
    aqi: int = Field(ge=1, le=5)
    pm2_5: float
    pm10: float
    no2: float
    o3: float
    source: Literal["openweathermap", "simulator"]
    recorded_at: datetime


class TransitRecord(BaseModel):
    vehicle_id: str
    route_id: str
    lat: float
    lng: float
    speed_kmh: float
    on_time: bool
    source: Literal["gtfs-rt", "simulator"]
    recorded_at: datetime


class WasteRecord(BaseModel):
    bin_id: str
    lat: float
    lng: float
    fill_level_pct: float = Field(ge=0, le=100)
    source: Literal["sensor", "simulator"]
    recorded_at: datetime


class DashboardSnapshot(BaseModel):
    avg_speed_kmh: float | None = None
    avg_aqi: float | None = None
    transit_on_time_pct: float | None = None
    avg_waste_fill_pct: float | None = None
    updated_at: datetime | None = None


class ConnectorStatus(BaseModel):
    name: str
    status: Literal["GREEN", "YELLOW", "RED"]
    last_success: datetime | None = None
    consecutive_failures: int = 0
    using_fallback: bool = False


class PopulationSimulationRequest(BaseModel):
    increase_percent: float = Field(ge=0, le=200)


class PopulationSimulationResponse(BaseModel):
    increase_percent: float
    projected_avg_congestion_index: float
    zones: dict
