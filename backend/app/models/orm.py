import uuid
from datetime import datetime

from geoalchemy2 import Geometry
from sqlalchemy import DateTime, Float, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.db import Base


class TrafficReading(Base):
    __tablename__ = "traffic_readings"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    segment_id: Mapped[str] = mapped_column(String, index=True)
    location = mapped_column(Geometry(geometry_type="POINT", srid=4326))
    current_speed_kmh: Mapped[float] = mapped_column(Float)
    free_flow_speed_kmh: Mapped[float] = mapped_column(Float)
    congestion_index: Mapped[float] = mapped_column(Float)
    source: Mapped[str] = mapped_column(String, default="simulator")
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)


class AirQualityReading(Base):
    __tablename__ = "air_quality_readings"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    station_id: Mapped[str] = mapped_column(String, index=True)
    location = mapped_column(Geometry(geometry_type="POINT", srid=4326))
    aqi: Mapped[int] = mapped_column(Float)
    pm2_5: Mapped[float] = mapped_column(Float)
    pm10: Mapped[float] = mapped_column(Float)
    no2: Mapped[float] = mapped_column(Float)
    o3: Mapped[float] = mapped_column(Float)
    source: Mapped[str] = mapped_column(String, default="simulator")
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)


class TransitPosition(Base):
    __tablename__ = "transit_positions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    vehicle_id: Mapped[str] = mapped_column(String, index=True)
    route_id: Mapped[str] = mapped_column(String, index=True)
    location = mapped_column(Geometry(geometry_type="POINT", srid=4326))
    speed_kmh: Mapped[float] = mapped_column(Float, default=0.0)
    on_time: Mapped[bool] = mapped_column(default=True)
    source: Mapped[str] = mapped_column(String, default="simulator")
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)


class WasteReading(Base):
    __tablename__ = "waste_readings"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    bin_id: Mapped[str] = mapped_column(String, index=True)
    location = mapped_column(Geometry(geometry_type="POINT", srid=4326))
    fill_level_pct: Mapped[float] = mapped_column(Float)
    source: Mapped[str] = mapped_column(String, default="simulator")
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)


class TemperatureReading(Base):
    __tablename__ = "temperature_readings"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    point_id: Mapped[str] = mapped_column(String, index=True)
    location = mapped_column(Geometry(geometry_type="POINT", srid=4326))
    thermal_comfort: Mapped[float] = mapped_column(Float)
    feels_like: Mapped[float] = mapped_column(Float)
    heat_index: Mapped[float] = mapped_column(Float)
    humidity: Mapped[float] = mapped_column(Float)
    source: Mapped[str] = mapped_column(String, default="simulator")
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)

