"""init: postgis/timescaledb extensions + core tables

Revision ID: 0001_init
Revises:
Create Date: 2026-08-21

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID
import geoalchemy2

revision: str = "0001_init"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")

    op.create_table(
        "traffic_readings",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("segment_id", sa.String, index=True),
        sa.Column("location", geoalchemy2.Geometry(geometry_type="POINT", srid=4326)),
        sa.Column("current_speed_kmh", sa.Float),
        sa.Column("free_flow_speed_kmh", sa.Float),
        sa.Column("congestion_index", sa.Float),
        sa.Column("source", sa.String, server_default="simulator"),
        sa.Column("recorded_at", sa.DateTime(timezone=True), server_default=sa.func.now(), index=True),
    )

    op.create_table(
        "air_quality_readings",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("station_id", sa.String, index=True),
        sa.Column("location", geoalchemy2.Geometry(geometry_type="POINT", srid=4326)),
        sa.Column("aqi", sa.Float),
        sa.Column("pm2_5", sa.Float),
        sa.Column("pm10", sa.Float),
        sa.Column("no2", sa.Float),
        sa.Column("o3", sa.Float),
        sa.Column("source", sa.String, server_default="simulator"),
        sa.Column("recorded_at", sa.DateTime(timezone=True), server_default=sa.func.now(), index=True),
    )

    op.create_table(
        "transit_positions",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("vehicle_id", sa.String, index=True),
        sa.Column("route_id", sa.String, index=True),
        sa.Column("location", geoalchemy2.Geometry(geometry_type="POINT", srid=4326)),
        sa.Column("speed_kmh", sa.Float, server_default="0"),
        sa.Column("on_time", sa.Boolean, server_default=sa.true()),
        sa.Column("source", sa.String, server_default="simulator"),
        sa.Column("recorded_at", sa.DateTime(timezone=True), server_default=sa.func.now(), index=True),
    )

    op.create_table(
        "waste_readings",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("bin_id", sa.String, index=True),
        sa.Column("location", geoalchemy2.Geometry(geometry_type="POINT", srid=4326)),
        sa.Column("fill_level_pct", sa.Float),
        sa.Column("source", sa.String, server_default="simulator"),
        sa.Column("recorded_at", sa.DateTime(timezone=True), server_default=sa.func.now(), index=True),
    )

    # These tables deliberately use UUID primary keys.  TimescaleDB requires
    # every unique constraint on a hypertable to include its partitioning
    # column, so converting them here would fail and roll back the entire
    # migration.  Keep ordinary PostGIS tables until a later migration can
    # introduce compatible composite keys and the hypertable conversion.


def downgrade() -> None:
    op.drop_table("waste_readings")
    op.drop_table("transit_positions")
    op.drop_table("air_quality_readings")
    op.drop_table("traffic_readings")
