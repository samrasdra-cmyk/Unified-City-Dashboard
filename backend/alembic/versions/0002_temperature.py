"""add temperature_readings table

Revision ID: 0002_temperature
Revises: 0001_init
Create Date: 2026-08-28

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID
import geoalchemy2

revision: str = "0002_temperature"
down_revision: Union[str, None] = "0001_init"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "temperature_readings",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("point_id", sa.String, index=True),
        sa.Column("location", geoalchemy2.Geometry(geometry_type="POINT", srid=4326)),
        sa.Column("thermal_comfort", sa.Float),
        sa.Column("feels_like", sa.Float),
        sa.Column("heat_index", sa.Float),
        sa.Column("humidity", sa.Float),
        sa.Column("source", sa.String, server_default="simulator"),
        sa.Column("recorded_at", sa.DateTime(timezone=True), server_default=sa.func.now(), index=True),
    )


def downgrade() -> None:
    op.drop_table("temperature_readings")
