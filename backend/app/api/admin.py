from fastapi import APIRouter

from app.models.schemas import ConnectorStatus
from app.services.api_adapter import get_connector_statuses

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])


@router.get("/connectors/status", response_model=list[ConnectorStatus])
async def connectors_status():
    return get_connector_statuses()
