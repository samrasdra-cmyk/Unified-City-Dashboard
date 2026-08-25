import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api import admin, dashboard, websocket
from app.core.config import get_settings
from app.core.kafka import ensure_topics
from app.services.api_adapter import start_api_adapter
from app.services.kafka_consumer import start_kafka_consumers

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("main")
settings = get_settings()

_background_tasks: list[asyncio.Task] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Unified City Dashboard backend (env=%s)", settings.ENV)
    try:
        ensure_topics()
    except Exception as e:  # noqa: BLE001
        logger.warning("Could not ensure Kafka topics on startup (will retry via producer): %s", e)

    _background_tasks.append(asyncio.create_task(start_api_adapter()))
    _background_tasks.append(asyncio.create_task(start_kafka_consumers()))

    yield

    logger.info("Shutting down, cancelling background tasks...")
    for task in _background_tasks:
        task.cancel()
    await asyncio.gather(*_background_tasks, return_exceptions=True)


app = FastAPI(title="Unified City Dashboard API", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in production to the frontend origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(dashboard.router)
app.include_router(admin.router)
app.include_router(websocket.router)


@app.get("/")
@app.head("/")
async def root():
    return {"status": "ok", "service": "Unified City Dashboard API", "city": settings.CITY_NAME}


@app.get("/health")
async def health():
    return {"status": "ok", "city": settings.CITY_NAME}
