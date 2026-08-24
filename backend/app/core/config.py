from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    ENV: str = "development"
    LOG_LEVEL: str = "INFO"

    # API Adapter
    API_ADAPTER_ENABLED: bool = True
    TOMTOM_API_KEY: str = ""
    OPENWEATHER_API_KEY: str = ""
    GTFS_RT_URL: str = ""

    # City grid
    CITY_NAME: str = "Sample City"
    CITY_CENTER_LAT: float = 52.5200
    CITY_CENTER_LNG: float = 13.4050
    GRID_RADIUS_DEG: float = 0.03
    GRID_STEP_DEG: float = 0.01

    # Polling
    TRAFFIC_POLL_INTERVAL: int = 60
    AIR_POLL_INTERVAL: int = 900
    TRANSIT_POLL_INTERVAL: int = 15
    WASTE_POLL_INTERVAL: int = 300

    # Circuit breaker
    CIRCUIT_BREAKER_FAILURE_THRESHOLD: int = 3
    CIRCUIT_BREAKER_RESET_SECONDS: int = 300

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"
    KAFKA_TOPIC_TRAFFIC: str = "traffic.speed"
    KAFKA_TOPIC_AIR: str = "air.quality"
    KAFKA_TOPIC_TRANSIT: str = "transit.gps"
    KAFKA_TOPIC_WASTE: str = "waste.level"
    KAFKA_CONSUMER_GROUP: str = "city-dashboard-consumer"

    # Postgres
    POSTGRES_HOST: str = "postgis"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "citydashboard"
    POSTGRES_USER: str = "citydashboard"
    POSTGRES_PASSWORD: str = "citydashboard_pw"
    DATABASE_URL: str = (
        "postgresql+asyncpg://citydashboard:citydashboard_pw@postgis:5432/citydashboard"
    )

    # Redis
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_SNAPSHOT_TTL_SECONDS: int = 10

    @property
    def effective_api_adapter_enabled(self) -> bool:
        """Force-disable the real adapter if no TomTom key is configured."""
        return bool(self.API_ADAPTER_ENABLED and self.TOMTOM_API_KEY)


@lru_cache
def get_settings() -> Settings:
    return Settings()
