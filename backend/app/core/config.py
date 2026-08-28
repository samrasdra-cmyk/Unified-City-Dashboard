from functools import lru_cache
import os
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pydantic import field_validator
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
    FORTYGUARD_API_KEY: str = ""
    FORTYGUARD_API_URL: str = "https://api.fortyguard.com/v1/thermal"

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
    TEMPERATURE_POLL_INTERVAL: int = 300

    # Circuit breaker
    CIRCUIT_BREAKER_FAILURE_THRESHOLD: int = 3
    CIRCUIT_BREAKER_RESET_SECONDS: int = 300

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"
    # Leave empty for local Docker Kafka. Aiven uses SASL_SSL and PLAIN.
    KAFKA_SECURITY_PROTOCOL: str = ""
    KAFKA_SASL_MECHANISM: str = "PLAIN"
    KAFKA_USERNAME: str = ""
    KAFKA_PASSWORD: str = ""
    # Optional Aiven CA bundle or raw PEM string
    KAFKA_SSL_CA_LOCATION: str = ""
    KAFKA_SSL_CA_PEM: str = ""
    KAFKA_TOPIC_TRAFFIC: str = "traffic.speed"
    KAFKA_TOPIC_AIR: str = "air.quality"
    KAFKA_TOPIC_TRANSIT: str = "transit.gps"
    KAFKA_TOPIC_WASTE: str = "waste.level"
    KAFKA_TOPIC_TEMPERATURE: str = "temperature.thermal"
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

    @field_validator("DATABASE_URL", mode="before")
    @classmethod
    def use_asyncpg_driver(cls, value: str) -> str:
        """Accept hosted-Postgres URLs with asyncpg-compatible options."""
        if value.startswith("postgresql://"):
            value = value.replace("postgresql://", "postgresql+asyncpg://", 1)
        elif value.startswith("postgres://"):
            value = value.replace("postgres://", "postgresql+asyncpg://", 1)

        # Providers commonly append libpq/psycopg-only options. SQLAlchemy
        # forwards URL query parameters to asyncpg, which expects `ssl` and does
        # not support `channel_binding`.
        url = urlsplit(value)
        query = [
            ("ssl" if key == "sslmode" else key, item_value)
            for key, item_value in parse_qsl(url.query, keep_blank_values=True)
            if key != "channel_binding"
        ]
        return urlunsplit((url.scheme, url.netloc, url.path, urlencode(query), url.fragment))

    # Redis
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    # A full connection URL takes precedence over REDIS_HOST and REDIS_PORT.
    REDIS_URL: str = ""
    REDIS_SNAPSHOT_TTL_SECONDS: int = 10

    @property
    def effective_api_adapter_enabled(self) -> bool:
        """Force-disable the real adapter if no external API key is configured."""
        return bool(self.API_ADAPTER_ENABLED and (self.TOMTOM_API_KEY or self.OPENWEATHER_API_KEY or self.FORTYGUARD_API_KEY))

    @property
    def kafka_client_config(self) -> dict[str, str]:
        """Connection options shared by Kafka producers, consumers, and admin clients."""
        config: dict[str, str] = {"bootstrap.servers": self.KAFKA_BOOTSTRAP_SERVERS}
        if self.KAFKA_SECURITY_PROTOCOL:
            config["security.protocol"] = self.KAFKA_SECURITY_PROTOCOL
        if self.KAFKA_USERNAME:
            config["sasl.mechanism"] = self.KAFKA_SASL_MECHANISM
            config["sasl.username"] = self.KAFKA_USERNAME
            config["sasl.password"] = self.KAFKA_PASSWORD

        # Resolve CA cert from PEM string, explicit path, or standard fallback paths
        if self.KAFKA_SSL_CA_PEM:
            config["ssl.ca.pem"] = self.KAFKA_SSL_CA_PEM
        else:
            candidates = [
                self.KAFKA_SSL_CA_LOCATION,
                os.path.join(os.path.dirname(__file__), "..", "ca.pem"),
                "ca.pem",
                "./ca.pem",
                "/app/ca.pem",
                "/app/app/ca.pem",
            ]
            for candidate in candidates:
                if candidate and Path(candidate).is_file():
                    config["ssl.ca.location"] = str(Path(candidate).resolve())
                    break

        return config


@lru_cache
def get_settings() -> Settings:
    return Settings()
