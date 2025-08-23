from pydantic import Field
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # MongoDB
    MONGO_DB_URL: str
    MONGO_DB_NAME: str

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_TOPIC: str
    KAFKA_GROUP_ID: str
    KAFKA_AUTO_OFFSET_RESET: str

    # Redis
    REDIS_URL: str

    # API
    API_BASE_URL: str
    API_BATCH_SIZE: int
    API_BATCH_TIMEOUT: float

    # WebSocket notifications
    WEBSOCKET_NOTIFICATION_ENABLED: bool

    class Config:
        env_file = ".env"

settings = Settings()
