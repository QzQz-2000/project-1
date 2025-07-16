# config.py
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_TOPIC: str
    KAFKA_GROUP_ID: str
    KAFKA_AUTO_OFFSET_RESET: str  

    INFLUXDB_URL: str
    INFLUXDB_TOKEN: str
    INFLUXDB_ORG: str
    INFLUXDB_BUCKET: str
    INFLUXDB_BATCH_SIZE: int
    INFLUXDB_FLUSH_INTERVAL_SECONDS: int = 10

    class Config:
        env_file = ".env"


settings = Settings()
