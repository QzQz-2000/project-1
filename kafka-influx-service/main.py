import asyncio
import json
import logging
import signal
from datetime import datetime
from typing import Dict, Union, Optional

from aiokafka import AIOKafkaConsumer
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions, ASYNCHRONOUS
from pydantic import BaseModel, Field, ValidationError
from pydantic_settings import BaseSettings

# -------------------------------
# 配置加载 (.env 支持)
# -------------------------------
class Settings(BaseSettings):
    KAFKA_TOPIC: str
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_GROUP_ID: str
    KAFKA_AUTO_OFFSET_RESET: str = "earliest"

    INFLUXDB_URL: str
    INFLUXDB_TOKEN: str
    INFLUXDB_ORG: str
    INFLUXDB_BUCKET: str

    INFLUXDB_BATCH_SIZE: int = 500
    INFLUXDB_FLUSH_INTERVAL_SECONDS: int = 5

    class Config:
        env_file = ".env"

settings = Settings()


# -------------------------------
# 日志设置
# -------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("kafka-influx")

# -------------------------------
# 数据模型
# -------------------------------
class Telemetry(BaseModel):
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    device_id: str
    environment_id: str
    location: Optional[str]
    properties: Dict[str, Union[int, float, str, bool]]

# -------------------------------
# InfluxDB Writer
# -------------------------------
class InfluxWriter:
    def __init__(self):
        self.client = InfluxDBClient(
            url=settings.INFLUXDB_URL,
            token=settings.INFLUXDB_TOKEN,
            org=settings.INFLUXDB_ORG,
            enable_gzip=True
        )
        self.write_api = self.client.write_api(write_options=WriteOptions(
            batch_size=settings.INFLUXDB_BATCH_SIZE,
            flush_interval=settings.INFLUXDB_FLUSH_INTERVAL_SECONDS * 1000,
            write_type=ASYNCHRONOUS
        ))
        logger.info("InfluxDB Writer 初始化成功")

    async def write(self, data: Telemetry):
        try:
            point = (
                Point("device_telemetry")
                .tag("device_id", data.device_id)
                .tag("environment_id", data.environment_id)
                .time(data.timestamp, WritePrecision.NS)
            )
            for k, v in data.properties.items():
                if isinstance(v, (int, float, bool, str)):
                    point.field(k, v)
            self.write_api.write(
                bucket=settings.INFLUXDB_BUCKET,
                org=settings.INFLUXDB_ORG,
                record=point
            )
        except Exception as e:
            logger.error(f"[InfluxDB] 写入失败: {e}", exc_info=True)

    def close(self):
        self.write_api.close()
        self.client.close()
        logger.info("InfluxDB 客户端已关闭")

# -------------------------------
# Kafka-Influx 桥接类
# -------------------------------
class KafkaInfluxBridge:
    def __init__(self, writer: InfluxWriter):
        self.writer = writer
        self.consumer = AIOKafkaConsumer(
            settings.KAFKA_TOPIC,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=settings.KAFKA_GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",
        )
        self.queue = asyncio.Queue(maxsize=5000)
        self.running = True

    async def consume_loop(self):
        await self.consumer.start()
        logger.info("Kafka 消费者启动成功")
        try:
            async for msg in self.consumer:
                if not self.running:
                    break
                try:
                    telemetry = Telemetry(**msg.value)
                    await self.queue.put(telemetry)
                except ValidationError as e:
                    logger.warning(f"[Kafka] 验证失败: {e}")
        finally:
            await self.consumer.stop()
            logger.info("Kafka 消费者已停止")

    async def write_loop(self):
        while self.running:
            data = await self.queue.get()
            await self.writer.write(data)

    async def stop(self):
        self.running = False
        self.writer.close()
        logger.info("服务已停止")

# -------------------------------
# 主函数
# -------------------------------
async def main():
    writer = InfluxWriter()
    bridge = KafkaInfluxBridge(writer)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(bridge.stop()))
        except NotImplementedError:
            pass  # Windows 上可能不支持

    await asyncio.gather(bridge.consume_loop(), bridge.write_loop())

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("手动中断程序 (Ctrl+C)")
