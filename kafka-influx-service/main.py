import asyncio
import json
import logging
import signal
from datetime import datetime
from typing import Dict, Union, Optional, List
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions, ASYNCHRONOUS
from pydantic import BaseModel, Field, ValidationError
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Kafka 配置
    KAFKA_TOPIC: str
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_GROUP_ID: str
    KAFKA_AUTO_OFFSET_RESET: str = "latest"  # 改为 latest，避免重复处理历史数据
    KAFKA_ENABLE_AUTO_COMMIT: bool = True
    KAFKA_AUTO_COMMIT_INTERVAL_MS: int = 5000
    KAFKA_MAX_POLL_RECORDS: int = 500
    
    # InfluxDB 配置
    INFLUXDB_URL: str
    INFLUXDB_TOKEN: str
    INFLUXDB_ORG: str
    INFLUXDB_BUCKET: str
    INFLUXDB_BATCH_SIZE: int = 500
    INFLUXDB_FLUSH_INTERVAL_SECONDS: int = 5
    INFLUXDB_RETRY_INTERVAL: int = 5000  # 重试间隔（毫秒）
    
    # 应用配置
    QUEUE_MAX_SIZE: int = 10000
    BATCH_TIMEOUT_SECONDS: float = 1.0
    MAX_RETRY_ATTEMPTS: int = 3
    LOG_LEVEL: str = "INFO"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("kafka-influx-bridge")


class Telemetry(BaseModel):
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    device_id: str
    environment_id: str
    payload: Dict[str, Union[int, float, str, bool, None]]  # 设备数据


class Metrics:
    def __init__(self):
        self.messages_consumed = 0
        self.messages_written = 0
        self.messages_failed = 0
        self.last_message_time = None
        
    def log_status(self):
        logger.info(
            f"状态统计 - 已消费: {self.messages_consumed}, "
            f"已写入: {self.messages_written}, "
            f"失败: {self.messages_failed}"
        )

class InfluxWriter:
    def __init__(self, metrics: Metrics):
        self.metrics = metrics
        self.client = InfluxDBClient(
            url=settings.INFLUXDB_URL,
            token=settings.INFLUXDB_TOKEN,
            org=settings.INFLUXDB_ORG,
            enable_gzip=True,
            timeout=30_000, 
            retries=3
        )       
        try:
            self.client.ping()
            logger.info("InfluxDB 连接测试成功")
        except Exception as e:
            logger.error(f"InfluxDB 连接失败: {e}")
            raise
            
        self.write_api = self.client.write_api(write_options=WriteOptions(
            batch_size=settings.INFLUXDB_BATCH_SIZE,
            flush_interval=settings.INFLUXDB_FLUSH_INTERVAL_SECONDS * 1000,
            retry_interval=settings.INFLUXDB_RETRY_INTERVAL,
            max_retries=settings.MAX_RETRY_ATTEMPTS,
            write_type=ASYNCHRONOUS
        ))
        logger.info("InfluxDB Writer 初始化成功")

    async def write_batch(self, data_list: List[Telemetry]):
        if not data_list:
            return      
        points = []
        for data in data_list:
            try:
                point = (
                    Point("device_telemetry")
                    .tag("device_id", data.device_id)
                    .tag("environment_id", data.environment_id)
                    .time(data.timestamp, WritePrecision.NS)
                )
                    
                for k, v in data.payload.items():
                    if isinstance(v, (int, float)):
                        point.field(k, float(v))
                    elif isinstance(v, bool):
                        point.field(k, v)
                    elif isinstance(v, str):
                        point.field(k, v)
                        
                points.append(point)
                
            except Exception as e:
                logger.error(f"构建数据点失败: {e}, 数据: {data}")
                self.metrics.messages_failed += 1
                
        if points:
            try:
                self.write_api.write(
                    bucket=settings.INFLUXDB_BUCKET,
                    org=settings.INFLUXDB_ORG,
                    record=points
                )
                self.metrics.messages_written += len(points)
                logger.debug(f"成功写入 {len(points)} 条数据到 InfluxDB")
            except Exception as e:
                logger.error(f"[InfluxDB] 批量写入失败: {e}")
                self.metrics.messages_failed += len(points)
                raise

    def close(self):
        try:
            self.write_api.close()
            self.client.close()
            logger.info("InfluxDB 客户端已关闭")
        except Exception as e:
            logger.error(f"关闭 InfluxDB 客户端时出错: {e}")


class KafkaInfluxBridge:
    def __init__(self):
        self.metrics = Metrics()
        self.writer = InfluxWriter(self.metrics)
        self.consumer = None
        self.queue = asyncio.Queue(maxsize=settings.QUEUE_MAX_SIZE)
        self.running = True
        self._tasks = []

    async def start(self):
        try:
            # 创建并启动 Kafka 消费者
            self.consumer = AIOKafkaConsumer(
                settings.KAFKA_TOPIC,
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                group_id=settings.KAFKA_GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset=settings.KAFKA_AUTO_OFFSET_RESET,
                enable_auto_commit=settings.KAFKA_ENABLE_AUTO_COMMIT,
                auto_commit_interval_ms=settings.KAFKA_AUTO_COMMIT_INTERVAL_MS,
                max_poll_records=settings.KAFKA_MAX_POLL_RECORDS,
                session_timeout_ms=30000,
                request_timeout_ms=40000,
            )
            
            await self.consumer.start()
            logger.info(f"Kafka 消费者启动成功，监听主题: {settings.KAFKA_TOPIC}")
            
            # 启动工作任务
            self._tasks = [
                asyncio.create_task(self.consume_loop()),
                asyncio.create_task(self.write_loop()),
                asyncio.create_task(self.monitor_loop())
            ]
            
            await asyncio.gather(*self._tasks, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"服务启动失败: {e}")
            raise

    async def consume_loop(self):
        """消费 Kafka 消息"""
        try:
            async for msg in self.consumer:
                if not self.running:
                    break
                    
                try:
                    # 解析和验证数据
                    telemetry = Telemetry(**msg.value)
                    
                    # 将数据放入队列
                    try:
                        self.queue.put_nowait(telemetry)
                        self.metrics.messages_consumed += 1
                        self.metrics.last_message_time = datetime.utcnow()
                    except asyncio.QueueFull:
                        logger.warning("队列已满，丢弃消息")
                        self.metrics.messages_failed += 1
                        
                except ValidationError as e:
                    logger.warning(f"[Kafka] 数据验证失败: {e.errors()}")
                    self.metrics.messages_failed += 1
                except Exception as e:
                    logger.error(f"[Kafka] 处理消息时出错: {e}")
                    self.metrics.messages_failed += 1
                    
        except KafkaError as e:
            logger.error(f"Kafka 消费错误: {e}")
        finally:
            if self.consumer:
                await self.consumer.stop()
                logger.info("Kafka 消费者已停止")

    async def write_loop(self):
        """批量写入数据到 InfluxDB"""
        batch = []
        last_flush_time = asyncio.get_event_loop().time()
        
        while self.running or not self.queue.empty():
            try:
                # 设置超时，确保定期刷新
                timeout = settings.BATCH_TIMEOUT_SECONDS
                
                try:
                    data = await asyncio.wait_for(
                        self.queue.get(), 
                        timeout=timeout
                    )
                    batch.append(data)
                except asyncio.TimeoutError:
                    pass
                
                # 检查是否需要刷新批次
                current_time = asyncio.get_event_loop().time()
                time_elapsed = current_time - last_flush_time
                
                should_flush = (
                    len(batch) >= settings.INFLUXDB_BATCH_SIZE or
                    (time_elapsed >= settings.INFLUXDB_FLUSH_INTERVAL_SECONDS and batch) or
                    (not self.running and batch)
                )
                
                if should_flush:
                    try:
                        await self.writer.write_batch(batch)
                        batch.clear()
                        last_flush_time = current_time
                    except Exception as e:
                        logger.error(f"批量写入失败，将重试: {e}")
                        # 可以实现重试逻辑或将失败的批次保存到备份队列
                        await asyncio.sleep(1)
                        
            except Exception as e:
                logger.error(f"写入循环出错: {e}", exc_info=True)
                await asyncio.sleep(1)
                
        # 处理剩余数据
        if batch:
            try:
                await self.writer.write_batch(batch)
            except Exception as e:
                logger.error(f"处理剩余批次时失败: {e}")

    async def monitor_loop(self):
        """定期输出监控信息"""
        while self.running:
            await asyncio.sleep(30)  # 每30秒输出一次
            self.metrics.log_status()
            
            # 检查消息接收情况
            if self.metrics.last_message_time:
                time_since_last = (datetime.utcnow() - self.metrics.last_message_time).seconds
                if time_since_last > 60:
                    logger.warning(f"已经 {time_since_last} 秒没有收到新消息")

    async def stop(self):
        """优雅停止服务"""
        logger.info("正在停止服务...")
        self.running = False
        
        # 等待队列清空（最多等待30秒）
        wait_time = 0
        while not self.queue.empty() and wait_time < 30:
            logger.info(f"等待队列清空，剩余消息: {self.queue.qsize()}")
            await asyncio.sleep(1)
            wait_time += 1
            
        # 取消所有任务
        for task in self._tasks:
            if not task.done():
                task.cancel()
                
        # 等待任务结束
        await asyncio.gather(*self._tasks, return_exceptions=True)
        
        # 关闭资源
        self.writer.close()
        self.metrics.log_status()
        logger.info("服务已完全停止")


async def main():
    bridge = KafkaInfluxBridge()
    
    # 设置信号处理
    setup_signal_handlers(bridge)
    
    try:
        await bridge.start()
    except KeyboardInterrupt:
        logger.info("收到键盘中断")
    except Exception as e:
        logger.error(f"服务异常退出: {e}", exc_info=True)
    finally:
        await bridge.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("程序已退出")
    except Exception as e:
        logger.error(f"程序异常: {e}", exc_info=True)
        exit(1)