import asyncio
import json
import logging
import signal
from datetime import datetime
from typing import Dict, Union, Optional, List
from contextlib import asynccontextmanager
import time

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions, ASYNCHRONOUS
from pydantic import BaseModel, Field, ValidationError
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Kafka
    KAFKA_TOPIC: str = "telemetry"
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"
    KAFKA_GROUP_ID: str
    KAFKA_AUTO_OFFSET_RESET: str = "latest"
    KAFKA_ENABLE_AUTO_COMMIT: bool = True
    KAFKA_AUTO_COMMIT_INTERVAL_MS: int = 5000
    KAFKA_MAX_POLL_RECORDS: int = 500
    
    # InfluxDB
    INFLUXDB_URL: str
    INFLUXDB_TOKEN: str
    INFLUXDB_ORG: str
    INFLUXDB_BUCKET: str
    INFLUXDB_BATCH_SIZE: int = 500
    INFLUXDB_FLUSH_INTERVAL_SECONDS: int = 5
    INFLUXDB_RETRY_INTERVAL: int = 5000
    
    QUEUE_MAX_SIZE: int = 10000
    BATCH_TIMEOUT_SECONDS: float = 1.0
    MAX_RETRY_ATTEMPTS: int = 5
    INFLUX_CONNECTION_RETRY_DELAY: int = 5
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
    payload: Dict[str, Union[int, float, str, bool, None]]


class Metrics:
    def __init__(self):
        self.messages_consumed = 0
        self.messages_written = 0
        self.messages_failed = 0
        self.messages_retried = 0
        self.last_message_time = None
        
    def log_status(self):
        logger.info(
            f"Status statistics - Consumed: {self.messages_consumed}, "
            f"Written: {self.messages_written}, "
            f"Retried: {self.messages_retried}, "
            f"Failed: {self.messages_failed}"
        )

class InfluxWriter:
    def __init__(self, metrics: Metrics):
        self.metrics = metrics
        self.client = None
        self.write_api = None
        
        self._connect_to_influxdb()

    def _connect_to_influxdb(self):
        retry_attempts = 0
        while retry_attempts < settings.MAX_RETRY_ATTEMPTS:
            try:
                self.client = InfluxDBClient(
                    url=settings.INFLUXDB_URL,
                    token=settings.INFLUXDB_TOKEN,
                    org=settings.INFLUXDB_ORG,
                    enable_gzip=True,
                    timeout=30_000, 
                    retries=3
                )
                self.client.ping()
                logger.info("InfluxDB connected successfully")
                self.write_api = self.client.write_api(write_options=WriteOptions(
                    batch_size=settings.INFLUXDB_BATCH_SIZE,
                    flush_interval=settings.INFLUXDB_FLUSH_INTERVAL_SECONDS * 1000,
                    retry_interval=settings.INFLUXDB_RETRY_INTERVAL,
                    max_retries=settings.MAX_RETRY_ATTEMPTS,
                    write_type=ASYNCHRONOUS
                ))
                logger.info("InfluxDB Writer initialized successfully")
                return
            except Exception as e:
                retry_attempts += 1
                logger.error(f"InfluxDB connection failed ({retry_attempts}/{settings.MAX_RETRY_ATTEMPTS}): {e}")
                if retry_attempts < settings.MAX_RETRY_ATTEMPTS:
                    logger.info(f"Waiting for {settings.INFLUX_CONNECTION_RETRY_DELAY}s and then retrying...")
                    time.sleep(settings.INFLUX_CONNECTION_RETRY_DELAY)
                else:
                    logger.critical("InfluxDB connection failed, maximum retries reached. Service will exit.")
                    raise

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
                logger.error(f"Failed to build data point: {e}, Data: {data}")
                self.metrics.messages_failed += 1
                
        if points:
            try:
                self.write_api.write(
                    bucket=settings.INFLUXDB_BUCKET,
                    org=settings.INFLUXDB_ORG,
                    record=points
                )
                self.metrics.messages_written += len(points)
                logger.info(f"Successfully wrote {len(points)} data points to InfluxDB.")
            except Exception as e:
                logger.error(f"[InfluxDB] Batch write failed: {e}")
                raise

    def close(self):
        try:
            if self.write_api:
                self.write_api.close()
            if self.client:
                self.client.close()
            logger.info("InfluxDB Client closed")
        except Exception as e:
            logger.error(f"An error occurred while closing the InfluxDB client: {e}")


class KafkaInfluxBridge:
    def __init__(self):
        self.metrics = Metrics()
        self.writer = InfluxWriter(self.metrics)
        self.consumer = None
        self.queue = asyncio.Queue(maxsize=settings.QUEUE_MAX_SIZE)
        self.retry_queue = asyncio.Queue()
        self.running = True
        self._tasks = []

    async def start(self):
        try:
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
            logger.info(f"Kafka consumer started successfully, listening to topic: {settings.KAFKA_TOPIC}")
            
            self._tasks = [
                asyncio.create_task(self.consume_loop()),
                asyncio.create_task(self.write_loop()),
                asyncio.create_task(self.retry_loop()),
                asyncio.create_task(self.monitor_loop())
            ]
            
            await asyncio.gather(*self._tasks, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"Service failed to start: {e}")
            raise

    async def consume_loop(self):
        try:
            async for msg in self.consumer:
                if not self.running:
                    break
                    
                try:
                    telemetry = Telemetry(**msg.value)
                    
                    try:
                        self.queue.put_nowait(telemetry)
                        self.metrics.messages_consumed += 1
                        self.metrics.last_message_time = datetime.utcnow()
                    except asyncio.QueueFull:
                        logger.warning("Queue is full, dropping message")
                        self.metrics.messages_failed += 1
                        
                except ValidationError as e:
                    logger.warning(f"[Kafka] Data validation failed: {e.errors()}")
                    self.metrics.messages_failed += 1
                except Exception as e:
                    # 捕获可能的 json.JSONDecodeError
                    logger.error(f"[Kafka] Error processing message: {e}, message value: {msg.value}")
                    self.metrics.messages_failed += 1
                    
        except KafkaError as e:
            logger.error(f"Kafka consumption error: {e}")
        finally:
            if self.consumer:
                await self.consumer.stop()
                logger.info("Kafka consumer stopped")

    async def write_loop(self):
        """Batch writes data to InfluxDB"""
        batch = []
        last_flush_time = asyncio.get_event_loop().time()
        
        while self.running or not self.queue.empty():
            try:
                # Set timeout to ensure periodic flushing
                timeout = settings.BATCH_TIMEOUT_SECONDS
                
                try:
                    data = await asyncio.wait_for(
                        self.queue.get(), 
                        timeout=timeout
                    )
                    batch.append(data)
                except asyncio.TimeoutError:
                    pass
                
                # Check if batch needs to be flushed
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
                        logger.error(f"Batch write failed, adding data to retry queue: {e}")
                        # Add failed batch to retry queue
                        await self.retry_queue.put(batch)
                        batch = [] # Clear current batch
                        
            except Exception as e:
                logger.error(f"Error in write loop: {e}", exc_info=True)
                await asyncio.sleep(1)
                
        # Handle remaining data
        if batch:
            try:
                await self.writer.write_batch(batch)
            except Exception as e:
                logger.error(f"Failed to process remaining batch: {e}")
                await self.retry_queue.put(batch)
    
    async def retry_loop(self):
        """New: Handles failed batches in the retry queue"""
        while self.running or not self.retry_queue.empty():
            try:
                batch = await asyncio.wait_for(
                    self.retry_queue.get(), 
                    timeout=1.0 # Keep active
                )
                
                # Exponential backoff retry
                retry_count = 0
                while retry_count < settings.MAX_RETRY_ATTEMPTS:
                    try:
                        await self.writer.write_batch(batch)
                        self.metrics.messages_retried += len(batch)
                        logger.info(f"Retry successful, wrote {len(batch)} data points")
                        break # Retry successful, exit loop
                    except Exception as e:
                        retry_count += 1
                        logger.warning(
                            f"Retry write failed ({retry_count}/{settings.MAX_RETRY_ATTEMPTS}): {e}"
                        )
                        if retry_count < settings.MAX_RETRY_ATTEMPTS:
                            # Delay retry
                            await asyncio.sleep(2**retry_count)
                        else:
                            logger.error(f"Batch write failed, maximum retries reached, data lost: {e}")
                            self.metrics.messages_failed += len(batch)
                self.retry_queue.task_done()
                
            except asyncio.TimeoutError:
                pass # Queue is empty, continue to wait
            except Exception as e:
                logger.error(f"Error in retry loop: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def monitor_loop(self):
        """Periodically outputs monitoring information"""
        while self.running:
            await asyncio.sleep(30)  # Output every 30 seconds
            self.metrics.log_status()
            
            # Check for new messages
            if self.metrics.last_message_time:
                time_since_last = (datetime.utcnow() - self.metrics.last_message_time).seconds
                if time_since_last > 60:
                    logger.warning(f"No new messages received for {time_since_last} seconds")

    async def stop(self):
        """Gracefully stops the service"""
        logger.info("Shutting down service...")
        self.running = False
        
        # Wait for queue to empty (max 30 seconds)
        wait_time = 0
        while not self.queue.empty() and wait_time < 30:
            logger.info(f"Waiting for main queue to empty, remaining messages: {self.queue.qsize()}")
            await asyncio.sleep(1)
            wait_time += 1
        
        # Wait for retry queue to empty (max 30 seconds)
        wait_time = 0
        while not self.retry_queue.empty() and wait_time < 30:
            logger.info(f"Waiting for retry queue to empty, remaining messages: {self.retry_queue.qsize()}")
            await asyncio.sleep(1)
            wait_time += 1
            
        # Cancel all tasks
        for task in self._tasks:
            if not task.done():
                task.cancel()
                
        # Wait for tasks to finish
        await asyncio.gather(*self._tasks, return_exceptions=True)
        
        # Close resources
        self.writer.close()
        self.metrics.log_status()
        logger.info("Service has fully stopped")

def setup_signal_handlers(bridge):
    """Sets up signal handlers for graceful shutdown"""
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(bridge.stop()))
    loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(bridge.stop()))
    logger.info("Signal handlers have been set up")

async def main():
    bridge = KafkaInfluxBridge()
    
    # Set up signal handling
    setup_signal_handlers(bridge)
    
    try:
        await bridge.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Service exited with an error: {e}", exc_info=True)
    finally:
        await bridge.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program exited")
    except Exception as e:
        logger.error(f"Program exited with an error: {e}", exc_info=True)
        exit(1)