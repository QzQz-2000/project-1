import asyncio
import aiohttp
import json
import logging
import os  # 添加缺少的 import
import redis.asyncio as redis
from datetime import datetime, timezone
from typing import Dict, List, Optional, Union
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field, ValidationError
from config import settings


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BatchUpdateItem(BaseModel): 
    """Batch update item model"""
    twin_id: str
    environment_id: str
    properties: Dict[str, Union[int, float, str, bool, None]]
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class TelemetryMessage(BaseModel):
    """Telemetry message model"""
    environment_id: str
    device_id: str
    payload: Dict[str, Union[int, float, str, bool, None]] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    class Config:
        # Allow datetime strings to be automatically parsed
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
    
    def __init__(self, **kwargs):
        # Handle timestamp string parsing
        if 'timestamp' in kwargs and isinstance(kwargs['timestamp'], str):
            try:
                # Parse timestamp string and ensure it's in UTC
                parsed_time = datetime.fromisoformat(kwargs['timestamp'].replace('Z', '+00:00'))
                # Convert to UTC if it has timezone info, otherwise assume it's UTC
                if parsed_time.tzinfo is not None:
                    kwargs['timestamp'] = parsed_time.astimezone(timezone.utc)
                else:
                    kwargs['timestamp'] = parsed_time.replace(tzinfo=timezone.utc)
            except ValueError:
                # If parsing fails, use current UTC time
                kwargs['timestamp'] = datetime.now(timezone.utc)
        super().__init__(**kwargs)


class Metrics:
    """Performance metrics collector"""
    def __init__(self):
        self.messages_consumed = 0
        self.twins_updated = 0
        self.update_failures = 0
        self.notifications_sent = 0
        self.start_time = datetime.now(timezone.utc)  # 使用 UTC 时间
    
    def log_status(self):
        """Output status statistics"""
        uptime = datetime.now(timezone.utc) - self.start_time  # 使用 UTC 时间计算
        logger.info(f"Metrics - Uptime: {uptime}, Messages Consumed: {self.messages_consumed}, "
                   f"Twins Updated: {self.twins_updated}, Failures: {self.update_failures}, "
                   f"Notifications Sent: {self.notifications_sent}")


class WebSocketNotificationService:
    """WebSocket notification service"""
    
    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.enabled = settings.WEBSOCKET_NOTIFICATION_ENABLED
    
    async def send_twin_properties_updated_notification(self, environment_id: str, twin_id: str, 
                                                       updated_properties: Dict, timestamp: datetime):
        """Send Twin properties updated notification"""
        if not self.enabled:
            return
        try:
            # Normalize timestamp to UTC
            if timestamp.tzinfo is not None:
                utc_timestamp = timestamp.astimezone(timezone.utc)
            else:
                utc_timestamp = timestamp.replace(tzinfo=timezone.utc)
            
            notification = {
                "type": "twin_properties_updated",
                "environment_id": environment_id,
                "twin_id": twin_id,
                "updated_properties": updated_properties,
                "timestamp": utc_timestamp.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "event_time": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            }
            
            # Publish to Redis channel, WebSocket server listens to it
            channel = f"ws_notifications:{environment_id}"
            await self.redis_client.publish(channel, json.dumps(notification))
            
            logger.debug(f"Sent Twin properties updated notification: {twin_id} -> {environment_id}")
            
        except Exception as e:
            logger.error(f"Failed to send Twin properties updated notification: {e}")
    
    async def send_system_status_notification(self, environment_id: str, status: Dict):
        """Send system status notification"""
        if not self.enabled:
            return
        
        try:
            notification = {
                "type": "system_status",
                "environment_id": environment_id,
                "status": status,
                "event_time": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            }
            
            channel = f"ws_notifications:{environment_id}"
            await self.redis_client.publish(channel, json.dumps(notification))
            
        except Exception as e:
            logger.error(f"Failed to send system status notification: {e}")


class OptimizedTwinUpdaterService:
    """Optimized Twin updater service - supports real-time notifications"""
    
    def __init__(self):
        self.running = True
        self.consumer = None
        self.db_client = None
        self.db = None
        self.http_session = None
        self.redis_client = None
        self.notification_service = None
        self._tasks = []
        
        # Batch update queues - grouped by environment
        self.update_queues = {}  # environment_id -> Queue
        self.metrics = Metrics()
        
        # API configuration
        self.api_base_url = settings.API_BASE_URL
        self.api_timeout = aiohttp.ClientTimeout(total=30)
        self.max_batch_size = settings.API_BATCH_SIZE
        self.batch_timeout = settings.API_BATCH_TIMEOUT

    async def connect_to_databases(self):
        """Initialize connections"""
        try:
            # Debug info
            logger.info(f"=== Service configuration ===")
            logger.info(f"MongoDB URL: {settings.MONGO_DB_URL}")
            logger.info(f"MongoDB DB Name: {settings.MONGO_DB_NAME}")
            logger.info(f"Redis URL: {settings.REDIS_URL}")
            logger.info(f"API Base URL: {settings.API_BASE_URL}")
            logger.info(f"Kafka Servers: {settings.KAFKA_BOOTSTRAP_SERVERS}")
            logger.info(f"==================")
            
            # MongoDB connection (for mapping lookup)
            logger.info(f"Attempting MongoDB connection: {settings.MONGO_DB_URL}")
            self.db_client = AsyncIOMotorClient(settings.MONGO_DB_URL, serverSelectionTimeoutMS=10000)
            self.db = self.db_client[settings.MONGO_DB_NAME]
            await self.db.command('ping')
            logger.info("MongoDB connected successfully")
        except Exception as e:
            logger.error(f"MongoDB connection failed: {e}")
            raise
        
        try:
            # Redis connection (for WebSocket notifications)
            logger.info(f"Attempting Redis connection: {settings.REDIS_URL}")
            self.redis_client = redis.from_url(settings.REDIS_URL)
            await self.redis_client.ping()
            logger.info("Redis connected successfully")
        except Exception as e:
            logger.warning(f"Redis connection failed: {e} - WebSocket notifications will be disabled")
            self.redis_client = None
        
        # Initialize notification service
        if self.redis_client:
            self.notification_service = WebSocketNotificationService(self.redis_client)
        
        # HTTP Session with connection pooling
        connector = aiohttp.TCPConnector(
            limit=100,  # total connection pool
            limit_per_host=20,  # connections per host
            keepalive_timeout=60,
            enable_cleanup_closed=True
        )
        
        self.http_session = aiohttp.ClientSession(
            connector=connector,
            timeout=self.api_timeout,
            headers={'Content-Type': 'application/json'}
        )
        
        logger.info("Database and HTTP connections initialized")

    async def start(self):
        """Start service"""
        await self.connect_to_databases()
        
        try:
            from aiokafka import AIOKafkaConsumer
            logger.info(f"Attempting Kafka connection: {settings.KAFKA_BOOTSTRAP_SERVERS}")
            
            self.consumer = AIOKafkaConsumer(
                settings.KAFKA_TOPIC,
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                group_id=settings.KAFKA_GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset=settings.KAFKA_AUTO_OFFSET_RESET,
                consumer_timeout_ms=1000,  # add timeout to avoid blocking
            )

            await self.consumer.start()
            logger.info("Kafka connected successfully")
        except Exception as e:
            logger.error(f"Kafka connection failed: {e}")
            raise
        
        self._tasks = [
            asyncio.create_task(self.consume_loop()),
            asyncio.create_task(self.monitor_loop()),
            asyncio.create_task(self.status_broadcaster_loop())  # status broadcasting loop
        ]
        
        logger.info("Twin Updater Service started successfully")
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def consume_loop(self):
        """Consume Kafka messages"""
        try:
            async for msg in self.consumer:
                if not self.running:
                    break
                    
                try:
                    # Use pydantic for validation and parsing
                    telemetry = TelemetryMessage(**msg.value)
                    self.metrics.messages_consumed += 1
                    
                    # Lookup device mapping
                    mapping = await self.db["device_twin_mappings"].find_one({
                        "environment_id": telemetry.environment_id,
                        "device_id": telemetry.device_id
                    })
                    
                    if not mapping:
                        logger.debug(f"Device {telemetry.device_id} has no mapping")
                        continue
                    
                    # Create update item
                    update_item = BatchUpdateItem(
                        twin_id=mapping["twin_id"],
                        environment_id=telemetry.environment_id,
                        properties=telemetry.payload,
                        timestamp=telemetry.timestamp
                    )
                    
                    # Add to environment batch queue
                    await self._add_to_batch_queue(update_item)
                    
                except ValidationError as e:
                    logger.warning(f"Data validation failed: {e.errors()}")
                    self.metrics.update_failures += 1
                except Exception as e:
                    logger.error(f"Failed to process message: {e}")
                    self.metrics.update_failures += 1
        except Exception as e:
            logger.error(f"Consume loop exception: {e}")
            if self.running:
                raise

    async def _add_to_batch_queue(self, update_item: BatchUpdateItem):
        """Add to batch update queue"""
        env_id = update_item.environment_id
        
        # Ensure environment queue exists
        if env_id not in self.update_queues:
            self.update_queues[env_id] = asyncio.Queue(maxsize=10000)
            # Start batch processing task for this environment
            task = asyncio.create_task(self._batch_update_loop(env_id))
            self._tasks.append(task)
        
        try:
            await self.update_queues[env_id].put(update_item)
        except asyncio.QueueFull:
            logger.warning(f"Update queue for environment {env_id} is full")
            self.metrics.update_failures += 1

    async def _batch_update_loop(self, environment_id: str):
        """Batch update loop for a specific environment"""
        queue = self.update_queues[environment_id]
        batch = {}  # twin_id -> properties
        last_flush_time = asyncio.get_event_loop().time()
        
        while self.running or not queue.empty():
            try:
                # Attempt to get update item
                try:
                    update_item = await asyncio.wait_for(
                        queue.get(), 
                        timeout=self.batch_timeout
                    )
                    
                    # Merge multiple updates for the same twin
                    twin_id = update_item.twin_id
                    if twin_id not in batch:
                        batch[twin_id] = {
                            "properties": {},
                            "timestamp": update_item.timestamp
                        }
                    
                    # Update properties (later overwrite earlier)
                    batch[twin_id]["properties"].update(update_item.properties)
                    # Use latest timestamp
                    if update_item.timestamp > batch[twin_id]["timestamp"]:
                        batch[twin_id]["timestamp"] = update_item.timestamp
                    
                except asyncio.TimeoutError:
                    pass
                
                # Check if flush is needed
                current_time = asyncio.get_event_loop().time()
                should_flush = (
                    len(batch) >= self.max_batch_size or
                    (current_time - last_flush_time >= self.batch_timeout and batch) or
                    (not self.running and batch)
                )
                
                if should_flush:
                    await self._execute_api_batch_update(environment_id, batch)
                    batch.clear()
                    last_flush_time = current_time
                    
            except Exception as e:
                logger.error(f"Batch update loop error for environment {environment_id}: {e}")
                await asyncio.sleep(1)

    async def _execute_api_batch_update(self, environment_id: str, batch: Dict):
        """Execute API batch update"""
        if not batch:
            return
        
        logger.info("Using unified UTC timestamp formatting with Z suffix!")
            
        # Prepare batch update data
        batch_updates = []
        for twin_id, data in batch.items():
            # Unified timestamp formatting - ensure UTC and Z suffix
            timestamp = data["timestamp"]
            if timestamp.tzinfo is not None:
                utc_timestamp = timestamp.astimezone(timezone.utc)
            else:
                utc_timestamp = timestamp.replace(tzinfo=timezone.utc)
            
            timestamp_str = utc_timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
            
            batch_updates.append({
                "twin_id": twin_id,
                "properties": data["properties"],
                "telemetry_last_updated": timestamp_str
            })
            
        logger.info(f"Unified timestamp example: {batch_updates[0]['telemetry_last_updated'] if batch_updates else 'N/A'}")
        
        # Call batch update API
        url = f"{self.api_base_url}/environments/{environment_id}/twins/batch-update"
        
        for attempt in range(3):  # max 3 retries
            try:
                async with self.http_session.post(url, json=batch_updates) as response:
                    if response.status == 200:
                        result = await response.json()
                        success_count = result.get("success_count", 0)
                        self.metrics.twins_updated += success_count
                        
                        if success_count != len(batch_updates):
                            failed_count = len(batch_updates) - success_count
                            logger.warning(f"Partial batch update failure: {failed_count} Twins failed")
                            self.metrics.update_failures += failed_count
                        
                        # Send Twin properties update notifications
                        await self._send_twin_update_notifications(environment_id, batch)
                        
                        logger.info(f"Successfully batch updated {success_count} Twins")
                        break
                        
                    elif response.status == 429:  # Rate limited
                        wait_time = 2 ** attempt
                        logger.warning(f"API rate limited, waiting {wait_time}s before retry")
                        await asyncio.sleep(wait_time)
                        
                    else:
                        error_text = await response.text()
                        logger.error(f"API call failed: {response.status} - {error_text}")
                        if attempt == 2:  # last attempt
                            self.metrics.update_failures += len(batch_updates)
                        
            except asyncio.TimeoutError:
                logger.warning(f"API call timeout (attempt {attempt + 1}/3)")
                if attempt == 2:
                    self.metrics.update_failures += len(batch_updates)
                    
            except Exception as e:
                logger.error(f"API call exception: {e}")
                if attempt == 2:
                    self.metrics.update_failures += len(batch_updates)

    async def _send_twin_update_notifications(self, environment_id: str, batch: Dict):
        """Send Twin properties update notifications"""
        if not self.notification_service:
            return
        
        try:
            for twin_id, data in batch.items():
                # Send Twin properties update notification
                await self.notification_service.send_twin_properties_updated_notification(
                    environment_id=environment_id,
                    twin_id=twin_id,
                    updated_properties=data["properties"],
                    timestamp=data["timestamp"]
                )
                self.metrics.notifications_sent += 1
                
        except Exception as e:
            logger.error(f"Failed to send Twin update notifications: {e}")

    async def monitor_loop(self):
        """Monitoring loop"""
        while self.running:
            await asyncio.sleep(30)
            self.metrics.log_status()
            
            # Output queue status
            for env_id, queue in self.update_queues.items():
                if queue.qsize() > 0:
                    logger.info(f"Environment {env_id} queue size: {queue.qsize()}")

    async def status_broadcaster_loop(self):
        """Status broadcasting loop - periodically sends system status to all environments"""
        while self.running:
            try:
                await asyncio.sleep(60)  # broadcast every minute
                
                # Build system status
                status = {
                    "service": "twin_updater",
                    "status": "running",
                    "uptime_seconds": int((datetime.now(timezone.utc) - self.metrics.start_time).total_seconds()),
                    "metrics": {
                        "messages_consumed": self.metrics.messages_consumed,
                        "twins_updated": self.metrics.twins_updated,
                        "update_failures": self.metrics.update_failures,
                        "notifications_sent": self.metrics.notifications_sent
                    },
                    "queue_sizes": {
                        env_id: queue.qsize() 
                        for env_id, queue in self.update_queues.items()
                    }
                }
                
                # Broadcast status to all active environments
                for env_id in self.update_queues.keys():
                    if self.notification_service:
                        await self.notification_service.send_system_status_notification(env_id, status)
                
            except Exception as e:
                logger.error(f"Status broadcast failed: {e}")

    async def stop(self):
        """Graceful shutdown"""
        logger.info("Stopping Twin Updater Service...")
        self.running = False
        
        # Wait for all queues to empty
        for env_id, queue in self.update_queues.items():
            wait_time = 0
            while not queue.empty() and wait_time < 30:
                logger.info(f"Waiting for environment {env_id} queue to empty: {queue.qsize()}")
                await asyncio.sleep(1)
                wait_time += 1
        
        # Cancel all tasks
        for task in self._tasks:
            if not task.done():
                task.cancel()
        
        await asyncio.gather(*self._tasks, return_exceptions=True)
        
        # Close connections
        if self.http_session:
            await self.http_session.close()
        if self.redis_client:
            await self.redis_client.close()
        if self.consumer:
            await self.consumer.stop()
        if self.db_client:
            self.db_client.close()
            
        self.metrics.log_status()
        logger.info("Service stopped completely")


# WebSocket notification listener service (runs inside FastAPI app)
class WebSocketNotificationListener:
    """WebSocket notification listener service - listens to Redis channels and forwards to WebSocket clients"""
    
    def __init__(self, redis_client, websocket_manager):
        self.redis_client = redis_client
        self.websocket_manager = websocket_manager
        self.running = False
        self.subscription_task = None
    
    async def start(self):
        """Start listener service"""
        self.running = True
        self.subscription_task = asyncio.create_task(self._listen_for_notifications())
        logger.info("WebSocket notification listener service started")
    
    async def stop(self):
        """Stop listener service"""
        self.running = False
        if self.subscription_task:
            self.subscription_task.cancel()
        logger.info("WebSocket notification listener service stopped")
    
    async def _listen_for_notifications(self):
        """Listen for Redis notifications"""
        pubsub = self.redis_client.pubsub()
        
        try:
            # Subscribe to all WebSocket notification channels
            await pubsub.psubscribe("ws_notifications:*")
            
            while self.running:
                try:
                    message = await pubsub.get_message(timeout=1.0)
                    if message and message['type'] == 'pmessage':
                        await self._handle_notification(message)
                        
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"Failed to process notification message: {e}")
                    
        except Exception as e:
            logger.error(f"Redis subscription failed: {e}")
        finally:
            await pubsub.unsubscribe()
    
    async def _handle_notification(self, message):
        """Handle notification message"""
        try:
            # Parse channel name to get environment_id
            channel = message['channel'].decode('utf-8')
            environment_id = channel.split(':')[1]
            
            # Parse message content
            notification_data = json.loads(message['data'].decode('utf-8'))
            
            # Forward to WebSocket connections for the environment
            await self.websocket_manager.broadcast_to_environment(
                notification_data, environment_id
            )
            
            logger.debug(f"Forwarded notification to environment {environment_id}: {notification_data['type']}")
            
        except Exception as e:
            logger.error(f"Failed to handle WebSocket notification: {e}")


async def main():
    """Main function - start Twin Updater Service"""
    service = OptimizedTwinUpdaterService()
    
    try:
        await service.start()
    except KeyboardInterrupt:
        logger.info("Stop signal received")
    except Exception as e:
        logger.error(f"Service runtime error: {e}")
    finally:
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())