import asyncio
import aiohttp
import json
import logging
import os  # 添加缺少的 import
import redis.asyncio as redis
from datetime import datetime, timezone
from typing import Dict, List, Optional
from dataclasses import dataclass
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class BatchUpdateItem: 
    twin_id: str
    environment_id: str
    properties: Dict
    timestamp: datetime

class Metrics:
    """性能指标收集器"""
    def __init__(self):
        self.messages_consumed = 0
        self.twins_updated = 0
        self.update_failures = 0
        self.notifications_sent = 0
        self.start_time = datetime.now()
    
    def log_status(self):
        """输出状态统计"""
        uptime = datetime.now() - self.start_time
        logger.info(f"状态统计 - 运行时间: {uptime}, 消息消费: {self.messages_consumed}, "
                   f"Twin更新: {self.twins_updated}, 失败次数: {self.update_failures}, "
                   f"通知发送: {self.notifications_sent}")

class TelemetryMessage:
    """遥测消息模型"""
    def __init__(self, **kwargs):
        self.environment_id = kwargs.get('environment_id')
        self.device_id = kwargs.get('device_id')
        self.payload = kwargs.get('payload', {})
        self.timestamp = kwargs.get('timestamp')
        if isinstance(self.timestamp, str):
            self.timestamp = datetime.fromisoformat(self.timestamp.replace('Z', '+00:00'))


class Settings:
    """配置管理"""
    # API配置 - 智能默认值
    API_BASE_URL = os.getenv("API_BASE_URL", "http://host.docker.internal:8000")
    API_BATCH_SIZE = int(os.getenv("API_BATCH_SIZE", "50"))
    API_BATCH_TIMEOUT = float(os.getenv("API_BATCH_TIMEOUT", "5.0"))  # 秒
    
    # Kafka配置
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telemetry")
    KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "twin-updater")
    KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")
    
    # MongoDB配置 - 确保容器环境下的正确默认值
    MONGO_DB_URL = os.getenv("MONGO_DB_URL", "mongodb://my-mongo:27017")
    MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "digital_twin_db")
    
    # Redis配置 - 智能默认值
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6380")
    
    # WebSocket通知配置
    WEBSOCKET_NOTIFICATION_ENABLED = os.getenv("WEBSOCKET_NOTIFICATION_ENABLED", "true").lower() == "true"

settings = Settings()

class WebSocketNotificationService:
    """WebSocket通知服务"""
    
    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.enabled = settings.WEBSOCKET_NOTIFICATION_ENABLED
    
    async def send_twin_properties_updated_notification(self, environment_id: str, twin_id: str, 
                                                       updated_properties: Dict, timestamp: datetime):
        """发送Twin属性更新通知"""
        if not self.enabled:
            return
        try:
            # 统一时间戳格式化
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
            
            # 发布到Redis频道，WebSocket服务器会监听这个频道
            channel = f"ws_notifications:{environment_id}"
            await self.redis_client.publish(channel, json.dumps(notification))
            
            logger.debug(f"发送Twin属性更新通知: {twin_id} -> {environment_id}")
            
        except Exception as e:
            logger.error(f"发送Twin属性更新通知失败: {e}")
    
    async def send_system_status_notification(self, environment_id: str, status: Dict):
        """发送系统状态通知"""
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
            logger.error(f"发送系统状态通知失败: {e}")

class OptimizedTwinUpdaterService:
    """优化的Twin更新服务 - 支持实时通知"""
    
    def __init__(self):
        self.running = True
        self.consumer = None
        self.db_client = None
        self.db = None
        self.http_session = None
        self.redis_client = None
        self.notification_service = None
        self._tasks = []
        
        # 批量更新队列 - 按environment分组
        self.update_queues = {}  # environment_id -> Queue
        self.metrics = Metrics()
        
        # API配置
        self.api_base_url = settings.API_BASE_URL
        self.api_timeout = aiohttp.ClientTimeout(total=30)
        self.max_batch_size = settings.API_BATCH_SIZE
        self.batch_timeout = settings.API_BATCH_TIMEOUT

    async def connect_to_databases(self):
        """初始化连接"""
        try:
            # 打印调试信息
            logger.info(f"=== 服务配置信息 ===")
            logger.info(f"MongoDB URL: {settings.MONGO_DB_URL}")
            logger.info(f"MongoDB DB Name: {settings.MONGO_DB_NAME}")
            logger.info(f"Redis URL: {settings.REDIS_URL}")
            logger.info(f"API Base URL: {settings.API_BASE_URL}")
            logger.info(f"Kafka Servers: {settings.KAFKA_BOOTSTRAP_SERVERS}")
            logger.info(f"==================")
            
            # MongoDB连接（用于查询映射关系）
            logger.info(f"尝试连接MongoDB: {settings.MONGO_DB_URL}")
            self.db_client = AsyncIOMotorClient(settings.MONGO_DB_URL, serverSelectionTimeoutMS=10000)
            self.db = self.db_client[settings.MONGO_DB_NAME]
            await self.db.command('ping')
            logger.info("MongoDB连接成功")
        except Exception as e:
            logger.error(f"MongoDB连接失败: {e}")
            raise
        
        try:
            # Redis连接（用于WebSocket通知）
            logger.info(f"尝试连接Redis: {settings.REDIS_URL}")
            self.redis_client = redis.from_url(settings.REDIS_URL)
            await self.redis_client.ping()
            logger.info("Redis连接成功")
        except Exception as e:
            logger.warning(f"Redis连接失败: {e} - WebSocket通知将被禁用")
            self.redis_client = None
        
        # 初始化通知服务
        if self.redis_client:
            self.notification_service = WebSocketNotificationService(self.redis_client)
        
        # HTTP Session with connection pooling
        connector = aiohttp.TCPConnector(
            limit=100,  # 总连接池大小
            limit_per_host=20,  # 每个host的连接数
            keepalive_timeout=60,
            enable_cleanup_closed=True
        )
        
        self.http_session = aiohttp.ClientSession(
            connector=connector,
            timeout=self.api_timeout,
            headers={'Content-Type': 'application/json'}
        )
        
        logger.info("数据库和HTTP连接初始化完成")

    async def start(self):
        """启动服务"""
        await self.connect_to_databases()
        
        try:
            from aiokafka import AIOKafkaConsumer
            logger.info(f"尝试连接Kafka: {settings.KAFKA_BOOTSTRAP_SERVERS}")
            
            self.consumer = AIOKafkaConsumer(
                settings.KAFKA_TOPIC,
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                group_id=settings.KAFKA_GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset=settings.KAFKA_AUTO_OFFSET_RESET,
                consumer_timeout_ms=1000,  # 添加超时，避免阻塞
            )

            await self.consumer.start()
            logger.info("Kafka连接成功")
        except Exception as e:
            logger.error(f"Kafka连接失败: {e}")
            raise
        
        self._tasks = [
            asyncio.create_task(self.consume_loop()),
            asyncio.create_task(self.monitor_loop()),
            asyncio.create_task(self.status_broadcaster_loop())  # 状态广播循环
        ]
        
        logger.info("Twin Updater Service启动成功")
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def consume_loop(self):
        """消费Kafka消息"""
        try:
            async for msg in self.consumer:
                if not self.running:
                    break
                    
                try:
                    telemetry = TelemetryMessage(**msg.value)
                    self.metrics.messages_consumed += 1
                    
                    # 查询设备映射关系
                    mapping = await self.db["device_twin_mappings"].find_one({
                        "environment_id": telemetry.environment_id,
                        "device_id": telemetry.device_id
                    })
                    
                    if not mapping:
                        logger.debug(f"设备 {telemetry.device_id} 无映射关系")
                        continue
                    
                    # 创建更新项
                    update_item = BatchUpdateItem(
                        twin_id=mapping["twin_id"],
                        environment_id=telemetry.environment_id,
                        properties=telemetry.payload,
                        timestamp=telemetry.timestamp
                    )
                    
                    # 添加到对应环境的批量队列
                    await self._add_to_batch_queue(update_item)
                    
                except Exception as e:
                    logger.error(f"处理消息失败: {e}")
                    self.metrics.update_failures += 1
        except Exception as e:
            logger.error(f"消费循环异常: {e}")
            if self.running:
                raise

    async def _add_to_batch_queue(self, update_item: BatchUpdateItem):
        """添加到批量更新队列"""
        env_id = update_item.environment_id
        
        # 确保环境队列存在
        if env_id not in self.update_queues:
            self.update_queues[env_id] = asyncio.Queue(maxsize=10000)
            # 为这个环境启动批量处理任务
            task = asyncio.create_task(self._batch_update_loop(env_id))
            self._tasks.append(task)
        
        try:
            await self.update_queues[env_id].put(update_item)
        except asyncio.QueueFull:
            logger.warning(f"环境 {env_id} 的更新队列已满")
            self.metrics.update_failures += 1

    async def _batch_update_loop(self, environment_id: str):
        """针对特定环境的批量更新循环"""
        queue = self.update_queues[environment_id]
        batch = {}  # twin_id -> properties
        last_flush_time = asyncio.get_event_loop().time()
        
        while self.running or not queue.empty():
            try:
                # 尝试获取更新项
                try:
                    update_item = await asyncio.wait_for(
                        queue.get(), 
                        timeout=self.batch_timeout
                    )
                    
                    # 合并同一twin的多次更新
                    twin_id = update_item.twin_id
                    if twin_id not in batch:
                        batch[twin_id] = {
                            "properties": {},
                            "timestamp": update_item.timestamp
                        }
                    
                    # 更新属性（后来的覆盖前面的）
                    batch[twin_id]["properties"].update(update_item.properties)
                    # 使用最新的时间戳
                    if update_item.timestamp > batch[twin_id]["timestamp"]:
                        batch[twin_id]["timestamp"] = update_item.timestamp
                    
                except asyncio.TimeoutError:
                    pass
                
                # 检查是否需要刷新
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
                logger.error(f"环境 {environment_id} 批量更新循环错误: {e}")
                await asyncio.sleep(1)

    async def _execute_api_batch_update(self, environment_id: str, batch: Dict):
        """执行API批量更新"""
        if not batch:
            return
        
        logger.info("🔧 使用修复后的时间戳格式化代码!")  # 明显的标识
            
        # 准备批量更新数据
        batch_updates = []
        for twin_id, data in batch.items():
            # 简单且可靠的时间戳格式化
            timestamp = data["timestamp"]
            # 统一转换为UTC时间并格式化为Z结尾的ISO格式
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
            
        logger.info(f"🕐 修复后的时间戳格式示例: {batch_updates[0]['telemetry_last_updated'] if batch_updates else 'N/A'}")
        
        # 调用批量更新API
        url = f"{self.api_base_url}/environments/{environment_id}/twins/batch-update"
        
        for attempt in range(3):  # 最多重试3次
            try:
                async with self.http_session.post(url, json=batch_updates) as response:
                    if response.status == 200:
                        result = await response.json()
                        success_count = result.get("success_count", 0)
                        self.metrics.twins_updated += success_count
                        
                        if success_count != len(batch_updates):
                            failed_count = len(batch_updates) - success_count
                            logger.warning(f"批量更新部分失败: {failed_count} 个Twin更新失败")
                            self.metrics.update_failures += failed_count
                        
                        # 发送Twin属性更新通知
                        await self._send_twin_update_notifications(environment_id, batch)
                        
                        logger.info(f"✅ 成功批量更新 {success_count} 个Twin")
                        break
                        
                    elif response.status == 429:  # Rate limited
                        wait_time = 2 ** attempt
                        logger.warning(f"API限流，等待 {wait_time}s 后重试")
                        await asyncio.sleep(wait_time)
                        
                    else:
                        error_text = await response.text()
                        logger.error(f"API调用失败: {response.status} - {error_text}")
                        if attempt == 2:  # 最后一次尝试
                            self.metrics.update_failures += len(batch_updates)
                        
            except asyncio.TimeoutError:
                logger.warning(f"API调用超时 (尝试 {attempt + 1}/3)")
                if attempt == 2:
                    self.metrics.update_failures += len(batch_updates)
                    
            except Exception as e:
                logger.error(f"API调用异常: {e}")
                if attempt == 2:
                    self.metrics.update_failures += len(batch_updates)

    async def _send_twin_update_notifications(self, environment_id: str, batch: Dict):
        """发送Twin属性更新通知"""
        if not self.notification_service:
            return
        
        try:
            for twin_id, data in batch.items():
                # 发送Twin属性更新通知
                await self.notification_service.send_twin_properties_updated_notification(
                    environment_id=environment_id,
                    twin_id=twin_id,
                    updated_properties=data["properties"],
                    timestamp=data["timestamp"]
                )
                self.metrics.notifications_sent += 1
                
        except Exception as e:
            logger.error(f"发送Twin更新通知失败: {e}")

    async def monitor_loop(self):
        """监控循环"""
        while self.running:
            await asyncio.sleep(30)
            self.metrics.log_status()
            
            # 输出队列状态
            for env_id, queue in self.update_queues.items():
                if queue.qsize() > 0:
                    logger.info(f"环境 {env_id} 队列大小: {queue.qsize()}")

    async def status_broadcaster_loop(self):
        """状态广播循环 - 定期向所有环境发送系统状态"""
        while self.running:
            try:
                await asyncio.sleep(60)  # 每分钟广播一次状态
                
                # 构建系统状态
                status = {
                    "service": "twin_updater",
                    "status": "running",
                    "uptime_seconds": int((datetime.now() - self.metrics.start_time).total_seconds()),
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
                
                # 向所有活跃环境广播状态
                for env_id in self.update_queues.keys():
                    if self.notification_service:
                        await self.notification_service.send_system_status_notification(env_id, status)
                
            except Exception as e:
                logger.error(f"状态广播失败: {e}")

    async def stop(self):
        """优雅停止"""
        logger.info("正在停止Twin Updater Service...")
        self.running = False
        
        # 等待所有队列清空
        for env_id, queue in self.update_queues.items():
            wait_time = 0
            while not queue.empty() and wait_time < 30:
                logger.info(f"等待环境 {env_id} 队列清空: {queue.qsize()}")
                await asyncio.sleep(1)
                wait_time += 1
        
        # 取消所有任务
        for task in self._tasks:
            if not task.done():
                task.cancel()
        
        await asyncio.gather(*self._tasks, return_exceptions=True)
        
        # 关闭连接
        if self.http_session:
            await self.http_session.close()
        if self.redis_client:
            await self.redis_client.close()
        if self.consumer:
            await self.consumer.stop()
        if self.db_client:
            self.db_client.close()
            
        self.metrics.log_status()
        logger.info("服务已完全停止")

# WebSocket通知监听服务（在FastAPI应用中运行）
class WebSocketNotificationListener:
    """WebSocket通知监听服务 - 监听Redis频道并转发给WebSocket客户端"""
    
    def __init__(self, redis_client, websocket_manager):
        self.redis_client = redis_client
        self.websocket_manager = websocket_manager
        self.running = False
        self.subscription_task = None
    
    async def start(self):
        """启动监听服务"""
        self.running = True
        self.subscription_task = asyncio.create_task(self._listen_for_notifications())
        logger.info("WebSocket通知监听服务已启动")
    
    async def stop(self):
        """停止监听服务"""
        self.running = False
        if self.subscription_task:
            self.subscription_task.cancel()
        logger.info("WebSocket通知监听服务已停止")
    
    async def _listen_for_notifications(self):
        """监听Redis通知频道"""
        pubsub = self.redis_client.pubsub()
        
        try:
            # 订阅所有WebSocket通知频道
            await pubsub.psubscribe("ws_notifications:*")
            
            while self.running:
                try:
                    message = await pubsub.get_message(timeout=1.0)
                    if message and message['type'] == 'pmessage':
                        await self._handle_notification(message)
                        
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"处理通知消息失败: {e}")
                    
        except Exception as e:
            logger.error(f"Redis订阅失败: {e}")
        finally:
            await pubsub.unsubscribe()
    
    async def _handle_notification(self, message):
        """处理通知消息"""
        try:
            # 解析频道名获取environment_id
            channel = message['channel'].decode('utf-8')
            environment_id = channel.split(':')[1]
            
            # 解析消息内容
            notification_data = json.loads(message['data'].decode('utf-8'))
            
            # 转发给对应环境的WebSocket连接
            await self.websocket_manager.broadcast_to_environment(
                notification_data, environment_id
            )
            
            logger.debug(f"转发通知到环境 {environment_id}: {notification_data['type']}")
            
        except Exception as e:
            logger.error(f"处理WebSocket通知失败: {e}")

# 主函数
async def main():
    """主函数 - 启动Twin Updater Service"""
    service = OptimizedTwinUpdaterService()
    
    try:
        await service.start()
    except KeyboardInterrupt:
        logger.info("收到停止信号")
    except Exception as e:
        logger.error(f"服务运行错误: {e}")
    finally:
        await service.stop()

if __name__ == "__main__":
    # 运行主服务
    asyncio.run(main())