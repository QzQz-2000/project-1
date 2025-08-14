import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Set, Callable
from dataclasses import dataclass, field
from enum import Enum
import uuid
import websockets
import aioredis
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import redis
from concurrent.futures import ThreadPoolExecutor
import threading
import time
from collections import defaultdict
import weakref

# ==================== 数据更新事件定义 ====================

class UpdateType(Enum):
    PROPERTY_UPDATE = "property_update"
    STATUS_CHANGE = "status_change"
    CONFIGURATION_UPDATE = "configuration_update"
    METADATA_UPDATE = "metadata_update"
    FULL_SYNC = "full_sync"

class UpdateSource(Enum):
    DEVICE_REPORTED = "device_reported"
    CLOUD_DESIRED = "cloud_desired"
    SYSTEM_INTERNAL = "system_internal"
    USER_MANUAL = "user_manual"
    BATCH_IMPORT = "batch_import"

@dataclass
class TwinUpdateEvent:
    """Twin更新事件"""
    event_id: str
    device_id: str
    tenant_id: str
    update_type: UpdateType
    source: UpdateSource
    timestamp: datetime
    data: Dict[str, Any]
    version: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class TwinSnapshot:
    """Twin快照"""
    device_id: str
    tenant_id: str
    timestamp: datetime
    properties: Dict[str, Any]
    status: str
    configuration: Dict[str, Any]
    version: int

# ==================== 实时更新处理器 ====================

class RealTimeTwinUpdateProcessor:
    """实时Twin更新处理器"""
    
    def __init__(self, kafka_producer: KafkaProducer, redis_client, influx_client=None):
        self.kafka_producer = kafka_producer
        self.redis_client = redis_client
        self.influx_client = influx_client
        self.logger = logging.getLogger(__name__)
        
        # 订阅者管理
        self.subscribers: Dict[str, Set[Callable]] = defaultdict(set)
        self.websocket_connections: Dict[str, Set[websockets.WebSocketServerProtocol]] = defaultdict(set)
        
        # 批量处理
        self.batch_buffer: Dict[str, List[TwinUpdateEvent]] = defaultdict(list)
        self.batch_lock = threading.Lock()
        self.batch_size = 100
        self.batch_timeout = 5  # 秒
        
        # 性能优化
        self.update_cache = {}  # 短期缓存避免重复更新
        self.cache_ttl = 1  # 缓存1秒
        
        # 版本控制
        self.version_vectors: Dict[str, int] = {}
        
        self.running = False
    
    async def start(self):
        """启动实时更新处理器"""
        self.running = True
        
        # 启动消费者任务
        asyncio.create_task(self._consume_update_events())
        asyncio.create_task(self._batch_processor())
        asyncio.create_task(self._cleanup_cache())
        
        self.logger.info("实时Twin更新处理器已启动")
    
    # ==================== 数据更新处理 ====================
    
    async def process_device_data(self, device_id: str, tenant_id: str, 
                                device_data: Dict[str, Any], source: UpdateSource = UpdateSource.DEVICE_REPORTED):
        """处理设备数据更新"""
        
        # 1. 创建更新事件
        update_event = TwinUpdateEvent(
            event_id=str(uuid.uuid4()),
            device_id=device_id,
            tenant_id=tenant_id,
            update_type=UpdateType.PROPERTY_UPDATE,
            source=source,
            timestamp=datetime.now(),
            data=device_data,
            version=await self._get_next_version(device_id)
        )
        
        # 2. 检查缓存，避免重复更新
        cache_key = f"{device_id}:{hash(json.dumps(device_data, sort_keys=True))}"
        current_time = time.time()
        
        if cache_key in self.update_cache:
            last_update_time = self.update_cache[cache_key]
            if current_time - last_update_time < self.cache_ttl:
                return  # 跳过重复更新
        
        self.update_cache[cache_key] = current_time
        
        # 3. 立即更新Redis中的Twin数据
        await self._update_twin_in_redis(update_event)
        
        # 4. 发送实时通知
        await self._notify_subscribers(update_event)
        
        # 5. 添加到批量处理队列
        await self._add_to_batch(update_event)
        
        # 6. 发送到Kafka供其他组件消费
        await self._publish_update_event(update_event)
    
    async def update_twin_desired_properties(self, device_id: str, tenant_id: str, 
                                           properties: Dict[str, Any], user_id: str = None):
        """更新Twin期望属性"""
        
        update_event = TwinUpdateEvent(
            event_id=str(uuid.uuid4()),
            device_id=device_id,
            tenant_id=tenant_id,
            update_type=UpdateType.CONFIGURATION_UPDATE,
            source=UpdateSource.CLOUD_DESIRED,
            timestamp=datetime.now(),
            data={"desired_properties": properties},
            version=await self._get_next_version(device_id),
            metadata={"user_id": user_id} if user_id else {}
        )
        
        # 更新Redis
        await self._update_twin_desired_in_redis(update_event)
        
        # 通知订阅者
        await self._notify_subscribers(update_event)
        
        # 发送到设备
        await self._notify_device_desired_change(device_id, tenant_id, properties)
        
        # 发布事件
        await self._publish_update_event(update_event)
    
    async def update_twin_status(self, device_id: str, tenant_id: str, 
                                new_status: str, reason: str = None):
        """更新Twin状态"""
        
        update_event = TwinUpdateEvent(
            event_id=str(uuid.uuid4()),
            device_id=device_id,
            tenant_id=tenant_id,
            update_type=UpdateType.STATUS_CHANGE,
            source=UpdateSource.SYSTEM_INTERNAL,
            timestamp=datetime.now(),
            data={"status": new_status, "reason": reason},
            version=await self._get_next_version(device_id)
        )
        
        await self._update_twin_status_in_redis(update_event)
        await self._notify_subscribers(update_event)
        await self._publish_update_event(update_event)
    
    # ==================== Redis更新操作 ====================
    
    async def _update_twin_in_redis(self, event: TwinUpdateEvent):
        """更新Redis中的Twin数据"""
        twin_key = f"twin:{event.tenant_id}:{event.device_id}"
        
        # 使用Redis管道提高性能
        pipe = self.redis_client.pipeline()
        
        # 更新属性
        for prop_name, prop_value in event.data.items():
            if prop_name in ['data', 'properties']:  # 嵌套数据处理
                for nested_key, nested_value in prop_value.items():
                    pipe.hset(f"{twin_key}:properties", nested_key, json.dumps({
                        "value": nested_value,
                        "timestamp": event.timestamp.isoformat(),
                        "version": event.version,
                        "source": event.source.value
                    }))
            else:
                pipe.hset(f"{twin_key}:properties", prop_name, json.dumps({
                    "value": prop_value,
                    "timestamp": event.timestamp.isoformat(),
                    "version": event.version,
                    "source": event.source.value
                }))
        
        # 更新基本信息
        pipe.hset(twin_key, mapping={
            "last_seen": event.timestamp.isoformat(),
            "last_update_version": event.version,
            "status": "online"
        })
        
        # 设置TTL
        pipe.expire(twin_key, 86400)  # 24小时
        pipe.expire(f"{twin_key}:properties", 86400)
        
        # 执行管道
        await pipe.execute()
        
        # 更新版本向量
        self.version_vectors[event.device_id] = event.version
    
    async def _update_twin_desired_in_redis(self, event: TwinUpdateEvent):
        """更新期望属性"""
        twin_key = f"twin:{event.tenant_id}:{event.device_id}"
        desired_key = f"{twin_key}:desired"
        
        pipe = self.redis_client.pipeline()
        
        for prop_name, prop_value in event.data.get("desired_properties", {}).items():
            pipe.hset(desired_key, prop_name, json.dumps({
                "value": prop_value,
                "timestamp": event.timestamp.isoformat(),
                "version": event.version
            }))
        
        pipe.expire(desired_key, 86400)
        await pipe.execute()
    
    async def _update_twin_status_in_redis(self, event: TwinUpdateEvent):
        """更新Twin状态"""
        twin_key = f"twin:{event.tenant_id}:{event.device_id}"
        
        await self.redis_client.hset(twin_key, mapping={
            "status": event.data["status"],
            "status_reason": event.data.get("reason", ""),
            "status_updated_at": event.timestamp.isoformat(),
            "version": event.version
        })
    
    # ==================== 实时通知系统 ====================
    
    def subscribe_to_twin_updates(self, device_id: str, callback: Callable[[TwinUpdateEvent], None]):
        """订阅Twin更新"""
        self.subscribers[device_id].add(callback)
        self.logger.debug(f"新增订阅者: {device_id}")
    
    def unsubscribe_from_twin_updates(self, device_id: str, callback: Callable):
        """取消订阅"""
        if device_id in self.subscribers:
            self.subscribers[device_id].discard(callback)
    
    async def add_websocket_connection(self, device_id: str, websocket: websockets.WebSocketServerProtocol):
        """添加WebSocket连接"""
        self.websocket_connections[device_id].add(websocket)
        
        # 发送当前Twin状态
        current_twin = await self._get_twin_snapshot(device_id)
        if current_twin:
            await self._send_websocket_message(websocket, {
                "type": "twin_snapshot",
                "data": current_twin.__dict__
            })
    
    async def remove_websocket_connection(self, device_id: str, websocket: websockets.WebSocketServerProtocol):
        """移除WebSocket连接"""
        if device_id in self.websocket_connections:
            self.websocket_connections[device_id].discard(websocket)
    
    async def _notify_subscribers(self, event: TwinUpdateEvent):
        """通知所有订阅者"""
        device_id = event.device_id
        
        # 通知回调函数订阅者
        if device_id in self.subscribers:
            for callback in self.subscribers[device_id]:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(event)
                    else:
                        callback(event)
                except Exception as e:
                    self.logger.error(f"通知订阅者失败: {e}")
        
        # 通知WebSocket连接
        await self._notify_websocket_connections(event)
        
        # 通知全局订阅者（监控、日志等）
        if "*" in self.subscribers:
            for callback in self.subscribers["*"]:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(event)
                    else:
                        callback(event)
                except Exception as e:
                    self.logger.error(f"通知全局订阅者失败: {e}")
    
    async def _notify_websocket_connections(self, event: TwinUpdateEvent):
        """通知WebSocket连接"""
        device_id = event.device_id
        
        if device_id not in self.websocket_connections:
            return
        
        message = {
            "type": "twin_update",
            "event_id": event.event_id,
            "device_id": event.device_id,
            "update_type": event.update_type.value,
            "source": event.source.value,
            "timestamp": event.timestamp.isoformat(),
            "data": event.data,
            "version": event.version
        }
        
        # 发送给所有连接的客户端
        disconnected_websockets = set()
        
        for websocket in self.websocket_connections[device_id]:
            try:
                await self._send_websocket_message(websocket, message)
            except websockets.exceptions.ConnectionClosed:
                disconnected_websockets.add(websocket)
            except Exception as e:
                self.logger.error(f"WebSocket发送失败: {e}")
                disconnected_websockets.add(websocket)
        
        # 清理断开的连接
        for websocket in disconnected_websockets:
            self.websocket_connections[device_id].discard(websocket)
    
    async def _send_websocket_message(self, websocket: websockets.WebSocketServerProtocol, message: Dict[str, Any]):
        """发送WebSocket消息"""
        try:
            await websocket.send(json.dumps(message, default=str))
        except Exception as e:
            self.logger.error(f"WebSocket消息发送失败: {e}")
            raise
    
    # ==================== 批量处理和持久化 ====================
    
    async def _add_to_batch(self, event: TwinUpdateEvent):
        """添加到批量处理队列"""
        with self.batch_lock:
            self.batch_buffer[event.device_id].append(event)
    
    async def _batch_processor(self):
        """批量处理器"""
        while self.running:
            try:
                await asyncio.sleep(self.batch_timeout)
                
                with self.batch_lock:
                    current_batches = dict(self.batch_buffer)
                    self.batch_buffer.clear()
                
                if current_batches:
                    await self._process_batches(current_batches)
                    
            except Exception as e:
                self.logger.error(f"批量处理错误: {e}")
    
    async def _process_batches(self, batches: Dict[str, List[TwinUpdateEvent]]):
        """处理批量数据"""
        
        # 持久化到InfluxDB
        if self.influx_client:
            await self._persist_to_influxdb(batches)
        
        # 生成聚合统计
        await self._generate_statistics(batches)
        
        # 清理旧数据
        await self._cleanup_old_data(batches)
    
    async def _persist_to_influxdb(self, batches: Dict[str, List[TwinUpdateEvent]]):
        """持久化到InfluxDB"""
        try:
            from influxdb_client import Point
            
            points = []
            
            for device_id, events in batches.items():
                for event in events:
                    # 为每个属性创建数据点
                    for prop_name, prop_value in event.data.items():
                        if isinstance(prop_value, (int, float)):
                            point = Point("twin_properties") \
                                .tag("device_id", event.device_id) \
                                .tag("tenant_id", event.tenant_id) \
                                .tag("property_name", prop_name) \
                                .tag("source", event.source.value) \
                                .field("value", float(prop_value)) \
                                .field("version", event.version) \
                                .time(event.timestamp)
                            
                            points.append(point)
            
            if points:
                write_api = self.influx_client.write_api()
                write_api.write(bucket="twin-data", record=points)
                self.logger.debug(f"已持久化 {len(points)} 个数据点到InfluxDB")
                
        except Exception as e:
            self.logger.error(f"InfluxDB持久化失败: {e}")
    
    # ==================== 辅助方法 ====================
    
    async def _get_next_version(self, device_id: str) -> int:
        """获取下一个版本号"""
        current_version = self.version_vectors.get(device_id, 0)
        next_version = current_version + 1
        self.version_vectors[device_id] = next_version
        return next_version
    
    async def _get_twin_snapshot(self, device_id: str) -> Optional[TwinSnapshot]:
        """获取Twin快照"""
        # 这里应该从Redis获取完整的Twin数据
        # 简化实现
        return TwinSnapshot(
            device_id=device_id,
            tenant_id="tenant1",  # 应该从实际数据获取
            timestamp=datetime.now(),
            properties={},
            status="online",
            configuration={},
            version=self.version_vectors.get(device_id, 1)
        )
    
    async def _publish_update_event(self, event: TwinUpdateEvent):
        """发布更新事件到Kafka"""
        try:
            message = {
                "event_id": event.event_id,
                "device_id": event.device_id,
                "tenant_id": event.tenant_id,
                "update_type": event.update_type.value,
                "source": event.source.value,
                "timestamp": event.timestamp.isoformat(),
                "data": event.data,
                "version": event.version,
                "metadata": event.metadata
            }
            
            self.kafka_producer.send(
                "twin.updates",
                value=json.dumps(message, default=str),
                key=event.device_id
            )
            
        except Exception as e:
            self.logger.error(f"发布Kafka事件失败: {e}")
    
    async def _notify_device_desired_change(self, device_id: str, tenant_id: str, properties: Dict[str, Any]):
        """通知设备期望属性变更"""
        message = {
            "type": "desired_properties_update",
            "device_id": device_id,
            "tenant_id": tenant_id,
            "properties": properties,
            "timestamp": datetime.now().isoformat()
        }
        
        self.kafka_producer.send(
            f"device.commands.{device_id}",
            value=json.dumps(message, default=str)
        )
    
    async def _consume_update_events(self):
        """消费更新事件"""
        consumer = KafkaConsumer(
            "iot.telemetry.tenant1",
            "iot.events.tenant1",
            bootstrap_servers=['localhost:9092'],
            group_id='twin-updater',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        while self.running:
            try:
                for message in consumer:
                    data = message.value
                    
                    await self.process_device_data(
                        device_id=data.get('device_id'),
                        tenant_id=data.get('tenant_id'),
                        device_data=data.get('data', {}),
                        source=UpdateSource.DEVICE_REPORTED
                    )
                    
            except Exception as e:
                self.logger.error(f"消费事件错误: {e}")
                await asyncio.sleep(5)
    
    async def _cleanup_cache(self):
        """清理缓存"""
        while self.running:
            try:
                await asyncio.sleep(60)  # 每分钟清理一次
                
                current_time = time.time()
                expired_keys = [
                    key for key, timestamp in self.update_cache.items()
                    if current_time - timestamp > self.cache_ttl * 10
                ]
                
                for key in expired_keys:
                    del self.update_cache[key]
                
            except Exception as e:
                self.logger.error(f"缓存清理错误: {e}")

# ==================== WebSocket服务器 ====================

class TwinWebSocketServer:
    """Twin WebSocket服务器"""
    
    def __init__(self, update_processor: RealTimeTwinUpdateProcessor):
        self.update_processor = update_processor
        self.logger = logging.getLogger(__name__)
    
    async def start_server(self, host="localhost", port=8765):
        """启动WebSocket服务器"""
        self.logger.info(f"启动Twin WebSocket服务器: ws://{host}:{port}")
        
        start_server = websockets.serve(
            self.handle_websocket_connection,
            host,
            port,
            ping_interval=30,
            ping_timeout=10
        )
        
        await start_server
    
    async def handle_websocket_connection(self, websocket, path):
        """处理WebSocket连接"""
        self.logger.info(f"新的WebSocket连接: {path}")
        
        try:
            # 解析路径获取设备ID
            device_id = self._extract_device_id_from_path(path)
            if not device_id:
                await websocket.close(code=4000, reason="Invalid device ID")
                return
            
            # 添加连接
            await self.update_processor.add_websocket_connection(device_id, websocket)
            
            # 保持连接
            async for message in websocket:
                await self._handle_websocket_message(websocket, device_id, message)
                
        except websockets.exceptions.ConnectionClosed:
            self.logger.info("WebSocket连接已关闭")
        except Exception as e:
            self.logger.error(f"WebSocket处理错误: {e}")
        finally:
            # 清理连接
            if 'device_id' in locals():
                await self.update_processor.remove_websocket_connection(device_id, websocket)
    
    def _extract_device_id_from_path(self, path: str) -> Optional[str]:
        """从路径提取设备ID"""
        # 期望路径格式: /twin/{device_id}
        parts = path.strip('/').split('/')
        if len(parts) >= 2 and parts[0] == 'twin':
            return parts[1]
        return None
    
    async def _handle_websocket_message(self, websocket, device_id: str, message: str):
        """处理WebSocket消息"""
        try:
            data = json.loads(message)
            message_type = data.get('type')
            
            if message_type == 'ping':
                await websocket.send(json.dumps({"type": "pong"}))
            
            elif message_type == 'subscribe_updates':
                # 客户端请求订阅更新
                await websocket.send(json.dumps({
                    "type": "subscription_confirmed",
                    "device_id": device_id
                }))
            
            elif message_type == 'get_current_state':
                # 客户端请求当前状态
                snapshot = await self.update_processor._get_twin_snapshot(device_id)
                await websocket.send(json.dumps({
                    "type": "current_state",
                    "data": snapshot.__dict__ if snapshot else None
                }))
            
        except json.JSONDecodeError:
            await websocket.send(json.dumps({
                "type": "error",
                "message": "Invalid JSON format"
            }))
        except Exception as e:
            self.logger.error(f"WebSocket消息处理错误: {e}")

# ==================== 使用示例 ====================

class TwinUpdateExample:
    """Twin更新系统使用示例"""
    
    async def run_example(self):
        """运行示例"""
        
        # 初始化组件
        from kafka import KafkaProducer
        import redis
        
        kafka_producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )
        
        redis_client = redis.Redis(host='localhost', port=6379, db=0)
        
        # 创建更新处理器
        update_processor = RealTimeTwinUpdateProcessor(
            kafka_producer=kafka_producer,
            redis_client=redis_client
        )
        
        # 启动处理器
        await update_processor.start()
        
        # 创建WebSocket服务器
        websocket_server = TwinWebSocketServer(update_processor)
        
        # 启动WebSocket服务器
        asyncio.create_task(websocket_server.start_server())
        
        print("=== Twin实时更新系统演示 ===")
        
        # 示例1：订阅Twin更新
        def on_twin_update(event: TwinUpdateEvent):
            print(f"Twin更新通知: {event.device_id} - {event.update_type.value}")
            print(f"数据: {event.data}")
        
        update_processor.subscribe_to_twin_updates("sensor_001", on_twin_update)
        
        # 示例2：模拟设备数据更新
        await asyncio.sleep(2)
        
        # 高频传感器数据
        for i in range(5):
            await update_processor.process_device_data(
                device_id="sensor_001",
                tenant_id="tenant1",
                device_data={
                    "temperature": 25.0 + i * 0.5,
                    "humidity": 60.0 + i * 2,
                    "timestamp": datetime.now().isoformat()
                }
            )
            await asyncio.sleep(1)
        
        # 示例3：更新期望属性
        await update_processor.update_twin_desired_properties(
            device_id="sensor_001",
            tenant_id="tenant1",
            properties={
                "sample_rate": 30,
                "alert_threshold": 35.0
            },
            user_id="admin"
        )
        
        # 示例4：状态变更
        await update_processor.update_twin_status(
            device_id="sensor_001",
            tenant_id="tenant1",
            new_status="maintenance",
            reason="Scheduled maintenance"
        )
        
        # 保持运行
        await asyncio.sleep(10)
        
        print("演示完成")

# ==================== 性能监控 ====================

class TwinUpdateMetrics:
    """Twin更新性能指标"""
    
    def __init__(self):
        self.metrics = {
            "updates_per_second": 0,
            "average_update_latency": 0,
            "websocket_connections": 0,
            "active_subscriptions": 0,
            "batch_processing_lag": 0
        }
    
    async def collect_metrics(self, update_processor: RealTimeTwinUpdateProcessor):
        """收集性能指标"""
        self.metrics["websocket_connections"] = sum(
            len(connections) for connections in update_processor.websocket_connections.values()
        )
        
        self.metrics["active_subscriptions"] = sum(
            len(subscribers) for subscribers in update_processor.subscribers.values()
        )
        
        return self.metrics

# ==================== 主程序 ====================

async def main():
    """主程序"""
    logging.basicConfig(level=logging.INFO)
    
    example = TwinUpdateExample()
    await example.run_example()

if __name__ == "__main__":
    asyncio.run(main())