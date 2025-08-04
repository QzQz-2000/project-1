import asyncio
import json
import uuid
from datetime import datetime
from typing import Dict, Any, Optional
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from kafka import KafkaProducer
from kafka.errors import KafkaError
import paho.mqtt.client as mqtt
import threading
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic 模型
class TelemetryData(BaseModel):
    environment_id: str
    device_id: str
    timestamp: Optional[datetime] = None
    data: Dict[str, Any]

class EventData(BaseModel):
    environment_id: str
    device_id: str
    timestamp: Optional[datetime] = None
    event_type: str
    data: Dict[str, Any]

class CommandData(BaseModel):
    environment_id: str
    device_id: str
    command: str
    payload: Dict[str, Any]
    request_id: str

# Kafka 生产者管理器
class KafkaManager:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        
    async def start(self):
        """启动 Kafka 生产者"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                batch_size=16384,
                linger_ms=10
            )
            logger.info("Kafka producer started successfully")
        except Exception as e:
            logger.error(f"Failed to start Kafka producer: {e}")
            raise
    
    async def stop(self):
        """停止 Kafka 生产者"""
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer stopped")
    
    async def send_message(self, topic: str, message: Dict[str, Any], key: str = None):
        """发送消息到 Kafka"""
        try:
            if not self.producer:
                raise Exception("Kafka producer not initialized")
            
            # 添加消息元数据
            enriched_message = {
                **message,
                "adapter_id": "fastapi-adapter",
                "received_at": datetime.now().isoformat(),
                "message_id": str(uuid.uuid4())
            }
            
            future = self.producer.send(topic, value=enriched_message, key=key)
            record_metadata = future.get(timeout=10)
            
            logger.info(f"Message sent to topic {topic}, partition {record_metadata.partition}, offset {record_metadata.offset}")
            return True
            
        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
            return False
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False

# MQTT 适配器
class MQTTAdapter:
    def __init__(self, kafka_manager: KafkaManager, broker_host: str = "localhost", broker_port: int = 1883):
        self.kafka_manager = kafka_manager
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client = None
        self.connected_devices = {}
        
    def start(self):
        """启动 MQTT 适配器"""
        self.client = mqtt.Client(client_id="fastapi-mqtt-adapter")
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        
        try:
            self.client.connect(self.broker_host, self.broker_port, 60)
            self.client.loop_start()
            logger.info(f"MQTT adapter started, connected to {self.broker_host}:{self.broker_port}")
        except Exception as e:
            logger.error(f"Failed to start MQTT adapter: {e}")
            raise
    
    def stop(self):
        """停止 MQTT 适配器"""
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("MQTT adapter stopped")
    
    def on_connect(self, client, userdata, flags, rc):
        """MQTT 连接回调"""
        if rc == 0:
            logger.info("MQTT adapter connected successfully")
            # 订阅设备主题
            client.subscribe("telemetry/+/+")
            client.subscribe("event/+/+")
            client.subscribe("command/+/+/res/+")
        else:
            logger.error(f"MQTT connection failed with code {rc}")
    
    def on_disconnect(self, client, userdata, rc):
        """MQTT 断开连接回调"""
        logger.info("MQTT adapter disconnected")
    
    def on_message(self, client, userdata, msg):
        """MQTT 消息处理回调"""
        try:
            topic_parts = msg.topic.split('/')
            if len(topic_parts) < 3:
                logger.warning(f"Invalid topic format: {msg.topic}")
                return
            
            message_type = topic_parts[0]
            environment_id = topic_parts[1]
            device_id = topic_parts[2]
            
            try:
                payload = json.loads(msg.payload.decode('utf-8'))
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON payload from {msg.topic}")
                return
            
            # 处理不同类型的消息
            if message_type == "telemetry":
                asyncio.create_task(self.handle_telemetry(environment_id, device_id, payload))
            elif message_type == "event":
                asyncio.create_task(self.handle_event(environment_id, device_id, payload))
            elif message_type == "command" and len(topic_parts) >= 5 and topic_parts[3] == "res":
                request_id = topic_parts[4]
                asyncio.create_task(self.handle_command_response(environment_id, device_id, request_id, payload))
            
        except Exception as e:
            logger.error(f"Error processing MQTT message: {e}")
    
    async def handle_telemetry(self, environment_id: str, device_id: str, payload: Dict[str, Any]):
        """处理遥测数据"""
        telemetry_message = {
            "type": "telemetry",
            "environment_id": environment_id,
            "device_id": device_id,
            "timestamp": datetime.now().isoformat(),
            "data": payload,
            "protocol": "mqtt"
        }
        
        topic = f"iot.telemetry.{environment_id}"
        key = f"{environment_id}:{device_id}"
        
        success = await self.kafka_manager.send_message(topic, telemetry_message, key)
        if success:
            logger.info(f"Telemetry data forwarded for device {device_id}")
        else:
            logger.error(f"Failed to forward telemetry data for device {device_id}")
    
    async def handle_event(self, environment_id: str, device_id: str, payload: Dict[str, Any]):
        """处理事件数据"""
        event_message = {
            "type": "event",
            "environment_id": environment_id,
            "device_id": device_id,
            "timestamp": datetime.now().isoformat(),
            "event_type": payload.get("event_type", "unknown"),
            "data": payload,
            "protocol": "mqtt"
        }
        
        topic = f"iot.events.{environment_id}"
        key = f"{environment_id}:{device_id}"
        
        success = await self.kafka_manager.send_message(topic, event_message, key)
        if success:
            logger.info(f"Event data forwarded for device {device_id}")
        else:
            logger.error(f"Failed to forward event data for device {device_id}")
    
    async def handle_command_response(self, environment_idd: str, device_id: str, request_id: str, payload: Dict[str, Any]):
        """处理命令响应"""
        response_message = {
            "type": "command_response",
            "environment_id": environment_id,
            "device_id": device_id,
            "request_id": request_id,
            "timestamp": datetime.now().isoformat(),
            "response": payload,
            "protocol": "mqtt"
        }
        
        topic = f"iot.command_responses.{environment_id}"
        key = f"{environment_id}:{device_id}:{request_id}"
        
        success = await self.kafka_manager.send_message(topic, response_message, key)
        if success:
            logger.info(f"Command response forwarded for device {device_id}")
        else:
            logger.error(f"Failed to forward command response for device {device_id}")
    
    async def send_command(self, environment_id: str, device_id: str, command: str, payload: Dict[str, Any]):
        """向设备发送命令"""
        request_id = str(uuid.uuid4())
        topic = f"command/{environment_id}/{device_id}/req/{request_id}"
        
        command_message = {
            "command": command,
            "payload": payload,
            "request_id": request_id,
            "timestamp": datetime.now().isoformat()
        }
        
        if self.client and self.client.is_connected():
            self.client.publish(topic, json.dumps(command_message), qos=1)
            logger.info(f"Command sent to device {device_id}")
            return request_id
        else:
            logger.error("MQTT client not connected")
            return None

# 全局变量
kafka_manager = KafkaManager()
mqtt_adapter = MQTTAdapter(kafka_manager)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用程序生命周期管理"""
    # 启动时
    await kafka_manager.start()
    mqtt_adapter.start()
    yield
    # 关闭时
    mqtt_adapter.stop()
    await kafka_manager.stop()

# FastAPI 应用
app = FastAPI(
    title="IoT Protocol Adapters",
    description="HTTP and MQTT adapters for IoT devices with Kafka integration",
    version="1.0.0",
    lifespan=lifespan
)

# HTTP 适配器端点
@app.post("/telemetry/{environment_id}/{device_id}")
async def send_telemetry(
    environment_id: str,
    device_id: str,
    data: Dict[str, Any],
    request: Request
):
    """接收设备遥测数据"""
    try:
        # 构造消息
        telemetry_message = {
            "type": "telemetry",
            "environment_id": environment_id,
            "device_id": device_id,
            "timestamp": datetime.now().isoformat(),
            "data": data,
            "protocol": "http",
            "headers": dict(request.headers)
        }
        
        # 发送到 Kafka
        topic = f"iot.telemetry.{environment_id}"
        key = f"{environment_id}:{device_id}"
        
        success = await kafka_manager.send_message(topic, telemetry_message, key)
        
        if success:
            logger.info(f"HTTP telemetry data received from device {device_id}")
            return {"status": "accepted", "message": "Telemetry data received"}
        else:
            raise HTTPException(status_code=500, detail="Failed to process telemetry data")
            
    except Exception as e:
        logger.error(f"Error processing HTTP telemetry: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/event/{environment_id}/{device_id}")
async def send_event(
    environment_id: str,
    device_id: str,
    event_data: Dict[str, Any],
    request: Request
):
    """接收设备事件数据"""
    try:
        # 构造消息
        event_message = {
            "type": "event",
            "environment_id": environment_id,
            "device_id": device_id,
            "timestamp": datetime.now().isoformat(),
            "event_type": event_data.get("event_type", "unknown"),
            "data": event_data,
            "protocol": "http",
            "headers": dict(request.headers)
        }
        
        # 发送到 Kafka
        topic = f"iot.events.{environment_id}"
        key = f"{environment_id}:{device_id}"
        
        success = await kafka_manager.send_message(topic, event_message, key)
        
        if success:
            logger.info(f"HTTP event data received from device {device_id}")
            return {"status": "accepted", "message": "Event data received"}
        else:
            raise HTTPException(status_code=500, detail="Failed to process event data")
            
    except Exception as e:
        logger.error(f"Error processing HTTP event: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/command/{environment_id}/{device_id}")
async def send_command(
    environment_id: str,
    device_id: str,
    command_data: Dict[str, Any]
):
    """向设备发送命令"""
    try:
        command = command_data.get("command")
        payload = command_data.get("payload", {})
        
        if not command:
            raise HTTPException(status_code=400, detail="Command is required")
        
        # 通过 MQTT 发送命令
        request_id = await mqtt_adapter.send_command(environment_id, device_id, command, payload)
        
        if request_id:
            return {
                "status": "sent",
                "request_id": request_id,
                "message": "Command sent to device"
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to send command")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error sending command: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/health")
async def health_check():
    """健康检查端点"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "kafka_connected": kafka_manager.producer is not None,
        "mqtt_connected": mqtt_adapter.client and mqtt_adapter.client.is_connected()
    }

@app.get("/")
async def root():
    """根端点"""
    return {
        "message": "IoT Protocol Adapters API",
        "version": "1.0.0",
        "endpoints": {
            "telemetry": "/telemetry/{environment_id}/{device_id}",
            "event": "/event/{environment_id}/{device_id}",
            "command": "/command/{environment_id}/{device_id}",
            "health": "/health"
        }
    }

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
