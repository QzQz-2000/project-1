import json
import pickle
import logging
from typing import Any, Dict, Optional
from datetime import datetime
import redis
from kafka import KafkaProducer
from models import BaseMessage, MessageType, FunctionTaskMessage, OrchestratorTaskMessage

logger = logging.getLogger(__name__)

class MessageSerializer:
    """消息序列化工具"""
    
    @staticmethod
    def serialize_message(message: BaseMessage) -> bytes:
        """序列化消息对象为字节"""
        try:
            # 将dataclass转换为dict
            if hasattr(message, '__dict__'):
                message_dict = message.__dict__.copy()
            else:
                message_dict = message
            
            # 处理datetime类型
            for key, value in message_dict.items():
                if isinstance(value, datetime):
                    message_dict[key] = value.isoformat()
                elif hasattr(value, 'value'):  # 处理Enum
                    message_dict[key] = value.value
            
            return json.dumps(message_dict, default=str).encode('utf-8')
        except Exception as e:
            logger.error(f"Failed to serialize message: {e}")
            raise

    @staticmethod
    def deserialize_message(data: bytes) -> BaseMessage:
        """反序列化字节为消息对象"""
        try:
            message_dict = json.loads(data.decode('utf-8'))
            message_type = MessageType(message_dict.get('message_type'))
            
            # 恢复datetime
            if 'timestamp' in message_dict and isinstance(message_dict['timestamp'], str):
                message_dict['timestamp'] = datetime.fromisoformat(message_dict['timestamp'])
            
            # 根据消息类型创建相应对象
            if message_type == MessageType.ORCHESTRATOR_TASK:
                return OrchestratorTaskMessage(**message_dict)
            elif message_type == MessageType.FUNCTION_TASK:
                return FunctionTaskMessage(**message_dict)
            else:
                logger.warning(f"Unknown message type: {message_type}")
                return BaseMessage(**message_dict)
                
        except Exception as e:
            logger.error(f"Failed to deserialize message: {e}")
            raise


class DataStoreManager:
    """数据存储管理器"""
    
    def __init__(self, redis_client: redis.Redis, default_ttl: int = 3600):
        self.redis_client = redis_client
        self.default_ttl = default_ttl
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def store_data(self, data: Any, data_id: str = None, ttl: int = None) -> str:
        """存储数据并返回数据ID"""
        import uuid
        
        if data_id is None:
            data_id = f"data_{uuid.uuid4().hex}"
        
        if ttl is None:
            ttl = self.default_ttl
        
        try:
            # 序列化数据
            serialized_data = pickle.dumps(data)
            
            # 存储到Redis
            self.redis_client.setex(
                f"mcs_data:{data_id}",
                ttl,
                serialized_data
            )
            
            self.logger.debug(f"Data stored with ID: {data_id}")
            return data_id
            
        except Exception as e:
            self.logger.error(f"Failed to store data: {e}")
            raise
    
    def retrieve_data(self, data_id: str) -> Any:
        """检索数据"""
        try:
            serialized_data = self.redis_client.get(f"mcs_data:{data_id}")
            if serialized_data is None:
                raise ValueError(f"Data not found for ID: {data_id}")
            
            data = pickle.loads(serialized_data)
            self.logger.debug(f"Data retrieved for ID: {data_id}")
            return data
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve data {data_id}: {e}")
            raise
    
    def delete_data(self, data_id: str) -> bool:
        """删除数据"""
        try:
            result = self.redis_client.delete(f"mcs_data:{data_id}")
            self.logger.debug(f"Data deleted for ID: {data_id}")
            return result > 0
        except Exception as e:
            self.logger.error(f"Failed to delete data {data_id}: {e}")
            return False


class KafkaManager:
    """Kafka管理器"""
    
    def __init__(self, bootstrap_servers: list):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_producer(self) -> KafkaProducer:
        """获取Kafka生产者"""
        if self.producer is None:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode('utf-8')
            )
        return self.producer
    
    def send_message(self, topic: str, message: BaseMessage):
        """发送消息"""
        try:
            producer = self.get_producer()
            serialized_message = MessageSerializer.serialize_message(message)
            
            future = producer.send(topic, value=serialized_message)
            # 可选：等待发送完成
            # future.get(timeout=10)
            
            self.logger.debug(f"Message sent to topic {topic}: {message.message_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to send message to {topic}: {e}")
            raise
    
    def close(self):
        """关闭连接"""
        if self.producer:
            self.producer.close()


class WorkflowValidator:
    """工作流验证器"""
    
    @staticmethod
    def validate_workflow_definition(workflow_def: Dict[str, Any], available_functions: set) -> tuple[bool, list]:
        """验证工作流定义"""
        errors = []
        
        # 检查基本字段
        if not workflow_def.get('name'):
            errors.append("Workflow name is required")
        
        steps = workflow_def.get('steps', [])
        if not steps:
            errors.append("Workflow must contain at least one step")
        
        # 检查步骤定义
        step_names = set()
        for i, step in enumerate(steps):
            step_name = step.get('name')
            if not step_name:
                errors.append(f"Step {i+1} missing name")
                continue
            
            if step_name in step_names:
                errors.append(f"Duplicate step name: {step_name}")
            step_names.add(step_name)
            
            # 检查函数是否存在
            function_name = step.get('function')
            if not function_name:
                errors.append(f"Step '{step_name}' missing function")
            elif function_name not in available_functions:
                errors.append(f"Function '{function_name}' not found in step '{step_name}'")
            
            # 检查依赖关系
            dependencies = step.get('dependencies', [])
            for dep in dependencies:
                if dep not in step_names and dep not in [s.get('name') for s in steps[:i]]:
                    errors.append(f"Step '{step_name}' depends on undefined step '{dep}'")
        
        return len(errors) == 0, errors


class HealthChecker:
    """健康检查器"""
    
    def __init__(self, redis_client: redis.Redis, kafka_manager: KafkaManager):
        self.redis_client = redis_client
        self.kafka_manager = kafka_manager
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def check_redis(self) -> bool:
        """检查Redis连接"""
        try:
            self.redis_client.ping()
            return True
        except Exception as e:
            self.logger.error(f"Redis health check failed: {e}")
            return False
    
    def check_kafka(self) -> bool:
        """检查Kafka连接"""
        try:
            producer = self.kafka_manager.get_producer()
            # 简单的连接检查
            return producer is not None
        except Exception as e:
            self.logger.error(f"Kafka health check failed: {e}")
            return False
    
    def get_health_status(self) -> Dict[str, Any]:
        """获取整体健康状态"""
        redis_ok = self.check_redis()
        kafka_ok = self.check_kafka()
        
        status = "healthy" if (redis_ok and kafka_ok) else "unhealthy"
        
        return {
            "status": status,
            "timestamp": datetime.utcnow().isoformat(),
            "components": {
                "redis": redis_ok,
                "kafka": kafka_ok
            }
        }