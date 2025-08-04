import os
import logging
from typing import List
from pydantic import BaseSettings

class Settings(BaseSettings):
    """统一配置管理"""
    
    # 服务配置
    service_name: str = "workflow-engine"
    service_port: int = 8000
    debug: bool = False
    
    # Kafka配置
    kafka_servers: str = "localhost:9092"
    kafka_group_id: str = "workflow-engine"
    
    # Redis配置  
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: str = None
    
    # 日志配置
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # 数据配置
    data_ttl: int = 3600  # 数据过期时间（秒）
    
    @property
    def kafka_servers_list(self) -> List[str]:
        return self.kafka_servers.split(',')
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

# 全局配置实例
settings = Settings()

def setup_logging():
    """设置日志配置"""
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper()),
        format=settings.log_format,
        force=True  # 强制重新配置
    )
    
    # 设置第三方库日志级别
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("redis").setLevel(logging.WARNING)
    logging.getLogger("uvicorn").setLevel(logging.INFO)
    
    return logging.getLogger(settings.service_name)