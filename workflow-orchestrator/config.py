import os
from typing import Dict

class Config:
    """工作流引擎配置类"""
    
    # Kafka设置
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    # Redis设置
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    REDIS_DB = int(os.getenv('REDIS_DB', 0))
    
    # 应用设置
    API_PORT = int(os.getenv('API_PORT', 8001))
    DATA_TTL = int(os.getenv('DATA_TTL', 3600))  # 1小时，任务结果缓存时间
    
    # 新增：工作流设置
    MAX_WORKFLOW_TASKS = int(os.getenv('MAX_WORKFLOW_TASKS', 100))  # 单个工作流最大任务数
    MAX_TASK_RETRY = int(os.getenv('MAX_TASK_RETRY', 3))  # 任务最大重试次数
    TASK_TIMEOUT = int(os.getenv('TASK_TIMEOUT', 300))  # 任务超时时间（秒）
    
    # 新增：清理设置
    CLEANUP_RESULTS_IMMEDIATELY = os.getenv('CLEANUP_RESULTS_IMMEDIATELY', 'false').lower() == 'true'
    CLEANUP_COMPLETED_WORKFLOWS_DAYS = int(os.getenv('CLEANUP_COMPLETED_WORKFLOWS_DAYS', 7))  # 完成工作流保留天数
    
    # 新增：日志设置
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', 'workflow_engine.log')
    
    # 新增：安全设置
    ALLOWED_FILE_PATHS = os.getenv('ALLOWED_FILE_PATHS', '/app/data,/tmp').split(',')  # 允许访问的文件路径
    MAX_FILE_SIZE_MB = int(os.getenv('MAX_FILE_SIZE_MB', 100))  # 最大文件大小限制
    
    # Kafka Topics
    TOPICS = {
        'WORKFLOW_SUBMIT': 'workflow.submit',
        'TASKS_FUNCTION': 'tasks.function',
        'TASKS_DATA': 'tasks.data',
        'TASKS_COMPLETED': 'tasks.completed',
        'WORKFLOWS_COMPLETED': 'workflows.completed'
    }
    
    # 新增：支持的数据源类型
    SUPPORTED_DATA_SOURCES = {
        'csv': {
            'description': 'CSV文件数据源',
            'required_params': ['file_path'],
            'optional_params': ['encoding', 'separator', 'header']
        },
        'sample': {
            'description': '示例数据生成器',
            'required_params': ['rows'],
            'optional_params': ['data_type', 'seed', 'columns']
        }
        # 可以扩展支持更多数据源类型，如数据库、API等
    }
    
    # 新增：函数执行环境设置
    FUNCTION_EXECUTION = {
        'memory_limit_mb': int(os.getenv('FUNCTION_MEMORY_LIMIT_MB', 512)),
        'cpu_timeout_seconds': int(os.getenv('FUNCTION_CPU_TIMEOUT_SECONDS', 60)),
        'max_output_rows': int(os.getenv('FUNCTION_MAX_OUTPUT_ROWS', 1000000))  # 函数输出最大行数
    }
    
    @classmethod
    def validate_config(cls) -> Dict[str, str]:
        """验证配置并返回错误信息"""
        errors = {}
        
        # 验证端口范围
        if not (1 <= cls.API_PORT <= 65535):
            errors['API_PORT'] = f"Invalid port number: {cls.API_PORT}"
        
        if not (1 <= cls.REDIS_PORT <= 65535):
            errors['REDIS_PORT'] = f"Invalid Redis port: {cls.REDIS_PORT}"
        
        # 验证时间设置
        if cls.DATA_TTL <= 0:
            errors['DATA_TTL'] = "DATA_TTL must be positive"
        
        if cls.TASK_TIMEOUT <= 0:
            errors['TASK_TIMEOUT'] = "TASK_TIMEOUT must be positive"
        
        # 验证文件路径
        for path in cls.ALLOWED_FILE_PATHS:
            if not os.path.isabs(path):
                errors['ALLOWED_FILE_PATHS'] = f"All paths must be absolute: {path}"
                break
        
        return errors
    
    @classmethod
    def get_redis_url(cls) -> str:
        """获取Redis连接URL"""
        return f"redis://{cls.REDIS_HOST}:{cls.REDIS_PORT}/{cls.REDIS_DB}"
    
    @classmethod
    def get_kafka_config(cls) -> Dict[str, str]:
        """获取Kafka配置"""
        return {
            'bootstrap_servers': cls.KAFKA_BOOTSTRAP_SERVERS,
            'client_id': 'workflow_engine'
        }
    
    @classmethod
    def is_file_path_allowed(cls, file_path: str) -> bool:
        """检查文件路径是否被允许"""
        abs_path = os.path.abspath(file_path)
        return any(abs_path.startswith(allowed_path) for allowed_path in cls.ALLOWED_FILE_PATHS)
    
    @classmethod
    def print_config(cls):
        """打印当前配置（用于调试）"""
        print("=== Workflow Engine Configuration ===")
        print(f"API Port: {cls.API_PORT}")
        print(f"Redis: {cls.REDIS_HOST}:{cls.REDIS_PORT}/{cls.REDIS_DB}")
        print(f"Kafka: {cls.KAFKA_BOOTSTRAP_SERVERS}")
        print(f"Data TTL: {cls.DATA_TTL} seconds")
        print(f"Max Workflow Tasks: {cls.MAX_WORKFLOW_TASKS}")
        print(f"Task Timeout: {cls.TASK_TIMEOUT} seconds")
        print(f"Log Level: {cls.LOG_LEVEL}")
        print(f"Allowed File Paths: {cls.ALLOWED_FILE_PATHS}")
        print(f"Cleanup Results Immediately: {cls.CLEANUP_RESULTS_IMMEDIATELY}")
        print("=" * 40)

# 全局配置实例
config = Config()