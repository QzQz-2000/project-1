import os
from typing import Dict

class Config:
    """Workflow Engine Configuration Class"""
    
    # Kafka Settings
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'broker:29092')
    
    # Redis Settings
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6380))
    REDIS_DB = int(os.getenv('REDIS_DB', 0))
    
    # Application Settings
    API_PORT = int(os.getenv('API_PORT', 8001))
    DATA_TTL = int(os.getenv('DATA_TTL', 3600))  # 1 hour, task result cache duration
    
    # New: Workflow Settings
    MAX_WORKFLOW_TASKS = int(os.getenv('MAX_WORKFLOW_TASKS', 100))  # Maximum tasks per workflow
    MAX_TASK_RETRY = int(os.getenv('MAX_TASK_RETRY', 3))  # Maximum task retries
    TASK_TIMEOUT = int(os.getenv('TASK_TIMEOUT', 300))  # Task timeout (seconds)
    
    # New: Cleanup Settings
    CLEANUP_RESULTS_IMMEDIATELY = os.getenv('CLEANUP_RESULTS_IMMEDIATELY', 'false').lower() == 'true'
    CLEANUP_COMPLETED_WORKFLOWS_DAYS = int(os.getenv('CLEANUP_COMPLETED_WORKFLOWS_DAYS', 7))  # Days to retain completed workflows
    
    # New: Logging Settings
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', 'workflow_engine.log')
    
    # New: Security Settings
    ALLOWED_FILE_PATHS = os.getenv('ALLOWED_FILE_PATHS', '/app/data,/tmp').split(',')  # Allowed file paths
    MAX_FILE_SIZE_MB = int(os.getenv('MAX_FILE_SIZE_MB', 100))  # Maximum file size limit
    
    # New: InfluxDB Settings 💡
    INFLUXDB_URL = os.getenv('INFLUXDB_URL', 'http://localhost:8086')
    INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN', 'mytoken')
    INFLUXDB_ORG = os.getenv('INFLUXDB_ORG', 'myorg')
    INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET', 'mybucket')
    
    # Kafka Topics
    TOPICS = {
        'WORKFLOW_SUBMIT': 'workflow.submit',
        'TASKS_FUNCTION': 'tasks.function',
        'TASKS_DATA': 'tasks.data',
        'TASKS_COMPLETED': 'tasks.completed',
        'WORKFLOWS_COMPLETED': 'workflows.completed'
    }
    
    # New: Supported Data Source Types
    SUPPORTED_DATA_SOURCES = {
        'csv': {
            'description': 'CSV file data source',
            'required_params': ['file_path'],
            'optional_params': ['encoding', 'separator', 'header']
        },
        'sample': {
            'description': 'Sample data generator',
            'required_params': ['rows'],
            'optional_params': ['data_type', 'seed', 'columns']
        },
        'influxdb': {
            'description': 'InfluxDB data source',
            'required_params': ['query'],
            'optional_params': ['bucket', 'org']
        }
    }
    
    # New: Function Execution Environment Settings
    FUNCTION_EXECUTION = {
        'memory_limit_mb': int(os.getenv('FUNCTION_MEMORY_LIMIT_MB', 512)),
        'cpu_timeout_seconds': int(os.getenv('FUNCTION_CPU_TIMEOUT_SECONDS', 60)),
        'max_output_rows': int(os.getenv('FUNCTION_MAX_OUTPUT_ROWS', 1000000))  # Maximum output rows for function
    }
    
    @classmethod
    def validate_config(cls) -> Dict[str, str]:
        """Validates configuration and returns errors"""
        errors = {}
        
        # Validate port range
        if not (1 <= cls.API_PORT <= 65535):
            errors['API_PORT'] = f"Invalid port number: {cls.API_PORT}"
        
        if not (1 <= cls.REDIS_PORT <= 65535):
            errors['REDIS_PORT'] = f"Invalid Redis port: {cls.REDIS_PORT}"
        
        # Validate time settings
        if cls.DATA_TTL <= 0:
            errors['DATA_TTL'] = "DATA_TTL must be positive"
        
        if cls.TASK_TIMEOUT <= 0:
            errors['TASK_TIMEOUT'] = "TASK_TIMEOUT must be positive"
        
        # Validate file paths
        for path in cls.ALLOWED_FILE_PATHS:
            if not os.path.isabs(path):
                errors['ALLOWED_FILE_PATHS'] = f"All paths must be absolute: {path}"
                break
        
        return errors
    
    @classmethod
    def get_redis_url(cls) -> str:
        """Gets Redis connection URL"""
        return f"redis://{cls.REDIS_HOST}:{cls.REDIS_PORT}/{cls.REDIS_DB}"
    
    @classmethod
    def get_kafka_config(cls) -> Dict[str, str]:
        """Gets Kafka configuration"""
        return {
            'bootstrap_servers': cls.KAFKA_BOOTSTRAP_SERVERS,
            'client_id': 'workflow_engine',
            'group_id': 'workflow_engine_group',
            'auto_offset_reset': 'latest'
        }
    
    @classmethod
    def get_influxdb_config(cls) -> Dict[str, str]:
        """Gets InfluxDB configuration"""
        return {
            'url': cls.INFLUXDB_URL,
            'token': cls.INFLUXDB_TOKEN,
            'org': cls.INFLUXDB_ORG,
            'bucket': cls.INFLUXDB_BUCKET
        }

    @classmethod
    def get_default_influxdb_config(cls) -> Dict[str, str]:
        """Gets default InfluxDB configuration"""
        return {
            'url': cls.INFLUXDB_URL,
            'token': cls.INFLUXDB_TOKEN,
            'org': cls.INFLUXDB_ORG,
            'bucket': cls.INFLUXDB_BUCKET
        }
    
    @classmethod
    def is_file_path_allowed(cls, file_path: str) -> bool:
        """Checks if a file path is allowed"""
        abs_path = os.path.abspath(file_path)
        return any(abs_path.startswith(allowed_path) for allowed_path in cls.ALLOWED_FILE_PATHS)
    
    @classmethod
    def print_config(cls):
        """Prints current configuration (for debugging)"""
        print("=== Workflow Engine Configuration ===")
        print(f"API Port: {cls.API_PORT}")
        print(f"Redis: {cls.REDIS_HOST}:{cls.REDIS_PORT}/{cls.REDIS_DB}")
        print(f"Kafka: {cls.KAFKA_BOOTSTRAP_SERVERS}")
        print(f"InfluxDB: {cls.INFLUXDB_URL}, Org: {cls.INFLUXDB_ORG}, Bucket: {cls.INFLUXDB_BUCKET}")
        print(f"Data TTL: {cls.DATA_TTL} seconds")
        print(f"Max Workflow Tasks: {cls.MAX_WORKFLOW_TASKS}")
        print(f"Task Timeout: {cls.TASK_TIMEOUT} seconds")
        print(f"Log Level: {cls.LOG_LEVEL}")
        print(f"Allowed File Paths: {cls.ALLOWED_FILE_PATHS}")
        print(f"Cleanup Results Immediately: {cls.CLEANUP_RESULTS_IMMEDIATELY}")
        print("=" * 40)

# Global configuration instance
config = Config()