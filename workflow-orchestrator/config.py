import os

class Config:
    # Kafka设置
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    # Redis设置
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    REDIS_DB = int(os.getenv('REDIS_DB', 0))
    
    # 应用设置
    API_PORT = int(os.getenv('API_PORT', 8000))
    DATA_TTL = int(os.getenv('DATA_TTL', 3600))  # 1小时
    
    # Kafka Topics
    TOPICS = {
        'WORKFLOW_SUBMIT': 'workflow.submit',
        'TASKS_FUNCTION': 'tasks.function',
        'TASKS_DATA': 'tasks.data',
        'TASKS_COMPLETED': 'tasks.completed',
        'WORKFLOWS_COMPLETED': 'workflows.completed'
    }

config = Config()