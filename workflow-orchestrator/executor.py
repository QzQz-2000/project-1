import json
import pickle
import asyncio
import logging
from datetime import datetime
import pandas as pd
import numpy as np
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import redis.asyncio as redis
from function_registry import function_registry
from config import config

logger = logging.getLogger(__name__)

class TaskExecutor:
    """任务执行器"""
    
    def __init__(self):
        self.consumer = None
        self.producer = None
        self.redis = None
        
    async def start(self):
        """启动执行器"""
        self.consumer = AIOKafkaConsumer(
            config.TOPICS['TASKS_FUNCTION'],
            config.TOPICS['TASKS_DATA'],
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            group_id='executor-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        self.producer = AIOKafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        self.redis = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            decode_responses=False
        )
        
        await self.consumer.start()
        await self.producer.start()
        
        logger.info("TaskExecutor started")
    
    async def stop(self):
        """停止执行器"""
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()
        if self.redis:
            await self.redis.close()
        logger.info("TaskExecutor stopped")
    
    async def run(self):
        """主循环"""
        try:
            async for message in self.consumer:
                task_message = message.value
                await self.execute_task(task_message)
        except Exception as e:
            logger.error(f"Error in executor main loop: {e}")
    
    async def execute_task(self, task_msg):
        """执行任务"""
        task_id = task_msg['task_id']
        logger.info(f"Executing task: {task_id}")
        
        try:
            if task_msg['task_type'] == 'FUNCTION':
                result = await self.execute_function_task(task_msg)
            elif task_msg['task_type'] == 'DATA':
                result = await self.execute_data_task(task_msg)
            else:
                raise ValueError(f"Unknown task type: {task_msg['task_type']}")
            
            result_key = f"result:{task_id}"
            await self.redis.setex(result_key, config.DATA_TTL, pickle.dumps(result))
            
            completion_msg = {
                'task_id': task_id,
                'workflow_id': task_msg['workflow_id'],
                'status': 'completed',
                'result_key': result_key,
                'timestamp': datetime.now().isoformat()
            }
            await self.producer.send(config.TOPICS['TASKS_COMPLETED'], completion_msg)
            
            logger.info(f"Task {task_id} completed successfully")
            
        except Exception as e:
            logger.error(f"Task {task_id} failed: {e}")
            
            failure_msg = {
                'task_id': task_id,
                'workflow_id': task_msg['workflow_id'],
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            await self.producer.send(config.TOPICS['TASKS_COMPLETED'], failure_msg)
    
    async def execute_function_task(self, task_msg):
        """执行函数任务"""
        function_name = task_msg['function_name']
        config_params = task_msg['config']
        
        input_data = await self.get_input_data(task_msg)
        
        func_instance = function_registry.get_function(function_name)
        result_data, metadata = func_instance.execute(input_data, config_params)
        
        logger.info(f"Function {function_name} executed: {metadata}")
        return result_data
    
    async def execute_data_task(self, task_msg):
        """执行数据任务"""
        data_source = task_msg['data_source']
        
        if data_source['source_type'] == 'csv':
            return await self.load_csv_data(data_source['query_config'])
        elif data_source['source_type'] == 'sample':
            return await self.generate_sample_data(data_source['query_config'])
        else:
            raise ValueError(f"Unsupported data source: {data_source['source_type']}")
    
    # TODO: 添加influxdb部分
    async def load_csv_data(self, query_config):
        """加载CSV数据"""
        file_path = query_config['file_path']
        logger.info(f"Loading CSV data from: {file_path}")
        
        try:
            data = pd.read_csv(file_path)
            logger.info(f"Loaded {len(data)} rows from CSV")
            return data
        except Exception as e:
            logger.error(f"Failed to load CSV: {e}")
            raise
    
    async def generate_sample_data(self, query_config):
        """生成示例数据"""
        rows = query_config.get('rows', 100)
        logger.info(f"Generating {rows} rows of sample data")
        
        np.random.seed(42)
        data = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=rows, freq='H'),
            'value': np.random.randn(rows) * 10 + 50,
            'category': np.random.choice(['A', 'B', 'C'], rows)
        })
        
        return data
    
    async def get_input_data(self, task_msg):
        """获取输入数据"""
        workflow_id = task_msg['workflow_id']
        task_id = task_msg['task_id']
        
        redis_json = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            decode_responses=True
        )
        
        workflow_data = await redis_json.get(f"workflow:{workflow_id}")
        if not workflow_data:
            logger.warning(f"No workflow data found for {workflow_id}")
            return pd.DataFrame()
        
        workflow = json.loads(workflow_data)
        
        current_task = None
        for task in workflow['tasks']:
            if task['task_id'] == task_id:
                current_task = task
                break
        
        if not current_task or not current_task['dependencies']:
            logger.info(f"Task {task_id} has no dependencies, using empty DataFrame")
            return pd.DataFrame()
        
        input_dataframes = []
        for dep_task_id in current_task['dependencies']:
            result_key = f"result:{dep_task_id}"
            result_data = await self.redis.get(result_key)
            if result_data:
                df = pickle.loads(result_data)
                input_dataframes.append(df)
                logger.info(f"Loaded dependency data from {dep_task_id}: {len(df)} rows")
        
        await redis_json.close()
        
        if len(input_dataframes) == 1:
            return input_dataframes[0]
        elif len(input_dataframes) > 1:
            result = pd.concat(input_dataframes, ignore_index=True)
            logger.info(f"Merged {len(input_dataframes)} dependencies: {len(result)} total rows")
            return result
        else:
            logger.warning(f"No dependency data found for task {task_id}")
            return pd.DataFrame()
