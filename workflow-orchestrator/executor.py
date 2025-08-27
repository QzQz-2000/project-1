import json
import pickle
import asyncio
import logging
from datetime import datetime
import pandas as pd
import numpy as np
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import redis.asyncio as redis
from registry import REGISTRY as function_registry
from config import config
from influx_handler import InfluxDBDataHandler  # 修正导入路径

logger = logging.getLogger(__name__)

class TaskExecutor:
    """任务执行器"""
    
    def __init__(self):
        self.consumer = None
        self.producer = None
        self.redis_bytes = None   # 存 pickle (二进制)
        self.redis_json = None    # 存 JSON (字符串)
        self._influxdb_clients = {}  # InfluxDB客户端缓存

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
        
        # 两个 Redis 实例，避免 decode_responses 混用
        self.redis_bytes = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            decode_responses=False
        )
        self.redis_json = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            decode_responses=True
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
        if self.redis_bytes:
            await self.redis_bytes.aclose()
        if self.redis_json:
            await self.redis_json.aclose()
        
        # 关闭所有 InfluxDB 客户端连接
        for client_key, client in self._influxdb_clients.items():
            try:
                client.close()
                logger.info(f"Closed InfluxDB client: {client_key}")
            except Exception as e:
                logger.warning(f"Error closing InfluxDB client {client_key}: {e}")
        
        self._influxdb_clients.clear()
        logger.info("TaskExecutor stopped")
    
    async def run(self):
        """主循环"""
        try:
            async for message in self.consumer:
                task_message = message.value
                await self.execute_task(task_message)
        except Exception as e:
            logger.error(f"Error in executor main loop: {e}", exc_info=True)

    async def execute_task(self, task_msg):
        """执行任务"""
        task_id = task_msg['task_id']
        environment_id = task_msg['environment_id']
        workflow_id = task_msg['workflow_id']
        
        logger.info(f"Executing task: {task_id}")
        
        try:
            self._validate_task_message(task_msg)
            
            if task_msg['task_type'] == 'FUNCTION':
                result, metadata = await self.execute_function_task(task_msg)
            elif task_msg['task_type'] == 'DATA':
                result, metadata = await self.execute_data_task(task_msg)
            else:
                raise ValueError(f"Unknown task type: {task_msg['task_type']}")
            
            # 存储结果（pickle 序列化）
            result_key = f"result:{task_id}"
            await self.redis_bytes.setex(result_key, config.DATA_TTL, pickle.dumps(result))
            
            completion_msg = {
                'task_id': task_id,
                'environment_id': environment_id,
                'workflow_id': workflow_id,
                'status': 'completed',
                'result_key': result_key,
                'metadata': metadata,
                'timestamp': datetime.now().isoformat()
            }
            await self.producer.send(config.TOPICS['TASKS_COMPLETED'], completion_msg)
            await self.producer.flush()
            
            logger.info(f"Task {task_id} completed successfully")
            
        except Exception as e:
            logger.error(f"Task {task_id} failed: {e}", exc_info=True)
            
            failure_msg = {
                'task_id': task_id,
                'environment_id': environment_id,
                'workflow_id': workflow_id,
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
            await self.producer.send(config.TOPICS['TASKS_COMPLETED'], failure_msg)
            await self.producer.flush()
    
    def _validate_task_message(self, task_msg):
        """验证任务消息格式"""
        required_fields = ['task_id', 'environment_id', 'workflow_id', 'task_type']
        for field in required_fields:
            if field not in task_msg:
                raise ValueError(f"Missing required field: {field}")
        
        if task_msg['task_type'] == 'FUNCTION' and 'function_name' not in task_msg:
            raise ValueError("Function name is required for FUNCTION tasks")
        
        if task_msg['task_type'] == 'DATA' and 'data_source' not in task_msg:
            raise ValueError("Data source is required for DATA tasks")

    async def execute_function_task(self, task_msg):
        """执行函数任务"""
        function_name = task_msg['function_name']
        config_params = task_msg['config']
        
        func_instance = function_registry.create_instance(function_name)
        if not func_instance:
            raise ValueError(f"Function '{function_name}' not found in registry")
        
        input_data = await self.get_input_data(task_msg)
        
        result_data, metadata = func_instance.execute(input_data, config_params)
        
        logger.info(f"Function {function_name} executed: {metadata}")
        return result_data, metadata

    async def execute_data_task(self, task_msg):
        """执行数据任务"""
        data_source = task_msg['data_source']
        source_type = data_source.get('source_type')
        
        if source_type == 'csv':
            result_data = await self.load_csv_data(data_source['query_config'])
        elif source_type == 'influxdb':
            result_data = await self.load_influxdb_data(data_source['query_config'])
        elif source_type == 'sample':
            result_data = await self.generate_sample_data(data_source['query_config'])
        else:
            raise ValueError(f"Unsupported data source: {source_type}")
            
        metadata = {
            "status": "success",
            "message": f"Data task from {source_type} executed successfully",
            "rows_loaded": len(result_data) if isinstance(result_data, pd.DataFrame) else 0,
            "columns": list(result_data.columns) if isinstance(result_data, pd.DataFrame) else []
        }
        return result_data, metadata

    async def load_csv_data(self, query_config):
        """加载CSV数据"""
        file_path = query_config.get('file_path')
        if not file_path:
            raise ValueError("CSV query config must specify file_path")
        
        # 验证文件路径是否被允许
        if not config.is_file_path_allowed(file_path):
            raise ValueError(f"File path not allowed: {file_path}")
        
        csv_params = {
            'encoding': query_config.get('encoding', 'utf-8'),
            'sep': query_config.get('separator', ','),
            'header': query_config.get('header', 0)
        }
        
        data = pd.read_csv(file_path, **csv_params)
        
        # 应用过滤器（如果有）
        if 'filters' in query_config:
            for filter_config in query_config['filters']:
                data = self._apply_data_filter(data, filter_config)
        
        logger.info(f"Loaded {len(data)} rows, {len(data.columns)} columns from CSV")
        return data

    async def load_influxdb_data(self, query_config):
        """加载InfluxDB数据"""
        if 'measurement' not in query_config:
            raise ValueError("InfluxDB query config must specify 'measurement'")
        
        client = self._get_influxdb_client()
        if not client:
            raise ValueError("InfluxDB client not configured")
        
        try:
            # 直接调用同步方法，无需使用 asyncio.to_thread
            data = client.query_data(
                measurement=query_config['measurement'],
                fields=query_config.get('fields', ["value"]),  # 修正默认字段名
                start=query_config.get('start', "-1h"),
                stop=query_config.get('stop', "now()"),
                tags=query_config.get('tags'),
                filters=query_config.get('filters'),
                pivot=query_config.get('pivot', True),
                post_pivot_filters=query_config.get('post_pivot_filters'),
                aggregation=query_config.get('aggregation'),
                aggregate_every=query_config.get('aggregate_every'),
                limit=query_config.get('limit')
            )
        except Exception as e:
            logger.error(f"Failed to query InfluxDB: {e}")
            raise
         
        logger.info(f"Loaded {len(data)} rows, {len(data.columns)} columns from InfluxDB")
        return data

    async def generate_sample_data(self, query_config):
        """生成示例数据"""
        rows = query_config.get('rows', 100)
        data_type = query_config.get('data_type', 'time_series')
        seed = query_config.get('seed', 42)
        
        logger.info(f"Generating {rows} rows of sample data (type: {data_type})")
        
        np.random.seed(seed)
        
        if data_type == 'time_series':
            data = pd.DataFrame({
                'time': pd.date_range('2024-01-01', periods=rows, freq='H'),
                'value': np.random.randn(rows) * 10 + 50,
                'category': np.random.choice(['A', 'B', 'C'], rows)
            })
        else:
            # 基础随机数据
            data = pd.DataFrame({
                'id': range(rows),
                'value': np.random.randn(rows) * 10 + 50,
                'category': np.random.choice(['A', 'B', 'C'], rows)
            })
        
        return data

    def _get_influxdb_client(self):
        """获取或创建InfluxDB客户端"""
        client_key = "default"
        if client_key not in self._influxdb_clients:
            # 从 config 获取 InfluxDB 配置
            connection_config = config.get_default_influxdb_config()
            
            try:
                self._influxdb_clients[client_key] = InfluxDBDataHandler(
                    url=connection_config["url"],
                    token=connection_config["token"],
                    org=connection_config["org"],
                    bucket=connection_config["bucket"]
                )
                logger.info("InfluxDB client created successfully")
            except Exception as e:
                logger.error(f"Failed to create InfluxDB client: {e}")
                return None
                
        return self._influxdb_clients[client_key]

    def _apply_data_filter(self, data, filter_config):
        """应用数据过滤"""
        column = filter_config.get('column')
        operator = filter_config.get('operator')
        value = filter_config.get('value')
        
        if column is None or operator is None or value is None:
            raise ValueError("Filter config must specify column, operator, and value")
        
        if column not in data.columns:
            raise ValueError(f"Filter column '{column}' not found in data")
        
        if operator == 'eq':
            return data[data[column] == value]
        elif operator == 'ne':
            return data[data[column] != value]
        elif operator == 'gt':
            return data[data[column] > value]
        elif operator == 'lt':
            return data[data[column] < value]
        elif operator == 'gte':
            return data[data[column] >= value]
        elif operator == 'lte':
            return data[data[column] <= value]
        elif operator == 'in':
            return data[data[column].isin(value)]
        else:
            raise ValueError(f"Unsupported filter operator: {operator}")

    async def get_input_data(self, task_msg):
        """获取输入数据"""
        task_id = task_msg['task_id']
        environment_id = task_msg['environment_id']
        workflow_id = task_msg['workflow_id']
        
        workflow_key = f"workflow:{environment_id}:{workflow_id}"
        workflow_data = await self.redis_json.get(workflow_key)
        if not workflow_data:
            return pd.DataFrame()
        
        workflow = json.loads(workflow_data)
        current_task = next((t for t in workflow['tasks'] if t['task_id'] == task_id), None)
        if not current_task or not current_task.get('dependencies'):
            return pd.DataFrame()
        
        input_dataframes = []
        for dep_task_id in current_task['dependencies']:
            result_key = f"result:{dep_task_id}"
            result_data = await self.redis_bytes.get(result_key)
            if result_data:
                df = pickle.loads(result_data)
                input_dataframes.append(df)
                logger.info(f"Loaded dependency data from {dep_task_id}: {len(df)} rows")
        
        if len(input_dataframes) == 1:
            return input_dataframes[0]
        elif len(input_dataframes) > 1:
            return pd.concat(input_dataframes, ignore_index=True)
        else:
            logger.warning(f"No dependency data found for task {task_id}")
            return pd.DataFrame()