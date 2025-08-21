# import json
# import pickle
# import asyncio
# import logging
# from datetime import datetime
# import pandas as pd
# import numpy as np
# from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
# import redis.asyncio as redis
# from registry import REGISTRY as function_registry
# from config import config

# logger = logging.getLogger(__name__)

# class TaskExecutor:
#     """任务执行器"""
    
#     def __init__(self):
#         self.consumer = None
#         self.producer = None
#         self.redis = None
        
#     async def start(self):
#         """启动执行器"""
#         self.consumer = AIOKafkaConsumer(
#             config.TOPICS['TASKS_FUNCTION'],
#             config.TOPICS['TASKS_DATA'],
#             bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
#             group_id='executor-group',
#             value_deserializer=lambda m: json.loads(m.decode('utf-8'))
#         )
        
#         self.producer = AIOKafkaProducer(
#             bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
#             value_serializer=lambda v: json.dumps(v).encode('utf-8')
#         )
        
#         self.redis = redis.Redis(
#             host=config.REDIS_HOST,
#             port=config.REDIS_PORT,
#             db=config.REDIS_DB,
#             decode_responses=False
#         )
        
#         await self.consumer.start()
#         await self.producer.start()
        
#         logger.info("TaskExecutor started")
    
#     async def stop(self):
#         """停止执行器"""
#         if self.consumer:
#             await self.consumer.stop()
#         if self.producer:
#             await self.producer.stop()
#         if self.redis:
#             await self.redis.close()
#         logger.info("TaskExecutor stopped")
    
#     async def run(self):
#         """主循环"""
#         try:
#             async for message in self.consumer:
#                 task_message = message.value
#                 await self.execute_task(task_message)
#         except Exception as e:
#             logger.error(f"Error in executor main loop: {e}")
    
#     async def execute_task(self, task_msg):
#         """执行任务"""
#         task_id = task_msg['task_id']
#         logger.info(f"Executing task: {task_id}")
        
#         try:
#             if task_msg['task_type'] == 'FUNCTION':
#                 result, metadata = await self.execute_function_task(task_msg)
#             elif task_msg['task_type'] == 'DATA':
#                 result, metadata = await self.execute_data_task(task_msg)
#             else:
#                 raise ValueError(f"Unknown task type: {task_msg['task_type']}")
            
#             result_key = f"result:{task_id}"
#             await self.redis.setex(result_key, config.DATA_TTL, pickle.dumps(result))
            
#             completion_msg = {
#                 'task_id': task_id,
#                 'workflow_id': task_msg['workflow_id'],
#                 'status': 'completed',
#                 'result_key': result_key,
#                 'timestamp': datetime.now().isoformat()
#             }
#             await self.producer.send(config.TOPICS['TASKS_COMPLETED'], completion_msg)
            
#             logger.info(f"Task {task_id} completed successfully")
            
#         except Exception as e:
#             logger.error(f"Task {task_id} failed: {e}")
            
#             failure_msg = {
#                 'task_id': task_id,
#                 'workflow_id': task_msg['workflow_id'],
#                 'status': 'failed',
#                 'error': str(e),
#                 'timestamp': datetime.now().isoformat()
#             }
#             await self.producer.send(config.TOPICS['TASKS_COMPLETED'], failure_msg)
    
#     async def execute_function_task(self, task_msg):
#         """执行函数任务"""
#         function_name = task_msg['function_name']
#         config_params = task_msg['config']
        
#         input_data = await self.get_input_data(task_msg)
        
#         func_instance = function_registry.create_instance(function_name)
#         result_data, metadata = func_instance.execute(input_data, config_params)
        
#         logger.info(f"Function {function_name} executed: {metadata}")
#         return result_data, metadata
    
#     async def execute_data_task(self, task_msg):
#         """执行数据任务"""
#         data_source = task_msg['data_source']
        
#         if data_source['source_type'] == 'csv':
#             result_data = await self.load_csv_data(data_source['query_config'])
#         elif data_source['source_type'] == 'sample':
#             result_data = await self.generate_sample_data(data_source['query_config'])
#         else:
#             raise ValueError(f"Unsupported data source: {data_source['source_type']}")
            
#         metadata = {
#             "status": "success",
#             "message": f"Data task from {data_source['source_type']} executed."
#         }
#         return result_data, metadata
    
#     # TODO: 添加influxdb部分
#     async def load_csv_data(self, query_config):
#         """加载CSV数据"""
#         file_path = query_config['file_path']
#         logger.info(f"Loading CSV data from: {file_path}")
        
#         try:
#             data = pd.read_csv(file_path)
#             logger.info(f"Loaded {len(data)} rows from CSV")
#             return data
#         except Exception as e:
#             logger.error(f"Failed to load CSV: {e}")
#             raise
    
#     async def generate_sample_data(self, query_config):
#         """生成示例数据"""
#         rows = query_config.get('rows', 100)
#         logger.info(f"Generating {rows} rows of sample data")
        
#         np.random.seed(42)
#         data = pd.DataFrame({
#             'timestamp': pd.date_range('2024-01-01', periods=rows, freq='H'),
#             'value': np.random.randn(rows) * 10 + 50,
#             'category': np.random.choice(['A', 'B', 'C'], rows)
#         })
        
#         return data
    
#     # async def get_input_data(self, task_msg):
#     #     """获取输入数据"""
#     #     workflow_id = task_msg['workflow_id']
#     #     task_id = task_msg['task_id']
        
#     #     redis_json = redis.Redis(
#     #         host=config.REDIS_HOST,
#     #         port=config.REDIS_PORT,
#     #         db=config.REDIS_DB,
#     #         decode_responses=True
#     #     )
        
#     #     workflow_data = await redis_json.get(f"workflow:{workflow_id}")
#     #     if not workflow_data:
#     #         logger.warning(f"No workflow data found for {workflow_id}")
#     #         return pd.DataFrame()
        
#     #     workflow = json.loads(workflow_data)
        
#     #     current_task = None
#     #     for task in workflow['tasks']:
#     #         if task['task_id'] == task_id:
#     #             current_task = task
#     #             break
        
#     #     if not current_task or not current_task['dependencies']:
#     #         logger.info(f"Task {task_id} has no dependencies, using empty DataFrame")
#     #         return pd.DataFrame()
        
#     #     input_dataframes = []
#     #     for dep_task_id in current_task['dependencies']:
#     #         result_key = f"result:{dep_task_id}"
#     #         result_data = await self.redis.get(result_key)
#     #         if result_data:
#     #             df = pickle.loads(result_data)
#     #             input_dataframes.append(df)
#     #             logger.info(f"Loaded dependency data from {dep_task_id}: {len(df)} rows")
        
#     #     await redis_json.close()
        
#     #     if len(input_dataframes) == 1:
#     #         return input_dataframes[0]
#     #     elif len(input_dataframes) > 1:
#     #         result = pd.concat(input_dataframes, ignore_index=True)
#     #         logger.info(f"Merged {len(input_dataframes)} dependencies: {len(result)} total rows")
#     #         return result
#     #     else:
#     #         logger.warning(f"No dependency data found for task {task_id}")
#     #         return pd.DataFrame()
#     async def get_input_data(self, task_msg):
#         """获取输入数据"""
#         task_id = task_msg['task_id']
#         environment_id = task_msg['environment_id']
#         workflow_id = task_msg['workflow_id']
        
#         redis_json = redis.Redis(
#             host=config.REDIS_HOST,
#             port=config.REDIS_PORT,
#             db=config.REDIS_DB,
#             decode_responses=True
#         )
        
#         workflow_data = await redis_json.get(f"workflow:{environment_id}:{workflow_id}")
#         if not workflow_data:
#             logger.warning(f"No workflow data found for {environment_id}:{workflow_id}")
#             return pd.DataFrame()
        
#         workflow = json.loads(workflow_data)
        
#         current_task = None
#         for task in workflow['tasks']:
#             if task['task_id'] == task_id:
#                 current_task = task
#                 break
        
#         if not current_task or not current_task['dependencies']:
#             logger.info(f"Task {task_id} has no dependencies, using empty DataFrame")
#             return pd.DataFrame()
        
#         input_dataframes = []
#         for dep_task_id in current_task['dependencies']:
#             result_key = f"result:{dep_task_id}"  # dep_task_id已经包含environment_id
#             result_data = await self.redis.get(result_key)
#             if result_data:
#                 df = pickle.loads(result_data)
#                 input_dataframes.append(df)
#                 logger.info(f"Loaded dependency data from {dep_task_id}: {len(df)} rows")
        
#         await redis_json.close()
        
#         if len(input_dataframes) == 1:
#             return input_dataframes[0]
#         elif len(input_dataframes) > 1:
#             result = pd.concat(input_dataframes, ignore_index=True)
#             logger.info(f"Merged {len(input_dataframes)} dependencies: {len(result)} total rows")
#             return result
#         else:
#             logger.warning(f"No dependency data found for task {task_id}")
#             return pd.DataFrame()


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
        environment_id = task_msg['environment_id']
        workflow_id = task_msg['workflow_id']
        
        logger.info(f"Executing task: {task_id}")
        
        try:
            # 验证任务消息格式
            self._validate_task_message(task_msg)
            
            if task_msg['task_type'] == 'FUNCTION':
                result, metadata = await self.execute_function_task(task_msg)
            elif task_msg['task_type'] == 'DATA':
                result, metadata = await self.execute_data_task(task_msg)
            else:
                raise ValueError(f"Unknown task type: {task_msg['task_type']}")
            
            # 使用环境隔离的key格式保存结果
            result_key = f"result:{task_id}"
            await self.redis.setex(result_key, config.DATA_TTL, pickle.dumps(result))
            
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
    
    def _extract_components_from_task_id(self, task_id: str):
        """从task_id中提取组件"""
        # task_id格式: environment_id:workflow_id#task_name
        try:
            env_workflow, task_name = task_id.split('#')
            environment_id, workflow_id = env_workflow.split(':')
            return environment_id, workflow_id, task_name
        except ValueError:
            raise ValueError(f"Invalid task_id format: {task_id}")

    async def execute_function_task(self, task_msg):
        """执行函数任务"""
        function_name = task_msg['function_name']
        config_params = task_msg['config']
        
        # 检查函数是否存在
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
        
        if not isinstance(data_source, dict):
            raise ValueError("Data source must be a dictionary")
        
        source_type = data_source.get('source_type')
        if not source_type:
            raise ValueError("Data source must specify source_type")
        
        if source_type == 'csv':
            result_data = await self.load_csv_data(data_source['query_config'])
        elif source_type == 'sample':
            result_data = await self.generate_sample_data(data_source['query_config'])
        else:
            raise ValueError(f"Unsupported data source: {source_type}")
            
        metadata = {
            "status": "success",
            "message": f"Data task from {source_type} executed successfully",
            "rows_loaded": len(result_data),
            "columns": list(result_data.columns)
        }
        return result_data, metadata

    async def load_csv_data(self, query_config):
        """加载CSV数据"""
        if not isinstance(query_config, dict):
            raise ValueError("Query config must be a dictionary")
        
        file_path = query_config.get('file_path')
        if not file_path:
            raise ValueError("CSV query config must specify file_path")
        
        logger.info(f"Loading CSV data from: {file_path}")
        
        try:
            # 支持额外的CSV读取参数
            csv_params = {
                'encoding': query_config.get('encoding', 'utf-8'),
                'sep': query_config.get('separator', ','),
                'header': query_config.get('header', 0)
            }
            
            data = pd.read_csv(file_path, **csv_params)
            
            # 应用可选的数据过滤
            if 'filters' in query_config:
                for filter_config in query_config['filters']:
                    data = self._apply_data_filter(data, filter_config)
            
            logger.info(f"Loaded {len(data)} rows, {len(data.columns)} columns from CSV")
            return data
        except Exception as e:
            logger.error(f"Failed to load CSV from {file_path}: {e}")
            raise

    async def generate_sample_data(self, query_config):
        """生成示例数据"""
        rows = query_config.get('rows', 100)
        data_type = query_config.get('data_type', 'time_series')
        
        logger.info(f"Generating {rows} rows of {data_type} sample data")
        
        np.random.seed(query_config.get('seed', 42))
        
        if data_type == 'time_series':
            data = pd.DataFrame({
                'timestamp': pd.date_range('2024-01-01', periods=rows, freq='H'),
                'value': np.random.randn(rows) * 10 + 50,
                'category': np.random.choice(['A', 'B', 'C'], rows)
            })
        elif data_type == 'random':
            num_cols = query_config.get('columns', 3)
            data = pd.DataFrame(np.random.randn(rows, num_cols), 
                              columns=[f'col_{i}' for i in range(num_cols)])
        else:
            raise ValueError(f"Unsupported sample data type: {data_type}")
        
        return data
    
    def _apply_data_filter(self, data, filter_config):
        """应用数据过滤"""
        column = filter_config.get('column')
        operator = filter_config.get('operator')
        value = filter_config.get('value')
        
        if not all([column, operator, value is not None]):
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
        
        # 使用文本解码的Redis客户端读取工作流信息
        redis_json = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            decode_responses=True
        )
        
        try:
            workflow_key = f"workflow:{environment_id}:{workflow_id}"
            workflow_data = await redis_json.get(workflow_key)
            if not workflow_data:
                logger.warning(f"No workflow data found for {workflow_key}")
                return pd.DataFrame()
            
            workflow = json.loads(workflow_data)
            
            current_task = None
            for task in workflow['tasks']:
                if task['task_id'] == task_id:
                    current_task = task
                    break
            
            if not current_task:
                logger.warning(f"Task {task_id} not found in workflow")
                return pd.DataFrame()
            
            if not current_task['dependencies']:
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
                else:
                    logger.warning(f"No result data found for dependency {dep_task_id}")
            
            if len(input_dataframes) == 1:
                return input_dataframes[0]
            elif len(input_dataframes) > 1:
                result = pd.concat(input_dataframes, ignore_index=True)
                logger.info(f"Merged {len(input_dataframes)} dependencies: {len(result)} total rows")
                return result
            else:
                logger.warning(f"No dependency data found for task {task_id}")
                return pd.DataFrame()
                
        finally:
            await redis_json.close()