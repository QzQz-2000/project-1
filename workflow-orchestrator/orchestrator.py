# import json
# import uuid
# import asyncio
# import logging
# from datetime import datetime
# from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
# import redis.asyncio as redis
# from models import WorkflowSubmissionRequest, TaskMessage
# from config import config

# logger = logging.getLogger(__name__)

# class WorkflowOrchestrator:
#     """工作流编排器"""
    
#     def __init__(self):
#         self.consumer = None
#         self.producer = None
#         self.redis = None
        
#     async def start(self):
#         """启动编排器"""
#         self.consumer = AIOKafkaConsumer(
#             config.TOPICS['WORKFLOW_SUBMIT'],
#             bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
#             group_id='orchestrator-group',
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
#             decode_responses=True
#         )
        
#         await self.consumer.start()
#         await self.producer.start()
        
#         logger.info("WorkflowOrchestrator started")
    
#     async def stop(self):
#         """停止编排器"""
#         if self.consumer:
#             await self.consumer.stop()
#         if self.producer:
#             await self.producer.stop()
#         if self.redis:
#             await self.redis.close()
#         logger.info("WorkflowOrchestrator stopped")
    
#     async def run(self):
#         """主循环"""
#         try:
#             async for message in self.consumer:
#                 workflow_msg = message.value
#                 await self.process_workflow_submission(workflow_msg)
#         except Exception as e:
#             logger.error(f"Error in orchestrator main loop: {e}")

#     # async def process_workflow_submission(self, workflow_msg):
#     #     """处理工作流提交"""
#     #     workflow_def = workflow_msg['workflow']
#     #     workflow_id = workflow_msg['workflow_id'] # 从消息中获取ID
        
#     #     logger.info(f"Received workflow submission, ID: {workflow_id}")
        
#     #     tasks = []
#     #     task_names = {step['name']: f"{workflow_id}#{step['name']}" for step in workflow_def['steps']}
        
#     #     # 建立任务图
#     #     for step in workflow_def['steps']:
#     #         task = {
#     #             'task_id': task_names[step['name']],
#     #             'name': step['name'],
#     #             'task_type': step['task_type'],
#     #             'config': step.get('config', {}),
#     #             'dependencies': [task_names[dep] for dep in step.get('dependencies', [])],
#     #             'status': 'pending'
#     #         }
            
#     #         if step['task_type'] == 'FUNCTION':
#     #             task['function_name'] = step.get('function_name')
#     #         elif step['task_type'] == 'DATA':
#     #             task['data_source'] = step.get('data_source_config')
            
#     #         tasks.append(task)
        
#     #     workflow_data = {
#     #         'workflow_id': workflow_id,
#     #         'name': workflow_def.get('name', 'Unnamed Workflow'),
#     #         'status': 'running',
#     #         'tasks': tasks,
#     #         'created_at': datetime.now().isoformat()
#     #     }
        
#     #     # 将工作流定义保存到Redis
#     #     await self.redis.set(f"workflow:{workflow_id}", json.dumps(workflow_data))
        
#     #     # 找到初始任务并发送
#     #     initial_tasks = [task for task in tasks if not task['dependencies']]
#     #     for task in initial_tasks:
#     #         await self.send_task(task)
            
#     #     logger.info(f"Orchestrated workflow {workflow_id} with {len(tasks)} tasks")

#     # async def send_task(self, task):
#     #     """发送任务消息"""
#     #     task_message = {
#     #         'task_id': task['task_id'],
#     #         'workflow_id': task['task_id'].split('#')[0],
#     #         'task_type': task['task_type'],
#     #         'config': task['config'],
#     #         'function_name': task.get('function_name'),
#     #         'data_source': task.get('data_source'),
#     #         'timestamp': datetime.now().isoformat()
#     #     }
        
#     #     if task['task_type'] == 'FUNCTION':
#     #         topic = config.TOPICS['TASKS_FUNCTION']
#     #     elif task['task_type'] == 'DATA':
#     #         topic = config.TOPICS['TASKS_DATA']
#     #     else:
#     #         raise ValueError(f"Unknown task type: {task['task_type']}")
        
#     #     await self.producer.send(topic, task_message)
#     #     logger.info(f"Sent task {task['task_id']} to {topic}")

#     async def process_workflow_submission(self, workflow_msg):
#         """处理工作流提交"""
#         workflow_def = workflow_msg['workflow']
#         workflow_id = workflow_msg['workflow_id']
#         environment_id = workflow_msg['environment_id']  # 获取环境ID
        
#         logger.info(f"Received workflow submission, ID: {workflow_id}, Environment: {environment_id}")
        
#         tasks = []
#         task_names = {step['name']: f"{environment_id}:{workflow_id}#{step['name']}" for step in workflow_def['steps']}
        
#         # 建立任务图
#         for step in workflow_def['steps']:
#             task = {
#                 'task_id': task_names[step['name']],
#                 'name': step['name'],
#                 'task_type': step['task_type'],
#                 'config': step.get('config', {}),
#                 'dependencies': [task_names[dep] for dep in step.get('dependencies', [])],
#                 'status': 'pending'
#             }
            
#             if step['task_type'] == 'FUNCTION':
#                 task['function_name'] = step.get('function_name')
#             elif step['task_type'] == 'DATA':
#                 task['data_source'] = step.get('data_source_config')
            
#             tasks.append(task)
        
#         workflow_data = {
#             'workflow_id': workflow_id,
#             'environment_id': environment_id,  # 添加环境ID
#             'name': workflow_def.get('name', 'Unnamed Workflow'),
#             'status': 'running',
#             'tasks': tasks,
#             'created_at': datetime.now().isoformat()
#         }
        
#         # 使用包含环境ID的key保存到Redis
#         await self.redis.set(f"workflow:{environment_id}:{workflow_id}", json.dumps(workflow_data))
        
#         # 找到初始任务并发送
#         initial_tasks = [task for task in tasks if not task['dependencies']]
#         for task in initial_tasks:
#             await self.send_task(task, environment_id)
            
#         logger.info(f"Orchestrated workflow {workflow_id} in environment {environment_id} with {len(tasks)} tasks")

#     async def send_task(self, task, environment_id):
#         """发送任务消息"""
#         task_message = {
#             'task_id': task['task_id'],
#             'environment_id': environment_id,  # 添加环境ID
#             'workflow_id': task['task_id'].split('#')[0].split(':')[1],  # 提取workflow_id
#             'task_type': task['task_type'],
#             'config': task['config'],
#             'function_name': task.get('function_name'),
#             'data_source': task.get('data_source'),
#             'timestamp': datetime.now().isoformat()
#         }
        
#         if task['task_type'] == 'FUNCTION':
#             topic = config.TOPICS['TASKS_FUNCTION']
#         elif task['task_type'] == 'DATA':
#             topic = config.TOPICS['TASKS_DATA']
#         else:
#             raise ValueError(f"Unknown task type: {task['task_type']}")
        
#         await self.producer.send(topic, task_message)
#         logger.info(f"Sent task {task['task_id']} to {topic}")

import json
import uuid
import asyncio
import logging
from datetime import datetime
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import redis.asyncio as redis
from models import WorkflowSubmissionRequest, TaskMessage
from config import config

logger = logging.getLogger(__name__)

class WorkflowOrchestrator:
    """工作流编排器"""
    
    def __init__(self):
        self.consumer = None
        self.producer = None
        self.redis = None
        
    async def start(self):
        """启动编排器"""
        self.consumer = AIOKafkaConsumer(
            config.TOPICS['WORKFLOW_SUBMIT'],
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            group_id='orchestrator-group',
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
            decode_responses=True
        )
        
        await self.consumer.start()
        await self.producer.start()
        
        logger.info("WorkflowOrchestrator started")
    
    async def stop(self):
        """停止编排器"""
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()
        if self.redis:
            await self.redis.close()
        logger.info("WorkflowOrchestrator stopped")
    
    async def run(self):
        """主循环"""
        try:
            async for message in self.consumer:
                workflow_msg = message.value
                await self.process_workflow_submission(workflow_msg)
        except Exception as e:
            logger.error(f"Error in orchestrator main loop: {e}")

    async def process_workflow_submission(self, workflow_msg):
        """处理工作流提交"""
        workflow_def = workflow_msg['workflow']
        workflow_id = workflow_msg['workflow_id']
        environment_id = workflow_msg['environment_id']
        
        logger.info(f"Received workflow submission, ID: {workflow_id}, Environment: {environment_id}")
        
        try:
            # 验证工作流定义
            self._validate_workflow_definition(workflow_def)
            
            tasks = []
            # 使用统一的任务ID格式: environment_id:workflow_id#task_name
            task_names = {step['name']: f"{environment_id}:{workflow_id}#{step['name']}" for step in workflow_def['steps']}
            
            # 建立任务图
            for step in workflow_def['steps']:
                task = {
                    'task_id': task_names[step['name']],
                    'name': step['name'],
                    'task_type': step['task_type'],
                    'config': step.get('config', {}),
                    'dependencies': [task_names[dep] for dep in step.get('dependencies', [])],
                    'status': 'pending',
                    'created_at': datetime.now().isoformat()
                }
                
                if step['task_type'] == 'FUNCTION':
                    task['function_name'] = step.get('function_name')
                    if not task['function_name']:
                        raise ValueError(f"Function name is required for FUNCTION task: {step['name']}")
                elif step['task_type'] == 'DATA':
                    task['data_source'] = step.get('data_source_config')
                    if not task['data_source']:
                        raise ValueError(f"Data source config is required for DATA task: {step['name']}")
                else:
                    raise ValueError(f"Unknown task type: {step['task_type']}")
                
                tasks.append(task)
            
            # 验证依赖关系
            self._validate_dependencies(tasks, task_names)
            
            workflow_data = {
                'workflow_id': workflow_id,
                'environment_id': environment_id,
                'name': workflow_def.get('name', 'Unnamed Workflow'),
                'description': workflow_def.get('description', ''),
                'status': 'running',
                'tasks': tasks,
                'created_at': datetime.now().isoformat(),
                'submitted_by': workflow_msg.get('submitted_by', 'unknown')
            }
            
            # 使用环境隔离的key保存到Redis
            workflow_key = f"workflow:{environment_id}:{workflow_id}"
            await self.redis.set(workflow_key, json.dumps(workflow_data))
            
            # 找到初始任务并发送
            initial_tasks = [task for task in tasks if not task['dependencies']]
            if not initial_tasks:
                raise ValueError("No initial tasks found (all tasks have dependencies)")
            
            for task in initial_tasks:
                await self.send_task(task, environment_id, workflow_id)
                
            logger.info(f"Orchestrated workflow {workflow_id} in environment {environment_id} with {len(tasks)} tasks, {len(initial_tasks)} initial tasks")
            
        except Exception as e:
            logger.error(f"Error processing workflow submission: {e}")
            # 标记工作流为失败状态
            await self._mark_workflow_failed(environment_id, workflow_id, str(e))
            raise

    def _validate_workflow_definition(self, workflow_def):
        """验证工作流定义"""
        if not isinstance(workflow_def, dict):
            raise ValueError("Workflow definition must be a dictionary")
        
        if 'steps' not in workflow_def:
            raise ValueError("Workflow definition must contain 'steps'")
        
        steps = workflow_def['steps']
        if not isinstance(steps, list) or len(steps) == 0:
            raise ValueError("Workflow steps must be a non-empty list")
        
        step_names = set()
        for step in steps:
            if not isinstance(step, dict):
                raise ValueError("Each step must be a dictionary")
            
            if 'name' not in step:
                raise ValueError("Each step must have a 'name'")
            
            if step['name'] in step_names:
                raise ValueError(f"Duplicate step name: {step['name']}")
            step_names.add(step['name'])
            
            if 'task_type' not in step:
                raise ValueError(f"Step '{step['name']}' must have a 'task_type'")
            
            if step['task_type'] not in ['FUNCTION', 'DATA']:
                raise ValueError(f"Invalid task_type '{step['task_type']}' in step '{step['name']}'")
    
    def _validate_dependencies(self, tasks, task_names):
        """验证依赖关系"""
        task_names_set = set(task_names.keys())
        
        for task in tasks:
            for dep in task['dependencies']:
                dep_name = dep.split('#')[-1]  # 从完整task_id中提取任务名
                if dep_name not in task_names_set:
                    raise ValueError(f"Task '{task['name']}' depends on non-existent task '{dep_name}'")
        
        # 检查循环依赖
        if self._has_circular_dependency(tasks):
            raise ValueError("Circular dependency detected in workflow")
    
    def _has_circular_dependency(self, tasks):
        """检查是否存在循环依赖"""
        # 简单的循环依赖检测，使用DFS
        task_map = {task['task_id']: task for task in tasks}
        visited = set()
        rec_stack = set()
        
        def dfs(task_id):
            if task_id in rec_stack:
                return True  # 发现循环
            if task_id in visited:
                return False
            
            visited.add(task_id)
            rec_stack.add(task_id)
            
            task = task_map.get(task_id)
            if task:
                for dep_id in task['dependencies']:
                    if dfs(dep_id):
                        return True
            
            rec_stack.remove(task_id)
            return False
        
        for task in tasks:
            if task['task_id'] not in visited:
                if dfs(task['task_id']):
                    return True
        return False

    async def send_task(self, task, environment_id: str, workflow_id: str):
        """发送任务消息"""
        task_message = {
            'task_id': task['task_id'],
            'environment_id': environment_id,
            'workflow_id': workflow_id,
            'task_type': task['task_type'],
            'config': task['config'],
            'function_name': task.get('function_name'),
            'data_source': task.get('data_source'),
            'timestamp': datetime.now().isoformat()
        }
        
        if task['task_type'] == 'FUNCTION':
            topic = config.TOPICS['TASKS_FUNCTION']
        elif task['task_type'] == 'DATA':
            topic = config.TOPICS['TASKS_DATA']
        else:
            raise ValueError(f"Unknown task type: {task['task_type']}")
        
        await self.producer.send(topic, task_message)
        logger.info(f"Sent task {task['task_id']} to {topic}")
    
    async def _mark_workflow_failed(self, environment_id: str, workflow_id: str, error_message: str):
        """标记工作流为失败状态"""
        try:
            workflow_key = f"workflow:{environment_id}:{workflow_id}"
            workflow_data = await self.redis.get(workflow_key)
            
            if workflow_data:
                workflow = json.loads(workflow_data)
                workflow['status'] = 'failed'
                workflow['error'] = error_message
                workflow['failed_at'] = datetime.now().isoformat()
                
                await self.redis.set(workflow_key, json.dumps(workflow))
                logger.info(f"Marked workflow {environment_id}:{workflow_id} as failed: {error_message}")
        except Exception as e:
            logger.error(f"Error marking workflow as failed: {e}")