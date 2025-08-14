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
                workflow_request = message.value
                await self.handle_workflow_submission(workflow_request)
        except Exception as e:
            logger.error(f"Error in orchestrator main loop: {e}")
    
    async def handle_workflow_submission(self, request_data):
        """处理工作流提交"""
        try:
            workflow_id = f"wf_{uuid.uuid4().hex}"
            logger.info(f"Processing workflow submission: {workflow_id}")
            
            task_graph = await self.build_task_graph(workflow_id, request_data['workflow'])
            
            await self.redis.setex(
                f"workflow:{workflow_id}",
                config.DATA_TTL,
                json.dumps(task_graph)
            )
            
            ready_tasks = [task for task in task_graph['tasks'] if not task['dependencies']]
            
            for task in ready_tasks:
                await self.send_task(task)
            
            logger.info(f"Workflow {workflow_id} submitted with {len(ready_tasks)} initial tasks")
            
        except Exception as e:
            logger.error(f"Error handling workflow submission: {e}")
    
    async def build_task_graph(self, workflow_id: str, workflow_def: dict):
        """构建任务图"""
        tasks = []
        
        for step in workflow_def['steps']:
            task = {
                'task_id': f"{workflow_id}#{step['name']}",
                'name': step['name'],
                'task_type': step['task_type'],
                'config': step.get('config', {}),
                'dependencies': [f"{workflow_id}#{dep}" for dep in step.get('dependencies', [])],
                'status': 'pending'
            }
            
            if step['task_type'] == 'FUNCTION':
                task['function_name'] = step.get('function_name')
            elif step['task_type'] == 'DATA':
                task['data_source'] = step.get('data_source_config')
            
            tasks.append(task)
        
        return {
            'workflow_id': workflow_id,
            'name': workflow_def.get('name', 'Unnamed Workflow'),
            'status': 'running',
            'tasks': tasks,
            'created_at': datetime.now().isoformat()
        }
    
    async def send_task(self, task):
        """发送任务消息"""
        task_message = {
            'task_id': task['task_id'],
            'workflow_id': task['task_id'].split('#')[0],
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
