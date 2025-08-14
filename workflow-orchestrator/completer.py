import json
import asyncio
import logging
from datetime import datetime
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import redis.asyncio as redis
from config import config

logger = logging.getLogger(__name__)

class CompletionHandler:
    """完成处理器"""
    
    def __init__(self):
        self.consumer = None
        self.producer = None
        self.redis = None
        
    async def start(self):
        """启动完成处理器"""
        self.consumer = AIOKafkaConsumer(
            config.TOPICS['TASKS_COMPLETED'],
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            group_id='completer-group',
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
        
        logger.info("CompletionHandler started")
    
    async def stop(self):
        """停止完成处理器"""
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()
        if self.redis:
            await self.redis.close()
        logger.info("CompletionHandler stopped")
    
    async def run(self):
        """主循环"""
        try:
            async for message in self.consumer:
                completion_msg = message.value
                await self.handle_task_completion(completion_msg)
        except Exception as e:
            logger.error(f"Error in completer main loop: {e}")
    
    async def handle_task_completion(self, completion_msg):
        """处理任务完成"""
        workflow_id = completion_msg['workflow_id']
        completed_task_id = completion_msg['task_id']
        status = completion_msg['status']
        
        logger.info(f"Handling task completion: {completed_task_id} - {status}")
        
        try:
            await self.update_task_status(workflow_id, completed_task_id, status)
            
            if status == 'completed':
                ready_tasks = await self.find_ready_tasks(workflow_id)
                
                for task in ready_tasks:
                    await self.send_task(task)
                
                logger.info(f"Found {len(ready_tasks)} ready tasks after {completed_task_id}")
            
            if await self.is_workflow_completed(workflow_id):
                await self.complete_workflow(workflow_id)
                
        except Exception as e:
            logger.error(f"Error handling task completion: {e}")
    
    async def update_task_status(self, workflow_id: str, task_id: str, status: str):
        """原子性更新任务状态"""
        lua_script = """
        local workflow_key = KEYS[1]
        local task_id = ARGV[1]
        local status = ARGV[2]
        
        local workflow_data = redis.call('GET', workflow_key)
        if not workflow_data then 
            return nil 
        end
        
        local workflow = cjson.decode(workflow_data)
        for i, task in ipairs(workflow.tasks) do
            if task.task_id == task_id then
                workflow.tasks[i].status = status
                workflow.tasks[i].updated_at = ARGV[3]
                break
            end
        end
        
        redis.call('SET', workflow_key, cjson.encode(workflow))
        return 'OK'
        """
        
        await self.redis.eval(
            lua_script,
            1,
            f"workflow:{workflow_id}",
            task_id,
            status,
            datetime.now().isoformat()
        )
        
        logger.info(f"Updated task {task_id} status to {status}")
    
    async def find_ready_tasks(self, workflow_id: str):
        """找出就绪的任务"""
        workflow_data = await self.redis.get(f"workflow:{workflow_id}")
        if not workflow_data:
            return []
        
        workflow = json.loads(workflow_data)
        ready_tasks = []
        
        for task in workflow['tasks']:
            if task['status'] != 'pending':
                continue
            
            dependencies_completed = True
            for dep_task_id in task['dependencies']:
                dep_task = next((t for t in workflow['tasks'] if t['task_id'] == dep_task_id), None)
                if not dep_task or dep_task['status'] != 'completed':
                    dependencies_completed = False
                    break
            
            if dependencies_completed:
                ready_tasks.append(task)
        
        return ready_tasks
    
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
        logger.info(f"Sent ready task {task['task_id']} to {topic}")
    
    async def is_workflow_completed(self, workflow_id: str):
        """检查工作流是否完成"""
        workflow_data = await self.redis.get(f"workflow:{workflow_id}")
        if not workflow_data:
            return True
        
        workflow = json.loads(workflow_data)
        
        for task in workflow['tasks']:
            if task['status'] not in ['completed', 'failed']:
                return False
        
        return True
    
    async def complete_workflow(self, workflow_id: str):
        """完成工作流"""
        workflow_data = await self.redis.get(f"workflow:{workflow_id}")
        workflow = json.loads(workflow_data) if workflow_data else {}
        
        completed_tasks = sum(1 for task in workflow.get('tasks', []) if task['status'] == 'completed')
        failed_tasks = sum(1 for task in workflow.get('tasks', []) if task['status'] == 'failed')
        total_tasks = len(workflow.get('tasks', []))
        
        workflow_status = 'completed' if failed_tasks == 0 else 'failed'
        
        completion_event = {
            'workflow_id': workflow_id,
            'status': workflow_status,
            'total_tasks': total_tasks,
            'completed_tasks': completed_tasks,
            'failed_tasks': failed_tasks,
            'timestamp': datetime.now().isoformat()
        }
        
        await self.producer.send(config.TOPICS['WORKFLOWS_COMPLETED'], completion_event)
        
        logger.info(f"Workflow {workflow_id} completed: {workflow_status} "
                   f"({completed_tasks}/{total_tasks} tasks successful)")
