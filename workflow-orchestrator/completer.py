# # import json
# # import asyncio
# # import logging
# # from datetime import datetime
# # from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
# # import redis.asyncio as redis
# # from config import config

# # logger = logging.getLogger(__name__)

# # class CompletionHandler:
# #     """完成处理器"""
    
# #     def __init__(self):
# #         self.consumer = None
# #         self.producer = None
# #         self.redis = None
        
# #     async def start(self):
# #         """启动完成处理器"""
# #         self.consumer = AIOKafkaConsumer(
# #             config.TOPICS['TASKS_COMPLETED'],
# #             bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
# #             group_id='completer-group',
# #             value_deserializer=lambda m: json.loads(m.decode('utf-8'))
# #         )
        
# #         self.producer = AIOKafkaProducer(
# #             bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
# #             value_serializer=lambda v: json.dumps(v).encode('utf-8')
# #         )
        
# #         self.redis = redis.Redis(
# #             host=config.REDIS_HOST,
# #             port=config.REDIS_PORT,
# #             db=config.REDIS_DB,
# #             decode_responses=True
# #         )
        
# #         await self.consumer.start()
# #         await self.producer.start()
        
# #         logger.info("CompletionHandler started")
    
# #     async def stop(self):
# #         """停止完成处理器"""
# #         if self.consumer:
# #             await self.consumer.stop()
# #         if self.producer:
# #             await self.producer.stop()
# #         if self.redis:
# #             await self.redis.close()
# #         logger.info("CompletionHandler stopped")
    
# #     async def run(self):
# #         """主循环"""
# #         try:
# #             async for message in self.consumer:
# #                 completion_msg = message.value
# #                 await self.handle_task_completion(completion_msg)
# #         except Exception as e:
# #             logger.error(f"Error in completer main loop: {e}")
    
# #     async def handle_task_completion(self, completion_msg):
# #         """处理任务完成"""
# #         workflow_id = completion_msg['workflow_id']
# #         completed_task_id = completion_msg['task_id']
# #         status = completion_msg['status']
        
# #         logger.info(f"Handling task completion: {completed_task_id} - {status}")
        
# #         try:
# #             await self.update_task_status(workflow_id, completed_task_id, status)
            
# #             if status == 'completed':
# #                 ready_tasks = await self.find_ready_tasks(workflow_id)
                
# #                 for task in ready_tasks:
# #                     await self.send_task(task)
                
# #                 logger.info(f"Found {len(ready_tasks)} ready tasks after {completed_task_id}")
            
# #             if await self.is_workflow_completed(workflow_id):
# #                 await self.complete_workflow(workflow_id)
                
# #         except Exception as e:
# #             logger.error(f"Error handling task completion: {e}")
    
# #     # async def update_task_status(self, workflow_id: str, task_id: str, status: str):
# #     #     """原子性更新任务状态"""
# #     #     lua_script = """
# #     #     local workflow_key = KEYS[1]
# #     #     local task_id = ARGV[1]
# #     #     local status = ARGV[2]
        
# #     #     local workflow_data = redis.call('GET', workflow_key)
# #     #     if not workflow_data then 
# #     #         return nil 
# #     #     end
        
# #     #     local workflow = cjson.decode(workflow_data)
# #     #     for i, task in ipairs(workflow.tasks) do
# #     #         if task.task_id == task_id then
# #     #             workflow.tasks[i].status = status
# #     #             workflow.tasks[i].updated_at = ARGV[3]
# #     #             break
# #     #         end
# #     #     end
        
# #     #     redis.call('SET', workflow_key, cjson.encode(workflow))
# #     #     return 'OK'
# #     #     """
        
# #     #     await self.redis.eval(
# #     #         lua_script,
# #     #         1,
# #     #         f"workflow:{workflow_id}",
# #     #         task_id,
# #     #         status,
# #     #         datetime.now().isoformat()
# #     #     )
        
# #     #     logger.info(f"Updated task {task_id} status to {status}")
    
# #     # async def find_ready_tasks(self, workflow_id: str):
# #     #     """找出就绪的任务"""
# #     #     workflow_data = await self.redis.get(f"workflow:{workflow_id}")
# #     #     if not workflow_data:
# #     #         return []
        
# #     #     workflow = json.loads(workflow_data)
# #     #     ready_tasks = []
        
# #     #     for task in workflow['tasks']:
# #     #         if task['status'] != 'pending':
# #     #             continue
            
# #     #         dependencies_completed = True
# #     #         for dep_task_id in task['dependencies']:
# #     #             dep_task = next((t for t in workflow['tasks'] if t['task_id'] == dep_task_id), None)
# #     #             if not dep_task or dep_task['status'] != 'completed':
# #     #                 dependencies_completed = False
# #     #                 break
            
# #     #         if dependencies_completed:
# #     #             ready_tasks.append(task)
        
# #     #     return ready_tasks

# #     async def update_task_status(self, workflow_id: str, task_id: str, status: str):
# #         """原子性更新任务状态"""
# #         # 从task_id中提取environment_id
# #         environment_id = task_id.split(':')[0]
        
# #         lua_script = """
# #         local workflow_key = KEYS[1]
# #         local task_id = ARGV[1]
# #         local status = ARGV[2]
        
# #         local workflow_data = redis.call('GET', workflow_key)
# #         if not workflow_data then 
# #             return nil 
# #         end
        
# #         local workflow = cjson.decode(workflow_data)
# #         for i, task in ipairs(workflow.tasks) do
# #             if task.task_id == task_id then
# #                 workflow.tasks[i].status = status
# #                 workflow.tasks[i].updated_at = ARGV[3]
# #                 break
# #             end
# #         end
        
# #         redis.call('SET', workflow_key, cjson.encode(workflow))
# #         return 'OK'
# #         """
        
# #         await self.redis.eval(
# #             lua_script,
# #             1,
# #             f"workflow:{environment_id}:{workflow_id}",  # 使用环境隔离的key
# #             task_id,
# #             status,
# #             datetime.now().isoformat()
# #         )

# #     async def find_ready_tasks(self, workflow_id: str):
# #         """找出就绪的任务"""
# #         # 从消息中获取environment_id
# #         environment_id = workflow_id.split(':')[0] if ':' in workflow_id else None
# #         if not environment_id:
# #             return []
        
# #         workflow_data = await self.redis.get(f"workflow:{environment_id}:{workflow_id}")
# #         if not workflow_data:
# #             return []
    
# #     async def send_task(self, task):
# #         """发送任务消息"""
# #         task_message = {
# #             'task_id': task['task_id'],
# #             'workflow_id': task['task_id'].split('#')[0],
# #             'task_type': task['task_type'],
# #             'config': task['config'],
# #             'function_name': task.get('function_name'),
# #             'data_source': task.get('data_source'),
# #             'timestamp': datetime.now().isoformat()
# #         }
        
# #         if task['task_type'] == 'FUNCTION':
# #             topic = config.TOPICS['TASKS_FUNCTION']
# #         elif task['task_type'] == 'DATA':
# #             topic = config.TOPICS['TASKS_DATA']
# #         else:
# #             raise ValueError(f"Unknown task type: {task['task_type']}")
        
# #         await self.producer.send(topic, task_message)
# #         logger.info(f"Sent ready task {task['task_id']} to {topic}")
    
# #     async def is_workflow_completed(self, workflow_id: str):
# #         """检查工作流是否完成"""
# #         workflow_data = await self.redis.get(f"workflow:{workflow_id}")
# #         if not workflow_data:
# #             return True
        
# #         workflow = json.loads(workflow_data)
        
# #         for task in workflow['tasks']:
# #             if task['status'] not in ['completed', 'failed']:
# #                 return False
        
# #         return True
    
# #     # async def complete_workflow(self, workflow_id: str):
# #     #     """完成工作流"""
# #     #     workflow_data = await self.redis.get(f"workflow:{workflow_id}")
# #     #     workflow = json.loads(workflow_data) if workflow_data else {}
        
# #     #     completed_tasks = sum(1 for task in workflow.get('tasks', []) if task['status'] == 'completed')
# #     #     failed_tasks = sum(1 for task in workflow.get('tasks', []) if task['status'] == 'failed')
# #     #     total_tasks = len(workflow.get('tasks', []))
        
# #     #     workflow_status = 'completed' if failed_tasks == 0 else 'failed'
        
# #     #     completion_event = {
# #     #         'workflow_id': workflow_id,
# #     #         'status': workflow_status,
# #     #         'total_tasks': total_tasks,
# #     #         'completed_tasks': completed_tasks,
# #     #         'failed_tasks': failed_tasks,
# #     #         'timestamp': datetime.now().isoformat()
# #     #     }
        
# #     #     await self.producer.send(config.TOPICS['WORKFLOWS_COMPLETED'], completion_event)
        
# #     #     logger.info(f"Workflow {workflow_id} completed: {workflow_status} "
# #     #                f"({completed_tasks}/{total_tasks} tasks successful)")

# #     async def complete_workflow(self, workflow_id: str):
# #         """完成工作流"""
# #         workflow_data = await self.redis.get(f"workflow:{workflow_id}")
# #         workflow = json.loads(workflow_data) if workflow_data else {}
        
# #         completed_tasks = sum(1 for task in workflow.get('tasks', []) if task['status'] == 'completed')
# #         failed_tasks = sum(1 for task in workflow.get('tasks', []) if task['status'] == 'failed')
# #         total_tasks = len(workflow.get('tasks', []))
        
# #         workflow_status = 'completed' if failed_tasks == 0 else 'failed'
        
# #         # 更新Redis中的工作流状态
# #         workflow['status'] = workflow_status
# #         workflow['completed_at'] = datetime.now().isoformat()
# #         await self.redis.set(f"workflow:{workflow_id}", json.dumps(workflow))
        
# #         completion_event = {
# #             'workflow_id': workflow_id,
# #             'status': workflow_status,
# #             'total_tasks': total_tasks,
# #             'completed_tasks': completed_tasks,
# #             'failed_tasks': failed_tasks,
# #             'timestamp': datetime.now().isoformat()
# #         }
        
# #         await self.producer.send(config.TOPICS['WORKFLOWS_COMPLETED'], completion_event)
        
# #         logger.info(f"Workflow {workflow_id} completed: {workflow_status} "
# #                 f"({completed_tasks}/{total_tasks} tasks successful)")

# import json
# import asyncio
# import logging
# from datetime import datetime
# from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
# import redis.asyncio as redis
# from config import config

# logger = logging.getLogger(__name__)

# class CompletionHandler:
#     """完成处理器 - 修复版本"""
    
#     def __init__(self):
#         self.consumer = None
#         self.producer = None
#         self.redis = None
        
#     async def start(self):
#         """启动完成处理器"""
#         self.consumer = AIOKafkaConsumer(
#             config.TOPICS['TASKS_COMPLETED'],
#             bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
#             group_id='completer-group',
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
        
#         logger.info("CompletionHandler started")
    
#     async def stop(self):
#         """停止完成处理器"""
#         if self.consumer:
#             await self.consumer.stop()
#         if self.producer:
#             await self.producer.stop()
#         if self.redis:
#             await self.redis.close()
#         logger.info("CompletionHandler stopped")
    
#     async def run(self):
#         """主循环"""
#         try:
#             async for message in self.consumer:
#                 completion_msg = message.value
#                 await self.handle_task_completion(completion_msg)
#         except Exception as e:
#             logger.error(f"Error in completer main loop: {e}")
    
#     async def handle_task_completion(self, completion_msg):
#         """处理任务完成"""
#         workflow_id = completion_msg['workflow_id']
#         completed_task_id = completion_msg['task_id']
#         status = completion_msg['status']
        
#         logger.info(f"Handling task completion: {completed_task_id} - {status}")
        
#         # 从task_id中提取环境ID
#         environment_id = self._extract_environment_id(completed_task_id)
#         if not environment_id:
#             logger.error(f"无法从task_id中提取environment_id: {completed_task_id}")
#             return
        
#         try:
#             await self.update_task_status(environment_id, workflow_id, completed_task_id, status)
            
#             if status == 'completed':
#                 ready_tasks = await self.find_ready_tasks(environment_id, workflow_id)
                
#                 for task in ready_tasks:
#                     await self.send_task(task, environment_id)
                
#                 logger.info(f"Found {len(ready_tasks)} ready tasks after {completed_task_id}")
            
#             if await self.is_workflow_completed(environment_id, workflow_id):
#                 await self.complete_workflow(environment_id, workflow_id)
                
#         except Exception as e:
#             logger.error(f"Error handling task completion: {e}")
    
#     def _extract_environment_id(self, task_id: str) -> str:
#         """从task_id中提取环境ID"""
#         # task_id格式: environment_id:workflow_id#task_name
#         parts = task_id.split(':')
#         if len(parts) >= 2:
#             return parts[0]
#         return None
    
#     def _extract_workflow_id(self, task_id: str) -> str:
#         """从task_id中提取工作流ID"""
#         # task_id格式: environment_id:workflow_id#task_name
#         parts = task_id.split(':')
#         if len(parts) >= 2:
#             workflow_part = parts[1].split('#')[0]
#             return workflow_part
#         return None

#     async def update_task_status(self, environment_id: str, workflow_id: str, task_id: str, status: str):
#         """原子性更新任务状态"""
#         lua_script = """
#         local workflow_key = KEYS[1]
#         local task_id = ARGV[1]
#         local status = ARGV[2]
        
#         local workflow_data = redis.call('GET', workflow_key)
#         if not workflow_data then 
#             return nil 
#         end
        
#         local workflow = cjson.decode(workflow_data)
#         for i, task in ipairs(workflow.tasks) do
#             if task.task_id == task_id then
#                 workflow.tasks[i].status = status
#                 workflow.tasks[i].updated_at = ARGV[3]
#                 break
#             end
#         end
        
#         redis.call('SET', workflow_key, cjson.encode(workflow))
#         return 'OK'
#         """
        
#         await self.redis.eval(
#             lua_script,
#             1,
#             f"workflow:{environment_id}:{workflow_id}",
#             task_id,
#             status,
#             datetime.now().isoformat()
#         )
        
#         logger.info(f"Updated task {task_id} status to {status}")

#     async def find_ready_tasks(self, environment_id: str, workflow_id: str):
#         """找出就绪的任务"""
#         workflow_data = await self.redis.get(f"workflow:{environment_id}:{workflow_id}")
#         if not workflow_data:
#             logger.warning(f"No workflow data found for {environment_id}:{workflow_id}")
#             return []
        
#         workflow = json.loads(workflow_data)
#         ready_tasks = []
        
#         for task in workflow['tasks']:
#             if task['status'] != 'pending':
#                 continue
            
#             dependencies_completed = True
#             for dep_task_id in task['dependencies']:
#                 dep_task = next((t for t in workflow['tasks'] if t['task_id'] == dep_task_id), None)
#                 if not dep_task or dep_task['status'] != 'completed':
#                     dependencies_completed = False
#                     break
            
#             if dependencies_completed:
#                 ready_tasks.append(task)
        
#         return ready_tasks
    
#     async def send_task(self, task, environment_id: str):
#         """发送任务消息"""
#         # 从task_id中提取workflow_id
#         workflow_id = self._extract_workflow_id(task['task_id'])
        
#         task_message = {
#             'task_id': task['task_id'],
#             'environment_id': environment_id,
#             'workflow_id': workflow_id,
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
#         logger.info(f"Sent ready task {task['task_id']} to {topic}")
    
#     async def is_workflow_completed(self, environment_id: str, workflow_id: str):
#         """检查工作流是否完成"""
#         workflow_data = await self.redis.get(f"workflow:{environment_id}:{workflow_id}")
#         if not workflow_data:
#             return True
        
#         workflow = json.loads(workflow_data)
        
#         for task in workflow['tasks']:
#             if task['status'] not in ['completed', 'failed']:
#                 return False
        
#         return True

#     async def complete_workflow(self, environment_id: str, workflow_id: str):
#         """完成工作流"""
#         workflow_data = await self.redis.get(f"workflow:{environment_id}:{workflow_id}")
#         workflow = json.loads(workflow_data) if workflow_data else {}
        
#         completed_tasks = sum(1 for task in workflow.get('tasks', []) if task['status'] == 'completed')
#         failed_tasks = sum(1 for task in workflow.get('tasks', []) if task['status'] == 'failed')
#         total_tasks = len(workflow.get('tasks', []))
        
#         workflow_status = 'completed' if failed_tasks == 0 else 'failed'
        
#         # 更新Redis中的工作流状态
#         workflow['status'] = workflow_status
#         workflow['completed_at'] = datetime.now().isoformat()
#         await self.redis.set(f"workflow:{environment_id}:{workflow_id}", json.dumps(workflow))
        
#         completion_event = {
#             'workflow_id': workflow_id,
#             'environment_id': environment_id,
#             'status': workflow_status,
#             'total_tasks': total_tasks,
#             'completed_tasks': completed_tasks,
#             'failed_tasks': failed_tasks,
#             'timestamp': datetime.now().isoformat()
#         }
        
#         await self.producer.send(config.TOPICS['WORKFLOWS_COMPLETED'], completion_event)
        
#         logger.info(f"Workflow {environment_id}:{workflow_id} completed: {workflow_status} "
#                 f"({completed_tasks}/{total_tasks} tasks successful)")

import json
import asyncio
import logging
from datetime import datetime
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import redis.asyncio as redis
from config import config

logger = logging.getLogger(__name__)

class CompletionHandler:
    """完成处理器 - 修复版本"""
    
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
        task_id = completion_msg['task_id']
        environment_id = completion_msg['environment_id']
        workflow_id = completion_msg['workflow_id']
        status = completion_msg['status']
        
        logger.info(f"Handling task completion: {task_id} - {status}")
        
        try:
            # 验证完成消息格式
            self._validate_completion_message(completion_msg)
            
            await self.update_task_status(environment_id, workflow_id, task_id, status)
            
            if status == 'completed':
                ready_tasks = await self.find_ready_tasks(environment_id, workflow_id)
                
                for task in ready_tasks:
                    await self.send_task(task, environment_id, workflow_id)
                
                logger.info(f"Found {len(ready_tasks)} ready tasks after {task_id}")
            
            # 检查工作流是否完成
            if await self.is_workflow_completed(environment_id, workflow_id):
                await self.complete_workflow(environment_id, workflow_id)
                
        except Exception as e:
            logger.error(f"Error handling task completion: {e}", exc_info=True)
    
    def _validate_completion_message(self, completion_msg):
        """验证完成消息格式"""
        required_fields = ['task_id', 'environment_id', 'workflow_id', 'status']
        for field in required_fields:
            if field not in completion_msg:
                raise ValueError(f"Missing required field in completion message: {field}")
        
        if completion_msg['status'] not in ['completed', 'failed']:
            raise ValueError(f"Invalid status: {completion_msg['status']}")
    
    def _extract_components_from_task_id(self, task_id: str):
        """从task_id中提取组件"""
        # task_id格式: environment_id:workflow_id#task_name
        try:
            env_workflow, task_name = task_id.split('#')
            environment_id, workflow_id = env_workflow.split(':')
            return environment_id, workflow_id, task_name
        except ValueError:
            raise ValueError(f"Invalid task_id format: {task_id}")

    async def update_task_status(self, environment_id: str, workflow_id: str, task_id: str, status: str):
        """原子性更新任务状态"""
        lua_script = """
        local workflow_key = KEYS[1]
        local task_id = ARGV[1]
        local status = ARGV[2]
        local timestamp = ARGV[3]
        
        local workflow_data = redis.call('GET', workflow_key)
        if not workflow_data then 
            return {err = "Workflow not found"}
        end
        
        local workflow = cjson.decode(workflow_data)
        local task_found = false
        
        for i, task in ipairs(workflow.tasks) do
            if task.task_id == task_id then
                workflow.tasks[i].status = status
                workflow.tasks[i].updated_at = timestamp
                task_found = true
                break
            end
        end
        
        if not task_found then
            return {err = "Task not found"}
        end
        
        redis.call('SET', workflow_key, cjson.encode(workflow))
        return 'OK'
        """
        
        try:
            result = await self.redis.eval(
                lua_script,
                1,
                f"workflow:{environment_id}:{workflow_id}",
                task_id,
                status,
                datetime.now().isoformat()
            )
            
            if isinstance(result, dict) and 'err' in result:
                raise ValueError(result['err'])
            
            logger.info(f"Updated task {task_id} status to {status}")
            
        except Exception as e:
            logger.error(f"Failed to update task status: {e}")
            raise

    async def find_ready_tasks(self, environment_id: str, workflow_id: str):
        """找出就绪的任务"""
        workflow_key = f"workflow:{environment_id}:{workflow_id}"
        workflow_data = await self.redis.get(workflow_key)
        
        if not workflow_data:
            logger.warning(f"No workflow data found for {workflow_key}")
            return []
        
        workflow = json.loads(workflow_data)
        ready_tasks = []
        
        for task in workflow['tasks']:
            if task['status'] != 'pending':
                continue
            
            # 检查所有依赖是否已完成
            dependencies_completed = True
            for dep_task_id in task['dependencies']:
                dep_task = next((t for t in workflow['tasks'] if t['task_id'] == dep_task_id), None)
                if not dep_task or dep_task['status'] != 'completed':
                    dependencies_completed = False
                    break
            
            if dependencies_completed:
                ready_tasks.append(task)
        
        return ready_tasks
    
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
        logger.info(f"Sent ready task {task['task_id']} to {topic}")
    
    async def is_workflow_completed(self, environment_id: str, workflow_id: str):
        """检查工作流是否完成"""
        workflow_key = f"workflow:{environment_id}:{workflow_id}"
        workflow_data = await self.redis.get(workflow_key)
        
        if not workflow_data:
            logger.warning(f"Workflow not found when checking completion: {workflow_key}")
            return True
        
        workflow = json.loads(workflow_data)
        
        for task in workflow['tasks']:
            if task['status'] not in ['completed', 'failed']:
                return False
        
        return True

    async def complete_workflow(self, environment_id: str, workflow_id: str):
        """完成工作流"""
        workflow_key = f"workflow:{environment_id}:{workflow_id}"
        workflow_data = await self.redis.get(workflow_key)
        
        if not workflow_data:
            logger.warning(f"Workflow not found when completing: {workflow_key}")
            return
        
        workflow = json.loads(workflow_data)
        
        completed_tasks = sum(1 for task in workflow.get('tasks', []) if task['status'] == 'completed')
        failed_tasks = sum(1 for task in workflow.get('tasks', []) if task['status'] == 'failed')
        total_tasks = len(workflow.get('tasks', []))
        
        workflow_status = 'completed' if failed_tasks == 0 else 'failed'
        
        # 更新Redis中的工作流状态
        workflow['status'] = workflow_status
        workflow['completed_at'] = datetime.now().isoformat()
        
        # 添加统计信息
        workflow['completion_stats'] = {
            'total_tasks': total_tasks,
            'completed_tasks': completed_tasks,
            'failed_tasks': failed_tasks
        }
        
        await self.redis.set(workflow_key, json.dumps(workflow))
        
        # 发送工作流完成事件
        completion_event = {
            'workflow_id': workflow_id,
            'environment_id': environment_id,
            'status': workflow_status,
            'total_tasks': total_tasks,
            'completed_tasks': completed_tasks,
            'failed_tasks': failed_tasks,
            'timestamp': datetime.now().isoformat()
        }
        
        await self.producer.send(config.TOPICS['WORKFLOWS_COMPLETED'], completion_event)
        
        logger.info(f"Workflow {environment_id}:{workflow_id} completed: {workflow_status} "
                f"({completed_tasks}/{total_tasks} tasks successful)")
        
        # 可选：清理过期的任务结果
        if workflow_status == 'completed':
            await self._cleanup_task_results(environment_id, workflow_id)
    
    async def _cleanup_task_results(self, environment_id: str, workflow_id: str):
        """清理任务结果（可选）"""
        try:
            # 根据配置决定是否立即清理结果
            cleanup_immediately = getattr(config, 'CLEANUP_RESULTS_IMMEDIATELY', False)
            
            if cleanup_immediately:
                pattern = f"result:{environment_id}:{workflow_id}#*"
                result_keys = await self.redis.keys(pattern)
                
                if result_keys:
                    await self.redis.delete(*result_keys)
                    logger.info(f"Cleaned up {len(result_keys)} task results for completed workflow {environment_id}:{workflow_id}")
        except Exception as e:
            logger.warning(f"Failed to cleanup task results: {e}")