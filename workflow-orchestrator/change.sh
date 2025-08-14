#!/bin/bash

echo "🔧 正在修复aioredis兼容性问题..."

# 停止当前运行的容器
echo "停止容器..."
docker-compose down

# 修复requirements.txt
echo "修复requirements.txt..."
cat > requirements.txt << 'EOF'
fastapi==0.104.1
uvicorn[standard]==0.24.0
aiokafka==0.8.10
redis==5.0.1
pandas==2.1.3
numpy==1.25.2
pydantic==2.5.0
aiohttp==3.9.1
EOF

# 修复orchestrator.py
echo "修复orchestrator.py..."
cat > orchestrator.py << 'EOF'
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
EOF

# 修复executor.py
echo "修复executor.py..."
cat > executor.py << 'EOF'
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
EOF

# 修复completer.py 
echo "修复completer.py..."
cat > completer.py << 'EOF'
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
EOF

# 修复api.py
echo "修复api.py..."
cat > api.py << 'EOF'
import json
import logging
from datetime import datetime
from fastapi import FastAPI, HTTPException
from aiokafka import AIOKafkaProducer
import redis.asyncio as redis
from models import WorkflowSubmissionRequest
from config import config

logger = logging.getLogger(__name__)

app = FastAPI(title="Simple Workflow Engine", version="1.0.0")

kafka_producer = None
redis_client = None

async def startup():
    """启动时初始化"""
    global kafka_producer, redis_client
    
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await kafka_producer.start()
    
    redis_client = redis.Redis(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        db=config.REDIS_DB,
        decode_responses=True
    )
    
    logger.info("API startup completed")

async def shutdown():
    """关闭时清理"""
    global kafka_producer, redis_client
    
    if kafka_producer:
        await kafka_producer.stop()
    if redis_client:
        await redis_client.close()
    
    logger.info("API shutdown completed")

app.add_event_handler("startup", startup)
app.add_event_handler("shutdown", shutdown)

@app.post("/workflows/submit")
async def submit_workflow(request: WorkflowSubmissionRequest):
    """提交工作流"""
    try:
        workflow_message = {
            'workflow': request.workflow.dict(),
            'submitted_by': request.submitted_by,
            'submitted_at': datetime.now().isoformat()
        }
        
        await kafka_producer.send(config.TOPICS['WORKFLOW_SUBMIT'], workflow_message)
        
        return {
            'message': 'Workflow submitted successfully',
            'submitted_at': workflow_message['submitted_at']
        }
        
    except Exception as e:
        logger.error(f"Error submitting workflow: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/workflows/{workflow_id}/status")
async def get_workflow_status(workflow_id: str):
    """获取工作流状态"""
    try:
        workflow_data = await redis_client.get(f"workflow:{workflow_id}")
        
        if not workflow_data:
            raise HTTPException(status_code=404, detail="Workflow not found")
        
        workflow = json.loads(workflow_data)
        
        task_stats = {}
        for task in workflow['tasks']:
            status = task['status']
            task_stats[status] = task_stats.get(status, 0) + 1
        
        return {
            'workflow_id': workflow_id,
            'name': workflow.get('name', 'Unnamed'),
            'status': workflow.get('status', 'unknown'),
            'created_at': workflow.get('created_at'),
            'total_tasks': len(workflow['tasks']),
            'task_statistics': task_stats,
            'tasks': workflow['tasks']
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting workflow status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """健康检查"""
    try:
        await redis_client.ping()
        
        return {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'services': {
                'redis': 'up',
                'kafka': 'up'
            }
        }
    except Exception as e:
        return {
            'status': 'unhealthy',
            'timestamp': datetime.now().isoformat(),
            'error': str(e)
        }

@app.get("/functions")
async def list_functions():
    """列出可用函数"""
    from function_registry import function_registry
    return {
        'functions': function_registry.list_functions()
    }
EOF

echo "✅ 修复完成！重新构建和启动..."

# 重新构建并启动
docker-compose up --build -d

echo "⏳ 等待服务启动..."
sleep 30

echo "🧪 运行测试..."
docker exec workflow-engine python test_workflow.py

echo "🎉 修复完成！"