import json
import logging
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from aiokafka import AIOKafkaProducer
import redis.asyncio as redis
from models import WorkflowSubmissionRequest
from config import config
import uuid
import pickle
import numpy as np
from fastapi.middleware.cors import CORSMiddleware

logger = logging.getLogger(__name__)

app = FastAPI(title="Simple Workflow Engine", version="1.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

@app.post("/environments/{environment_id}/workflows/submit")
async def submit_workflow(environment_id: str, request: WorkflowSubmissionRequest):
    """提交工作流到指定环境"""
    try:
        workflow_id = str(uuid.uuid4())
        workflow_message = {
            'workflow_id': workflow_id,
            'environment_id': environment_id,
            'workflow': request.workflow.dict(),
            'submitted_by': request.submitted_by,
            'submitted_at': datetime.now().isoformat()
        }
        
        await kafka_producer.send(config.TOPICS['WORKFLOW_SUBMIT'], workflow_message)
        
        return {
            'message': 'Workflow submitted successfully',
            'workflow_id': workflow_id,
            'environment_id': environment_id,
            'submitted_at': workflow_message['submitted_at']
        }
        
    except Exception as e:
        logger.error(f"Error submitting workflow: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/environments/{environment_id}/workflows/{workflow_id}/status")
async def get_workflow_status(environment_id: str, workflow_id: str):
    """获取指定环境下工作流状态"""
    try:
        workflow_key = f"workflow:{environment_id}:{workflow_id}"
        workflow_data = await redis_client.get(workflow_key)
        
        if not workflow_data:
            raise HTTPException(status_code=404, detail="Workflow not found")
        
        workflow = json.loads(workflow_data)
        
        # 验证工作流是否属于指定环境
        if workflow.get('environment_id') != environment_id:
            raise HTTPException(status_code=404, detail="Workflow not found in this environment")
        
        task_stats = {}
        for task in workflow['tasks']:
            status = task['status']
            task_stats[status] = task_stats.get(status, 0) + 1
        
        return {
            'workflow_id': workflow_id,
            'environment_id': environment_id,
            'name': workflow.get('name', 'Unnamed'),
            'status': workflow.get('status', 'unknown'),
            'created_at': workflow.get('created_at'),
            'completed_at': workflow.get('completed_at'),
            'total_tasks': len(workflow['tasks']),
            'task_statistics': task_stats,
            'tasks': workflow['tasks']
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting workflow status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/environments/{environment_id}/workflows")
async def list_workflows(environment_id: str):
    """列出指定环境下的所有工作流"""
    try:
        # 获取环境下所有工作流key
        pattern = f"workflow:{environment_id}:*"
        keys = await redis_client.keys(pattern)
        
        workflows = []
        for key in keys:
            workflow_data = await redis_client.get(key)
            if workflow_data:
                workflow = json.loads(workflow_data)
                workflows.append({
                    'workflow_id': workflow['workflow_id'],
                    'name': workflow.get('name', 'Unnamed'),
                    'status': workflow.get('status', 'unknown'),
                    'created_at': workflow.get('created_at'),
                    'completed_at': workflow.get('completed_at'),
                    'total_tasks': len(workflow.get('tasks', []))
                })
        
        return {'workflows': workflows}
        
    except Exception as e:
        logger.error(f"Error listing workflows: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/environments/{environment_id}/workflows/{workflow_id}")
async def delete_workflow(environment_id: str, workflow_id: str):
    """删除指定环境下的工作流"""
    try:
        workflow_key = f"workflow:{environment_id}:{workflow_id}"
        
        # 检查工作流是否存在
        if not await redis_client.exists(workflow_key):
            raise HTTPException(status_code=404, detail="Workflow not found")
        
        # 删除工作流数据
        await redis_client.delete(workflow_key)
        
        # 删除相关的任务结果
        pattern = f"result:{environment_id}:{workflow_id}#*"
        result_keys = await redis_client.keys(pattern)
        if result_keys:
            await redis_client.delete(*result_keys)
        
        return {'message': 'Workflow deleted successfully'}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting workflow: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 在 api.py 中新增端点

@app.get("/environments/{environment_id}/workflows/{workflow_id}/final-result")
async def get_workflow_final_result(environment_id: str, workflow_id: str):
    """获取工作流的最终结果（最后完成任务的结果）"""
    try:
        # 先获取工作流状态
        workflow_key = f"workflow:{environment_id}:{workflow_id}"
        workflow_data = await redis_client.get(workflow_key)
        
        if not workflow_data:
            raise HTTPException(status_code=404, detail="Workflow not found")
        
        workflow = json.loads(workflow_data)
        
        # 验证工作流是否属于指定环境
        if workflow.get('environment_id') != environment_id:
            raise HTTPException(status_code=404, detail="Workflow not found in this environment")
        
        # 找到最后一个完成的任务
        completed_tasks = [task for task in workflow['tasks'] if task['status'] == 'completed']
        
        if not completed_tasks:
            return {
                'message': 'No completed tasks found',
                'workflow_status': workflow.get('status', 'unknown'),
                'total_tasks': len(workflow['tasks']),
                'completed_tasks': 0
            }
        
        # 获取最后一个完成任务的结果（按创建时间或任务依赖顺序）
        # 这里简单地取最后一个，也可以根据依赖关系找真正的"最终"任务
        final_task = completed_tasks[-1]
        
        # 获取该任务的结果
        result_key = f"result:{final_task['task_id']}"
        
        redis_binary = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            decode_responses=False
        )
        
        serialized_data = await redis_binary.get(result_key)
        await redis_binary.close()
        
        if not serialized_data:
            return {
                'message': f'Result data not found for final task: {final_task["name"]}',
                'final_task_name': final_task['name'],
                'workflow_status': workflow.get('status', 'unknown')
            }
        
        result_df = pickle.loads(serialized_data)
        result_df = result_df.replace([np.inf, -np.inf], np.nan).fillna("N/A")
        
        return {
            'workflow_id': workflow_id,
            'workflow_status': workflow.get('status', 'unknown'),
            'final_task_name': final_task['name'],
            'final_task_status': final_task['status'],
            'result_summary': {
                'total_rows': len(result_df),
                'total_columns': len(result_df.columns),
                'columns': list(result_df.columns)
            },
            'data': result_df.to_dict("records")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting workflow final result: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

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
    try:
        from registry import REGISTRY as function_registry
        return {
            'functions': function_registry.list_all()
        }
    except Exception as e:
        logger.error(f"Error listing functions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 添加环境管理端点
@app.get("/environments")
async def list_environments():
    """列出所有环境"""
    try:
        pattern = "workflow:*"
        keys = await redis_client.keys(pattern)
        
        environments = set()
        for key in keys:
            # 从 workflow:environment_id:workflow_id 中提取环境ID
            parts = key.split(':')
            if len(parts) >= 3:
                environments.add(parts[1])
        
        return {'environments': sorted(list(environments))}
    except Exception as e:
        logger.error(f"Error listing environments: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/environments/{environment_id}/status")
async def get_environment_status(environment_id: str):
    """获取环境状态统计"""
    try:
        pattern = f"workflow:{environment_id}:*"
        keys = await redis_client.keys(pattern)
        
        total_workflows = len(keys)
        status_counts = {'running': 0, 'completed': 0, 'failed': 0, 'unknown': 0}
        
        for key in keys:
            workflow_data = await redis_client.get(key)
            if workflow_data:
                workflow = json.loads(workflow_data)
                status = workflow.get('status', 'unknown')
                status_counts[status] = status_counts.get(status, 0) + 1
        
        return {
            'environment_id': environment_id,
            'total_workflows': total_workflows,
            'workflow_status_counts': status_counts
        }
    except Exception as e:
        logger.error(f"Error getting environment status: {e}")
        raise HTTPException(status_code=500, detail=str(e))