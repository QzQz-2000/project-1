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
