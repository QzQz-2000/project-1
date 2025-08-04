import asyncio
import json
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional
from contextlib import asynccontextmanager

import redis
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# 使用统一的配置、模型和工具
from config import settings, setup_logging
from models import (
    WorkflowSubmissionRequest, WorkflowExecutionResponse, 
    WorkflowStatusResponse, FunctionInfo
)
from engine import MessageDrivenWorkflowEngine
from registry import REGISTRY
from utils import HealthChecker, KafkaManager, DataStoreManager

# 设置日志
logger = setup_logging()

class WorkflowMicroservice:
    """工作流引擎微服务主类"""
    
    def __init__(self):
        self.engine: Optional[MessageDrivenWorkflowEngine] = None
        self.health_checker: Optional[HealthChecker] = None
        self.running = False
        self.logger = logger.getChild(self.__class__.__name__)
        
    async def start(self):
        """启动微服务"""
        try:
            self.logger.info("正在启动工作流引擎微服务...")
            
            # 1. 初始化工作流引擎
            self.engine = MessageDrivenWorkflowEngine()
            
            # 2. 启动引擎（这会启动所有内部组件和Agent）
            await self.engine.start()
            
            # 3. 初始化健康检查器
            redis_client = redis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=settings.redis_db,
                password=settings.redis_password
            )
            kafka_manager = KafkaManager(settings.kafka_servers_list)
            self.health_checker = HealthChecker(redis_client, kafka_manager)
            
            self.running = True
            self.logger.info("工作流引擎微服务启动成功")
            
            # 4. 记录注册的函数
            functions = REGISTRY.get_all_metadata()
            self.logger.info(f"已注册函数数量: {len(functions)}")
            for func in functions:
                self.logger.info(f"  - {func['name']} ({func['category']})")
                
        except Exception as e:
            self.logger.error(f"启动微服务失败: {e}")
            raise
    
    async def stop(self):
        """停止微服务"""
        try:
            self.logger.info("正在停止工作流引擎微服务...")
            
            if self.engine:
                await self.engine.stop()
            
            self.running = False
            self.logger.info("工作流引擎微服务已停止")
            
        except Exception as e:
            self.logger.error(f"停止微服务失败: {e}")
    
    async def submit_workflow(self, request: WorkflowSubmissionRequest) -> WorkflowExecutionResponse:
        """提交工作流执行"""
        if not self.running or not self.engine:
            raise HTTPException(status_code=503, detail="工作流引擎未运行")
        
        try:
            workflow_id = str(uuid.uuid4())
            submitted_at = datetime.now()
            
            # 1. 验证工作流定义
            await self._validate_workflow(request.workflow)
            
            # 2. 存储工作流信息到Redis
            workflow_info = {
                "workflow_id": workflow_id,
                "status": "submitted",
                "submitted_at": submitted_at.isoformat(),
                "issuer": request.issuer,
                "workflow_definition": request.workflow.dict(),
                "input_data": request.input_data,
                "priority": request.priority,
                "metadata": request.metadata
            }
            
            # 使用engine的data_store
            self.engine.data_store.store_data(workflow_info, f"workflow_info:{workflow_id}", ttl=86400)
            
            # 3. 提交到工作流引擎
            engine_workflow_def = {
                "name": request.workflow.name,
                "description": request.workflow.description,
                "steps": [step.dict() for step in request.workflow.steps],
                "variables": request.workflow.variables,
                "input_data": request.input_data
            }
            
            execution_id = await self.engine.submit_workflow(
                workflow_definition=engine_workflow_def,
                issuer=request.issuer
            )
            
            # 4. 更新执行ID
            workflow_info["execution_id"] = execution_id
            workflow_info["status"] = "running"
            workflow_info["started_at"] = datetime.now().isoformat()
            
            self.engine.data_store.store_data(workflow_info, f"workflow_info:{workflow_id}", ttl=86400)
            
            self.logger.info(f"工作流已提交: {workflow_id} -> {execution_id}")
            
            return WorkflowExecutionResponse(
                workflow_id=workflow_id,
                execution_id=execution_id,
                status="running",
                submitted_at=submitted_at,
                message="工作流已成功提交并开始执行"
            )
            
        except Exception as e:
            self.logger.error(f"提交工作流失败: {e}")
            raise HTTPException(status_code=500, detail=f"提交工作流失败: {str(e)}")
    
    async def get_workflow_status(self, workflow_id: str) -> WorkflowStatusResponse:
        """获取工作流状态"""
        try:
            # 1. 获取基本工作流信息
            try:
                workflow_info = self.engine.data_store.retrieve_data(f"workflow_info:{workflow_id}")
            except ValueError:
                raise HTTPException(status_code=404, detail="工作流未找到")
            
            # 2. 获取执行进度
            execution_id = workflow_info.get("execution_id")
            progress = {}
            task_details = []
            
            if execution_id:
                try:
                    task_graph = self.engine.data_store.retrieve_data(f"graph_{execution_id}")
                    if task_graph:
                        # 计算进度
                        total_tasks = len(task_graph)
                        completed_tasks = sum(1 for task in task_graph.values() 
                                           if task.get('status') == 'completed')
                        failed_tasks = sum(1 for task in task_graph.values() 
                                        if task.get('status') == 'failed')
                        
                        progress = {
                            "total_tasks": total_tasks,
                            "completed_tasks": completed_tasks,
                            "failed_tasks": failed_tasks,
                            "progress_percentage": (completed_tasks / total_tasks) * 100 if total_tasks > 0 else 0
                        }
                        
                        # 任务详情
                        for task_id, task_info in task_graph.items():
                            task_details.append({
                                "task_id": task_id,
                                "function_name": task_info.get("function_name"),
                                "status": task_info.get("status"),
                                "dependencies": task_info.get("dependencies", []),
                                "config": task_info.get("config", {})
                            })
                            
                        # 更新工作流状态
                        if completed_tasks == total_tasks and failed_tasks == 0:
                            workflow_info["status"] = "completed"
                            workflow_info["completed_at"] = datetime.now().isoformat()
                        elif failed_tasks > 0:
                            workflow_info["status"] = "failed"
                            workflow_info["completed_at"] = datetime.now().isoformat()
                        
                        # 更新到存储
                        self.engine.data_store.store_data(workflow_info, f"workflow_info:{workflow_id}", ttl=86400)
                        
                except Exception as e:
                    self.logger.warning(f"获取任务图状态失败: {e}")
            
            return WorkflowStatusResponse(
                workflow_id=workflow_id,
                execution_id=workflow_info.get("execution_id", ""),
                status=workflow_info.get("status", "unknown"),
                submitted_at=datetime.fromisoformat(workflow_info["submitted_at"]),
                started_at=datetime.fromisoformat(workflow_info["started_at"]) if workflow_info.get("started_at") else None,
                completed_at=datetime.fromisoformat(workflow_info["completed_at"]) if workflow_info.get("completed_at") else None,
                progress=progress,
                error_message=workflow_info.get("error_message"),
                task_details=task_details
            )
            
        except HTTPException:
            raise
        except Exception as e:
            self.logger.error(f"获取工作流状态失败: {e}")
            raise HTTPException(status_code=500, detail=f"获取状态失败: {str(e)}")
    
    def get_available_functions(self) -> List[FunctionInfo]:
        """获取可用函数列表"""
        try:
            functions = REGISTRY.get_all_metadata()
            return [
                FunctionInfo(
                    name=func["name"],
                    description=func["description"],
                    category=func["category"],
                    version=func.get("version", "1.0.0"),
                    tags=func.get("tags", []),
                    config_schema=func.get("config_schema", {})
                )
                for func in functions
            ]
        except Exception as e:
            self.logger.error(f"获取函数列表失败: {e}")
            raise HTTPException(status_code=500, detail=f"获取函数列表失败: {str(e)}")
    
    async def _validate_workflow(self, workflow):
        """验证工作流定义"""
        # 1. 检查步骤是否为空
        if not workflow.steps:
            raise ValueError("工作流必须包含至少一个步骤")
        
        # 2. 检查函数是否都存在
        available_functions = set(func['name'] for func in REGISTRY.get_all_metadata())
        step_names = set()
        
        for step in workflow.steps:
            function_name = step.function
            if function_name not in available_functions:
                raise ValueError(f"未找到函数: {function_name}")
                
            if step.name in step_names:
                raise ValueError(f"重复的步骤名称: {step.name}")
            step_names.add(step.name)
        
        # 3. 检查依赖关系
        for step in workflow.steps:
            for dep in step.dependencies:
                if dep not in step_names:
                    raise ValueError(f"步骤 {step.name} 依赖的步骤 {dep} 不存在")

# ==================== FastAPI应用创建 ====================

# 全局微服务实例
workflow_service = WorkflowMicroservice()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时
    await workflow_service.start()
    yield
    # 关闭时
    await workflow_service.stop()

# 创建FastAPI应用
app = FastAPI(
    title="工作流引擎微服务",
    description="基于消息驱动的数据处理工作流引擎",
    version="1.0.0",
    lifespan=lifespan
)

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== API路由定义 ====================

@app.post("/api/v1/workflows/submit", response_model=WorkflowExecutionResponse)
async def submit_workflow(request: WorkflowSubmissionRequest):
    """提交工作流执行"""
    return await workflow_service.submit_workflow(request)

@app.get("/api/v1/workflows/{workflow_id}/status", response_model=WorkflowStatusResponse)
async def get_workflow_status(workflow_id: str):
    """获取工作流执行状态"""
    return await workflow_service.get_workflow_status(workflow_id)

@app.get("/api/v1/functions", response_model=List[FunctionInfo])
async def list_functions(
    category: Optional[str] = Query(None, description="按类别过滤"),
    search: Optional[str] = Query(None, description="搜索关键词")
):
    """获取可用函数列表"""
    functions = workflow_service.get_available_functions()
    
    # 过滤
    if category:
        functions = [f for f in functions if f.category == category]
    
    if search:
        search_lower = search.lower()
        functions = [
            f for f in functions 
            if search_lower in f.name.lower() or 
               search_lower in f.description.lower() or
               any(search_lower in tag.lower() for tag in f.tags)
        ]
    
    return functions

@app.get("/api/v1/functions/{function_name}")
async def get_function_details(function_name: str):
    """获取特定函数的详细信息"""
    function_class = REGISTRY.get(function_name)
    if not function_class:
        raise HTTPException(status_code=404, detail=f"函数 {function_name} 未找到")
    
    return {
        "name": function_class.metadata.name,
        "description": function_class.metadata.description,
        "category": function_class.metadata.category.value,
        "version": function_class.metadata.version,
        "tags": function_class.metadata.tags,
        "config_schema": function_class.config_schema,
        "supports_parallel": getattr(function_class, 'supports_parallel', False),
        "cache_enabled": getattr(function_class, 'cache_enabled', False)
    }

@app.get("/api/v1/system/health")
async def health_check():
    """健康检查"""
    if workflow_service.health_checker:
        return workflow_service.health_checker.get_health_status()
    else:
        return {
            "status": "unhealthy",
            "timestamp": datetime.utcnow().isoformat(),
            "error": "Health checker not initialized"
        }

@app.get("/api/v1/system/info")
async def system_info():
    """系统信息"""
    return {
        "service_name": settings.service_name,
        "version": "1.0.0",
        "debug": settings.debug,
        "registered_functions": len(REGISTRY.get_all_metadata()),
        "timestamp": datetime.utcnow().isoformat()
    }

# ==================== 启动配置 ====================

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.service_port,
        reload=settings.debug,
        log_level=settings.log_level.lower()
    )