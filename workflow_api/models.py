from datetime import datetime
from enum import Enum
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from pydantic import BaseModel, Field

# ==================== 枚举定义 ====================

class MessageType(Enum):
    ORCHESTRATOR_TASK = "orchestrator_task"
    FUNCTION_TASK = "function_task"
    DATA_TASK = "data_task"

class MessageDirection(Enum):
    REQUEST = "request"
    RESPONSE = "response"

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class WorkflowStatus(Enum):
    SUBMITTED = "submitted"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class FunctionCategory(Enum):
    TRANSFORM = "transform"
    FILTER = "filter"
    AGGREGATION = "aggregation"
    ANALYSIS = "analysis"
    ALARM = "alarm"
    PREDICTION = "prediction"
    CONTROL = "control"
    IO = "io"

# ==================== API模型 (Pydantic) ====================

class WorkflowStep(BaseModel):
    """工作流步骤"""
    name: str = Field(..., description="步骤名称")
    function: str = Field(..., description="函数名称")
    config: Dict[str, Any] = Field(default_factory=dict, description="函数配置")
    dependencies: List[str] = Field(default_factory=list, description="依赖步骤")

class WorkflowDefinition(BaseModel):
    """工作流定义"""
    name: str = Field(..., description="工作流名称")
    description: str = Field(default="", description="工作流描述")
    steps: List[WorkflowStep] = Field(..., description="工作流步骤")
    variables: Dict[str, Any] = Field(default_factory=dict, description="全局变量")
    timeout: int = Field(default=3600, description="超时时间(秒)")

class WorkflowSubmissionRequest(BaseModel):
    """工作流提交请求"""
    workflow: WorkflowDefinition = Field(..., description="工作流定义")
    issuer: str = Field(default="anonymous", description="提交者ID")
    input_data: Optional[Dict[str, Any]] = Field(default=None, description="输入数据")
    #priority: int = Field(default=1, description="优先级(1-10)")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="元数据")

class WorkflowExecutionResponse(BaseModel):
    """工作流执行响应"""
    workflow_id: str = Field(..., description="工作流ID")
    execution_id: str = Field(..., description="执行ID")
    status: str = Field(..., description="状态")
    submitted_at: datetime = Field(..., description="提交时间")
    message: str = Field(..., description="响应消息")

class WorkflowStatusResponse(BaseModel):
    """工作流状态响应"""
    workflow_id: str
    execution_id: str
    status: str
    submitted_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    progress: Dict[str, Any] = Field(default_factory=dict)
    error_message: Optional[str] = None
    task_details: List[Dict[str, Any]] = Field(default_factory=list)

class FunctionInfo(BaseModel):
    """函数信息"""
    name: str
    description: str
    category: str
    version: str
    tags: List[str]
    config_schema: Dict[str, Any]


@dataclass
class BaseMessage:
    """基础消息结构"""
    message_type: MessageType
    direction: MessageDirection
    message_id: str
    timestamp: datetime = field(default_factory=datetime.now)
    issuer: str = "system"

@dataclass 
class OrchestratorTaskMessage(BaseMessage):
    """编排器任务消息"""
    workflow_definition: Dict[str, Any] = field(default_factory=dict)
    execution_id: Optional[str] = None
    result_data: Optional[Any] = None
    error_message: Optional[str] = None

@dataclass
class FunctionTaskMessage(BaseMessage):
    """函数任务消息"""
    function_name: str = ""
    agent_name: str = ""
    task_id: str = ""
    function_config: Dict[str, Any] = field(default_factory=dict)
    input_data_location: Optional[str] = None
    output_data_location: Optional[str] = None
    input_data: Optional[Any] = None
    output_data: Optional[Any] = None
    dependencies: List[str] = field(default_factory=list)
    dependents: List[str] = field(default_factory=list)
    status: TaskStatus = TaskStatus.PENDING
    error_message: Optional[str] = None


@dataclass
class FunctionMetadata:
    """函数元数据"""
    name: str
    description: str
    category: FunctionCategory
    version: str = "1.0.0"
    author: str = "system"
    tags: List[str] = field(default_factory=list)
    
@dataclass
class ValidationResult:
    """验证结果"""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)