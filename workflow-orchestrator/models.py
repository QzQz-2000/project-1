from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
from datetime import datetime, timezone
import uuid

class WorkflowStep(BaseModel):
    name: str
    task_type: str  # 'FUNCTION' or 'DATA'
    function_name: Optional[str] = None
    config: Dict[str, Any] = {}
    data_source_config: Optional[Dict[str, Any]] = None
    dependencies: List[str] = []

class WorkflowDefinition(BaseModel):
    name: str
    description: str = ""
    steps: List[WorkflowStep]

class WorkflowSubmissionRequest(BaseModel):
    workflow: WorkflowDefinition
    submitted_by: str = "user"

class TaskMessage(BaseModel):
    task_id: str
    workflow_id: str
    task_type: str
    config: Dict[str, Any] = {}
    function_name: Optional[str] = None
    data_source: Optional[Dict[str, Any]] = None
    timestamp: str

class CompletionMessage(BaseModel):
    task_id: str
    workflow_id: str
    status: str  # 'completed' or 'failed'
    result_key: Optional[str] = None
    error: Optional[str] = None
    timestamp: str