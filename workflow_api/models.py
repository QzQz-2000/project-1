from typing import Dict, Any, List
from pydantic import BaseModel

class FunctionRequest(BaseModel):
    function_name: str
    config: Dict[str, Any]
    data: List[Dict[str, Any]]  # 等价于 pandas DataFrame
