import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from typing import Dict, Any, Tuple, List
import logging

logger = logging.getLogger(__name__)

class BaseFunction(ABC):
    """基础函数类"""
    
    @abstractmethod
    def execute(self, data: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """执行函数，返回(结果数据, 元数据)"""
        pass

class MovingAverageFunction(BaseFunction):
    """移动平均函数"""
    
    def execute(self, data: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        field = config.get('field', 'value')
        window = config.get('window', 5)
        
        if field not in data.columns:
            raise ValueError(f"Column '{field}' not found in data")
        
        result = data.copy()
        result[f'{field}_ma{window}'] = data[field].rolling(window=window).mean()
        
        metadata = {
            'function': 'MovingAverage',
            'input_rows': len(data),
            'output_rows': len(result),
            'window': window,
            'field': field
        }
        
        logger.info(f"MovingAverage: processed {len(data)} rows, window={window}")
        return result, metadata

class FilterFunction(BaseFunction):
    """数据过滤函数"""
    
    def execute(self, data: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        field = config.get('field', 'value')
        threshold = config.get('threshold', 0)
        operator = config.get('operator', '>')  # '>', '<', '>=', '<=', '==', '!='
        
        if field not in data.columns:
            raise ValueError(f"Column '{field}' not found in data")
        
        if operator == '>':
            result = data[data[field] > threshold]
        elif operator == '<':
            result = data[data[field] < threshold]
        elif operator == '>=':
            result = data[data[field] >= threshold]
        elif operator == '<=':
            result = data[data[field] <= threshold]
        elif operator == '==':
            result = data[data[field] == threshold]
        elif operator == '!=':
            result = data[data[field] != threshold]
        else:
            raise ValueError(f"Unsupported operator: {operator}")
        
        metadata = {
            'function': 'Filter',
            'input_rows': len(data),
            'output_rows': len(result),
            'field': field,
            'threshold': threshold,
            'operator': operator
        }
        
        logger.info(f"Filter: {len(data)} -> {len(result)} rows, {field} {operator} {threshold}")
        return result, metadata

class FunctionRegistry:
    """函数注册表"""
    
    def __init__(self):
        self.functions = {
            'MovingAverage': MovingAverageFunction,
            'Filter': FilterFunction
        }
    
    def get_function(self, name: str) -> BaseFunction:
        """获取函数实例"""
        if name not in self.functions:
            raise ValueError(f"Function '{name}' not found")
        return self.functions[name]()
    
    def list_functions(self) -> List[str]:
        """列出所有可用函数"""
        return list(self.functions.keys())

# 全局注册表实例
function_registry = FunctionRegistry()