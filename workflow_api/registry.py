import pandas as pd
import numpy as np
from scipy.signal import butter, filtfilt
from abc import ABC, abstractmethod
from typing import Dict, Any, Type, List, Optional, Union, Callable, Tuple
import joblib
from sklearn.linear_model import LinearRegression
import jsonschema
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import asyncio
from functools import lru_cache, wraps
import warnings
import yaml
import hashlib
import json
from enum import Enum
import inspect

# 导入统一的配置和模型
from config import settings, setup_logging
from models import FunctionCategory, FunctionMetadata, ValidationResult, ExecutionMode

# 设置日志
logger = setup_logging()

class BaseFunction(ABC):
    """增强版基础函数类"""
    # 类属性定义
    metadata: FunctionMetadata = None
    config_schema: Dict[str, Any] = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {},
        "additionalProperties": False
    }
    
    # 性能相关配置
    supports_parallel: bool = False
    chunk_size: int = 10000
    
    # 缓存配置
    cache_enabled: bool = False
    cache_ttl: int = 3600
    
    def __init__(self):
        """初始化函数实例"""
        self.logger = logger.getChild(self.__class__.__name__)
        self._cache = {}
        self._execution_stats = {
            "total_runs": 0,
            "total_errors": 0,
            "avg_execution_time": 0,
            "last_run": None
        }
        
        # 验证必需的元数据
        if not self.metadata:
            raise ValueError(f"Function {self.__class__.__name__} must define metadata")

    @abstractmethod
    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """执行核心逻辑"""
        pass

    def validate_input(self, data: pd.DataFrame, config: Dict[str, Any]) -> ValidationResult:
        """验证输入数据和配置"""
        result = ValidationResult(is_valid=True)
        
        # 验证数据
        if data.empty:
            result.warnings.append("Input DataFrame is empty")
        
        # 验证配置
        try:
            jsonschema.validate(instance=config, schema=self.config_schema)
        except jsonschema.ValidationError as e:
            result.is_valid = False
            error_path = ".".join(map(str, e.path)) if e.path else "root"
            result.errors.append(f"Config validation failed: {e.message} at '{error_path}'")
        
        return result

    def execute(self, data: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """执行函数的包装方法，包含验证、缓存、统计等"""
        import time
        start_time = time.time()
        
        # 更新执行统计
        self._execution_stats["total_runs"] += 1
        self._execution_stats["last_run"] = datetime.now()
        
        try:
            # 输入验证
            validation_result = self.validate_input(data, config)
            if not validation_result.is_valid:
                raise ValueError(f"Validation failed: {'; '.join(validation_result.errors)}")
            
            # 缓存检查
            if self.cache_enabled:
                cache_key = self._generate_cache_key(data, config)
                if cache_key in self._cache:
                    cached_result, cached_time = self._cache[cache_key]
                    if time.time() - cached_time < self.cache_ttl:
                        self.logger.debug(f"Returning cached result for {self.metadata.name}")
                        return cached_result, {"cached": True, "execution_time": 0}
            
            # 执行函数
            if self.supports_parallel and len(data) > self.chunk_size:
                result = self._run_parallel(data, config)
            else:
                result = self.run(data.copy(), config)
            
            # 更新缓存
            if self.cache_enabled:
                self._cache[cache_key] = (result, time.time())
            
            # 计算执行时间
            execution_time = time.time() - start_time
            self._update_avg_execution_time(execution_time)
            
            meta = {
                "execution_time": execution_time,
                "cached": False,
                "warnings": validation_result.warnings,
                "rows_processed": len(data),
                "rows_output": len(result)
            }
            
            return result, meta
            
        except Exception as e:
            self._execution_stats["total_errors"] += 1
            self.logger.error(f"Error executing {self.metadata.name}: {e}", exc_info=True)
            raise

    def _run_parallel(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """并行执行处理"""
        chunks = np.array_split(data, max(1, len(data) // self.chunk_size))
        
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.run, chunk.copy(), config) for chunk in chunks]
            results = [future.result() for future in futures]
        
        return pd.concat(results, ignore_index=True)

    def _generate_cache_key(self, data: pd.DataFrame, config: Dict[str, Any]) -> str:
        """生成缓存键"""
        # 使用数据的形状、列名和配置生成唯一键
        data_repr = f"{data.shape}_{list(data.columns)}_{data.index[0] if not data.empty else ''}"
        config_repr = json.dumps(config, sort_keys=True)
        return hashlib.md5(f"{data_repr}_{config_repr}".encode()).hexdigest()

    def _update_avg_execution_time(self, new_time: float):
        """更新平均执行时间"""
        n = self._execution_stats["total_runs"]
        old_avg = self._execution_stats["avg_execution_time"]
        self._execution_stats["avg_execution_time"] = (old_avg * (n - 1) + new_time) / n

    def get_stats(self) -> Dict[str, Any]:
        """获取执行统计信息"""
        return self._execution_stats.copy()

    # 辅助方法
    def _validate_columns(self, data: pd.DataFrame, required_columns: List[str]):
        """验证必需的列"""
        missing_cols = [col for col in required_columns if col not in data.columns]
        if missing_cols:
            raise ValueError(f"Function '{self.metadata.name}' requires columns: {', '.join(missing_cols)}.")

    def _validate_numeric_column(self, data: pd.DataFrame, field: str):
        """验证数值列"""
        if not pd.api.types.is_numeric_dtype(data[field]):
            raise TypeError(f"Function '{self.metadata.name}' requires numeric data in column '{field}'.")


class FunctionRegistry:
    """增强版函数注册表"""
    
    def __init__(self):
        self._functions: Dict[str, Type[BaseFunction]] = {}
        self._categories: Dict[FunctionCategory, List[str]] = {cat: [] for cat in FunctionCategory}
        self._tags: Dict[str, List[str]] = {}
        
    def register(self, cls: Type[BaseFunction]) -> Type[BaseFunction]:
        """注册函数"""
        if not issubclass(cls, BaseFunction):
            raise TypeError(f"Function class {cls.__name__} must inherit from BaseFunction.")
        
        if not cls.metadata:
            raise ValueError(f"Function class {cls.__name__} must define metadata.")
        
        name = cls.metadata.name
        if name in self._functions:
            raise ValueError(f"Function '{name}' already registered.")
        
        self._functions[name] = cls
        self._categories[cls.metadata.category].append(name)
        
        # 索引标签
        for tag in cls.metadata.tags:
            if tag not in self._tags:
                self._tags[tag] = []
            self._tags[tag].append(name)
        
        logger.debug(f"Registered function: {name} ({cls.__name__})")
        return cls
    
    def get(self, name: str) -> Optional[Type[BaseFunction]]:
        """获取函数类"""
        return self._functions.get(name)
    
    def get_by_category(self, category: FunctionCategory) -> List[str]:
        """按类别获取函数"""
        return self._categories.get(category, [])
    
    def get_by_tag(self, tag: str) -> List[str]:
        """按标签获取函数"""
        return self._tags.get(tag, [])
    
    def search(self, 
               query: str = None,
               category: FunctionCategory = None,
               tags: List[str] = None) -> List[Dict[str, Any]]:
        """搜索函数"""
        results = []
        
        for name, cls in self._functions.items():
            meta = cls.metadata
            
            # 类别过滤
            if category and meta.category != category:
                continue
            
            # 标签过滤
            if tags and not any(tag in meta.tags for tag in tags):
                continue
            
            # 关键词搜索
            if query:
                query_lower = query.lower()
                if not (query_lower in meta.name.lower() or 
                        query_lower in meta.description.lower() or
                        any(query_lower in tag.lower() for tag in meta.tags)):
                    continue
            
            results.append({
                "name": meta.name,
                "description": meta.description,
                "category": meta.category.value,
                "version": meta.version,
                "tags": meta.tags,
                "config_schema": cls.config_schema
            })
        
        return results
    
    def get_all_metadata(self) -> List[Dict[str, Any]]:
        """获取所有函数元数据"""
        return self.search()

# 全局注册表实例
REGISTRY = FunctionRegistry()

# 装饰器
def register_function(cls: Type[BaseFunction]) -> Type[BaseFunction]:
    """函数注册装饰器"""
    return REGISTRY.register(cls)

# --- Data Transformation Functions ---

@register_function
class MovingAverageFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="MovingAverage",
        description="计算滑动平均值",
        category=FunctionCategory.TRANSFORM,
        tags=["smoothing", "time-series", "statistics"],
        version="2.0.0"
    )
    
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "window": {"type": "integer", "default": 5, "minimum": 1, "description": "滑动窗口大小"},
            "field": {"type": "string", "default": "value", "description": "要应用滑动平均的列名"},
            "method": {"type": "string", "enum": ["simple", "exponential", "weighted"], "default": "simple"},
            "alpha": {"type": "number", "minimum": 0, "maximum": 1, "default": 0.3, "description": "指数平滑系数"},
            "weights": {"type": "array", "items": {"type": "number"}, "description": "加权平均的权重"},
            "center": {"type": "boolean", "default": False, "description": "是否居中对齐窗口"},
            "min_periods": {"type": "integer", "minimum": 1, "description": "最小观测数"},
            "drop_na_rows": {"type": "boolean", "default": True, "description": "是否删除NaN行"}
        },
        "required": ["field"],
        "additionalProperties": False
    }
    
    supports_parallel = True
    cache_enabled = True

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config["field"]
        window = config.get("window", 5)
        method = config.get("method", "simple")
        drop_na = config.get("drop_na_rows", True)
        
        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)
        
        if data[field].empty:
            self.logger.warning(f"Field '{field}' is empty. Returning original data.")
            return data.copy()
        
        result = data.copy()
        output_field = f"{field}_ma{window}"
        
        if method == "simple":
            center = config.get("center", False)
            min_periods = config.get("min_periods", 1)
            result[output_field] = data[field].rolling(
                window=window, 
                center=center,
                min_periods=min_periods
            ).mean()
            
        elif method == "exponential":
            alpha = config.get("alpha", 0.3)
            result[output_field] = data[field].ewm(alpha=alpha, adjust=False).mean()
            
        elif method == "weighted":
            weights = config.get("weights")
            if not weights:
                # 默认线性递增权重
                weights = np.arange(1, window + 1)
            else:
                weights = np.array(weights[:window])
            
            def weighted_average(series):
                return np.average(series, weights=weights)
            
            result[output_field] = data[field].rolling(window=window).apply(weighted_average, raw=True)
        
        if drop_na:
            original_rows = len(result)
            result = result.dropna(subset=[output_field])
            dropped_rows = original_rows - len(result)
            self.logger.info(f"Applied {method} moving average to '{field}'. Dropped {dropped_rows} NaN rows.")
        else:
            self.logger.info(f"Applied {method} moving average to '{field}'. NaN rows kept.")
        
        return result

@register_function
class FillNaFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="FillNA",
        description="填充缺失值",
        category=FunctionCategory.TRANSFORM,
        tags=["preprocessing", "missing-data", "cleaning"]
    )
    
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "method": {
                "type": "string", 
                "default": "ffill", 
                "enum": ["ffill", "bfill", "mean", "median", "mode", "value", "interpolate"], 
                "description": "填充方法"
            },
            "field": {"type": "string", "default": "value", "description": "要填充的列名"},
            "fill_value": {"description": "当method为'value'时的填充值"},
            "limit": {"type": "integer", "minimum": 1, "description": "连续填充的最大数量"},
            "interpolation_method": {
                "type": "string", 
                "enum": ["linear", "polynomial", "spline"],
                "default": "linear",
                "description": "插值方法"
            }
        },
        "required": ["field"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        method = config.get("method", "ffill")
        field = config.get("field", "value")
        fill_value = config.get("fill_value")
        limit = config.get("limit")

        self._validate_columns(data, [field])

        processed_data = data.copy()
        
        if processed_data[field].empty:
            self.logger.warning(f"Field '{field}' is empty. No NA to fill.")
            return processed_data

        if method == "mean":
            self._validate_numeric_column(processed_data, field)
            processed_data[field] = processed_data[field].fillna(processed_data[field].mean())
        elif method == "median":
            self._validate_numeric_column(processed_data, field)
            processed_data[field] = processed_data[field].fillna(processed_data[field].median())
        elif method == "mode":
            mode_value = processed_data[field].mode()
            if not mode_value.empty:
                processed_data[field] = processed_data[field].fillna(mode_value[0])
        elif method == "value":
            if fill_value is None:
                self.logger.warning(f"Method is 'value' but 'fill_value' not provided.")
            else:
                processed_data[field] = processed_data[field].fillna(fill_value)
        elif method == "interpolate":
            interp_method = config.get("interpolation_method", "linear")
            processed_data[field] = processed_data[field].interpolate(method=interp_method, limit=limit)
        else:  # ffill or bfill
            processed_data[field] = processed_data[field].fillna(method=method, limit=limit)
        
        self.logger.info(f"Filled NA in '{field}' using '{method}' method.")
        return processed_data

@register_function
class NormalizeFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="Normalize",
        description="数据归一化",
        category=FunctionCategory.TRANSFORM,
        tags=["preprocessing", "scaling", "normalization"]
    )
    
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "field": {"type": "string", "default": "value", "description": "要归一化的列名"},
            "method": {
                "type": "string",
                "enum": ["minmax", "zscore", "robust", "maxabs"],
                "default": "minmax",
                "description": "归一化方法"
            },
            "feature_range": {
                "type": "array",
                "items": {"type": "number"},
                "default": [0, 1],
                "description": "MinMax归一化的目标范围"
            },
            "output_field": {"type": "string", "description": "归一化后结果的新列名"}
        },
        "required": ["field"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        method = config.get("method", "minmax")
        output_field = config.get("output_field", f"{field}_norm")

        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)

        processed_data = data.copy()
        
        if method == "minmax":
            feature_range = config.get("feature_range", [0, 1])
            min_val = processed_data[field].min()
            max_val = processed_data[field].max()
            
            if max_val == min_val:
                self.logger.warning(f"Min and max values are the same for '{field}'. Setting to range minimum.")
                processed_data[output_field] = feature_range[0]
            else:
                processed_data[output_field] = (processed_data[field] - min_val) / (max_val - min_val)
                processed_data[output_field] = processed_data[output_field] * (feature_range[1] - feature_range[0]) + feature_range[0]
                
        elif method == "zscore":
            mean_val = processed_data[field].mean()
            std_val = processed_data[field].std()
            
            if std_val == 0:
                self.logger.warning(f"Standard deviation is zero for '{field}'. Setting normalized values to 0.")
                processed_data[output_field] = 0.0
            else:
                processed_data[output_field] = (processed_data[field] - mean_val) / std_val
                
        elif method == "robust":
            median_val = processed_data[field].median()
            q1 = processed_data[field].quantile(0.25)
            q3 = processed_data[field].quantile(0.75)
            iqr = q3 - q1
            
            if iqr == 0:
                self.logger.warning(f"IQR is zero for '{field}'. Setting normalized values to 0.")
                processed_data[output_field] = 0.0
            else:
                processed_data[output_field] = (processed_data[field] - median_val) / iqr
                
        elif method == "maxabs":
            max_abs = processed_data[field].abs().max()
            if max_abs == 0:
                self.logger.warning(f"Max absolute value is zero for '{field}'. Setting normalized values to 0.")
                processed_data[output_field] = 0.0
            else:
                processed_data[output_field] = processed_data[field] / max_abs
        
        self.logger.info(f"Normalized field '{field}' using '{method}' method to '{output_field}'.")
        return processed_data

@register_function
class DifferencingFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="Differencing",
        description="计算差分，常用于平稳化时间序列",
        category=FunctionCategory.TRANSFORM,
        tags=["time-series", "stationarity", "preprocessing"]
    )
    
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "field": {"type": "string", "default": "value", "description": "要计算差分的列名"},
            "periods": {"type": "integer", "default": 1, "description": "差分周期", "minimum": 1},
            "order": {"type": "integer", "default": 1, "description": "差分阶数", "minimum": 1},
            "output_field": {"type": "string", "description": "差分结果的新列名"},
            "drop_na_rows": {"type": "boolean", "default": True, "description": "是否删除NaN行"}
        },
        "required": ["field"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        periods = config.get("periods", 1)
        order = config.get("order", 1)
        output_field = config.get("output_field", f"{field}_diff{order}")
        drop_na_rows = config.get("drop_na_rows", True)

        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)

        processed_data = data.copy()
        
        # 执行多阶差分
        diff_result = processed_data[field]
        for _ in range(order):
            diff_result = diff_result.diff(periods=periods)
        
        processed_data[output_field] = diff_result
        
        if drop_na_rows:
            original_rows = len(processed_data)
            processed_data = processed_data.dropna(subset=[output_field])
            dropped_rows = original_rows - len(processed_data)
            self.logger.info(f"Applied {order}-order differencing with period {periods} to '{field}'. Dropped {dropped_rows} NaN rows.")
        else:
            self.logger.info(f"Applied {order}-order differencing with period {periods} to '{field}'. NaN rows kept.")
            
        return processed_data

# --- Statistical Functions ---

@register_function
class StdDeviationFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="StdDeviation",
        description="计算滑动标准差",
        category=FunctionCategory.ANALYSIS,
        tags=["statistics", "volatility", "time-series"]
    )
    
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "window": {"type": "integer", "default": 5, "description": "滑动窗口大小", "minimum": 1},
            "field": {"type": "string", "default": "value", "description": "要计算标准差的列名"},
            "output_field": {"type": "string", "description": "标准差结果的新列名"},
            "ddof": {"type": "integer", "default": 1, "description": "自由度调整", "minimum": 0}
        },
        "required": ["field"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        window = config.get("window", 5)
        field = config.get("field", "value")
        output_field = config.get("output_field", f"{field}_std{window}")
        ddof = config.get("ddof", 1)

        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)

        processed_data = data.copy()
        processed_data[output_field] = processed_data[field].rolling(window=window).std(ddof=ddof)
        
        self.logger.info(f"Calculated rolling standard deviation for '{field}' with window {window}.")
        return processed_data

# --- Anomaly Detection Functions ---

@register_function
class ZScoreFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="ZScore",
        description="Z-Score异常检测",
        category=FunctionCategory.ANALYSIS,
        tags=["anomaly-detection", "outlier", "statistics"]
    )
    
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "field": {"type": "string", "default": "value", "description": "要计算Z-Score的列名"},
            "threshold": {"type": "number", "default": 3.0, "description": "异常阈值", "minimum": 0},
            "window": {"type": "integer", "description": "滑动窗口大小（可选）", "minimum": 1},
            "zscore_output_field": {"type": "string", "default": "zscore", "description": "Z-Score结果列名"},
            "anomaly_output_field": {"type": "string", "default": "is_anomaly", "description": "异常标记结果列名"}
        },
        "required": ["field"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        threshold = config.get("threshold", 3.0)
        window = config.get("window")
        zscore_output_field = config.get("zscore_output_field", "zscore")
        anomaly_output_field = config.get("anomaly_output_field", "is_anomaly")

        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)

        processed_data = data.copy()
        
        if window:
            # 滑动窗口Z-Score
            rolling = processed_data[field].rolling(window=window)
            mean_val = rolling.mean()
            std_val = rolling.std()
        else:
            # 全局Z-Score
            mean_val = processed_data[field].mean()
            std_val = processed_data[field].std()

        if isinstance(std_val, pd.Series):
            processed_data[zscore_output_field] = np.where(
                std_val == 0, 0, (processed_data[field] - mean_val) / std_val
            )
        else:
            if std_val == 0:
                self.logger.warning(f"Standard deviation for '{field}' is zero. Z-scores will be zero.")
                processed_data[zscore_output_field] = 0.0
            else:
                processed_data[zscore_output_field] = (processed_data[field] - mean_val) / std_val
        
        processed_data[anomaly_output_field] = processed_data[zscore_output_field].abs() > threshold
        
        anomaly_count = processed_data[anomaly_output_field].sum()
        self.logger.info(f"Calculated Z-Scores for '{field}' (threshold={threshold}). Found {anomaly_count} anomalies.")
        return processed_data

@register_function
class IQRAnomalyFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="IQRAnomaly",
        description="基于四分位距(IQR)的异常检测",
        category=FunctionCategory.ANALYSIS,
        tags=["anomaly-detection", "outlier", "statistics", "robust"]
    )
    
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "field": {"type": "string", "default": "value", "description": "要检测的列名"},
            "k": {"type": "number", "default": 1.5, "description": "IQR倍数", "minimum": 0},
            "lower_percentile": {"type": "number", "default": 25, "description": "下四分位数", "minimum": 0, "maximum": 50},
            "upper_percentile": {"type": "number", "default": 75, "description": "上四分位数", "minimum": 50, "maximum": 100},
            "score_output_field": {"type": "string", "default": "iqr_score", "description": "异常分数列名"},
            "anomaly_output_field": {"type": "string", "default": "is_outlier", "description": "异常标记列名"}
        },
        "required": ["field"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        k = config.get("k", 1.5)
        lower_pct = config.get("lower_percentile", 25) / 100
        upper_pct = config.get("upper_percentile", 75) / 100
        score_field = config.get("score_output_field", "iqr_score")
        anomaly_field = config.get("anomaly_output_field", "is_outlier")

        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)

        processed_data = data.copy()
        
        Q1 = processed_data[field].quantile(lower_pct)
        Q3 = processed_data[field].quantile(upper_pct)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - k * IQR
        upper_bound = Q3 + k * IQR
        
        # 计算异常分数（距离边界的相对距离）
        processed_data[score_field] = np.maximum(
            (lower_bound - processed_data[field]) / IQR,
            (processed_data[field] - upper_bound) / IQR
        )
        processed_data[score_field] = processed_data[score_field].clip(lower=0)
        
        processed_data[anomaly_field] = (processed_data[field] < lower_bound) | (processed_data[field] > upper_bound)
        
        outlier_count = processed_data[anomaly_field].sum()
        self.logger.info(f"IQR anomaly detection on '{field}' found {outlier_count} outliers (k={k}).")
        
        return processed_data

# --- Alarm Functions ---

@register_function
class ThresholdAlarmFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="ThresholdAlarm",
        description="基于阈值的告警",
        category=FunctionCategory.ALARM,
        tags=["alarm", "monitoring", "threshold"]
    )
    
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "field": {"type": "string", "default": "value", "description": "要检查的列名"},
            "threshold": {"type": "number", "default": 80, "description": "阈值"},
            "mode": {
                "type": "string", 
                "default": "greater", 
                "enum": ["greater", "less", "equal", "not_equal", "greater_equal", "less_equal", "between", "not_between"],
                "description": "判断模式"
            },
            "lower_threshold": {"type": "number", "description": "下限阈值（用于between模式）"},
            "upper_threshold": {"type": "number", "description": "上限阈值（用于between模式）"},
            "alarm_output_field": {"type": "string", "default": "is_alarm", "description": "告警标记结果列名"},
            "severity_field": {"type": "string", "description": "告警严重程度列名"},
            "severity_levels": {
                "type": "object",
                "description": "告警严重程度配置",
                "properties": {
                    "warning": {"type": "number"},
                    "critical": {"type": "number"}
                }
            }
        },
        "required": ["field", "threshold", "mode"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: dict) -> pd.DataFrame:
        field = config.get("field", "value")
        threshold = config.get("threshold", 80)
        mode = config.get("mode", "greater")
        alarm_output_field = config.get("alarm_output_field", "is_alarm")
        severity_field = config.get("severity_field")
        severity_levels = config.get("severity_levels")

        self._validate_columns(data, [field])
        processed_data = data.copy()

        if not pd.api.types.is_numeric_dtype(processed_data[field]):
            self.logger.error(f"Field '{field}' is not numeric. Setting '{alarm_output_field}' to False.")
            processed_data[alarm_output_field] = False
            return processed_data

        # 基本阈值判断
        if mode == "greater":
            processed_data[alarm_output_field] = processed_data[field] > threshold
        elif mode == "less":
            processed_data[alarm_output_field] = processed_data[field] < threshold
        elif mode == "equal":
            processed_data[alarm_output_field] = processed_data[field] == threshold
        elif mode == "not_equal":
            processed_data[alarm_output_field] = processed_data[field] != threshold
        elif mode == "greater_equal":
            processed_data[alarm_output_field] = processed_data[field] >= threshold
        elif mode == "less_equal":
            processed_data[alarm_output_field] = processed_data[field] <= threshold
        elif mode == "between":
            lower = config.get("lower_threshold", threshold)
            upper = config.get("upper_threshold", threshold)
            processed_data[alarm_output_field] = processed_data[field].between(lower, upper)
        elif mode == "not_between":
            lower = config.get("lower_threshold", threshold)
            upper = config.get("upper_threshold", threshold)
            processed_data[alarm_output_field] = ~processed_data[field].between(lower, upper)
        
        # 告警严重程度
        if severity_field and severity_levels:
            processed_data[severity_field] = "normal"
            
            if "warning" in severity_levels:
                warning_threshold = severity_levels["warning"]
                if mode in ["greater", "greater_equal"]:
                    processed_data.loc[processed_data[field] > warning_threshold, severity_field] = "warning"
                elif mode in ["less", "less_equal"]:
                    processed_data.loc[processed_data[field] < warning_threshold, severity_field] = "warning"
            
            if "critical" in severity_levels:
                critical_threshold = severity_levels["critical"]
                if mode in ["greater", "greater_equal"]:
                    processed_data.loc[processed_data[field] > critical_threshold, severity_field] = "critical"
                elif mode in ["less", "less_equal"]:
                    processed_data.loc[processed_data[field] < critical_threshold, severity_field] = "critical"
        
        alarm_count = processed_data[alarm_output_field].sum()
        self.logger.info(f"Applied ThresholdAlarm to '{field}' (mode='{mode}'). Found {alarm_count} alarms.")
        return processed_data

@register_function
class RateOfChangeAlarmFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="RateOfChangeAlarm",
        description="基于变化速率的告警",
        category=FunctionCategory.ALARM,
        tags=["alarm", "monitoring", "rate-of-change", "derivative"]
    )
    
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "field": {"type": "string", "default": "value", "description": "计算速率的字段"},
            "rate_threshold": {"type": "number", "default": 10.0, "description": "速率阈值", "minimum": 0},
            "time_field": {"type": "string", "description": "时间列名（可选）"},
            "rate_type": {"type": "string", "enum": ["absolute", "percentage"], "default": "absolute", "description": "速率类型"},
            "window": {"type": "integer", "minimum": 1, "description": "计算速率的窗口大小"},
            "rate_output_field": {"type": "string", "default": "rate_of_change", "description": "变化速率列名"},
            "alarm_output_field": {"type": "string", "default": "rate_alarm", "description": "速率告警列名"}
        },
        "required": ["field"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: dict) -> pd.DataFrame:
        field = config.get("field", "value")
        rate_threshold = config.get("rate_threshold", 10.0)
        time_field = config.get("time_field")
        rate_type = config.get("rate_type", "absolute")
        window = config.get("window", 1)
        rate_output_field = config.get("rate_output_field", "rate_of_change")
        alarm_output_field = config.get("alarm_output_field", "rate_alarm")

        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)

        processed_data = data.copy()
        
        # 计算变化率
        if time_field:
            self._validate_columns(data, [time_field])
            # 基于时间的变化率
            if not pd.api.types.is_datetime64_any_dtype(processed_data[time_field]):
                processed_data[time_field] = pd.to_datetime(processed_data[time_field])
            
            time_diff = processed_data[time_field].diff(window).dt.total_seconds()
            value_diff = processed_data[field].diff(window)
            processed_data[rate_output_field] = value_diff / time_diff
        else:
            # 简单差分
            if rate_type == "absolute":
                processed_data[rate_output_field] = processed_data[field].diff(window)
            else:  # percentage
                processed_data[rate_output_field] = processed_data[field].pct_change(window) * 100
        
        # 告警判断
        processed_data[alarm_output_field] = processed_data[rate_output_field].abs() > rate_threshold
        
        alarm_count = processed_data[alarm_output_field].sum()
        self.logger.info(f"Applied RateOfChangeAlarm to '{field}' (threshold={rate_threshold}). Found {alarm_count} alarms.")
        return processed_data

@register_function
class CombinedAlarmFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="CombinedAlarm",
        description="组合多个条件的复合告警",
        category=FunctionCategory.ALARM,
        tags=["alarm", "monitoring", "composite", "logic"]
    )
    
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "conditions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "field": {"type": "string"},
                        "operator": {"type": "string", "enum": ["gt", "lt", "eq", "ne", "ge", "le"]},
                        "value": {"type": "number"}
                    },
                    "required": ["field", "operator", "value"]
                },
                "minItems": 1,
                "description": "告警条件列表"
            },
            "logic": {"type": "string", "enum": ["and", "or"], "default": "and", "description": "条件组合逻辑"},
            "alarm_output_field": {"type": "string", "default": "combined_alarm", "description": "组合告警标记列名"}
        },
        "required": ["conditions"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: dict) -> pd.DataFrame:
        conditions = config["conditions"]
        logic = config.get("logic", "and")
        alarm_output_field = config.get("alarm_output_field", "combined_alarm")

        processed_data = data.copy()
        condition_results = []

        for cond in conditions:
            field = cond["field"]
            operator = cond["operator"]
            value = cond["value"]
            
            self._validate_columns(data, [field])
            
            if operator == "gt":
                result = processed_data[field] > value
            elif operator == "lt":
                result = processed_data[field] < value
            elif operator == "eq":
                result = processed_data[field] == value
            elif operator == "ne":
                result = processed_data[field] != value
            elif operator == "ge":
                result = processed_data[field] >= value
            elif operator == "le":
                result = processed_data[field] <= value
            
            condition_results.append(result)
        
        # 组合条件
        if logic == "and":
            processed_data[alarm_output_field] = np.logical_and.reduce(condition_results)
        else:  # or
            processed_data[alarm_output_field] = np.logical_or.reduce(condition_results)
        
        alarm_count = processed_data[alarm_output_field].sum()
        self.logger.info(f"Applied CombinedAlarm with {len(conditions)} conditions (logic='{logic}'). Found {alarm_count} alarms.")
        return processed_data

# --- Aggregation Functions ---

@register_function
class AggregatorFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="Aggregator",
        description="按时间和分组聚合数据",
        category=FunctionCategory.AGGREGATION,
        tags=["aggregation", "time-series", "groupby", "resampling"]
    )
    
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "time_field": {"type": "string", "default": "time", "description": "时间列名"},
            "value_fields": {
                "type": "array", 
                "items": {"type": "string"},
                "description": "要聚合的数值列名列表"
            },
            "interval": {"type": "string", "default": "1min", "description": "聚合时间粒度"},
            "method": {
                "type": "string", 
                "default": "mean", 
                "enum": ["mean", "sum", "max", "min", "first", "last", "count", "median", "std", "var"],
                "description": "聚合方法"
            },
            "group_by_fields": {
                "type": "array", 
                "items": {"type": "string"}, 
                "description": "分组字段"
            },
            "fill_method": {
                "type": "string",
                "enum": ["none", "ffill", "bfill", "linear"],
                "default": "none",
                "description": "填充方法"
            }
        },
        "required": ["time_field", "interval", "method"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: dict) -> pd.DataFrame:
        time_field = config.get("time_field", "time")
        value_fields = config.get("value_fields")
        interval = config.get("interval", "1min")
        method = config.get("method", "mean")
        group_by_fields = config.get("group_by_fields", [])
        fill_method = config.get("fill_method", "none")

        # 验证列
        required_cols = [time_field] + (value_fields or []) + group_by_fields
        self._validate_columns(data, required_cols)

        processed_data = data.copy()
        
        if processed_data.empty:
            self.logger.warning("Input DataFrame is empty. Returning empty DataFrame.")
            return pd.DataFrame()

        # 转换时间列
        if not pd.api.types.is_datetime64_any_dtype(processed_data[time_field]):
            try:
                processed_data[time_field] = pd.to_datetime(processed_data[time_field])
            except Exception as e:
                self.logger.error(f"Failed to convert '{time_field}' to datetime: {e}")
                return data.copy()

        processed_data.set_index(time_field, inplace=True)
        
        # 确定要聚合的列
        if not value_fields:
            # 如果未指定，使用所有数值列
            value_fields = processed_data.select_dtypes(include=[np.number]).columns.tolist()
            if group_by_fields:
                value_fields = [f for f in value_fields if f not in group_by_fields]

        if group_by_fields:
            grouped = processed_data.groupby(group_by_fields).resample(interval)[value_fields]
        else:
            grouped = processed_data.resample(interval)[value_fields]

        # 执行聚合
        if hasattr(grouped, method):
            aggregated_data = getattr(grouped, method)()
        else:
            self.logger.error(f"Aggregation method '{method}' not supported.")
            return pd.DataFrame()
        
        # 重置索引
        aggregated_data = aggregated_data.reset_index()
        
        # 填充缺失值
        if fill_method != "none" and fill_method:
            if fill_method == "linear":
                for field in value_fields:
                    aggregated_data[field] = aggregated_data[field].interpolate(method="linear")
            else:
                aggregated_data[value_fields] = aggregated_data[value_fields].fillna(method=fill_method)
        
        self.logger.info(f"Aggregated data by '{interval}' using '{method}' method. Result has {len(aggregated_data)} rows.")
        return aggregated_data

# --- I/O Functions ---

@register_function
class CSVExporterFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="CSVExporter",
        description="导出数据到CSV文件",
        category=FunctionCategory.IO,
        tags=["export", "csv", "file", "output"]
    )
    
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "file_path": {"type": "string", "default": "./output.csv", "description": "输出文件路径"},
            "index": {"type": "boolean", "default": False, "description": "是否写入索引"},
            "encoding": {"type": "string", "default": "utf-8", "description": "文件编码"},
            "sep": {"type": "string", "default": ",", "description": "分隔符"},
            "columns": {"type": "array", "items": {"type": "string"}, "description": "要导出的列"},
            "append": {"type": "boolean", "default": False, "description": "是否追加到文件"},
            "header": {"type": "boolean", "default": True, "description": "是否写入列名"},
            "raise_on_error": {"type": "boolean", "default": False, "description": "失败时是否抛出异常"}
        },
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        file_path = config.get("file_path", "./output.csv")
        include_index = config.get("index", False)
        encoding = config.get("encoding", "utf-8")
        sep = config.get("sep", ",")
        columns = config.get("columns")
        append = config.get("append", False)
        header = config.get("header", True)
        raise_on_error = config.get("raise_on_error", False)

        # 选择要导出的列
        export_data = data[columns] if columns else data

        try:
            mode = 'a' if append else 'w'
            header = header and not append  # 追加模式下不写入列名
            
            export_data.to_csv(
                file_path, 
                index=include_index, 
                encoding=encoding,
                sep=sep,
                mode=mode,
                header=header
            )
            self.logger.info(f"Data successfully exported to {file_path} ({len(export_data)} rows)")
        except Exception as e:
            self.logger.error(f"Failed to export data to CSV: {e}", exc_info=True)
            if raise_on_error:
                raise
                
        return data  # 返回原始数据

# --- Signal Processing Functions ---

@register_function
class FourierTransformFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="FourierTransform",
        description="快速傅里叶变换分析频率成分",
        category=FunctionCategory.ANALYSIS,
        tags=["signal-processing", "frequency", "fft", "spectral"]
    )
    
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "field": {"type": "string", "default": "value", "description": "要进行FFT的列名"},
            "sampling_rate": {"type": "number", "description": "采样率(Hz)"},
            "output_type": {
                "type": "string",
                "enum": ["full", "magnitude", "phase", "power"],
                "default": "magnitude",
                "description": "输出类型"
            },
            "normalize": {"type": "boolean", "default": True, "description": "是否归一化"},
            "return_dataframe": {"type": "boolean", "default": True, "description": "是否返回频率分析结果"}
        },
        "required": ["field"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        sampling_rate = config.get("sampling_rate", 1.0)
        output_type = config.get("output_type", "magnitude")
        normalize = config.get("normalize", True)
        return_df = config.get("return_dataframe", True)

        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)

        y = data[field].dropna().values
        if len(y) == 0:
            self.logger.warning(f"No valid data in field '{field}' for FFT.")
            return pd.DataFrame()

        # 执行FFT
        fft_result = np.fft.fft(y)
        frequencies = np.fft.fftfreq(len(y), 1/sampling_rate)
        
        # 只保留正频率部分
        positive_freq_idx = frequencies > 0
        frequencies = frequencies[positive_freq_idx]
        fft_result = fft_result[positive_freq_idx]
        
        # 计算输出
        if output_type == "magnitude":
            values = np.abs(fft_result)
        elif output_type == "phase":
            values = np.angle(fft_result)
        elif output_type == "power":
            values = np.abs(fft_result) ** 2
        else:  # full
            values = fft_result
        
        # 归一化
        if normalize and output_type in ["magnitude", "power"]:
            values = values / np.max(values)
        
        if return_df:
            result_df = pd.DataFrame({
                "frequency": frequencies,
                f"{field}_fft_{output_type}": values
            })
            self.logger.info(f"Applied FFT to '{field}'. Result has {len(result_df)} frequency components.")
            return result_df
        else:
            # 添加频率分析结果到原数据
            processed_data = data.copy()
            # 存储主频率和幅值
            dominant_freq_idx = np.argmax(np.abs(values))
            processed_data[f"{field}_dominant_frequency"] = frequencies[dominant_freq_idx]
            processed_data[f"{field}_dominant_amplitude"] = values[dominant_freq_idx]
            return processed_data

@register_function
class LowPassFilterFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="LowPassFilter",
        description="低通滤波器去除高频噪声",
        category=FunctionCategory.FILTER,
        tags=["signal-processing", "filter", "denoising", "smoothing"]
    )
    
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "field": {"type": "string", "default": "value", "description": "要滤波的列名"},
            "cutoff": {"type": "number", "default": 0.1, "description": "截止频率", "minimum": 0.0, "maximum": 0.5},
            "order": {"type": "integer", "default": 4, "description": "滤波器阶数", "minimum": 1},
            "filter_type": {
                "type": "string",
                "enum": ["butterworth", "chebyshev", "elliptic"],
                "default": "butterworth",
                "description": "滤波器类型"
            },
            "output_field": {"type": "string", "description": "滤波结果列名"}
        },
        "required": ["field", "cutoff"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        cutoff = config.get("cutoff", 0.1)
        order = config.get("order", 4)
        filter_type = config.get("filter_type", "butterworth")
        output_field = config.get("output_field", f"{field}_filtered")

        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)
        
        processed_data = data.copy()

        if len(processed_data) < order + 1:
            self.logger.warning(f"Not enough data points for filter order {order}. Skipping filtering.")
            processed_data[output_field] = processed_data[field]
            return processed_data

        try:
            # 设计滤波器
            if filter_type == "butterworth":
                b, a = butter(order, cutoff, btype='low', analog=False)
            else:
                # 其他滤波器类型需要额外参数，这里简化为Butterworth
                self.logger.warning(f"Filter type '{filter_type}' not fully implemented. Using Butterworth.")
                b, a = butter(order, cutoff, btype='low', analog=False)
            
            # 应用滤波器
            processed_data[output_field] = filtfilt(b, a, processed_data[field].values)
            
            self.logger.info(f"Applied {filter_type} low-pass filter to '{field}' (cutoff={cutoff}, order={order}).")
        except Exception as e:
            self.logger.error(f"Error applying filter: {e}")
            processed_data[output_field] = processed_data[field]
            
        return processed_data

@register_function
class BandPassFilterFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="BandPassFilter",
        description="带通滤波器保留特定频率范围",
        category=FunctionCategory.FILTER,
        tags=["signal-processing", "filter", "frequency-selection"]
    )
    
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "field": {"type": "string", "default": "value", "description": "要滤波的列名"},
            "lowcut": {"type": "number", "default": 0.1, "description": "低截止频率", "minimum": 0.0},
            "highcut": {"type": "number", "default": 0.3, "description": "高截止频率", "minimum": 0.0, "maximum": 0.5},
            "order": {"type": "integer", "default": 4, "description": "滤波器阶数", "minimum": 1},
            "output_field": {"type": "string", "description": "滤波结果列名"}
        },
        "required": ["field", "lowcut", "highcut"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        lowcut = config.get("lowcut", 0.1)
        highcut = config.get("highcut", 0.3)
        order = config.get("order", 4)
        output_field = config.get("output_field", f"{field}_bandpass")

        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)
        
        processed_data = data.copy()

        if len(processed_data) < order + 1:
            self.logger.warning(f"Not enough data points for filter order {order}. Skipping filtering.")
            processed_data[output_field] = processed_data[field]
            return processed_data

        if lowcut >= highcut:
            self.logger.error(f"Low cutoff ({lowcut}) must be less than high cutoff ({highcut}).")
            processed_data[output_field] = processed_data[field]
            return processed_data

        try:
            # 设计带通滤波器
            b, a = butter(order, [lowcut, highcut], btype='band', analog=False)
            
            # 应用滤波器
            processed_data[output_field] = filtfilt(b, a, processed_data[field].values)
            
            self.logger.info(f"Applied band-pass filter to '{field}' (lowcut={lowcut}, highcut={highcut}, order={order}).")
        except Exception as e:
            self.logger.error(f"Error applying band-pass filter: {e}")
            processed_data[output_field] = processed_data[field]
            
        return processed_data