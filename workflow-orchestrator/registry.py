import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from typing import Dict, Any, Type, List, Optional
from enum import Enum
import logging

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FunctionCategory(Enum):
    """函数分类"""
    TRANSFORM = "transform"      # 数据变换
    ANALYSIS = "analysis"        # 数据分析
    FILTER = "filter"           # 数据过滤
    ALARM = "alarm"             # 告警
    AGGREGATION = "aggregation" # 聚合
    IO = "io"                   # 输入输出

class FunctionMetadata:
    """函数元数据"""
    def __init__(self, name: str, description: str, category: FunctionCategory, 
                 tags: List[str] = None, version: str = "1.0.0"):
        self.name = name
        self.description = description
        self.category = category
        self.tags = tags or []
        self.version = version

class BaseFunction(ABC):
    """简化版基础函数类"""
    metadata: FunctionMetadata = None
    
    def __init__(self):
        if not self.metadata:
            raise ValueError(f"Function {self.__class__.__name__} must define metadata")
        self.logger = logger.getChild(self.__class__.__name__)

    @abstractmethod
    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """执行核心逻辑"""
        pass

    def validate_input(self, data: pd.DataFrame, config: Dict[str, Any]) -> bool:
        """简单的输入验证"""
        if data.empty:
            self.logger.warning("Input DataFrame is empty")
        return True

    def execute(self, data: pd.DataFrame, config: Dict[str, Any]) -> (pd.DataFrame, Dict[str, Any]):
        """执行函数，返回数据和元数据"""
        try:
            # 简单验证
            self.validate_input(data, config)
            
            # 执行函数
            result = self.run(data.copy(), config)
            
            metadata = {
                "status": "success",
                "message": f"Successfully executed {self.metadata.name}"
            }
            self.logger.info(metadata["message"])
            return result, metadata
            
        except Exception as e:
            self.logger.error(f"Error executing {self.metadata.name}: {e}")
            raise

    def _validate_columns(self, data: pd.DataFrame, required_columns: List[str]):
        """验证必需的列"""
        missing_cols = [col for col in required_columns if col not in data.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")

    def _validate_numeric_column(self, data: pd.DataFrame, field: str):
        """验证数值列"""
        if not pd.api.types.is_numeric_dtype(data[field]):
            raise TypeError(f"Column '{field}' must be numeric")

class FunctionRegistry:
    """简化版函数注册表"""
    
    def __init__(self):
        self._functions: Dict[str, Type[BaseFunction]] = {}
        self._categories: Dict[FunctionCategory, List[str]] = {cat: [] for cat in FunctionCategory}
        
    def register(self, cls: Type[BaseFunction]) -> Type[BaseFunction]:
        """注册函数"""
        if not issubclass(cls, BaseFunction):
            raise TypeError(f"Function class {cls.__name__} must inherit from BaseFunction")
        
        if not cls.metadata:
            raise ValueError(f"Function class {cls.__name__} must define metadata")
        
        name = cls.metadata.name
        if name in self._functions:
            raise ValueError(f"Function '{name}' already registered")
        
        self._functions[name] = cls
        self._categories[cls.metadata.category].append(name)
        
        logger.info(f"Registered function: {name}")
        return cls
    
    def get(self, name: str) -> Optional[Type[BaseFunction]]:
        """获取函数类"""
        return self._functions.get(name)
    
    def create_instance(self, name: str) -> Optional[BaseFunction]:
        """创建函数实例"""
        cls = self.get(name)
        return cls() if cls else None
    
    def execute(self, name: str, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """执行指定函数"""
        instance = self.create_instance(name)
        if not instance:
            raise ValueError(f"Function '{name}' not found")
        return instance.execute(data, config)
    
    def get_by_category(self, category: FunctionCategory) -> List[str]:
        """按类别获取函数"""
        return self._categories.get(category, [])
    
    def list_all(self) -> List[Dict[str, Any]]:
        """列出所有函数"""
        result = []
        for name, cls in self._functions.items():
            meta = cls.metadata
            result.append({
                "name": meta.name,
                "description": meta.description,
                "category": meta.category.value,
                "tags": meta.tags,
                "version": meta.version
            })
        return result

# 全局注册表实例
REGISTRY = FunctionRegistry()

# 装饰器
def register_function(cls: Type[BaseFunction]) -> Type[BaseFunction]:
    """函数注册装饰器"""
    return REGISTRY.register(cls)

# 数据变换函数
@register_function
class MovingAverageFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="MovingAverage",
        description="计算滑动平均值",
        category=FunctionCategory.TRANSFORM,
        tags=["smoothing", "time-series", "statistics"]
    )

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        window = config.get("window", 5)
        method = config.get("method", "simple")  # simple, exponential, weighted
        
        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)
        
        result = data.copy()
        output_field = f"{field}_ma{window}"
        
        if method == "simple":
            result[output_field] = data[field].rolling(window=window).mean()
        elif method == "exponential":
            alpha = config.get("alpha", 0.3)
            result[output_field] = data[field].ewm(alpha=alpha).mean()
        elif method == "weighted":
            weights = config.get("weights", list(range(1, window + 1)))
            result[output_field] = data[field].rolling(window=window).apply(
                lambda x: np.average(x, weights=weights), raw=True
            )
        
        return result

@register_function
class FillNaFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="FillNA",
        description="填充缺失值",
        category=FunctionCategory.TRANSFORM,
        tags=["preprocessing", "missing-data"]
    )

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        method = config.get("method", "ffill")  # ffill, bfill, mean, median, mode, value, interpolate
        fill_value = config.get("fill_value")
        
        self._validate_columns(data, [field])
        
        result = data.copy()
        
        if method == "mean":
            self._validate_numeric_column(result, field)
            result[field] = result[field].fillna(result[field].mean())
        elif method == "median":
            self._validate_numeric_column(result, field)
            result[field] = result[field].fillna(result[field].median())
        elif method == "mode":
            mode_value = result[field].mode()
            if not mode_value.empty:
                result[field] = result[field].fillna(mode_value[0])
        elif method == "value" and fill_value is not None:
            result[field] = result[field].fillna(fill_value)
        elif method == "interpolate":
            result[field] = result[field].interpolate()
        else:  # ffill or bfill
            result[field] = result[field].fillna(method=method)
        
        return result

@register_function
class NormalizeFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="Normalize",
        description="数据归一化",
        category=FunctionCategory.TRANSFORM,
        tags=["preprocessing", "scaling"]
    )

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        method = config.get("method", "minmax")  # minmax, zscore, robust, maxabs
        output_field = config.get("output_field", f"{field}_norm")
        
        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)
        
        result = data.copy()
        
        if method == "minmax":
            feature_range = config.get("feature_range", [0, 1])
            min_val = result[field].min()
            max_val = result[field].max()
            if max_val != min_val:
                result[output_field] = (result[field] - min_val) / (max_val - min_val)
                result[output_field] = result[output_field] * (feature_range[1] - feature_range[0]) + feature_range[0]
            else:
                result[output_field] = feature_range[0]
        elif method == "zscore":
            mean_val = result[field].mean()
            std_val = result[field].std()
            if std_val != 0:
                result[output_field] = (result[field] - mean_val) / std_val
            else:
                result[output_field] = 0.0
        elif method == "robust":
            median_val = result[field].median()
            q1 = result[field].quantile(0.25)
            q3 = result[field].quantile(0.75)
            iqr = q3 - q1
            if iqr != 0:
                result[output_field] = (result[field] - median_val) / iqr
            else:
                result[output_field] = 0.0
        elif method == "maxabs":
            max_abs = result[field].abs().max()
            if max_abs != 0:
                result[output_field] = result[field] / max_abs
            else:
                result[output_field] = 0.0
        
        return result

# 统计分析函数
@register_function
class StdDeviationFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="StdDeviation",
        description="计算滑动标准差",
        category=FunctionCategory.ANALYSIS,
        tags=["statistics", "volatility", "time-series"]
    )

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        window = config.get("window", 5)
        output_field = config.get("output_field", f"{field}_std{window}")
        
        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)
        
        result = data.copy()
        result[output_field] = result[field].rolling(window=window).std()
        
        return result

@register_function
class ZScoreFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="ZScore",
        description="Z-Score异常检测",
        category=FunctionCategory.ANALYSIS,
        tags=["anomaly-detection", "outlier", "statistics"]
    )

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        threshold = config.get("threshold", 3.0)
        window = config.get("window")  # 可选滑动窗口
        zscore_field = config.get("zscore_output_field", "zscore")
        anomaly_field = config.get("anomaly_output_field", "is_anomaly")
        
        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)
        
        result = data.copy()
        
        if window:
            # 滑动窗口Z-Score
            rolling = result[field].rolling(window=window)
            mean_val = rolling.mean()
            std_val = rolling.std()
            result[zscore_field] = (result[field] - mean_val) / std_val
        else:
            # 全局Z-Score
            mean_val = result[field].mean()
            std_val = result[field].std()
            if std_val != 0:
                result[zscore_field] = (result[field] - mean_val) / std_val
            else:
                result[zscore_field] = 0.0
        
        result[anomaly_field] = result[zscore_field].abs() > threshold
        
        return result

@register_function
class IQRAnomalyFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="IQRAnomaly",
        description="基于四分位距(IQR)的异常检测",
        category=FunctionCategory.ANALYSIS,
        tags=["anomaly-detection", "outlier", "statistics", "robust"]
    )

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        k = config.get("k", 1.5)
        score_field = config.get("score_output_field", "iqr_score")
        anomaly_field = config.get("anomaly_output_field", "is_outlier")
        
        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)
        
        result = data.copy()
        
        Q1 = result[field].quantile(0.25)
        Q3 = result[field].quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - k * IQR
        upper_bound = Q3 + k * IQR
        
        # 计算异常分数
        result[score_field] = np.maximum(
            (lower_bound - result[field]) / IQR,
            (result[field] - upper_bound) / IQR
        )
        result[score_field] = result[score_field].clip(lower=0)
        
        result[anomaly_field] = (result[field] < lower_bound) | (result[field] > upper_bound)
        
        return result

# 告警函数
@register_function
class ThresholdAlarmFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="ThresholdAlarm",
        description="基于阈值的告警",
        category=FunctionCategory.ALARM,
        tags=["alarm", "monitoring", "threshold"]
    )

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        threshold = config.get("threshold", 80)
        mode = config.get("mode", "greater")  # greater, less, equal, between等
        alarm_field = config.get("alarm_output_field", "is_alarm")
        
        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)
        
        result = data.copy()
        
        if mode == "greater":
            result[alarm_field] = result[field] > threshold
        elif mode == "less":
            result[alarm_field] = result[field] < threshold
        elif mode == "equal":
            result[alarm_field] = result[field] == threshold
        elif mode == "greater_equal":
            result[alarm_field] = result[field] >= threshold
        elif mode == "less_equal":
            result[alarm_field] = result[field] <= threshold
        elif mode == "between":
            lower = config.get("lower_threshold", threshold)
            upper = config.get("upper_threshold", threshold)
            result[alarm_field] = result[field].between(lower, upper)
        
        return result

@register_function
class RateOfChangeAlarmFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="RateOfChangeAlarm",
        description="基于变化速率的告警",
        category=FunctionCategory.ALARM,
        tags=["alarm", "monitoring", "rate-of-change"]
    )

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        rate_threshold = config.get("rate_threshold", 10.0)
        rate_type = config.get("rate_type", "absolute")  # absolute, percentage
        window = config.get("window", 1)
        rate_field = config.get("rate_output_field", "rate_of_change")
        alarm_field = config.get("alarm_output_field", "rate_alarm")
        
        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)
        
        result = data.copy()
        
        if rate_type == "absolute":
            result[rate_field] = result[field].diff(window)
        else:  # percentage
            result[rate_field] = result[field].pct_change(window) * 100
        
        result[alarm_field] = result[rate_field].abs() > rate_threshold
        
        return result

@register_function
class CombinedAlarmFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="CombinedAlarm",
        description="组合多个条件的复合告警",
        category=FunctionCategory.ALARM,
        tags=["alarm", "monitoring", "composite", "logic"]
    )

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        conditions = config["conditions"]  # 必须提供条件列表
        logic = config.get("logic", "and")  # and, or
        alarm_field = config.get("alarm_output_field", "combined_alarm")
        
        result = data.copy()
        condition_results = []
        
        for cond in conditions:
            field = cond["field"]
            operator = cond["operator"]  # gt, lt, eq, ne, ge, le
            value = cond["value"]
            
            self._validate_columns(data, [field])
            
            if operator == "gt":
                cond_result = result[field] > value
            elif operator == "lt":
                cond_result = result[field] < value
            elif operator == "eq":
                cond_result = result[field] == value
            elif operator == "ne":
                cond_result = result[field] != value
            elif operator == "ge":
                cond_result = result[field] >= value
            elif operator == "le":
                cond_result = result[field] <= value
            else:
                raise ValueError(f"Unsupported operator: {operator}")
            
            condition_results.append(cond_result)
        
        # 组合条件
        if logic == "and":
            result[alarm_field] = np.logical_and.reduce(condition_results)
        else:  # or
            result[alarm_field] = np.logical_or.reduce(condition_results)
        
        return result

# 聚合函数
@register_function
class AggregatorFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="Aggregator",
        description="按时间聚合数据",
        category=FunctionCategory.AGGREGATION,
        tags=["aggregation", "time-series", "resampling"]
    )

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        time_field = config.get("time_field", "time")
        value_fields = config.get("value_fields")  # 可以是多个字段
        interval = config.get("interval", "1min")
        method = config.get("method", "mean")
        group_by_fields = config.get("group_by_fields", [])
        
        self._validate_columns(data, [time_field])
        
        result = data.copy()
        
        # 转换时间列
        if not pd.api.types.is_datetime64_any_dtype(result[time_field]):
            result[time_field] = pd.to_datetime(result[time_field])
        
        result.set_index(time_field, inplace=True)
        
        # 确定要聚合的列
        if not value_fields:
            value_fields = result.select_dtypes(include=[np.number]).columns.tolist()
            if group_by_fields:
                value_fields = [f for f in value_fields if f not in group_by_fields]
        
        # 分组聚合
        if group_by_fields:
            grouped = result.groupby(group_by_fields).resample(interval)[value_fields]
        else:
            grouped = result.resample(interval)[value_fields]
        
        # 执行聚合
        if hasattr(grouped, method):
            aggregated = getattr(grouped, method)()
            return aggregated.reset_index()
        else:
            raise ValueError(f"Unsupported aggregation method: {method}")

# 输入输出函数
@register_function
class CSVExporterFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="CSVExporter",
        description="导出数据到CSV文件",
        category=FunctionCategory.IO,
        tags=["export", "csv", "file", "output"]
    )

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        file_path = config.get("file_path", "./output.csv")
        include_index = config.get("index", False)
        encoding = config.get("encoding", "utf-8")
        columns = config.get("columns")  # 指定要导出的列
        
        export_data = data[columns] if columns else data
        
        try:
            export_data.to_csv(file_path, index=include_index, encoding=encoding)
            self.logger.info(f"Data exported to {file_path} ({len(export_data)} rows)")
        except Exception as e:
            self.logger.error(f"Failed to export CSV: {e}")
            raise
        
        return data  # 返回原始数据

# 信号处理和滤波函数
@register_function
class FourierTransformFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="FourierTransform",
        description="快速傅里叶变换分析频率成分",
        category=FunctionCategory.ANALYSIS,
        tags=["signal-processing", "frequency", "fft", "spectral"]
    )

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        sampling_rate = config.get("sampling_rate", 1.0)
        output_type = config.get("output_type", "magnitude")  # magnitude, phase, power
        
        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)
        
        y = data[field].dropna().values
        if len(y) == 0:
            self.logger.warning("No valid data for FFT")
            return pd.DataFrame()
        
        # 执行FFT
        fft_result = np.fft.fft(y)
        frequencies = np.fft.fftfreq(len(y), 1/sampling_rate)
        
        # 只保留正频率
        positive_freq_idx = frequencies > 0
        frequencies = frequencies[positive_freq_idx]
        fft_result = fft_result[positive_freq_idx]
        
        if output_type == "magnitude":
            values = np.abs(fft_result)
        elif output_type == "phase":
            values = np.angle(fft_result)
        elif output_type == "power":
            values = np.abs(fft_result) ** 2
        
        return pd.DataFrame({
            "frequency": frequencies,
            f"{field}_fft_{output_type}": values
        })

@register_function
class LowPassFilterFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="LowPassFilter",
        description="低通滤波器去除高频噪声",
        category=FunctionCategory.FILTER,
        tags=["signal-processing", "filter", "denoising"]
    )

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        cutoff = config.get("cutoff", 0.1)
        order = config.get("order", 4)
        output_field = config.get("output_field", f"{field}_filtered")
        
        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)
        
        result = data.copy()
        
        try:
            from scipy.signal import butter, filtfilt
            
            if len(result) < order + 1:
                self.logger.warning("Not enough data points for filtering")
                result[output_field] = result[field]
                return result
            
            b, a = butter(order, cutoff, btype='low', analog=False)
            result[output_field] = filtfilt(b, a, result[field].values)
            
        except ImportError:
            self.logger.warning("scipy not available, using simple moving average")
            window = max(3, int(1/cutoff))
            result[output_field] = result[field].rolling(window=window).mean()
        except Exception as e:
            self.logger.error(f"Filter error: {e}")
            result[output_field] = result[field]
        
        return result

@register_function
class BandPassFilterFunction(BaseFunction):
    metadata = FunctionMetadata(
        name="BandPassFilter",
        description="带通滤波器保留特定频率范围",
        category=FunctionCategory.FILTER,
        tags=["signal-processing", "filter", "frequency-selection"]
    )

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        lowcut = config.get("lowcut", 0.1)
        highcut = config.get("highcut", 0.3)
        order = config.get("order", 4)
        output_field = config.get("output_field", f"{field}_bandpass")
        
        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)
        
        result = data.copy()
        
        if lowcut >= highcut:
            self.logger.error("Low cutoff must be less than high cutoff")
            result[output_field] = result[field]
            return result
        
        try:
            from scipy.signal import butter, filtfilt
            
            if len(result) < order + 1:
                self.logger.warning("Not enough data points for filtering")
                result[output_field] = result[field]
                return result
            
            b, a = butter(order, [lowcut, highcut], btype='band', analog=False)
            result[output_field] = filtfilt(b, a, result[field].values)
            
        except ImportError:
            self.logger.warning("scipy not available, band-pass filter unavailable")
            result[output_field] = result[field]
        except Exception as e:
            self.logger.error(f"Filter error: {e}")
            result[output_field] = result[field]
        
        return result

# --- 使用示例 ---
if __name__ == "__main__":
    # 创建测试数据
    dates = pd.date_range('2024-01-01', periods=100, freq='1min')
    test_data = pd.DataFrame({
        'time': dates,
        'value': np.random.randn(100) * 10 + 50 + 5 * np.sin(np.arange(100) * 0.1),  # 添加周期性
        'category': np.random.choice(['A', 'B'], 100)
    })
    test_data.loc[10:15, 'value'] = np.nan  # 添加一些缺失值
    test_data.loc[50:52, 'value'] = 120  # 添加一些异常值
    
    print("Original data shape:", test_data.shape)
    print("Missing values:", test_data['value'].isna().sum())
    
    # 1. 填充缺失值
    filled_data, _ = REGISTRY.execute("FillNA", test_data, {
        "field": "value",
        "method": "interpolate"
    })
    print("After filling NA:", filled_data['value'].isna().sum())
    
    # 2. 计算移动平均（多种方法）
    ma_data, _ = REGISTRY.execute("MovingAverage", filled_data, {
        "field": "value",
        "window": 5,
        "method": "exponential",
        "alpha": 0.3
    })
    print("After moving average:", ma_data.columns.tolist())
    
    # 3. 计算标准差
    std_data, _ = REGISTRY.execute("StdDeviation", ma_data, {
        "field": "value",
        "window": 10
    })
    print("After std calculation:", std_data.columns.tolist())
    
    # 4. 数据归一化
    norm_data, _ = REGISTRY.execute("Normalize", std_data, {
        "field": "value",
        "method": "zscore"
    })
    print("After normalization:", norm_data.columns.tolist())
    
    # 5. Z-Score异常检测
    zscore_data, _ = REGISTRY.execute("ZScore", norm_data, {
        "field": "value",
        "threshold": 2.0
    })
    zscore_anomalies = zscore_data['is_anomaly'].sum()
    print(f"Z-Score anomalies: {zscore_anomalies}")
    
    # 6. IQR异常检测
    iqr_data, _ = REGISTRY.execute("IQRAnomaly", zscore_data, {
        "field": "value",
        "k": 1.5
    })
    iqr_anomalies = iqr_data['is_outlier'].sum()
    print(f"IQR anomalies: {iqr_anomalies}")
    
    # 7. 阈值告警
    alarm_data, _ = REGISTRY.execute("ThresholdAlarm", iqr_data, {
        "field": "value_norm",
        "threshold": 1.5,
        "mode": "greater"
    })
    alarm_count = alarm_data['is_alarm'].sum()
    print(f"Threshold alarms: {alarm_count}")
    
    # 8. 变化率告警
    rate_alarm_data, _ = REGISTRY.execute("RateOfChangeAlarm", alarm_data, {
        "field": "value",
        "rate_threshold": 15.0,
        "rate_type": "absolute"
    })
    rate_alarms = rate_alarm_data['rate_alarm'].sum()
    print(f"Rate of change alarms: {rate_alarms}")
    
    # 9. 组合告警
    combined_alarm_data, _ = REGISTRY.execute("CombinedAlarm", rate_alarm_data, {
        "conditions": [
            {"field": "value", "operator": "gt", "value": 70},
            {"field": "is_anomaly", "operator": "eq", "value": True}
        ],
        "logic": "and"
    })
    combined_alarms = combined_alarm_data['combined_alarm'].sum()
    print(f"Combined alarms: {combined_alarms}")
    
    # 10. 低通滤波
    filtered_data, _ = REGISTRY.execute("LowPassFilter", combined_alarm_data, {
        "field": "value",
        "cutoff": 0.1,
        "order": 4
    })
    print("After low-pass filter:", filtered_data.columns.tolist())
    
    # 11. 数据聚合
    agg_data, _ = REGISTRY.execute("Aggregator", filtered_data, {
        "time_field": "time",
        "value_fields": ["value", "value_filtered"],
        "interval": "5min",
        "method": "mean"
    })
    print("After aggregation shape:", agg_data.shape)
    
    # 12. FFT频谱分析
    try:
        fft_data, _ = REGISTRY.execute("FourierTransform", test_data, {
            "field": "value",
            "sampling_rate": 1.0,
            "output_type": "magnitude"
        })
        print("FFT analysis shape:", fft_data.shape)
    except Exception as e:
        print(f"FFT analysis skipped: {e}")
    
    # 13. 导出CSV
    try:
        REGISTRY.execute("CSVExporter", agg_data, {
            "file_path": "./processed_data.csv",
            "index": False
        })
        print("Data exported to CSV")
    except Exception as e:
        print(f"CSV export failed: {e}")
    
    # 列出所有注册的函数
    print(f"\nTotal registered functions: {len(REGISTRY.list_all())}")
    print("\nRegistered functions by category:")
    for category in FunctionCategory:
        functions = REGISTRY.get_by_category(category)
        if functions:
            print(f"\n{category.value.upper()}:")
            for func_name in functions:
                func_cls = REGISTRY.get(func_name)
                print(f"  - {func_name}: {func_cls.metadata.description}")
    
    print(f"\nExample processing complete! Original data: {len(test_data)} rows -> Aggregated: {len(agg_data)} rows")