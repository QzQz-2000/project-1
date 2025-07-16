import pandas as pd
import numpy as np
from scipy.signal import butter, filtfilt
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Type, List, Optional, Union
import joblib # For loading models, you might use others like tensorflow, torch
from sklearn.linear_model import LinearRegression
import jsonschema # Import for JSON Schema validation

# --- Setup Logging ---
# It's good practice to set up basic logging at the application's entry point.
# Set default level to INFO for production, DEBUG for development.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# --- Abstract Base Class for Functions ---
class BaseFunction(ABC):
    """
    Abstract base class for all platform functions.
    Ensures all concrete functions adhere to a common interface.
    """
    name: str = "UnnamedFunction"
    description: str = "No description provided."
    # Enhanced JSON Schema default structure
    config_schema: Dict[str, Any] = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Function Configuration",
        "type": "object",
        "properties": {},
        "additionalProperties": False # By default, disallow additional properties for stricter validation
    }

    def __init__(self):
        """Initializes a logger for each function instance."""
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """
        Executes the core logic of the function.
        Functions should aim to return a *new* DataFrame, or a copy of the input
        DataFrame with modifications, to avoid unintended side effects on the
        original DataFrame passed into `run_function`.
        
        :param data: Input Pandas DataFrame.
        :param config: Configuration parameters for the function.
        :return: Processed Pandas DataFrame.
        """
        pass

    def _validate_columns(self, data: pd.DataFrame, required_columns: List[str]):
        """
        Helper method to validate if the DataFrame contains all required columns.
        """
        missing_cols = [col for col in required_columns if col not in data.columns]
        if missing_cols:
            raise ValueError(f"Function '{self.name}' requires columns: {', '.join(missing_cols)}.")

    def _validate_numeric_column(self, data: pd.DataFrame, field: str):
        """
        Helper method to validate if a specified column is numeric.
        """
        if not pd.api.types.is_numeric_dtype(data[field]):
            raise TypeError(f"Function '{self.name}' requires numeric data in column '{field}', but found non-numeric type.")


# --- Function Registry and Decorator ---
FUNCTION_REGISTRY: Dict[str, Type[BaseFunction]] = {}

def register_function(cls: Type[BaseFunction]):
    """
    Decorator to register function classes into the global registry.
    Ensures the class inherits from BaseFunction and has a valid name.
    """
    if not issubclass(cls, BaseFunction):
        raise TypeError(f"Function class {cls.__name__} must inherit from BaseFunction.")
    if not hasattr(cls, 'name') or not isinstance(cls.name, str) or not cls.name:
        raise AttributeError(f"Function class {cls.__name__} must define a non-empty 'name' attribute.")
    
    if cls.name in FUNCTION_REGISTRY:
        raise ValueError(f"Function with name '{cls.name}' already registered by class {FUNCTION_REGISTRY[cls.name].__name__}. Current class: {cls.__name__}")
    
    FUNCTION_REGISTRY[cls.name] = cls
    logging.debug(f"Registered function: {cls.name} ({cls.__name__})")
    return cls

# --- Data Transformation Functions ---

@register_function
class MovingAverageFunction(BaseFunction):
    name = "MovingAverage"
    description = "计算滑动平均值"
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "MovingAverage Configuration",
        "type": "object",
        "properties": {
            "window": {"type": "integer", "default": 5, "description": "滑动窗口大小", "minimum": 1},
            "field": {"type": "string", "default": "value", "description": "要应用滑动平均的列名"},
            "drop_na_rows": {"type": "boolean", "default": True, "description": "是否删除因滑动窗口导致的NaN行"}
        },
        "required": ["field"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: dict) -> pd.DataFrame:
        window = config.get("window", 5)
        field = config.get("field", "value")
        drop_na_rows = config.get("drop_na_rows", True)

        self._validate_columns(data, [field]) # Validate input column
        self._validate_numeric_column(data, field)

        if data[field].empty:
            self.logger.warning(f"Field '{field}' in DataFrame is empty. Cannot apply MovingAverage. Returning original data.")
            return data.copy()

        processed_data = data.copy() # Work on a copy
        # Create a new column for the result
        new_column_name = f"{field}_ma{window}"
        processed_data[new_column_name] = processed_data[field].rolling(window=window).mean()
        
        if drop_na_rows:
            original_rows = len(processed_data)
            processed_data = processed_data.dropna(subset=[new_column_name])
            dropped_rows = original_rows - len(processed_data)
            self.logger.info(f"Applied MovingAverage to '{field}' with window {window}. New column: {new_column_name}. Dropped {dropped_rows} rows with NaN values.")
        else:
            self.logger.info(f"Applied MovingAverage to '{field}' with window {window}. New column: {new_column_name}. NaN rows kept.")
        
        return processed_data

@register_function
class FillNaFunction(BaseFunction):
    name = "FillNA"
    description = "填充缺失值"
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "FillNA Configuration",
        "type": "object",
        "properties": {
            "method": {"type": "string", "default": "ffill", "enum": ["ffill", "bfill", "mean", "value"], "description": "填充方法"},
            "field": {"type": "string", "default": "value", "description": "要填充的列名"},
            # fill_value's type should be flexible, runtime check for compatibility
            "fill_value": {"description": "当method为'value'时的填充值 (类型需与列兼容)"}
        },
        "required": ["field"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        method = config.get("method", "ffill")
        field = config.get("field", "value")
        fill_value = config.get("fill_value")

        self._validate_columns(data, [field])

        processed_data = data.copy() # Work on a copy to avoid SettingWithCopyWarning
        
        if processed_data[field].empty:
            self.logger.warning(f"Field '{field}' in DataFrame is empty. No NA to fill. Returning original data.")
            return processed_data

        if method == "mean":
            self._validate_numeric_column(processed_data, field) # Ensure numeric for mean
            processed_data[field] = processed_data[field].fillna(processed_data[field].mean())
            self.logger.info(f"Filled NA in '{field}' using mean method.")
        elif method == "value":
            if fill_value is None:
                self.logger.warning(f"Method is 'value' but 'fill_value' is not provided for field '{field}'. Skipping fill.")
            else:
                # Basic type compatibility check for fill_value and column dtype
                if pd.isna(processed_data[field]).all(): # If all are NA, dtype might be object, allow fill
                    processed_data[field] = processed_data[field].fillna(fill_value)
                elif not isinstance(fill_value, (int, float)) and pd.api.types.is_numeric_dtype(processed_data[field]):
                    self.logger.warning(f"Fill value '{fill_value}' type might not be compatible with numeric field '{field}'. Attempting fill anyway.")
                    processed_data[field] = processed_data[field].fillna(fill_value)
                else:
                    processed_data[field] = processed_data[field].fillna(fill_value)
                self.logger.info(f"Filled NA in '{field}' with specific value '{fill_value}'.")
        else: # ffill or bfill
            processed_data[field] = processed_data[field].fillna(method=method)
            self.logger.info(f"Filled NA in '{field}' using '{method}' method.")
        return processed_data

@register_function
class NormalizeFunction(BaseFunction):
    name = "Normalize"
    description = "将指定列的值归一化到 [0, 1]"
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Normalize Configuration",
        "type": "object",
        "properties": {
            "field": {"type": "string", "default": "value", "description": "要归一化的列名"},
            "output_field": {"type": "string", "description": "归一化后结果的新列名 (默认为原列名_norm)"}
        },
        "required": ["field"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        output_field = config.get("output_field", f"{field}_norm")

        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)

        processed_data = data.copy()
        min_val = processed_data[field].min()
        max_val = processed_data[field].max()

        if max_val == min_val:
            self.logger.warning(f"Min and max values are the same for '{field}'. Normalized value will be 0.0.")
            processed_data[output_field] = 0.0
        else:
            processed_data[output_field] = (processed_data[field] - min_val) / (max_val - min_val)
        
        self.logger.info(f"Normalized field '{field}' to '{output_field}'.")
        return processed_data

@register_function
class DifferencingFunction(BaseFunction):
    name = "Differencing"
    description = "计算一阶差分，常用于平稳化时间序列"
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Differencing Configuration",
        "type": "object",
        "properties": {
            "field": {"type": "string", "default": "value", "description": "要计算差分的列名"},
            "periods": {"type": "integer", "default": 1, "description": "差分周期", "minimum": 1},
            "output_field": {"type": "string", "description": "差分结果的新列名 (默认为原列名_diff)"},
            "drop_na_rows": {"type": "boolean", "default": True, "description": "是否删除因差分导致的NaN行"}
        },
        "required": ["field"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        periods = config.get("periods", 1)
        output_field = config.get("output_field", f"{field}_diff")
        drop_na_rows = config.get("drop_na_rows", True)

        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)

        processed_data = data.copy()
        processed_data[output_field] = processed_data[field].diff(periods=periods)
        
        if drop_na_rows:
            original_rows = len(processed_data)
            processed_data = processed_data.dropna(subset=[output_field])
            dropped_rows = original_rows - len(processed_data)
            self.logger.info(f"Applied {periods}-period differencing to '{field}'. New column: {output_field}. Dropped {dropped_rows} rows with NaN values.")
        else:
            self.logger.info(f"Applied {periods}-period differencing to '{field}'. New column: {output_field}. NaN rows kept.")
        return processed_data

@register_function
class StdDeviationFunction(BaseFunction):
    name = "StdDeviation"
    description = "计算滑动标准差"
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "StdDeviation Configuration",
        "type": "object",
        "properties": {
            "window": {"type": "integer", "default": 5, "description": "滑动窗口大小", "minimum": 1},
            "field": {"type": "string", "default": "value", "description": "要计算标准差的列名"},
            "output_field": {"type": "string", "description": "标准差结果的新列名 (默认为原列名_std)"}
        },
        "required": ["field"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        window = config.get("window", 5)
        field = config.get("field", "value")
        output_field = config.get("output_field", f"{field}_std")

        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)

        processed_data = data.copy()
        processed_data[output_field] = processed_data[field].rolling(window=window).std()
        self.logger.info(f"Calculated rolling standard deviation for '{field}' with window {window}. New column: {output_field}")
        return processed_data.dropna(subset=[output_field]) # Still drop NaN for std by default, as they are often invalid.

@register_function
class ZScoreFunction(BaseFunction):
    name = "ZScore"
    description = "标记z-score异常值"
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "ZScore Configuration",
        "type": "object",
        "properties": {
            "field": {"type": "string", "default": "value", "description": "要计算Z-Score的列名"},
            "threshold": {"type": "number", "default": 2.0, "description": "异常阈值 (绝对值)", "minimum": 0},
            "zscore_output_field": {"type": "string", "default": "zscore", "description": "Z-Score结果列名"},
            "anomaly_output_field": {"type": "string", "default": "is_anomaly", "description": "异常标记结果列名"}
        },
        "required": ["field"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        threshold = config.get("threshold", 2.0)
        zscore_output_field = config.get("zscore_output_field", "zscore")
        anomaly_output_field = config.get("anomaly_output_field", "is_anomaly")

        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)

        processed_data = data.copy()
        mean_val = processed_data[field].mean()
        std_val = processed_data[field].std()

        if std_val == 0:
            self.logger.warning(f"Standard deviation for '{field}' is zero. Z-scores will be zero. Cannot detect anomalies based on std deviation for this data range.")
            processed_data[zscore_output_field] = 0.0
            processed_data[anomaly_output_field] = False
        else:
            processed_data[zscore_output_field] = (processed_data[field] - mean_val) / std_val
            processed_data[anomaly_output_field] = processed_data[zscore_output_field].abs() > threshold
        
        self.logger.info(f"Calculated Z-Scores for '{field}' (threshold={threshold}).")
        return processed_data

# --- Alarming Function ---
@register_function
class ThresholdAlarmFunction(BaseFunction):
    name = "ThresholdAlarm"
    description = "基于阈值判断触发告警标记"
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "ThresholdAlarm Configuration",
        "type": "object",
        "properties": {
            "field": {"type": "string", "default": "value", "description": "要检查的列名"},
            "threshold": {"type": "number", "default": 80, "description": "阈值"},
            "mode": {"type": "string", "default": "greater", "enum": ["greater", "less", "equal", "not_equal", "greater_equal", "less_equal"], "description": "判断模式"},
            "alarm_output_field": {"type": "string", "default": "is_alarm", "description": "告警标记结果列名"}
        },
        "required": ["field", "threshold", "mode"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: dict) -> pd.DataFrame:
        field = config.get("field", "value")
        threshold = config.get("threshold", 80)
        mode = config.get("mode", "greater")
        alarm_output_field = config.get("alarm_output_field", "is_alarm")

        self._validate_columns(data, [field])
        processed_data = data.copy() # Work on a copy

        if not pd.api.types.is_numeric_dtype(processed_data[field]):
            self.logger.error(f"Field '{field}' is not numeric, cannot apply threshold alarm. Setting '{alarm_output_field}' to False.")
            processed_data[alarm_output_field] = False # Add column with default value
            return processed_data

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
        else:
            # This case should ideally be caught by JSON schema validation, but included for robustness
            self.logger.error(f"Unsupported mode '{mode}' for ThresholdAlarm. Setting '{alarm_output_field}' to False.")
            processed_data[alarm_output_field] = False
            return processed_data
        
        self.logger.info(f"Applied ThresholdAlarm to '{field}' (threshold={threshold}, mode='{mode}'). New column: {alarm_output_field}")
        return processed_data

# --- Aggregation Function ---
@register_function
class AggregatorFunction(BaseFunction):
    name = "Aggregator"
    description = "按时间粒度聚合数据"
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Aggregator Configuration",
        "type": "object",
        "properties": {
            "time_field": {"type": "string", "default": "time", "description": "时间列名"},
            "value_field": {"type": "string", "default": "value", "description": "要聚合的数值列名"},
            "interval": {"type": "string", "default": "1min", "description": "聚合时间粒度，例如 '1min', '1h', '1D'"},
            "method": {"type": "string", "default": "mean", "enum": ["mean", "sum", "max", "min", "first", "last", "count", "median"], "description": "聚合方法"}, # Added count, median
            "group_by_fields": {"type": "array", "items": {"type": "string"}, "description": "可选：按这些字段分组后聚合"}
        },
        "required": ["time_field", "value_field", "interval", "method"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: dict) -> pd.DataFrame:
        time_field = config.get("time_field", "time")
        value_field = config.get("value_field", "value")
        interval = config.get("interval", "1min")
        method = config.get("method", "mean")
        group_by_fields = config.get("group_by_fields", [])

        self._validate_columns(data, [time_field, value_field] + group_by_fields)

        processed_data = data.copy()
        
        if processed_data.empty:
            self.logger.warning("Input DataFrame is empty. Aggregation skipped. Returning empty DataFrame.")
            return pd.DataFrame()

        # Convert time_field to datetime only if it's not already
        if not pd.api.types.is_datetime64_any_dtype(processed_data[time_field]):
            try:
                processed_data[time_field] = pd.to_datetime(processed_data[time_field])
                self.logger.debug(f"Converted '{time_field}' to datetime type.")
            except Exception as e:
                self.logger.error(f"Failed to convert '{time_field}' to datetime type: {e}. Cannot perform aggregation. Returning original data.", exc_info=True)
                return data.copy()

        processed_data.set_index(time_field, inplace=True)
        self._validate_numeric_column(processed_data, value_field) # Value field must be numeric for aggregation

        if group_by_fields:
            # Ensure group_by_fields are not in index if they are columns
            if any(f in processed_data.index.names for f in group_by_fields):
                self.logger.error(f"Cannot group by fields that are part of the index: {group_by_fields}. Returning original data.")
                return data.copy() # Revert to original if invalid state

            grouped = processed_data.groupby(group_by_fields).resample(interval)[value_field]
        else:
            grouped = processed_data.resample(interval)[value_field]

        # Use getattr to call the specified aggregation method dynamically
        if not hasattr(grouped, method):
            self.logger.error(f"Aggregation method '{method}' not found for grouped data. Returning empty DataFrame.")
            return pd.DataFrame()
            
        aggregated_data = getattr(grouped, method)()

        if group_by_fields:
            aggregated_data = aggregated_data.reset_index()
        else:
            # For non-grouped aggregation, reset_index to make time_field a column again
            aggregated_data = aggregated_data.to_frame().reset_index()
            
        self.logger.info(f"Aggregated '{value_field}' by '{interval}' with '{method}' method. Grouped by: {group_by_fields}")
        return aggregated_data.dropna() # Drop rows where aggregation might result in NA (e.g., empty intervals)

# --- Storage Function ---
@register_function
class CSVExporterFunction(BaseFunction):
    name = "CSVExporter"
    description = "将数据导出为CSV文件"
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "CSVExporter Configuration",
        "type": "object",
        "properties": {
            "file_path": {"type": "string", "default": "./output.csv", "description": "输出CSV文件的完整路径"},
            "index": {"type": "boolean", "default": False, "description": "是否写入DataFrame索引"},
            "encoding": {"type": "string", "default": "utf-8", "description": "CSV文件编码"},
            "raise_on_error": {"type": "boolean", "default": False, "description": "导出失败时是否抛出异常"}
        },
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        file_path = config.get("file_path", "./output.csv")
        include_index = config.get("index", False)
        encoding = config.get("encoding", "utf-8")
        raise_on_error = config.get("raise_on_error", False)

        try:
            data.to_csv(file_path, index=include_index, encoding=encoding)
            self.logger.info(f"Data successfully exported to {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to export data to CSV at {file_path}: {e}", exc_info=True)
            if raise_on_error:
                raise # Re-raise the exception to stop the workflow
        return data # Always return the data, even on error (if not re-raising), to allow workflow to continue

# --- Fourier Transform Function ---
@register_function
class FourierTransformFunction(BaseFunction):
    name = "FourierTransform"
    description = "执行快速傅里叶变换（FFT）以分析频率成分"
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "FourierTransform Configuration",
        "type": "object",
        "properties": {
            "field": {"type": "string", "default": "value", "description": "要进行FFT的数值列名"},
            "output_frequency_field": {"type": "string", "default": "frequency", "description": "输出频率列名"},
            "output_amplitude_field": {"type": "string", "default": "amplitude", "description": "输出振幅列名"}
        },
        "required": ["field"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        output_freq_field = config.get("output_frequency_field", "frequency")
        output_amp_field = config.get("output_amplitude_field", "amplitude")

        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)

        y = data[field].values
        if len(y) == 0:
            self.logger.warning(f"No data points in field '{field}' for Fourier Transform. Returning empty DataFrame.")
            return pd.DataFrame(columns=[output_freq_field, output_amp_field]) # Return DataFrame with expected columns

        fft_result = np.fft.fft(y)
        frequencies = np.fft.fftfreq(len(y))
        
        result_df = pd.DataFrame({
            output_freq_field: frequencies,
            output_amp_field: np.abs(fft_result)
        })
        self.logger.info(f"Applied Fourier Transform to '{field}'. Resulting DataFrame has {len(result_df)} rows.")
        return result_df

# --- Low Pass Filter Function ---
@register_function
class LowPassFilterFunction(BaseFunction):
    name = "LowPassFilter"
    description = "应用低通滤波器以去除高频噪声"
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "LowPassFilter Configuration",
        "type": "object",
        "properties": {
            "field": {"type": "string", "default": "value", "description": "要滤波的数值列名"},
            "cutoff": {"type": "number", "default": 0.1, "description": "截止频率 (0.0 到 0.5)", "minimum": 0.0, "maximum": 0.5},
            "order": {"type": "integer", "default": 4, "description": "滤波器阶数", "minimum": 1},
            "output_field": {"type": "string", "description": "滤波结果的新列名 (默认为原列名_filtered)"}
        },
        "required": ["field", "cutoff", "order"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        cutoff = config.get("cutoff", 0.1)
        order = config.get("order", 4)
        output_field = config.get("output_field", f"{field}_filtered")

        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)
        
        processed_data = data.copy()

        if len(processed_data) < order + 1:
            self.logger.warning(f"Not enough data points ({len(processed_data)}) for filter order ({order}). Skipping LowPassFilter for '{field}'. Output column '{output_field}' will be filled with original values.")
            processed_data[output_field] = processed_data[field] # Add the column with original values
            return processed_data

        try:
            b, a = butter(order, cutoff, btype='low', analog=False)
            processed_data[output_field] = filtfilt(b, a, processed_data[field].values)
            self.logger.info(f"Applied LowPassFilter to '{field}' (cutoff={cutoff}, order={order}). New column: {output_field}")
        except Exception as e:
            self.logger.error(f"Error applying LowPassFilter to '{field}': {e}. Output column '{output_field}' will be filled with original values.", exc_info=True)
            processed_data[output_field] = processed_data[field] # Fallback to original values on error
        return processed_data

# --- Simple PID Controller Function (Stateless for Batch Processing) ---
@register_function
class PIDControllerFunction(BaseFunction):
    name = "PIDController"
    description = "基于目标值的简易 PID 控制器 (批量无状态计算)"
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "PIDController Configuration",
        "type": "object",
        "properties": {
            "field": {"type": "string", "default": "value", "description": "当前过程值（PV）的列名"},
            "target": {"type": "number", "default": 100, "description": "目标设定点（SP）"},
            "Kp": {"type": "number", "default": 1.0, "description": "比例增益"},
            "Ki": {"type": "number", "default": 0.1, "description": "积分增益"},
            "Kd": {"type": "number", "default": 0.01, "description": "微分增益"},
            "output_error_field": {"type": "string", "default": "error", "description": "误差列名"},
            "output_integral_field": {"type": "string", "default": "integral", "description": "积分项列名"},
            "output_derivative_field": {"type": "string", "default": "derivative", "description": "微分项列名"},
            "output_control_field": {"type": "string", "default": "control_output", "description": "控制输出列名"},
            "initial_integral": {"type": "number", "default": 0.0, "description": "积分项的初始值 (仅用于批量计算)"},
            "initial_previous_error": {"type": "number", "default": 0.0, "description": "上一个时间步的误差值 (仅用于批量计算)"}
        },
        "required": ["field", "target", "Kp", "Ki", "Kd"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        field = config.get("field", "value")
        target = config["target"]
        Kp, Ki, Kd = config["Kp"], config["Ki"], config["Kd"]
        initial_integral = config.get("initial_integral", 0.0)
        initial_previous_error = config.get("initial_previous_error", 0.0)
        
        error_field = config.get("output_error_field", "error")
        integral_field = config.get("output_integral_field", "integral")
        derivative_field = config.get("output_derivative_field", "derivative")
        control_field = config.get("output_control_field", "control_output")

        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)

        processed_data = data.copy()
        
        error = target - processed_data[field]
        processed_data[error_field] = error
        
        # Cumulative sum for integral, starting from initial_integral
        processed_data[integral_field] = error.cumsum() + initial_integral
        
        # First difference for derivative, fillna(0) for the first element
        # and adjust for initial_previous_error for the first actual diff
        diff_error = error.diff()
        if not diff_error.empty:
            diff_error.iloc[0] = error.iloc[0] - initial_previous_error
        processed_data[derivative_field] = diff_error.fillna(0)
        
        processed_data[control_field] = Kp * processed_data[error_field] + \
                                        Ki * processed_data[integral_field] + \
                                        Kd * processed_data[derivative_field]
        
        self.logger.info(f"Applied PID controller to '{field}' (Target={target}, Kp={Kp}, Ki={Ki}, Kd={Kd}).")
        
        # Note: For a truly stateful PID, the last_error and integral would be stored in the function instance
        # or an external state store between calls for real-time applications.
        # This implementation is for batch processing where each 'run' is an independent calculation.
        return processed_data

# --- AI Model Prediction Function ---

# ModelLoader (Placeholder - implement based on your actual ML framework)
class ModelLoader:
    def __init__(self, model_path: str, expected_features: Optional[List[str]] = None):
        self.model_path = model_path
        self._model = None
        self.expected_features = expected_features
        self.logger = logging.getLogger(self.__class__.__name__)
        self._load_model()

    def _load_model(self):
        """
        Loads the machine learning model from the specified path.
        Replace this with your actual model loading logic (e.g., joblib.load, tf.keras.models.load_model).
        """
        try:
            # Example: Load a scikit-learn model or a dummy object for demonstration
            # For a real scenario: self._model = joblib.load(self.model_path)
            
            # --- MOCK MODEL FOR DEMONSTRATION ---
            class MockPredictor:
                def __init__(self, expected_features_mock: Optional[List[str]]):
                    self.expected_features_mock = expected_features_mock
                    self.logger = logging.getLogger(self.__class__.__name__ + ".MockPredictor")
                    self.logger.debug(f"MockPredictor initialized with expected features: {expected_features_mock}")

                def predict(self, X_input: pd.DataFrame) -> np.ndarray:
                    if self.expected_features_mock and not X_input.columns.equals(pd.Index(self.expected_features_mock)):
                        self.logger.warning(f"Mock predictor: Input features {X_input.columns.tolist()} do not match expected features {self.expected_features_mock}. Prediction might be inaccurate or fail.")
                        # You could raise an error here or try to reorder columns
                        # For now, let's assume it proceeds if the critical feature exists.
                    
                    if 'current_temperature' in X_input.columns:
                        return (X_input['current_temperature'] * 0.8 + 5 + np.random.rand(len(X_input)) * 2).values # Added some noise
                    elif not X_input.empty:
                        self.logger.warning("Mock predictor: 'current_temperature' not in input features for mock prediction. Returning zeros.")
                        return np.zeros(len(X_input))
                    else:
                        return np.array([])
            self._model = MockPredictor(self.expected_features)
            self.logger.info(f"Successfully loaded mock model for path: {self.model_path}")
            # --- END MOCK MODEL ---

        except FileNotFoundError:
            self.logger.error(f"Model file not found at: {self.model_path}")
            self._model = None
        except Exception as e:
            self.logger.error(f"Error loading model from {self.model_path}: {e}", exc_info=True)
            self._model = None

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        Performs prediction using the loaded model.
        :param X: Input features DataFrame.
        :return: Array of prediction results.
        """
        if self._model is None:
            self.logger.error(f"Model not loaded for path: {self.model_path}. Cannot predict.")
            return np.array([])
        
        if X.empty:
            self.logger.warning("Input DataFrame for prediction is empty. Returning empty array.")
            return np.array([])
        
        # Ensure features match expected order/set if `expected_features` is provided
        if self.expected_features:
            missing_features = [f for f in self.expected_features if f not in X.columns]
            if missing_features:
                self.logger.error(f"Missing required features for prediction: {missing_features}.")
                return np.array([])
            
            # Reorder columns to match expected features
            try:
                X_ordered = X[self.expected_features]
            except KeyError as e:
                self.logger.error(f"Error reordering features for prediction. Check if features list is correct: {e}", exc_info=True)
                return np.array([])
        else:
            X_ordered = X # Proceed with given columns if no expected features are specified

        try:
            return self._model.predict(X_ordered)
        except Exception as e:
            self.logger.error(f"Error during model prediction with input shape {X_ordered.shape}: {e}", exc_info=True)
            return np.array([])


@register_function
class PredictModelFunction(BaseFunction):
    name = "PredictModel"
    description = "加载并使用机器学习模型进行预测"
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "PredictModel Configuration",
        "type": "object",
        "properties": {
            "model_path": {"type": "string", "description": "模型文件路径"},
            "features": {"type": "array", "items": {"type": "string"}, "minItems": 1, "description": "要使用的特征列名"},
            "output_column": {"type": "string", "default": "prediction", "description": "预测结果的列名"}
        },
        "required": ["model_path", "features"],
        "additionalProperties": False
    }

    def __init__(self):
        super().__init__()
        self.loader_cache: Dict[str, ModelLoader] = {} # Cache for model loaders

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        path = config["model_path"]
        features = config["features"]
        output_column = config.get("output_column", "prediction")

        self._validate_columns(data, features) # Ensure all required feature columns exist

        processed_data = data.copy()

        if path not in self.loader_cache:
            # Pass expected features to ModelLoader for internal validation
            self.loader_cache[path] = ModelLoader(path, expected_features=features) 
            self.logger.info(f"Initialized ModelLoader for path: {path}")
        loader = self.loader_cache[path]

        X_input = processed_data[features]
        preds = loader.predict(X_input)
        
        # Check if predictions were made and match the input data length
        if preds.size > 0 and len(preds) == len(processed_data):
            processed_data[output_column] = preds
            self.logger.info(f"Predictions generated and saved to column '{output_column}'.")
        else:
            self.logger.warning(f"Prediction result size mismatch or empty for path '{path}'. No predictions added to column '{output_column}'. Filling with NaN.")
            processed_data[output_column] = np.nan # Or other default value for failed predictions

        return processed_data

# --- Helper Functions for Registry and Execution ---

def get_all_functions_meta() -> List[Dict[str, Any]]:
    """
    Returns metadata for all registered functions, including their name,
    description, and configuration schema.
    """
    return [
        {
            "name": cls.name,
            "description": cls.description,
            "config_schema": cls.config_schema
        }
        for cls in FUNCTION_REGISTRY.values()
    ]

def run_function(function_name: str, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    Executes a registered function with the given data and configuration.
    :param function_name: The 'name' of the function as registered.
    :param data: The input Pandas DataFrame.
    :param config: The configuration dictionary for the function.
    :return: The DataFrame returned by the function.
    :raises ValueError: If the function is not registered or configuration is invalid.
    :raises jsonschema.ValidationError: If the configuration does not match the function's schema.
    """
    fn_class = FUNCTION_REGISTRY.get(function_name)
    if not fn_class:
        raise ValueError(f"Function '{function_name}' not registered.")
    
    # JSON Schema validation
    try:
        jsonschema.validate(instance=config, schema=fn_class.config_schema)
    except jsonschema.ValidationError as e:
        # Provide a more user-friendly error message, extracting key details
        error_path = ".".join(map(str, e.path)) if e.path else "root"
        raise ValueError(f"Invalid configuration for function '{function_name}': {e.message} at '{error_path}'. Check function's config_schema.") from e

    instance = fn_class() # Instantiate the function class
    instance.logger.info(f"Starting execution of function '{instance.name}' with config: {config}")
    
    # Pass a copy of the DataFrame to the function's run method
    # This ensures that the original DataFrame remains untouched if the function modifies its input in-place
    # (though functions are encouraged to return a new DataFrame).
    result_df = instance.run(data.copy(), config) # Ensure copy is passed
    
    instance.logger.info(f"Finished execution of function '{instance.name}'. Result shape: {result_df.shape}")
    return result_df

# --- New Functions ---

@register_function
class LinearRegressionPredictorFunction(BaseFunction):
    name = "LinearRegressionPredictor"
    description = "基于时间的线性回归预测下一个值"
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "LinearRegressionPredictor Configuration",
        "type": "object",
        "properties": {
            "predict_steps": {"type": "integer", "default": 1, "description": "预测多少步", "minimum": 1},
            "time_field": {"type": "string", "default": "time", "description": "时间列名"},
            "field": {"type": "string", "default": "value", "description": "预测值列名"},
            "prediction_output_field": {"type": "string", "default": "predicted_value", "description": "预测结果的新列名"},
            "is_prediction_flag_field": {"type": "string", "default": "is_prediction", "description": "标记是否为预测值的布尔列名"}
        },
        "required": ["time_field", "field"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: dict) -> pd.DataFrame:
        predict_steps = config.get("predict_steps", 1)
        time_field = config.get("time_field", "time")
        field = config.get("field", "value")
        prediction_output_field = config.get("prediction_output_field", "predicted_value")
        is_prediction_flag_field = config.get("is_prediction_flag_field", "is_prediction")

        self._validate_columns(data, [time_field, field])
        self._validate_numeric_column(data, field)

        processed_data = data.copy()

        if processed_data.empty:
            self.logger.warning("Input DataFrame is empty. Cannot perform LinearRegressionPrediction. Returning empty DataFrame.")
            return pd.DataFrame(columns=[time_field, field, prediction_output_field, is_prediction_flag_field])

        # Convert time to integer timestamps (seconds or milliseconds) for regression
        # Use pd.to_datetime safely first
        if not pd.api.types.is_datetime64_any_dtype(processed_data[time_field]):
            try:
                processed_data[time_field] = pd.to_datetime(processed_data[time_field])
                self.logger.debug(f"Converted '{time_field}' to datetime type for regression.")
            except Exception as e:
                self.logger.error(f"Failed to convert '{time_field}' to datetime. Cannot perform linear regression prediction: {e}", exc_info=True)
                return data.assign(**{prediction_output_field:np.nan, is_prediction_flag_field:False}) # Add columns with nan/False

        # Ensure time is sorted for correct differencing if necessary, and for future prediction
        processed_data.sort_values(by=time_field, inplace=True)
        
        # Use total seconds from an arbitrary epoch or just milliseconds from start for better precision
        # Or, just use integer seconds for simplicity as in original
        processed_data["_time_numeric"] = (processed_data[time_field] - processed_data[time_field].iloc[0]).dt.total_seconds()
        
        X = processed_data[["_time_numeric"]].values
        y = processed_data[field].values
        
        if len(X) < 2:
            self.logger.warning(f"Not enough data points ({len(X)}) for linear regression. Need at least 2 points. Skipping prediction.")
            return processed_data.assign(**{prediction_output_field:np.nan, is_prediction_flag_field:False}).drop(columns=["_time_numeric"])

        model = LinearRegression()
        model.fit(X, y)

        # Calculate time step from existing data to project future times
        # If timestamps are not uniform, this might need refinement (e.g., average step)
        # Using the last interval for simplicity
        time_diffs = np.diff(processed_data["_time_numeric"].values)
        if time_diffs.size > 0:
            avg_step = time_diffs.mean()
        else: # Should not happen if len(X) >= 2, but for safety
            avg_step = 1 # Fallback to 1 second step if no diffs

        last_numeric_time = processed_data["_time_numeric"].iloc[-1]
        last_datetime = processed_data[time_field].iloc[-1]

        future_numeric_times = np.array([[last_numeric_time + i * avg_step] for i in range(1, predict_steps + 1)])
        future_datetimes = [last_datetime + pd.Timedelta(seconds=i * avg_step) for i in range(1, predict_steps + 1)]

        predictions = model.predict(future_numeric_times)

        # Create DataFrame for predictions
        prediction_df = pd.DataFrame({
            time_field: future_datetimes,
            prediction_output_field: predictions,
            is_prediction_flag_field: True
        })
        
        # Original data will have the predicted value column as NaN and flag as False
        # Create output DataFrame by adding the new columns to the original data
        original_data_with_prediction_cols = processed_data.assign(**{
            prediction_output_field: np.nan,
            is_prediction_flag_field: False
        }).drop(columns=["_time_numeric"]) # Drop temporary numeric time column

        # Concatenate original data with predictions
        result_df = pd.concat([original_data_with_prediction_cols, prediction_df], ignore_index=True)
        
        self.logger.info(f"Applied LinearRegressionPredictor to '{field}' predicting {predict_steps} steps. Added '{prediction_output_field}' and '{is_prediction_flag_field}' columns.")
        return result_df


@register_function
class RateOfChangeAlarmFunction(BaseFunction):
    name = "RateOfChangeAlarm"
    description = "基于变化速率的告警"
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "RateOfChangeAlarm Configuration",
        "type": "object",
        "properties": {
            "rate_threshold": {"type": "number", "default": 10.0, "description": "允许最大变化速率 (绝对值)", "minimum": 0},
            "field": {"type": "string", "default": "value", "description": "计算速率的字段"},
            "rate_output_field": {"type": "string", "default": "rate_of_change", "description": "变化速率结果列名"},
            "alarm_output_field": {"type": "string", "default": "rate_alarm", "description": "速率告警标记结果列名"}
        },
        "required": ["field"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: dict) -> pd.DataFrame:
        rate_threshold = config.get("rate_threshold", 10.0)
        field = config.get("field", "value")
        rate_output_field = config.get("rate_output_field", "rate_of_change")
        alarm_output_field = config.get("alarm_output_field", "rate_alarm")

        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)

        processed_data = data.copy()
        
        # Calculate rate of change
        processed_data[rate_output_field] = processed_data[field].diff()
        
        # Mark alarm. First row will be False due to NaN from diff()
        processed_data[alarm_output_field] = processed_data[rate_output_field].abs() > rate_threshold
        
        self.logger.info(f"Applied RateOfChangeAlarm to '{field}' (threshold={rate_threshold}). Added '{rate_output_field}' and '{alarm_output_field}' columns.")
        return processed_data


@register_function
class TrendDetectorFunction(BaseFunction):
    name = "TrendDetector"
    description = "判断趋势方向（上升/下降/平稳/未知）"
    config_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "TrendDetector Configuration",
        "type": "object",
        "properties": {
            "field": {"type": "string", "default": "value", "description": "判断趋势的字段"},
            "tolerance": {"type": "number", "default": 1e-6, "description": "判断为'平稳'的数值变化容忍度", "minimum": 0},
            "trend_output_field": {"type": "string", "default": "trend", "description": "趋势结果列名"}
        },
        "required": ["field"],
        "additionalProperties": False
    }

    def run(self, data: pd.DataFrame, config: dict) -> pd.DataFrame:
        field = config.get("field", "value")
        tolerance = config.get("tolerance", 1e-6)
        trend_output_field = config.get("trend_output_field", "trend")
        
        self._validate_columns(data, [field])
        self._validate_numeric_column(data, field)

        processed_data = data.copy()
        
        # Calculate difference. First value will be NaN.
        diff_values = processed_data[field].diff()

        # Apply trend logic. Using fillna("unknown") for the first row or any NaNs
        # Using an epsilon/tolerance for 'flat' trend
        processed_data[trend_output_field] = diff_values.apply(
            lambda x: "up" if x > tolerance else \
                      ("down" if x < -tolerance else \
                       ("flat" if pd.notna(x) else "unknown"))
        )
        
        self.logger.info(f"Applied TrendDetector to '{field}' (tolerance={tolerance}). Added '{trend_output_field}' column.")
        return processed_data

# --- Example Usage ---
if __name__ == "__main__":
    # Configure the root logger directly for the example run
    logging.getLogger().setLevel(logging.DEBUG) # Set to DEBUG to see all messages including info/debug
    # logging.basicConfig is typically called once at app startup, here we ensure it's set for the script.

    df_initial = pd.DataFrame({
        "time": pd.to_datetime(["2025-07-07 14:00:00", "2025-07-07 14:01:00", "2025-07-07 14:02:00",
                                "2025-07-07 14:03:00", "2025-07-07 14:04:00", "2025-07-07 14:05:00",
                                "2025-07-07 14:06:00", "2025-07-07 14:07:00", "2025-07-07 14:08:00",
                                "2025-07-07 14:09:00"]),
        "value": [70, 75, np.nan, 85, 82, 90, 95, 88, np.nan, 81],
        "device_id": ["sensor_A"]*5 + ["sensor_B"]*5,
        "temperature": [25.1, 25.5, 25.0, 26.1, 26.5, 27.0, 27.5, 27.1, 26.8, 26.0],
        "pressure": [100.1, 100.5, 100.2, 101.0, 101.5, 101.2, 101.8, 101.5, 101.1, 100.9],
        "sensor_A_val": [10, 12, 11, 13, 14, np.nan, 15, 16, 17, 18] # For PID example
    })
    
    print("--- Original DataFrame ---")
    print(df_initial.to_string())
    print("-" * 50)

    try:
        # Step 1: Fill missing values in 'value' column using mean
        print("\n--- Running FillNAFunction on 'value' ---")
        df_step1 = run_function("FillNA", df_initial, {"field": "value", "method": "mean"})
        print(df_step1.to_string())
        print("-" * 50)

        # Step 2: Calculate Moving Average on the filled 'value' (keep NaN rows for demonstration)
        print("\n--- Running MovingAverageFunction on 'value' ---")
        df_step2 = run_function("MovingAverage", df_step1, {"field": "value", "window": 3, "drop_na_rows": False})
        print(df_step2.to_string())
        print("-" * 50)
        
        # Step 3: Normalize 'temperature'
        print("\n--- Running NormalizeFunction on 'temperature' ---")
        df_step3 = run_function("Normalize", df_step2, {"field": "temperature", "output_field": "temp_normalized"})
        print(df_step3.to_string())
        print("-" * 50)

        # Step 4: Apply Threshold Alarm on 'value'
        print("\n--- Running ThresholdAlarmFunction on 'value' ---")
        df_step4 = run_function("ThresholdAlarm", df_step3, {"field": "value", "threshold": 85, "mode": "greater"})
        print(df_step4.to_string())
        print("-" * 50)

        # Step 5: Aggregate 'value' by device_id and 5-minute interval (mean)
        print("\n--- Running AggregatorFunction ---")
        df_step5 = run_function("Aggregator", df_step4, {
            "time_field": "time", 
            "value_field": "value", 
            "interval": "5min", 
            "method": "mean",
            "group_by_fields": ["device_id"]
        })
        print(df_step5.to_string())
        print("-" * 50)

        # Step 6: Make Predictions using PredictModelFunction
        print("\n--- Running PredictModelFunction ---")
        # For the mock model, we need 'current_temperature' as a feature.
        # Let's assume 'temperature' from our initial df is used as 'current_temperature' after preprocessing.
        df_for_prediction = df_step4.copy() # Use the data after threshold alarm
        df_for_prediction.rename(columns={"temperature": "current_temperature"}, inplace=True)

        df_predicted = run_function(
            "PredictModel", 
            df_for_prediction, 
            {"model_path": "your_model.pkl", "features": ["current_temperature"], "output_column": "predicted_value"}
        )
        print(df_predicted[['time', 'device_id', 'current_temperature', 'predicted_value', 'is_alarm']].to_string())
        print("-" * 50)

        # Step 7: Apply PID Controller (Stateless Batch)
        print("\n--- Running PIDControllerFunction ---")
        df_pid_output = run_function("PIDController", df_initial.copy(), {
            "field": "temperature",
            "target": 26.0,
            "Kp": 0.5,
            "Ki": 0.05,
            "Kd": 0.02
        })
        print(df_pid_output[['time', 'temperature', 'error', 'integral', 'derivative', 'control_output']].to_string())
        print("-" * 50)

        # Step 8: Apply Linear Regression Predictor
        print("\n--- Running LinearRegressionPredictorFunction ---")
        df_lr_predicted = run_function("LinearRegressionPredictor", df_initial.copy(), {
            "time_field": "time",
            "field": "temperature",
            "predict_steps": 3
        })
        print(df_lr_predicted.to_string())
        print("-" * 50)

        # Step 9: Apply Rate of Change Alarm
        print("\n--- Running RateOfChangeAlarmFunction ---")
        df_roc_alarm = run_function("RateOfChangeAlarm", df_initial.copy(), {
            "field": "temperature",
            "rate_threshold": 0.2
        })
        print(df_roc_alarm[['time', 'temperature', 'rate_of_change', 'rate_alarm']].to_string())
        print("-" * 50)

        # Step 10: Apply Trend Detector
        print("\n--- Running TrendDetectorFunction ---")
        df_trend = run_function("TrendDetector", df_initial.copy(), {
            "field": "temperature",
            "tolerance": 0.05
        })
        print(df_trend[['time', 'temperature', 'trend']].to_string())
        print("-" * 50)

        # Step 11: Export data to CSV (e.g., the last processed DataFrame)
        print("\n--- Running CSVExporterFunction ---")
        run_function("CSVExporter", df_trend, {"file_path": "./final_processed_data.csv", "index": False, "raise_on_error": True})
        print("-" * 50)


        # Example: Get all function metadata
        print("\n--- All Registered Functions Metadata ---")
        for func_meta in get_all_functions_meta():
            print(f"Name: {func_meta['name']}")
            print(f"  Description: {func_meta['description']}")
            print(f"  Config Schema: {func_meta['config_schema']}")
            print("---")

    except ValueError as e:
        logging.error(f"Error during function execution: {e}")
    except TypeError as e:
        logging.error(f"Type error during function execution: {e}")
    except jsonschema.ValidationError as e:
        logging.error(f"Configuration validation error: {e.message} at {e.path}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)