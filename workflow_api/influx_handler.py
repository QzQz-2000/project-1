# # import pandas as pd
# # from pandas import DataFrame, concat
# # from typing import List, Optional, Dict, Union, Any
# # from influxdb_client import InfluxDBClient
# # from influxdb_client.rest import ApiException
# # import logging
# # import os
# # from dotenv import load_dotenv

# # load_dotenv()

# # # 配置日志系统
# # logging.basicConfig(
# #     level=logging.INFO,  # 设置为 DEBUG
# #     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# # )

# # # 获取日志器实例
# # logger = logging.getLogger(__name__)

# # class InfluxDBDataHandler:
# #     """
# #     一个用于InfluxDB数据查询的类，封装了Flux查询的构建和执行。
# #     """
# #     ALLOWED_AGGREGATIONS = {
# #         "mean", "max", "min", "sum", "count", "first", "last",
# #         "median", "mode", "stddev", "spread", "distinct", "integral", "rate"
# #     }

# #     def __init__(self, url: str, token: str, org: str, bucket: str):
# #         self.client = InfluxDBClient(url=url, token=token, org=org)
# #         self.query_api = self.client.query_api()
# #         self.bucket = bucket
# #         self.org = org
# #         logger.info(f"InfluxDBDataHandler initialized for org: '{org}', bucket: '{bucket}'")

# #     def _log_query(self, query: str):
# #         logger.debug("[InfluxDB Query]:\n%s", query)

# #     def _escape_flux_string(self, value: str) -> str:
# #         return value.replace('\\', '\\\\').replace('"', '\\"')

# #     def _format_time_param(self, t: Union[str, int]) -> str:
# #         if isinstance(t, str):
# #             if t.startswith("-") or t == "now()":
# #                 return t
# #             return f'"{t}"'
# #         return str(t)

# #     def query_data(
# #         self,
# #         measurement: Union[str, List[str]],
# #         fields: Optional[List[str]] = None,
# #         start: Union[str, int] = "-1h",
# #         stop: Union[str, int] = "now()",
# #         aggregation: Optional[str] = None,
# #         aggregate_every: Optional[str] = None,
# #         tags: Optional[Dict[str, Union[str, List[str]]]] = None,
# #         filters: Optional[List[str]] = None,
# #         limit: Optional[int] = None,
# #         pivot: bool = False,
# #         fill: Optional[Union[bool, str, int, float]] = False,
# #         merge_result: bool = True
# #     ) -> DataFrame:
# #         start_str = self._format_time_param(start)
# #         stop_str = self._format_time_param(stop)

# #         flux_query_parts = [
# #             f'from(bucket: "{self._escape_flux_string(self.bucket)}")',
# #             f'|> range(start: {start_str}, stop: {stop_str})'
# #         ]
        
# #         # 测量名过滤
# #         if isinstance(measurement, str):
# #             flux_query_parts.append(f'|> filter(fn: (r) => r["_measurement"] == "{self._escape_flux_string(measurement)}")')
# #         else:
# #             m_filter = " or ".join(
# #                 [f'r["_measurement"] == "{self._escape_flux_string(m)}"' for m in measurement]
# #             )
# #             flux_query_parts.append(f'|> filter(fn: (r) => ({m_filter}))')

# #         # 字段过滤
# #         if fields:
# #             field_filters = " or ".join([f'r["_field"] == "{self._escape_flux_string(field)}"' for field in fields])
# #             flux_query_parts.append(f'|> filter(fn: (r) => ({field_filters}))')

# #         # 标签过滤
# #         if tags:
# #             for key, value in tags.items():
# #                 if isinstance(value, list):
# #                     tag_expr = " or ".join(
# #                         [f'r["{self._escape_flux_string(key)}"] == "{self._escape_flux_string(str(v))}"' for v in value]
# #                     )
# #                     flux_query_parts.append(f'|> filter(fn: (r) => ({tag_expr}))')
# #                 else:
# #                     flux_query_parts.append(f'|> filter(fn: (r) => r["{self._escape_flux_string(key)}"] == "{self._escape_flux_string(str(value))}")')

# #         # 自定义过滤表达式
# #         if filters:
# #             for f in filters:
# #                 flux_query_parts.append(f'|> filter(fn: (r) => ({f}))')

# #         # 聚合处理
# #         if aggregation:
# #             aggregation = aggregation.lower()
# #             if aggregation not in self.ALLOWED_AGGREGATIONS:
# #                 raise ValueError(f"Unsupported aggregation function: {aggregation}. Allowed: {', '.join(self.ALLOWED_AGGREGATIONS)}")
# #             if not aggregate_every:
# #                 raise ValueError("Aggregation function requires 'aggregate_every' time interval.")
# #             flux_query_parts.append(f'|> aggregateWindow(every: {aggregate_every}, fn: {aggregation}, createEmpty: false)')

# #         # 填充缺失值 (针对 InfluxDB 2.7.0+ 优化)
# #         if fill is not False:
# #             # 填充通常在聚合之后才有意义，如果存在聚合，则进行填充
# #             if aggregation and aggregate_every:
# #                 if fill is True:  # usePrevious
# #                     flux_query_parts.append('|> fill(usePrevious: true)')
# #                 elif isinstance(fill, str):
# #                     # 这里删除了fill='linear'相关处理
# #                     flux_query_parts.append(f'|> fill(value: "{self._escape_flux_string(fill)}")')
# #                 elif isinstance(fill, (int, float)):
# #                     flux_query_parts.append(f'|> fill(value: {fill})')
# #                 else:
# #                     logger.warning(f"Unsupported fill option type: {type(fill)}. Fill will be skipped.")
# #             else:
# #                 logger.warning("Fill option is typically used with aggregation and 'aggregate_every'. Ignoring fill without proper context.")

# #         if pivot:
# #             flux_query_parts.append('|> group(columns: ["_measurement", "_field"])')
# #             flux_query_parts.append('|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")')

# #         if limit:
# #             flux_query_parts.append('|> sort(columns: ["_time"], desc: true)')
# #             flux_query_parts.append(f'|> limit(n: {limit})')

# #         final_flux_query = "\n".join(flux_query_parts)
# #         self._log_query(final_flux_query)

# #         try:
# #             result = self.query_api.query_data_frame(final_flux_query)
# #         except ApiException as e:
# #             logger.error(f"[API ERROR] InfluxDB API exception during query: Status {e.status}, Reason: {e.reason}, Body: {e.body}", exc_info=True)
# #             return pd.DataFrame()
# #         except Exception as e:
# #             logger.error(f"[ERROR] Query failed unexpectedly: {e}", exc_info=True)
# #             return pd.DataFrame()

# #         if isinstance(result, list):
# #             result = [df for df in result if not df.empty]
# #             if not result:
# #                 logger.info(f"Query returned an empty list of DataFrames for measurement: {measurement}, fields: {fields}")
# #                 return pd.DataFrame()
            
# #             if merge_result:
# #                 try:
# #                     result = concat(result, ignore_index=True)
# #                 except Exception as e:
# #                     logger.error(f"Failed to concatenate multiple DataFrames: {e}", exc_info=True)
# #                     return pd.DataFrame()

# #         if isinstance(result, DataFrame) and not result.empty:
# #             if '_time' in result.columns:
# #                 result['_time'] = pd.to_datetime(result['_time'])
            
# #             rename_map = {"_time": "time", "_value": "value", "_field": "field", "_measurement": "measurement"}
# #             result.rename(columns={k: v for k, v in rename_map.items() if k in result.columns}, inplace=True)
            
# #             result.sort_values("time", inplace=True)
# #             result.reset_index(drop=True, inplace=True)
# #         else:
# #             logger.info(f"Query for measurement: {measurement}, fields: {fields} returned an empty DataFrame.")
# #             result = pd.DataFrame()

# #         return result

# #     def close(self):
# #         self.client.close()
# #         logger.info("InfluxDB client connection closed.")


# if __name__ == "__main__":
#     logging.basicConfig(
#         level=logging.INFO,
#         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#     )
#     logging.getLogger("InfluxDBDataHandler").setLevel(logging.DEBUG)

#     # --------------------------
#     # 从环境变量中获取配置信息
#     # --------------------------
#     URL = os.getenv("INFLUXDB_URL")
#     TOKEN = os.getenv("INFLUXDB_TOKEN")
#     ORG = os.getenv("INFLUXDB_ORG")
#     BUCKET = os.getenv("INFLUXDB_BUCKET")

#     # 检查是否所有必要的环境变量都已设置
#     if not all([URL, TOKEN, ORG, BUCKET]):
#         logger.error("InfluxDB connection parameters are not fully set in environment variables (or .env file).")
#         logger.error("Please ensure INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET are defined.")
#         exit(1)

#     handler = InfluxDBDataHandler(url=URL, token=TOKEN, org=ORG, bucket=BUCKET)

#     try:
#         # --- 示例查询 1: 基本查询，获取最近1小时的温度和湿度数据 ---
#         logger.info("\n--- Query Example 1: Basic query for temperature and humidity ---")
#         df_basic = handler.query_data(
#             measurement="home",
#             fields=["temp", "hum"],
#             start="-1h"
#         )
#         print("Basic Query Result (first 5 rows):\n", df_basic.head())
#         print(f"Total rows: {len(df_basic)}\n")

#         # --- 示例查询 2: 聚合查询，获取过去24小时每5分钟的平均温度和湿度 ---
#         logger.info("\n--- Query Example 2: Aggregated query for average temp/hum every 5 minutes ---")
#         df_agg = handler.query_data(
#             measurement="home",
#             fields=["temp", "hum"],
#             start="-24h",
#             stop="now()",
#             aggregation="mean",
#             aggregate_every="5m",
#             pivot=True
#         )
#         print("Aggregated Query Result (first 5 rows):\n", df_agg.head())
#         print(f"Total rows: {len(df_agg)}\n")

#         # --- 示例查询 3: 带标签过滤，查询特定房间的温度最大值 ---
#         logger.info("\n--- Query Example 3: Tag filtering and max aggregation for a specific room ---")
#         df_tag_filter = handler.query_data(
#             measurement="home",
#             fields=["temp"],
#             start="-6h",
#             aggregation="max",
#             aggregate_every="1h",
#             tags={"room": "Living Room"},
#             limit=5
#         )
#         print("Tag Filter Query Result (first 5 rows):\n", df_tag_filter.head())
#         print(f"Total rows: {len(df_tag_filter)}\n")

#         # --- 示例查询 4: 复杂过滤和多测量查询 ---
#         logger.info("\n--- Query Example 4: Complex filtering and multiple measurements ---")
#         df_complex = handler.query_data(
#             measurement=["home", "office"],
#             fields=["power"],
#             start="-7d",
#             filters=['r._value > 100', 'r.device_id != "device_X"'],
#             tags={"location": ["kitchen", "basement"]},
#             aggregation="sum",
#             aggregate_every="1d",
#             pivot=True,
#             limit=3
#         )
#         print("Complex Query Result (first 5 rows):\n", df_complex.head())
#         print(f"Total rows: {len(df_complex)}\n")

#         # --- 示例查询 5: 填充缺失值 (usePrevious) ---
#         logger.info("\n--- Query Example 5: Fill missing values with previous ---")
#         df_fill_prev = handler.query_data(
#             measurement="home",
#             fields=["temp"],
#             start="-2h",
#             aggregation="mean",
#             aggregate_every="10s",
#             fill=True
#         )
#         print("Fill with Previous Query Result (first 5 rows):\n", df_fill_prev.head())
#         print(f"Total rows: {len(df_fill_prev)}\n")

#         # --- 示例查询 6: 填充缺失值 (特定值) ---
#         logger.info("\n--- Query Example 6: Fill missing values with a specific value ---")
#         df_fill_value = handler.query_data(
#             measurement="home",
#             fields=["temp"],
#             start="-2h",
#             aggregation="mean",
#             aggregate_every="10s",
#             fill=0.0
#         )
#         print("Fill with Specific Value Query Result (first 5 rows):\n", df_fill_value.head())
#         print(f"Total rows: {len(df_fill_value)}\n")

#     except ValueError as ve:
#         logger.error(f"Configuration or query error: {ve}")
#     except ApiException as ae:
#         logger.error(f"InfluxDB API error: {ae.status} - {ae.reason} - {ae.body}")
#     except Exception as e:
#         logger.error(f"An unexpected error occurred: {e}", exc_info=True)
#     finally:
#         handler.close()
#         logger.info("Application finished.")

import pandas as pd
from pandas import DataFrame, concat
from typing import List, Optional, Dict, Union, Any, Tuple, Callable
from influxdb_client import InfluxDBClient
from influxdb_client.rest import ApiException
from influxdb_client.client.flux_table import FluxTable
import logging
import os
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
from dotenv import load_dotenv

load_dotenv()

# 配置日志系统
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


class AggregationType(Enum):
    """支持的聚合类型枚举"""
    MEAN = "mean"
    MAX = "max"
    MIN = "min"
    SUM = "sum"
    COUNT = "count"
    FIRST = "first"
    LAST = "last"
    MEDIAN = "median"
    MODE = "mode"
    STDDEV = "stddev"
    SPREAD = "spread"
    DISTINCT = "distinct"
    INTEGRAL = "integral"
    RATE = "rate"
    PERCENTILE = "percentile"


class FillMethod(Enum):
    """填充方法枚举"""
    NONE = "none"
    NULL = "null"
    PREVIOUS = "previous"
    LINEAR = "linear"
    VALUE = "value"


@dataclass
class QueryResult:
    """查询结果封装类"""
    data: DataFrame
    success: bool
    error: Optional[str] = None
    query: Optional[str] = None
    execution_time: Optional[float] = None
    row_count: int = 0
    
    @property
    def is_empty(self) -> bool:
        return self.data.empty if self.data is not None else True


@dataclass
class QueryConfig:
    """查询配置类"""
    measurement: Union[str, List[str]]
    fields: Optional[List[str]] = None
    start: Union[str, int, datetime] = "-1h"
    stop: Union[str, int, datetime] = "now()"
    aggregation: Optional[Union[str, AggregationType]] = None
    aggregate_every: Optional[str] = None
    tags: Optional[Dict[str, Union[str, List[str]]]] = None
    filters: Optional[List[str]] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    pivot: bool = False
    fill: Optional[Union[FillMethod, str, int, float]] = None
    group_by: Optional[List[str]] = None
    order_by: Optional[List[Tuple[str, str]]] = None  # [(column, 'asc'|'desc')]
    timezone: Optional[str] = None
    

class InfluxDBQueryBuilder:
    """Flux 查询构建器"""
    
    def __init__(self, bucket: str, org: str):
        self.bucket = bucket
        self.org = org
        self.query_parts: List[str] = []
        
    def add_from(self) -> 'InfluxDBQueryBuilder':
        self.query_parts.append(f'from(bucket: "{self._escape(self.bucket)}")')
        return self
        
    def add_range(self, start: str, stop: str) -> 'InfluxDBQueryBuilder':
        self.query_parts.append(f'|> range(start: {start}, stop: {stop})')
        return self
        
    def add_filter(self, filter_expr: str) -> 'InfluxDBQueryBuilder':
        self.query_parts.append(f'|> filter(fn: (r) => {filter_expr})')
        return self
        
    def add_aggregate_window(self, every: str, fn: str, create_empty: bool = False) -> 'InfluxDBQueryBuilder':
        self.query_parts.append(
            f'|> aggregateWindow(every: {every}, fn: {fn}, createEmpty: {str(create_empty).lower()})'
        )
        return self
        
    def add_fill(self, method: Union[FillMethod, str, int, float]) -> 'InfluxDBQueryBuilder':
        if isinstance(method, FillMethod):
            if method == FillMethod.PREVIOUS:
                self.query_parts.append('|> fill(usePrevious: true)')
            elif method == FillMethod.LINEAR:
                self.query_parts.append('|> fill(column: "_value", usePrevious: false)')
            elif method == FillMethod.NULL:
                self.query_parts.append('|> fill(value: null)')
        elif isinstance(method, (int, float)):
            self.query_parts.append(f'|> fill(value: {method})')
        elif isinstance(method, str):
            self.query_parts.append(f'|> fill(value: "{self._escape(method)}")')
        return self
        
    def add_pivot(self) -> 'InfluxDBQueryBuilder':
        self.query_parts.append('|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")')
        return self
        
    def add_group(self, columns: List[str]) -> 'InfluxDBQueryBuilder':
        cols = ', '.join([f'"{col}"' for col in columns])
        self.query_parts.append(f'|> group(columns: [{cols}])')
        return self
        
    def add_sort(self, columns: List[Tuple[str, str]]) -> 'InfluxDBQueryBuilder':
        sort_cols = []
        desc_cols = []
        for col, order in columns:
            sort_cols.append(f'"{col}"')
            if order.lower() == 'desc':
                desc_cols.append('true')
            else:
                desc_cols.append('false')
        
        cols_str = ', '.join(sort_cols)
        desc_str = ', '.join(desc_cols)
        self.query_parts.append(f'|> sort(columns: [{cols_str}], desc: [{desc_str}])')
        return self
        
    def add_limit(self, n: int, offset: Optional[int] = None) -> 'InfluxDBQueryBuilder':
        if offset:
            self.query_parts.append(f'|> limit(n: {n}, offset: {offset})')
        else:
            self.query_parts.append(f'|> limit(n: {n})')
        return self
        
    def add_timezone(self, tz: str) -> 'InfluxDBQueryBuilder':
        self.query_parts.append(f'|> timeShift(duration: 0s, columns: ["_time"], timezone: "{tz}")')
        return self
        
    def build(self) -> str:
        return '\n'.join(self.query_parts)
        
    @staticmethod
    def _escape(value: str) -> str:
        return value.replace('\\', '\\\\').replace('"', '\\"')


class InfluxDBDataHandler:
    """
    增强版 InfluxDB 数据查询处理器
    """
    
    def __init__(
        self, 
        url: str, 
        token: str, 
        org: str, 
        bucket: str,
        timeout: int = 30000,
        verify_ssl: bool = True,
        enable_gzip: bool = True
    ):
        """
        初始化 InfluxDB 客户端
        
        Args:
            url: InfluxDB 服务器 URL
            token: 认证令牌
            org: 组织名称
            bucket: 数据桶名称
            timeout: 超时时间（毫秒）
            verify_ssl: 是否验证 SSL 证书
            enable_gzip: 是否启用 GZIP 压缩
        """
        self.url = url
        self.org = org
        self.bucket = bucket
        
        self.client = InfluxDBClient(
            url=url, 
            token=token, 
            org=org,
            timeout=timeout,
            verify_ssl=verify_ssl,
            enable_gzip=enable_gzip
        )
        
        self.query_api = self.client.query_api()
        
        # 测试连接
        try:
            self.client.ping()
            logger.info(f"Successfully connected to InfluxDB at {url}")
        except Exception as e:
            logger.error(f"Failed to connect to InfluxDB: {e}")
            raise
            
        logger.info(f"InfluxDBDataHandler initialized for org: '{org}', bucket: '{bucket}'")

    def query_data(
        self,
        config: QueryConfig,
        merge_result: bool = True,
        raw_response: bool = False,
        progress_callback: Optional[Callable[[int], None]] = None
    ) -> QueryResult:
        """
        执行查询
        
        Args:
            config: 查询配置对象
            merge_result: 是否合并多个 DataFrame
            raw_response: 是否返回原始响应
            progress_callback: 进度回调函数
            
        Returns:
            QueryResult: 查询结果对象
        """
        import time
        start_time = time.time()
        
        try:
            # 构建查询
            query = self._build_query(config)
            logger.debug(f"Executing Flux query:\n{query}")
            
            # 执行查询
            if raw_response:
                result = self.query_api.query(query)
                df = self._process_flux_tables(result, merge_result, progress_callback)
            else:
                result = self.query_api.query_data_frame(query)
                df = self._process_dataframes(result, merge_result, progress_callback)
            
            # 后处理
            if not df.empty:
                df = self._post_process_dataframe(df, config)
            
            execution_time = time.time() - start_time
            
            return QueryResult(
                data=df,
                success=True,
                query=query,
                execution_time=execution_time,
                row_count=len(df) if df is not None else 0
            )
            
        except ApiException as e:
            error_msg = f"InfluxDB API error: Status {e.status}, Reason: {e.reason}"
            logger.error(error_msg)
            return QueryResult(
                data=pd.DataFrame(),
                success=False,
                error=error_msg,
                query=query if 'query' in locals() else None,
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            error_msg = f"Query failed: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return QueryResult(
                data=pd.DataFrame(),
                success=False,
                error=error_msg,
                query=query if 'query' in locals() else None,
                execution_time=time.time() - start_time
            )

    def query_data_simple(
        self,
        measurement: Union[str, List[str]],
        fields: Optional[List[str]] = None,
        start: Union[str, int, datetime] = "-1h",
        stop: Union[str, int, datetime] = "now()",
        **kwargs
    ) -> DataFrame:
        """
        简化的查询接口，保持向后兼容
        
        Returns:
            DataFrame: 查询结果数据
        """
        config = QueryConfig(
            measurement=measurement,
            fields=fields,
            start=start,
            stop=stop,
            **kwargs
        )
        
        result = self.query_data(config)
        if not result.success:
            logger.error(f"Query failed: {result.error}")
        
        return result.data

    def query_latest(
        self,
        measurement: Union[str, List[str]],
        fields: Optional[List[str]] = None,
        tags: Optional[Dict[str, Union[str, List[str]]]] = None,
        last_n: int = 1
    ) -> DataFrame:
        """
        查询最新的数据点
        
        Args:
            measurement: 测量名称
            fields: 字段列表
            tags: 标签过滤条件
            last_n: 返回最新的 n 条记录
            
        Returns:
            DataFrame: 最新数据
        """
        config = QueryConfig(
            measurement=measurement,
            fields=fields,
            start="-7d",  # 查询最近7天
            stop="now()",
            tags=tags,
            limit=last_n,
            order_by=[("_time", "desc")]
        )
        
        return self.query_data(config).data

    def query_aggregated(
        self,
        measurement: Union[str, List[str]],
        fields: List[str],
        aggregation: Union[str, AggregationType],
        window: str,
        start: Union[str, int, datetime] = "-1h",
        stop: Union[str, int, datetime] = "now()",
        fill: Optional[Union[FillMethod, str, int, float]] = None,
        **kwargs
    ) -> DataFrame:
        """
        执行聚合查询
        
        Args:
            measurement: 测量名称
            fields: 字段列表
            aggregation: 聚合函数
            window: 时间窗口（如 "5m", "1h"）
            start: 开始时间
            stop: 结束时间
            fill: 填充方法
            **kwargs: 其他查询参数
            
        Returns:
            DataFrame: 聚合结果
        """
        config = QueryConfig(
            measurement=measurement,
            fields=fields,
            start=start,
            stop=stop,
            aggregation=aggregation,
            aggregate_every=window,
            fill=fill,
            **kwargs
        )
        
        return self.query_data(config).data

    def _build_query(self, config: QueryConfig) -> str:
        """构建 Flux 查询语句"""
        builder = InfluxDBQueryBuilder(self.bucket, self.org)
        
        # 基础查询
        builder.add_from()
        
        # 时间范围
        start_str = self._format_time_param(config.start)
        stop_str = self._format_time_param(config.stop)
        builder.add_range(start_str, stop_str)
        
        # 测量名过滤
        if isinstance(config.measurement, str):
            builder.add_filter(f'r["_measurement"] == "{self._escape(config.measurement)}"')
        else:
            m_filters = ' or '.join([f'r["_measurement"] == "{self._escape(m)}"' for m in config.measurement])
            builder.add_filter(f'({m_filters})')
        
        # 字段过滤
        if config.fields:
            field_filters = ' or '.join([f'r["_field"] == "{self._escape(f)}"' for f in config.fields])
            builder.add_filter(f'({field_filters})')
        
        # 标签过滤
        if config.tags:
            for key, value in config.tags.items():
                if isinstance(value, list):
                    tag_filters = ' or '.join([f'r["{self._escape(key)}"] == "{self._escape(str(v))}"' for v in value])
                    builder.add_filter(f'({tag_filters})')
                else:
                    builder.add_filter(f'r["{self._escape(key)}"] == "{self._escape(str(value))}"')
        
        # 自定义过滤器
        if config.filters:
            for f in config.filters:
                builder.add_filter(f)
        
        # 聚合
        if config.aggregation:
            if not config.aggregate_every:
                raise ValueError("Aggregation requires 'aggregate_every' parameter")
            
            agg_fn = config.aggregation
            if isinstance(agg_fn, AggregationType):
                agg_fn = agg_fn.value
                
            builder.add_aggregate_window(config.aggregate_every, agg_fn)
            
            # 填充
            if config.fill is not None:
                builder.add_fill(config.fill)
        
        # 分组
        if config.group_by:
            builder.add_group(config.group_by)
        
        # 数据透视
        if config.pivot:
            if not config.group_by:
                builder.add_group(["_measurement", "_field"])
            builder.add_pivot()
        
        # 排序
        if config.order_by:
            builder.add_sort(config.order_by)
        elif config.limit and not config.aggregation:
            # 默认按时间降序排序
            builder.add_sort([("_time", "desc")])
        
        # 限制和偏移
        if config.limit:
            builder.add_limit(config.limit, config.offset)
        
        # 时区
        if config.timezone:
            builder.add_timezone(config.timezone)
        
        return builder.build()

    def _process_flux_tables(
        self, 
        tables: List[FluxTable], 
        merge: bool,
        progress_callback: Optional[Callable[[int], None]] = None
    ) -> DataFrame:
        """处理 Flux 表格响应"""
        dataframes = []
        total_records = sum(len(table.records) for table in tables)
        processed = 0
        
        for table in tables:
            records = []
            for record in table.records:
                records.append(record.values)
                processed += 1
                if progress_callback and processed % 100 == 0:
                    progress_callback(int(processed / total_records * 100))
                    
            if records:
                df = pd.DataFrame(records)
                dataframes.append(df)
        
        if not dataframes:
            return pd.DataFrame()
            
        if merge and len(dataframes) > 1:
            return pd.concat(dataframes, ignore_index=True)
        elif len(dataframes) == 1:
            return dataframes[0]
        else:
            return dataframes

    def _process_dataframes(
        self, 
        result: Union[DataFrame, List[DataFrame]], 
        merge: bool,
        progress_callback: Optional[Callable[[int], None]] = None
    ) -> DataFrame:
        """处理 DataFrame 响应"""
        if isinstance(result, DataFrame):
            return result
            
        if isinstance(result, list):
            # 过滤空 DataFrame
            result = [df for df in result if not df.empty]
            
            if not result:
                return pd.DataFrame()
                
            if merge:
                return pd.concat(result, ignore_index=True)
            elif len(result) == 1:
                return result[0]
                
        return pd.DataFrame()

    def _post_process_dataframe(self, df: DataFrame, config: QueryConfig) -> DataFrame:
        """后处理 DataFrame"""
        if df.empty:
            return df
            
        # 转换时间列
        if '_time' in df.columns:
            df['_time'] = pd.to_datetime(df['_time'])
            
        # 重命名列
        rename_map = {
            "_time": "time",
            "_value": "value",
            "_field": "field",
            "_measurement": "measurement"
        }
        df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)
        
        # 删除不需要的列
        drop_columns = ['result', 'table', '_start', '_stop']
        df.drop(columns=[col for col in drop_columns if col in df.columns], inplace=True)
        
        # 重置索引
        df.reset_index(drop=True, inplace=True)
        
        return df

    def _format_time_param(self, t: Union[str, int, datetime]) -> str:
        """格式化时间参数"""
        if isinstance(t, str):
            if t.startswith("-") or t == "now()":
                return t
            return f'"{t}"'
        elif isinstance(t, int):
            return str(t)
        elif isinstance(t, datetime):
            return f'"{t.isoformat()}Z"'
        else:
            return str(t)

    def _escape(self, value: str) -> str:
        """转义 Flux 字符串"""
        return value.replace('\\', '\\\\').replace('"', '\\"')

    def get_measurements(self) -> List[str]:
        """获取所有测量名称"""
        query = f'''
        import "influxdata/influxdb/schema"
        schema.measurements(bucket: "{self._escape(self.bucket)}")
        '''
        
        result = self.query_api.query(query)
        measurements = []
        
        for table in result:
            for record in table.records:
                measurements.append(record.get_value())
                
        return sorted(measurements)

    def get_field_keys(self, measurement: str) -> List[str]:
        """获取指定测量的所有字段键"""
        query = f'''
        import "influxdata/influxdb/schema"
        schema.fieldKeys(
            bucket: "{self._escape(self.bucket)}",
            predicate: (r) => r._measurement == "{self._escape(measurement)}"
        )
        '''
        
        result = self.query_api.query(query)
        fields = []
        
        for table in result:
            for record in table.records:
                fields.append(record.get_value())
                
        return sorted(fields)

    def get_tag_keys(self, measurement: str) -> List[str]:
        """获取指定测量的所有标签键"""
        query = f'''
        import "influxdata/influxdb/schema"
        schema.tagKeys(
            bucket: "{self._escape(self.bucket)}",
            predicate: (r) => r._measurement == "{self._escape(measurement)}"
        )
        '''
        
        result = self.query_api.query(query)
        tags = []
        
        for table in result:
            for record in table.records:
                tags.append(record.get_value())
                
        return sorted(tags)

    def get_tag_values(self, measurement: str, tag_key: str) -> List[str]:
        """获取指定标签的所有值"""
        query = f'''
        import "influxdata/influxdb/schema"
        schema.tagValues(
            bucket: "{self._escape(self.bucket)}",
            tag: "{self._escape(tag_key)}",
            predicate: (r) => r._measurement == "{self._escape(measurement)}"
        )
        '''
        
        result = self.query_api.query(query)
        values = []
        
        for table in result:
            for record in table.records:
                values.append(record.get_value())
                
        return sorted(values)

    def test_connection(self) -> bool:
        """测试连接是否正常"""
        try:
            self.client.ping()
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def close(self):
        """关闭客户端连接"""
        try:
            self.client.close()
            logger.info("InfluxDB client connection closed.")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")


# 示例用法
if __name__ == "__main__":
    # 从环境变量加载配置
    handler = InfluxDBDataHandler(
        url=os.getenv("INFLUXDB_URL", "http://localhost:8086"),
        token=os.getenv("INFLUXDB_TOKEN"),
        org=os.getenv("INFLUXDB_ORG", "my-org"),
        bucket=os.getenv("INFLUXDB_BUCKET", "my-bucket")
    )
    
    # 示例1: 简单查询
    df = handler.query_data_simple(
        measurement="temperature",
        fields=["value"],
        start="-1h"
    )
    print(f"Simple query returned {len(df)} rows")
    
    # 示例2: 使用配置对象的复杂查询
    config = QueryConfig(
        measurement=["temperature", "humidity"],
        fields=["value"],
        start="-24h",
        aggregation=AggregationType.MEAN,
        aggregate_every="1h",
        tags={"location": ["room1", "room2"]},
        fill=FillMethod.PREVIOUS,
        pivot=True
    )
    
    result = handler.query_data(config)
    if result.success:
        print(f"Complex query returned {result.row_count} rows in {result.execution_time:.2f} seconds")
    else:
        print(f"Query failed: {result.error}")
    
    # 示例3: 获取最新数据
    latest = handler.query_latest(
        measurement="temperature",
        last_n=5
    )
    print(f"Latest data: {latest}")
    
    # 示例4: 聚合查询
    aggregated = handler.query_aggregated(
        measurement="temperature",
        fields=["value"],
        aggregation="mean",
        window="5m",
        start="-6h"
    )
    print(f"Aggregated data shape: {aggregated.shape}")
    
    # 关闭连接
    handler.close()