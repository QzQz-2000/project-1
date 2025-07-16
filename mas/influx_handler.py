import pandas as pd
from pandas import DataFrame, concat
from typing import List, Optional, Dict, Union, Any
from influxdb_client import InfluxDBClient
from influxdb_client.rest import ApiException
import logging
import os
from dotenv import load_dotenv

load_dotenv()

# 配置日志系统
logging.basicConfig(
    level=logging.INFO,  # 设置为 DEBUG
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# 获取日志器实例
logger = logging.getLogger(__name__)

class InfluxDBDataHandler:
    """
    一个用于InfluxDB数据查询的类，封装了Flux查询的构建和执行。
    """
    ALLOWED_AGGREGATIONS = {
        "mean", "max", "min", "sum", "count", "first", "last",
        "median", "mode", "stddev", "spread", "distinct", "integral", "rate"
    }

    def __init__(self, url: str, token: str, org: str, bucket: str):
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.query_api = self.client.query_api()
        self.bucket = bucket
        self.org = org
        logger.info(f"InfluxDBDataHandler initialized for org: '{org}', bucket: '{bucket}'")

    def _log_query(self, query: str):
        logger.debug("[InfluxDB Query]:\n%s", query)

    def _escape_flux_string(self, value: str) -> str:
        return value.replace('\\', '\\\\').replace('"', '\\"')

    def _format_time_param(self, t: Union[str, int]) -> str:
        if isinstance(t, str):
            if t.startswith("-") or t == "now()":
                return t
            return f'"{t}"'
        return str(t)

    def query_data(
        self,
        measurement: Union[str, List[str]],
        fields: Optional[List[str]] = None,
        start: Union[str, int] = "-1h",
        stop: Union[str, int] = "now()",
        aggregation: Optional[str] = None,
        aggregate_every: Optional[str] = None,
        tags: Optional[Dict[str, Union[str, List[str]]]] = None,
        filters: Optional[List[str]] = None,
        limit: Optional[int] = None,
        pivot: bool = False,
        fill: Optional[Union[bool, str, int, float]] = False,
        merge_result: bool = True
    ) -> DataFrame:
        start_str = self._format_time_param(start)
        stop_str = self._format_time_param(stop)

        flux_query_parts = [
            f'from(bucket: "{self._escape_flux_string(self.bucket)}")',
            f'|> range(start: {start_str}, stop: {stop_str})'
        ]
        
        # 测量名过滤
        if isinstance(measurement, str):
            flux_query_parts.append(f'|> filter(fn: (r) => r["_measurement"] == "{self._escape_flux_string(measurement)}")')
        else:
            m_filter = " or ".join(
                [f'r["_measurement"] == "{self._escape_flux_string(m)}"' for m in measurement]
            )
            flux_query_parts.append(f'|> filter(fn: (r) => ({m_filter}))')

        # 字段过滤
        if fields:
            field_filters = " or ".join([f'r["_field"] == "{self._escape_flux_string(field)}"' for field in fields])
            flux_query_parts.append(f'|> filter(fn: (r) => ({field_filters}))')

        # 标签过滤
        if tags:
            for key, value in tags.items():
                if isinstance(value, list):
                    tag_expr = " or ".join(
                        [f'r["{self._escape_flux_string(key)}"] == "{self._escape_flux_string(str(v))}"' for v in value]
                    )
                    flux_query_parts.append(f'|> filter(fn: (r) => ({tag_expr}))')
                else:
                    flux_query_parts.append(f'|> filter(fn: (r) => r["{self._escape_flux_string(key)}"] == "{self._escape_flux_string(str(value))}")')

        # 自定义过滤表达式
        if filters:
            for f in filters:
                flux_query_parts.append(f'|> filter(fn: (r) => ({f}))')

        # 聚合处理
        if aggregation:
            aggregation = aggregation.lower()
            if aggregation not in self.ALLOWED_AGGREGATIONS:
                raise ValueError(f"Unsupported aggregation function: {aggregation}. Allowed: {', '.join(self.ALLOWED_AGGREGATIONS)}")
            if not aggregate_every:
                raise ValueError("Aggregation function requires 'aggregate_every' time interval.")
            flux_query_parts.append(f'|> aggregateWindow(every: {aggregate_every}, fn: {aggregation}, createEmpty: false)')

        # 填充缺失值 (针对 InfluxDB 2.7.0+ 优化)
        if fill is not False:
            # 填充通常在聚合之后才有意义，如果存在聚合，则进行填充
            if aggregation and aggregate_every:
                if fill is True:  # usePrevious
                    flux_query_parts.append('|> fill(usePrevious: true)')
                elif isinstance(fill, str):
                    # 这里删除了fill='linear'相关处理
                    flux_query_parts.append(f'|> fill(value: "{self._escape_flux_string(fill)}")')
                elif isinstance(fill, (int, float)):
                    flux_query_parts.append(f'|> fill(value: {fill})')
                else:
                    logger.warning(f"Unsupported fill option type: {type(fill)}. Fill will be skipped.")
            else:
                logger.warning("Fill option is typically used with aggregation and 'aggregate_every'. Ignoring fill without proper context.")

        if pivot:
            flux_query_parts.append('|> group(columns: ["_measurement", "_field"])')
            flux_query_parts.append('|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")')

        if limit:
            flux_query_parts.append('|> sort(columns: ["_time"], desc: true)')
            flux_query_parts.append(f'|> limit(n: {limit})')

        final_flux_query = "\n".join(flux_query_parts)
        self._log_query(final_flux_query)

        try:
            result = self.query_api.query_data_frame(final_flux_query)
        except ApiException as e:
            logger.error(f"[API ERROR] InfluxDB API exception during query: Status {e.status}, Reason: {e.reason}, Body: {e.body}", exc_info=True)
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"[ERROR] Query failed unexpectedly: {e}", exc_info=True)
            return pd.DataFrame()

        if isinstance(result, list):
            result = [df for df in result if not df.empty]
            if not result:
                logger.info(f"Query returned an empty list of DataFrames for measurement: {measurement}, fields: {fields}")
                return pd.DataFrame()
            
            if merge_result:
                try:
                    result = concat(result, ignore_index=True)
                except Exception as e:
                    logger.error(f"Failed to concatenate multiple DataFrames: {e}", exc_info=True)
                    return pd.DataFrame()

        if isinstance(result, DataFrame) and not result.empty:
            if '_time' in result.columns:
                result['_time'] = pd.to_datetime(result['_time'])
            
            rename_map = {"_time": "time", "_value": "value", "_field": "field", "_measurement": "measurement"}
            result.rename(columns={k: v for k, v in rename_map.items() if k in result.columns}, inplace=True)
            
            result.sort_values("time", inplace=True)
            result.reset_index(drop=True, inplace=True)
        else:
            logger.info(f"Query for measurement: {measurement}, fields: {fields} returned an empty DataFrame.")
            result = pd.DataFrame()

        return result

    def close(self):
        self.client.close()
        logger.info("InfluxDB client connection closed.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logging.getLogger("InfluxDBDataHandler").setLevel(logging.DEBUG)

    # --------------------------
    # 从环境变量中获取配置信息
    # --------------------------
    URL = os.getenv("INFLUXDB_URL")
    TOKEN = os.getenv("INFLUXDB_TOKEN")
    ORG = os.getenv("INFLUXDB_ORG")
    BUCKET = os.getenv("INFLUXDB_BUCKET")

    # 检查是否所有必要的环境变量都已设置
    if not all([URL, TOKEN, ORG, BUCKET]):
        logger.error("InfluxDB connection parameters are not fully set in environment variables (or .env file).")
        logger.error("Please ensure INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET are defined.")
        exit(1)

    handler = InfluxDBDataHandler(url=URL, token=TOKEN, org=ORG, bucket=BUCKET)

    try:
        # --- 示例查询 1: 基本查询，获取最近1小时的温度和湿度数据 ---
        logger.info("\n--- Query Example 1: Basic query for temperature and humidity ---")
        df_basic = handler.query_data(
            measurement="home",
            fields=["temp", "hum"],
            start="-1h"
        )
        print("Basic Query Result (first 5 rows):\n", df_basic.head())
        print(f"Total rows: {len(df_basic)}\n")

        # --- 示例查询 2: 聚合查询，获取过去24小时每5分钟的平均温度和湿度 ---
        logger.info("\n--- Query Example 2: Aggregated query for average temp/hum every 5 minutes ---")
        df_agg = handler.query_data(
            measurement="home",
            fields=["temp", "hum"],
            start="-24h",
            stop="now()",
            aggregation="mean",
            aggregate_every="5m",
            pivot=True
        )
        print("Aggregated Query Result (first 5 rows):\n", df_agg.head())
        print(f"Total rows: {len(df_agg)}\n")

        # --- 示例查询 3: 带标签过滤，查询特定房间的温度最大值 ---
        logger.info("\n--- Query Example 3: Tag filtering and max aggregation for a specific room ---")
        df_tag_filter = handler.query_data(
            measurement="home",
            fields=["temp"],
            start="-6h",
            aggregation="max",
            aggregate_every="1h",
            tags={"room": "Living Room"},
            limit=5
        )
        print("Tag Filter Query Result (first 5 rows):\n", df_tag_filter.head())
        print(f"Total rows: {len(df_tag_filter)}\n")

        # --- 示例查询 4: 复杂过滤和多测量查询 ---
        logger.info("\n--- Query Example 4: Complex filtering and multiple measurements ---")
        df_complex = handler.query_data(
            measurement=["home", "office"],
            fields=["power"],
            start="-7d",
            filters=['r._value > 100', 'r.device_id != "device_X"'],
            tags={"location": ["kitchen", "basement"]},
            aggregation="sum",
            aggregate_every="1d",
            pivot=True,
            limit=3
        )
        print("Complex Query Result (first 5 rows):\n", df_complex.head())
        print(f"Total rows: {len(df_complex)}\n")

        # --- 示例查询 5: 填充缺失值 (usePrevious) ---
        logger.info("\n--- Query Example 5: Fill missing values with previous ---")
        df_fill_prev = handler.query_data(
            measurement="home",
            fields=["temp"],
            start="-2h",
            aggregation="mean",
            aggregate_every="10s",
            fill=True
        )
        print("Fill with Previous Query Result (first 5 rows):\n", df_fill_prev.head())
        print(f"Total rows: {len(df_fill_prev)}\n")

        # --- 示例查询 6: 填充缺失值 (特定值) ---
        logger.info("\n--- Query Example 6: Fill missing values with a specific value ---")
        df_fill_value = handler.query_data(
            measurement="home",
            fields=["temp"],
            start="-2h",
            aggregation="mean",
            aggregate_every="10s",
            fill=0.0
        )
        print("Fill with Specific Value Query Result (first 5 rows):\n", df_fill_value.head())
        print(f"Total rows: {len(df_fill_value)}\n")

    except ValueError as ve:
        logger.error(f"Configuration or query error: {ve}")
    except ApiException as ae:
        logger.error(f"InfluxDB API error: {ae.status} - {ae.reason} - {ae.body}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        handler.close()
        logger.info("Application finished.")
