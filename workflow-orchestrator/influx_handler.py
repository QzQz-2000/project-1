# import pandas as pd
# from pandas import DataFrame, concat
# from typing import List, Optional, Dict, Union, Any
# from influxdb_client import InfluxDBClient
# from influxdb_client.rest import ApiException
# import logging
# import os
# from dotenv import load_dotenv

# load_dotenv()

# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )

# logger = logging.getLogger(__name__)

# class InfluxDBDataHandler:
#     ALLOWED_AGGREGATIONS = {
#         "mean", "max", "min", "sum", "count", "first", "last",
#         "median", "mode", "stddev", "spread", "distinct", "integral", "rate"
#     }

#     def __init__(self, url: str, token: str, org: str, bucket: str):
#         self.client = InfluxDBClient(url=url, token=token, org=org)
#         self.query_api = self.client.query_api()
#         self.bucket = bucket
#         self.org = org
#         logger.info(f"InfluxDBDataHandler initialized for org: '{org}', bucket: '{bucket}'")

#     def _log_query(self, query: str):
#         logger.debug("[InfluxDB Query]:\n%s", query)

#     def _escape_flux_string(self, value: str) -> str:
#         return value.replace('\\', '\\\\').replace('"', '\\"')

#     def _format_time_param(self, t: Union[str, int]) -> str:
#         if isinstance(t, str):
#             if t.startswith("-") or t == "now()":
#                 return t
#             return f'"{t}"'
#         return str(t)

#     def query_data(
#         self,
#         measurement: Union[str, List[str]],
#         fields: Optional[List[str]] = None,
#         start: Union[str, int] = "-1h",
#         stop: Union[str, int] = "now()",
#         aggregation: Optional[str] = None,
#         aggregate_every: Optional[str] = None,
#         tags: Optional[Dict[str, Union[str, List[str]]]] = None,
#         filters: Optional[List[str]] = None,
#         limit: Optional[int] = None,
#         pivot: bool = False,
#         merge_result: bool = True
#     ) -> DataFrame:
#         start_str = self._format_time_param(start)
#         stop_str = self._format_time_param(stop)

#         flux_query_parts = [
#             f'from(bucket: "{self._escape_flux_string(self.bucket)}")',
#             f'|> range(start: {start_str}, stop: {stop_str})'
#         ]
        
#         if isinstance(measurement, str):
#             flux_query_parts.append(f'|> filter(fn: (r) => r["_measurement"] == "{self._escape_flux_string(measurement)}")')
#         else:
#             m_filter = " or ".join(
#                 [f'r["_measurement"] == "{self._escape_flux_string(m)}"' for m in measurement]
#             )
#             flux_query_parts.append(f'|> filter(fn: (r) => ({m_filter}))')

#         if fields:
#             field_filters = " or ".join([f'r["_field"] == "{self._escape_flux_string(field)}"' for field in fields])
#             flux_query_parts.append(f'|> filter(fn: (r) => ({field_filters}))')

#         if tags:
#             for key, value in tags.items():
#                 if isinstance(value, list):
#                     tag_expr = " or ".join(
#                         [f'r["{self._escape_flux_string(key)}"] == "{self._escape_flux_string(str(v))}"' for v in value]
#                     )
#                     flux_query_parts.append(f'|> filter(fn: (r) => ({tag_expr}))')
#                 else:
#                     flux_query_parts.append(f'|> filter(fn: (r) => r["{self._escape_flux_string(key)}"] == "{self._escape_flux_string(str(value))}")')

#         if filters:
#             for f in filters:
#                 flux_query_parts.append(f'|> filter(fn: (r) => ({f}))')

#         if aggregation:
#             aggregation = aggregation.lower()
#             if aggregation not in self.ALLOWED_AGGREGATIONS:
#                 raise ValueError(f"Unsupported aggregation function: {aggregation}. Allowed: {', '.join(self.ALLOWED_AGGREGATIONS)}")
#             if not aggregate_every:
#                 raise ValueError("Aggregation function requires 'aggregate_every' time interval.")
#             flux_query_parts.append(f'|> aggregateWindow(every: {aggregate_every}, fn: {aggregation}, createEmpty: false)')

#         if pivot:
#             flux_query_parts.append('|> group(columns: ["_measurement", "_field"])')
#             flux_query_parts.append('|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")')

#         if limit:
#             flux_query_parts.append('|> sort(columns: ["_time"], desc: true)')
#             flux_query_parts.append(f'|> limit(n: {limit})')

#         final_flux_query = "\n".join(flux_query_parts)
#         self._log_query(final_flux_query)

#         try:
#             result = self.query_api.query_data_frame(final_flux_query)
#         except ApiException as e:
#             logger.error(f"[API ERROR] InfluxDB API exception during query: Status {e.status}, Reason: {e.reason}, Body: {e.body}", exc_info=True)
#             return pd.DataFrame()
#         except Exception as e:
#             logger.error(f"[ERROR] Query failed unexpectedly: {e}", exc_info=True)
#             return pd.DataFrame()

#         if isinstance(result, list):
#             result = [df for df in result if not df.empty]
#             if not result:
#                 logger.info(f"Query returned an empty list of DataFrames for measurement: {measurement}, fields: {fields}")
#                 return pd.DataFrame()
            
#             if merge_result:
#                 try:
#                     result = concat(result, ignore_index=True)
#                 except Exception as e:
#                     logger.error(f"Failed to concatenate multiple DataFrames: {e}", exc_info=True)
#                     return pd.DataFrame()

#         if isinstance(result, DataFrame) and not result.empty:
#             if '_time' in result.columns:
#                 result['_time'] = pd.to_datetime(result['_time'])
            
#             rename_map = {"_time": "time", "_value": "value", "_field": "field", "_measurement": "measurement"}
#             result.rename(columns={k: v for k, v in rename_map.items() if k in result.columns}, inplace=True)
            
#             result.sort_values("time", inplace=True)
#             result.reset_index(drop=True, inplace=True)
#         else:
#             logger.info(f"Query for measurement: {measurement}, fields: {fields} returned an empty DataFrame.")
#             result = pd.DataFrame()

#         return result

#     def close(self):
#         self.client.close()
#         logger.info("InfluxDB client connection closed.")

import pandas as pd
from pandas import DataFrame, concat
from typing import List, Optional, Dict, Union, Any
from influxdb_client import InfluxDBClient
from influxdb_client.rest import ApiException
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class InfluxDBDataHandler:
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
        post_pivot_filters: Optional[List[str]] = None,  # 新增：pivot后过滤
        limit: Optional[int] = None,
        pivot: bool = False,
        merge_result: bool = True
    ) -> DataFrame:
        start_str = self._format_time_param(start)
        stop_str = self._format_time_param(stop)

        flux_query_parts = [
            f'from(bucket: "{self._escape_flux_string(self.bucket)}")',
            f'|> range(start: {start_str}, stop: {stop_str})'
        ]
        
        if isinstance(measurement, str):
            flux_query_parts.append(f'|> filter(fn: (r) => r["_measurement"] == "{self._escape_flux_string(measurement)}")')
        else:
            m_filter = " or ".join(
                [f'r["_measurement"] == "{self._escape_flux_string(m)}"' for m in measurement]
            )
            flux_query_parts.append(f'|> filter(fn: (r) => ({m_filter}))')

        if fields:
            field_filters = " or ".join([f'r["_field"] == "{self._escape_flux_string(field)}"' for field in fields])
            flux_query_parts.append(f'|> filter(fn: (r) => ({field_filters}))')

        if tags:
            for key, value in tags.items():
                if isinstance(value, list):
                    tag_expr = " or ".join(
                        [f'r["{self._escape_flux_string(key)}"] == "{self._escape_flux_string(str(v))}"' for v in value]
                    )
                    flux_query_parts.append(f'|> filter(fn: (r) => ({tag_expr}))')
                else:
                    flux_query_parts.append(f'|> filter(fn: (r) => r["{self._escape_flux_string(key)}"] == "{self._escape_flux_string(str(value))}")')

        # 修复：区分pivot前后的filters
        if filters:
            # filters在pivot之前应用（适用于对_field、_measurement等原始字段的过滤）
            for f in filters:
                flux_query_parts.append(f'|> filter(fn: (r) => ({f}))')

        if aggregation:
            aggregation = aggregation.lower()
            if aggregation not in self.ALLOWED_AGGREGATIONS:
                raise ValueError(f"Unsupported aggregation function: {aggregation}. Allowed: {', '.join(self.ALLOWED_AGGREGATIONS)}")
            if not aggregate_every:
                raise ValueError("Aggregation function requires 'aggregate_every' time interval.")
            flux_query_parts.append(f'|> aggregateWindow(every: {aggregate_every}, fn: {aggregation}, createEmpty: false)')

        if pivot:
            flux_query_parts.append('|> group(columns: ["_measurement", "_field"])')
            flux_query_parts.append('|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")')
            
            # post_pivot_filters在pivot之后应用（适用于对pivot后字段的过滤）
            if post_pivot_filters:
                for f in post_pivot_filters:
                    flux_query_parts.append(f'|> filter(fn: (r) => ({f}))')

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