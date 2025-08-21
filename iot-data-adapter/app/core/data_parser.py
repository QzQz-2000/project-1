import json
from datetime import datetime, timezone

def parse_device_data(raw_data, source_protocol='unknown', device_id=None, environment_id=None):
    """
    解析设备数据，使用 ISO 8601 格式的 UTC 时间戳（Z格式）。
    如果 payload 中有 timestamp，则使用它，并从 payload 中删除。
    """
    parsed_data = {
        "timestamp": None,
        "device_id": device_id,
        "environment_id": environment_id,
        "payload": {}
    }

    try:
        payload = {}
        if isinstance(raw_data, str):
            # 尝试 JSON 解析
            try:
                payload = json.loads(raw_data)
            except json.JSONDecodeError:
                # 非JSON字符串的兜底处理
                if source_protocol == 'mqtt':
                    # 例如，处理 "101.5" 这种纯数值数据
                    try:
                        value = float(raw_data)
                        payload = {"value": value}
                    except ValueError:
                        # 如果不是数值，作为原始字符串存入
                        payload = {"value": raw_data}
                else:
                    # 对于其他协议，如果无法解析为JSON，则失败
                    print(f"Error decoding JSON from {source_protocol} data: {raw_data}")
                    return None
        elif isinstance(raw_data, dict):
            payload = raw_data.copy()
        else:
            print(f"Unsupported raw data type: {type(raw_data)}")
            return None

        # 从 payload 中提取并删除 timestamp
        ts = payload.pop("timestamp", None)
        if ts:
            try:
                if isinstance(ts, (int, float)):
                    # 假设是毫秒时间戳
                    dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                elif isinstance(ts, str):
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                else:
                    raise ValueError("Unsupported timestamp format")
            except Exception as e:
                print(f"Invalid timestamp in payload: {ts}, error: {e}")
                dt = datetime.now(timezone.utc)
        else:
            dt = datetime.now(timezone.utc)

        parsed_data["timestamp"] = dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        # 从 payload 中提取并删除 device_id / environment_id
        if not parsed_data["device_id"] and "device_id" in payload:
            parsed_data["device_id"] = str(payload.pop("device_id"))
        if not parsed_data["environment_id"] and "environment_id" in payload:
            parsed_data["environment_id"] = str(payload.pop("environment_id"))

        # 验证字段
        if not parsed_data["device_id"]:
            print(f"Missing device_id in {source_protocol} data")
            return None
        if not parsed_data["environment_id"]:
            print(f"Missing environment_id in {source_protocol} data")
            return None
        
        # 将剩余的 payload 赋值给解析数据
        parsed_data["payload"] = payload
        return parsed_data

    except Exception as e:
        print(f"Error parsing data from {source_protocol}: {e}")
        return None