# common_data_parser.py

def parse_device_data(raw_data, source_protocol, device_id=None):
    """
    统一解析不同协议的原始设备数据。
    这里只是一个示例解析逻辑，实际应用中会更复杂。

    Args:
        raw_data (str or dict): 原始数据，可能是JSON字符串或字典。
        source_protocol (str): 数据来源协议 (e.g., 'http', 'mqtt').
        device_id (str, optional): 如果能从协议层获取，可直接传入。

    Returns:
        dict: 统一格式的解析后数据，如果解析失败返回None。
    """
    import json
    import time

    parsed_data = {
        "timestamp": int(time.time() * 1000),  # 毫秒级时间戳
        "source_protocol": source_protocol,
        "device_id": device_id,
        "payload": {}
    }

    try:
        if isinstance(raw_data, str):
            payload = json.loads(raw_data)
        elif isinstance(raw_data, dict):
            payload = raw_data
        else:
            print(f"Unsupported raw data type: {type(raw_data)}")
            return None

        # 示例：尝试从payload中提取 device_id，如果外部未提供
        if not parsed_data["device_id"] and "device_id" in payload:
            parsed_data["device_id"] = str(payload.pop("device_id")) # 从payload中移除，避免重复

        # 实际应用中，这里会根据不同的设备类型和数据格式进行更复杂的解析和标准化
        # 例如，可以根据 device_id 或 payload 中的某个字段来查找对应的解析规则
        parsed_data["payload"] = payload

        return parsed_data

    except json.JSONDecodeError:
        print(f"Error decoding JSON from {source_protocol} data: {raw_data}")
        return None
    except Exception as e:
        print(f"Error parsing data from {source_protocol}: {e}")
        return None