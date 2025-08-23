import json
from datetime import datetime, timezone

def parse_device_data(raw_data, source_protocol='unknown', device_id=None, environment_id=None):

    parsed_data = {
        "timestamp": None,
        "device_id": device_id,
        "environment_id": environment_id,
        "payload": {}
    }

    try:
        payload = {}
        if isinstance(raw_data, str):

            try:
                payload = json.loads(raw_data)
            except json.JSONDecodeError:

                if source_protocol == 'mqtt':
                    try:
                        value = float(raw_data)
                        payload = {"value": value}
                    except ValueError:
                        payload = {"value": raw_data}
                else:
                    print(f"Error decoding JSON from {source_protocol} data: {raw_data}")
                    return None
        elif isinstance(raw_data, dict):
            payload = raw_data.copy()
        else:
            print(f"Unsupported raw data type: {type(raw_data)}")
            return None

        ts = payload.pop("timestamp", None)
        if ts:
            try:
                if isinstance(ts, (int, float)):
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

        # parse data timestamp
        parsed_data["timestamp"] = dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        if not parsed_data["device_id"] and "device_id" in payload:
            parsed_data["device_id"] = str(payload.pop("device_id"))
        if not parsed_data["environment_id"] and "environment_id" in payload:
            parsed_data["environment_id"] = str(payload.pop("environment_id"))

        if not parsed_data["device_id"]:
            print(f"Missing device_id in {source_protocol} data")
            return None
        if not parsed_data["environment_id"]:
            print(f"Missing environment_id in {source_protocol} data")
            return None
        
        parsed_data["payload"] = payload
        return parsed_data

    except Exception as e:
        print(f"Error parsing data from {source_protocol}: {e}")
        return None