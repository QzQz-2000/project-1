import os
import time
import random
import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# InfluxDB 配置
url = "http://localhost:8086"
token = "mytoken"  # 替换为你的 token
org = "myorg"      # 替换为你的组织名称
bucket = "mybucket"  # 替换为你的 bucket 名称

# 创建 InfluxDB 客户端
client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

# 模拟的设备列表
devices = ["device-1", "device-2", "device-3"]

def generate_and_write_data():
    """生成并写入模拟数据"""
    data_points = []
    
    # 获取当前时间
    current_time = datetime.datetime.utcnow().isoformat()
    
    for device_id in devices:
        # 为每个设备生成随机数据
        temperature = round(random.uniform(20.0, 30.0), 2)
        humidity = round(random.uniform(40.0, 60.0), 2)
        
        print(f"[{current_time}] Writing data for {device_id}: Temperature={temperature}°C, Humidity={humidity}%")
        
        # 创建数据点对象
        temp_point = (
            Point("telemetry")  # 测量名称
            .tag("device_id", device_id)
            .field("temperature", temperature)
            .time(current_time)
        )
        
        humidity_point = (
            Point("telemetry") # 测量名称
            .tag("device_id", device_id)
            .field("humidity", humidity)
            .time(current_time)
        )
        
        data_points.append(temp_point)
        data_points.append(humidity_point)

    # 批量写入数据
    write_api.write(bucket=bucket, org=org, record=data_points)
    print("Data written successfully.")

if __name__ == "__main__":
    print("Starting data generation and writing...")
    try:
        while True:
            generate_and_write_data()
            time.sleep(2)  # 每5秒写入一次
    except KeyboardInterrupt:
        print("\nStopping script.")
    finally:
        client.close()