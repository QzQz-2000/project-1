import time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# 连接配置（替换成你的环境变量或直接赋值）
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "mytoken"
INFLUXDB_ORG = "myorg"
INFLUXDB_BUCKET = "mybucket"

def write_test_data():
    # 创建客户端
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)

    write_api = client.write_api(write_options=SYNCHRONOUS)

    # 写入几条模拟温度和湿度数据
    points = []
    timestamp = int(time.time())  # 当前时间戳，单位秒

    # 模拟 5 条数据，每隔 10 秒，随机温度和湿度
    import random
    for i in range(5):
        point = (
            Point("home")
            .tag("room", "Living Room")
            .field("temp", 20 + random.uniform(0, 5))   # 20 ~ 25 度
            .field("hum", 30 + random.uniform(0, 10))   # 30 ~ 40 %
            .time(timestamp + i * 10, WritePrecision.S)
        )
        points.append(point)

    # 批量写入
    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)
    print("写入完成！")

if __name__ == "__main__":
    write_test_data()
