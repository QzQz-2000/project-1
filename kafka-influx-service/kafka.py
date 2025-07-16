# producer.py
import asyncio
import json
import random
from datetime import datetime
from aiokafka import AIOKafkaProducer
from config import settings # 导入你的配置

async def send_telemetry_data():
    producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        device_ids = ["device_001", "device_002", "device_003"]
        locations = ["lab_a", "factory_floor", "warehouse"]

        while True:
            device_id = random.choice(device_ids)
            location = random.choice(locations)
            
            # 模拟遥测数据，符合 Telemetry 模型
            telemetry_data = {
                "timestamp": datetime.utcnow().isoformat() + "Z", # ISO 8601 格式
                "device_id": device_id,
                "location": location,
                "properties": {
                    "temperature": round(random.uniform(20.0, 30.0), 2),
                    "humidity": round(random.uniform(40.0, 60.0), 2),
                    "pressure": round(random.uniform(900.0, 1100.0), 2),
                    "battery_level": random.randint(0, 100),
                    "is_active": random.choice([True, False])
                }
            }
            
            message_value = json.dumps(telemetry_data).encode("utf-8")
            
            await producer.send_and_wait(settings.KAFKA_TOPIC, message_value)
            print(f"Sent: {telemetry_data}")
            await asyncio.sleep(1) # 每秒发送一条消息
    except asyncio.CancelledError:
        print("Producer task cancelled.")
    finally:
        await producer.stop()
        print("Producer stopped.")

if __name__ == "__main__":
    # 安装 aiokafka
    # pip install aiokafka
    try:
        asyncio.run(send_telemetry_data())
    except KeyboardInterrupt:
        print("Producer interrupted by user.")
