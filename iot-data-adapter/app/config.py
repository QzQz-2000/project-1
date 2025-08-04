import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telemetry")

MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto")   # 默认改成mosquitto容器名，容器内访问用服务名
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))

HTTP_HOST = os.getenv("HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.getenv("HTTP_PORT", 5000))
