import requests
import paho.mqtt.client as mqtt
import json
import time

# 配置
HTTP_URL = "http://localhost:5000/data/http/env1/device123"
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "devices/env1/device456/data"

# 测试数据
http_payload = {
    "timestamp": "2025-08-03T12:34:56Z",
    "device_id": "abc123",
    "environment_id": "env1",
    "temperature": 28.3
}

mqtt_payload = {
    "temperature": 26.1,
    "humidity": 52,
    "custom": "mqtt_test"
}

# ========= 发送 HTTP 数据 ============
def send_http():
    print("🚀 Sending HTTP data...")
    try:
        response = requests.post(HTTP_URL, json=http_payload, timeout=5)
        print(f"✅ HTTP Status: {response.status_code}")
        print(f"✅ HTTP Response: {response.json()}")
    except Exception as e:
        print(f"❌ HTTP Error: {e}")

# ========= 发送 MQTT 数据 ============
def send_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("🔌 Connected to MQTT broker!")
            result = client.publish(MQTT_TOPIC, json.dumps(mqtt_payload))
            if result[0] == 0:
                print(f"✅ Sent MQTT message to topic `{MQTT_TOPIC}`")
            else:
                print(f"❌ Failed to send MQTT message: {result}")
            client.disconnect()
        else:
            print(f"❌ MQTT connection failed with code {rc}")

    client = mqtt.Client()
    client.on_connect = on_connect
    try:
        print("🚀 Sending MQTT data...")
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_start()
        time.sleep(2)
        client.loop_stop()
    except Exception as e:
        print(f"❌ MQTT Error: {e}")

# ========= 执行测试 ============
if __name__ == "__main__":
    send_http()
    print("-" * 40)
    send_mqtt()
