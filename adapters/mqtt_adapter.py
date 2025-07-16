# mqtt_adapter.py

import paho.mqtt.client as mqtt
from common_data_parser import parse_device_data
from kafka_producer_client import KafkaProducerClient
import time
import threading
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# TODO: 配置写到配置文件
kafka_producer = KafkaProducerClient(topic='device_data_raw')

def on_connect(client, userdata, flags, rc):
    """
    MQTT 连接成功回调。
    """
    if rc == 0:
        logger.info("MQTT Adapter connected to broker!")
        # 订阅所有以 'devices/' 开头的主题
        # 假设设备会发布数据到 'devices/<device_id>/data'
        client.subscribe("devices/+/data", 0)
        logger.info("Subscribed to topic: devices/+/data")
    else:
        logger.error(f"Failed to connect to MQTT broker, return code {rc}\n")

def on_message(client, userdata, msg):
    """
    接收到 MQTT 消息回调。
    """
    topic = msg.topic
    payload = msg.payload.decode('utf-8')
    
    logger.info(f"Received MQTT message from topic '{topic}': {payload}")

    # 尝试从 topic 中提取 device_id
    device_id = None
    parts = topic.split('/')
    if len(parts) >= 2 and parts[0] == 'devices':
        device_id = parts[1]

    parsed_data = parse_device_data(payload, source_protocol='mqtt', device_id=device_id)

    if parsed_data:
        try:
            kafka_producer.send_data(parsed_data)
            logger.info(f"Successfully processed MQTT data for device {device_id} and sent to Kafka.")
        except Exception as e:
            logger.error(f"Error sending parsed MQTT data to Kafka for {device_id}: {e}")
    else:
        logger.warning(f"Failed to parse MQTT data from {topic}")

def run_mqtt_adapter(broker_address="localhost", broker_port=1883):
    """
    启动 MQTT Adapter。
    """
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(broker_address, broker_port, 60)
        logger.info(f"Connecting to MQTT broker at {broker_address}:{broker_port}")
        client.loop_forever() # 保持连接并处理消息
    except Exception as e:
        logger.critical(f"MQTT Adapter fatal error: {e}")

if __name__ == '__main__':
    run_mqtt_adapter()