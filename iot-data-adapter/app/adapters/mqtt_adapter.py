# import paho.mqtt.client as mqtt
# import logging
# from core.data_parser import parse_device_data
# from core.kafka_producer import KafkaProducerClient
# from config import MQTT_BROKER, MQTT_PORT, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

# logger = logging.getLogger("mqtt_adapter")

# kafka_producer = KafkaProducerClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC)

# def on_connect(client, userdata, flags, rc):
#     if rc == 0:
#         logger.info("MQTT Adapter connected to broker!")
        
#         # 只订阅新的topic格式：devices/{environment_id}/{device_id}/data
#         client.subscribe("devices/+/+/data", 0)
#         logger.info("Subscribed to topic: devices/+/+/data")
        
#     else:
#         logger.error(f"Failed to connect to MQTT broker, return code {rc}")

# def on_message(client, userdata, msg):
#     topic = msg.topic
#     payload = msg.payload.decode('utf-8')
#     logger.info(f"Received MQTT message from topic '{topic}': {payload}")

#     parts = topic.split('/')
    
#     # 解析格式：devices/{environment_id}/{device_id}/data
#     if len(parts) == 4 and parts[0] == 'devices' and parts[3] == 'data':
#         environment_id = parts[1]
#         device_id = parts[2]
#         logger.info(f"Parsed topic - Environment: {environment_id}, Device: {device_id}")
#     else:
#         logger.error(f"Invalid topic format: {topic}. Expected: devices/{environment_id}/{device_id}/data")
#         return

#     # 解析数据并发送到Kafka
#     parsed_data = parse_device_data(
#         payload,  
#         device_id=device_id,
#         environment_id=environment_id
#     )
    
#     if parsed_data:
#         try:
#             kafka_producer.send_data(parsed_data)
#             logger.info(f"Processed MQTT data for device {environment_id}/{device_id} and sent to Kafka.")
#         except Exception as e:
#             logger.error(f"Error sending parsed MQTT data to Kafka for {environment_id}/{device_id}: {e}")
#     else:
#         logger.warning(f"Failed to parse MQTT data from {topic}")

# def start_mqtt_loop():
#     client = mqtt.Client()
#     client.on_connect = on_connect
#     client.on_message = on_message
#     try:
#         client.connect(MQTT_BROKER, MQTT_PORT, 60)
#         logger.info(f"Connecting to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
#         client.loop_forever()
#     except Exception as e:
#         logger.critical(f"MQTT Adapter fatal error: {e}")

# mqtt_adapter.py
import paho.mqtt.client as mqtt
import logging
from core.data_parser import parse_device_data
from config import MQTT_BROKER, MQTT_PORT

logger = logging.getLogger("mqtt_adapter")

# 声明全局变量，用于在 on_message 中访问 Kafka Producer 实例
kafka_producer_instance = None

def on_connect(client, userdata, flags, rc):
    """当客户端连接到 MQTT Broker 时调用。"""
    if rc == 0:
        logger.info("Connected to MQTT Broker successfully.")
        # 订阅所有设备数据主题
        client.subscribe("devices/+/+/data")
        logger.info("Subscribed to topic: 'devices/+/+/data'")
    else:
        logger.error(f"Failed to connect to MQTT Broker, return code: {rc}")

def on_message(client, userdata, msg):
    """当收到 MQTT 消息时调用。"""
    topic = msg.topic
    try:
        payload = msg.payload.decode('utf-8')
        logger.info(f"Received MQTT message from topic '{topic}': {payload}")
    except UnicodeDecodeError:
        logger.error(f"Failed to decode MQTT payload from topic '{topic}'. Assuming binary data.")
        return

    parts = topic.split('/')
    
    if len(parts) == 4 and parts[0] == 'devices' and parts[3] == 'data':
        environment_id = parts[1]
        device_id = parts[2]
        logger.info(f"Parsed topic - Environment: {environment_id}, Device: {device_id}")
    else:
        logger.error(f"Invalid topic format: {topic}. Expected: devices/{environment_id}/{device_id}/data")
        return

    parsed_data = parse_device_data(
        payload,  
        device_id=device_id,
        environment_id=environment_id,
        source_protocol='mqtt' # 新增参数
    )
    
    if parsed_data:
        try:
            # 使用全局变量访问 kafka_producer_instance
            if kafka_producer_instance:
                kafka_producer_instance.send_data(parsed_data)
                logger.info(f"Processed MQTT data for device {environment_id}/{device_id} and sent to Kafka.")
            else:
                logger.error("Kafka Producer instance is not initialized.")
        except Exception as e:
            logger.error(f"Error sending parsed MQTT data to Kafka for {environment_id}/{device_id}: {e}")
    else:
        logger.warning(f"Failed to parse MQTT data from {topic}")

def start_mqtt_loop(kp):
    """
    启动 MQTT 循环，并接收 Kafka Producer 实例。
    """
    global kafka_producer_instance
    kafka_producer_instance = kp # 将传入的实例赋值给全局变量
    
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        logger.info(f"Connecting to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
        client.loop_forever()
    except Exception as e:
        logger.critical(f"MQTT Adapter fatal error: {e}")