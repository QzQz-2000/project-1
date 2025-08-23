import logging
from core.data_parser import parse_device_data
from config import MQTT_BROKER, MQTT_PORT

logger = logging.getLogger("mqtt_adapter")

# global kafka instance
kafka_producer_instance = None

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        # subscribe all device data
        logger.info("Connected to MQTT Broker successfully.")
        client.subscribe("devices/+/+/data")
        logger.info("Subscribed to topic: 'devices/+/+/data'")
    else:
        logger.error(f"Failed to connect to MQTT Broker, return code: {rc}")

def on_message(client, userdata, msg):
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

    # parse the data
    parsed_data = parse_device_data(
        payload,  
        device_id=device_id,
        environment_id=environment_id,
        source_protocol='mqtt'
    )
    
    if parsed_data:
        try:
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
    # kafka instance
    global kafka_producer_instance
    kafka_producer_instance = kp
    
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        logger.info(f"Connecting to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
        client.loop_forever()
    except Exception as e:
        logger.critical(f"MQTT Adapter fatal error: {e}")
