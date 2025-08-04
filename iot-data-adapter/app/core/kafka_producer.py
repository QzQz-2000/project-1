from confluent_kafka import Producer
import json
import socket
from config import KAFKA_BOOTSTRAP_SERVERS

class KafkaProducerClient:
    def __init__(self, bootstrap_servers, topic='Telemetry'):
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': socket.gethostname()
        }
        self.producer = Producer(conf)
        self.topic = topic
        print(f"Kafka Producer initialized for topic: {self.topic}, servers: {bootstrap_servers}")

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to topic '{msg.topic()}' [{msg.partition()}] at offset {msg.offset()}")

    def send_data(self, data):
        try:
            # 生成更具描述性的key，包含environment_id和device_id
            environment_id = data.get("environment_id", "unknown")
            device_id = data.get("device_id", "unknown")
            
            # 使用environment_id和device_id组合作为key，确保同一设备的消息在同一分区
            key = f"{environment_id}:{device_id}".encode('utf-8')
            value = json.dumps(data).encode('utf-8')
            
            self.producer.produce(
                self.topic, 
                key=key, 
                value=value, 
                callback=self.delivery_report
            )
            self.producer.poll(0)
            
        except BufferError:
            print("Local producer queue is full. Retrying...")
            self.producer.poll(1)
            self.producer.produce(
                self.topic, 
                key=key, 
                value=value, 
                callback=self.delivery_report
            )
        except Exception as e:
            print(f"Failed to send message to Kafka: {e}")

    def flush(self):
        self.producer.flush()
        print("Kafka Producer flushed.")