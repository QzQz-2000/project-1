from confluent_kafka import Producer
import json
import socket

# TODO: 配置写入配置文件
class KafkaProducerClient:
    def __init__(self, bootstrap_servers='localhost:9092', topic='device_data_raw'):
        """
        初始化 Kafka 生产者。

        Args:
            bootstrap_servers (str): Kafka brokers 地址。
            topic (str): 数据将发送到的 Kafka 主题。
        """
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': socket.gethostname()
        }
        self.producer = Producer(conf)
        self.topic = topic
        print(f"Kafka Producer initialized for topic: {self.topic}, servers: {bootstrap_servers}")

    def delivery_report(self, err, msg):
        """
        消息发送回调函数。
        """
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to topic '{msg.topic()}' [{msg.partition()}] at offset {msg.offset()}")

    def send_data(self, data):
        """
        发送解析后的数据到 Kafka。

        Args:
            data (dict): 统一格式的解析后数据。
        """
        try:
            # 使用 device_id 作为 key，确保同一设备的数据发送到同一分区（如果分区策略支持）
            key = data.get("device_id", "unknown").encode('utf-8')
            value = json.dumps(data).encode('utf-8')
            self.producer.produce(self.topic, key=key, value=value, callback=self.delivery_report)
            self.producer.poll(0) # 触发回调
        except BufferError:
            print("Local producer queue is full. Retrying...")
            self.producer.poll(1) # 等待缓冲区可用
            self.producer.produce(self.topic, key=key, value=value, callback=self.delivery_report)
        except Exception as e:
            print(f"Failed to send message to Kafka: {e}")

    def flush(self):
        """
        等待所有待发送消息发送完毕。
        """
        self.producer.flush()
        print("Kafka Producer flushed.")