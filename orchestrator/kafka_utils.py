import asyncio
import json
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092" # Kafka 服务的地址，在 Docker Compose 中定义

async def get_kafka_producer():
    """
    提供一个异步 Kafka 生产者实例，用于依赖注入。
    """
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()

async def consume_messages(topic: str, group_id: str, handler):
    """
    一个通用的 Kafka 消息消费者。
    Args:
        topic (str): 要订阅的 Kafka 主题。
        group_id (str): 消费者组 ID。
        handler (callable): 处理接收到消息的异步函数。
    """
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset="earliest" # 从最早的可用偏移量开始消费
    )
    await consumer.start()
    try:
        async for msg in consumer:
            print(f"Consumed message from {topic} (Offset: {msg.offset}): {msg.value.decode()}")
            try:
                # 尝试解析 JSON 消息
                data = json.loads(msg.value.decode())
                await handler(data)
            except json.JSONDecodeError:
                print(f"Error decoding JSON from message: {msg.value.decode()}")
            except Exception as e:
                print(f"Error processing message in handler: {e}")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    # 这是一个简单的测试示例，通常不会直接运行此文件
    # 消费者通常在 FastAPI 应用的 startup 事件中启动
    async def test_handler(data):
        print(f"Test Handler received: {data}")

    async def main():
        # 启动一个测试消费者（这将一直运行直到停止）
        # asyncio.create_task(consume_messages("test_topic", "test_group", test_handler))
        # print("Test consumer started. Send messages to 'test_topic' to see output.")

        # 发送一条测试消息
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        await producer.start()
        try:
            test_message = {"hello": "world", "from": "kafka_utils_test"}
            await producer.send_and_wait("test_topic", json.dumps(test_message).encode())
            print("Sent test message.")
        finally:
            await producer.stop()
        await asyncio.sleep(5) # 给消费者一些时间来处理

    # asyncio.run(main())
    print("This file is primarily for utility functions and not meant for direct execution.")
    print("Consumers are typically started within FastAPI app startup events.")