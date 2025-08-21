# import threading
# import logging
# from fastapi import FastAPI
# from adapters import http_adapter, mqtt_adapter
# from config import HTTP_HOST, HTTP_PORT

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger("main")

# app = FastAPI(
#     title="Digital Twin Data Collection Service",
#     description="支持HTTP和MQTT设备数据采集并转发至Kafka",
#     version="1.0.0"
# )

# app.include_router(http_adapter.router)

# def run_mqtt():
#     mqtt_adapter.start_mqtt_loop()

# if __name__ == '__main__':
#     import uvicorn
#     # MQTT启动在单独线程
#     threading.Thread(target=run_mqtt, daemon=True).start()
#     logger.info("Starting HTTP server...")
#     uvicorn.run(app, host=HTTP_HOST, port=HTTP_PORT)

# main.py
import threading
import logging
from fastapi import FastAPI
from core.kafka_producer import KafkaProducerClient
from config import HTTP_HOST, HTTP_PORT, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

# 将 adapters 作为模块导入
from adapters import http_adapter, mqtt_adapter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

# 在这里只初始化一次 Kafka Producer
kafka_producer = KafkaProducerClient(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    topic=KAFKA_TOPIC
)

# FastAPI 应用程序
app = FastAPI(
    title="Digital Twin Data Collection Service",
    description="支持HTTP和MQTT设备数据采集并转发至Kafka",
    version="1.0.0"
)

# 将 kafka_producer 实例附加到 http_adapter 的 router 上
# 这样在 http_adapter 中可以通过 router.kafka_producer 访问到
http_adapter.router.kafka_producer = kafka_producer

# 将 HTTP 适配器的路由包含进来
app.include_router(http_adapter.router)

def run_mqtt_adapter(kp):
    """
    启动 MQTT 适配器，并传入 Kafka Producer 实例。
    """
    mqtt_adapter.start_mqtt_loop(kp)

if __name__ == '__main__':
    import uvicorn
    
    # 在单独的线程中启动 MQTT 适配器
    # 将 kafka_producer 实例作为参数传递
    threading.Thread(target=run_mqtt_adapter, args=(kafka_producer,), daemon=True).start()
    
    logger.info("Starting HTTP server...")
    uvicorn.run(app, host=HTTP_HOST, port=HTTP_PORT)