import threading
import logging
from fastapi import FastAPI
from core.kafka_producer import KafkaProducerClient
from config import HTTP_HOST, HTTP_PORT, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
import uvicorn
from adapters import http_adapter, mqtt_adapter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

kafka_producer = KafkaProducerClient(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    topic=KAFKA_TOPIC
)

app = FastAPI(
    title="Digital Twin Data Collection Service",
    description="HTTP and MQTT adapters",
    version="1.0.0"
)

http_adapter.router.kafka_producer = kafka_producer

app.include_router(http_adapter.router)

def run_mqtt_adapter(kp):

    mqtt_adapter.start_mqtt_loop(kp)

if __name__ == '__main__':
    
    threading.Thread(target=run_mqtt_adapter, args=(kafka_producer,), daemon=True).start()
    
    logger.info("Starting HTTP server...")
    uvicorn.run(app, host=HTTP_HOST, port=HTTP_PORT)