import threading
import logging
from fastapi import FastAPI
from adapters import http_adapter, mqtt_adapter
from config import HTTP_HOST, HTTP_PORT

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

app = FastAPI(
    title="Digital Twin Data Collection Service",
    description="支持HTTP和MQTT设备数据采集并转发至Kafka",
    version="1.0.0"
)

app.include_router(http_adapter.router)

def run_mqtt():
    mqtt_adapter.start_mqtt_loop()

if __name__ == '__main__':
    import uvicorn
    # MQTT启动在单独线程
    threading.Thread(target=run_mqtt, daemon=True).start()
    logger.info("Starting HTTP server...")
    uvicorn.run(app, host=HTTP_HOST, port=HTTP_PORT)
