# http_adapter.py

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from common_data_parser import parse_device_data
from kafka_producer_client import KafkaProducerClient
import uvicorn
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Digital Twin Data Collection HTTP Adapter",
    description="接收来自HTTP设备的实时数据并转发至Kafka。",
    version="1.0.0"
)

# 初始化 Kafka 生产者
kafka_producer = KafkaProducerClient(topic='device_data_raw') # 假设Kafka运行在本地9092端口

# 定义数据模型，使用 Pydantic 获得自动验证和文档
class DeviceDataPayload(BaseModel):
    """
    设备数据载荷的 Pydantic 模型。
    可以根据实际设备数据结构进行扩展。
    """
    # 允许 payload 包含任意字段，但最好定义具体字段进行验证
    # 例如：temperature: float, humidity: float, etc.
    # 这里为了通用性，使用 extra=Extra.allow
    model_config = {
        "extra": "allow" # 允许模型包含未在 Pydantic 中定义的额外字段
    }

    # 示例字段（可选，如果数据结构固定，强烈建议定义）
    # temperature: float = Field(..., description="设备温度")
    # humidity: float = Field(..., description="设备湿度")

@app.on_event("startup")
async def startup_event():
    """
    FastAPI 应用启动时执行的事件。
    可以在这里进行一些初始化操作，例如数据库连接等。
    """
    logger.info("HTTP Adapter application starting up...")

@app.on_event("shutdown")
async def shutdown_event():
    """
    FastAPI 应用关闭时执行的事件。
    确保在关闭前刷新 Kafka 生产者，防止数据丢失。
    """
    logger.info("HTTP Adapter application shutting down...")
    if kafka_producer:
        kafka_producer.flush()
        logger.info("Kafka Producer flushed during shutdown.")


@app.post("/data/http/{device_id}", summary="接收HTTP设备数据", response_description="数据接收状态")
async def receive_http_data(
    device_id: str,
    request: Request
):
    """
    **接收来自指定`device_id`的HTTP POST数据。**

    此端点用于接收各种HTTP设备上传的JSON格式数据。
    - **`device_id`**: 设备的唯一标识符，将从URL路径中获取。
    - **请求体**: 设备的原始数据，应为JSON格式。

    **返回值:**
    - `200 OK`: 数据成功接收并转发到Kafka。
    - `400 Bad Request`: 数据解析失败或请求格式不正确。
    - `500 Internal Server Error`: 服务器内部错误。
    """
    try:
        # 尝试获取 JSON 数据
        raw_data = await request.json()
    except Exception:
        # 如果不是有效的 JSON，尝试获取原始文本数据
        raw_data = (await request.body()).decode('utf-8')
        logger.warning(f"Received non-JSON HTTP data from {device_id}: {raw_data}")


    logger.info(f"Received HTTP data from {device_id}: {raw_data}")

    parsed_data = parse_device_data(raw_data, source_protocol='http', device_id=device_id)

    if parsed_data:
        try:
            kafka_producer.send_data(parsed_data)
            logger.info(f"Successfully processed data for device {device_id} and sent to Kafka.")
            return JSONResponse(
                status_code=200,
                content={"status": "success", "message": "Data received and sent to Kafka"}
            )
        except Exception as e:
            logger.error(f"Error sending parsed data to Kafka for {device_id}: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Internal server error: Failed to send data to Kafka: {str(e)}"
            )
    else:
        logger.warning(f"Failed to parse data from {device_id}: {raw_data}")
        raise HTTPException(
            status_code=400,
            detail="Failed to parse incoming data. Ensure correct JSON format."
        )

# 如果直接运行此文件，则启动 FastAPI 应用
if __name__ == '__main__':
    # 推荐使用 uvicorn.run 来运行 FastAPI 应用
    # host='0.0.0.0' 允许从任何网络接口访问
    # port=5000 是你选择的端口
    # reload=True 在开发环境下很有用，代码更改时会自动重启服务
    uvicorn.run(app, host="0.0.0.0", port=5000, log_level="info")