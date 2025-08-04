from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from core.data_parser import parse_device_data
from core.kafka_producer import KafkaProducerClient
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
import logging

logger = logging.getLogger("http_adapter")
router = APIRouter()

kafka_producer = KafkaProducerClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC)

@router.post("/data/http/{environment_id}/{device_id}", summary="接收HTTP设备数据")
async def receive_http_data(environment_id: str, device_id: str, request: Request):
    try:
        raw_data = await request.json()
    except Exception:
        raw_data = (await request.body()).decode('utf-8')
        logger.warning(f"Received non-JSON HTTP data from {environment_id}/{device_id}: {raw_data}")

    logger.info(f"Received HTTP data from {environment_id}/{device_id}: {raw_data}")

    parsed_data = parse_device_data(
        raw_data, 
        device_id=device_id,
        environment_id=environment_id
    )
    
    if parsed_data:
        try:
            kafka_producer.send_data(parsed_data)
            logger.info(f"Processed HTTP data for device {environment_id}/{device_id} and sent to Kafka.")
            return JSONResponse(
                status_code=200, 
                content={
                    "status": "success", 
                    "message": "Data received and sent to Kafka",
                    "environment_id": environment_id,
                    "device_id": device_id
                }
            )
        except Exception as e:
            logger.error(f"Error sending parsed data to Kafka for {environment_id}/{device_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Internal server error: {e}")
    else:
        logger.warning(f"Failed to parse data from {environment_id}/{device_id}")
        raise HTTPException(status_code=400, detail="Failed to parse incoming data. Missing required fields or invalid format.")