from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from core.data_parser import parse_device_data
import logging

logger = logging.getLogger("http_adapter")
router = APIRouter()

@router.post("/data/http/{environment_id}/{device_id}", summary="http adapters")
async def receive_http_data(environment_id: str, device_id: str, request: Request):
    # kafka instance
    kafka_producer = router.kafka_producer
    
    try:
        raw_data = await request.json()
    except Exception:
        raw_data = (await request.body()).decode('utf-8')
        logger.warning(f"Received non-JSON HTTP data from {environment_id}/{device_id}: {raw_data}")

    logger.info(f"Received HTTP data from {environment_id}/{device_id}: {raw_data}")

    # transform the data
    parsed_data = parse_device_data(
        raw_data, 
        device_id=device_id,
        environment_id=environment_id,
        source_protocol='http'
    )
    
    # send to kafka
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

