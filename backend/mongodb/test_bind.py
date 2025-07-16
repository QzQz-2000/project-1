import asyncio
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
from aiokafka import AIOKafkaConsumer

app = FastAPI()


# TODO: 建立链接前，需要添加env_id, 需要检查env、twin、device存不存在，还有链接是否存在
# TODO: 解绑关系，删除数据库中的链接，检查是否存在，存在的话删除，允许之后建立新的链接
# TODO：建立连接后的操作，触发读取kafka的数据

MONGO_URI = "mongodb://admin:secret@localhost:27017"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "device_data"

@app.on_event("startup")
async def startup():
    mongo_client = AsyncIOMotorClient(MONGO_URI)
    app.state.mongo = mongo_client
    app.state.db = mongo_client["digitaltwins_db"]
    app.state.twin_clients = {}  # twin_id -> WebSocket
    app.state.kafka_task = asyncio.create_task(kafka_consumer_loop())

@app.on_event("shutdown")
async def shutdown():
    app.state.kafka_task.cancel()
    try:
        await app.state.kafka_task
    except asyncio.CancelledError:
        pass
    app.state.mongo.close()

class BindRequest(BaseModel):
    twin_id: str
    device_id: str

@app.post("/bind_twin_device")
async def bind_twin_device(req: BindRequest, request: Request):
    db = request.app.state.db
    await db["twin_device_bindings"].update_one(
        {"twin_id": req.twin_id},
        {"$set": {"device_id": req.device_id}},
        upsert=True,
    )
    return {"message": "绑定成功"}

@app.websocket("/ws/{twin_id}")
async def websocket_endpoint(websocket: WebSocket, twin_id: str):
    await websocket.accept()
    app.state.twin_clients[twin_id] = websocket
    try:
        while True:
            await websocket.receive_text()  # 心跳维持连接
    except WebSocketDisconnect:
        app.state.twin_clients.pop(twin_id, None)

async def kafka_consumer_loop():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="twin_consumer_group",
        auto_offset_reset="latest",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            try:
                data = json.loads(msg.value.decode())
                await handle_device_data(data)
            except Exception as e:
                print("消息处理异常：", e)
    finally:
        await consumer.stop()

async def handle_device_data(data: dict):
    device_id = data.get("device_id")
    timestamp = data.get("timestamp")
    if not (device_id and timestamp):
        return

    db = app.state.db

    # 更新 MongoDB：仅当时间戳更大时才更新
    await db["latest_state"].update_one(
        {"device_id": device_id},
        {
            "$max": {"timestamp": timestamp},
            "$set": data
        },
        upsert=True
    )

    # 查找绑定的 twin_id
    cursor = db["twin_device_bindings"].find({"device_id": device_id})
    twins = [doc["twin_id"] async for doc in cursor]

    for twin_id in twins:
        ws = app.state.twin_clients.get(twin_id)
        if ws:
            try:
                await ws.send_text(json.dumps(data))
            except:
                app.state.twin_clients.pop(twin_id, None)
