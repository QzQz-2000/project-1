# 标准库
import os
import re
import json
import logging
import yaml
from datetime import datetime
from typing import Optional, List, Dict, Any, Union, Set
from enum import Enum
from contextlib import asynccontextmanager

# FastAPI 相关
from fastapi import (
    FastAPI,
    UploadFile,
    File,
    Request,
    status,
    Form,
    HTTPException,
    Query,
    Depends
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.concurrency import run_in_threadpool
from fastapi import APIRouter # <-- 新增导入 APIRouter

# Pydantic 配置
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

# 数据库驱动
from pymongo.errors import DuplicateKeyError
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import DESCENDING # <-- 新增导入 DESCENDING，用于排序
from neo4j import AsyncGraphDatabase
from neo4j.exceptions import Neo4jError
from influxdb_client import InfluxDBClient

# Kafka 异步生产者
from aiokafka import AIOKafkaProducer

# 加载配置文件，配置logging
from dotenv import load_dotenv # <-- 确保导入 load_dotenv
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Settings ---
class Settings(BaseSettings):
    MONGO_URI: str = Field(os.getenv("MONGO_URI", "mongodb://localhost:27017"))
    NEO4J_URI: str = Field(os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    NEO4J_USER: str = Field(os.getenv("NEO4J_USER", "neo4j"))
    NEO4J_PASSWORD: str = Field(os.getenv("NEO4J_PASSWORD", "password"))
    KAFKA_BROKER: str = Field(os.getenv("KAFKA_BROKER", "localhost:9092"))
    INFLUXDB_URL: str = Field(os.getenv("INFLUXDB_URL", "http://localhost:8086"))
    INFLUXDB_TOKEN: str = Field(os.getenv("INFLUXDB_TOKEN", "my-token"))
    INFLUXDB_ORG: str = Field(os.getenv("INFLUXDB_ORG", "my-org"))
    INFLUXDB_BUCKET: str = Field(os.getenv("INFLUXDB_BUCKET", "my-bucket"))

settings = Settings()

# --- Global Regex for Relationship Names ---
REL_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

app = FastAPI(title="Digital Twin Platform API", version="0.1.0")

# --- CORS Middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有来源，生产环境应限制为特定域名
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有 HTTP 方法 (GET, POST, PUT, DELETE, etc.)
    allow_headers=["*"],  # 允许所有 HTTP 头
)

# --- App Clients ---
class AppClients:
    mongo: AsyncIOMotorClient = None
    neo4j: AsyncGraphDatabase = None
    kafka_producer: AIOKafkaProducer = None
    influxdb_client: InfluxDBClient = None

# --- Database & Client Initialization/Shutdown ---
@app.on_event("startup")
async def startup_db_client():
    """Initializes MongoDB, Neo4j, Kafka, and InfluxDB clients on app startup."""
    try:
        # MongoDB 数据库连接
        app.state.clients = AppClients()
        app.state.clients.mongo = AsyncIOMotorClient(settings.MONGO_URI)
        await app.state.clients.mongo.admin.command('ping')
        logger.info("Connected to MongoDB.")

        # 创建 MongoDB 索引 (确保唯一性)
        await app.state.clients.mongo["digital_twin_db"]["environments"].create_index(
            "environment_id", unique=True
        )
        await app.state.clients.mongo["digital_twin_db"]["models"].create_index(
            [("environment_id", 1), ("model_id", 1)], unique=True
        )
        await app.state.clients.mongo["digital_twin_db"]["twins"].create_index(
            [("environment_id", 1), ("twin_id", 1)], unique=True
        )
        await app.state.clients.mongo["digital_twin_db"]["devices"].create_index(
            [("environment_id", 1), ("device_id", 1)], unique=True
        )
        await app.state.clients.mongo["digital_twin_db"]["workflows"].create_index(
            [("environment_id", 1), ("workflow_id", 1)], unique=True
        )
        logger.info("MongoDB indexes created/ensured.")

        # Neo4j 数据库连接
        app.state.clients.neo4j = AsyncGraphDatabase.driver(
            settings.NEO4J_URI,
            auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
        )
        await app.state.clients.neo4j.verify_connectivity()
        logger.info("Connected to Neo4j.")

        # 创建 Neo4j 约束 (确保是异步调用)
        async def create_neo4j_constraint_async(driver: AsyncGraphDatabase, query: str):
            """Helper to create Neo4j constraints asynchronously."""
            try:
                async with driver.session() as session:
                    await session.run(query)
                    logger.info(f"Neo4j constraint created: {query}")
            except Neo4jError as e:
                if "already exists" in str(e):
                    logger.warning(f"Neo4j constraint already exists: {query}")
                else:
                    logger.error(f"Failed to create Neo4j constraint {query}: {e}", exc_info=True)
                    raise

        # Twin 节点的唯一性约束
        await create_neo4j_constraint_async(app.state.clients.neo4j, "CREATE CONSTRAINT twin_id_unique IF NOT EXISTS FOR (t:Twin) REQUIRE t.twin_id IS UNIQUE")
        # 移除不合理的 environment_id 唯一性约束
        # await create_neo4j_constraint_async(app.state.clients.neo4j, "CREATE CONSTRAINT twin_environment_id_unique IF NOT EXISTS FOR (t:Twin) REQUIRE t.environment_id IS UNIQUE")

        # Kafka 生产者初始化
        app.state.clients.kafka_producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BROKER)
        await app.state.clients.kafka_producer.start()
        logger.info("Connected to Kafka.")

        # InfluxDB 客户端初始化
        app.state.clients.influxdb_client = InfluxDBClient(
            url=settings.INFLUXDB_URL,
            token=settings.INFLUXDB_TOKEN,
            org=settings.INFLUXDB_ORG
        )
        app.state.clients.influxdb_client.ping()
        logger.info("Connected to InfluxDB.")

    except Exception as e:
        logger.critical(f"Failed to connect to one or more clients: {e}", exc_info=True)
        await shutdown_db_client()
        raise

@app.on_event("shutdown")
async def shutdown_db_client():
    """Closes all database and client connections on app shutdown."""
    if app.state.clients.mongo:
        app.state.clients.mongo.close()
        logger.info("MongoDB connection closed.")
    if app.state.clients.neo4j:
        await app.state.clients.neo4j.close()
        logger.info("Neo4j connection closed.")
    if app.state.clients.kafka_producer:
        await app.state.clients.kafka_producer.stop()
        logger.info("Kafka producer stopped.")
    if app.state.clients.influxdb_client:
        app.state.clients.influxdb_client.close()
        logger.info("InfluxDB client closed.")

# --- Helper Function ---
def remove_mongo_id(data: Dict[str, Any]) -> Dict[str, Any]:
    """Removes the '_id' field from a dictionary."""
    if '_id' in data:
        copied_data = data.copy()
        copied_data.pop('_id')
        return copied_data
    return data

# --- Base Schema for common fields ---
class BaseSchema(BaseModel):
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    class Config:
        json_encoders = {datetime: lambda v: v.isoformat() + "Z"}
        populate_by_name = True
    def model_dump_mongo(self, **kwargs) -> Dict[str, Any]:
        data = self.model_dump(**kwargs)
        for field in ['created_at', 'updated_at']:
            if isinstance(data.get(field), datetime):
                data[field] = data[field].isoformat() + "Z"
        return data

# --- Environment Schemas ---
class EnvironmentModel(BaseSchema):
    environment_id: str
    display_name: str
    description: Optional[str] = None

class EnvironmentCreateRequest(BaseModel):
    environment_id: str
    display_name: str
    description: Optional[str] = None

class EnvironmentUpdateRequest(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None

# --- Model Schemas ---
class PropertyType(str, Enum):
    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"

class PropertyDefinition(BaseModel):
    type: PropertyType
    unit: Optional[str] = None
    description: Optional[str] = None
    is_required: bool = False

class RelationshipDefinition(BaseModel):
    target_model_id: str
    description: Optional[str] = None
    min_cardinality: int = 0
    max_cardinality: Optional[int] = None

# 新增 ModelCreateRequest，用于 POST 请求体
class ModelCreateRequest(BaseModel):
    model_id: str
    display_name: str
    description: Optional[str] = None
    properties: Dict[str, PropertyDefinition] = Field(default_factory=dict)
    relationships: Dict[str, RelationshipDefinition] = Field(default_factory=dict)
    telemetry: Dict[str, Any] = Field(default_factory=dict)
    commands: Dict[str, Any] = Field(default_factory=dict)

class ModelSchema(BaseSchema):
    model_id: str
    environment_id: str
    display_name: str
    description: Optional[str] = None
    properties: Dict[str, PropertyDefinition] = Field(default_factory=dict)
    relationships: Dict[str, RelationshipDefinition] = Field(default_factory=dict)
    telemetry: Dict[str, Any] = Field(default_factory=dict)
    commands: Dict[str, Any] = Field(default_factory=dict)

# --- Twin Schemas ---
class TwinSchema(BaseSchema):
    twin_id: str
    environment_id: str
    model_id: str
    properties: Optional[Dict[str, Any]] = Field(default_factory=dict)
    telemetry_last_updated: Optional[datetime] = None

class TwinCreateRequest(BaseModel):
    twin_id: str
    model_id: str
    properties: Optional[Dict[str, Any]] = Field(default_factory=dict)

class TwinUpdateRequest(BaseModel):
    properties: Optional[Dict[str, Any]] = Field(default_factory=dict)
    telemetry_last_updated: Optional[datetime] = None

# --- Device Schemas ---
class DeviceModel(BaseSchema):
    device_id: str
    environment_id: str
    display_name: str
    description: Optional[str] = None
    properties: Optional[Dict[str, Any]] = Field(default_factory=dict)

class DeviceCreateRequest(BaseModel):
    device_id: str
    display_name: str
    description: Optional[str] = None
    properties: Optional[Dict[str, Any]] = Field(default_factory=dict)

class DeviceUpdateRequest(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None
    properties: Optional[Dict[str, Any]] = Field(default_factory=dict)

# --- Relationship Schemas ---
class RelationshipModel(BaseSchema):
    source_twin_id: str
    target_twin_id: str
    relationship_name: str
    environment_id: str

class RelationshipCreateRequest(BaseModel):
    source_twin_id: str
    target_twin_id: str
    relationship_name: str

# --- Telemetry Schema ---
class Telemetry(BaseModel):
    device_id: str
    environment_id: str
    location: Optional[str]
    properties: Dict[str, Union[float, int, str, bool]]

# --- Workflow Schemas ---
class WorkflowSchema(BaseSchema):
    workflow_id: str
    environment_id: str
    file_name: str
    content: str # Store YAML content as string
    is_active: bool = False

class WorkflowUploadRequest(BaseModel):
    workflow_id: str
    file_name: str
    content: str

class WorkflowViewResponse(BaseModel):
    workflow_id: str
    file_name: str
    content: str # Return content as string for viewing

class WorkflowListResponse(BaseModel):
    workflows: List[WorkflowSchema]

# --- Service Classes ---

class TwinService:
    def __init__(self, db: AsyncIOMotorClient, neo4j_driver: AsyncGraphDatabase):
        self.twins_collection = db["digital_twin_db"]["twins"]
        self.models_collection = db["digital_twin_db"]["models"]
        self.neo4j_driver = neo4j_driver

    async def _validate_twin_properties_against_model(self, environment_id: str, model_id: str, properties: Dict[str, Any]):
        """Helper to validate twin properties against its model's definition."""
        model_doc = await self.models_collection.find_one({"environment_id": environment_id, "model_id": model_id})
        if not model_doc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Model '{model_id}' not found in environment '{environment_id}'.")

        model_schema = ModelSchema(**remove_mongo_id(model_doc))

        for prop_name, prop_value in properties.items():
            if prop_name not in model_schema.properties:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Property '{prop_name}' not defined in model '{model_id}'.")

            prop_def = model_schema.properties[prop_name]
            expected_type = prop_def.type.value

            if expected_type == "string" and not isinstance(prop_value, str):
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Property '{prop_name}' must be a string.")
            elif expected_type == "number" and not isinstance(prop_value, (int, float)):
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Property '{prop_name}' must be a number.")
            elif expected_type == "boolean" and not isinstance(prop_value, bool):
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Property '{prop_name}' must be a boolean.")

    async def create_twin(self, environment_id: str, twin_data: TwinCreateRequest) -> TwinSchema:
        logger.info(f"Attempting to create twin '{twin_data.twin_id}' in environment '{environment_id}'.")

        existing_twin = await self.twins_collection.find_one({"environment_id": environment_id, "twin_id": twin_data.twin_id})
        if existing_twin:
            logger.warning(f"Twin '{twin_data.twin_id}' already exists in environment '{environment_id}'.")
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Twin '{twin_data.twin_id}' already exists in this environment.")

        await self._validate_twin_properties_against_model(environment_id, twin_data.model_id, twin_data.properties)

        new_twin_doc = TwinSchema(
            twin_id=twin_data.twin_id,
            environment_id=environment_id,
            model_id=twin_data.model_id,
            properties=twin_data.properties
        )

        try:
            insert_result = await self.twins_collection.insert_one(new_twin_doc.model_dump_mongo(by_alias=True))
            logger.info(f"Twin '{new_twin_doc.twin_id}' successfully inserted into MongoDB with ID: {insert_result.inserted_id}")
        except Exception as e:
            logger.critical(f"Failed to insert twin '{new_twin_doc.twin_id}' into MongoDB: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create twin in document database.")

        try:
            async with self.neo4j_driver.session() as session:
                query = """
                MERGE (t:Twin {twin_id: $twin_id, environment_id: $environment_id})
                SET t.model_id = $model_id,
                    t.properties = $properties,
                    t.created_at = $created_at,
                    t.updated_at = $updated_at
                """
                await session.run(query,
                                twin_id=new_twin_doc.twin_id,
                                environment_id=new_twin_doc.environment_id,
                                model_id=new_twin_doc.model_id,
                                properties=json.dumps(new_twin_doc.properties),
                                created_at=new_twin_doc.created_at.isoformat(),
                                updated_at=new_twin_doc.updated_at.isoformat())
                logger.info(f"Twin '{new_twin_doc.twin_id}' successfully created in Neo4j.")
        except Exception as e:
            logger.critical(f"Failed to create twin '{new_twin_doc.twin_id}' in Neo4j: {e}. Data inconsistency: Twin created in MongoDB but not Neo4j.", exc_info=True)
            await self.twins_collection.delete_one({"_id": insert_result.inserted_id})
            logger.warning(f"Rolled back MongoDB twin creation for '{new_twin_doc.twin_id}' due to Neo4j failure.")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create twin in graph database. Data rolled back in document database.")

        return new_twin_doc

    async def get_twin(self, environment_id: str, twin_id: str) -> TwinSchema:
        """Retrieves a single twin."""
        twin_doc = await self.twins_collection.find_one({"environment_id": environment_id, "twin_id": twin_id})
        if not twin_doc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Twin '{twin_id}' not found in this environment.")
        return TwinSchema(**remove_mongo_id(twin_doc))

    async def list_twins(self, environment_id: str, model_id: Optional[str] = None, skip: int = 0, limit: int = 100) -> List[TwinSchema]:
        """Lists all twins for a given environment, with optional model filtering."""
        query = {"environment_id": environment_id}
        if model_id:
            query["model_id"] = model_id
        cursor = self.twins_collection.find(query).sort("created_at", DESCENDING).skip(skip).limit(limit)
        twins = []
        async for twin_doc in cursor:
            twins.append(TwinSchema(**remove_mongo_id(twin_doc)))
        return twins

    async def update_twin(self, environment_id: str, twin_id: str, update_data: TwinUpdateRequest) -> TwinSchema:
        logger.info(f"Attempting to update twin '{twin_id}' in environment '{environment_id}'.")

        existing_twin_doc = await self.twins_collection.find_one({"environment_id": environment_id, "twin_id": twin_id})
        if not existing_twin_doc:
            logger.warning(f"Twin '{twin_id}' not found for update in environment '{environment_id}'.")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Twin '{twin_id}' not found in this environment.")

        update_mongo_data = update_data.model_dump(exclude_unset=True)
        if "properties" in update_mongo_data:
            await self._validate_twin_properties_against_model(environment_id, existing_twin_doc["model_id"], update_mongo_data["properties"])
            existing_properties = existing_twin_doc.get("properties", {})
            existing_properties.update(update_mongo_data["properties"])
            update_mongo_data["properties"] = existing_properties

        update_mongo_data["updated_at"] = datetime.utcnow()

        try:
            update_result = await self.twins_collection.update_one(
                {"environment_id": environment_id, "twin_id": twin_id},
                {"$set": update_mongo_data}
            )
            if update_result.matched_count == 0:
                logger.warning(f"Twin '{twin_id}' not matched for update in MongoDB, despite initial find.")
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Twin '{twin_id}' not found for update.")
            logger.info(f"Twin '{twin_id}' successfully updated in MongoDB.")
        except Exception as e:
            logger.critical(f"Failed to update twin '{twin_id}' in MongoDB: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update twin in document database.")

        try:
            async with self.neo4j_driver.session() as session:
                query = """
                MATCH (t:Twin {twin_id: $twin_id, environment_id: $environment_id})
                SET t.properties = $properties_json,
                    t.updated_at = $updated_at
                """
                neo4j_properties = existing_twin_doc.get("properties", {})
                if "properties" in update_data.model_dump(exclude_unset=True):
                    neo4j_properties.update(update_data.properties)

                await session.run(query,
                                twin_id=twin_id,
                                environment_id=environment_id,
                                properties_json=json.dumps(neo4j_properties),
                                updated_at=update_mongo_data["updated_at"].isoformat())
                logger.info(f"Twin '{twin_id}' successfully updated in Neo4j.")
        except Exception as e:
            logger.critical(f"Failed to update twin '{twin_id}' in Neo4j: {e}. Data inconsistency: Twin updated in MongoDB but not Neo4j.", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update twin in graph database. Data is inconsistent.")

        updated_twin_doc = await self.twins_collection.find_one({"environment_id": environment_id, "twin_id": twin_id})
        return TwinSchema(**remove_mongo_id(updated_twin_doc))

    async def delete_twin(self, environment_id: str, twin_id: str) -> Dict[str, str]:
        """
        Deletes a twin and its relationships from Neo4j and MongoDB.
        """
        logger.info(f"Attempting to delete twin '{twin_id}' in environment '{environment_id}'.")

        twin_doc = await self.twins_collection.find_one({"environment_id": environment_id, "twin_id": twin_id})
        if not twin_doc:
            logger.warning(f"Twin '{twin_id}' not found in MongoDB for environment '{environment_id}'.")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Twin '{twin_id}' not found in this environment.")

        try:
            async with self.neo4j_driver.session() as session:
                result = await session.run(
                    """
                    MATCH (t:Twin {twin_id: $twin_id, environment_id: $environment_id})
                    DETACH DELETE t
                    RETURN count(t) AS deleted_count
                    """,
                    twin_id=twin_id, environment_id=environment_id
                )
                record = await result.single()
                neo4j_deleted_count = record["deleted_count"] if record else 0

                if neo4j_deleted_count > 0:
                    logger.info(f"Twin '{twin_id}' and its relationships successfully deleted from Neo4j.")
                else:
                    logger.warning(f"Twin '{twin_id}' not found in Neo4j for environment '{environment_id}'. Possible prior inconsistency.")

        except Exception as e:
            logger.error(f"Failed to delete twin '{twin_id}' from Neo4j: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete twin '{twin_id}' from graph database: {e}. Data might be inconsistent. Please contact support."
            )

        try:
            mongo_delete_result = await self.twins_collection.delete_one({"environment_id": environment_id, "twin_id": twin_id})

            if mongo_delete_result.deleted_count == 0:
                logger.warning(f"Twin '{twin_id}' not found in MongoDB during deletion, after Neo4j operation.")
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Twin '{twin_id}' not found in MongoDB during deletion.")

            logger.info(f"Twin '{twin_id}' in environment '{environment_id}' successfully deleted from MongoDB.")

        except Exception as e:
            logger.critical(f"Failed to delete twin '{twin_id}' from MongoDB: {e}. Data inconsistency: Twin might be deleted from Neo4j but not MongoDB.", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete twin '{twin_id}' from document database: {e}. Data is now inconsistent. Please contact support."
            )

        logger.info(f"Twin '{twin_id}' in environment '{environment_id}' fully deleted.")
        return {"detail": f"Twin '{twin_id}' deleted successfully."}


class EnvironmentService:
    def __init__(
        self,
        db: AsyncIOMotorClient,
        twin_service: 'TwinService', # EnvironmentService 现在只依赖 TwinService 来处理孪生删除
    ):
        self.env_collection = db["digital_twin_db"]["environments"]
        self.twins_collection = db["digital_twin_db"]["twins"]
        self.models_collection = db["digital_twin_db"]["models"]
        self.devices_collection = db["digital_twin_db"]["devices"]
        self.twin_service = twin_service # 保留 TwinService 实例以进行级联删除

    async def create_environment(self, env_data: EnvironmentCreateRequest) -> EnvironmentModel:
        """Creates a new environment."""
        logger.info(f"Attempting to create environment '{env_data.environment_id}'.")
        existing_env = await self.env_collection.find_one({"environment_id": env_data.environment_id})
        if existing_env:
            logger.warning(f"Environment '{env_data.environment_id}' already exists.")
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Environment '{env_data.environment_id}' already exists.")

        new_env = EnvironmentModel(
            environment_id=env_data.environment_id,
            display_name=env_data.display_name,
            description=env_data.description
        )
        try:
            await self.env_collection.insert_one(new_env.model_dump_mongo(by_alias=True))
            logger.info(f"Environment '{new_env.environment_id}' created successfully in MongoDB.")
        except Exception as e:
            logger.critical(f"Failed to create environment '{new_env.environment_id}' in MongoDB: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create environment.")
        return new_env

    async def list_environments(self, skip: int = 0, limit: int = 100) -> List[EnvironmentModel]:
        """Lists all environments."""
        cursor = self.env_collection.find({}).sort("created_at", DESCENDING).skip(skip).limit(limit)
        environments = []
        async for env_doc in cursor:
            environments.append(EnvironmentModel(**remove_mongo_id(env_doc)))
        return environments

    async def get_environment(self, environment_id: str) -> EnvironmentModel:
        """Retrieves a single environment."""
        env_doc = await self.env_collection.find_one({"environment_id": environment_id})
        if not env_doc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Environment '{environment_id}' not found.")
        return EnvironmentModel(**remove_mongo_id(env_doc))

    async def update_environment(self, environment_id: str, update_data: EnvironmentUpdateRequest) -> EnvironmentModel:
        """Updates an existing environment."""
        logger.info(f"Attempting to update environment '{environment_id}'.")
        update_doc = update_data.model_dump(exclude_unset=True)
        if not update_doc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No fields to update.")

        update_doc["updated_at"] = datetime.utcnow()
        result = await self.env_collection.update_one(
            {"environment_id": environment_id},
            {"$set": update_doc}
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Environment '{environment_id}' not found.")
        logger.info(f"Environment '{environment_id}' updated successfully.")

        updated_env = await self.env_collection.find_one({"environment_id": environment_id})
        return EnvironmentModel(**remove_mongo_id(updated_env))

    async def delete_environment(self, environment_id: str) -> Dict[str, str]:
        """
        Deletes an environment and all its associated digital twins, models, and devices.
        This operation cascades deletion to all related data in MongoDB and cleans up twin nodes and relationships in Neo4j.
        Environment nodes themselves are not in Neo4j, so no Neo4j environment node deletion occurs.
        """
        logger.info(f"Attempting to delete environment '{environment_id}' and all its contents.")

        env_doc = await self.env_collection.find_one({"environment_id": environment_id})
        if not env_doc:
            logger.warning(f"Environment '{environment_id}' not found in MongoDB for deletion.")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Environment '{environment_id}' not found.")

        # Delete all twins belonging to this environment (cascades to Neo4j via TwinService)
        twins_to_delete = []
        async for twin_doc in self.twins_collection.find({"environment_id": environment_id}, {"twin_id": 1}):
            twins_to_delete.append(twin_doc["twin_id"])

        for twin_id in twins_to_delete:
            try:
                await self.twin_service.delete_twin(environment_id, twin_id)
                logger.info(f"Successfully deleted twin '{twin_id}' as part of environment '{environment_id}' deletion.")
            except HTTPException as e:
                logger.error(f"Failed to delete twin '{twin_id}' during environment '{environment_id}' cleanup: {e.detail}", exc_info=True)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to delete twin '{twin_id}' during environment cleanup: {e.detail}. Environment deletion aborted."
                )
            except Exception as e:
                logger.critical(f"Critical error deleting twin '{twin_id}' during environment '{environment_id}' cleanup: {e}", exc_info=True)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Critical error during environment cleanup for twin '{twin_id}': {e}. Environment deletion aborted."
                )
        if twins_to_delete:
            logger.info(f"Successfully deleted {len(twins_to_delete)} twins for environment '{environment_id}'.")
        else:
            logger.info(f"No twins found to delete for environment '{environment_id}'.")

        # Delete all devices belonging to this environment (bulk delete in MongoDB)
        try:
            delete_devices_result = await self.devices_collection.delete_many({"environment_id": environment_id})
            if delete_devices_result.deleted_count > 0:
                logger.info(f"Successfully deleted {delete_devices_result.deleted_count} devices for environment '{environment_id}'.")
            else:
                logger.info(f"No devices found to delete for environment '{environment_id}'.")
        except Exception as e:
            logger.critical(f"Failed to delete devices for environment '{environment_id}' in MongoDB: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete devices for environment '{environment_id}': {e}. Environment deletion aborted."
            )

        # Delete all models belonging to this environment (bulk delete in MongoDB)
        try:
            delete_models_result = await self.models_collection.delete_many({"environment_id": environment_id})
            if delete_models_result.deleted_count > 0:
                logger.info(f"Successfully deleted {delete_models_result.deleted_count} models for environment '{environment_id}'.")
            else:
                logger.info(f"No models found to delete for environment '{environment_id}'.")
        except Exception as e:
            logger.critical(f"Failed to delete models for environment '{environment_id}' in MongoDB: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete models for environment '{environment_id}': {e}. Environment deletion aborted."
            )

        # Finally, delete the environment itself from MongoDB
        try:
            mongo_delete_result = await self.env_collection.delete_one({"environment_id": environment_id})
            if mongo_delete_result.deleted_count == 0:
                logger.warning(f"Environment '{environment_id}' not found in MongoDB during final self-deletion step.")
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Environment '{environment_id}' not found during final deletion.")
            logger.info(f"Environment '{environment_id}' successfully deleted from MongoDB.")
        except Exception as e:
            logger.critical(f"Failed to delete environment '{environment_id}' from MongoDB: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete environment '{environment_id}' from document database: {e}. Please contact support."
            )

        logger.info(f"Skipping Neo4j deletion for environment '{environment_id}' as no environment node is expected.")
        logger.info(f"Environment '{environment_id}' and all its associated data fully deleted.")
        return {"detail": f"Environment '{environment_id}' and all its contents deleted successfully."}


class DeviceService:
    def __init__(self, db: AsyncIOMotorClient):
        self.devices_collection = db["digital_twin_db"]["devices"]

    async def create_device(self, environment_id: str, device_data: DeviceCreateRequest) -> DeviceModel:
        logger.info(f"Attempting to create device '{device_data.device_id}' in environment '{environment_id}'.")
        existing_device = await self.devices_collection.find_one({"environment_id": environment_id, "device_id": device_data.device_id})
        if existing_device:
            logger.warning(f"Device '{device_data.device_id}' already exists in environment '{environment_id}'.")
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Device '{device_data.device_id}' already exists in this environment.")

        new_device = DeviceModel(
            device_id=device_data.device_id,
            environment_id=environment_id,
            display_name=device_data.display_name,
            description=device_data.description,
            properties=device_data.properties
        )
        try:
            await self.devices_collection.insert_one(new_device.model_dump_mongo(by_alias=True))
            logger.info(f"Device '{new_device.device_id}' created successfully.")
        except Exception as e:
            logger.critical(f"Failed to create device '{new_device.device_id}': {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create device.")
        return new_device

    async def list_devices(self, environment_id: str, skip: int = 0, limit: int = 100) -> List[DeviceModel]:
        """Lists all devices for a given environment."""
        query = {"environment_id": environment_id}
        cursor = self.devices_collection.find(query).sort("created_at", DESCENDING).skip(skip).limit(limit)
        devices = []
        async for device_doc in cursor:
            devices.append(DeviceModel(**remove_mongo_id(device_doc))) # <-- 修正：Device_Model 改为 DeviceModel
        return devices

    async def get_device(self, environment_id: str, device_id: str) -> DeviceModel:
        """Retrieves a single device."""
        device_doc = await self.devices_collection.find_one({"environment_id": environment_id, "device_id": device_id})
        if not device_doc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Device '{device_id}' not found in this environment.")
        return DeviceModel(**remove_mongo_id(device_doc))

    async def update_device(self, environment_id: str, device_id: str, update_data: DeviceUpdateRequest) -> DeviceModel:
        """Updates an existing device."""
        logger.info(f"Attempting to update device '{device_id}' in environment '{environment_id}'.")
        update_doc = update_data.model_dump(exclude_unset=True)
        if not update_doc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No fields to update.")

        update_doc["updated_at"] = datetime.utcnow()
        result = await self.devices_collection.update_one(
            {"environment_id": environment_id, "device_id": device_id},
            {"$set": update_doc}
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Device '{device_id}' not found in this environment.")
        logger.info(f"Device '{device_id}' updated successfully.")

        updated_device = await self.devices_collection.find_one({"environment_id": environment_id, "device_id": device_id})
        return DeviceModel(**remove_mongo_id(updated_device))

    async def delete_device(self, environment_id: str, device_id: str) -> Dict[str, str]:
        """Deletes a specific device."""
        logger.info(f"Attempting to delete device '{device_id}' in environment '{environment_id}'.")
        result = await self.devices_collection.delete_one({"environment_id": environment_id, "device_id": device_id})
        if result.deleted_count == 0:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Device '{device_id}' not found in this environment.")
        logger.info(f"Device '{device_id}' deleted successfully.")
        return {"detail": f"Device '{device_id}' deleted successfully."}


class ModelService:
    def __init__(self, db: AsyncIOMotorClient):
        self.models_collection = db["digital_twin_db"]["models"]
        self.twins_collection = db["digital_twin_db"]["twins"]

    async def create_model(self, environment_id: str, model_data: ModelCreateRequest) -> ModelSchema:
        """Creates a new model definition."""
        logger.info(f"Attempting to create model '{model_data.model_id}' in environment '{environment_id}'.")

        env_collection = self.models_collection.database["environments"]
        env_exists = await env_collection.find_one({"environment_id": environment_id})
        if not env_exists:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Environment '{environment_id}' not found.")

        existing_model = await self.models_collection.find_one({"environment_id": environment_id, "model_id": model_data.model_id})
        if existing_model:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Model '{model_data.model_id}' already exists in environment '{environment_id}'.")

        new_model_doc = ModelSchema(
            model_id=model_data.model_id,
            environment_id=environment_id,
            display_name=model_data.display_name,
            description=model_data.description,
            properties=model_data.properties,
            relationships=model_data.relationships,
            telemetry=model_data.telemetry,
            commands=model_data.commands
        )

        try:
            await self.models_collection.insert_one(new_model_doc.model_dump_mongo(by_alias=True))
            logger.info(f"Model '{new_model_doc.model_id}' created successfully to MongoDB.")
        except DuplicateKeyError:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Model '{new_model_doc.model_id}' already exists in environment '{environment_id}'.")
        except Exception as e:
            logger.critical(f"Failed to insert model '{new_model_doc.model_id}' into MongoDB: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create model.")

        return new_model_doc

    async def get_model(self, environment_id: str, model_id: str) -> ModelSchema:
        """Retrieves a single model."""
        model_doc = await self.models_collection.find_one({"environment_id": environment_id, "model_id": model_id})
        if not model_doc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Model '{model_id}' not found in environment '{environment_id}'.")
        return ModelSchema(**remove_mongo_id(model_doc))

    async def list_models(self, environment_id: str, skip: int = 0, limit: int = 100) -> List[ModelSchema]:
        """Lists all models for a given environment."""
        query = {"environment_id": environment_id}
        cursor = self.models_collection.find(query).sort("created_at", DESCENDING).skip(skip).limit(limit)
        models = []
        async for model_doc in cursor:
            models.append(ModelSchema(**remove_mongo_id(model_doc)))
        return models

    async def delete_model(self, environment_id: str, model_id: str) -> Dict[str, str]:
        """Deletes a specific model."""
        logger.info(f"Attempting to delete model '{model_id}' in environment '{environment_id}'.")

        existing_model = await self.models_collection.find_one({"environment_id": environment_id, "model_id": model_id})
        if not existing_model:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Model '{model_id}' not found in environment '{environment_id}'.")

        existing_twin_instance = await self.twins_collection.find_one(
            {"environment_id": environment_id, "model_id": model_id}
        )
        if existing_twin_instance:
            logger.warning(f"Model '{model_id}' cannot be deleted as active twin instances still exist.")
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Model '{model_id}' cannot be deleted as active twin instances still exist. Please delete associated twins first."
            )

        result = await self.models_collection.delete_one({"environment_id": environment_id, "model_id": model_id})
        if result.deleted_count == 0:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Model '{model_id}' not found in environment '{environment_id}' during deletion.")
        logger.info(f"Model '{model_id}' deleted successfully from MongoDB.")
        return {"detail": f"Model '{model_id}' deleted successfully."}


class RelationshipService:
    def __init__(self, db: AsyncIOMotorClient, neo4j_driver: AsyncGraphDatabase):
        self.relationships_collection = db["digital_twin_db"]["relationships"]
        self.twins_collection = db["digital_twin_db"]["twins"]
        self.neo4j_driver = neo4j_driver

    async def create_relationship(self, environment_id: str, rel_data: RelationshipCreateRequest) -> RelationshipModel:
        """Creates a new relationship between two twins."""
        logger.info(f"Attempting to create relationship '{rel_data.relationship_name}' between '{rel_data.source_twin_id}' and '{rel_data.target_twin_id}'.")

        source_twin = await self.twins_collection.find_one({"environment_id": environment_id, "twin_id": rel_data.source_twin_id})
        target_twin = await self.twins_collection.find_one({"environment_id": environment_id, "twin_id": rel_data.target_twin_id})

        if not source_twin:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Source twin '{rel_data.source_twin_id}' not found in this environment.")
        if not target_twin:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Target twin '{rel_data.target_twin_id}' not found in this environment.")

        existing_rel_mongo = await self.relationships_collection.find_one({
            "environment_id": environment_id,
            "source_twin_id": rel_data.source_twin_id,
            "target_twin_id": rel_data.target_twin_id,
            "relationship_name": rel_data.relationship_name
        })
        if existing_rel_mongo:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Relationship '{rel_data.relationship_name}' already exists between '{rel_data.source_twin_id}' and '{rel_data.target_twin_id}'.")

        new_rel = RelationshipModel(
            source_twin_id=rel_data.source_twin_id,
            target_twin_id=rel_data.target_twin_id,
            relationship_name=rel_data.relationship_name,
            environment_id=environment_id
        )

        try:
            insert_result = await self.relationships_collection.insert_one(new_rel.model_dump_mongo(by_alias=True))
            logger.info(f"Relationship between '{new_rel.source_twin_id}' and '{new_rel.target_twin_id}' created in MongoDB with ID: {insert_result.inserted_id}")
        except Exception as e:
            logger.critical(f"Failed to insert relationship into MongoDB: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create relationship in document database.")

        try:
            async with self.neo4j_driver.session() as session:
                query = f"""
                MATCH (a:Twin {{twin_id: $source_twin_id, environment_id: $environment_id}})
                MATCH (b:Twin {{twin_id: $target_twin_id, environment_id: $environment_id}})
                MERGE (a)-[r:`{rel_data.relationship_name}`]->(b)
                SET r.environment_id = $environment_id,
                    r.created_at = $created_at,
                    r.updated_at = $updated_at
                RETURN r
                """
                await session.run(query,
                                source_twin_id=new_rel.source_twin_id,
                                target_twin_id=new_rel.target_twin_id,
                                relationship_name=new_rel.relationship_name,
                                environment_id=new_rel.environment_id,
                                created_at=new_rel.created_at.isoformat(),
                                updated_at=new_rel.updated_at.isoformat())
            logger.info(f"Relationship '{new_rel.relationship_name}' between '{new_rel.source_twin_id}' and '{new_rel.target_twin_id}' created in Neo4j.")
        except Exception as e:
            logger.critical(f"Failed to create relationship in Neo4j: {e}. Data inconsistency: Relationship created in MongoDB but not Neo4j.", exc_info=True)
            await self.relationships_collection.delete_one({"_id": insert_result.inserted_id})
            logger.warning(f"Rolled back MongoDB relationship creation due to Neo4j failure.")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create relationship in graph database. Data rolled back in document database.")

        return new_rel

    async def delete_relationship(self, environment_id: str, source_twin_id: str, target_twin_id: str, relationship_name: str) -> Dict[str, str]:
        """Deletes a relationship between two twins."""
        logger.info(f"Attempting to delete relationship '{relationship_name}' from '{source_twin_id}' to '{target_twin_id}'.")

        mongo_result = await self.relationships_collection.delete_one({
            "environment_id": environment_id,
            "source_twin_id": source_twin_id,
            "target_twin_id": target_twin_id,
            "relationship_name": relationship_name
        })
        if mongo_result.deleted_count == 0:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Relationship '{relationship_name}' from '{source_twin_id}' to '{target_twin_id}' not found.")
        logger.info(f"Relationship deleted from MongoDB.")

        try:
            async with self.neo4j_driver.session() as session:
                query = f"""
                MATCH (a:Twin {{twin_id: $source_twin_id, environment_id: $environment_id}})-[r:`{relationship_name}`]->(b:Twin {{twin_id: $target_twin_id, environment_id: $environment_id}})
                DELETE r
                """
                await session.run(query,
                                source_twin_id=source_twin_id,
                                target_twin_id=target_twin_id,
                                environment_id=environment_id)
                logger.info(f"Relationship '{relationship_name}' deleted from Neo4j.")
        except Exception as e:
            logger.error(f"Failed to delete relationship from Neo4j: {e}. Data inconsistency: Relationship deleted from MongoDB but not Neo4j.", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to delete relationship from graph database: {e}. Data is inconsistent. Please contact support."
            )

        return {"detail": f"Relationship '{relationship_name}' deleted successfully."}

    async def list_relationships(self, environment_id: str, twin_id: Optional[str] = None) -> List[RelationshipModel]:
        """Lists all relationships for an environment, optionally filtered by a specific twin."""
        query = {"environment_id": environment_id}
        if twin_id:
            query = {
                "environment_id": environment_id,
                "$or": [{"source_twin_id": twin_id}, {"target_twin_id": twin_id}]
            }
        cursor = self.relationships_collection.find(query).sort("created_at", DESCENDING)
        relationships = []
        async for rel_doc in cursor:
            relationships.append(RelationshipModel(**remove_mongo_id(rel_doc)))
        return relationships


class TelemetryService:
    def __init__(self, influxdb_client: InfluxDBClient, bucket: str, org: str):
        self.influxdb_client = influxdb_client
        self.bucket = bucket
        self.org = org
        self.write_api = influxdb_client.write_api()
        self.query_api = influxdb_client.query_api()

    async def write_telemetry(self, telemetry_data: Telemetry):
        """Writes telemetry data to InfluxDB."""
        logger.info(f"Writing telemetry for device '{telemetry_data.device_id}' in environment '{telemetry_data.environment_id}'.")
        point = {
            "measurement": "telemetry",
            "tags": {
                "device_id": telemetry_data.device_id,
                "environment_id": telemetry_data.environment_id,
                "location": telemetry_data.location
            },
            "fields": telemetry_data.properties,
            "time": datetime.utcnow().isoformat() + "Z"
        }
        try:
            await run_in_threadpool(self.write_api.write, self.bucket, self.org, point)
            logger.info(f"Telemetry for device '{telemetry_data.device_id}' written to InfluxDB.")
        except Exception as e:
            logger.critical(f"Failed to write telemetry for device '{telemetry_data.device_id}' to InfluxDB: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to write telemetry data.")

    async def query_telemetry(
        self,
        environment_id: str,
        device_id: str,
        start_time: str,
        end_time: Optional[str] = None,
        field: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Queries telemetry data from InfluxDB."""
        logger.info(f"Querying telemetry for device '{device_id}' in environment '{environment_id}'.")

        flux_query = f'''
        from(bucket: "{self.bucket}")
        |> range(start: {start_time}, stop: {end_time if end_time else "now()"})
        |> filter(fn: (r) => r["_measurement"] == "telemetry")
        |> filter(fn: (r) => r["device_id"] == "{device_id}")
        |> filter(fn: (r) => r["environment_id"] == "{environment_id}")
        '''
        if field:
            flux_query += f'|> filter(fn: (r) => r["_field"] == "{field}")'
        
        flux_query += '|> yield(name: "results")'

        try:
            tables = await run_in_threadpool(self.query_api.query, flux_query, org=self.org)
            results = []
            for table in tables:
                for record in table.records:
                    results.append({
                        "time": record.get_time().isoformat(),
                        "field": record.get_field(),
                        "value": record.get_value(),
                        "device_id": record.values.get("device_id"),
                        "environment_id": record.values.get("environment_id"),
                        "location": record.values.get("location")
                    })
            logger.info(f"Successfully queried telemetry for device '{device_id}'. Found {len(results)} records.")
            return results
        except Exception as e:
            logger.critical(f"Failed to query telemetry for device '{device_id}' from InfluxDB: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to query telemetry data.")


class WorkflowService:
    def __init__(self, db: AsyncIOMotorClient):
        self.workflows_collection = db["digital_twin_db"]["workflows"]

    async def upload_workflow(self, environment_id: str, workflow_id: str, file_name: str, content: bytes) -> WorkflowSchema:
        logger.info(f"Attempting to upload workflow '{workflow_id}' in environment '{environment_id}'.")

        try:
            yaml_content = content.decode('utf-8')
            yaml.safe_load(yaml_content)
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Invalid YAML content: {e}")

        existing_workflow = await self.workflows_collection.find_one({"environment_id": environment_id, "workflow_id": workflow_id})
        if existing_workflow:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Workflow '{workflow_id}' already exists in environment '{environment_id}'.")

        new_workflow = WorkflowSchema(
            workflow_id=workflow_id,
            environment_id=environment_id,
            file_name=file_name,
            content=yaml_content
        )

        try:
            await self.workflows_collection.insert_one(new_workflow.model_dump_mongo(by_alias=True))
            logger.info(f"Workflow '{workflow_id}' uploaded successfully.")
        except Exception as e:
            logger.critical(f"Failed to upload workflow '{workflow_id}': {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to upload workflow.")

        return new_workflow

    async def delete_workflow(self, environment_id: str, workflow_id: str) -> Dict[str, str]:
        logger.info(f"Attempting to delete workflow '{workflow_id}' in environment '{environment_id}'.")
        result = await self.workflows_collection.delete_one({"environment_id": environment_id, "workflow_id": workflow_id})
        if result.deleted_count == 0:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Workflow '{workflow_id}' not found in this environment.")
        logger.info(f"Workflow '{workflow_id}' deleted successfully.")
        return {"detail": f"Workflow '{workflow_id}' deleted successfully."}

    async def list_workflows(self, environment_id: str) -> WorkflowListResponse:
        logger.info(f"Listing workflows for environment '{environment_id}'.")
        workflows = []
        cursor = self.workflows_collection.find({"environment_id": environment_id}).sort("created_at", DESCENDING)
        async for wf_doc in cursor:
            workflows.append(WorkflowSchema(**remove_mongo_id(wf_doc)))
        logger.info(f"Found {len(workflows)} workflows for environment '{environment_id}'.")
        return WorkflowListResponse(workflows=workflows)

    async def view_workflow(self, environment_id: str, workflow_id: str) -> WorkflowViewResponse:
        logger.info(f"Viewing workflow '{workflow_id}' in environment '{environment_id}'.")
        workflow_doc = await self.workflows_collection.find_one({"environment_id": environment_id, "workflow_id": workflow_id})
        if not workflow_doc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Workflow '{workflow_id}' not found in this environment.")
        return WorkflowViewResponse(
            workflow_id=workflow_doc["workflow_id"],
            file_name=workflow_doc["file_name"],
            content=workflow_doc["content"]
        )


# --- Dependency Injection Functions ---
async def get_db(request: Request) -> AsyncIOMotorClient:
    return request.app.state.clients.mongo["digital_twin_db"]

async def get_neo4j_driver(request: Request) -> AsyncGraphDatabase:
    return request.app.state.clients.neo4j

async def get_kafka_producer(request: Request) -> AIOKafkaProducer:
    return request.app.state.clients.kafka_producer

async def get_influxdb_client(request: Request) -> InfluxDBClient:
    return request.app.state.clients.influxdb_client

# 确保 TwinService 是第一个被依赖的，因为 EnvironmentService 需要它
async def get_twin_service(
    db: AsyncIOMotorClient = Depends(get_db),
    neo4j_driver: AsyncGraphDatabase = Depends(get_neo4j_driver)
) -> TwinService:
    return TwinService(db, neo4j_driver)

# EnvironmentService 现在只依赖 TwinService
async def get_environment_service(
    db: AsyncIOMotorClient = Depends(get_db),
    twin_service: 'TwinService' = Depends(get_twin_service) # 注入 TwinService
) -> EnvironmentService:
    return EnvironmentService(db, twin_service)


async def get_model_service(db: AsyncIOMotorClient = Depends(get_db)) -> ModelService:
    return ModelService(db)

async def get_device_service(db: AsyncIOMotorClient = Depends(get_db)) -> DeviceService:
    return DeviceService(db)

async def get_relationship_service(
    db: AsyncIOMotorClient = Depends(get_db),
    neo4j_driver: AsyncGraphDatabase = Depends(get_neo4j_driver)
) -> RelationshipService:
    return RelationshipService(db, neo4j_driver)

async def get_telemetry_service(
    influxdb_client: InfluxDBClient = Depends(get_influxdb_client)
) -> TelemetryService:
    return TelemetryService(influxdb_client, settings.INFLUXDB_BUCKET, settings.INFLUXDB_ORG)

async def get_workflow_service(db: AsyncIOMotorClient = Depends(get_db)) -> WorkflowService:
    return WorkflowService(db)


# 初始化路由时只设置tags，不设置prefix
environment_router = APIRouter(tags=["Environments"])
model_router = APIRouter(tags=["Models"])
twin_router = APIRouter(tags=["Twins"])
device_router = APIRouter(tags=["Devices"])
relationship_router = APIRouter(tags=["Relationships"])
telemetry_router = APIRouter(tags=["Telemetry"])
workflow_router = APIRouter(tags=["Workflows"])


# --- Environment API Endpoints ---
@environment_router.post("", response_model=EnvironmentModel, status_code=status.HTTP_201_CREATED)
async def create_environment_api(
    env_data: EnvironmentCreateRequest,
    environment_service: EnvironmentService = Depends(get_environment_service)
):
    """Creates a new digital twin environment."""
    return await environment_service.create_environment(env_data)

@environment_router.get("", response_model=List[EnvironmentModel])
async def list_environments_api(
    environment_service: EnvironmentService = Depends(get_environment_service),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, le=1000)
):
    """Lists all digital twin environments."""
    return await environment_service.list_environments(skip, limit)

@environment_router.get("/{environment_id}", response_model=EnvironmentModel)
async def get_environment_api(
    environment_id: str,
    environment_service: EnvironmentService = Depends(get_environment_service)
):
    """Retrieves a specific digital twin environment by ID."""
    return await environment_service.get_environment(environment_id)

@environment_router.put("/{environment_id}", response_model=EnvironmentModel)
async def update_environment_api(
    environment_id: str,
    update_data: EnvironmentUpdateRequest,
    environment_service: EnvironmentService = Depends(get_environment_service)
):
    """Updates an existing digital twin environment."""
    return await environment_service.update_environment(environment_id, update_data)

@environment_router.delete("/{environment_id}", status_code=status.HTTP_200_OK)
async def delete_environment_api(
    environment_id: str,
    environment_service: EnvironmentService = Depends(get_environment_service)
):
    """Deletes a digital twin environment and all its contents (twins, models, devices, relationships)."""
    return await environment_service.delete_environment(environment_id)

# --- Model API Endpoints ---
# 更改为接收 ModelCreateRequest
@model_router.post("", response_model=ModelSchema, status_code=status.HTTP_201_CREATED)
async def create_model_api( # 重命名函数名
    environment_id: str, # 路径参数
    model_data: ModelCreateRequest, # 请求体
    model_service: ModelService = Depends(get_model_service)
):
    """Creates a new model definition."""
    return await model_service.create_model(environment_id, model_data)

@model_router.get("/{model_id}", response_model=ModelSchema)
async def get_model_api(
    environment_id: str,
    model_id: str,
    model_service: ModelService = Depends(get_model_service)
):
    """Retrieves a specific model by ID."""
    return await model_service.get_model(environment_id, model_id)

@model_router.get("", response_model=List[ModelSchema])
async def list_models_api(
    environment_id: str,
    model_service: ModelService = Depends(get_model_service),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, le=1000)
):
    """Lists all models for a given environment."""
    return await model_service.list_models(environment_id, skip, limit)

@model_router.delete("/{model_id}", status_code=status.HTTP_200_OK)
async def delete_model_api(
    environment_id: str,
    model_id: str,
    model_service: ModelService = Depends(get_model_service)
):
    """Deletes a specific model. Fails if active twin instances use this model."""
    return await model_service.delete_model(environment_id, model_id)

# --- Twin API Endpoints ---
@twin_router.post("", response_model=TwinSchema, status_code=status.HTTP_201_CREATED)
async def create_twin_api(
    environment_id: str,
    twin_data: TwinCreateRequest,
    twin_service: TwinService = Depends(get_twin_service)
):
    """Creates a new digital twin instance."""
    return await twin_service.create_twin(environment_id, twin_data)

@twin_router.get("/{twin_id}", response_model=TwinSchema)
async def get_twin_api(
    environment_id: str,
    twin_id: str,
    twin_service: TwinService = Depends(get_twin_service)
):
    """Retrieves a specific twin by ID."""
    return await twin_service.get_twin(environment_id, twin_id)

@twin_router.get("", response_model=List[TwinSchema])
async def list_twins_api(
    environment_id: str,
    twin_service: TwinService = Depends(get_twin_service),
    model_id: Optional[str] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, le=1000)
):
    """Lists all twins for a given environment, with optional model filtering."""
    return await twin_service.list_twins(environment_id, model_id, skip, limit)

@twin_router.put("/{twin_id}", response_model=TwinSchema)
async def update_twin_api(
    environment_id: str,
    twin_id: str,
    update_data: TwinUpdateRequest,
    twin_service: TwinService = Depends(get_twin_service)
):
    """Updates an existing digital twin instance."""
    return await twin_service.update_twin(environment_id, twin_id, update_data)

@twin_router.delete("/{twin_id}", status_code=status.HTTP_200_OK)
async def delete_twin_api(
    environment_id: str,
    twin_id: str,
    twin_service: TwinService = Depends(get_twin_service)
):
    """Deletes a specific twin."""
    return await twin_service.delete_twin(environment_id, twin_id)

# --- Device API Endpoints ---
@device_router.post("", response_model=DeviceModel, status_code=status.HTTP_201_CREATED)
async def create_device_api(
    environment_id: str,
    device_data: DeviceCreateRequest,
    device_service: DeviceService = Depends(get_device_service)
):
    """Creates a new device."""
    return await device_service.create_device(environment_id, device_data)

@device_router.get("/{device_id}", response_model=DeviceModel)
async def get_device_api(
    environment_id: str,
    device_id: str,
    device_service: DeviceService = Depends(get_device_service)
):
    """Retrieves a specific device by ID."""
    return await device_service.get_device(environment_id, device_id)

@device_router.get("", response_model=List[DeviceModel])
async def list_devices_api(
    environment_id: str,
    device_service: DeviceService = Depends(get_device_service),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, le=1000)
):
    """Lists all devices for a given environment."""
    return await device_service.list_devices(environment_id, skip, limit)

@device_router.put("/{device_id}", response_model=DeviceModel)
async def update_device_api(
    environment_id: str,
    device_id: str,
    update_data: DeviceUpdateRequest,
    device_service: DeviceService = Depends(get_device_service)
):
    """Updates an existing device."""
    return await device_service.update_device(environment_id, device_id, update_data)

@device_router.delete("/{device_id}", status_code=status.HTTP_200_OK)
async def delete_device_api(
    environment_id: str,
    device_id: str,
    device_service: DeviceService = Depends(get_device_service)
):
    """Deletes a specific device."""
    return await device_service.delete_device(environment_id, device_id)

# --- Relationship API Endpoints ---
@relationship_router.post("", response_model=RelationshipModel, status_code=status.HTTP_201_CREATED)
async def create_relationship_api(
    environment_id: str,
    rel_data: RelationshipCreateRequest,
    relationship_service: RelationshipService = Depends(get_relationship_service)
):
    """Creates a new relationship between two twins."""
    return await relationship_service.create_relationship(environment_id, rel_data)

@relationship_router.delete("")
async def delete_relationship_api(
    environment_id: str,
    source_twin_id: str = Query(...),
    target_twin_id: str = Query(...),
    relationship_name: str = Query(...),
    relationship_service: RelationshipService = Depends(get_relationship_service)
):
    """Deletes a specific relationship between two twins."""
    return await relationship_service.delete_relationship(environment_id, source_twin_id, target_twin_id, relationship_name)

@relationship_router.get("", response_model=List[RelationshipModel])
async def list_relationships_api(
    environment_id: str,
    twin_id: Optional[str] = Query(None),
    relationship_service: RelationshipService = Depends(get_relationship_service)
):
    """Lists all relationships for an environment, optionally filtered by a specific twin."""
    return await relationship_service.list_relationships(environment_id, twin_id)

# --- Telemetry API Endpoints ---
@telemetry_router.post("", status_code=status.HTTP_204_NO_CONTENT)
async def write_telemetry_api(
    environment_id: str,
    telemetry_data: Telemetry,
    telemetry_service: TelemetryService = Depends(get_telemetry_service)
):
    """Writes telemetry data."""
    if telemetry_data.environment_id != environment_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Environment ID in payload must match path parameter.")
    return await telemetry_service.write_telemetry(telemetry_data)

@telemetry_router.get("", response_model=List[Dict[str, Any]])
async def query_telemetry_api(
    environment_id: str,
    device_id: str = Query(...),
    start_time: str = Query(..., description="Start time in RFC3339 format, e.g., '2023-01-01T00:00:00Z' or '-1h'"),
    end_time: Optional[str] = Query(None, description="End time in RFC3339 format, e.g., '2023-01-01T01:00:00Z' or 'now()'"),
    field: Optional[str] = Query(None, description="Specific telemetry field to query"),
    telemetry_service: TelemetryService = Depends(get_telemetry_service)
):
    """Queries telemetry data."""
    return await telemetry_service.query_telemetry(environment_id, device_id, start_time, end_time, field)

# --- Workflow API Endpoints ---
@workflow_router.post("", status_code=status.HTTP_201_CREATED)
async def upload_workflow_api(
    environment_id: str,
    workflow_id: str = Form(...),
    file: UploadFile = File(...),
    workflow_service: WorkflowService = Depends(get_workflow_service) # 使用 Depends 注入
):
    """Uploads a new workflow definition (YAML)."""
    content = await file.read()
    return await workflow_service.upload_workflow(environment_id, workflow_id, file.filename, content)

@workflow_router.delete("/{workflow_id}", status_code=status.HTTP_200_OK)
async def delete_workflow_api(
    environment_id: str,
    workflow_id: str,
    workflow_service: WorkflowService = Depends(get_workflow_service) # 使用 Depends 注入
):
    """Deletes a specific workflow."""
    return await workflow_service.delete_workflow(environment_id, workflow_id)

@workflow_router.get("", response_model=WorkflowListResponse)
async def list_workflows_api(
    environment_id: str,
    workflow_service: WorkflowService = Depends(get_workflow_service) # 使用 Depends 注入
):
    """Lists all workflows for a given environment."""
    return await workflow_service.list_workflows(environment_id)

@workflow_router.get("/{workflow_id}", response_model=WorkflowViewResponse)
async def view_workflow_api(
    environment_id: str,
    workflow_id: str,
    workflow_service: WorkflowService = Depends(get_workflow_service) # 使用 Depends 注入
):
    """Retrieves the content of a specific workflow."""
    return await workflow_service.view_workflow(environment_id, workflow_id)


# 子路由的 include_router，修正 prefix 以正确捕获 environment_id
app.include_router(environment_router, prefix="/environments", tags=["Environments"])
app.include_router(model_router, prefix="/environments/{environment_id}/models", tags=["Models"])
app.include_router(twin_router, prefix="/environments/{environment_id}/twins", tags=["Twins"])
app.include_router(device_router, prefix="/environments/{environment_id}/devices", tags=["Devices"])
app.include_router(relationship_router, prefix="/environments/{environment_id}/relationships", tags=["Relationships"])
app.include_router(telemetry_router, prefix="/environments/{environment_id}/telemetry", tags=["Telemetry"])
app.include_router(workflow_router, prefix="/environments/{environment_id}/workflows", tags=["Workflows"])