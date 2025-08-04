# 标准库
import os
import re
import json
import logging
import yaml
import hashlib
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, List, Dict, Any, Union, Set, TypeVar, Generic
from enum import Enum
from contextlib import asynccontextmanager
from functools import lru_cache

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
    Depends,
    Path,
    Body
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.concurrency import run_in_threadpool
from fastapi import APIRouter

# Pydantic 配置
from pydantic import BaseModel, Field, validator
from pydantic_settings import BaseSettings

# 数据库驱动
from pymongo.errors import DuplicateKeyError
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import DESCENDING, IndexModel
from neo4j import AsyncGraphDatabase
from neo4j.exceptions import Neo4jError
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from aiokafka import AIOKafkaProducer

from dotenv import load_dotenv

load_dotenv()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Settings ---
class Settings(BaseSettings):
    # Database connections
    MONGO_URI: str = Field(default="mongodb://localhost:27017")
    MONGO_DB_NAME: str = Field(default="digital_twin_db")
    
    NEO4J_URI: str = Field(default="bolt://localhost:7687")
    NEO4J_USER: str = Field(default="neo4j")
    NEO4J_PASSWORD: str = Field(default="password")
    
    KAFKA_BROKER: str = Field(default="localhost:9092")
    
    INFLUXDB_URL: str = Field(default="http://localhost:8086")
    INFLUXDB_TOKEN: str = Field(default="mytoken")
    INFLUXDB_ORG: str = Field(default="myorg")
    INFLUXDB_BUCKET: str = Field(default="mybucket")
    
    # API settings
    API_TITLE: str = Field(default="Digital Twin Platform API")
    API_VERSION: str = Field(default="0.2.0")
    
    # Performance settings
    DEFAULT_PAGE_SIZE: int = Field(default=100)
    MAX_PAGE_SIZE: int = Field(default=1000)
    
    class Config:
        env_file = ".env"

settings = Settings()

REL_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

class TimestampMixin(BaseModel):
    """Mixin for adding timestamp fields to models."""
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() + "Z"
        }
        populate_by_name = True

    def model_dump_mongo(self, **kwargs) -> Dict[str, Any]:
        """Convert model to MongoDB-compatible dictionary."""
        data = self.model_dump(**kwargs)
        # Ensure datetime fields are properly formatted
        for field in ['created_at', 'updated_at', 'last_executed', 'telemetry_last_updated']:
            if field in data and isinstance(data[field], datetime):
                data[field] = data[field].isoformat() + "Z"
        return data

# --- Utility Functions ---
def remove_mongo_id(data: Dict[str, Any]) -> Dict[str, Any]:
    """Remove MongoDB _id field from dictionary."""
    return {k: v for k, v in data.items() if k != '_id'}

def validate_identifier(identifier: str, field_name: str) -> str:
    """Validate identifier format."""
    if not re.match(r"^[a-zA-Z0-9_-]+$", identifier):
        raise ValueError(f"{field_name} must contain only alphanumeric characters, hyphens, and underscores")
    if len(identifier) < 3 or len(identifier) > 50:
        raise ValueError(f"{field_name} must be between 3 and 50 characters")
    return identifier

class PropertyType(str, Enum):
    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"
    OBJECT = "object"
    ARRAY = "array"

# TODO: workflow的要不要在这定义还待定
class WorkflowStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"
    DRAFT = "draft"

class WorkflowType(str, Enum):
    AUTOMATION = "automation"
    DATA_PROCESSING = "data_processing"
    MONITORING = "monitoring"
    CUSTOM = "custom"

# TODO: 是否保留限制条件待定
class PropertyDefinition(BaseModel):
    type: PropertyType
    unit: Optional[str] = None
    description: Optional[str] = None
    is_required: bool = False
    default_value: Optional[Any] = None
    constraints: Optional[Dict[str, Any]] = None  # min, max, pattern, etc.

class EnvironmentCreate(BaseModel):
    environment_id: str
    display_name: str
    description: Optional[str] = None
    
    @validator('environment_id')
    def validate_environment_id(cls, v):
        return validate_identifier(v, 'environment_id')

class EnvironmentUpdate(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None

class Environment(TimestampMixin):
    environment_id: str
    display_name: str
    description: Optional[str] = None

class ModelCreate(BaseModel):
    model_id: str
    display_name: str
    description: Optional[str] = None
    properties: Dict[str, PropertyDefinition] = Field(default_factory=dict)
    
    @validator('model_id')
    def validate_model_id(cls, v):
        return validate_identifier(v, 'model_id')

class ModelUpdate(BaseModel):
    description: Optional[str] = None

class Model(TimestampMixin):
    model_id: str
    environment_id: str
    display_name: str
    description: Optional[str] = None
    properties: Dict[str, PropertyDefinition] = Field(default_factory=dict)

class TwinCreate(BaseModel):
    twin_id: str
    model_id: str
    properties: Optional[Dict[str, Any]] = Field(default_factory=dict)
    
    @validator('twin_id')
    def validate_twin_id(cls, v):
        return validate_identifier(v, 'twin_id')

# TODO: twin
class TwinUpdate(BaseModel):
    properties: Optional[Dict[str, Any]] = None
    telemetry_last_updated: Optional[datetime] = None

class Twin(TimestampMixin):
    twin_id: str
    environment_id: str
    model_id: str
    properties: Dict[str, Any] = Field(default_factory=dict)
    telemetry_last_updated: Optional[datetime] = None

class DeviceCreate(BaseModel):
    device_id: str
    display_name: str
    description: Optional[str] = None
    
    @validator('device_id')
    def validate_device_id(cls, v):
        return validate_identifier(v, 'device_id')

class DeviceUpdate(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None

class Device(TimestampMixin):
    device_id: str
    environment_id: str
    display_name: str
    description: Optional[str] = None
    properties: Dict[str, Any] = Field(default_factory=dict)
    # Statistics
    telemetry_points_count: int = 0

class RelationshipCreate(BaseModel):
    source_twin_id: str
    target_twin_id: str
    relationship_name: str
    
    @validator('relationship_name')
    def validate_relationship_name(cls, v):
        if not REL_NAME_PATTERN.match(v):
            raise ValueError("Relationship name must contain only letters, numbers, and underscores, and start with a letter")
        return v

class Relationship(TimestampMixin):
    source_twin_id: str
    target_twin_id: str
    relationship_name: str
    environment_id: str

class WorkflowCreate(BaseModel):
    workflow_id: str
    file_name: str
    content: str
    workflow_type: WorkflowType = WorkflowType.CUSTOM
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    
    @validator('workflow_id')
    def validate_workflow_id(cls, v):
        return validate_identifier(v, 'workflow_id')

class WorkflowUpdate(BaseModel):
    file_name: Optional[str] = None
    content: Optional[str] = None
    workflow_type: Optional[WorkflowType] = None
    status: Optional[WorkflowStatus] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = None
    version: Optional[str] = None

class Workflow(TimestampMixin):
    workflow_id: str
    environment_id: str
    file_name: str
    content: str
    workflow_type: WorkflowType = WorkflowType.CUSTOM
    status: WorkflowStatus = WorkflowStatus.DRAFT
    description: Optional[str] = None
    version: str = "1.0.0"
    tags: List[str] = Field(default_factory=list)
    # Execution statistics
    last_executed: Optional[datetime] = None
    execution_count: int = 0
    # Metadata
    file_size: int = 0
    checksum: str = ""

class WorkflowSummary(BaseModel):
    """Workflow summary for list views (without content)."""
    workflow_id: str
    environment_id: str
    file_name: str
    workflow_type: WorkflowType
    status: WorkflowStatus
    description: Optional[str] = None
    version: str
    tags: List[str]
    created_at: datetime
    updated_at: datetime
    last_executed: Optional[datetime] = None
    execution_count: int
    file_size: int

# Response Models
class PaginatedResponse(BaseModel, Generic[TypeVar('T')]):
    items: List[TypeVar('T')]
    total_count: int
    page: int
    page_size: int
    has_next: bool
    has_previous: bool

class OperationResponse(BaseModel):
    success: bool
    message: str
    details: Optional[Dict[str, Any]] = None

# --- Exceptions ---
class ResourceNotFoundError(HTTPException):
    def __init__(self, resource_type: str, resource_id: str, environment_id: Optional[str] = None):
        if environment_id:
            detail = f"{resource_type} '{resource_id}' not found in environment '{environment_id}'"
        else:
            detail = f"{resource_type} '{resource_id}' not found"
        super().__init__(status_code=status.HTTP_404_NOT_FOUND, detail=detail)

class ResourceConflictError(HTTPException):
    def __init__(self, resource_type: str, resource_id: str, environment_id: Optional[str] = None):
        if environment_id:
            detail = f"{resource_type} '{resource_id}' already exists in environment '{environment_id}'"
        else:
            detail = f"{resource_type} '{resource_id}' already exists"
        super().__init__(status_code=status.HTTP_409_CONFLICT, detail=detail)

class ValidationError(HTTPException):
    def __init__(self, message: str):
        super().__init__(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=message)

class DependencyError(HTTPException):
    def __init__(self, message: str):
        super().__init__(status_code=status.HTTP_409_CONFLICT, detail=message)


class DatabaseClients:
    """Centralized database clients manager."""
    
    def __init__(self):
        self.mongo_client: Optional[AsyncIOMotorClient] = None
        self.mongo_db: Optional[AsyncIOMotorDatabase] = None
        self.neo4j_driver: Optional[AsyncGraphDatabase] = None
        self.kafka_producer: Optional[AIOKafkaProducer] = None
        self.influxdb_client: Optional[InfluxDBClient] = None
        
    async def initialize(self):
        """Initialize all database connections."""
        try:
            # MongoDB
            self.mongo_client = AsyncIOMotorClient(settings.MONGO_URI)
            self.mongo_db = self.mongo_client[settings.MONGO_DB_NAME]
            await self.mongo_client.admin.command('ping')
            logger.info("Connected to MongoDB")
            
            # Create indexes
            await self._create_mongodb_indexes()
            
            # Neo4j
            self.neo4j_driver = AsyncGraphDatabase.driver(
                settings.NEO4J_URI,
                auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
            )
            await self.neo4j_driver.verify_connectivity()
            logger.info("Connected to Neo4j")
            
            # Create constraints
            await self._create_neo4j_constraints()
            
            # Kafka
            self.kafka_producer = AIOKafkaProducer(
                bootstrap_servers=settings.KAFKA_BROKER
            )
            await self.kafka_producer.start()
            logger.info("Connected to Kafka")
            
            # InfluxDB
            self.influxdb_client = InfluxDBClient(
                url=settings.INFLUXDB_URL,
                token=settings.INFLUXDB_TOKEN,
                org=settings.INFLUXDB_ORG
            )
            self.influxdb_client.ping()
            logger.info("Connected to InfluxDB")
            
        except Exception as e:
            logger.error(f"Failed to initialize database clients: {e}")
            await self.cleanup()
            raise
    
    async def cleanup(self):
        """Close all database connections."""
        if self.mongo_client:
            self.mongo_client.close()
        if self.neo4j_driver:
            await self.neo4j_driver.close()
        if self.kafka_producer:
            await self.kafka_producer.stop()
        if self.influxdb_client:
            self.influxdb_client.close()
    
    async def _create_mongodb_indexes(self):
        """Create MongoDB indexes."""
        indexes = [
            # Environments
            (self.mongo_db["environments"], [IndexModel("environment_id", unique=True)]),
            # Models
            (self.mongo_db["models"], [IndexModel([("environment_id", 1), ("model_id", 1)], unique=True)]),
            # Twins
            (self.mongo_db["twins"], [
                IndexModel([("environment_id", 1), ("twin_id", 1)], unique=True),
                IndexModel([("environment_id", 1), ("model_id", 1)])
            ]),
            # Devices
            (self.mongo_db["devices"], [IndexModel([("environment_id", 1), ("device_id", 1)], unique=True)]),
            # Workflows
            (self.mongo_db["workflows"], [IndexModel([("environment_id", 1), ("workflow_id", 1)], unique=True)]),
            # Relationships
            (self.mongo_db["relationships"], [
                IndexModel([
                    ("environment_id", 1),
                    ("source_twin_id", 1),
                    ("target_twin_id", 1),
                    ("relationship_name", 1)
                ], unique=True)
            ])
        ]
        
        for collection, index_models in indexes:
            await collection.create_indexes(index_models)
        logger.info("MongoDB indexes created")
    
    async def _create_neo4j_constraints(self):
        """Create Neo4j constraints."""
        constraints = [
            "CREATE CONSTRAINT twin_id_unique IF NOT EXISTS FOR (t:Twin) REQUIRE t.twin_id IS UNIQUE",
            "CREATE INDEX twin_environment_id IF NOT EXISTS FOR (t:Twin) ON (t.environment_id)",
            "CREATE INDEX twin_model_id IF NOT EXISTS FOR (t:Twin) ON (t.model_id)"
        ]
        
        async with self.neo4j_driver.session() as session:
            for constraint in constraints:
                try:
                    await session.run(constraint)
                except Neo4jError as e:
                    if "already exists" not in str(e):
                        raise

# --- Base Service Class ---
class BaseService(ABC):
    """Base service class with common functionality."""
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self._cache = {}  # Simple cache for existence checks
        
    @property
    @abstractmethod
    def collection_name(self) -> str:
        """Return the MongoDB collection name."""
        pass
    
    @property
    def collection(self):
        """Return the MongoDB collection."""
        return self.db[self.collection_name]
    
    async def check_environment_exists(self, environment_id: str) -> bool:
        """Check if environment exists (with caching)."""
        cache_key = f"env:{environment_id}"
        if cache_key in self._cache:
            return self._cache[cache_key]
            
        exists = await self.db["environments"].count_documents(
            {"environment_id": environment_id}, limit=1
        ) > 0
        self._cache[cache_key] = exists
        return exists
    
    def clear_cache(self, pattern: Optional[str] = None):
        """Clear cache entries."""
        if pattern:
            self._cache = {k: v for k, v in self._cache.items() if not k.startswith(pattern)}
        else:
            self._cache.clear()

# --- Service Classes ---
class EnvironmentService(BaseService):
    """Service for managing environments."""
    
    collection_name = "environments"
    
    def __init__(self, db: AsyncIOMotorDatabase, twin_service: 'TwinService'):
        super().__init__(db)
        self.twin_service = twin_service
        
    async def create(self, data: EnvironmentCreate) -> Environment:
        """Create a new environment."""
        # Check if already exists
        if await self.collection.count_documents({"environment_id": data.environment_id}, limit=1) > 0:
            raise ResourceConflictError("Environment", data.environment_id)
        
        environment = Environment(**data.model_dump())
        await self.collection.insert_one(environment.model_dump_mongo())
        
        # Update cache
        self._cache[f"env:{data.environment_id}"] = True
        
        logger.info(f"Created environment: {data.environment_id}")
        return environment
    
    async def get(self, environment_id: str) -> Environment:
        """Get an environment by ID."""
        doc = await self.collection.find_one({"environment_id": environment_id})
        if not doc:
            raise ResourceNotFoundError("Environment", environment_id)
        return Environment(**remove_mongo_id(doc))
    
    async def list(self, skip: int = 0, limit: int = 100) -> PaginatedResponse[Environment]:
        """List environments with pagination."""
        total_count = await self.collection.count_documents({})
        
        cursor = self.collection.find({}).sort("created_at", DESCENDING).skip(skip).limit(limit)
        items = [Environment(**remove_mongo_id(doc)) async for doc in cursor]
        
        return PaginatedResponse(
            items=items,
            total_count=total_count,
            page=skip // limit + 1,
            page_size=limit,
            has_next=skip + limit < total_count,
            has_previous=skip > 0
        )
    
    async def update(self, environment_id: str, data: EnvironmentUpdate) -> Environment:
        """Update an environment."""
        update_data = data.model_dump(exclude_unset=True)
        if not update_data:
            raise ValidationError("No fields to update")
        
        update_data["updated_at"] = datetime.utcnow()
        
        result = await self.collection.update_one(
            {"environment_id": environment_id},
            {"$set": update_data}
        )
        
        if result.matched_count == 0:
            raise ResourceNotFoundError("Environment", environment_id)
        
        return await self.get(environment_id)
    
    async def delete(self, environment_id: str) -> OperationResponse:
        """Delete an environment and all its resources."""
        # Check if exists
        if not await self.check_environment_exists(environment_id):
            raise ResourceNotFoundError("Environment", environment_id)
        
        # Delete all twins (this will cascade to Neo4j)
        twin_ids = await self.db["twins"].distinct("twin_id", {"environment_id": environment_id})
        for twin_id in twin_ids:
            await self.twin_service.delete(environment_id, twin_id)
        
        # Delete other resources
        collections_to_clean = ["devices", "models", "workflows", "relationships"]
        for collection_name in collections_to_clean:
            result = await self.db[collection_name].delete_many({"environment_id": environment_id})
            if result.deleted_count > 0:
                logger.info(f"Deleted {result.deleted_count} {collection_name} from environment {environment_id}")
        
        # Delete environment
        await self.collection.delete_one({"environment_id": environment_id})
        
        # Clear cache
        self.clear_cache(f"env:{environment_id}")
        
        logger.info(f"Deleted environment: {environment_id}")
        return OperationResponse(
            success=True,
            message=f"Environment '{environment_id}' and all its resources deleted successfully"
        )

class DeviceService(BaseService):
    """Service for managing devices."""
    
    collection_name = "devices"
    
    async def create(self, environment_id: str, data: DeviceCreate) -> Device:
        """Create a new device."""
        # Check environment exists
        if not await self.check_environment_exists(environment_id):
            raise ResourceNotFoundError("Environment", environment_id)
        
        # Check if device already exists
        if await self.collection.count_documents({
            "environment_id": environment_id,
            "device_id": data.device_id
        }, limit=1) > 0:
            raise ResourceConflictError("Device", data.device_id, environment_id)
        
        device = Device(environment_id=environment_id, **data.model_dump())
        await self.collection.insert_one(device.model_dump_mongo())
        
        logger.info(f"Created device: {data.device_id} in environment: {environment_id}")
        return device
    
    async def get(self, environment_id: str, device_id: str) -> Device:
        """Get a device by ID."""
        doc = await self.collection.find_one({
            "environment_id": environment_id,
            "device_id": device_id
        })
        if not doc:
            raise ResourceNotFoundError("Device", device_id, environment_id)
        return Device(**remove_mongo_id(doc))
    
    async def list(self, environment_id: str, skip: int = 0, limit: int = 100) -> PaginatedResponse[Device]:
        """List devices in an environment."""
        query = {"environment_id": environment_id}
        total_count = await self.collection.count_documents(query)
        
        cursor = self.collection.find(query).sort("created_at", DESCENDING).skip(skip).limit(limit)
        items = [Device(**remove_mongo_id(doc)) async for doc in cursor]
        
        return PaginatedResponse(
            items=items,
            total_count=total_count,
            page=skip // limit + 1,
            page_size=limit,
            has_next=skip + limit < total_count,
            has_previous=skip > 0
        )
    # TODO: 待修改，是否需要增加schema
    async def update(self, environment_id: str, device_id: str, data: DeviceUpdate) -> Device:
        """Update a device."""
        update_data = data.model_dump(exclude_unset=True)
        if not update_data:
            raise ValidationError("No fields to update")
        
        update_data["updated_at"] = datetime.utcnow()
        
        result = await self.collection.update_one(
            {"environment_id": environment_id, "device_id": device_id},
            {"$set": update_data}
        )
        
        if result.matched_count == 0:
            raise ResourceNotFoundError("Device", device_id, environment_id)
        
        return await self.get(environment_id, device_id)
    
    async def delete(self, environment_id: str, device_id: str) -> OperationResponse:
        """Delete a device."""
        result = await self.collection.delete_one({
            "environment_id": environment_id,
            "device_id": device_id
        })
        
        if result.deleted_count == 0:
            raise ResourceNotFoundError("Device", device_id, environment_id)
        
        logger.info(f"Deleted device: {device_id} from environment: {environment_id}")
        return OperationResponse(
            success=True,
            message=f"Device '{device_id}' deleted successfully"
        )


class ModelService(BaseService):
    """Service for managing models."""
    
    collection_name = "models"
    
    async def create(self, environment_id: str, data: ModelCreate) -> Model:
        """Create a new model."""
        # Check environment exists
        if not await self.check_environment_exists(environment_id):
            raise ResourceNotFoundError("Environment", environment_id)
        
        # Check if model already exists
        if await self.collection.count_documents({
            "environment_id": environment_id,
            "model_id": data.model_id
        }, limit=1) > 0:
            raise ResourceConflictError("Model", data.model_id, environment_id)
        
        model = Model(environment_id=environment_id, **data.model_dump())
        await self.collection.insert_one(model.model_dump_mongo())
        
        logger.info(f"Created model: {data.model_id} in environment: {environment_id}")
        return model
    
    async def get(self, environment_id: str, model_id: str) -> Model:
        """Get a model by ID."""
        doc = await self.collection.find_one({
            "environment_id": environment_id,
            "model_id": model_id
        })
        if not doc:
            raise ResourceNotFoundError("Model", model_id, environment_id)
        return Model(**remove_mongo_id(doc))
    
    async def list(self, environment_id: str, skip: int = 0, limit: int = 100) -> PaginatedResponse[Model]:
        """List models in an environment."""
        query = {"environment_id": environment_id}
        total_count = await self.collection.count_documents(query)
        
        cursor = self.collection.find(query).sort("created_at", DESCENDING).skip(skip).limit(limit)
        items = [Model(**remove_mongo_id(doc)) async for doc in cursor]
        
        return PaginatedResponse(
            items=items,
            total_count=total_count,
            page=skip // limit + 1,
            page_size=limit,
            has_next=skip + limit < total_count,
            has_previous=skip > 0
        )
    
    async def update(self, environment_id: str, model_id: str, data: ModelUpdate) -> Model:
        """Update a model."""
        update_data = data.model_dump(exclude_unset=True)
        if not update_data:
            raise ValidationError("No fields to update")
        
        update_data["updated_at"] = datetime.utcnow()
        
        result = await self.collection.update_one(
            {"environment_id": environment_id, "model_id": model_id},
            {"$set": update_data}
        )
        
        if result.matched_count == 0:
            raise ResourceNotFoundError("Model", model_id, environment_id)
        
        return await self.get(environment_id, model_id)
    
    async def delete(self, environment_id: str, model_id: str) -> OperationResponse:
        """Delete a model."""
        # Check if model has twins
        twin_count = await self.db["twins"].count_documents({
            "environment_id": environment_id,
            "model_id": model_id
        }, limit=1)
        
        if twin_count > 0:
            raise DependencyError(
                f"Cannot delete model '{model_id}'. "
                "There are digital twins using this model. Please delete associated twins first."
            )
        
        result = await self.collection.delete_one({
            "environment_id": environment_id,
            "model_id": model_id
        })
        
        if result.deleted_count == 0:
            raise ResourceNotFoundError("Model", model_id, environment_id)
        
        logger.info(f"Deleted model: {model_id} from environment: {environment_id}")
        return OperationResponse(
            success=True,
            message=f"Model '{model_id}' deleted successfully"
        )

class TwinService(BaseService):
    """Service for managing digital twins."""
    
    collection_name = "twins"
    
    def __init__(self, db: AsyncIOMotorDatabase, neo4j_driver: AsyncGraphDatabase):
        super().__init__(db)
        self.neo4j_driver = neo4j_driver

    # TODO: 是否需要验证
    async def validate_properties(self, environment_id: str, model_id: str, properties: Dict[str, Any]):
        """Validate twin properties against model definition."""
        model_doc = await self.db["models"].find_one({
            "environment_id": environment_id,
            "model_id": model_id
        })
        
        if not model_doc:
            raise ResourceNotFoundError("Model", model_id, environment_id)
        
        model = Model(**remove_mongo_id(model_doc))
        
        # Check required properties
        for prop_name, prop_def in model.properties.items():
            if prop_def.is_required and prop_name not in properties:
                # Check if there's a default value
                if prop_def.default_value is not None:
                    properties[prop_name] = prop_def.default_value
                else:
                    raise ValidationError(f"Required property '{prop_name}' is missing")
        
        # Validate property types
        for prop_name, prop_value in properties.items():
            if prop_name not in model.properties:
                raise ValidationError(f"Property '{prop_name}' not defined in model '{model_id}'")
            
            prop_def = model.properties[prop_name]
            
            # Type validation
            if prop_def.type == PropertyType.STRING and not isinstance(prop_value, str):
                raise ValidationError(f"Property '{prop_name}' must be a string")
            elif prop_def.type == PropertyType.NUMBER and not isinstance(prop_value, (int, float)):
                raise ValidationError(f"Property '{prop_name}' must be a number")
            elif prop_def.type == PropertyType.BOOLEAN and not isinstance(prop_value, bool):
                raise ValidationError(f"Property '{prop_name}' must be a boolean")
            
            # Constraint validation
            if prop_def.constraints:
                if "min" in prop_def.constraints and prop_value < prop_def.constraints["min"]:
                    raise ValidationError(f"Property '{prop_name}' must be >= {prop_def.constraints['min']}")
                if "max" in prop_def.constraints and prop_value > prop_def.constraints["max"]:
                    raise ValidationError(f"Property '{prop_name}' must be <= {prop_def.constraints['max']}")
                if "pattern" in prop_def.constraints and isinstance(prop_value, str):
                    if not re.match(prop_def.constraints["pattern"], prop_value):
                        raise ValidationError(f"Property '{prop_name}' does not match required pattern")
    
    async def create(self, environment_id: str, data: TwinCreate) -> Twin:
        """Create a new digital twin."""
        # Check environment exists
        if not await self.check_environment_exists(environment_id):
            raise ResourceNotFoundError("Environment", environment_id)
        
        # Check if twin already exists
        if await self.collection.count_documents({
            "environment_id": environment_id,
            "twin_id": data.twin_id
        }, limit=1) > 0:
            raise ResourceConflictError("Twin", data.twin_id, environment_id)
        
        # Validate properties against model
        await self.validate_properties(environment_id, data.model_id, data.properties)
        
        twin = Twin(environment_id=environment_id, **data.model_dump())
        
        # Create in MongoDB
        await self.collection.insert_one(twin.model_dump_mongo())
        
        # Create in Neo4j
        try:
            async with self.neo4j_driver.session() as session:
                await session.run(
                    """
                    CREATE (t:Twin {
                        twin_id: $twin_id,
                        environment_id: $environment_id,
                        model_id: $model_id
                    })
                    """,
                    twin_id=twin.twin_id,
                    environment_id=twin.environment_id,
                    model_id=twin.model_id)
        except Exception as e:
            # Rollback MongoDB operation
            await self.collection.delete_one({"twin_id": data.twin_id, "environment_id": environment_id})
            logger.error(f"Failed to create twin in Neo4j: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create twin in graph database"
            )
        
        logger.info(f"Created twin: {data.twin_id} in environment: {environment_id}")
        return twin
    
    async def get(self, environment_id: str, twin_id: str) -> Twin:
        """Get a twin by ID."""
        doc = await self.collection.find_one({
            "environment_id": environment_id,
            "twin_id": twin_id
        })
        if not doc:
            raise ResourceNotFoundError("Twin", twin_id, environment_id)
        return Twin(**remove_mongo_id(doc))
    
    async def list(self, environment_id: str, model_id: Optional[str] = None, 
                   skip: int = 0, limit: int = 100) -> PaginatedResponse[Twin]:
        """List twins in an environment."""
        query = {"environment_id": environment_id}
        if model_id:
            query["model_id"] = model_id
        
        total_count = await self.collection.count_documents(query)
        
        cursor = self.collection.find(query).sort("created_at", DESCENDING).skip(skip).limit(limit)
        items = [Twin(**remove_mongo_id(doc)) async for doc in cursor]
        
        return PaginatedResponse(
            items=items,
            total_count=total_count,
            page=skip // limit + 1,
            page_size=limit,
            has_next=skip + limit < total_count,
            has_previous=skip > 0
        )
    
    # TODO: twin的update到底可以update什么
    async def update(self, environment_id: str, twin_id: str, data: TwinUpdate) -> Twin:
        """Update a twin."""
        # Get existing twin
        existing = await self.get(environment_id, twin_id)
        
        update_data = data.model_dump(exclude_unset=True)
        if not update_data:
            raise ValidationError("No fields to update")
        
        # If updating properties, validate them
        if "properties" in update_data:
            merged_properties = {**existing.properties, **update_data["properties"]}
            await self.validate_properties(environment_id, existing.model_id, merged_properties)
            update_data["properties"] = merged_properties
        
        update_data["updated_at"] = datetime.utcnow()
        
        # Update MongoDB
        result = await self.collection.update_one(
            {"environment_id": environment_id, "twin_id": twin_id},
            {"$set": update_data}
        )
        
        if result.matched_count == 0:
            raise ResourceNotFoundError("Twin", twin_id, environment_id)
        
        return await self.get(environment_id, twin_id)
    
    async def delete(self, environment_id: str, twin_id: str) -> OperationResponse:
        """Delete a twin and its relationships."""
        # Check if exists
        if not await self.collection.count_documents({
            "environment_id": environment_id,
            "twin_id": twin_id
        }, limit=1) > 0:
            raise ResourceNotFoundError("Twin", twin_id, environment_id)
        
        # Delete from Neo4j (including relationships)
        try:
            async with self.neo4j_driver.session() as session:
                result = await session.run(
                    """
                    MATCH (t:Twin {twin_id: $twin_id, environment_id: $environment_id})
                    DETACH DELETE t
                    RETURN count(t) AS deleted_count
                    """,
                    twin_id=twin_id,
                    environment_id=environment_id
                )
                record = await result.single()
                if record and record["deleted_count"] == 0:
                    logger.warning(f"Twin {twin_id} not found in Neo4j")
        except Exception as e:
            logger.error(f"Failed to delete twin from Neo4j: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to delete twin from graph database"
            )
        
        # Delete from MongoDB
        result = await self.collection.delete_one({
            "environment_id": environment_id,
            "twin_id": twin_id
        })
        
        if result.deleted_count == 0:
            raise ResourceNotFoundError("Twin", twin_id, environment_id)
        
        # Also delete relationships from MongoDB
        await self.db["relationships"].delete_many({
            "environment_id": environment_id,
            "$or": [
                {"source_twin_id": twin_id},
                {"target_twin_id": twin_id}
            ]
        })
        
        logger.info(f"Deleted twin: {twin_id} from environment: {environment_id}")
        return OperationResponse(
            success=True,
            message=f"Twin '{twin_id}' and its relationships deleted successfully"
        )


class RelationshipService:
    """Service for managing relationships between twins."""
    
    def __init__(self, db: AsyncIOMotorDatabase, neo4j_driver: AsyncGraphDatabase):
        self.db = db
        self.collection = db["relationships"]
        self.neo4j_driver = neo4j_driver
    
    async def create(self, environment_id: str, data: RelationshipCreate) -> Relationship:
        """Create a new relationship between twins."""
        # Validate twins exist
        twins_collection = self.db["twins"]
        for twin_id in [data.source_twin_id, data.target_twin_id]:
            if not await twins_collection.count_documents({
                "environment_id": environment_id,
                "twin_id": twin_id
            }, limit=1) > 0:
                raise ResourceNotFoundError("Twin", twin_id, environment_id)
        
        # Check if relationship already exists
        if await self.collection.count_documents({
            "environment_id": environment_id,
            "source_twin_id": data.source_twin_id,
            "target_twin_id": data.target_twin_id,
            "relationship_name": data.relationship_name
        }, limit=1) > 0:
            raise ResourceConflictError(
                "Relationship",
                f"{data.relationship_name} between {data.source_twin_id} and {data.target_twin_id}",
                environment_id
            )
        
        #relationship = Relationship(environment_id=environment_id, **data.model_dump())
        
        # Create in MongoDB
        # await self.collection.insert_one(relationship.model_dump_mongo())
        
        # Create in Neo4j
        try:
            async with self.neo4j_driver.session() as session:
                query = f"""
                MATCH (a:Twin {{twin_id: $source_twin_id, environment_id: $environment_id}})
                MATCH (b:Twin {{twin_id: $target_twin_id, environment_id: $environment_id}})
                CREATE (a)-[r:`{data.relationship_name}` {{
                    environment_id: $environment_id
                }}]->(b)
                RETURN r
                """
                await session.run(
                    query,
                    source_twin_id=data.source_twin_id,
                    target_twin_id=data.target_twin_id,
                    environment_id=environment_id,

                )
        except Exception as e:
            # # Rollback MongoDB operation
            # await self.collection.delete_one({
            #     "environment_id": environment_id,
            #     "source_twin_id": data.source_twin_id,
            #     "target_twin_id": data.target_twin_id,
            #     "relationship_name": data.relationship_name
            # })
            logger.error(f"Failed to create relationship in Neo4j: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create relationship in graph database"
            )
        
        logger.info(f"Created relationship: {data.relationship_name} in environment: {environment_id}")
        return relationship
    
    # TODO：是否有必要有这个函数？
    async def get(self, environment_id: str, source_twin_id: str, 
                  target_twin_id: str, relationship_name: str) -> Relationship:
        """Get a specific relationship."""
        doc = await self.collection.find_one({
            "environment_id": environment_id,
            "source_twin_id": source_twin_id,
            "target_twin_id": target_twin_id,
            "relationship_name": relationship_name
        })
        if not doc:
            raise ResourceNotFoundError(
                "Relationship",
                f"{relationship_name} between {source_twin_id} and {target_twin_id}",
                environment_id
            )
        return Relationship(**remove_mongo_id(doc))
    
    async def list(self, environment_id: str, twin_id: Optional[str] = None,
                   skip: int = 0, limit: int = 100) -> PaginatedResponse[Relationship]:
        """List relationships in an environment."""
        query = {"environment_id": environment_id}
        if twin_id:
            query["$or"] = [
                {"source_twin_id": twin_id},
                {"target_twin_id": twin_id}
            ]
        
        total_count = await self.collection.count_documents(query)
        
        cursor = self.collection.find(query).sort("created_at", DESCENDING).skip(skip).limit(limit)
        items = [Relationship(**remove_mongo_id(doc)) async for doc in cursor]
        
        return PaginatedResponse(
            items=items,
            total_count=total_count,
            page=skip // limit + 1,
            page_size=limit,
            has_next=skip + limit < total_count,
            has_previous=skip > 0
        )
    
    async def delete(self, environment_id: str, source_twin_id: str,
                     target_twin_id: str, relationship_name: str) -> OperationResponse:
        """Delete a relationship."""
        # Delete from Neo4j
        try:
            async with self.neo4j_driver.session() as session:
                query = f"""
                MATCH (a:Twin {{twin_id: $source_twin_id, environment_id: $environment_id}})
                      -[r:`{relationship_name}`]->
                      (b:Twin {{twin_id: $target_twin_id, environment_id: $environment_id}})
                DELETE r
                RETURN count(r) AS deleted_count
                """
                result = await session.run(
                    query,
                    source_twin_id=source_twin_id,
                    target_twin_id=target_twin_id,
                    environment_id=environment_id
                )
                record = await result.single()
                if record and record["deleted_count"] == 0:
                    logger.warning(f"Relationship not found in Neo4j")
        except Exception as e:
            logger.error(f"Failed to delete relationship from Neo4j: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to delete relationship from graph database"
            )
        
        # # Delete from MongoDB
        # result = await self.collection.delete_one({
        #     "environment_id": environment_id,
        #     "source_twin_id": source_twin_id,
        #     "target_twin_id": target_twin_id,
        #     "relationship_name": relationship_name
        # })
        
        # if result.deleted_count == 0:
        #     raise ResourceNotFoundError(
        #         "Relationship",
        #         f"{relationship_name} between {source_twin_id} and {target_twin_id}",
        #         environment_id
        #     )
        
        logger.info(f"Deleted relationship: {relationship_name} from environment: {environment_id}")
        return OperationResponse(
            success=True,
            message=f"Relationship '{relationship_name}' deleted successfully"
        )

class WorkflowService(BaseService):
    """Service for managing workflows."""
    
    collection_name = "workflows"
    
    def validate_yaml_content(self, content: str) -> Dict[str, Any]:
        """Validate and parse YAML content."""
        try:
            yaml_data = yaml.safe_load(content)
            if yaml_data is None:
                raise ValidationError("YAML content is empty")
            if not isinstance(yaml_data, dict):
                raise ValidationError("YAML content must be a dictionary")
            return yaml_data
        except yaml.YAMLError as e:
            raise ValidationError(f"Invalid YAML syntax: {e}")
    
    def calculate_checksum(self, content: str) -> str:
        """Calculate SHA256 checksum of content."""
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    async def create(self, environment_id: str, data: WorkflowCreate) -> Workflow:
        """Create a new workflow."""
        # Check environment exists
        if not await self.check_environment_exists(environment_id):
            raise ResourceNotFoundError("Environment", environment_id)
        
        # Check if workflow already exists
        if await self.collection.count_documents({
            "environment_id": environment_id,
            "workflow_id": data.workflow_id
        }, limit=1) > 0:
            raise ResourceConflictError("Workflow", data.workflow_id, environment_id)
        
        # Validate YAML content
        self.validate_yaml_content(data.content)
        
        workflow = Workflow(
            environment_id=environment_id,
            **data.model_dump(),
            file_size=len(data.content.encode('utf-8')),
            checksum=self.calculate_checksum(data.content)
        )
        
        await self.collection.insert_one(workflow.model_dump_mongo())
        
        logger.info(f"Created workflow: {data.workflow_id} in environment: {environment_id}")
        return workflow
    
    async def upload(self, environment_id: str, workflow_id: str, 
                     file_name: str, content: bytes) -> Workflow:
        """Upload a workflow from file."""
        # Decode content
        try:
            yaml_content = content.decode('utf-8')
        except UnicodeDecodeError:
            raise ValidationError("File content is not valid UTF-8")
        
        # Create workflow
        data = WorkflowCreate(
            workflow_id=workflow_id,
            file_name=file_name,
            content=yaml_content
        )
        
        return await self.create(environment_id, data)
    
    async def get(self, environment_id: str, workflow_id: str) -> Workflow:
        """Get a workflow by ID."""
        doc = await self.collection.find_one({
            "environment_id": environment_id,
            "workflow_id": workflow_id
        })
        if not doc:
            raise ResourceNotFoundError("Workflow", workflow_id, environment_id)
        return Workflow(**remove_mongo_id(doc))
    
    async def list(self, environment_id: str, status: Optional[WorkflowStatus] = None,
                   workflow_type: Optional[WorkflowType] = None,
                   skip: int = 0, limit: int = 100) -> PaginatedResponse[WorkflowSummary]:
        """List workflows in an environment."""
        query = {"environment_id": environment_id}
        if status:
            query["status"] = status.value
        if workflow_type:
            query["workflow_type"] = workflow_type.value
        
        total_count = await self.collection.count_documents(query)
        
        cursor = self.collection.find(query).sort("created_at", DESCENDING).skip(skip).limit(limit)
        
        # Convert to summaries (without content)
        items = []
        async for doc in cursor:
            workflow = Workflow(**remove_mongo_id(doc))
            summary = WorkflowSummary(
                workflow_id=workflow.workflow_id,
                environment_id=workflow.environment_id,
                file_name=workflow.file_name,
                workflow_type=workflow.workflow_type,
                status=workflow.status,
                description=workflow.description,
                version=workflow.version,
                tags=workflow.tags,
                created_at=workflow.created_at,
                updated_at=workflow.updated_at,
                last_executed=workflow.last_executed,
                execution_count=workflow.execution_count,
                file_size=workflow.file_size
            )
            items.append(summary)
        
        return PaginatedResponse(
            items=items,
            total_count=total_count,
            page=skip // limit + 1,
            page_size=limit,
            has_next=skip + limit < total_count,
            has_previous=skip > 0
        )
    
    async def update(self, environment_id: str, workflow_id: str, data: WorkflowUpdate) -> Workflow:
        """Update a workflow."""
        update_data = data.model_dump(exclude_unset=True)
        if not update_data:
            raise ValidationError("No fields to update")
        
        # If updating content, validate and update metadata
        if "content" in update_data:
            self.validate_yaml_content(update_data["content"])
            update_data["file_size"] = len(update_data["content"].encode('utf-8'))
            update_data["checksum"] = self.calculate_checksum(update_data["content"])
        
        update_data["updated_at"] = datetime.utcnow()
        
        result = await self.collection.update_one(
            {"environment_id": environment_id, "workflow_id": workflow_id},
            {"$set": update_data}
        )
        
        if result.matched_count == 0:
            raise ResourceNotFoundError("Workflow", workflow_id, environment_id)
        
        return await self.get(environment_id, workflow_id)
    
    async def delete(self, environment_id: str, workflow_id: str) -> OperationResponse:
        """Delete a workflow."""
        result = await self.collection.delete_one({
            "environment_id": environment_id,
            "workflow_id": workflow_id
        })
        
        if result.deleted_count == 0:
            raise ResourceNotFoundError("Workflow", workflow_id, environment_id)
        
        logger.info(f"Deleted workflow: {workflow_id} from environment: {environment_id}")
        return OperationResponse(
            success=True,
            message=f"Workflow '{workflow_id}' deleted successfully"
        )
    
    async def activate(self, environment_id: str, workflow_id: str) -> Workflow:
        """Activate a workflow."""
        return await self.update(
            environment_id,
            workflow_id,
            WorkflowUpdate(status=WorkflowStatus.ACTIVE)
        )
    
    async def deactivate(self, environment_id: str, workflow_id: str) -> Workflow:
        """Deactivate a workflow."""
        return await self.update(
            environment_id,
            workflow_id,
            WorkflowUpdate(status=WorkflowStatus.INACTIVE)
        )


app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION,
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database clients
db_clients = DatabaseClients()

@app.on_event("startup")
async def startup():
    """Initialize application on startup."""
    await db_clients.initialize()
    logger.info("Application started successfully")

@app.on_event("shutdown")
async def shutdown():
    """Cleanup on shutdown."""
    await db_clients.cleanup()
    logger.info("Application shutdown complete")

# --- Dependency Injection ---
def get_db() -> AsyncIOMotorDatabase:
    """Get MongoDB database instance."""
    return db_clients.mongo_db

def get_neo4j_driver() -> AsyncGraphDatabase:
    """Get Neo4j driver instance."""
    return db_clients.neo4j_driver

def get_kafka_producer() -> AIOKafkaProducer:
    """Get Kafka producer instance."""
    return db_clients.kafka_producer

def get_influxdb_client() -> InfluxDBClient:
    """Get InfluxDB client instance."""
    return db_clients.influxdb_client

# Service dependencies
async def get_twin_service(
    db: AsyncIOMotorDatabase = Depends(get_db),
    neo4j_driver: AsyncGraphDatabase = Depends(get_neo4j_driver)
) -> TwinService:
    return TwinService(db, neo4j_driver)

async def get_environment_service(
    db: AsyncIOMotorDatabase = Depends(get_db),
    twin_service: TwinService = Depends(get_twin_service)
) -> EnvironmentService:
    return EnvironmentService(db, twin_service)

async def get_model_service(db: AsyncIOMotorDatabase = Depends(get_db)) -> ModelService:
    return ModelService(db)

async def get_device_service(db: AsyncIOMotorDatabase = Depends(get_db)) -> DeviceService:
    return DeviceService(db)

async def get_relationship_service(
    db: AsyncIOMotorDatabase = Depends(get_db),
    neo4j_driver: AsyncGraphDatabase = Depends(get_neo4j_driver)
) -> RelationshipService:
    return RelationshipService(db, neo4j_driver)

async def get_workflow_service(db: AsyncIOMotorDatabase = Depends(get_db)) -> WorkflowService:
    return WorkflowService(db)

# --- API Routes ---
# Environment endpoints
@app.post("/environments", response_model=Environment, status_code=status.HTTP_201_CREATED, tags=["Environments"])
async def create_environment(
    data: EnvironmentCreate,
    service: EnvironmentService = Depends(get_environment_service)
):
    """Create a new digital twin environment."""
    return await service.create(data)

@app.get("/environments", response_model=PaginatedResponse[Environment], tags=["Environments"])
async def list_environments(
    skip: int = Query(0, ge=0),
    limit: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
    service: EnvironmentService = Depends(get_environment_service)
):
    """List all digital twin environments."""
    return await service.list(skip, limit)

@app.get("/environments/{environment_id}", response_model=Environment, tags=["Environments"])
async def get_environment(
    environment_id: str = Path(..., description="Environment identifier"),
    service: EnvironmentService = Depends(get_environment_service)
):
    """Get a specific digital twin environment."""
    return await service.get(environment_id)

@app.put("/environments/{environment_id}", response_model=Environment, tags=["Environments"])
async def update_environment(
    environment_id: str = Path(..., description="Environment identifier"),
    data: EnvironmentUpdate = Body(...),
    service: EnvironmentService = Depends(get_environment_service)
):
    """Update a digital twin environment."""
    return await service.update(environment_id, data)

@app.delete("/environments/{environment_id}", response_model=OperationResponse, tags=["Environments"])
async def delete_environment(
    environment_id: str = Path(..., description="Environment identifier"),
    service: EnvironmentService = Depends(get_environment_service)
):
    """Delete a digital twin environment and all its resources."""
    return await service.delete(environment_id)

# Model endpoints
@app.post("/environments/{environment_id}/models", response_model=Model, status_code=status.HTTP_201_CREATED, tags=["Models"])
async def create_model(
    environment_id: str = Path(..., description="Environment identifier"),
    data: ModelCreate = Body(...),
    service: ModelService = Depends(get_model_service)
):
    """Create a new model definition."""
    return await service.create(environment_id, data)

@app.get("/environments/{environment_id}/models", response_model=PaginatedResponse[Model], tags=["Models"])
async def list_models(
    environment_id: str = Path(..., description="Environment identifier"),
    skip: int = Query(0, ge=0),
    limit: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
    service: ModelService = Depends(get_model_service)
):
    """List all models in an environment."""
    return await service.list(environment_id, skip, limit)

@app.get("/environments/{environment_id}/models/{model_id}", response_model=Model, tags=["Models"])
async def get_model(
    environment_id: str = Path(..., description="Environment identifier"),
    model_id: str = Path(..., description="Model identifier"),
    service: ModelService = Depends(get_model_service)
):
    """Get a specific model."""
    return await service.get(environment_id, model_id)

@app.put("/environments/{environment_id}/models/{model_id}", response_model=Model, tags=["Models"])
async def update_model(
    environment_id: str = Path(..., description="Environment identifier"),
    model_id: str = Path(..., description="Model identifier"),
    data: ModelUpdate = Body(...),
    service: ModelService = Depends(get_model_service)
):
    """Update a model."""
    return await service.update(environment_id, model_id, data)

@app.delete("/environments/{environment_id}/models/{model_id}", response_model=OperationResponse, tags=["Models"])
async def delete_model(
    environment_id: str = Path(..., description="Environment identifier"),
    model_id: str = Path(..., description="Model identifier"),
    service: ModelService = Depends(get_model_service)
):
    """Delete a model. Fails if twins exist using this model."""
    return await service.delete(environment_id, model_id)

# Twin endpoints
@app.post("/environments/{environment_id}/twins", response_model=Twin, status_code=status.HTTP_201_CREATED, tags=["Twins"])
async def create_twin(
    environment_id: str = Path(..., description="Environment identifier"),
    data: TwinCreate = Body(...),
    service: TwinService = Depends(get_twin_service)
):
    """Create a new digital twin instance."""
    return await service.create(environment_id, data)

@app.get("/environments/{environment_id}/twins", response_model=PaginatedResponse[Twin], tags=["Twins"])
async def list_twins(
    environment_id: str = Path(..., description="Environment identifier"),
    model_id: Optional[str] = Query(None, description="Filter by model ID"),
    skip: int = Query(0, ge=0),
    limit: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
    service: TwinService = Depends(get_twin_service)
):
    """List all twins in an environment."""
    return await service.list(environment_id, model_id, skip, limit)

@app.get("/environments/{environment_id}/twins/{twin_id}", response_model=Twin, tags=["Twins"])
async def get_twin(
    environment_id: str = Path(..., description="Environment identifier"),
    twin_id: str = Path(..., description="Twin identifier"),
    service: TwinService = Depends(get_twin_service)
):
    """Get a specific twin."""
    return await service.get(environment_id, twin_id)

@app.put("/environments/{environment_id}/twins/{twin_id}", response_model=Twin, tags=["Twins"])
async def update_twin(
    environment_id: str = Path(..., description="Environment identifier"),
    twin_id: str = Path(..., description="Twin identifier"),
    data: TwinUpdate = Body(...),
    service: TwinService = Depends(get_twin_service)
):
    """Update a twin."""
    return await service.update(environment_id, twin_id, data)

@app.delete("/environments/{environment_id}/twins/{twin_id}", response_model=OperationResponse, tags=["Twins"])
async def delete_twin(
    environment_id: str = Path(..., description="Environment identifier"),
    twin_id: str = Path(..., description="Twin identifier"),
    service: TwinService = Depends(get_twin_service)
):
    """Delete a twin and its relationships."""
    return await service.delete(environment_id, twin_id)

# Device endpoints
@app.post("/environments/{environment_id}/devices", response_model=Device, status_code=status.HTTP_201_CREATED, tags=["Devices"])
async def create_device(
    environment_id: str = Path(..., description="Environment identifier"),
    data: DeviceCreate = Body(...),
    service: DeviceService = Depends(get_device_service)
):
    """Create a new device."""
    return await service.create(environment_id, data)

@app.get("/environments/{environment_id}/devices", response_model=PaginatedResponse[Device], tags=["Devices"])
async def list_devices(
    environment_id: str = Path(..., description="Environment identifier"),
    skip: int = Query(0, ge=0),
    limit: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
    service: DeviceService = Depends(get_device_service)
):
    """List all devices in an environment."""
    return await service.list(environment_id, skip, limit)

@app.get("/environments/{environment_id}/devices/{device_id}", response_model=Device, tags=["Devices"])
async def get_device(
    environment_id: str = Path(..., description="Environment identifier"),
    device_id: str = Path(..., description="Device identifier"),
    service: DeviceService = Depends(get_device_service)
):
    """Get a specific device."""
    return await service.get(environment_id, device_id)

@app.put("/environments/{environment_id}/devices/{device_id}", response_model=Device, tags=["Devices"])
async def update_device(
    environment_id: str = Path(..., description="Environment identifier"),
    device_id: str = Path(..., description="Device identifier"),
    data: DeviceUpdate = Body(...),
    service: DeviceService = Depends(get_device_service)
):
    """Update a device."""
    return await service.update(environment_id, device_id, data)

@app.delete("/environments/{environment_id}/devices/{device_id}", response_model=OperationResponse, tags=["Devices"])
async def delete_device(
    environment_id: str = Path(..., description="Environment identifier"),
    device_id: str = Path(..., description="Device identifier"),
    service: DeviceService = Depends(get_device_service)
):
    """Delete a device."""
    return await service.delete(environment_id, device_id)

# Relationship endpoints
@app.post("/environments/{environment_id}/relationships", response_model=Relationship, status_code=status.HTTP_201_CREATED, tags=["Relationships"])
async def create_relationship(
    environment_id: str = Path(..., description="Environment identifier"),
    data: RelationshipCreate = Body(...),
    service: RelationshipService = Depends(get_relationship_service)
):
    """Create a new relationship between twins."""
    return await service.create(environment_id, data)

@app.get("/environments/{environment_id}/relationships", response_model=PaginatedResponse[Relationship], tags=["Relationships"])
async def list_relationships(
    environment_id: str = Path(..., description="Environment identifier"),
    twin_id: Optional[str] = Query(None, description="Filter by twin ID"),
    skip: int = Query(0, ge=0),
    limit: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
    service: RelationshipService = Depends(get_relationship_service)
):
    """List relationships in an environment."""
    return await service.list(environment_id, twin_id, skip, limit)

@app.get("/environments/{environment_id}/relationships/{source_twin_id}/{relationship_name}/{target_twin_id}", 
         response_model=Relationship, tags=["Relationships"])
async def get_relationship(
    environment_id: str = Path(..., description="Environment identifier"),
    source_twin_id: str = Path(..., description="Source twin identifier"),
    relationship_name: str = Path(..., description="Relationship name"),
    target_twin_id: str = Path(..., description="Target twin identifier"),
    service: RelationshipService = Depends(get_relationship_service)
):
    """Get a specific relationship."""
    return await service.get(environment_id, source_twin_id, target_twin_id, relationship_name)

@app.delete("/environments/{environment_id}/relationships/{source_twin_id}/{relationship_name}/{target_twin_id}", 
            response_model=OperationResponse, tags=["Relationships"])
async def delete_relationship(
    environment_id: str = Path(..., description="Environment identifier"),
    source_twin_id: str = Path(..., description="Source twin identifier"),
    relationship_name: str = Path(..., description="Relationship name"),
    target_twin_id: str = Path(..., description="Target twin identifier"),
    service: RelationshipService = Depends(get_relationship_service)
):
    """Delete a relationship."""
    return await service.delete(environment_id, source_twin_id, target_twin_id, relationship_name)

# Workflow endpoints
@app.post("/environments/{environment_id}/workflows", response_model=Workflow, status_code=status.HTTP_201_CREATED, tags=["Workflows"])
async def create_workflow(
    environment_id: str = Path(..., description="Environment identifier"),
    data: WorkflowCreate = Body(...),
    service: WorkflowService = Depends(get_workflow_service)
):
    """Create a new workflow."""
    return await service.create(environment_id, data)

@app.post("/environments/{environment_id}/workflows/upload", response_model=Workflow, status_code=status.HTTP_201_CREATED, tags=["Workflows"])
async def upload_workflow(
    environment_id: str = Path(..., description="Environment identifier"),
    workflow_id: str = Form(..., description="Workflow identifier"),
    file: UploadFile = File(..., description="YAML workflow file"),
    service: WorkflowService = Depends(get_workflow_service)
):
    """Upload a workflow from a YAML file."""
    content = await file.read()
    return await service.upload(environment_id, workflow_id, file.filename, content)

@app.get("/environments/{environment_id}/workflows", response_model=PaginatedResponse[WorkflowSummary], tags=["Workflows"])
async def list_workflows(
    environment_id: str = Path(..., description="Environment identifier"),
    status: Optional[WorkflowStatus] = Query(None, description="Filter by status"),
    workflow_type: Optional[WorkflowType] = Query(None, description="Filter by type"),
    skip: int = Query(0, ge=0),
    limit: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
    service: WorkflowService = Depends(get_workflow_service)
):
    """List workflows in an environment."""
    return await service.list(environment_id, status, workflow_type, skip, limit)

@app.get("/environments/{environment_id}/workflows/{workflow_id}", response_model=Workflow, tags=["Workflows"])
async def get_workflow(
    environment_id: str = Path(..., description="Environment identifier"),
    workflow_id: str = Path(..., description="Workflow identifier"),
    service: WorkflowService = Depends(get_workflow_service)
):
    """Get a specific workflow with full content."""
    return await service.get(environment_id, workflow_id)

@app.put("/environments/{environment_id}/workflows/{workflow_id}", response_model=Workflow, tags=["Workflows"])
async def update_workflow(
    environment_id: str = Path(..., description="Environment identifier"),
    workflow_id: str = Path(..., description="Workflow identifier"),
    data: WorkflowUpdate = Body(...),
    service: WorkflowService = Depends(get_workflow_service)
):
    """Update a workflow."""
    return await service.update(environment_id, workflow_id, data)

@app.delete("/environments/{environment_id}/workflows/{workflow_id}", response_model=OperationResponse, tags=["Workflows"])
async def delete_workflow(
    environment_id: str = Path(..., description="Environment identifier"),
    workflow_id: str = Path(..., description="Workflow identifier"),
    service: WorkflowService = Depends(get_workflow_service)
):
    """Delete a workflow."""
    return await service.delete(environment_id, workflow_id)

@app.post("/environments/{environment_id}/workflows/{workflow_id}/activate", response_model=Workflow, tags=["Workflows"])
async def activate_workflow(
    environment_id: str = Path(..., description="Environment identifier"),
    workflow_id: str = Path(..., description="Workflow identifier"),
    service: WorkflowService = Depends(get_workflow_service)
):
    """Activate a workflow."""
    return await service.activate(environment_id, workflow_id)

@app.post("/environments/{environment_id}/workflows/{workflow_id}/deactivate", response_model=Workflow, tags=["Workflows"])
async def deactivate_workflow(
    environment_id: str = Path(..., description="Environment identifier"),
    workflow_id: str = Path(..., description="Workflow identifier"),
    service: WorkflowService = Depends(get_workflow_service)
):
    """Deactivate a workflow."""
    return await service.deactivate(environment_id, workflow_id)

# Health check endpoint
@app.get("/health", tags=["Health"])
async def health_check():
    """Check application health."""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "version": settings.API_VERSION
    }

# Root endpoint
@app.get("/", tags=["General"])
async def root():
    """API root endpoint."""
    return {
        "title": settings.API_TITLE,
        "version": settings.API_VERSION,
        "docs": "/docs",
        "health": "/health"
    }