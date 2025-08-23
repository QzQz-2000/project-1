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

from pydantic import BaseModel, Field, validator
from pydantic_settings import BaseSettings
from pymongo.errors import DuplicateKeyError
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import DESCENDING, IndexModel
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from aiokafka import AIOKafkaProducer

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# settings
class Settings(BaseSettings):
    MONGO_URI: str = Field(default="mongodb://localhost:27017")
    MONGO_DB_NAME: str = Field(default="digital_twin_db")
    
    KAFKA_BROKER: str = Field(default="localhost:9092")
    
    INFLUXDB_URL: str = Field(default="http://localhost:8086")
    INFLUXDB_TOKEN: str = Field(default="mytoken")
    INFLUXDB_ORG: str = Field(default="myorg")
    INFLUXDB_BUCKET: str = Field(default="mybucket")
    
    API_TITLE: str = Field(default="Digital Twin Platform API")
    API_VERSION: str = Field(default="0.3.0")
    
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
        # ensure datetime fields are properly formatted
        for field in ['created_at', 'updated_at', 'last_executed', 'telemetry_last_updated']:
            if field in data and isinstance(data[field], datetime):
                data[field] = data[field].isoformat() + "Z"
        return data

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


# Properties
class PropertyType(str, Enum):
    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"
    OBJECT = "object"
    ARRAY = "array"

class PropertyDefinition(BaseModel):
    type: PropertyType
    unit: Optional[str] = None
    description: Optional[str] = None
    is_required: bool = False
    default_value: Optional[Any] = None

# Environment
class EnvironmentCreate(BaseModel):
    environment_id: str
    display_name: str
    description: Optional[str] = None
    
    # auto check
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

# Model - 定义twin的模板和属性结构
class ModelCreate(BaseModel):
    model_id: str
    display_name: str
    description: Optional[str] = None
    properties: Dict[str, PropertyDefinition] = Field(default_factory=dict)
    
    @validator('model_id')
    def validate_model_id(cls, v):
        return validate_identifier(v, 'model_id')

class ModelUpdate(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None

class Model(TimestampMixin):
    model_id: str
    environment_id: str
    display_name: str
    description: Optional[str] = None
    properties: Dict[str, PropertyDefinition] = Field(default_factory=dict)

class DeviceCreate(BaseModel):
    device_id: str
    display_name: str
    description: Optional[str] = None
    device_type: Optional[str] = None
    location: Optional[str] = None
    manufacturer: Optional[str] = None
    
    @validator('device_id')
    def validate_device_id(cls, v):
        return validate_identifier(v, 'device_id')

class DeviceUpdate(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None
    device_type: Optional[str] = None
    location: Optional[str] = None
    manufacturer: Optional[str] = None

class Device(TimestampMixin):
    device_id: str
    environment_id: str
    display_name: str
    description: Optional[str] = None
    device_type: Optional[str] = None
    location: Optional[str] = None
    manufacturer: Optional[str] = None

class TwinCreate(BaseModel):
    twin_id: str
    model_id: str
    display_name: Optional[str] = None
    properties: Optional[Dict[str, Any]] = Field(default_factory=dict)
    
    @validator('twin_id')
    def validate_twin_id(cls, v):
        return validate_identifier(v, 'twin_id')

class TwinUpdate(BaseModel):
    display_name: Optional[str] = None
    properties: Optional[Dict[str, Any]] = None
    telemetry_last_updated: Optional[datetime] = None

class Twin(TimestampMixin):
    twin_id: str
    environment_id: str
    model_id: str
    display_name: Optional[str] = None
    properties: Dict[str, Any] = Field(default_factory=dict)
    telemetry_last_updated: Optional[datetime] = None

class DeviceTwinMappingCreate(BaseModel):
    device_id: str
    twin_id: str
    mapping_type: str = "direct"
    description: Optional[str] = None

class DeviceTwinMappingUpdate(BaseModel):
    mapping_type: Optional[str] = None
    description: Optional[str] = None

class DeviceTwinMapping(TimestampMixin):
    device_id: str
    twin_id: str
    environment_id: str
    mapping_type: str = "direct"
    description: Optional[str] = None

class RelationshipCreate(BaseModel):
    source_twin_id: str
    target_twin_id: str
    relationship_name: str
    properties: Optional[Dict[str, Any]] = Field(default_factory=dict)
    
    @validator('relationship_name')
    def validate_relationship_name(cls, v):
        if not REL_NAME_PATTERN.match(v):
            raise ValueError("Relationship name must contain only letters, numbers, and underscores, and start with a letter")
        return v

class RelationshipUpdate(BaseModel):
    properties: Optional[Dict[str, Any]] = None

class Relationship(TimestampMixin):
    source_twin_id: str
    target_twin_id: str
    relationship_name: str
    environment_id: str
    properties: Dict[str, Any] = Field(default_factory=dict)

class TreeNode(BaseModel):
    id: str
    label: str
    type: str
    children: List['TreeNode'] = Field(default_factory=list)
    metadata: Optional[Dict[str, Any]] = None

TreeNode.model_rebuild()

class TreeGraph(BaseModel):
    nodes: List[TreeNode]
    relationships: List[Relationship]

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
            # Devices
            (self.mongo_db["devices"], [IndexModel([("environment_id", 1), ("device_id", 1)], unique=True)]),
            # Twins
            (self.mongo_db["twins"], [
                IndexModel([("environment_id", 1), ("twin_id", 1)], unique=True),
                IndexModel([("environment_id", 1), ("model_id", 1)])
            ]),
            # Device-Twin Mappings
            (self.mongo_db["device_twin_mappings"], [
                IndexModel([("environment_id", 1), ("device_id", 1), ("twin_id", 1)], unique=True),
                IndexModel([("environment_id", 1), ("device_id", 1)]),
                IndexModel([("environment_id", 1), ("twin_id", 1)])
            ]),
            (self.mongo_db["relationships"], [
                IndexModel([
                    ("environment_id", 1),
                    ("source_twin_id", 1),
                    ("target_twin_id", 1)
                ], unique=True),
                IndexModel([("environment_id", 1), ("source_twin_id", 1)]),
                IndexModel([("environment_id", 1), ("target_twin_id", 1)])
            ])
        ]
        
        for collection, index_models in indexes:
            await collection.create_indexes(index_models)
        logger.info("MongoDB indexes created")

class BaseService(ABC):
    """Base service class with common functionality."""
    
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self._cache = {}
        
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
        
        # Delete all twins (this will cascade to relationships)
        twin_ids = await self.db["twins"].distinct("twin_id", {"environment_id": environment_id})
        for twin_id in twin_ids:
            await self.twin_service.delete(environment_id, twin_id)
        
        # Delete other resources
        collections_to_clean = ["devices", "models", "relationships", "device_twin_mappings"]
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
    """Service for managing devices"""
    
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
        # Check if device has mappings to twins
        mapping_count = await self.db["device_twin_mappings"].count_documents({
            "environment_id": environment_id,
            "device_id": device_id
        }, limit=1)
        
        if mapping_count > 0:
            raise DependencyError(
                f"Cannot delete device '{device_id}'. "
                "There are twins mapped to this device. Please remove mappings first."
            )
        
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
    """Service for managing digital twins - model的实例."""
    
    collection_name = "twins"
    
    def __init__(self, db: AsyncIOMotorDatabase):
        super().__init__(db)

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
        
        # Delete from MongoDB
        result = await self.collection.delete_one({
            "environment_id": environment_id,
            "twin_id": twin_id
        })
        
        if result.deleted_count == 0:
            raise ResourceNotFoundError("Twin", twin_id, environment_id)
        
        # Also delete relationships and mappings
        await self.db["relationships"].delete_many({
            "environment_id": environment_id,
            "$or": [
                {"source_twin_id": twin_id},
                {"target_twin_id": twin_id}
            ]
        })
        
        await self.db["device_twin_mappings"].delete_many({
            "environment_id": environment_id,
            "twin_id": twin_id
        })
        
        logger.info(f"Deleted twin: {twin_id} from environment: {environment_id}")
        return OperationResponse(
            success=True,
            message=f"Twin '{twin_id}' and its relationships deleted successfully"
        )

class DeviceTwinMappingService(BaseService):
    """Service for managing device-twin mappings."""
    
    collection_name = "device_twin_mappings"
    
    async def create(self, environment_id: str, data: DeviceTwinMappingCreate) -> DeviceTwinMapping:
        """Create a new device-twin mapping."""
        # Check environment exists
        if not await self.check_environment_exists(environment_id):
            raise ResourceNotFoundError("Environment", environment_id)
        
        # Check device exists
        if not await self.db["devices"].count_documents({
            "environment_id": environment_id,
            "device_id": data.device_id
        }, limit=1) > 0:
            raise ResourceNotFoundError("Device", data.device_id, environment_id)
        
        # Check twin exists
        if not await self.db["twins"].count_documents({
            "environment_id": environment_id,
            "twin_id": data.twin_id
        }, limit=1) > 0:
            raise ResourceNotFoundError("Twin", data.twin_id, environment_id)
        
        # Check if mapping already exists
        if await self.collection.count_documents({
            "environment_id": environment_id,
            "device_id": data.device_id,
            "twin_id": data.twin_id
        }, limit=1) > 0:
            raise ResourceConflictError(
                "Mapping",
                f"between device '{data.device_id}' and twin '{data.twin_id}'",
                environment_id
            )
        
        mapping = DeviceTwinMapping(environment_id=environment_id, **data.model_dump())
        await self.collection.insert_one(mapping.model_dump_mongo())
        
        logger.info(f"Created device-twin mapping: {data.device_id} -> {data.twin_id} in environment: {environment_id}")
        return mapping
    
    async def get_by_device(self, environment_id: str, device_id: str) -> List[DeviceTwinMapping]:
        """Get all mappings for a device."""
        cursor = self.collection.find({
            "environment_id": environment_id,
            "device_id": device_id
        }).sort("created_at", DESCENDING)
        
        return [DeviceTwinMapping(**remove_mongo_id(doc)) async for doc in cursor]
    
    async def get_by_twin(self, environment_id: str, twin_id: str) -> List[DeviceTwinMapping]:
        """Get all mappings for a twin."""
        cursor = self.collection.find({
            "environment_id": environment_id,
            "twin_id": twin_id
        }).sort("created_at", DESCENDING)
        
        return [DeviceTwinMapping(**remove_mongo_id(doc)) async for doc in cursor]
    
    async def list(self, environment_id: str, skip: int = 0, limit: int = 100) -> PaginatedResponse[DeviceTwinMapping]:
        """List all mappings in an environment."""
        query = {"environment_id": environment_id}
        total_count = await self.collection.count_documents(query)
        
        cursor = self.collection.find(query).sort("created_at", DESCENDING).skip(skip).limit(limit)
        items = [DeviceTwinMapping(**remove_mongo_id(doc)) async for doc in cursor]
        
        return PaginatedResponse(
            items=items,
            total_count=total_count,
            page=skip // limit + 1,
            page_size=limit,
            has_next=skip + limit < total_count,
            has_previous=skip > 0
        )
    
    async def update(self, environment_id: str, device_id: str, twin_id: str, data: DeviceTwinMappingUpdate) -> DeviceTwinMapping:
        """Update a device-twin mapping."""
        update_data = data.model_dump(exclude_unset=True)
        if not update_data:
            raise ValidationError("No fields to update")
        
        update_data["updated_at"] = datetime.utcnow()
        
        result = await self.collection.update_one(
            {"environment_id": environment_id, "device_id": device_id, "twin_id": twin_id},
            {"$set": update_data}
        )
        
        if result.matched_count == 0:
            raise ResourceNotFoundError(
                "Mapping",
                f"between device '{device_id}' and twin '{twin_id}'",
                environment_id
            )
        
        doc = await self.collection.find_one({
            "environment_id": environment_id,
            "device_id": device_id,
            "twin_id": twin_id
        })
        return DeviceTwinMapping(**remove_mongo_id(doc))
    
    async def delete(self, environment_id: str, device_id: str, twin_id: str) -> OperationResponse:
        """Delete a device-twin mapping."""
        result = await self.collection.delete_one({
            "environment_id": environment_id,
            "device_id": device_id,
            "twin_id": twin_id
        })
        
        if result.deleted_count == 0:
            raise ResourceNotFoundError(
                "Mapping",
                f"between device '{device_id}' and twin '{twin_id}'",
                environment_id
            )
        
        logger.info(f"Deleted device-twin mapping: {device_id} -> {twin_id} from environment: {environment_id}")
        return OperationResponse(
            success=True,
            message=f"Mapping between device '{device_id}' and twin '{twin_id}' deleted successfully"
        )

class RelationshipService(BaseService):
    """Service for managing relationships between twins"""
    
    collection_name = "relationships"
    
    def __init__(self, db: AsyncIOMotorDatabase):
        super().__init__(db)
        
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
        
        # Check if any relationship already exists between these twins (bidirectional)
        existing_relationship = await self.collection.find_one({
            "environment_id": environment_id,
            "$or": [
                {
                    "source_twin_id": data.source_twin_id,
                    "target_twin_id": data.target_twin_id
                },
                {
                    "source_twin_id": data.target_twin_id,
                    "target_twin_id": data.source_twin_id
                }
            ]
        })

        if existing_relationship:
            raise ResourceConflictError(
                "Relationship",
                f"A relationship already exists between {data.source_twin_id} and {data.target_twin_id}",
                environment_id
            )

        # Create relationship object
        relationship = Relationship(environment_id=environment_id, **data.model_dump())
        
        # Store in MongoDB
        await self.collection.insert_one(relationship.model_dump_mongo())
        logger.info(f"Created relationship: {data.relationship_name} in environment: {environment_id}")
        return relationship
    
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
    
    async def update(self, environment_id: str, source_twin_id: str,
                     target_twin_id: str, relationship_name: str, 
                     data: RelationshipUpdate) -> Relationship:
        """Update relationship properties."""
        update_data = data.model_dump(exclude_unset=True)
        if not update_data:
            raise ValidationError("No fields to update")
        
        update_data["updated_at"] = datetime.utcnow()
        
        result = await self.collection.update_one(
            {
                "environment_id": environment_id,
                "source_twin_id": source_twin_id,
                "target_twin_id": target_twin_id,
                "relationship_name": relationship_name
            },
            {"$set": update_data}
        )
        
        if result.matched_count == 0:
            raise ResourceNotFoundError(
                "Relationship",
                f"{relationship_name} between {source_twin_id} and {target_twin_id}",
                environment_id
            )
        
        return await self.get(environment_id, source_twin_id, target_twin_id, relationship_name)
    
    async def delete(self, environment_id: str, source_twin_id: str,
                     target_twin_id: str, relationship_name: str) -> OperationResponse:
        """Delete a relationship."""
        result = await self.collection.delete_one({
            "environment_id": environment_id,
            "source_twin_id": source_twin_id,
            "target_twin_id": target_twin_id,
            "relationship_name": relationship_name
        })
        
        if result.deleted_count == 0:
            raise ResourceNotFoundError(
                "Relationship",
                f"{relationship_name} between {source_twin_id} and {target_twin_id}",
                environment_id
            )
        
        logger.info(f"Deleted relationship: {relationship_name} from environment: {environment_id}")
        return OperationResponse(
            success=True,
            message=f"Relationship '{relationship_name}' deleted successfully"
        )

    async def get_tree_graph(self, environment_id: str, root_twin_id: Optional[str] = None) -> TreeGraph:
        
        cursor = self.collection.find({"environment_id": environment_id})
        relationships = [Relationship(**remove_mongo_id(doc)) async for doc in cursor]
        
        twins_cursor = self.db["twins"].find({"environment_id": environment_id})
        twins_dict = {}
        async for doc in twins_cursor:
            twin = Twin(**remove_mongo_id(doc))
            twins_dict[twin.twin_id] = twin
        
        if not relationships:
            nodes = []
            for twin_id, twin in twins_dict.items():
                nodes.append(TreeNode(
                    id=twin_id,
                    label=twin.display_name or twin_id,
                    type="twin",
                    metadata={
                        "model_id": twin.model_id,
                        "properties": twin.properties,
                        "created_at": twin.created_at.isoformat() if twin.created_at else None,
                        "telemetry_last_updated": twin.telemetry_last_updated.isoformat() if twin.telemetry_last_updated else None
                    }
                ))
            return TreeGraph(nodes=nodes, relationships=relationships)
        
        children_map = {}
        all_children = set()
        
        for rel in relationships:
            if rel.source_twin_id not in children_map:
                children_map[rel.source_twin_id] = []
            children_map[rel.source_twin_id].append(rel.target_twin_id)
            all_children.add(rel.target_twin_id)
        
        all_twins_in_relations = set()
        for rel in relationships:
            all_twins_in_relations.add(rel.source_twin_id)
            all_twins_in_relations.add(rel.target_twin_id)
        
        root_candidates = all_twins_in_relations - all_children
        
        if root_twin_id and root_twin_id in twins_dict:
            root_nodes = [root_twin_id]
        else:
            root_nodes = list(root_candidates)
        
        if not root_nodes and twins_dict:
            root_nodes = [next(iter(twins_dict.keys()))]
        
        def build_tree_node(twin_id: str, visited: set) -> Optional[TreeNode]:
            if twin_id in visited:
                return None
            
            twin = twins_dict.get(twin_id)
            if not twin:
                return None
            
            visited.add(twin_id)
            
            node = TreeNode(
                id=twin_id,
                label=twin.display_name or twin_id,
                type="twin",
                metadata={
                    "model_id": twin.model_id,
                    "properties": twin.properties,
                    "created_at": twin.created_at.isoformat() if twin.created_at else None,
                    "telemetry_last_updated": twin.telemetry_last_updated.isoformat() if twin.telemetry_last_updated else None
                }
            )
            
            children = children_map.get(twin_id, [])
            for child_id in children:
                child_node = build_tree_node(child_id, visited)
                if child_node:
                    node.children.append(child_node)
            
            visited.remove(twin_id)
            return node
        
        tree_nodes = []
        global_visited = set()
        
        for root_id in root_nodes:
            if root_id not in global_visited:
                root_node = build_tree_node(root_id, set())
                if root_node:
                    tree_nodes.append(root_node)
                    def collect_visited(node):
                        global_visited.add(node.id)
                        for child in node.children:
                            collect_visited(child)
                    collect_visited(root_node)
        
        for twin_id, twin in twins_dict.items():
            if twin_id not in global_visited:
                tree_nodes.append(TreeNode(
                    id=twin_id,
                    label=twin.display_name or twin_id,
                    type="twin",
                    metadata={
                        "model_id": twin.model_id,
                        "properties": twin.properties,
                        "created_at": twin.created_at.isoformat() if twin.created_at else None,
                        "telemetry_last_updated": twin.telemetry_last_updated.isoformat() if twin.telemetry_last_updated else None
                    }
                ))
        
        return TreeGraph(nodes=tree_nodes, relationships=relationships)
    
    async def get_statistics(self, environment_id: str) -> Dict[str, Any]:
        """Get relationship statistics for an environment."""
        pipeline = [
            {"$match": {"environment_id": environment_id}},
            {
                "$group": {
                    "_id": None,
                    "total_relationships": {"$sum": 1},
                    "unique_twins": {
                        "$addToSet": {
                            "$setUnion": [
                                ["$source_twin_id"],
                                ["$target_twin_id"]
                            ]
                        }
                    },
                    "relationship_types": {"$addToSet": "$relationship_name"}
                }
            },
            {
                "$project": {
                    "total_relationships": 1,
                    "unique_twins_count": {"$size": {"$arrayElemAt": ["$unique_twins", 0]}},
                    "relationship_types_count": {"$size": "$relationship_types"},
                    "relationship_types": 1
                }
            }
        ]
        
        cursor = self.collection.aggregate(pipeline)
        result = await cursor.to_list(length=1)
        
        if result:
            stats = result[0]
            return {
                "total_relationships": stats.get("total_relationships", 0),
                "unique_twins_count": stats.get("unique_twins_count", 0),
                "relationship_types_count": stats.get("relationship_types_count", 0),
                "relationship_types": stats.get("relationship_types", [])
            }
        else:
            return {
                "total_relationships": 0,
                "unique_twins_count": 0,
                "relationship_types_count": 0,
                "relationship_types": []
            }

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

def get_kafka_producer() -> AIOKafkaProducer:
    """Get Kafka producer instance."""
    return db_clients.kafka_producer

def get_influxdb_client() -> InfluxDBClient:
    """Get InfluxDB client instance."""
    return db_clients.influxdb_client

# Service dependencies
async def get_twin_service(
    db: AsyncIOMotorDatabase = Depends(get_db)
) -> TwinService:
    return TwinService(db)

async def get_environment_service(
    db: AsyncIOMotorDatabase = Depends(get_db),
    twin_service: TwinService = Depends(get_twin_service)
) -> EnvironmentService:
    return EnvironmentService(db, twin_service)

async def get_model_service(db: AsyncIOMotorDatabase = Depends(get_db)) -> ModelService:
    return ModelService(db)

async def get_device_service(db: AsyncIOMotorDatabase = Depends(get_db)) -> DeviceService:
    return DeviceService(db)

async def get_device_twin_mapping_service(
    db: AsyncIOMotorDatabase = Depends(get_db)
) -> DeviceTwinMappingService:
    return DeviceTwinMappingService(db)

async def get_relationship_service(
    db: AsyncIOMotorDatabase = Depends(get_db)
) -> RelationshipService:
    return RelationshipService(db)


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

# Device endpoints (仅存储基本信息)
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

# Twin endpoints (model的实例，包含属性)
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


@app.post("/environments/{environment_id}/twins/batch-update", 
         response_model=Dict[str, int], tags=["Twins"])
async def batch_update_twins(
    environment_id: str = Path(..., description="Environment identifier"),
    updates: List[Dict] = Body(..., description="Batch update data"),
    service: TwinService = Depends(get_twin_service)
):
    success_count = 0
    failure_count = 0
    
    for update_data in updates:
        try:
            twin_id = update_data["twin_id"]
            properties = update_data.get("properties", {})
            telemetry_last_updated = update_data.get("telemetry_last_updated")
            
            update_obj = TwinUpdate(
                properties=properties,
                telemetry_last_updated=telemetry_last_updated
            )
            
            await service.update(environment_id, twin_id, update_obj)
            success_count += 1
            
        except Exception as e:
            logger.error(f"Batch update of Twin failed: {e}")
            failure_count += 1
    
    return {
        "success_count": success_count,
        "failure_count": failure_count,
        "total_count": len(updates)
    }

@app.delete("/environments/{environment_id}/twins/{twin_id}", response_model=OperationResponse, tags=["Twins"])
async def delete_twin(
    environment_id: str = Path(..., description="Environment identifier"),
    twin_id: str = Path(..., description="Twin identifier"),
    service: TwinService = Depends(get_twin_service)
):
    """Delete a twin and its relationships."""
    return await service.delete(environment_id, twin_id)

# Device-Twin Mapping endpoints
@app.post("/environments/{environment_id}/mappings", response_model=DeviceTwinMapping, status_code=status.HTTP_201_CREATED, tags=["Device-Twin Mappings"])
async def create_device_twin_mapping(
    environment_id: str = Path(..., description="Environment identifier"),
    data: DeviceTwinMappingCreate = Body(...),
    service: DeviceTwinMappingService = Depends(get_device_twin_mapping_service)
):
    """Create a new device-twin mapping."""
    return await service.create(environment_id, data)

@app.get("/environments/{environment_id}/mappings", response_model=PaginatedResponse[DeviceTwinMapping], tags=["Device-Twin Mappings"])
async def list_device_twin_mappings(
    environment_id: str = Path(..., description="Environment identifier"),
    skip: int = Query(0, ge=0),
    limit: int = Query(settings.DEFAULT_PAGE_SIZE, ge=1, le=settings.MAX_PAGE_SIZE),
    service: DeviceTwinMappingService = Depends(get_device_twin_mapping_service)
):
    """List all device-twin mappings in an environment."""
    return await service.list(environment_id, skip, limit)

@app.get("/environments/{environment_id}/mappings/device/{device_id}", response_model=List[DeviceTwinMapping], tags=["Device-Twin Mappings"])
async def get_device_mappings(
    environment_id: str = Path(..., description="Environment identifier"),
    device_id: str = Path(..., description="Device identifier"),
    service: DeviceTwinMappingService = Depends(get_device_twin_mapping_service)
):
    """Get all mappings for a specific device."""
    return await service.get_by_device(environment_id, device_id)

@app.get("/environments/{environment_id}/mappings/twin/{twin_id}", response_model=List[DeviceTwinMapping], tags=["Device-Twin Mappings"])
async def get_twin_mappings(
    environment_id: str = Path(..., description="Environment identifier"),
    twin_id: str = Path(..., description="Twin identifier"),
    service: DeviceTwinMappingService = Depends(get_device_twin_mapping_service)
):
    """Get all mappings for a specific twin."""
    return await service.get_by_twin(environment_id, twin_id)

@app.put("/environments/{environment_id}/mappings/{device_id}/{twin_id}", response_model=DeviceTwinMapping, tags=["Device-Twin Mappings"])
async def update_device_twin_mapping(
    environment_id: str = Path(..., description="Environment identifier"),
    device_id: str = Path(..., description="Device identifier"),
    twin_id: str = Path(..., description="Twin identifier"),
    data: DeviceTwinMappingUpdate = Body(...),
    service: DeviceTwinMappingService = Depends(get_device_twin_mapping_service)
):
    """Update a device-twin mapping."""
    return await service.update(environment_id, device_id, twin_id, data)

@app.delete("/environments/{environment_id}/mappings/{device_id}/{twin_id}", response_model=OperationResponse, tags=["Device-Twin Mappings"])
async def delete_device_twin_mapping(
    environment_id: str = Path(..., description="Environment identifier"),
    device_id: str = Path(..., description="Device identifier"),
    twin_id: str = Path(..., description="Twin identifier"),
    service: DeviceTwinMappingService = Depends(get_device_twin_mapping_service)
):
    """Delete a device-twin mapping."""
    return await service.delete(environment_id, device_id, twin_id)

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

@app.get("/environments/{environment_id}/relationships/{source_twin_id}/{target_twin_id}/{relationship_name}", 
         response_model=Relationship, tags=["Relationships"])
async def get_relationship(
    environment_id: str = Path(..., description="Environment identifier"),
    source_twin_id: str = Path(..., description="Source twin identifier"),
    target_twin_id: str = Path(..., description="Target twin identifier"),
    relationship_name: str = Path(..., description="Relationship name"),
    service: RelationshipService = Depends(get_relationship_service)
):
    """Get a specific relationship."""
    return await service.get(environment_id, source_twin_id, target_twin_id, relationship_name)

@app.put("/environments/{environment_id}/relationships/{source_twin_id}/{target_twin_id}/{relationship_name}", 
         response_model=Relationship, tags=["Relationships"])
async def update_relationship(
    environment_id: str = Path(..., description="Environment identifier"),
    source_twin_id: str = Path(..., description="Source twin identifier"),
    target_twin_id: str = Path(..., description="Target twin identifier"),
    relationship_name: str = Path(..., description="Relationship name"),
    data: RelationshipUpdate = Body(...),
    service: RelationshipService = Depends(get_relationship_service)
):
    """Update a relationship."""
    return await service.update(environment_id, source_twin_id, target_twin_id, relationship_name, data)

@app.delete("/environments/{environment_id}/relationships/{source_twin_id}/{target_twin_id}/{relationship_name}", 
            response_model=OperationResponse, tags=["Relationships"])
async def delete_relationship(
    environment_id: str = Path(..., description="Environment identifier"),
    source_twin_id: str = Path(..., description="Source twin identifier"),
    target_twin_id: str = Path(..., description="Target twin identifier"),
    relationship_name: str = Path(..., description="Relationship name"),
    service: RelationshipService = Depends(get_relationship_service)
):
    """Delete a relationship."""
    return await service.delete(environment_id, source_twin_id, target_twin_id, relationship_name)

# 树状图数据 endpoint
@app.get("/environments/{environment_id}/tree-graph", response_model=TreeGraph, tags=["Visualization"])
async def get_tree_graph(
    environment_id: str = Path(..., description="Environment identifier"),
    root_twin_id: Optional[str] = Query(None, description="Root twin ID for tree visualization"),
    service: RelationshipService = Depends(get_relationship_service)
):
    """Get tree graph data for frontend visualization."""
    return await service.get_tree_graph(environment_id, root_twin_id)

# 获取统计数据
@app.get("/environments/{environment_id}/relationships/stats", response_model=Dict[str, Any], tags=["Relationships"])
async def get_relationship_statistics(
    environment_id: str = Path(..., description="Environment identifier"),
    service: RelationshipService = Depends(get_relationship_service)
):
    """Get relationship statistics for an environment."""
    return await service.get_statistics(environment_id)

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