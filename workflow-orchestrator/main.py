# import asyncio
# import logging
# import signal
# import sys
# import uvicorn
# from orchestrator import WorkflowOrchestrator
# from executor import TaskExecutor
# from completer import CompletionHandler
# from config import config

# # 配置日志
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )
# logger = logging.getLogger(__name__)

# class WorkflowEngine:
#     """工作流引擎主类"""
    
#     def __init__(self):
#         self.orchestrator = WorkflowOrchestrator()
#         self.executor = TaskExecutor()
#         self.completer = CompletionHandler()
#         self.running = False
        
#     async def start(self):
#         """启动所有组件"""
#         logger.info("Starting Workflow Engine...")
        
#         # 启动所有组件
#         await self.orchestrator.start()
#         await self.executor.start()
#         await self.completer.start()
        
#         self.running = True
#         logger.info("All components started successfully")
        
#         # 并发运行所有组件
#         await asyncio.gather(
#             self.orchestrator.run(),
#             self.executor.run(),
#             self.completer.run(),
#             self.start_api_server()
#         )
    
#     async def start_api_server(self):
#         """启动API服务器"""
#         logger.info(f"Starting API server on port {config.API_PORT}")
        
#         config_uvicorn = uvicorn.Config(
#             "api:app",
#             host="0.0.0.0",
#             port=config.API_PORT,
#             log_level="info"
#         )
#         server = uvicorn.Server(config_uvicorn)
#         await server.serve()
    
#     async def stop(self):
#         """停止所有组件"""
#         if not self.running:
#             return
            
#         logger.info("Stopping Workflow Engine...")
        
#         await self.orchestrator.stop()
#         await self.executor.stop()
#         await self.completer.stop()
        
#         self.running = False
#         logger.info("Workflow Engine stopped")

# # 全局引擎实例
# engine = WorkflowEngine()

# def signal_handler(signum, frame):
#     """信号处理器"""
#     logger.info(f"Received signal {signum}, shutting down...")
#     asyncio.create_task(engine.stop())

# async def main():
#     """主函数"""
#     # 注册信号处理器
#     signal.signal(signal.SIGINT, signal_handler)
#     signal.signal(signal.SIGTERM, signal_handler)
    
#     try:
#         await engine.start()
#     except KeyboardInterrupt:
#         logger.info("Received keyboard interrupt")
#     except Exception as e:
#         logger.error(f"Engine error: {e}")
#     finally:
#         await engine.stop()

# if __name__ == "__main__":
#     asyncio.run(main())

import asyncio
import logging
import signal
import sys
import uvicorn
from orchestrator import WorkflowOrchestrator
from executor import TaskExecutor
from completer import CompletionHandler
from config import config

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('workflow_engine.log')
    ]
)
logger = logging.getLogger(__name__)

class WorkflowEngine:
    """工作流引擎主类"""
    
    def __init__(self):
        self.orchestrator = WorkflowOrchestrator()
        self.executor = TaskExecutor()
        self.completer = CompletionHandler()
        self.running = False
        self.server_task = None
        
    async def start(self):
        """启动所有组件"""
        logger.info("Starting Workflow Engine...")
        
        try:
            # 启动所有组件
            await self.orchestrator.start()
            await self.executor.start()
            await self.completer.start()
            
            self.running = True
            logger.info("All components started successfully")
            
            # 并发运行所有组件
            tasks = [
                asyncio.create_task(self.orchestrator.run(), name="orchestrator"),
                asyncio.create_task(self.executor.run(), name="executor"),
                asyncio.create_task(self.completer.run(), name="completer"),
                asyncio.create_task(self.start_api_server(), name="api_server")
            ]
            
            # 等待任何一个任务完成或失败
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            
            # 如果有任务异常完成，记录错误
            for task in done:
                if task.exception():
                    logger.error(f"Task {task.get_name()} failed: {task.exception()}")
                else:
                    logger.info(f"Task {task.get_name()} completed normally")
            
            # 取消剩余任务
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                    
        except Exception as e:
            logger.error(f"Error starting workflow engine: {e}")
            raise
    
    async def start_api_server(self):
        """启动API服务器"""
        logger.info(f"Starting API server on port {config.API_PORT}")
        
        config_uvicorn = uvicorn.Config(
            "api:app",
            host="0.0.0.0",
            port=config.API_PORT,
            log_level="info",
            access_log=True
        )
        server = uvicorn.Server(config_uvicorn)
        
        try:
            await server.serve()
        except Exception as e:
            logger.error(f"API server error: {e}")
            raise
    
    async def stop(self):
        """停止所有组件"""
        if not self.running:
            return
            
        logger.info("Stopping Workflow Engine...")
        
        try:
            # 停止所有组件
            await asyncio.gather(
                self.orchestrator.stop(),
                self.executor.stop(),
                self.completer.stop(),
                return_exceptions=True
            )
            
            self.running = False
            logger.info("Workflow Engine stopped successfully")
            
        except Exception as e:
            logger.error(f"Error stopping workflow engine: {e}")

# 全局引擎实例
engine = WorkflowEngine()

def signal_handler(signum, frame):
    """信号处理器"""
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    
    # 创建停止任务
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(engine.stop())
    else:
        asyncio.run(engine.stop())

async def health_check():
    """启动前的健康检查"""
    logger.info("Performing health checks...")
    
    try:
        # 检查Redis连接
        import redis.asyncio as redis
        redis_client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            decode_responses=True
        )
        await redis_client.ping()
        await redis_client.close()
        logger.info("Redis connection OK")
        
        # 检查Kafka配置
        from aiokafka import AIOKafkaProducer
        producer = AIOKafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: v.encode('utf-8')
        )
        await producer.start()
        await producer.stop()
        logger.info("Kafka connection OK")
        
        # 检查函数注册表
        from registry import REGISTRY
        functions = REGISTRY.list_all()
        logger.info(f"Function registry OK: {len(functions)} functions available")
        
        logger.info("All health checks passed")
        return True
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return False

async def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("Simple Workflow Engine Starting")
    logger.info("=" * 60)
    
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # 执行健康检查
        if not await health_check():
            logger.error("Health checks failed, exiting...")
            sys.exit(1)
        
        # 启动引擎
        await engine.start()
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Engine error: {e}", exc_info=True)
    finally:
        await engine.stop()
        logger.info("Workflow Engine shutdown complete")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)