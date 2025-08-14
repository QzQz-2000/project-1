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
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WorkflowEngine:
    """工作流引擎主类"""
    
    def __init__(self):
        self.orchestrator = WorkflowOrchestrator()
        self.executor = TaskExecutor()
        self.completer = CompletionHandler()
        self.running = False
        
    async def start(self):
        """启动所有组件"""
        logger.info("Starting Workflow Engine...")
        
        # 启动所有组件
        await self.orchestrator.start()
        await self.executor.start()
        await self.completer.start()
        
        self.running = True
        logger.info("All components started successfully")
        
        # 并发运行所有组件
        await asyncio.gather(
            self.orchestrator.run(),
            self.executor.run(),
            self.completer.run(),
            self.start_api_server()
        )
    
    async def start_api_server(self):
        """启动API服务器"""
        logger.info(f"Starting API server on port {config.API_PORT}")
        
        config_uvicorn = uvicorn.Config(
            "api:app",
            host="0.0.0.0",
            port=config.API_PORT,
            log_level="info"
        )
        server = uvicorn.Server(config_uvicorn)
        await server.serve()
    
    async def stop(self):
        """停止所有组件"""
        if not self.running:
            return
            
        logger.info("Stopping Workflow Engine...")
        
        await self.orchestrator.stop()
        await self.executor.stop()
        await self.completer.stop()
        
        self.running = False
        logger.info("Workflow Engine stopped")

# 全局引擎实例
engine = WorkflowEngine()

def signal_handler(signum, frame):
    """信号处理器"""
    logger.info(f"Received signal {signum}, shutting down...")
    asyncio.create_task(engine.stop())

async def main():
    """主函数"""
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await engine.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Engine error: {e}")
    finally:
        await engine.stop()

if __name__ == "__main__":
    asyncio.run(main())