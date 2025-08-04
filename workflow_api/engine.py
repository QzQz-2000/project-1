import asyncio
import json
import uuid
import traceback
from kafka import KafkaProducer, KafkaConsumer
import pandas as pd
import redis
from registry import REGISTRY
from datetime import datetime
from typing import Dict, Any, List, Optional

# 使用统一的配置、模型和工具
from config import settings, setup_logging
from models import (
    MessageType, MessageDirection, TaskStatus,
    OrchestratorTaskMessage, FunctionTaskMessage
)
from utils import (
    MessageSerializer, DataStoreManager, KafkaManager, 
    WorkflowValidator
)

# 设置日志
logger = setup_logging()

class WorkflowOrchestrator:
    """工作流编排器 - 对应论文中的MAS编排器"""
    
    def __init__(self, kafka_manager: KafkaManager, data_store: DataStoreManager):
        self.kafka_manager = kafka_manager
        self.data_store = data_store
        self.logger = logger.getChild(self.__class__.__name__)
        self.running = False
        self.validator = WorkflowValidator()
    
    async def start(self):
        """启动编排器"""
        self.running = True
        asyncio.create_task(self._consume_orchestrator_messages())
        self.logger.info("工作流编排器已启动")
    
    async def stop(self):
        """停止编排器"""
        self.running = False
        self.logger.info("工作流编排器已停止")
    
    async def _consume_orchestrator_messages(self):
        """消费编排器消息"""
        consumer = KafkaConsumer(
            'orchestrator.tasks',
            bootstrap_servers=settings.kafka_servers_list,
            group_id=f'{settings.kafka_group_id}-orchestrator',
            value_deserializer=MessageSerializer.deserialize_message,
            auto_offset_reset='latest'
        )
        
        while self.running:
            try:
                for message in consumer:
                    orchestrator_msg = message.value
                    
                    if orchestrator_msg.direction == MessageDirection.REQUEST:
                        await self._handle_workflow_request(orchestrator_msg)
                    
            except Exception as e:
                self.logger.error(f"编排器消息处理错误: {e}")
                await asyncio.sleep(5)
    
    async def _handle_workflow_request(self, orchestrator_msg: OrchestratorTaskMessage):
        """处理工作流请求"""
        try:
            execution_id = str(uuid.uuid4())
            workflow_def = orchestrator_msg.workflow_definition
            
            self.logger.info(f"开始编排工作流: {workflow_def.get('name', 'Unknown')} (执行ID: {execution_id})")
            
            # 验证工作流定义
            available_functions = set(func['name'] for func in REGISTRY.get_all_metadata())
            is_valid, errors = self.validator.validate_workflow_definition(workflow_def, available_functions)
            
            if not is_valid:
                raise ValueError(f"工作流验证失败: {'; '.join(errors)}")
            
            # 解析工作流步骤
            steps = workflow_def.get('steps', [])
            
            # 构建依赖关系图
            task_graph = self._build_task_dependency_graph(steps, execution_id, orchestrator_msg.issuer)
            
            # 存储任务图到数据存储
            graph_data_id = self.data_store.store_data(task_graph, f"graph_{execution_id}")
            
            # 触发初始任务（没有依赖的任务）
            await self._trigger_initial_tasks(task_graph, orchestrator_msg.issuer)
            
            self.logger.info(f"工作流 {execution_id} 编排完成，已触发初始任务")
            
        except Exception as e:
            self.logger.error(f"工作流编排失败: {e}")
            await self._send_orchestrator_response(
                orchestrator_msg, 
                execution_id="failed",
                error_message=str(e)
            )
    
    def _build_task_dependency_graph(self, steps: List[Dict], execution_id: str, issuer: str) -> Dict[str, Dict]:
        """构建任务依赖关系图"""
        task_graph = {}
        
        for i, step in enumerate(steps):
            task_id = f"{execution_id}_{step['name']}"
            
            # 确定依赖关系（简单的顺序依赖）
            dependencies = []
            if i > 0:
                prev_task_id = f"{execution_id}_{steps[i-1]['name']}"
                dependencies.append(prev_task_id)
                
            # 查找显式依赖
            explicit_deps = step.get('dependencies', [])
            for dep_name in explicit_deps:
                dep_task_id = f"{execution_id}_{dep_name}"
                if dep_task_id not in dependencies:
                    dependencies.append(dep_task_id)
            
            task_graph[task_id] = {
                'task_id': task_id,
                'function_name': step['function'],
                'agent_name': f"agent_{step['function']}",
                'config': step.get('config', {}),
                'dependencies': dependencies,
                'dependents': [],  # 稍后填充
                'status': TaskStatus.PENDING.value,
                'issuer': issuer,
                'execution_id': execution_id
            }
        
        # 填充反向依赖关系
        for task_id, task_info in task_graph.items():
            for dep_id in task_info['dependencies']:
                if dep_id in task_graph:
                    task_graph[dep_id]['dependents'].append(task_id)
        
        return task_graph
    
    async def _trigger_initial_tasks(self, task_graph: Dict[str, Dict], issuer: str):
        """触发初始任务（没有依赖的任务）"""
        for task_id, task_info in task_graph.items():
            if not task_info['dependencies']:  # 没有依赖的任务
                await self._send_function_task_message(task_info, issuer)
    
    async def _send_function_task_message(self, task_info: Dict, issuer: str, input_data_location: str = None):
        """发送函数任务消息"""
        function_msg = FunctionTaskMessage(
            message_type=MessageType.FUNCTION_TASK,
            direction=MessageDirection.REQUEST,
            message_id=str(uuid.uuid4()),
            issuer=issuer,
            function_name=task_info['function_name'],
            agent_name=task_info['agent_name'],
            task_id=task_info['task_id'],
            function_config=task_info['config'],
            input_data_location=input_data_location,
            dependencies=task_info['dependencies'],
            dependents=task_info['dependents']
        )
        
        # 发送到对应的函数执行器topic
        topic = f"function.{task_info['function_name']}"
        self.kafka_manager.send_message(topic, function_msg)
        
        self.logger.info(f"已发送函数任务消息: {task_info['task_id']} -> {topic}")
    
    async def _send_orchestrator_response(self, request_msg: OrchestratorTaskMessage, 
                                        execution_id: str, result_data: Any = None, 
                                        error_message: str = None):
        """发送编排器响应消息"""
        response_msg = OrchestratorTaskMessage(
            message_type=MessageType.ORCHESTRATOR_TASK,
            direction=MessageDirection.RESPONSE,
            message_id=str(uuid.uuid4()),
            issuer=request_msg.issuer,
            execution_id=execution_id,
            result_data=result_data,
            error_message=error_message
        )
        
        self.kafka_manager.send_message('orchestrator.responses', response_msg)


class FunctionExecutorAgent:
    """函数执行器代理 - 对应论文中的Agent"""
    
    def __init__(self, function_name: str, kafka_manager: KafkaManager, 
                 data_store: DataStoreManager):
        self.function_name = function_name
        self.kafka_manager = kafka_manager
        self.data_store = data_store
        self.logger = logger.getChild(f"FunctionAgent_{function_name}")
        self.running = False
        
        # 获取函数类
        self.function_class = REGISTRY.get(function_name)
        if not self.function_class:
            raise ValueError(f"Function {function_name} not found in registry")
        
        self.function_instance = self.function_class()
    
    async def start(self):
        """启动函数执行器代理"""
        self.running = True
        asyncio.create_task(self._consume_function_messages())
        self.logger.info(f"函数执行器代理 {self.function_name} 已启动")
    
    async def stop(self):
        """停止函数执行器代理"""
        self.running = False
        self.logger.info(f"函数执行器代理 {self.function_name} 已停止")
    
    async def _consume_function_messages(self):
        """消费函数任务消息"""
        topic = f"function.{self.function_name}"
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=settings.kafka_servers_list,
            group_id=f'{settings.kafka_group_id}-function-{self.function_name}',
            value_deserializer=MessageSerializer.deserialize_message,
            auto_offset_reset='latest'
        )
        
        while self.running:
            try:
                for message in consumer:
                    function_msg = message.value
                    
                    if (function_msg.direction == MessageDirection.REQUEST and 
                        function_msg.function_name == self.function_name):
                        await self._handle_function_task(function_msg)
                    
            except Exception as e:
                self.logger.error(f"函数消息处理错误: {e}")
                await asyncio.sleep(5)
    
    async def _handle_function_task(self, function_msg: FunctionTaskMessage):
        """处理函数任务"""
        try:
            self.logger.info(f"开始执行任务: {function_msg.task_id}")
            
            # 获取输入数据
            input_data = await self._get_input_data(function_msg)
            
            # 执行函数
            result_data, execution_meta = self.function_instance.execute(
                input_data, 
                function_msg.function_config
            )
            
            # 存储结果数据
            output_data_location = self.data_store.store_data(
                result_data, 
                f"output_{function_msg.task_id}"
            )
            
            # 发送完成响应
            await self._send_function_response(
                function_msg, 
                output_data_location=output_data_location,
                status=TaskStatus.COMPLETED
            )
            
            # 通知编排器任务完成
            await self._notify_task_completion(function_msg, output_data_location)
            
            self.logger.info(f"任务完成: {function_msg.task_id}")
            
        except Exception as e:
            self.logger.error(f"任务执行失败 {function_msg.task_id}: {e}")
            await self._send_function_response(
                function_msg,
                status=TaskStatus.FAILED,
                error_message=str(e)
            )
    
    async def _get_input_data(self, function_msg: FunctionTaskMessage) -> pd.DataFrame:
        """获取输入数据"""
        if function_msg.input_data is not None:
            return function_msg.input_data
        
        if function_msg.input_data_location:
            return self.data_store.retrieve_data(function_msg.input_data_location)
        
        # 如果没有输入数据，返回空DataFrame（某些函数可能不需要输入）
        return pd.DataFrame()
    
    async def _send_function_response(self, request_msg: FunctionTaskMessage,
                                    output_data_location: str = None,
                                    status: TaskStatus = TaskStatus.COMPLETED,
                                    error_message: str = None):
        """发送函数执行响应"""
        response_msg = FunctionTaskMessage(
            message_type=MessageType.FUNCTION_TASK,
            direction=MessageDirection.RESPONSE,
            message_id=str(uuid.uuid4()),
            issuer=request_msg.issuer,
            function_name=request_msg.function_name,
            agent_name=request_msg.agent_name,
            task_id=request_msg.task_id,
            output_data_location=output_data_location,
            status=status,
            error_message=error_message
        )
        
        self.kafka_manager.send_message('function.responses', response_msg)
    
    async def _notify_task_completion(self, function_msg: FunctionTaskMessage, 
                                    output_data_location: str):
        """通知编排器任务完成"""
        completion_msg = {
            'type': 'TASK_COMPLETED',
            'task_id': function_msg.task_id,
            'execution_id': function_msg.task_id.split('_')[0],
            'output_data_location': output_data_location,
            'dependents': function_msg.dependents
        }
        
        producer = self.kafka_manager.get_producer()
        producer.send(
            'task.completions',
            value=json.dumps(completion_msg).encode()
        )


class TaskCompletionHandler:
    """任务完成处理器 - 处理任务间的依赖关系"""
    
    def __init__(self, kafka_manager: KafkaManager, data_store: DataStoreManager):
        self.kafka_manager = kafka_manager
        self.data_store = data_store
        self.logger = logger.getChild(self.__class__.__name__)
        self.running = False
    
    async def start(self):
        """启动完成处理器"""
        self.running = True
        asyncio.create_task(self._consume_completion_messages())
        self.logger.info("任务完成处理器已启动")
    
    async def stop(self):
        """停止完成处理器"""
        self.running = False
        self.logger.info("任务完成处理器已停止")
    
    async def _consume_completion_messages(self):
        """消费任务完成消息"""
        consumer = KafkaConsumer(
            'task.completions',
            bootstrap_servers=settings.kafka_servers_list,
            group_id=f'{settings.kafka_group_id}-completion-handler',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
        
        while self.running:
            try:
                for message in consumer:
                    completion_data = message.value
                    await self._handle_task_completion(completion_data)
                    
            except Exception as e:
                self.logger.error(f"任务完成处理错误: {e}")
                await asyncio.sleep(5)
    
    async def _handle_task_completion(self, completion_data: Dict):
        """处理任务完成"""
        try:
            task_id = completion_data['task_id']
            execution_id = completion_data['execution_id']
            output_data_location = completion_data['output_data_location']
            dependents = completion_data['dependents']
            
            # 获取任务图
            task_graph = self.data_store.retrieve_data(f"graph_{execution_id}")
            
            # 更新任务状态
            task_graph[task_id]['status'] = TaskStatus.COMPLETED.value
            task_graph[task_id]['output_data_location'] = output_data_location
            
            # 检查依赖任务是否可以执行
            newly_ready_tasks = []
            for dependent_id in dependents:
                if dependent_id in task_graph:
                    dependent_task = task_graph[dependent_id]
                    
                    # 检查所有依赖是否完成
                    all_deps_completed = all(
                        task_graph[dep_id]['status'] == TaskStatus.COMPLETED.value
                        for dep_id in dependent_task['dependencies']
                    )
                    
                    if all_deps_completed and dependent_task['status'] == TaskStatus.PENDING.value:
                        newly_ready_tasks.append(dependent_task)
            
            # 更新任务图
            self.data_store.store_data(task_graph, f"graph_{execution_id}")
            
            # 触发新的就绪任务
            for ready_task in newly_ready_tasks:
                await self._trigger_dependent_task(ready_task, task_graph)
            
            # 检查工作流是否完成
            if self._is_workflow_completed(task_graph):
                await self._handle_workflow_completion(execution_id, task_graph)
            
        except Exception as e:
            self.logger.error(f"处理任务完成失败: {e}")
    
    async def _trigger_dependent_task(self, task_info: Dict, task_graph: Dict):
        """触发依赖任务"""
        # 收集输入数据
        input_data_list = []
        for dep_id in task_info['dependencies']:
            dep_output_location = task_graph[dep_id].get('output_data_location')
            if dep_output_location:
                input_data_list.append(self.data_store.retrieve_data(dep_output_location))
        
        # 合并输入数据（简单合并，实际可能需要更复杂的逻辑）
        if input_data_list:
            if len(input_data_list) == 1:
                combined_data = input_data_list[0]
            else:
                # 多个DataFrame合并
                combined_data = pd.concat(input_data_list, ignore_index=True)
            
            input_data_location = self.data_store.store_data(
                combined_data, 
                f"input_{task_info['task_id']}"
            )
        else:
            input_data_location = None
        
        # 发送函数任务消息
        function_msg = FunctionTaskMessage(
            message_type=MessageType.FUNCTION_TASK,
            direction=MessageDirection.REQUEST,
            message_id=str(uuid.uuid4()),
            issuer=task_info['issuer'],
            function_name=task_info['function_name'],
            agent_name=task_info['agent_name'],
            task_id=task_info['task_id'],
            function_config=task_info['config'],
            input_data_location=input_data_location,
            dependencies=task_info['dependencies'],
            dependents=task_info['dependents']
        )
        
        topic = f"function.{task_info['function_name']}"
        self.kafka_manager.send_message(topic, function_msg)
        
        self.logger.info(f"已触发依赖任务: {task_info['task_id']}")
    
    def _is_workflow_completed(self, task_graph: Dict) -> bool:
        """检查工作流是否完成"""
        return all(
            task_info['status'] in [TaskStatus.COMPLETED.value, TaskStatus.FAILED.value]
            for task_info in task_graph.values()
        )
    
    async def _handle_workflow_completion(self, execution_id: str, task_graph: Dict):
        """处理工作流完成"""
        # 收集所有输出数据
        final_outputs = {}
        for task_id, task_info in task_graph.items():
            if task_info.get('output_data_location'):
                final_outputs[task_id] = task_info['output_data_location']
        
        # 发送工作流完成消息
        completion_msg = {
            'type': 'WORKFLOW_COMPLETED',
            'execution_id': execution_id,
            'final_outputs': final_outputs,
            'completion_time': datetime.now().isoformat()
        }
        
        producer = self.kafka_manager.get_producer()
        producer.send(
            'workflow.completions',
            value=json.dumps(completion_msg).encode()
        )
        
        self.logger.info(f"工作流完成: {execution_id}")


class MessageDrivenWorkflowEngine:
    """消息驱动工作流引擎 - 主入口"""
    
    def __init__(self):
        # 使用统一配置
        self.kafka_manager = KafkaManager(settings.kafka_servers_list)
        
        # 初始化Redis客户端
        redis_client = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            password=settings.redis_password,
            decode_responses=False
        )
        
        self.data_store = DataStoreManager(redis_client, settings.data_ttl)
        self.orchestrator = WorkflowOrchestrator(self.kafka_manager, self.data_store)
        self.completion_handler = TaskCompletionHandler(self.kafka_manager, self.data_store)
        
        # 函数执行器代理池
        self.function_agents = {}
        
        self.logger = logger.getChild(self.__class__.__name__)
    
    async def start(self):
        """启动引擎"""
        # 启动编排器和完成处理器
        await self.orchestrator.start()
        await self.completion_handler.start()
        
        # 为所有注册的函数创建执行器代理
        await self._create_function_agents()
        
        self.logger.info("消息驱动工作流引擎已启动")
    
    async def _create_function_agents(self):
        """为所有注册函数创建执行器代理"""
        # 获取所有注册的函数
        registered_functions = REGISTRY.get_all_metadata()
        
        for func_meta in registered_functions:
            function_name = func_meta['name']
            if function_name not in self.function_agents:
                agent = FunctionExecutorAgent(
                    function_name, 
                    self.kafka_manager, 
                    self.data_store
                )
                await agent.start()
                self.function_agents[function_name] = agent
                
                self.logger.info(f"已创建函数执行器代理: {function_name}")
    
    async def submit_workflow(self, workflow_definition: Dict[str, Any], issuer: str = "user") -> str:
        """提交工作流执行请求"""
        orchestrator_msg = OrchestratorTaskMessage(
            message_type=MessageType.ORCHESTRATOR_TASK,
            direction=MessageDirection.REQUEST,
            message_id=str(uuid.uuid4()),
            issuer=issuer,
            workflow_definition=workflow_definition
        )
        
        # 发送到编排器
        self.kafka_manager.send_message('orchestrator.tasks', orchestrator_msg)
        
        self.logger.info(f"已提交工作流: {workflow_definition.get('name', 'Unknown')}")
        return orchestrator_msg.message_id
    
    async def stop(self):
        """停止引擎"""
        await self.orchestrator.stop()
        await self.completion_handler.stop()
        
        for agent in self.function_agents.values():
            await agent.stop()
        
        self.kafka_manager.close()
        self.logger.info("消息驱动工作流引擎已停止")


# ==================== 使用示例 ====================

async def example_usage():
    """使用示例"""
    # 创建引擎（使用统一配置）
    engine = MessageDrivenWorkflowEngine()
    
    # 启动引擎
    await engine.start()
    
    # 定义工作流
    workflow_definition = {
        "name": "数据处理流水线",
        "description": "IoT数据处理和异常检测",
        "steps": [
            {
                "name": "fill_missing",
                "function": "FillNA",
                "config": {
                    "method": "ffill",
                    "field": "value"
                }
            },
            {
                "name": "smooth_data",
                "function": "MovingAverage",
                "config": {
                    "field": "value",
                    "window": 5
                }
            },
            {
                "name": "detect_anomalies",
                "function": "ZScore",
                "config": {
                    "field": "value_ma5",
                    "threshold": 3.0
                }
            },
            {
                "name": "threshold_alarm",
                "function": "ThresholdAlarm",
                "config": {
                    "field": "value_ma5",
                    "threshold": 100,
                    "mode": "greater"
                }
            }
        ]
    }
    
    # 提交工作流
    workflow_id = await engine.submit_workflow(workflow_definition, issuer="user_001")
    
    print(f"工作流已提交: {workflow_id}")
    
    # 保持运行
    await asyncio.sleep(30)
    
    # 停止引擎
    await engine.stop()

if __name__ == "__main__":
    asyncio.run(example_usage())