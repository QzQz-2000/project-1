import json
import asyncio
import aiohttp
import pandas as pd
import os
from datetime import datetime

async def test_workflow():
    """测试工作流"""
    print("🧪 Testing Simple Workflow Engine")
    print("=" * 50)
    
    # 创建测试数据
    os.makedirs('data', exist_ok=True)
    
    # 生成测试CSV
    test_data = pd.DataFrame({
        'timestamp': pd.date_range('2024-01-01', periods=50, freq='H'),
        'value': [10, 15, 20, 25, 30, 35, 40, 45, 50, 55] * 5,
        'category': ['A', 'B'] * 25
    })
    test_data.to_csv('data/test_data.csv', index=False)
    print(f"✅ Created test data: {len(test_data)} rows")
    
    # 等待服务启动
    print("⏳ Waiting for services to start...")
    await asyncio.sleep(10)
    
    # 定义测试工作流
    workflow = {
        "name": "Simple Test Workflow",
        "description": "Load data -> Calculate moving average -> Filter results",
        "steps": [
            {
                "name": "load_data",
                "task_type": "DATA",
                "data_source_config": {
                    "source_type": "csv",
                    "query_config": {
                        "file_path": "data/test_data.csv"
                    }
                }
            },
            {
                "name": "moving_average",
                "task_type": "FUNCTION",
                "function_name": "MovingAverage",
                "dependencies": ["load_data"],
                "config": {
                    "field": "value",
                    "window": 5
                }
            },
            {
                "name": "filter_data",
                "task_type": "FUNCTION",
                "function_name": "Filter",
                "dependencies": ["moving_average"],
                "config": {
                    "field": "value_ma5",
                    "threshold": 30,
                    "operator": ">"
                }
            }
        ]
    }
    
    # 提交工作流
    async with aiohttp.ClientSession() as session:
        try:
            print("📤 Submitting workflow...")
            
            submit_data = {
                "workflow": workflow,
                "submitted_by": "test_user"
            }
            
            async with session.post(
                'http://localhost:8000/workflows/submit',
                json=submit_data
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    print(f"✅ Workflow submitted: {result}")
                else:
                    print(f"❌ Failed to submit workflow: {response.status}")
                    print(await response.text())
                    return
            
            # 等待一段时间让工作流执行
            print("⏳ Waiting for workflow execution...")
            await asyncio.sleep(15)
            
            # 检查健康状态
            print("🏥 Checking health...")
            async with session.get('http://localhost:8000/health') as response:
                if response.status == 200:
                    health = await response.json()
                    print(f"✅ Health check: {health['status']}")
                else:
                    print(f"❌ Health check failed: {response.status}")
            
            # 列出可用函数
            print("📋 Listing functions...")
            async with session.get('http://localhost:8000/functions') as response:
                if response.status == 200:
                    functions = await response.json()
                    print(f"✅ Available functions: {functions['functions']}")
                else:
                    print(f"❌ Failed to list functions: {response.status}")
            
            print("\n🎉 Test completed!")
            print("Note: Check the logs to see workflow execution details")
            
        except Exception as e:
            print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_workflow())