import json
import asyncio
import aiohttp
import pandas as pd
import os
from datetime import datetime

async def test_workflow():
    """测试工作流，并打印最终结果"""
    print("🧪 Testing Simple Workflow Engine")
    print("=" * 50)
    
    # 1. 创建测试数据
    os.makedirs('data', exist_ok=True)
    
    # 生成测试CSV
    test_data = pd.DataFrame({
        'timestamp': pd.date_range('2024-01-01', periods=50, freq='H'),
        'value': [10, 15, 20, 25, 30, 35, 40, 45, 50, 55] * 5,
        'category': ['A', 'B'] * 25
    })
    test_data.to_csv('data/test_data.csv', index=False)
    print(f"✅ Created test data: {len(test_data)} rows")
    
    # 2. 等待服务启动 (重要!)
    print("⏳ Waiting for services to start...")
    await asyncio.sleep(20)
    
    # 3. 定义测试工作流
    workflow = {
        "name": "Moving Average and Threshold Alarm Workflow",
        "description": "Load data -> Calculate moving average -> Apply threshold alarm",
        "steps": [
            {
                "name": "load_data",
                "task_type": "DATA",
                "data_source_config": {
                    "source_type": "csv",
                    "query_config": {
                        "file_path": "/app/data/test_data.csv"
                    }
                }
            },
            {
                "name": "calculate_ma",
                "task_type": "FUNCTION",
                "function_name": "MovingAverage",
                "config": {
                    "field": "value",
                    "window": 5
                },
                "dependencies": ["load_data"]
            },
            {
                "name": "threshold_alarm",
                "task_type": "FUNCTION",
                "function_name": "ThresholdAlarm",
                "config": {
                    "field": "value_ma5",
                    "threshold": 30,
                    "mode": "greater"
                },
                "dependencies": ["calculate_ma"]
            }
        ]
    }
    
    async with aiohttp.ClientSession() as session:
        try:
            # 4. 提交工作流
            print("\n🚀 Submitting workflow...")
            submit_url = 'http://localhost:8001/workflows/submit'
            payload = {
                "workflow": workflow,
                "submitted_by": "test_script"
            }
            async with session.post(submit_url, json=payload) as response:
                if response.status == 200:
                    result = await response.json()
                    workflow_id = result.get('workflow_id')
                    print(f"✅ Workflow submitted with ID: {workflow_id}")
                else:
                    print(f"❌ Failed to submit workflow: {response.status}")
                    print(await response.text())
                    return
            
            # 5. 持续查询工作流状态，直到完成
            print("⏳ Polling workflow status until completion...")
            status_url = f'http://localhost:8001/workflows/{workflow_id}/status'
            
            max_retries = 30
            for i in range(max_retries):
                await asyncio.sleep(2)
                async with session.get(status_url) as response:
                    if response.status == 200:
                        status_info = await response.json()
                        workflow_status = status_info['status']
                        print(f"  - Status check {i+1}: {workflow_status.upper()}")
                        if workflow_status in ['completed', 'failed']:
                            print(f"✅ Workflow completed with status: {workflow_status.upper()}")
                            break
                    else:
                        print(f"❌ Failed to get workflow status: {response.status}")
                        print(await response.text())
                        return
            else:
                print("⚠️ Workflow timed out or did not complete within the expected time.")
                return
            
            # 6. 获取最终结果
            print("\n📊 Fetching final result from API...")
            result_url = f'http://localhost:8001/workflows/{workflow_id}/tasks/threshold_alarm/result'
            
            async with session.get(result_url) as response:
                if response.status == 200:
                    result_data = await response.json()
                    print("✅ Successfully fetched final result!")
                    
                    if result_data:
                        result_df = pd.DataFrame(result_data)
                        print("\n--- Final Results (threshold_alarm) ---")
                        print(result_df)
                    else:
                        print("No data returned for final task.")
                else:
                    print(f"❌ Failed to get final result: {response.status}")
                    print(await response.text())
            
            print("\n🎉 Test completed!")
            
        except aiohttp.ClientConnectorError as e:
            print(f"❌ Connection Error: The API server might not be running or reachable at http://localhost:8000. Error: {e}")
        except Exception as e:
            print(f"❌ An unexpected error occurred: {e}")

if __name__ == "__main__":
    asyncio.run(test_workflow())

# import json
# import asyncio
# import aiohttp
# import pandas as pd
# import os
# from datetime import datetime

# async def test_workflow():
#     """测试工作流，使用正确的环境隔离API端点"""
#     print("测试简单工作流引擎")
#     print("=" * 50)
    
#     # 1. 创建测试数据
#     os.makedirs('data', exist_ok=True)
    
#     # 生成测试CSV
#     test_data = pd.DataFrame({
#         'timestamp': pd.date_range('2024-01-01', periods=50, freq='H'),
#         'value': [10, 15, 20, 25, 30, 35, 40, 45, 50, 55] * 5,
#         'category': ['A', 'B'] * 25
#     })
#     test_data.to_csv('data/test_data.csv', index=False)
#     print(f"创建测试数据: {len(test_data)} 行")
    
#     # 2. 等待服务启动
#     print("等待服务启动...")
#     await asyncio.sleep(20)
    
#     # 3. 定义测试环境ID
#     environment_id = "test_env_001"
    
#     # 4. 定义测试工作流
#     workflow = {
#         "name": "移动平均和阈值告警工作流",
#         "description": "加载数据 -> 计算移动平均 -> 应用阈值告警",
#         "steps": [
#             {
#                 "name": "load_data",
#                 "task_type": "DATA",
#                 "data_source_config": {
#                     "source_type": "influxdb",
#                     "query_config": {
#                         "measurement": "test_measurement",
#                         "fields": ["value"],
#                         "start": "-4h",             
#                         "stop": "now()",
#                         "pivot": True   
#                     }
#                 }
#             },
#             {
#                 "name": "calculate_ma",
#                 "task_type": "FUNCTION",
#                 "function_name": "MovingAverage",
#                 "config": {
#                     "field": "value",
#                     "window": 5
#                 },
#                 "dependencies": ["load_data"]
#             },
#             {
#                 "name": "threshold_alarm",
#                 "task_type": "FUNCTION",
#                 "function_name": "ThresholdAlarm",
#                 "config": {
#                     "field": "value_ma5",
#                     "threshold": 30,
#                     "mode": "greater"
#                 },
#                 "dependencies": ["calculate_ma"]
#             }
#         ]
#     }
    
#     async with aiohttp.ClientSession() as session:
#         try:
#             # 5. 提交工作流到指定环境
#             print(f"\n向环境 {environment_id} 提交工作流...")
#             submit_url = f'http://localhost:8001/environments/{environment_id}/workflows/submit'
#             payload = {
#                 "workflow": workflow,
#                 "submitted_by": "test_script"
#             }
#             async with session.post(submit_url, json=payload) as response:
#                 if response.status == 200:
#                     result = await response.json()
#                     workflow_id = result.get('workflow_id')
#                     print(f"工作流已提交，ID: {workflow_id}")
#                     print(f"   环境ID: {result.get('environment_id')}")
#                 else:
#                     print(f"工作流提交失败: {response.status}")
#                     print(await response.text())
#                     return
            
#             # 6. 持续查询工作流状态，直到完成
#             print("轮询工作流状态直到完成...")
#             status_url = f'http://localhost:8001/environments/{environment_id}/workflows/{workflow_id}/status'
            
#             max_retries = 30
#             for i in range(max_retries):
#                 await asyncio.sleep(2)
#                 async with session.get(status_url) as response:
#                     if response.status == 200:
#                         status_info = await response.json()
#                         workflow_status = status_info['status']
#                         task_stats = status_info.get('task_statistics', {})
#                         print(f"  - 状态检查 {i+1}: {workflow_status.upper()}")
#                         print(f"    任务统计: {task_stats}")
#                         if workflow_status in ['completed', 'failed']:
#                             print(f"工作流完成，状态: {workflow_status.upper()}")
#                             break
#                     else:
#                         print(f"获取工作流状态失败: {response.status}")
#                         print(await response.text())
#                         return
#             else:
#                 print("工作流超时或未在预期时间内完成。")
#                 return
            
#             # 7. 获取最终结果
#             print("\n从API获取最终结果...")
#             result_url = f'http://localhost:8001/environments/{environment_id}/workflows/{workflow_id}/tasks/threshold_alarm/result'
            
#             async with session.get(result_url) as response:
#                 if response.status == 200:
#                     result_data = await response.json()
#                     print("成功获取最终结果!")
                    
#                     if result_data:
#                         result_df = pd.DataFrame(result_data)
#                         print("\n--- 最终结果 (threshold_alarm) ---")
#                         print(f"数据形状: {result_df.shape}")
#                         print("\n前10行:")
#                         print(result_df.head(10))
                        
#                         # 显示告警统计
#                         if 'is_alarm' in result_df.columns:
#                             alarm_count = result_df['is_alarm'].sum()
#                             total_count = len(result_df)
#                             print(f"\n告警统计: {alarm_count}/{total_count} 条记录触发告警")
                        
#                         # 保存结果到文件
#                         result_df.to_csv('workflow_result.csv', index=False)
#                         print("结果已保存到 workflow_result.csv")
#                     else:
#                         print("最终任务未返回数据。")
#                 else:
#                     print(f"获取最终结果失败: {response.status}")
#                     print(await response.text())
            
#             # 8. 列出环境中的所有工作流
#             print(f"\n列出环境 {environment_id} 中的所有工作流...")
#             list_url = f'http://localhost:8001/environments/{environment_id}/workflows'
#             async with session.get(list_url) as response:
#                 if response.status == 200:
#                     workflows_data = await response.json()
#                     workflows = workflows_data.get('workflows', [])
#                     print(f"环境中共有 {len(workflows)} 个工作流:")
#                     for wf in workflows:
#                         print(f"  - {wf['workflow_id']}: {wf['name']} ({wf['status']})")
#                 else:
#                     print(f"获取工作流列表失败: {response.status}")
            
#             print("\n测试完成!")
            
#         except aiohttp.ClientConnectorError as e:
#             print(f"连接错误: API服务器可能未运行或无法访问 http://localhost:8001。错误: {e}")
#         except Exception as e:
#             print(f"发生意外错误: {e}")

# async def test_health_check():
#     """测试健康检查端点"""
#     print("\n测试健康检查...")
#     async with aiohttp.ClientSession() as session:
#         try:
#             async with session.get('http://localhost:8001/health') as response:
#                 if response.status == 200:
#                     health_data = await response.json()
#                     print(f"服务健康状态: {health_data['status']}")
#                     print(f"   服务状态: {health_data.get('services', {})}")
#                 else:
#                     print(f"健康检查失败: {response.status}")
#         except Exception as e:
#             print(f"健康检查错误: {e}")

# async def test_functions_list():
#     """测试函数列表端点"""
#     print("\n测试可用函数列表...")
#     async with aiohttp.ClientSession() as session:
#         try:
#             async with session.get('http://localhost:8001/functions') as response:
#                 if response.status == 200:
#                     functions_data = await response.json()
#                     functions = functions_data.get('functions', [])
#                     print(f"可用函数 ({len(functions)} 个):")
#                     for func in functions:
#                         print(f"  - {func['name']}: {func['description']} ({func['category']})")
#                 else:
#                     print(f"获取函数列表失败: {response.status}")
#         except Exception as e:
#             print(f"函数列表错误: {e}")

# async def test_environments():
#     """测试环境管理端点"""
#     print("\n测试环境管理...")
#     async with aiohttp.ClientSession() as session:
#         try:
#             # 列出所有环境
#             async with session.get('http://localhost:8001/environments') as response:
#                 if response.status == 200:
#                     env_data = await response.json()
#                     environments = env_data.get('environments', [])
#                     print(f"发现 {len(environments)} 个环境:")
#                     for env in environments:
#                         print(f"  - {env}")
                        
#                         # 获取环境状态
#                         env_status_url = f'http://localhost:8001/environments/{env}/status'
#                         async with session.get(env_status_url) as env_response:
#                             if env_response.status == 200:
#                                 env_status = await env_response.json()
#                                 print(f"    工作流总数: {env_status['total_workflows']}")
#                                 print(f"    状态统计: {env_status['workflow_status_counts']}")
#                 else:
#                     print(f"获取环境列表失败: {response.status}")
#         except Exception as e:
#             print(f"环境管理测试错误: {e}")

# async def test_complex_workflow():
#     """测试复杂工作流"""
#     print("\n测试复杂工作流...")
    
#     environment_id = "123"
    
#     # 复杂工作流：数据加载 -> 填充缺失值 -> 移动平均 -> 标准差 -> 归一化 -> 异常检测 -> 告警
#     complex_workflow = {
#         "name": "复杂数据处理管道",
#         "description": "完整的数据预处理和异常检测流水线",
#         "steps": [
#             {
#                 "name": "load_data",
#                 "task_type": "DATA",
#                 "data_source_config": {
#                     "source_type": "influxdb",
#                     "query_config": {
#                         "measurement": "test_measurement",
#                         "fields": ["value"],
#                         "start": "-4h",             
#                         "stop": "now()",
#                         "pivot": True   
#                     }
#                 }
#             },
#             {
#                 "name": "fill_missing",
#                 "task_type": "FUNCTION",
#                 "function_name": "FillNA",
#                 "config": {
#                     "field": "value",
#                     "method": "interpolate"
#                 },
#                 "dependencies": ["load_data"]
#             },
#             {
#                 "name": "moving_average",
#                 "task_type": "FUNCTION",
#                 "function_name": "MovingAverage",
#                 "config": {
#                     "field": "value",
#                     "window": 5,
#                     "method": "simple"
#                 },
#                 "dependencies": ["fill_missing"]
#             },
#             {
#                 "name": "calculate_std",
#                 "task_type": "FUNCTION",
#                 "function_name": "StdDeviation",
#                 "config": {
#                     "field": "value",
#                     "window": 10
#                 },
#                 "dependencies": ["moving_average"]
#             },
#             {
#                 "name": "normalize_data",
#                 "task_type": "FUNCTION",
#                 "function_name": "Normalize",
#                 "config": {
#                     "field": "value",
#                     "method": "zscore"
#                 },
#                 "dependencies": ["calculate_std"]
#             },
#             {
#                 "name": "detect_anomalies",
#                 "task_type": "FUNCTION",
#                 "function_name": "ZScore",
#                 "config": {
#                     "field": "value_norm",
#                     "threshold": 2.0
#                 },
#                 "dependencies": ["normalize_data"]
#             },
#             {
#                 "name": "final_alarm",
#                 "task_type": "FUNCTION",
#                 "function_name": "CombinedAlarm",
#                 "config": {
#                     "conditions": [
#                         {"field": "value", "operator": "gt", "value": 60},
#                         {"field": "is_anomaly", "operator": "eq", "value": True}
#                     ],
#                     "logic": "and"
#                 },
#                 "dependencies": ["detect_anomalies"]
#             }
#         ]
#     }
    
#     async with aiohttp.ClientSession() as session:
#         try:
#             # 提交复杂工作流
#             submit_url = f'http://localhost:8001/environments/{environment_id}/workflows/submit'
#             payload = {
#                 "workflow": complex_workflow,
#                 "submitted_by": "complex_test"
#             }
            
#             async with session.post(submit_url, json=payload) as response:
#                 if response.status == 200:
#                     result = await response.json()
#                     workflow_id = result.get('workflow_id')
#                     print(f"复杂工作流已提交，ID: {workflow_id}")
                    
#                     # 监控进度
#                     status_url = f'http://localhost:8001/environments/{environment_id}/workflows/{workflow_id}/status'
                    
#                     for i in range(60):  # 更长的等待时间
#                         await asyncio.sleep(3)
#                         async with session.get(status_url) as status_response:
#                             if status_response.status == 200:
#                                 status_info = await status_response.json()
#                                 workflow_status = status_info['status']
#                                 task_stats = status_info.get('task_statistics', {})
#                                 print(f"复杂工作流状态 {i+1}: {workflow_status} - {task_stats}")
                                
#                                 if workflow_status in ['completed', 'failed']:
#                                     break
                    
#                     # 获取最终结果
#                     if workflow_status == 'completed':
#                         result_url = f'http://localhost:8001/environments/{environment_id}/workflows/{workflow_id}/tasks/final_alarm/result'
#                         async with session.get(result_url) as result_response:
#                             if result_response.status == 200:
#                                 result_data = await result_response.json()
#                                 if result_data:
#                                     result_df = pd.DataFrame(result_data)
#                                     print(f"\n复杂工作流结果: {result_df.shape}")
                                    
#                                     # 保存复杂工作流结果
#                                     result_df.to_csv('complex_workflow_result.csv', index=False)
#                                     print("复杂工作流结果已保存到 complex_workflow_result.csv")
#                 else:
#                     print(f"复杂工作流提交失败: {response.status}")
#                     print(await response.text())
                    
#         except Exception as e:
#             print(f"复杂工作流测试错误: {e}")

# if __name__ == "__main__":
#     async def main():
#         await test_health_check()
#         await test_functions_list()
#         await test_environments()
#         await test_workflow()
#         await test_complex_workflow()
    
#     asyncio.run(main())