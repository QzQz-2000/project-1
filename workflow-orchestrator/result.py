import redis
import pickle
import pandas as pd
import sys
from typing import Optional

def get_task_result(environment_id: str, workflow_id: str, task_name: str, 
                   redis_host: str = "localhost", redis_port: int = 6380) -> Optional[pd.DataFrame]:
    """
    从Redis中获取任务结果
    
    Args:
        environment_id: 环境ID
        workflow_id: 工作流ID
        task_name: 任务名称
        redis_host: Redis主机地址
        redis_port: Redis端口
        
    Returns:
        任务结果DataFrame，如果不存在则返回None
    """
    try:
        # 连接到Redis（使用新的环境隔离键格式）
        r = redis.Redis(host=redis_host, port=redis_port, decode_responses=False)
        
        # 构造新的结果键格式：result:environment_id:workflow_id#task_name
        result_key = f"result:{environment_id}:{workflow_id}#{task_name}"
        
        print(f"查找结果键: {result_key}")
        
        # 获取原始序列化数据
        raw_data = r.get(result_key)
        
        if raw_data is None:
            print(f"未找到结果数据: {result_key}")
            return None
        
        # 反序列化成DataFrame
        df = pickle.loads(raw_data)
        
        print(f"成功加载结果数据: {len(df)} 行, {len(df.columns)} 列")
        return df
        
    except Exception as e:
        print(f"获取任务结果时出错: {e}")
        return None
    finally:
        r.close()

def list_all_results(redis_host: str = "localhost", redis_port: int = 6380):
    """列出所有可用的结果键"""
    try:
        r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        
        # 查找所有结果键
        pattern = "result:*"
        keys = r.keys(pattern)
        
        print(f"找到 {len(keys)} 个结果键:")
        for key in sorted(keys):
            print(f"  - {key}")
            
        return keys
        
    except Exception as e:
        print(f"列出结果键时出错: {e}")
        return []
    finally:
        r.close()

def parse_result_key(key: str) -> dict:
    """解析结果键，提取环境ID、工作流ID和任务名"""
    try:
        # 移除"result:"前缀
        if key.startswith("result:"):
            key = key[7:]  # 移除 "result:" 前缀
        
        # 分割 environment_id:workflow_id#task_name
        env_workflow, task_name = key.split('#')
        environment_id, workflow_id = env_workflow.split(':')
        
        return {
            'environment_id': environment_id,
            'workflow_id': workflow_id,
            'task_name': task_name,
            'full_key': f"result:{key}"
        }
    except ValueError as e:
        print(f"解析键格式失败 '{key}': {e}")
        return {}

def interactive_result_viewer():
    """交互式结果查看器"""
    print("=== 工作流结果查看器 ===")
    print("输入 'list' 查看所有可用结果")
    print("输入 'quit' 退出")
    print()
    
    while True:
        command = input("请输入命令或结果键: ").strip()
        
        if command.lower() == 'quit':
            break
        elif command.lower() == 'list':
            keys = list_all_results()
            if keys:
                print("\n可用的结果键:")
                for i, key in enumerate(keys, 1):
                    parsed = parse_result_key(key)
                    if parsed:
                        print(f"{i:2d}. {key}")
                        print(f"     环境: {parsed['environment_id']}")
                        print(f"     工作流: {parsed['workflow_id']}")
                        print(f"     任务: {parsed['task_name']}")
                print()
        elif command.startswith("result:"):
            # 直接使用完整的结果键
            parsed = parse_result_key(command)
            if parsed:
                df = get_task_result(
                    parsed['environment_id'],
                    parsed['workflow_id'], 
                    parsed['task_name']
                )
                if df is not None:
                    print(f"\n结果预览 ({command}):")
                    print(df.head(10))
                    print(f"\n数据形状: {df.shape}")
                    if len(df.columns) <= 10:
                        print(f"列名: {list(df.columns)}")
                    print()
        else:
            # 尝试解析为 environment_id:workflow_id#task_name 格式
            parsed = parse_result_key(f"result:{command}")
            if parsed:
                df = get_task_result(
                    parsed['environment_id'],
                    parsed['workflow_id'],
                    parsed['task_name']
                )
                if df is not None:
                    print(f"\n结果预览:")
                    print(df.head(10))
                    print(f"\n数据形状: {df.shape}")
                    if len(df.columns) <= 10:
                        print(f"列名: {list(df.columns)}")
                    print()
            else:
                print("无效的命令格式。请使用 'list' 查看可用结果或输入有效的结果键。")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "list":
            list_all_results()
        elif len(sys.argv) == 4:
            # 命令行参数: python result.py environment_id workflow_id task_name
            environment_id, workflow_id, task_name = sys.argv[1], sys.argv[2], sys.argv[3]
            df = get_task_result(environment_id, workflow_id, task_name)
            if df is not None:
                print("结果数据:")
                print(df.head(10))
                print(f"\n数据形状: {df.shape}")
        else:
            print("用法:")
            print("  python result.py list                                    # 列出所有结果")
            print("  python result.py <environment_id> <workflow_id> <task_name>  # 查看特定结果")
            print("  python result.py                                        # 交互式模式")
    else:
        # 交互式模式
        interactive_result_viewer()