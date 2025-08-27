# performance_visualizer.py
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta

class PerformanceVisualizer:
    def __init__(self):
        self.style = 'seaborn-v0_8'
        plt.style.use(self.style) if self.style in plt.style.available else None
        
    def plot_response_time_distribution(self, response_times, title="HTTP响应时间分布"):
        """绘制响应时间分布图"""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # 直方图
        ax1.hist(response_times, bins=50, alpha=0.7, color='skyblue', edgecolor='black')
        ax1.set_xlabel('响应时间 (ms)')
        ax1.set_ylabel('频次')
        ax1.set_title(f'{title} - 直方图')
        ax1.axvline(np.mean(response_times), color='red', linestyle='--', 
                   label=f'平均值: {np.mean(response_times):.2f}ms')
        ax1.axvline(np.percentile(response_times, 95), color='orange', linestyle='--',
                   label=f'95th: {np.percentile(response_times, 95):.2f}ms')
        ax1.legend()
        
        # 箱线图
        ax2.boxplot(response_times, vert=True)
        ax2.set_ylabel('响应时间 (ms)')
        ax2.set_title(f'{title} - 箱线图')
        
        plt.tight_layout()
        plt.show()
        
        # 保存图片
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        plt.savefig(f'response_time_distribution_{timestamp}.png', dpi=300, bbox_inches='tight')
        
    def plot_throughput_over_time(self, timestamps, throughput_data, title="吞吐量随时间变化"):
        """绘制吞吐量时序图"""
        fig, ax = plt.subplots(figsize=(12, 6))
        
        ax.plot(timestamps, throughput_data, linewidth=2, color='darkblue', marker='o', markersize=4)
        ax.fill_between(timestamps, throughput_data, alpha=0.3, color='lightblue')
        
        ax.set_xlabel('时间')
        ax.set_ylabel('吞吐量 (requests/sec)')
        ax.set_title(title)
        ax.grid(True, alpha=0.3)
        
        # 添加统计信息
        avg_throughput = np.mean(throughput_data)
        max_throughput = np.max(throughput_data)
        ax.axhline(avg_throughput, color='red', linestyle='--', alpha=0.7,
                  label=f'平均吞吐量: {avg_throughput:.0f}')
        ax.axhline(max_throughput, color='green', linestyle='--', alpha=0.7,
                  label=f'峰值吞吐量: {max_throughput:.0f}')
        
        ax.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        plt.savefig(f'throughput_over_time_{timestamp}.png', dpi=300, bbox_inches='tight')
    
    def create_interactive_dashboard(self, metrics_data):
        """创建交互式Plotly仪表板"""
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('HTTP响应时间', 'MQTT消息处理速率', '系统资源使用', 'Kafka延迟'),
            specs=[[{"secondary_y": True}, {"secondary_y": True}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # HTTP响应时间
        if 'http_response_times' in metrics_data:
            fig.add_trace(
                go.Scatter(
                    x=list(range(len(metrics_data['http_response_times']))),
                    y=metrics_data['http_response_times'],
                    mode='lines+markers',
                    name='响应时间',
                    line=dict(color='blue', width=2)
                ),
                row=1, col=1
            )
        
        # MQTT消息处理速率
        if 'mqtt_message_rate' in metrics_data:
            fig.add_trace(
                go.Scatter(
                    x=list(range(len(metrics_data['mqtt_message_rate']))),
                    y=metrics_data['mqtt_message_rate'],
                    mode='lines',
                    name='消息速率',
                    line=dict(color='green', width=2)
                ),
                row=1, col=2
            )
        
        # 系统资源使用
        if 'cpu_usage' in metrics_data and 'memory_usage' in metrics_data:
            fig.add_trace(
                go.Scatter(
                    x=list(range(len(metrics_data['cpu_usage']))),
                    y=metrics_data['cpu_usage'],
                    mode='lines',
                    name='CPU %',
                    line=dict(color='red', width=2)
                ),
                row=2, col=1
            )
            
            fig.add_trace(
                go.Scatter(
                    x=list(range(len(metrics_data['memory_usage']))),
                    y=metrics_data['memory_usage'],
                    mode='lines',
                    name='内存 %',
                    line=dict(color='orange', width=2)
                ),
                row=2, col=1
            )
        
        # Kafka延迟
        if 'kafka_latency' in metrics_data:
            fig.add_trace(
                go.Scatter(
                    x=list(range(len(metrics_data['kafka_latency']))),
                    y=metrics_data['kafka_latency'],
                    mode='lines+markers',
                    name='Kafka延迟',
                    line=dict(color='purple', width=2)
                ),
                row=2, col=2
            )
        
        fig.update_layout(
            title="数字孪生系统性能监控仪表板",
            showlegend=True,
            height=800
        )
        
        fig.show()
        fig.write_html(f"performance_dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html")
    
    def plot_container_resource_comparison(self, container_metrics):
        """绘制容器资源对比图"""
        containers = list(container_metrics.keys())
        cpu_usage = [np.mean(metrics['cpu']) for metrics in container_metrics.values()]
        memory_usage = [np.mean(metrics['memory']) for metrics in container_metrics.values()]
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # CPU使用率对比
        bars1 = ax1.bar(containers, cpu_usage, color='lightcoral', alpha=0.8)
        ax1.set_ylabel('平均CPU使用率 (%)')
        ax1.set_title('容器CPU使用率对比')
        ax1.tick_params(axis='x', rotation=45)
        
        # 在柱状图上显示数值
        for bar, value in zip(bars1, cpu_usage):
            ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                    f'{value:.1f}%', ha='center', va='bottom')
        
        # 内存使用率对比
        bars2 = ax2.bar(containers, memory_usage, color='lightblue', alpha=0.8)
        ax2.set_ylabel('平均内存使用率 (%)')
        ax2.set_title('容器内存使用率对比')
        ax2.tick_params(axis='x', rotation=45)
        
        for bar, value in zip(bars2, memory_usage):
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                    f'{value:.1f}%', ha='center', va='bottom')
        
        plt.tight_layout()
        plt.show()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        plt.savefig(f'container_resource_comparison_{timestamp}.png', dpi=300, bbox_inches='tight')

# 使用示例和测试数据生成
def generate_sample_data():
    """生成示例测试数据"""
    np.random.seed(42)
    
    # HTTP响应时间数据（模拟）
    response_times = np.random.lognormal(mean=3.5, sigma=0.5, size=10000)  # 对数正态分布
    response_times = response_times * 10  # 转换为毫秒
    
    # 吞吐量数据
    time_points = [datetime.now() - timedelta(minutes=x) for x in range(60, 0, -1)]
    base_throughput = 1500
    throughput_data = [base_throughput + np.random.normal(0, 200) + 
                      300 * np.sin(i * 0.1) for i in range(60)]
    
    # 综合指标数据
    metrics_data = {
        'http_response_times': response_times[:100].tolist(),
        'mqtt_message_rate': [5000 + np.random.normal(0, 500) for _ in range(100)],
        'cpu_usage': [30 + 20 * np.sin(i * 0.1) + np.random.normal(0, 5) for i in range(100)],
        'memory_usage': [60 + 15 * np.cos(i * 0.05) + np.random.normal(0, 3) for i in range(100)],
        'kafka_latency': [50 + np.random.exponential(20) for _ in range(100)]
    }
    
    # 容器资源数据
    container_metrics = {
        'iot-data-adapter': {
            'cpu': [25 + np.random.normal(0, 5) for _ in range(100)],
            'memory': [45 + np.random.normal(0, 8) for _ in range(100)]
        },
        'broker': {
            'cpu': [35 + np.random.normal(0, 7) for _ in range(100)],
            'memory': [60 + np.random.normal(0, 10) for _ in range(100)]
        },
        'kafka-influx-service': {
            'cpu': [20 + np.random.normal(0, 4) for _ in range(100)],
            'memory': [35 + np.random.normal(0, 6) for _ in range(100)]
        },
        'influxdb': {
            'cpu': [40 + np.random.normal(0, 8) for _ in range(100)],
            'memory': [70 + np.random.normal(0, 12) for _ in range(100)]
        }
    }
    
    return response_times, time_points, throughput_data, metrics_data, container_metrics

if __name__ == "__main__":
    # 生成示例数据并可视化
    response_times, time_points, throughput_data, metrics_data, container_metrics = generate_sample_data()
    
    visualizer = PerformanceVisualizer()
    
    # 绘制各种图表
    print("生成响应时间分布图...")
    visualizer.plot_response_time_distribution(response_times)
    
    print("生成吞吐量时序图...")
    visualizer.plot_throughput_over_time(time_points, throughput_data)
    
    print("生成交互式仪表板...")
    visualizer.create_interactive_dashboard(metrics_data)
    
    print("生成容器资源对比图...")
    visualizer.plot_container_resource_comparison(container_metrics)
    
    print("所有图表生成完成！")