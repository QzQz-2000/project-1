# 使用官方 Python 3.11 镜像
FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 复制依赖文件（假设你有 requirements.txt）
COPY requirements.txt .

# 安装依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY . .

# 设置环境变量（可选）
ENV PYTHONUNBUFFERED=1

# 暴露端口（如果你有 HTTP 或 WebSocket 服务需要暴露）
EXPOSE 8000

# 启动应用
CMD ["python", "main.py"]
