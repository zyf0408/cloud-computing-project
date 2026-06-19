#!/bin/bash
# 华为云 ECS 部署脚本
# 服务器IP: 119.3.216.113

set -e

echo "=========================================="
echo "开始部署云计算课设项目"
echo "=========================================="

# 1. 安装 Docker
echo "[1/8] 安装 Docker..."
curl -fsSL https://get.docker.com | sh
systemctl start docker
systemctl enable docker
usermod -aG docker root
docker --version

# 2. 安装 Docker Compose
echo "[2/8] 安装 Docker Compose..."
curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose
docker-compose --version

# 3. 克隆项目代码
echo "[3/8] 准备项目代码..."
mkdir -p /opt/cloud-project
cd /opt/cloud-project

# 4. 创建项目文件
echo "[4/8] 创建项目文件..."

# 创建 app.py
cat > app.py << 'EOF'
import os
from flask import Flask, jsonify

app = Flask(__name__)

# 打印学号和姓名
print("学号: SCAI004712, 姓名: 周云富")

# 从环境变量读取 Redis 配置
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")

# 连接 Redis
import redis

if REDIS_PASSWORD:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True,
    )
else:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )


@app.route("/api/ping")
def ping():
    try:
        redis_client.ping()
        redis_status = "connected"
    except Exception:
        redis_status = "disconnected"
    return jsonify({"status": "ok", "redis": redis_status})


@app.route("/api/health")
def health():
    return jsonify({"status": "healthy"})


@app.route("/api/set/<key>/<value>")
def set_key(key, value):
    redis_client.set(key, value)
    return jsonify({"key": key, "value": value, "status": "ok"})


@app.route("/api/get/<key>")
def get_key(key):
    value = redis_client.get(key)
    if value is None:
        return jsonify({"key": key, "value": None, "status": "not_found"}), 404
    return jsonify({"key": key, "value": value, "status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
EOF

# 创建 requirements.txt
cat > requirements.txt << 'EOF'
flask==3.0.0
redis==5.0.1
numpy==1.26.0
EOF

# 创建 Dockerfile.backend
cat > Dockerfile.backend << 'EOF'
# Stage 1: Builder
FROM python:3.11-slim AS builder

WORKDIR /build
COPY requirements.txt .
RUN pip install --target=/build/packages -r requirements.txt

# Stage 2: Runtime
FROM python:3.11-slim

WORKDIR /app
COPY --from=builder /build/packages /app/packages
COPY app.py .

ENV PYTHONPATH="/app/packages:$PYTHONPATH"

EXPOSE 5000

CMD ["python", "app.py"]
EOF

# 创建 index.html
cat > index.html << 'EOF'
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>云计算课设 - 周云富</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
        }
        .card {
            background: white;
            border-radius: 16px;
            padding: 48px 64px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.15);
            text-align: center;
        }
        h1 { color: #333; font-size: 28px; margin-bottom: 12px; }
        .info { color: #666; font-size: 18px; }
        .id { color: #764ba2; font-weight: bold; font-size: 22px; margin-top: 8px; }
    </style>
</head>
<body>
    <div class="card">
        <h1>云计算课设 - 周云富</h1>
        <p class="info">学号2023112549</p>
        <p class="id">SCAI004712</p>
    </div>
</body>
</html>
EOF

# 创建 Dockerfile.frontend
cat > Dockerfile.frontend << 'EOF'
FROM nginx:1.25-alpine

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY index.html /usr/share/nginx/html/
EOF

# 创建 nginx.conf
cat > nginx.conf << 'EOF'
server {
    listen 80;
    server_name localhost;
    location / {
        root /usr/share/nginx/html;
        index index.html;
    }
}
EOF

# 创建 docker-compose.yml
cat > docker-compose.yml << 'EOF'
version: "3.9"

services:
  backend:
    build:
      context: ./
      dockerfile: Dockerfile.backend
    ports:
      - "5000:5000"
    environment:
      - REDIS_HOST=redis
      - REDIS_PORT=6379
      - REDIS_PASSWORD=${REDIS_PASSWORD:-}
    depends_on:
      - redis

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  frontend:
    build:
      context: ./
      dockerfile: Dockerfile.frontend
    ports:
      - "80:80"
    depends_on:
      - backend
EOF

echo "[5/8] 构建 Docker 镜像..."
docker build -t backend:v1 -f Dockerfile.backend .
docker build -t frontend:v1 -f Dockerfile.frontend .

echo "[6/8] 启动服务..."
docker-compose up -d

echo "[7/8] 验证服务..."
sleep 5
curl -s http://localhost:5000/api/health || echo "Backend not ready yet"
curl -s http://localhost:80 || echo "Frontend not ready yet"

echo "[8/8] 部署完成！"
echo "=========================================="
echo "Backend API: http://119.3.216.113:5000"
echo "Frontend: http://119.3.216.113"
echo "=========================================="
