#!/bin/bash
set -e
echo "=========================================="
echo "华为云 ECS 部署脚本"
echo "=========================================="

# 1. 安装 Docker
echo "[1/6] 安装 Docker..."
if ! command -v docker &> /dev/null; then
    curl -fsSL https://get.docker.com | sh
    systemctl start docker
    systemctl enable docker
fi
docker --version

# 2. 创建项目目录
echo "[2/6] 创建项目文件..."
mkdir -p /opt/cloud-project
cd /opt/cloud-project

cat > app.py << 'PYEOF'
import os
from flask import Flask, jsonify

app = Flask(__name__)
print("学号: SCAI004712, 姓名: 周云富")

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")

import redis
if REDIS_PASSWORD:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
else:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

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
PYEOF

cat > requirements.txt << 'REQEOF'
flask==3.0.0
redis==5.0.1
numpy==1.26.0
REQEOF

cat > Dockerfile << 'DOCEOF'
FROM python:3.11-slim AS builder
WORKDIR /build
COPY requirements.txt .
RUN pip install --target=/build/packages -r requirements.txt

FROM python:3.11-slim
WORKDIR /app
COPY --from=builder /build/packages /app/packages
COPY app.py .
ENV PYTHONPATH="/app/packages:$PYTHONPATH"
EXPOSE 5000
CMD ["python", "app.py"]
DOCEOF

# 3. 构建镜像
echo "[3/6] 构建 Docker 镜像..."
docker build -t backend:v1 .

# 4. 运行容器
echo "[4/6] 启动服务..."
docker stop backend 2>/dev/null || true
docker rm backend 2>/dev/null || true
docker run -d --name backend -p 5000:5000 backend:v1

# 5. 验证
echo "[5/6] 验证服务..."
sleep 3
curl -s http://localhost:5000/api/health || echo "Health check failed"

echo "=========================================="
echo "部署完成！"
echo "Backend API: http://119.3.216.113:5000"
echo "Health Check: http://119.3.216.113:5000/api/health"
echo "=========================================="
