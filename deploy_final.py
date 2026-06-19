#!/usr/bin/env python3
"""
华为云 ECS 自动部署脚本 - 使用 Paramiko
"""
import paramiko
import time
import sys

SERVER_IP = "119.3.216.113"
USERNAME = "root"
PASSWORD = "Cloud123456!"

def run_command(ssh, command, timeout=300):
    """在远程服务器上执行命令"""
    print(f"\n>>> 执行: {command[:60]}...")
    stdin, stdout, stderr = ssh.exec_command(command, timeout=timeout, get_pty=True)
    
    # 实时输出
    output_lines = []
    while not stdout.channel.exit_status_ready():
        if stdout.channel.recv_ready():
            output = stdout.channel.recv(1024).decode('utf-8', errors='ignore')
            print(output, end='')
            output_lines.append(output)
        time.sleep(0.5)
    
    # 获取剩余输出
    remaining = stdout.read().decode('utf-8', errors='ignore')
    if remaining:
        print(remaining, end='')
        output_lines.append(remaining)
    
    error = stderr.read().decode('utf-8', errors='ignore')
    if error and error.strip():
        print(f"[STDERR] {error}", file=sys.stderr)
    
    exit_code = stdout.channel.recv_exit_status()
    return exit_code, ''.join(output_lines), error

def main():
    print("=" * 60)
    print("华为云 ECS 自动部署")
    print("=" * 60)
    
    # 连接服务器
    print(f"\n[1/8] 连接服务器 {SERVER_IP}...")
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(SERVER_IP, username=USERNAME, password=PASSWORD, timeout=30, look_for_keys=False)
        print("✓ 连接成功!")
    except Exception as e:
        print(f"✗ 连接失败: {e}")
        sys.exit(1)
    
    try:
        # 1. 安装 Docker
        print("\n[2/8] 安装 Docker...")
        run_command(ssh, """
            if ! command -v docker &> /dev/null; then
                echo "正在安装 Docker..."
                curl -fsSL https://get.docker.com | sh
                systemctl start docker
                systemctl enable docker
            else
                echo "Docker 已安装"
            fi
            docker --version
        """, timeout=300)
        
        # 2. 创建项目目录
        print("\n[3/8] 创建项目目录...")
        run_command(ssh, "mkdir -p /opt/cloud-project && echo '目录创建成功'")
        
        # 3. 上传文件 - 使用 SFTP
        print("\n[4/8] 上传项目文件...")
        sftp = ssh.open_sftp()
        
        # app.py
        app_py_content = '''import os
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
'''
        
        with sftp.file('/opt/cloud-project/app.py', 'w') as f:
            f.write(app_py_content)
        print("✓ app.py 上传成功")
        
        # requirements.txt
        requirements = 'flask==3.0.0\nredis==5.0.1\nnumpy==1.26.0\n'
        with sftp.file('/opt/cloud-project/requirements.txt', 'w') as f:
            f.write(requirements)
        print("✓ requirements.txt 上传成功")
        
        # Dockerfile
        dockerfile = '''FROM python:3.11-slim AS builder
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
'''
        with sftp.file('/opt/cloud-project/Dockerfile', 'w') as f:
            f.write(dockerfile)
        print("✓ Dockerfile 上传成功")
        
        sftp.close()
        
        # 5. 构建 Docker 镜像
        print("\n[5/8] 构建 Docker 镜像...")
        run_command(ssh, "cd /opt/cloud-project && docker build -t backend:v1 .", timeout=600)
        
        # 6. 运行容器
        print("\n[6/8] 启动服务...")
        run_command(ssh, """
            docker stop backend 2>/dev/null || true
            docker rm backend 2>/dev/null || true
            docker run -d --name backend -p 5000:5000 backend:v1
            echo "容器启动完成"
        """)
        
        # 7. 验证服务
        print("\n[7/8] 验证服务...")
        time.sleep(5)
        run_command(ssh, "curl -s http://localhost:5000/api/health || echo 'Health check failed'")
        
        # 8. 检查运行状态
        print("\n[8/8] 检查容器状态...")
        run_command(ssh, "docker ps | grep backend")
        
        print("\n" + "=" * 60)
        print("✓ 部署完成!")
        print("=" * 60)
        print(f"Backend API: http://{SERVER_IP}:5000")
        print(f"Health Check: http://{SERVER_IP}:5000/api/health")
        print(f"Ping: http://{SERVER_IP}:5000/api/ping")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ 部署失败: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
    finally:
        ssh.close()
        print("\nSSH 连接已关闭")

if __name__ == "__main__":
    main()
