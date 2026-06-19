#!/usr/bin/env python3
"""
华为云 ECS 自动部署脚本 - 使用 subprocess 管道输入密码
"""
import subprocess
import time
import sys
import os

SERVER_IP = "119.3.216.113"
USERNAME = "root"
PASSWORD = "Cloud123456!"

def run_ssh_command(command, timeout=300):
    """运行 SSH 命令并自动输入密码"""
    full_command = f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {USERNAME}@{SERVER_IP} '{command}'"
    
    print(f"\n>>> 执行: {command[:60]}...")
    
    # 使用 pexpect 或手动管道
    proc = subprocess.Popen(
        full_command,
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # 发送密码
    proc.stdin.write(PASSWORD + "\n")
    proc.stdin.flush()
    
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
        print(stdout)
        if stderr:
            print(f"[STDERR] {stderr}", file=sys.stderr)
        return proc.returncode, stdout, stderr
    except subprocess.TimeoutExpired:
        proc.kill()
        print("命令超时", file=sys.stderr)
        return -1, "", "Timeout"

def upload_file(local_path, remote_path):
    """上传文件到服务器"""
    print(f"\n>>> 上传: {local_path} -> {remote_path}")
    
    # 先创建本地临时脚本
    script_content = f"""#!/bin/bash
# 在服务器上创建文件
cat > {remote_path} << 'FILEEOF'
"""
    
    with open(local_path, 'r') as f:
        content = f.read()
    
    script_content += content
    script_content += "\nFILEEOF\n"
    
    # 保存临时脚本
    temp_script = os.path.join(os.environ['TEMP'], 'upload_script.sh')
    with open(temp_script, 'w') as f:
        f.write(script_content)
    
    # 执行上传脚本
    return run_ssh_command(f"bash -c '{script_content}'", timeout=60)

def main():
    print("=" * 60)
    print("华为云 ECS 自动部署")
    print("=" * 60)
    
    # 测试连接
    print("\n[1/8] 测试连接...")
    code, out, err = run_ssh_command("echo 'Connection successful'")
    if code != 0:
        print(f"✗ 连接失败: {err}")
        print("尝试使用 VNC 登录...")
        return
    
    print("✓ 连接成功!")
    
    # 安装 Docker
    print("\n[2/8] 安装 Docker...")
    run_ssh_command("""
        if ! command -v docker &> /dev/null; then
            echo "Installing Docker..."
            curl -fsSL https://get.docker.com | sh
            systemctl start docker
            systemctl enable docker
        fi
        docker --version
    """, timeout=300)
    
    # 创建项目目录
    print("\n[3/8] 创建项目目录...")
    run_ssh_command("mkdir -p /opt/cloud-project")
    
    # 创建 app.py
    print("\n[4/8] 创建项目文件...")
    app_py = '''import os
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
    
    # 使用 here document 创建文件
    run_ssh_command(f"cat > /opt/cloud-project/app.py << 'PYEOF'\n{app_py}\nPYEOF")
    
    # 创建 requirements.txt
    requirements = "flask==3.0.0\nredis==5.0.1\nnumpy==1.26.0\n"
    run_ssh_command(f"cat > /opt/cloud-project/requirements.txt << 'REQEOF'\n{requirements}\nREQEOF")
    
    # 创建 Dockerfile
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
    run_ssh_command(f"cat > /opt/cloud-project/Dockerfile << 'DOCEOF'\n{dockerfile}\nDOCEOF")
    
    # 构建镜像
    print("\n[5/8] 构建 Docker 镜像...")
    run_ssh_command("cd /opt/cloud-project && docker build -t backend:v1 .", timeout=600)
    
    # 运行容器
    print("\n[6/8] 启动服务...")
    run_ssh_command("""
        docker stop backend 2>/dev/null || true
        docker rm backend 2>/dev/null || true
        docker run -d --name backend -p 5000:5000 backend:v1
    """)
    
    # 验证
    print("\n[7/8] 验证服务...")
    time.sleep(5)
    run_ssh_command("curl -s http://localhost:5000/api/health")
    
    # 检查状态
    print("\n[8/8] 检查容器状态...")
    run_ssh_command("docker ps | grep backend")
    
    print("\n" + "=" * 60)
    print("✓ 部署完成!")
    print("=" * 60)
    print(f"Backend API: http://{SERVER_IP}:5000")
    print(f"Health Check: http://{SERVER_IP}:5000/api/health")
    print("=" * 60)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n✗ 部署失败: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
