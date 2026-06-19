#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
华为云自动化部署脚本 (Windows本地运行)
作者: 周云富 (SCAI004712)
功能: 自动通过SSH连接ECS，安装Docker，构建镜像，推送到SWR，创建CCE集群并部署
"""

import subprocess
import time
import os
import sys

# 配置
ECS_IP = "119.3.216.113"
ECS_USER = "root"
ECS_PASSWORD = "Cloud123456!"  # 需要用户确认或重置
SWR_ORG = "hid_62icj9ew-afwakp"
SWR_REGISTRY = "swr.cn-north-4.myhuaweicloud.com"
REGION = "cn-north-4"
PROJECT_DIR = "/root/cloud-computing-project"

def run_ssh_command(command, timeout=60):
    """通过SSH在远程服务器上执行命令"""
    ssh_cmd = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "PasswordAuthentication=yes",
        "-o", "ConnectTimeout=10",
        f"{ECS_USER}@{ECS_IP}",
        command
    ]
    
    try:
        proc = subprocess.Popen(
            ssh_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # 发送密码
        stdout, stderr = proc.communicate(input=ECS_PASSWORD + "\n", timeout=timeout)
        
        return proc.returncode, stdout, stderr
    except subprocess.TimeoutExpired:
        proc.kill()
        return -1, "", "Command timeout"
    except Exception as e:
        return -1, "", str(e)

def test_ssh_connection():
    """测试SSH连接"""
    print("测试SSH连接...")
    returncode, stdout, stderr = run_ssh_command("echo 'SSH_OK'")
    
    if returncode == 0 and "SSH_OK" in stdout:
        print("SSH连接成功!")
        return True
    else:
        print(f"SSH连接失败: {stderr}")
        return False

def install_docker_remote():
    """在远程ECS上安装Docker"""
    print("在ECS上安装Docker...")
    
    commands = """
    # 卸载旧版本
    yum remove -y docker docker-client docker-client-latest docker-common docker-latest docker-latest-logrotate docker-logrotate docker-engine 2>/dev/null || true
    
    # 安装依赖
    yum install -y yum-utils device-mapper-persistent-data lvm2
    
    # 添加华为云Docker镜像源
    yum-config-manager --add-repo https://repo.huaweicloud.com/docker-ce/linux/centos/docker-ce.repo
    
    # 安装Docker
    yum install -y docker-ce docker-ce-cli containerd.io
    
    # 启动Docker
    systemctl start docker
    systemctl enable docker
    
    # 验证安装
    docker --version
    echo "Docker安装完成"
    """
    
    returncode, stdout, stderr = run_ssh_command(commands, timeout=300)
    print(stdout)
    if stderr:
        print(f"错误: {stderr}")
    
    return returncode == 0

def create_project_remote():
    """在远程ECS上创建项目文件"""
    print("创建项目文件...")
    
    # 创建requirements.txt
    requirements = """flask==2.3.3
redis==5.0.1
gunicorn==21.2.0
"""
    
    # 创建app.py
    app_py = '''import os
from flask import Flask, jsonify

app = Flask(__name__)

print("学号: SCAI004712, 姓名: 周云富")

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")

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
'''
    
    # 创建Dockerfile
    dockerfile = '''# Stage 1: Builder
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
'''
    
    # 创建目录和文件
    commands = f"""
    mkdir -p {PROJECT_DIR}/backend
    cat > {PROJECT_DIR}/backend/requirements.txt << 'REQUIREMENTS_EOF'
{requirements}
REQUIREMENTS_EOF
    cat > {PROJECT_DIR}/backend/app.py << 'APP_EOF'
{app_py}
APP_EOF
    cat > {PROJECT_DIR}/backend/Dockerfile.backend << 'DOCKERFILE_EOF'
{dockerfile}
DOCKERFILE_EOF
    echo "项目文件创建完成"
    """
    
    returncode, stdout, stderr = run_ssh_command(commands, timeout=60)
    print(stdout)
    return returncode == 0

def build_image_remote():
    """在远程ECS上构建Docker镜像"""
    print("构建Docker镜像...")
    
    commands = f"""
    cd {PROJECT_DIR}/backend
    docker build -f Dockerfile.backend -t backend:v1 .
    docker tag backend:v1 {SWR_REGISTRY}/{SWR_ORG}/backend:v1
    echo "镜像构建完成"
    docker images | grep backend
    """
    
    returncode, stdout, stderr = run_ssh_command(commands, timeout=300)
    print(stdout)
    return returncode == 0

def main():
    print("=" * 60)
    print("华为云自动化部署脚本")
    print("作者: 周云富 (SCAI004712)")
    print("=" * 60)
    
    # 测试SSH连接
    if not test_ssh_connection():
        print("SSH连接失败，请检查:")
        print("1. ECS服务器是否运行中")
        print("2. 密码是否正确")
        print("3. 安全组是否允许SSH(22端口)访问")
        print("\n如果需要重置密码，请:")
        print("1. 登录华为云控制台")
        print("2. 进入ECS管理页面")
        print("3. 选择服务器，点击'更多' -> '重置密码'")
        return False
    
    # 安装Docker
    if not install_docker_remote():
        print("Docker安装失败")
        return False
    
    # 创建项目文件
    if not create_project_remote():
        print("项目文件创建失败")
        return False
    
    # 构建镜像
    if not build_image_remote():
        print("镜像构建失败")
        return False
    
    print("\n" + "=" * 60)
    print("基础环境准备完成!")
    print("=" * 60)
    print("\n下一步需要手动操作:")
    print("1. 在华为云控制台获取SWR临时登录指令")
    print(f"   访问: https://console.huaweicloud.com/swr/?region={REGION}#/swr/dashboard")
    print("2. 登录SWR并推送镜像")
    print("3. 创建CCE集群")
    print("4. 部署应用到K8s")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
