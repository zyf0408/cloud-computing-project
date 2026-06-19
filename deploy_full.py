#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
华为云完整自动化部署脚本
作者: 周云富 (SCAI004712)
功能: 自动连接ECS，安装Docker，构建镜像，推送到SWR
"""

import subprocess
import time
import sys
import os

# 配置
ECS_IP = "119.3.216.113"
ECS_USER = "root"
ECS_PASSWORD = "CC123456789！"
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
        
        stdout, stderr = proc.communicate(input=ECS_PASSWORD + "\n", timeout=timeout)
        
        return proc.returncode, stdout, stderr
    except subprocess.TimeoutExpired:
        proc.kill()
        return -1, "", "Command timeout"
    except Exception as e:
        return -1, "", str(e)

def wait_for_server():
    """等待服务器重启完成"""
    print("=" * 60)
    print("等待服务器重启完成...")
    print("=" * 60)
    
    max_retries = 60
    retry_interval = 10
    
    for i in range(max_retries):
        returncode, stdout, stderr = run_ssh_command("echo SSH_OK", timeout=15)
        
        if returncode == 0 and "SSH_OK" in stdout:
            print(f"\n服务器已就绪! (尝试 {i+1} 次)")
            return True
        else:
            print(f"尝试 {i+1}/{max_retries}: 服务器未就绪, 继续等待...")
        
        time.sleep(retry_interval)
    
    print("\n等待超时!")
    return False

def install_docker():
    """安装Docker"""
    print("\n" + "=" * 60)
    print("步骤1: 安装Docker")
    print("=" * 60)
    
    commands = """
    echo "开始安装Docker..."
    
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
    echo "Docker安装完成!"
    """
    
    returncode, stdout, stderr = run_ssh_command(commands, timeout=300)
    print(stdout)
    if stderr:
        print(f"错误: {stderr}")
    
    return returncode == 0

def create_project_files():
    """创建项目文件"""
    print("\n" + "=" * 60)
    print("步骤2: 创建项目文件")
    print("=" * 60)
    
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
    echo "项目文件创建完成!"
    ls -la {PROJECT_DIR}/backend/
    """
    
    returncode, stdout, stderr = run_ssh_command(commands, timeout=60)
    print(stdout)
    if stderr:
        print(f"错误: {stderr}")
    
    return returncode == 0

def build_image():
    """构建Docker镜像"""
    print("\n" + "=" * 60)
    print("步骤3: 构建Docker镜像")
    print("=" * 60)
    
    commands = f"""
    cd {PROJECT_DIR}/backend
    echo "构建后端镜像..."
    docker build -f Dockerfile.backend -t backend:v1 .
    docker tag backend:v1 {SWR_REGISTRY}/{SWR_ORG}/backend:v1
    echo "镜像构建完成!"
    docker images | grep backend
    """
    
    returncode, stdout, stderr = run_ssh_command(commands, timeout=300)
    print(stdout)
    if stderr:
        print(f"错误: {stderr}")
    
    return returncode == 0

def create_k8s_configs():
    """创建K8s配置文件"""
    print("\n" + "=" * 60)
    print("步骤4: 创建K8s配置文件")
    print("=" * 60)
    
    namespace_yaml = """apiVersion: v1
kind: Namespace
metadata:
  name: cloud-computing
"""
    
    configmap_yaml = """apiVersion: v1
kind: ConfigMap
metadata:
  name: backend-config
  namespace: cloud-computing
data:
  REDIS_HOST: "redis-svc"
  REDIS_PORT: "6379"
  APP_ENV: "production"
"""
    
    secret_yaml = """apiVersion: v1
kind: Secret
metadata:
  name: redis-secret
  namespace: cloud-computing
type: Opaque
data:
  password: cmVkaXMxMjM0NTY=
"""
    
    pvc_yaml = """apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: redis-data-pvc
  namespace: cloud-computing
spec:
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
  storageClassName: csi-disk
"""
    
    deployment_yaml = f"""apiVersion: apps/v1
kind: Deployment
metadata:
  name: backend
  namespace: cloud-computing
  labels:
    app: backend
spec:
  replicas: 2
  selector:
    matchLabels:
      app: backend
  template:
    metadata:
      labels:
        app: backend
    spec:
      containers:
      - name: backend
        image: {SWR_REGISTRY}/{SWR_ORG}/backend:v1
        ports:
        - containerPort: 5000
        resources:
          requests:
            cpu: "100m"
            memory: "128Mi"
          limits:
            cpu: "500m"
            memory: "512Mi"
        envFrom:
        - configMapRef:
            name: backend-config
        env:
        - name: REDIS_PASSWORD
          valueFrom:
            secretKeyRef:
              name: redis-secret
              key: password
        livenessProbe:
          httpGet:
            path: /api/ping
            port: 5000
          initialDelaySeconds: 10
          periodSeconds: 15
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis
  namespace: cloud-computing
  labels:
    app: redis
spec:
  replicas: 1
  selector:
    matchLabels:
      app: redis
  template:
    metadata:
      labels:
        app: redis
    spec:
      containers:
      - name: redis
        image: redis:7-alpine
        args:
        - --requirepass
        - $(REDIS_PASSWORD)
        env:
        - name: REDIS_PASSWORD
          valueFrom:
            secretKeyRef:
              name: redis-secret
              key: password
        ports:
        - containerPort: 6379
        volumeMounts:
        - name: redis-data
          mountPath: /data
      volumes:
      - name: redis-data
        persistentVolumeClaim:
          claimName: redis-data-pvc
"""
    
    service_yaml = """apiVersion: v1
kind: Service
metadata:
  name: backend-svc
  namespace: cloud-computing
  annotations:
    kubernetes.io/elb.class: union
spec:
  type: LoadBalancer
  selector:
    app: backend
  ports:
  - port: 80
    targetPort: 5000
    protocol: TCP
---
apiVersion: v1
kind: Service
metadata:
  name: redis-svc
  namespace: cloud-computing
spec:
  type: ClusterIP
  selector:
    app: redis
  ports:
  - port: 6379
    targetPort: 6379
    protocol: TCP
"""
    
    hpa_yaml = """apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: backend-hpa
  namespace: cloud-computing
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: backend
  minReplicas: 1
  maxReplicas: 4
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 60
"""
    
    commands = f"""
    mkdir -p {PROJECT_DIR}/k8s
    cat > {PROJECT_DIR}/k8s/namespace.yaml << 'EOF'
{namespace_yaml}
EOF
    cat > {PROJECT_DIR}/k8s/configmap.yaml << 'EOF'
{configmap_yaml}
EOF
    cat > {PROJECT_DIR}/k8s/secret.yaml << 'EOF'
{secret_yaml}
EOF
    cat > {PROJECT_DIR}/k8s/pvc.yaml << 'EOF'
{pvc_yaml}
EOF
    cat > {PROJECT_DIR}/k8s/deployment.yaml << 'EOF'
{deployment_yaml}
EOF
    cat > {PROJECT_DIR}/k8s/service.yaml << 'EOF'
{service_yaml}
EOF
    cat > {PROJECT_DIR}/k8s/hpa.yaml << 'EOF'
{hpa_yaml}
EOF
    echo "K8s配置文件创建完成!"
    ls -la {PROJECT_DIR}/k8s/
    """
    
    returncode, stdout, stderr = run_ssh_command(commands, timeout=60)
    print(stdout)
    if stderr:
        print(f"错误: {stderr}")
    
    return returncode == 0

def install_kubectl():
    """安装kubectl"""
    print("\n" + "=" * 60)
    print("步骤5: 安装kubectl")
    print("=" * 60)
    
    commands = """
    echo "安装kubectl..."
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
    chmod +x kubectl
    mv kubectl /usr/local/bin/
    kubectl version --client
    echo "kubectl安装完成!"
    """
    
    returncode, stdout, stderr = run_ssh_command(commands, timeout=120)
    print(stdout)
    if stderr:
        print(f"错误: {stderr}")
    
    return returncode == 0

def main():
    print("=" * 60)
    print("华为云完整自动化部署脚本")
    print("作者: 周云富 (SCAI004712)")
    print("=" * 60)
    
    # 等待服务器重启
    if not wait_for_server():
        print("服务器未就绪，退出部署")
        return False
    
    # 安装Docker
    if not install_docker():
        print("Docker安装失败")
        return False
    
    # 创建项目文件
    if not create_project_files():
        print("项目文件创建失败")
        return False
    
    # 构建镜像
    if not build_image():
        print("镜像构建失败")
        return False
    
    # 创建K8s配置
    if not create_k8s_configs():
        print("K8s配置创建失败")
        return False
    
    # 安装kubectl
    if not install_kubectl():
        print("kubectl安装失败")
        return False
    
    print("\n" + "=" * 60)
    print("基础环境准备完成!")
    print("=" * 60)
    print("\n下一步需要手动操作:")
    print("1. 在华为云控制台获取SWR临时登录指令")
    print(f"   访问: https://console.huaweicloud.com/swr/?region={REGION}#/swr/dashboard")
    print("2. 登录SWR并推送镜像:")
    print(f"   docker login -u cn-north-4@{SWR_ORG} -p <密码> {SWR_REGISTRY}")
    print(f"   docker push {SWR_REGISTRY}/{SWR_ORG}/backend:v1")
    print("3. 创建CCE集群并获取kubeconfig")
    print("4. 部署应用: kubectl apply -f /root/cloud-computing-project/k8s/")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
