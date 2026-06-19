#!/bin/bash
# =============================================================================
# 华为云完整部署脚本
# 作者: 周云富 (SCAI004712)
# 用途: 在华为云ECS上安装Docker，构建镜像并推送到SWR，然后部署到CCE
# =============================================================================

set -e

# 配置变量
SWR_ORG="hid_62icj9ew-afwakp"
SWR_REGISTRY="swr.cn-north-4.myhuaweicloud.com"
REGION="cn-north-4"
PROJECT_DIR="/root/cloud-computing-project"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# =============================================================================
# 步骤1: 安装Docker
# =============================================================================
install_docker() {
    log_info "步骤1: 安装Docker..."
    
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
    log_info "Docker安装完成"
}

# =============================================================================
# 步骤2: 登录华为云SWR
# =============================================================================
login_swr() {
    log_info "步骤2: 登录华为云SWR..."
    
    # 获取临时登录指令 (需要在华为云控制台生成)
    log_warn "请先在华为云控制台获取SWR临时登录指令"
    log_warn "访问: https://console.huaweicloud.com/swr/?region=$REGION#/swr/dashboard"
    
    # 示例登录指令:
    # docker login -u cn-north-4@$SWR_ORG -p <临时密码> $SWR_REGISTRY
}

# =============================================================================
# 步骤3: 创建项目目录
# =============================================================================
setup_project() {
    log_info "步骤3: 创建项目目录..."
    
    mkdir -p $PROJECT_DIR
    cd $PROJECT_DIR
    
    # 创建必要的目录
    mkdir -p backend k8s spark
    
    log_info "项目目录创建完成"
}

# =============================================================================
# 步骤4: 构建Docker镜像
# =============================================================================
build_images() {
    log_info "步骤4: 构建Docker镜像..."
    
    cd $PROJECT_DIR/backend
    
    # 创建requirements.txt
    cat > requirements.txt << 'EOF'
flask==2.3.3
redis==5.0.1
gunicorn==21.2.0
EOF
    
    # 创建app.py
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
    
    # 创建后端Dockerfile
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
    
    # 构建后端镜像
    docker build -f Dockerfile.backend -t backend:v1 .
    docker tag backend:v1 $SWR_REGISTRY/$SWR_ORG/backend:v1
    
    log_info "后端镜像构建完成"
}

# =============================================================================
# 步骤5: 推送镜像到SWR
# =============================================================================
push_images() {
    log_info "步骤5: 推送镜像到SWR..."
    
    docker push $SWR_REGISTRY/$SWR_ORG/backend:v1
    
    log_info "镜像推送完成"
}

# =============================================================================
# 步骤6: 创建K8s配置文件
# =============================================================================
setup_k8s() {
    log_info "步骤6: 创建K8s配置文件..."
    
    cd $PROJECT_DIR/k8s
    
    # 创建namespace
    cat > namespace.yaml << EOF
apiVersion: v1
kind: Namespace
metadata:
  name: cloud-computing
EOF
    
    # 创建configmap
    cat > configmap.yaml << 'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: backend-config
  namespace: cloud-computing
data:
  REDIS_HOST: "redis-svc"
  REDIS_PORT: "6379"
  APP_ENV: "production"
EOF
    
    # 创建secret
    cat > secret.yaml << 'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: redis-secret
  namespace: cloud-computing
type: Opaque
data:
  password: cmVkaXMxMjM0NTY=
EOF
    
    # 创建PVC
    cat > pvc.yaml << 'EOF'
apiVersion: v1
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
EOF
    
    # 创建deployment
    cat > deployment.yaml << EOF
apiVersion: apps/v1
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
        image: $SWR_REGISTRY/$SWR_ORG/backend:v1
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
        - \$(REDIS_PASSWORD)
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
EOF
    
    # 创建service
    cat > service.yaml << 'EOF'
apiVersion: v1
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
EOF
    
    # 创建HPA
    cat > hpa.yaml << 'EOF'
apiVersion: autoscaling/v2
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
EOF
    
    log_info "K8s配置文件创建完成"
}

# =============================================================================
# 步骤7: 安装kubectl和连接CCE
# =============================================================================
setup_kubectl() {
    log_info "步骤7: 安装kubectl..."
    
    # 下载kubectl
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
    chmod +x kubectl
    mv kubectl /usr/local/bin/
    
    # 验证安装
    kubectl version --client
    
    log_info "kubectl安装完成"
    log_warn "请从华为云CCE控制台获取kubeconfig文件并配置"
}

# =============================================================================
# 步骤8: 部署到K8s
# =============================================================================
deploy_k8s() {
    log_info "步骤8: 部署到Kubernetes..."
    
    cd $PROJECT_DIR/k8s
    
    # 应用所有配置
    kubectl apply -f namespace.yaml
    kubectl apply -f configmap.yaml
    kubectl apply -f secret.yaml
    kubectl apply -f pvc.yaml
    kubectl apply -f deployment.yaml
    kubectl apply -f service.yaml
    kubectl apply -f hpa.yaml
    
    log_info "K8s部署完成"
    
    # 等待部署完成
    log_info "等待Pod启动..."
    kubectl wait --for=condition=ready pod -l app=backend -n cloud-computing --timeout=300s
    kubectl wait --for=condition=ready pod -l app=redis -n cloud-computing --timeout=300s
    
    log_info "所有Pod已就绪"
}

# =============================================================================
# 步骤9: 验证部署
# =============================================================================
verify_deployment() {
    log_info "步骤9: 验证部署..."
    
    # 查看所有资源
    kubectl get all -n cloud-computing
    
    # 获取服务地址
    log_info "服务地址:"
    kubectl get svc backend-svc -n cloud-computing
    
    # 测试API
    BACKEND_IP=$(kubectl get svc backend-svc -n cloud-computing -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
    if [ -n "$BACKEND_IP" ]; then
        log_info "测试API..."
        curl http://$BACKEND_IP/api/ping
        echo
        curl http://$BACKEND_IP/api/health
        echo
    fi
    
    log_info "部署验证完成"
}

# =============================================================================
# 主函数
# =============================================================================
main() {
    log_info "========================================"
    log_info "华为云部署脚本开始执行"
    log_info "学号: SCAI004712, 姓名: 周云富"
    log_info "========================================"
    
    install_docker
    setup_project
    build_images
    # push_images  # 需要SWR登录
    setup_k8s
    setup_kubectl
    # deploy_k8s   # 需要CCE集群和kubeconfig
    # verify_deployment
    
    log_info "========================================"
    log_info "基础环境准备完成"
    log_info "请手动执行以下步骤:"
    log_info "1. 登录SWR: docker login -u cn-north-4@$SWR_ORG -p <密码> $SWR_REGISTRY"
    log_info "2. 推送镜像: docker push $SWR_REGISTRY/$SWR_ORG/backend:v1"
    log_info "3. 创建CCE集群并获取kubeconfig"
    log_info "4. 部署应用: kubectl apply -f $PROJECT_DIR/k8s/"
    log_info "========================================"
}

# 执行主函数
main
