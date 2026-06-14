# 云计算课程设计

> **学号**: SCAI004712 | **姓名**: 周云富

---

## 项目概述

本项目为云计算课程设计，涵盖容器化部署、Kubernetes 编排、Spark 大数据分析三大核心模块，完整演示了云原生应用的构建与部署流程。

---

## 技术栈

| 类别 | 技术 |
|------|------|
| 后端框架 | Flask (Python) |
| 缓存数据库 | Redis |
| 容器化 | Docker + Docker Compose |
| 编排平台 | Kubernetes |
| 大数据处理 | Apache Spark (PySpark) |
| 对象存储 | OBS/S3 (s3a 协议) |

---

## 项目结构

```
.
├── backend/                    # Flask 后端服务
│   ├── app.py                  # 主应用：Redis 读写、健康检查 API
│   ├── static/index.html       # 前端展示页面
│   ├── requirements.txt        # Python 依赖
│   ├── Dockerfile.backend      # 后端多阶段构建镜像
│   ├── Dockerfile.frontend     # 前端 Nginx 镜像
│   ├── docker-compose.yml      # 本地 Docker Compose 编排
│   └── nginx.conf              # Nginx 反向代理配置
├── k8s/                        # Kubernetes 部署清单
│   ├── deployment.yaml         # Backend + Redis Deployment
│   ├── service.yaml            # LoadBalancer + ClusterIP Service
│   ├── configmap.yaml          # 环境变量配置
│   ├── secret.yaml             # 敏感信息（密码）
│   ├── pvc.yaml                # 持久化存储声明
│   └── hpa.yaml                # 水平自动扩缩容
└── spark/                      # Spark 大数据分析
    ├── wordcount.py            # WordCount 示例（环境验证）
    ├── spark_analysis.py       # Spark SQL 数据分析（4个查询）
    ├── performance_compare.py  # Pandas vs PySpark 性能对比
    ├── data_cleaning.py        # 数据清洗脚本
    └── sparkapplication.yaml   # Spark Operator 提交配置
```

---

## 模块详解

### 1. Backend 服务 (`backend/`)

基于 Flask 的 RESTful API 服务，集成 Redis 缓存，提供以下接口：

| 接口 | 方法 | 说明 |
|------|------|------|
| `/api/ping` | GET | 连通性测试，返回 Redis 连接状态 |
| `/api/health` | GET | 健康检查 |
| `/api/set/<key>/<value>` | GET | 写入 Redis 键值对 |
| `/api/get/<key>` | GET | 读取 Redis 键值 |

**本地启动**：
```bash
cd backend
docker-compose up -d
```

**Docker 多阶段构建**：
- Stage 1: `builder` 安装依赖到 `/build/packages`
- Stage 2: `runtime` 复制依赖并运行 `app.py`

---

### 2. Kubernetes 部署 (`k8s/`)

完整的云原生部署配置：

| 资源 | 说明 |
|------|------|
| Deployment | Backend (2 副本) + Redis (1 副本) |
| Service | Backend 使用 LoadBalancer 暴露，Redis 使用 ClusterIP |
| ConfigMap | 存储非敏感环境变量 |
| Secret | 存储 Redis 密码 |
| PVC | Redis 数据持久化 |
| HPA | CPU 利用率超 60% 自动扩容，范围 1~4 副本 |

**部署命令**：
```bash
kubectl apply -f k8s/
```

---

### 3. Spark 大数据分析 (`spark/`)

#### A-1: WordCount 示例 (`wordcount.py`)
- 用途：Spark 环境验证
- 支持本地模式与 OBS/S3 远程模式
- 自动回退到内置示例数据

#### A-2: Spark SQL 数据分析 (`spark_analysis.py`)
4 个核心分析查询，均支持 DataFrame API 和 Spark SQL 两种方式：

| 查询 | 技术点 | 说明 |
|------|--------|------|
| 查询1 | `GROUP BY` + `agg` | 按年份统计电影数量和平均评分 |
| 查询2 | `ORDER BY` + `LIMIT` | 评分最高的 10 部电影（>=1000 条评价） |
| 查询3 | `explode` + `split` | 按年份统计各类型电影产量变化 |
| 查询4 | 窗口函数 `ROW_NUMBER()` | 每个国家电影评分排名 |

#### A-3: 性能对比 (`performance_compare.py`)
对比三种方式执行 GROUP BY 聚合的性能：
1. **Pandas 单机版**
2. **PySpark executorInstances=1**
3. **PySpark executorInstances=2**

包含 Amdahl 定律理论分析：
```
Speedup = 1 / ((1-P) + P/N)
```

---

## 快速开始

### 环境要求
- Python 3.11+
- Docker & Docker Compose
- Kubernetes 集群（可选）
- Apache Spark 3.x（可选）

### 1. 本地运行 Backend
```bash
cd backend
docker-compose up -d
# 访问 http://localhost:5000
```

### 2. 部署到 Kubernetes
```bash
kubectl apply -f k8s/
# 查看服务状态
kubectl get svc,pod,hpa
```

### 3. 运行 Spark 分析
```bash
# 本地模式
cd spark
SPARK_LOCAL=true python spark_analysis.py

# 性能对比
SPARK_LOCAL=true python performance_compare.py
```

---

## 镜像仓库

Backend 镜像推送至华为云 SWR：
```
swr.cn-north-4.myhuaweicloud.com/YOUR_ORG/backend:v1
```

---

## 许可证

MIT License
