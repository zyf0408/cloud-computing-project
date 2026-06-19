#!/bin/sh
# Spark Kubernetes entrypoint
# 解析 SPARK_K8S_CMD 环境变量决定启动 Driver 或 Executor
shift 2
exec "$@"
