#!/bin/bash

# 构建Docker镜像
echo "Building Docker image..."
docker-compose build

# 启动服务
echo "Starting services..."
docker-compose up -d

# 显示容器日志
echo "Showing container logs..."
docker-compose logs -f 