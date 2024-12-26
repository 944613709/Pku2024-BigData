# 使用Python 3.8作为基础镜像
FROM python:3.9

# 设置工作目录
WORKDIR /app

# 安装系统依赖
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

# 复制项目文件
COPY . /app/

# 安装Python依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 设置环境变量
ENV PYTHONPATH=/app

# 设置入口点
ENTRYPOINT ["python", "auto_create_hive_table/cn/pku/EntranceApp.py"] 