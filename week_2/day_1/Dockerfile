FROM apache/airflow:3.3.0

# Chuyển sang user root nếu cần cài thêm package hệ thống (OS level)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Trở lại user airflow chuẩn để cài pip packages (An toàn bảo mật)
USER airflow

# Cài đặt các Provider cần thiết
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres \
    apache-airflow-providers-amazon \
    apache-airflow-providers-http