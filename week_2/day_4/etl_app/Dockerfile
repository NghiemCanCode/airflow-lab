FROM python:3.11-slim

# Tối ưu hóa buffer output để log stream realtime về Airflow UI
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Cài đặt thư viện nặng độc lập với Airflow
RUN pip install --no-cache-dir polars==0.20.0 pyarrow==14.0.1

COPY src/ /app/src/

ENTRYPOINT ["python", "/app/src/main.py"]