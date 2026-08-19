FROM apache/airflow:3.3.0-python3.13

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow-providers-amazon==9.33.0"

# Immutable application code
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root include/ /opt/airflow/include/