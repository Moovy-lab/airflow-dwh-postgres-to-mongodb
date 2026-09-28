FROM apache/airflow:2.10.2

USER root

# Installation de Java
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Installation des providers avec des versions compatibles Airflow 2.x
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres==5.12.0 \
    apache-airflow-providers-mongo==4.1.1 \
    apache-airflow-providers-apache-spark==4.1.3
