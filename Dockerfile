FROM apache/airflow:2.10.2

USER airflow

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.12.txt" \
    apache-airflow-providers-postgres \
    apache-airflow-providers-mongo