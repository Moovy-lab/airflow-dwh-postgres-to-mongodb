FROM apache/airflow:2.10.2

ARG POSTGRES_JDBC_VERSION=42.7.3

USER root

# Installation de Java
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Installation des providers avec des versions compatibles Airflow 2.x
# pyspark est epingle a 3.5.0 : le provider apache-spark ne fixe aucune version,
# donc pip installerait pyspark 4.x, incompatible avec le cluster Spark 3.5.0
# declare dans le docker-compose.
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres==5.12.0 \
    apache-airflow-providers-mongo==4.1.1 \
    apache-airflow-providers-apache-spark==4.1.3 \
    pyspark==3.5.0

# Driver JDBC PostgreSQL, necessaire pour lire la source depuis les executors Spark.
# Il est telecharge dans l'image plutot que versionne dans le repo, puis distribue
# aux workers par l'option spark.jars au moment du spark-submit.
RUN mkdir -p /opt/airflow/spark/jars && \
    curl -fsSL \
      -o "/opt/airflow/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" && \
    chmod 644 "/opt/airflow/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar"
