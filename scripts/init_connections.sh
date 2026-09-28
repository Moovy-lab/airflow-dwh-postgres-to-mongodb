#!/usr/bin/env bash
#
# Cree les connexions Airflow du projet de facon reproductible.
#
# A executer dans le conteneur Airflow :
#
#   docker compose run --rm --entrypoint bash airflow-scheduler \
#       /opt/airflow/scripts/init_connections.sh
#
# Les hotes correspondent aux noms de services du docker-compose, donc les
# connexions fonctionnent a l'interieur du reseau Docker (et non via
# host.docker.internal, qui n'est necessaire que pour un acces depuis l'hote).

set -euo pipefail

reset_connection() {
    local conn_id="$1"
    airflow connections delete "$conn_id" >/dev/null 2>&1 || true
}

echo "Connexion PostgreSQL source..."
reset_connection postgres_default
airflow connections add postgres_default \
    --conn-type postgres \
    --conn-host postgres-source \
    --conn-port 5432 \
    --conn-schema school \
    --conn-login school \
    --conn-password school \
    --conn-description "Base scolaire source (service postgres-source)"

echo "Connexion MongoDB Data Warehouse..."
reset_connection mongo_default
airflow connections add mongo_default \
    --conn-type mongo \
    --conn-host mongodb-dwh \
    --conn-port 27017 \
    --conn-description "Data Warehouse MongoDB (service mongodb-dwh)"

echo "Connexion Spark..."
reset_connection spark_default
# Le provider apache-spark construit l'URL --master ainsi :
#   master = "{host}:{port}" si port est renseigne, sinon "{host}"
# Il faut donc que le schema spark:// fasse partie du host.
airflow connections add spark_default \
    --conn-type spark \
    --conn-host spark://spark-master \
    --conn-port 7077 \
    --conn-description "Cluster Spark standalone (service spark-master)"

echo
echo "Connexions creees :"
airflow connections list --conn-id postgres_default
airflow connections list --conn-id mongo_default
airflow connections list --conn-id spark_default
