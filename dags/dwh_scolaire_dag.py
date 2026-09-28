from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator, ShortCircuitOperator

import json
import logging

logger = logging.getLogger(__name__)


# CONFIGURATION DU DAG

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 9, 15),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# CONFIGURATION SPARK

PG_CONN_ID = "postgres_default"
MONGO_CONN_ID = "mongo_default"
SPARK_CONN_ID = "spark_default"

DWH_DATABASE = "dwh_airflow"

# L'URL du master Spark (spark://spark-master:7077) n'est pas un parametre de
# SparkSubmitOperator : elle est lue dans la connexion Airflow SPARK_CONN_ID,
# ou le provider la reconstruit a partir de host + port.

# Chemins valables dans le conteneur Airflow (cf. volumes du docker-compose).
SPARK_APPLICATION = "/opt/airflow/spark/jobs/spark.py"

# Version du driver JDBC a faire correspondre a l'ARG POSTGRES_JDBC_VERSION du Dockerfile.
SPARK_JDBC_VERSION = "42.7.3"
SPARK_JDBC_JAR = f"file:///opt/airflow/spark/jars/postgresql-{SPARK_JDBC_VERSION}.jar"

# Le chargement est court-circuite au-dela de ce taux de valeurs NULL.
QC_MAX_NULL_RATE = 0.20


def spark_arguments(mode):

    # Les identifiants sont lus dans les connexions Airflow : une seule source de
    # verite. Limite connue pour un projet local : ils apparaissent dans la ligne
    # de commande du processus spark-submit, donc dans ps et dans l'UI Spark.
    pg_conn = BaseHook.get_connection(PG_CONN_ID)
    mongo_conn = BaseHook.get_connection(MONGO_CONN_ID)

    pg_url = (
        f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}"
    )

    return [
        "--mode", mode,
        "--pg-url", pg_url,
        "--pg-user", pg_conn.login,
        "--pg-password", pg_conn.password,
        "--mongo-uri", f"mongodb://{mongo_conn.host}:{mongo_conn.port}",
        "--mongo-database", DWH_DATABASE,
        "--qc-max-null-rate", str(QC_MAX_NULL_RATE),
    ]


# FONCTIONS DE CONTROLE QUALITE


def check_data_quality():

    client = MongoHook(mongo_conn_id=MONGO_CONN_ID).get_conn()

    try:
        rapport = client[DWH_DATABASE]["qc_report"].find_one({"_id": "dernier_rapport"})
    finally:
        client.close()

    if rapport is None:
        raise AirflowFailException(
            "Aucun rapport de qualite n'a ete produit par le job Spark"
        )

    taux_null = rapport.get("valeur_null_rate", 1.0)

    logger.info(
        "Controle qualite : %s notes, taux de valeurs NULL %.2f%% (seuil %.2f%%)",
        rapport.get("note_total"),
        taux_null * 100,
        rapport.get("seuil_null_rate", QC_MAX_NULL_RATE) * 100,
    )

    if rapport.get("statut") == "ANOMALIE":
        logger.warning(
            "Anomalie detectee sur les valeurs de notes : le chargement "
            "de fait_notes est court-circuite"
        )
        return False

    return True



# FONCTION GENERIQUE POSTGRESQL -> MONGODB


def migrate_table(sql_query, mongo_collection):

    # 1. Connexion à PostgreSQL

    pg_hook = PostgresHook(
        postgres_conn_id="postgres_default"
    )

    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    cursor.execute(sql_query)
    rows = cursor.fetchall()

    logger.info(
        "[%s] %d lignes extraites depuis PostgreSQL",
        mongo_collection, len(rows)
    )

    # 2. Connexion à MongoDB

    mongo_hook = MongoHook(
        mongo_conn_id="mongo_default"
    )

    client = mongo_hook.get_conn()

    db = client["dwh_airflow"]

    collection = db[mongo_collection]

    # 3. Rechargement complet de la collection

    collection.drop()

    # 4. Transformation

    documents = []

    for row in rows:

        data = row[0] if isinstance(row, tuple) else row

        if isinstance(data, str):
            document = json.loads(data)
        else:
            document = data

        documents.append(document)

    # 5. Contrôle qualité : taux de valeurs NULL par champ

    field_null_counts = {}

    for document in documents:
        for field, value in document.items():
            if value is None:
                field_null_counts[field] = field_null_counts.get(field, 0) + 1

    for field, null_count in sorted(field_null_counts.items()):
        logger.warning(
            "[%s] champ '%s' : %d/%d valeurs NULL (%.1f%%)",
            mongo_collection, field, null_count, len(documents),
            null_count / len(documents) * 100
        )

    # 6. Insertion

    if documents:
        collection.insert_many(documents)

    logger.info(
        "[%s] chargement terminé : %d documents dans la collection",
        mongo_collection, collection.count_documents({})
    )

    # 7. Fermeture des connexions

    cursor.close()
    connection.close()
    client.close()


# DAG

with DAG(
    dag_id="dwh_scolaire_postgres_to_mongo",

    default_args=default_args,

    description=(
        "ETL du Data Warehouse scolaire "
        "PostgreSQL vers MongoDB"
    ),

    schedule="@daily",

    catchup=False,

) as dag:

    # DIM_ETUDIANT

    sql_etudiant = """

        SELECT json_build_object(

            'id_DimEtudiant',
                ROW_NUMBER() OVER (
                    ORDER BY e.id_etudiant
                ),

            'id_etudiant',
                e.id_etudiant,

            'nom',
                e.nom,

            'prenom',
                e.prenom,

            'date_naissance',
                e.date_naissance,

            'sexe',
                e.sexe

        )::text

        FROM ETUDIANT e;

    """


    # DIM_ENSEIGNANT

    sql_enseignant = """

        SELECT json_build_object(

            'id_DimEnseignant',
                p.id_professeur,

            'id_enseignant',
                p.id_professeur,

            'nom',
                p.nom,

            'prenom',
                p.prenom,

            'specialite',
                p.specialite

        )::text

        FROM PROFESSEUR p;

    """


    # DIM_CLASSE

    sql_classe = """

        SELECT json_build_object(

            'id_DimClasse',
                c.id_classe,

            'id_classe',
                c.id_classe,

            'niveau',
                c.niveau,

            'filiere',
                c.filiere

        )::text

        FROM CLASSE c;

    """


    # DIM_MATIERE

    sql_matiere = """

        SELECT json_build_object(

            'id_DimMatiere',
                m.id_matiere,

            'id_matiere',
                m.id_matiere,

            'libelle',
                m.libelle,

            'credit',
                m.coefficient

        )::text

        FROM MATIERE m;

    """


    # DIM_TEMPS

    sql_temps = """

        SELECT json_build_object(

            'id_DimTemps',
                ROW_NUMBER() OVER (
                    ORDER BY t.date_evaluation
                ),

            'date_evaluation',
                t.date_evaluation,

            'annee',
                EXTRACT(
                    YEAR FROM t.date_evaluation
                )::INT,

            'mois',
                EXTRACT(
                    MONTH FROM t.date_evaluation
                )::INT,

            'semestre',
                CASE
                    WHEN EXTRACT(
                        MONTH FROM t.date_evaluation
                    ) <= 6
                    THEN 1
                    ELSE 2
                END

        )::text

        FROM (
            SELECT DISTINCT date_evaluation
            FROM NOTE
        ) t;

    """


    # DIM_TYPE_EVALUATION

    sql_type_evaluation = """

        SELECT json_build_object(

            'id_DimTypeEvaluation',
                ROW_NUMBER() OVER (
                    ORDER BY t.type_evaluation
                ),

            'type_evaluation',
                t.type_evaluation

        )::text

        FROM (
            SELECT DISTINCT type_evaluation
            FROM NOTE
        ) t;

    """


    # FAIT_NOTES - VERSION SQL CONSERVEE POUR REFERENCE
    # Cette requete n'est plus executee : fait_notes est desormais produite par
    # le job PySpark (tache run_pyspark_load). Elle est gardee ici pour comparer les
    # deux approches et pour documenter le modele sans transformation.

    sql_fait_notes = """

                     SELECT json_build_object( \
                                    'id_fait',
                                    n.id_note, \
                                    'note',
                                    n.valeur, \
                                    'credit',
                                    m.coefficient, \
                                    'id_DimTemps_Fk',
                                    temps.id_DimTemps, \
                                    'id_DimTypeEvaluation_FK',
                                    type_eval.id_DimTypeEvaluation, \
                                    'id_DimEtudiant_FK',
                                    e.id_etudiant, \
                                    'id_DimMatiere_FK',
                                    m.id_matiere, \
                                    'id_DimClasse_FK',
                                    c.id_classe \
                            ) ::text

                     FROM NOTE n

                              JOIN MATIERE m
                                   ON n.id_matiere = m.id_matiere

                              JOIN ETUDIANT e
                                   ON n.id_etudiant = e.id_etudiant

                              JOIN CLASSE c
                                   ON e.id_classe = c.id_classe

                              JOIN (SELECT date_evaluation, \
                                           ROW_NUMBER() OVER (
                    ORDER BY date_evaluation
                ) AS id_DimTemps \

                                    FROM (SELECT DISTINCT date_evaluation \
                                          FROM NOTE) dates) temps
                                   ON temps.date_evaluation = n.date_evaluation

                              JOIN (SELECT type_evaluation, \
                                           ROW_NUMBER() OVER (
                    ORDER BY type_evaluation
                ) AS id_DimTypeEvaluation \

                                    FROM (SELECT DISTINCT type_evaluation \
                                          FROM NOTE) types) type_eval
                                   ON type_eval.type_evaluation = n.type_evaluation; \

                     """


    # TASK : DIM_ETUDIANT

    t_etudiant = PythonOperator(

        task_id="load_dim_etudiant",

        python_callable=migrate_table,

        op_kwargs={
            "sql_query": sql_etudiant,

            "mongo_collection": "dim_etudiant",
        },
    )


    # TASK : DIM_ENSEIGNANT

    t_enseignant = PythonOperator(

        task_id="load_dim_enseignant",

        python_callable=migrate_table,

        op_kwargs={
            "sql_query": sql_enseignant,

            "mongo_collection": "dim_enseignant",
        },
    )


    # TASK : DIM_CLASSE

    t_classe = PythonOperator(

        task_id="load_dim_classe",

        python_callable=migrate_table,

        op_kwargs={
            "sql_query": sql_classe,

            "mongo_collection": "dim_classe",
        },
    )


    # TASK : DIM_MATIERE

    t_matiere = PythonOperator(

        task_id="load_dim_matiere",

        python_callable=migrate_table,

        op_kwargs={
            "sql_query": sql_matiere,

            "mongo_collection": "dim_matiere",
        },
    )


    # TASK : DIM_TEMPS

    t_temps = PythonOperator(

        task_id="load_dim_temps",

        python_callable=migrate_table,

        op_kwargs={
            "sql_query": sql_temps,

            "mongo_collection": "dim_temps",
        },
    )


    # TASK : DIM_TYPE_EVALUATION

    t_type_evaluation = PythonOperator(

        task_id="load_dim_type_evaluation",

        python_callable=migrate_table,

        op_kwargs={
            "sql_query": sql_type_evaluation,

            "mongo_collection": "dim_type_evaluation",
        },
    )


    # TASK : CONTROLE QUALITE (job Spark)

    t_spark_qc = SparkSubmitOperator(

        task_id="run_pyspark_qc",

        application=SPARK_APPLICATION,

        conn_id=SPARK_CONN_ID,

        application_args=spark_arguments("qc"),

        # jars : distribue le driver JDBC aux executeurs du cluster.
        # driver_class_path : indispensable en plus, car la lecture JDBC
        # (creation du DataFrameReader) a lieu dans le driver, et un jar
        # passe par --jars seul n'y est pas ajoute.
        jars=SPARK_JDBC_JAR,

        driver_class_path=SPARK_JDBC_JAR,

        verbose=True,
    )


    # TASK : VERIFICATION DU RAPPORT DE QUALITE

    t_check_qualite = ShortCircuitOperator(

        task_id="check_qualite",

        python_callable=check_data_quality,
    )


    # TASK : CHARGEMENT FAIT_NOTES ET INDICATEURS (job Spark)
    # Remplace l'ancien load_fait_notes en SQL.

    t_spark_load = SparkSubmitOperator(

        task_id="run_pyspark_load",

        application=SPARK_APPLICATION,

        conn_id=SPARK_CONN_ID,

        application_args=spark_arguments("load"),

        jars=SPARK_JDBC_JAR,

        driver_class_path=SPARK_JDBC_JAR,

        verbose=True,
    )


    # DEPENDANCES

    [
        t_etudiant,
        t_enseignant,
        t_classe,
        t_matiere,
        t_temps,
        t_type_evaluation,

    ] >> t_spark_qc >> t_check_qualite >> t_spark_load
