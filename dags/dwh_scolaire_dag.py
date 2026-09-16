from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator

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


    # FAIT_NOTES

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


    # TASK : FAIT_NOTES

    t_fait_notes = PythonOperator(

        task_id="load_fait_notes",

        python_callable=migrate_table,

        op_kwargs={
            "sql_query": sql_fait_notes,

            "mongo_collection": "fait_notes",
        },
    )


    # DEPENDANCES

    [
        t_etudiant,
        t_enseignant,
        t_classe,
        t_matiere,
        t_temps,
        t_type_evaluation,

    ] >> t_fait_notes