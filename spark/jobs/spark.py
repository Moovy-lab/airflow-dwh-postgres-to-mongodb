"""
Job PySpark du DWH scolaire.

Ce job est soumis par Airflow via SparkSubmitOperator et s'execute en deux modes :

  --mode qc    Analyse la source PostgreSQL et publie un rapport de qualite
               dans la collection qc_report de MongoDB. Le DAG lit ce rapport
               et decide de poursuivre ou non le chargement.

  --mode load  Construit la table de faits fait_notes et deux collections
               d'indicateurs, puis les ecrit dans MongoDB.

La lecture de PostgreSQL se fait en JDBC partitionne sur id_matiere : la table
NOTE est donc relue en N lectures paralleles par les executors Spark, et non
dans un unique SELECT execute par le driver.

L'ecriture dans MongoDB se fait depuis le driver avec pymongo : les executors
n'ont pas pymongo (l'image apache/spark est une image Scala) et l'on evite
d'embarquer un second connecteur.
"""

import argparse
import logging
from datetime import datetime, timezone

from pymongo import MongoClient
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dwh_spark_job")

DWH_DATABASE = "dwh_airflow"
JDBC_MAX_PARTITIONS = 4
MONGO_BATCH_SIZE = 1000


def parse_args():
    parser = argparse.ArgumentParser(description="Job PySpark du DWH scolaire")
    parser.add_argument("--mode", choices=["qc", "load"], required=True)
    parser.add_argument("--pg-url", required=True)
    parser.add_argument("--pg-user", required=True)
    parser.add_argument("--pg-password", required=True)
    parser.add_argument("--mongo-uri", required=True)
    parser.add_argument("--mongo-database", default=DWH_DATABASE)
    parser.add_argument("--qc-max-null-rate", type=float, default=0.20)
    return parser.parse_args()


def jdbc_options(args, dbtable):
    return {
        "url": args.pg_url,
        "user": args.pg_user,
        "password": args.pg_password,
        "dbtable": dbtable,
    }


def read_source(spark, args, dbtable, partition_column=None, lower_bound=None,
                upper_bound=None, num_partitions=None):
    options = jdbc_options(args, dbtable)
    if partition_column is not None:
        options.update({
            "partitionColumn": partition_column,
            "lowerBound": lower_bound,
            "upperBound": upper_bound,
            "numPartitions": num_partitions,
        })
    return spark.read.format("jdbc").options(**options).load()


def read_note(spark, args):
    """Lit NOTE en JDBC partitionne sur l'intervalle reel des id_matiere."""
    bornes = read_source(
        spark, args, "(SELECT max(id_matiere) AS max_id FROM NOTE) bornes"
    ).collect()[0]["max_id"]
    max_id = int(bornes)

    if max_id < 1:
        raise ValueError("La table NOTE ne contient aucune matiere : rien a traiter")

    num_partitions = min(JDBC_MAX_PARTITIONS, max_id)

    logger.info(
        "Lecture JDBC de NOTE partitionnee sur id_matiere [1 ; %d] en %d partitions",
        max_id, num_partitions
    )

    return read_source(
        spark, args, "NOTE",
        partition_column="id_matiere",
        lower_bound=1,
        upper_bound=max_id + 1,
        num_partitions=num_partitions,
    )


def build_dims_temporelles(note):
    """Recalcule les cles de substitution id_DimTemps et id_DimTypeEvaluation.

    Remplace les ROW_NUMBER() OVER (...) de la version SQL du DAG.
    """
    temps = (
        note.select(F.col("date_evaluation").cast("date").alias("date_evaluation"))
        .distinct()
        .withColumn(
            "id_DimTemps",
            F.dense_rank().over(Window.orderBy("date_evaluation"))
        )
    )

    types = (
        note.select(F.col("type_evaluation").cast("string").alias("type_evaluation"))
        .distinct()
        .withColumn(
            "id_DimTypeEvaluation",
            F.dense_rank().over(Window.orderBy("type_evaluation"))
        )
    )

    return temps, types


def build_fait_notes(spark, args, note):
    temps, types = build_dims_temporelles(note)

    matiere = read_source(spark, args, "MATIERE").select(
        F.col("id_matiere").cast("int").alias("m_id_matiere"),
        F.col("libelle").cast("string").alias("libelle"),
        F.col("coefficient").cast("int").alias("credit"),
    )

    etudiant = read_source(spark, args, "ETUDIANT").select(
        F.col("id_etudiant").cast("string").alias("e_id_etudiant"),
        F.col("id_classe").cast("int").alias("e_id_classe"),
    )

    classe = read_source(spark, args, "CLASSE").select(
        F.col("id_classe").cast("int").alias("c_id_classe")
    )

    return (
        note.select(
            F.col("id_note").cast("int").alias("id_note"),
            F.col("valeur").cast("double").alias("valeur"),
            F.col("date_evaluation").cast("date").alias("date_evaluation"),
            F.col("type_evaluation").cast("string").alias("type_evaluation"),
            F.col("id_matiere").cast("int").alias("n_id_matiere"),
            F.col("id_etudiant").cast("string").alias("n_id_etudiant"),
        )
        .join(temps, "date_evaluation", "inner")
        .join(types, "type_evaluation", "inner")
        .join(matiere, F.col("n_id_matiere") == F.col("m_id_matiere"), "inner")
        .join(etudiant, F.col("n_id_etudiant") == F.col("e_id_etudiant"), "inner")
        .join(classe, F.col("e_id_classe") == F.col("c_id_classe"), "inner")
        .select(
            F.col("id_note").alias("id_fait"),
            F.col("valeur").alias("note"),
            F.col("credit"),
            F.col("id_DimTemps").cast("int").alias("id_DimTemps_Fk"),
            F.col("id_DimTypeEvaluation").cast("int").alias("id_DimTypeEvaluation_FK"),
            F.col("e_id_etudiant").alias("id_DimEtudiant_FK"),
            F.col("m_id_matiere").alias("id_DimMatiere_FK"),
            F.col("c_id_classe").alias("id_DimClasse_FK"),
        )
    )


def build_agg_matiere(fait):
    """Moyenne, etendue et taux de reussite par matiere."""
    notees = fait.filter(F.col("note").isNotNull())
    return (
        notees.groupBy("id_DimMatiere_FK")
        .agg(
            F.count("*").alias("nb_notes"),
            F.round(F.avg("note"), 2).alias("moyenne"),
            F.round(F.min("note"), 2).alias("note_min"),
            F.round(F.max("note"), 2).alias("note_max"),
            F.sum(F.when(F.col("note") >= 10, 1).otherwise(0)).alias("nb_reussites"),
            F.round(
                F.avg(F.when(F.col("note") >= 10, 100.0).otherwise(0.0)), 2
            ).alias("taux_reussite_pct"),
        )
    )


def build_agg_etudiant(fait):
    """Moyenne, etendue, credits et taux de reussite par etudiant."""
    notees = fait.filter(F.col("note").isNotNull())
    return (
        notees.groupBy("id_DimEtudiant_FK")
        .agg(
            F.count("*").alias("nb_notes"),
            F.round(F.avg("note"), 2).alias("moyenne"),
            F.round(F.min("note"), 2).alias("note_min"),
            F.round(F.max("note"), 2).alias("note_max"),
            F.sum("credit").alias("credits_total"),
            F.sum(F.when(F.col("note") >= 10, 1).otherwise(0)).alias("nb_reussites"),
            F.round(
                F.avg(F.when(F.col("note") >= 10, 100.0).otherwise(0.0)), 2
            ).alias("taux_reussite_pct"),
        )
    )


def write_collection(client, database, collection, rows):
    """Recharge completement une collection, par lots, depuis le driver.

    Le pipeline est en rechargement total : la collection est videe puis
    repeuplee, comme le fait deja la partie SQL du DAG.
    """
    cible = client[database][collection]
    cible.drop()

    total = 0
    lot = []

    for row in rows:
        lot.append(row.asDict(recursive=True))
        if len(lot) >= MONGO_BATCH_SIZE:
            cible.insert_many(lot)
            total += len(lot)
            lot = []

    if lot:
        cible.insert_many(lot)
        total += len(lot)

    return total


def run_qc(spark, args, note, client):
    total = note.count()
    valeurs_null = note.filter(F.col("valeur").isNull()).count()
    taux_null = round(valeurs_null / total, 4) if total else 1.0

    rapport = {
        "horodatage": datetime.now(timezone.utc).isoformat(),
        "note_total": total,
        "valeur_null": valeurs_null,
        "valeur_null_rate": taux_null,
        "seuil_null_rate": args.qc_max_null_rate,
        "statut": "OK" if taux_null <= args.qc_max_null_rate else "ANOMALIE",
    }

    client[args.mongo_database]["qc_report"].replace_one(
        {"_id": "dernier_rapport"}, rapport, upsert=True
    )

    logger.info(
        "Controle qualite : %d notes, %d valeurs NULL (%.2f%%), seuil %.2f%% -> %s",
        total, valeurs_null, taux_null * 100,
        args.qc_max_null_rate * 100, rapport["statut"]
    )

    return rapport


def run_load(spark, args, note, client):
    fait = build_fait_notes(spark, args, note)

    ecritures = [
        ("fait_notes", fait),
        ("agg_moyenne_par_matiere", build_agg_matiere(fait)),
        ("agg_moyenne_par_etudiant", build_agg_etudiant(fait)),
    ]

    for nom, dataframe in ecritures:
        nombre = write_collection(
            client, args.mongo_database, nom, dataframe.toLocalIterator()
        )
        logger.info("Collection %s : %d documents ecrits", nom, nombre)


def main():
    args = parse_args()

    spark = SparkSession.builder.appName(f"dwh_scolaire_{args.mode}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    client = MongoClient(args.mongo_uri)

    try:
        note = read_note(spark, args)

        if args.mode == "qc":
            run_qc(spark, args, note, client)
        else:
            run_load(spark, args, note, client)
    finally:
        client.close()
        spark.stop()


if __name__ == "__main__":
    main()
