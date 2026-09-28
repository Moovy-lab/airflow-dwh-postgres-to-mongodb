# 🎬 Démo — Data Warehouse scolaire : Airflow + Spark → MongoDB

Script de démonstration de bout en bout du pipeline. À suivre dans l'ordre.
Toutes les commandes sont lancées depuis la racine du projet.

---

## 0. Rappel de l'environnement

| Composant      | Accès                       | Identifiants          |
| -------------- | --------------------------- | --------------------- |
| **Airflow UI** | http://localhost:8080       | `airflow` / `airflow` |
| **Spark UI**   | http://localhost:8081       | —                     |
| **PostgreSQL** | localhost:5433 (db `school`) | `school` / `school`  |
| **MongoDB**    | localhost:27018 (db `dwh_airflow`) | —            |

Services Docker : `postgres-source`, `mongodb-dwh`, `spark-master`,
`spark-worker`, `airflow-init`, `airflow-scheduler`, `airflow-webserver`.

---

## 1. Vérifier que la pile est démarrée

```bash
docker compose ps
```

Résultat attendu (tous `Up` / `healthy`) :

```text
airflow-airflow-webserver-1   Up ... (healthy)    0.0.0.0:8080->8080/tcp
airflow-mongodb-dwh-1         Up ... (healthy)    0.0.0.0:27018->27017/tcp
airflow-postgres-meta-1       Up ... (healthy)
airflow-postgres-source-1     Up ... (healthy)    0.0.0.0:5433->5432/tcp
airflow-spark-master-1        Up ... (healthy)    0.0.0.0:7077->7077/tcp, 0.0.0.0:8081->8080/tcp
airflow-spark-worker-1        Up ...
```

> Si tout est arrêté ou s'il s'agit d'une première installation, voir le README
> (sinon : `docker compose up -d`).

---

## 2. Vérifier la source PostgreSQL

```bash
docker compose exec postgres-source psql -U school -d school -c \
  "SELECT (SELECT count(*) FROM etudiant) etudiants,
          (SELECT count(*) FROM matiere)  matieres,
          (SELECT count(*) FROM note)     notes;"
```

Résultat attendu :

```text
 etudiants | matieres | notes
-----------+----------+--------
      1000 |       30 |  23940
```

---

## 3. Vérifier le cluster Spark

Ouvrir **http://localhost:8081** :

* le master doit être `ALIVE` ;
* un worker doit être enregistré : **2 cœurs**, **1024.0 MiB** ;
* aucune application active au départ (ou seulement les jobs précédents en `FINISHED`).

Depuis la ligne de commande :

```bash
docker compose logs spark-worker | Select-String "Successfully registered"
```

---

## 4. Lancer le pipeline depuis l'UI Airflow

1. Ouvrir **http://localhost:8080**.
2. Se connecter avec `airflow` / `airflow`.
3. Aller dans **DAGs** et chercher `dwh_scolaire_postgres_to_mongo`.
4. Activer le DAG (toggle à gauche).
5. Cliquer sur **▶ / Trigger DAG** (triangle avec « Trigger DAG »).
6. Ouvrir le run : les tâches s'exécutent dans l'ordre graphique.

Déroulé attendu des tâches :

```text
load_dim_etudiant / load_dim_enseignant / load_dim_classe
load_dim_matiere / load_dim_temps / load_dim_type_evaluation
                          │  (en parallèle)
                          ▼
                    run_pyspark_qc        → contrôle qualité source
                          ▼
                    check_qualite         → court-circuit si ANOMALIE
                          ▼
                    run_pyspark_load      → fait_notes + indicateurs
```

Chaque run dure environ **4 à 5 minutes** en local (Spark lourd à démarrer).

---

## 5. Suivre l'exécution en ligne de commande (alternative à l'UI)

```bash
# état des tâches du dernier run
docker compose exec airflow-scheduler airflow dags list-runs \
  -d dwh_scolaire_postgres_to_mongo
```

Pour l'état des tâches d'un run précis :

```bash
docker compose exec airflow-scheduler airflow tasks states-for-dag-run \
  dwh_scolaire_postgres_to_mongo <run_id>
```

Exemple de run_id pour un déclenchement manuel :
`manual__2026-09-28T14:31:54+00:00`.

---

## 6. Vérifier le contrôle qualité

```bash
docker compose exec mongodb-dwh mongosh dwh_airflow --quiet \
  --eval "printjson(db.qc_report.findOne())"
```

Résultat attendu (statut `OK`, seuil 20 %) :

```json
{
  "_id": "dernier_rapport",
  "horodatage": "…",
  "note_total": 23940,
  "valeur_null": 730,
  "valeur_null_rate": 0.0305,
  "seuil_null_rate": 0.2,
  "statut": "OK"
}
```

---

## 7. Vérifier le Data Warehouse

```bash
docker compose exec mongodb-dwh mongosh dwh_airflow --quiet \
  --eval "db.getCollectionNames().forEach(function(c){ print(c + ' : ' + db[c].countDocuments()) })"
```

Résultat attendu :

```text
agg_moyenne_par_etudiant : 1000
agg_moyenne_par_matiere : 30
dim_classe : 24
dim_enseignant : 60
dim_etudiant : 1000
dim_matiere : 30
dim_temps : 289
dim_type_evaluation : 2
fait_notes : 23940
qc_report : 1
```

Un échantillon de faits :

```bash
docker compose exec mongodb-dwh mongosh dwh_airflow --quiet \
  --eval "printjson(db.fait_notes.findOne())"
```

```json
{
  "id_fait": 17370,
  "note": 12.73,
  "credit": 3,
  "id_DimTemps_Fk": 2,
  "id_DimTypeEvaluation_FK": 2,
  "id_DimEtudiant_FK": "ETU2025-00729",
  "id_DimMatiere_FK": 7,
  "id_DimClasse_FK": 1
}
```

---

## 8. Petit exemple d'analyse

Notes ≥ 10 :

```bash
docker compose exec mongodb-dwh mongosh dwh_airflow --quiet \
  --eval "print('notes >= 10 : ' + db.fait_notes.countDocuments({ note: { \$gte: 10 } }))"
```

Étudiants avec une moyenne < 8 (précalculé par Spark) :

```bash
docker compose exec mongodb-dwh mongosh dwh_airflow --quiet \
  --eval "db.agg_moyenne_par_etudiant.find({ moyenne: { \$lt: 8 } }).sort({ moyenne: 1 }).limit(5).forEach(printjson)"
```

---

## 9. Démo optionnelle : la barrière qualité (court-circuit du DAG)

Objectif : montrer que le DAG **bloque le chargement** si la source est
jugée anormale.

### a. Baisser le seuil sous le taux réel

Dans `dags/dwh_scolaire_dag.py`, passer :

```python
QC_MAX_NULL_RATE = 0.20   # →  0.01
```

Le scheduler reparse le fichier automatiquement (quelques secondes).

### b. Relancer le DAG

Trigger à nouveau le DAG (cf. étape 4). On obtient alors :

* les 6 dimensions : `success` ;
* `run_pyspark_qc` : `success`, mais rapport `statut: "ANOMALIE"` ;
* `check_qualite` : `success` **mais** il coupe la chaîne ;
* `run_pyspark_load` : **`skipped`** (page non exécutée).

Dans les logs de `check_qualite` :

```text
Anomalie detectee sur les valeurs de notes : le chargement de fait_notes est court-circuite
```

`fait_notes` et les indicateurs ne sont pas réécrits.

### c. Restaurer puis revérifier

Stopper ici et **revert obligatoire** :

```python
QC_MAX_NULL_RATE = 0.01   # →  0.20
```

Relancer le DAG une dernière fois : `fait_notes` revient à `23940` documents.

> Attention : ne pas laisser le seuil à `0.01` dans le fichier final.

---

## 10. Arrêt (fin de démo)

```bash
docker compose down
```

Pour repartir d'une base vierge (supprime **tout** : source, Mongo, métadonnées
Airflow) :

```bash
docker compose down -v
```

⚠️ `down -v` supprime aussi les données importées (G2.sql sera rejoué au
prochain démarrage de `postgres-source`).