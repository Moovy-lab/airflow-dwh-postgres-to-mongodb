# 🎓 Data Warehouse Scolaire — ETL PostgreSQL → MongoDB avec Airflow et Spark

Projet de **Data Engineering** permettant de construire et d'alimenter un **Data Warehouse scolaire** à partir d'une base de données PostgreSQL.

Le pipeline ETL est orchestré avec **Apache Airflow**. Les dimensions sont produites en SQL, la table de faits et les indicateurs sont calculés par un job **PySpark** exécuté sur un cluster **Spark standalone**, et le Data Warehouse est stocké dans **MongoDB**.

---

## 📌 Présentation du projet

L'objectif du projet est de mettre en place un pipeline permettant de :

1. récupérer les données depuis **PostgreSQL** ;
2. transformer les données selon un modèle dimensionnel ;
3. charger les dimensions dans **MongoDB** ;
4. contrôler la qualité de la source avant tout chargement ;
5. construire la table de faits `fait_notes` et des indicateurs agrégés avec **Spark** ;
6. orchestrer automatiquement l'ensemble du processus avec **Apache Airflow** ;
7. exécuter l'environnement avec **Docker**.

### Architecture générale

```text
                  ┌─────────────────────┐
                  │     PostgreSQL      │
                  │    Base scolaire    │
                  └──────────┬──────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
       6 dimensions (SQL)         fait_notes + indicateurs
              │                        (PySpark)
              │                             │
              └──────────────┬──────────────┘
                             │
                  ┌──────────▼──────────┐
                  │   Apache Airflow    │
                  │   (scheduler)       │
                  └──────────┬──────────┘
                             │
              spark-submit   │
                             ▼
                  ┌─────────────────────┐        ┌─────────────────────┐
                  │    Spark Master     │◄──────►│     Spark Worker    │
                  └─────────────────────┘        └─────────────────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │      MongoDB        │
                  │   Data Warehouse    │
                  └─────────────────────┘
```

---

# 🛠️ Technologies utilisées

| Technologie           | Utilisation                                     |
| --------------------- | ----------------------------------------------- |
| **PostgreSQL**        | Base de données source                          |
| **Apache Airflow**    | Orchestration du pipeline ETL                   |
| **Apache Spark**      | Calcul distribué de la table de faits          |
| **PySpark**           | Job Spark de transformation et d'agrégation    |
| **MongoDB**           | Data Warehouse cible                            |
| **Docker**            | Conteneurisation de tous les composants         |
| **Python**            | Développement des tâches ETL                    |
| **SQL**               | Extraction et transformation des dimensions     |
| **Portainer**         | Administration graphique des conteneurs         |

---

# 🗄️ Base de données source

La base PostgreSQL contient les principales tables suivantes :

```text
CLASSE
   │
   └── ETUDIANT

PROFESSEUR
   │
   └── ENSEIGNER ─── MATIERE
                         │
                         └── CONCERNER ─── CLASSE

ETUDIANT ─────────────── NOTE ───────────── MATIERE
                           │
                           └── PROFESSEUR
```

### Tables principales

* `CLASSE`
* `ETUDIANT`
* `PROFESSEUR`
* `MATIERE`
* `ENSEIGNER`
* `CONCERNER`
* `NOTE`

La table `NOTE` contient notamment :

```text
id_note
type_evaluation
date_evaluation
valeur
id_etudiant
id_matiere
id_professeur
```

### Jeu de données initial

Le script d'initialisation est versionné dans `data/G2.sql`. Il est exécuté
automatiquement au **premier** démarrage du service `postgres-source`, car le
conteneur monte ce dossier dans `/docker-entrypoint-initdb.d`.

Le script contient à l'origine quelques instructions d'exercice (insertion d'un
étudiant de test, ajout d'une colonne, trigger d'audit) qui échouent sur les
contraintes d'intégrité ou qui modifient le schéma. Elles sont **commentées**
avec le marqueur suivant :

```sql
-- [DESACTIVE - exercice de TP]
```

Le volume `postgres-source-volume` conserve les données : pour rejouer
l'initialisation, il faut supprimer le volume puis recréer le service.

---

# ⭐ Modèle dimensionnel

Le Data Warehouse est organisé autour de plusieurs dimensions et d'une table de faits.

### Dimensions

```text
dim_etudiant
dim_enseignant
dim_classe
dim_matiere
dim_temps
dim_type_evaluation
```

### Table de faits

```text
fait_notes
```

Le modèle peut être représenté ainsi :

```text
                         ┌──────────────────┐
                         │  dim_enseignant  │
                         └────────┬─────────┘
                                  │
                                  │
┌─────────────────┐              │              ┌─────────────────┐
│  dim_etudiant   │──────────┐    │    ┌─────────│   dim_matiere   │
└─────────────────┘          │    │    │         └─────────────────┘
                             ▼    ▼    ▼
                        ┌─────────────────┐
                        │   fait_notes    │
                        └─────────────────┘
                             ▲    ▲    ▲
                             │    │    │
┌─────────────────┐         │    │    └──────────┐
│   dim_classe    │─────────┘    │               │
└─────────────────┘              │               │
                                │               │
                         ┌───────┴────────┐ ┌────┴──────────────────┐
                         │   dim_temps    │ │ dim_type_evaluation  │
                         └────────────────┘ └───────────────────────┘
```

---

# 📦 Collections MongoDB

La base MongoDB utilisée comme Data Warehouse est :

```text
dwh_airflow
```

Elle contient les collections suivantes :

```text
dwh_airflow
│
├── dim_etudiant
├── dim_enseignant
├── dim_classe
├── dim_matiere
├── dim_temps
├── dim_type_evaluation
├── fait_notes
├── agg_moyenne_par_matiere
├── agg_moyenne_par_etudiant
└── qc_report
```

| Collection                  | Rôle                                            | Origine      |
| --------------------------- | ----------------------------------------------- | ------------ |
| `dim_*`                     | Dimensions du modèle en étoile                  | SQL (Airflow)|
| `fait_notes`                | Table de faits                                  | PySpark      |
| `agg_moyenne_par_matiere`   | Indicateurs par matière                         | PySpark      |
| `agg_moyenne_par_etudiant`  | Indicateurs par étudiant                        | PySpark      |
| `qc_report`                 | Dernier rapport de contrôle qualité             | PySpark      |

---

# 🔄 Pipeline ETL

Le DAG Airflow principal est :

```text
dwh_scolaire_postgres_to_mongo
```

Il réalise les traitements suivants :

```text
PostgreSQL
    │
    ├── ETUDIANT ──────────────► dim_etudiant
    │
    ├── PROFESSEUR ────────────► dim_enseignant
    │
    ├── CLASSE ────────────────► dim_classe
    │
    ├── MATIERE ───────────────► dim_matiere
    │
    ├── NOTE ──────────────────► dim_temps
    │
    ├── NOTE ──────────────────► dim_type_evaluation
    │
    ├── NOTE ──────────────────► qc_report        (contrôle qualité)
    │
    └── NOTE + dimensions ─────► fait_notes
                                  agg_moyenne_par_matiere
                                  agg_moyenne_par_etudiant
```

---

# ⚙️ Fonctionnement de l'ETL

## 1. Extraction des dimensions

Airflow utilise `PostgresHook` pour se connecter à PostgreSQL.

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

pg_hook = PostgresHook(
    postgres_conn_id="postgres_default"
)
```

Les requêtes SQL permettent d'extraire les données nécessaires.

---

## 2. Transformation des dimensions

Les données PostgreSQL sont transformées en documents JSON.

Exemple :

```sql
SELECT json_build_object(
    'id_DimMatiere', m.id_matiere,
    'id_matiere', m.id_matiere,
    'libelle', m.libelle,
    'credit', m.coefficient
)
FROM MATIERE m;
```

---

## 3. Chargement des dimensions

Airflow utilise `MongoHook` pour accéder à MongoDB :

```python
from airflow.providers.mongo.hooks.mongo import MongoHook

mongo_hook = MongoHook(
    mongo_conn_id="mongo_default"
)
```

Chaque collection est **entièrement rechargée** à chaque exécution du DAG : la collection cible est d'abord vidée, puis repeuplée avec les documents fraîchement extraits :

```python
collection.drop()
collection.insert_many(documents)
```

Cela garantit que le contenu de MongoDB reflète toujours exactement l'état courant de PostgreSQL (pas de doublons, pas de champs obsolètes qui subsisteraient d'une exécution précédente).

---

## 4. Contrôle qualité (Spark)

Le job Spark est exécuté une première fois en mode `qc`. Il compte les notes,
compte les valeurs nulles et publie le rapport :

```javascript
{
  _id: "dernier_rapport",
  horodatage: "2026-09-28T14:33:43.273944+00:00",
  note_total: 23940,
  valeur_null: 730,
  valeur_null_rate: 0.0305,
  seuil_null_rate: 0.2,
  statut: "OK"
}
```

La tâche Airflow `check_qualite` relit ce rapport. Si le statut vaut `ANOMALIE`,
un `ShortCircuitOperator` **empêche** le chargement des faits : les dimensions
sont bien chargées, mais `fait_notes` n'est pas produit sur une source douteuse.

Le seuil par défaut est de 20 % de valeurs nulles, modifiable avec la constante
`QC_MAX_NULL_RATE` du DAG.

---

## 5. Table de faits et indicateurs (Spark)

Le job est ensuite exécuté en mode `load`. Il :

* lit `NOTE` en **JDBC partitionné** sur `id_matiere` (jusqu'à 4 partitions en
  parallèle) au lieu d'un `SELECT` unique piloté par le driver ;
* recalcule les clés de substitution `id_DimTemps` et `id_DimTypeEvaluation`
  avec `dense_rank` (équivalent des `ROW_NUMBER() OVER (ORDER BY ...)` SQL) ;
* joint `MATIERE`, `ETUDIANT` et `CLASSE` pour produire `fait_notes` ;
* calcule deux collections d'indicateurs agrégés ;
* écrit le tout dans MongoDB par lots de 1000 documents.

L'écriture MongoDB est faite **depuis le driver** avec `pymongo` : les
exécuteurs n'ont pas `pymongo` (l'image `apache/spark` est une image Scala) et
l'on évite d'embarquer un second connecteur.

---

# 📊 Table de faits `fait_notes`

La collection `fait_notes` constitue le cœur du Data Warehouse.

Chaque document représente une note attribuée à un étudiant.

Exemple réel :

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

> ⚠️ **Pas de lien `fait_notes` → `dim_enseignant`.** Dans la base source, la table `NOTE` ne renseigne jamais `id_professeur` (colonne systématiquement `NULL`) : les données générées associent un professeur à une **matière** (table `ENSEIGNER`, relation many-to-many), jamais à une note individuelle. Il n'existe donc aucune façon fiable et non ambiguë de déduire l'enseignant d'une note précise. La dimension `dim_enseignant` est tout de même chargée (utile pour d'autres analyses, ex. charge d'enseignement par professeur), mais `fait_notes` ne la référence pas.

### Relations

```text
id_DimEtudiant_FK
        │
        ▼
   dim_etudiant

id_DimMatiere_FK
        │
        ▼
   dim_matiere

id_DimClasse_FK
        │
        ▼
   dim_classe

id_DimTemps_Fk
        │
        ▼
   dim_temps

id_DimTypeEvaluation_FK
        │
        ▼
dim_type_evaluation
```

### Indicateurs

`agg_moyenne_par_matiere` :

```json
{
    "id_DimMatiere_FK": 28,
    "nb_notes": 724,
    "moyenne": 11.65,
    "note_min": -4.85,
    "note_max": 24.82,
    "nb_reussites": 476,
    "taux_reussite_pct": 65.75
}
```

`agg_moyenne_par_etudiant` :

```json
{
    "id_DimEtudiant_FK": "ETU2024-00307",
    "nb_notes": 26,
    "moyenne": 10.23,
    "note_min": -3.58,
    "note_max": 21.2,
    "credits_total": 74,
    "nb_reussites": 13,
    "taux_reussite_pct": 50
}
```

Une note est considérée comme réussie à partir de `10`.

---

# ⚡ Apache Spark

## Cluster

Le projet utilise un cluster **Spark standalone** déclaré dans
`docker-compose.yaml` :

| Service       | Image              | Rôle                                |
| ------------- | ------------------ | ----------------------------------- |
| `spark-master`| `apache/spark:3.5.0` | maître du cluster, UI web          |
| `spark-worker`| `apache/spark:3.5.0` | exécuteur (1 Gio, 2 cœurs)         |

Le driver, lui, tourne dans le conteneur Airflow : c'est lui qui soumet
l'application et qui écrit dans MongoDB.

> ⚠️ **Différence avec la distribution binaire officielle.** Dans l'image
> `apache/spark`, les scripts sont dans `/opt/spark/bin` et **pas** dans
> `/opt/spark/sbin` comme dans l'archive officielle. Le master est donc lancé
> directement par le processus Java, au premier plan (pas de daemon), ce qui est
> plus adapté à Docker :
>
> ```yaml
> command:
>   - /opt/spark/bin/spark-class
>   - org.apache.spark.deploy.master.Master
>   - --host
>   - spark-master
>   - --port
>   - "7077"
> ```
>
> Le healthcheck du master utilise `bash` et non `sh` : dans cette image
> `/bin/sh` est `dash`, qui ne gère pas `/dev/tcp`.

## Driver JDBC PostgreSQL

Le job lit PostgreSQL en JDBC : le driver `postgresql-42.7.3.jar` est téléchargé
et embarqué dans l'image Airflow (`/opt/airflow/spark/jars/`), puis transmis au
cluster par Airflow.

```dockerfile
ARG POSTGRES_JDBC_VERSION=42.7.3
```

> ⚠️ **`jars` ne suffit pas.** Le paramètre `jars` de `SparkSubmitOperator`
> génère un `--jars`, qui distribue le driver aux exécuteurs. Or la lecture JDBC
> (création du `DataFrameReader`) a lieu dans le **driver**. Il faut donc aussi
> `driver_class_path`, sans quoi l'exécution échoue avec
> `java.sql.SQLException: No suitable driver`.
>
> ```python
> jars=SPARK_JDBC_JAR,
> driver_class_path=SPARK_JDBC_JAR,
> ```

## Soumission depuis Airflow

L'URL du master n'est pas passée en paramètre de la tâche : elle est lue dans la
connexion Airflow `spark_default`, qui doit donc porter le **schéma dans le
champ host** (`spark://spark-master` + port `7077`), car le provider reconstruit
l'URL ainsi :

```python
master = f"{conn.host}:{conn.port}" if conn.port else conn.host
```

Le job est piloté par un seul script, `spark/jobs/spark.py`, monté en lecture
dans `/opt/airflow/spark/jobs`. Son mode est choisi par un argument :

```text
--mode qc     contrôle qualité, écrit qc_report
--mode load   écrit fait_notes et les deux collections d'indicateurs
```

Le fichier est volontairement **hors du dossier `dags/`** : un fichier placé
dans `dags/` est interprété par le DagBag, et un job soumis par
`SparkSubmitOperator` n'a rien à faire dans le DagBag.

---

# 🐳 Docker

L'environnement complet est exécuté avec Docker Compose.

### Services

| Service             | Rôle                                              | Port exposé |
| ------------------- | ------------------------------------------------- | ----------- |
| `postgres-meta`     | Base interne d'Airflow                            | aucun       |
| `postgres-source`   | Base scolaire source (données du projet)          | 5433        |
| `mongodb-dwh`       | Data Warehouse MongoDB                            | 27018       |
| `spark-master`      | Maître du cluster Spark + UI web                  | 7077, 8081  |
| `spark-worker`      | Exécuteur Spark                                   | aucun       |
| `airflow-init`      | Migration de la base Airflow + création de l'admin| aucun       |
| `airflow-scheduler` | Planification et exécution des DAG                | aucun       |
| `airflow-webserver` | Interface web Airflow                             | 8080        |

> ⚠️ **Les ports 5432 et 27017 ne sont volontairement pas utilisés.** Ils sont
> occupés par les instances PostgreSQL et MongoDB installées sur la machine
> Windows, que ce projet ne modifie pas. Le projet utilise donc `5433` pour sa
> base source et `27018` pour son Data Warehouse. Côté Airflow, ce sont les noms
> de services Docker qui sont utilisés (`postgres-source`, `mongodb-dwh`,
> `spark-master`), et non `host.docker.internal`.

### Vérifier les conteneurs

```bash
docker compose ps
```

### Démarrer les services

```bash
docker compose up -d
```

### Arrêter les services

```bash
docker compose down
```

### Voir les logs

```bash
docker compose logs -f
```

Pour le scheduler :

```bash
docker compose logs airflow-scheduler
```

---

# 🌐 Interfaces

### Apache Airflow

```text
http://localhost:8080
```

Identifiants créés par le service `airflow-init` :

```text
airflow / airflow
```

Airflow permet de :

* visualiser le DAG ;
* lancer manuellement le pipeline ;
* consulter les logs ;
* suivre l'état des tâches ;
* analyser les erreurs d'exécution.

### Apache Spark

```text
http://localhost:8081
```

L'interface montre l'état du cluster, le worker enregistré et les applications
soumises, avec leurs DAG d'exécution (stages et tâches).

### Portainer

```text
http://localhost:9000
```

Portainer permet de gérer graphiquement les conteneurs Docker.

---

# 🔑 Connexions Airflow

Trois connexions sont utilisées. Elles sont créées de façon **reproductible**
par le script `scripts/init_connections.sh`, qui supprime puis recrée chaque
connexion :

```bash
docker compose run --rm --entrypoint bash airflow-scheduler \
    /opt/airflow/scripts/init_connections.sh
```

Ce script doit être exécuté **après** `airflow-init` et **avant** de démarrer
le scheduler, car le DAG lit les identifiants dans les connexions au moment de
son analyse.

## PostgreSQL

```text
Connection ID : postgres_default
Type          : postgres
Host          : postgres-source
Port          : 5432
Database      : school
Login         : school
Password      : school
```

## MongoDB

```text
Connection ID : mongo_default
Type          : mongo
Host          : mongodb-dwh
Port          : 27017
```

## Spark

```text
Connection ID : spark_default
Type          : spark
Host          : spark://spark-master
Port          : 7077
```

Le champ `host` contient volontairement le schéma `spark://` (voir la section
Apache Spark).

---

# 📁 Structure du projet

```text
.
│
├── dags/
│   └── dwh_scolaire_dag.py       DAG : dimensions SQL + 2 jobs Spark
│
├── spark/
│   └── jobs/
│       └── spark.py              job PySpark (modes qc et load)
│
├── scripts/
│   └── init_connections.sh       création reproductible des connexions
│
├── data/
│   └── G2.sql                    jeu de données source (init PostgreSQL)
│
├── logs/
│
├── plugins/
│
├── docker-compose.yaml
│
├── Dockerfile
│
├── requirements.txt
│
└── README.md
```

---

# 🚀 Installation

## 1. Cloner le projet

```bash
git clone <URL_DU_REPOSITORY>
```

Puis :

```bash
cd airflow-dwh-postgres-to-mongodb
```

---

## 2. Construire les images Docker

```bash
docker compose build
```

L'image Airflow installe les providers PostgreSQL, Mongo et Apache Spark,
`pyspark==3.5.0` (aligné sur l'image du cluster), une JRE et le driver JDBC
PostgreSQL.

---

## 3. Initialiser Airflow

```bash
docker compose up airflow-init
```

Ce service migre la base interne d'Airflow et crée l'utilisateur `airflow`.

---

## 4. Créer les connexions Airflow

```bash
docker compose run --rm --entrypoint bash airflow-scheduler \
    /opt/airflow/scripts/init_connections.sh
```

---

## 5. Démarrer la pile complète

```bash
docker compose up -d
```

Vérifier :

```bash
docker compose ps
```

Le Data Warehouse MongoDB est vide pour l'instant : il est alimenté par le DAG.

---

# ▶️ Exécution du pipeline

Une fois Airflow démarré :

1. ouvrir `http://localhost:8080` ;
2. se connecter avec `airflow` / `airflow` ;
3. rechercher le DAG :

```text
dwh_scolaire_postgres_to_mongo
```

4. activer le DAG ;
5. lancer une exécution manuelle ;
6. consulter les tâches.

Les tâches sont exécutées dans cet ordre :

```text
load_dim_etudiant         ┐
load_dim_enseignant       │
load_dim_classe           │
load_dim_matiere          ├─ en parallèle
load_dim_temps            │
load_dim_type_evaluation  ┘
              │
              ▼
        run_pyspark_qc          (job Spark, mode qc)
              │
              ▼
        check_qualite           (court-circuit si ANOMALIE)
              │
              ▼
        run_pyspark_load        (job Spark, mode load)
```

La table de faits est chargée après les dimensions **et** après le contrôle
qualité, afin que les clés de référence soient disponibles et que les faits ne
soient produits que sur une source valide.

En ligne de commande :

```bash
# état des tâches d'un run
docker compose exec airflow-scheduler airflow tasks states-for-dag-run \
    dwh_scolaire_postgres_to_mongo manual__<run_id>

# erreurs d'import des DAG
docker compose exec airflow-scheduler airflow dags list-import-errors
```

---

# 🧪 Vérification des données

Après l'exécution du DAG, MongoDB doit contenir :

```text
dwh_airflow
│
├── dim_etudiant             → 1000    données étudiants
├── dim_enseignant           → 60      données enseignants
├── dim_classe               → 24      données classes
├── dim_matiere              → 30      données matières
├── dim_temps                → 289     calendrier des évaluations
├── dim_type_evaluation      → 2       devoir / examen
├── fait_notes               → 23940   faits scolaires
├── agg_moyenne_par_matiere  → 30      indicateurs par matière
├── agg_moyenne_par_etudiant → 1000    indicateurs par étudiant
└── qc_report                → 1       dernier contrôle qualité
```

Exemple dans MongoDB :

```bash
docker compose exec mongodb-dwh mongosh dwh_airflow
```

```javascript
show collections
```

Puis :

```javascript
db.fait_notes.findOne()
db.qc_report.findOne()
```

Pour compter les faits :

```javascript
db.fait_notes.countDocuments()
```

---

# 🔍 Exemple de requête analytique

Nombre de notes enregistrées :

```javascript
db.fait_notes.countDocuments()
```

Notes supérieures ou égales à 10 :

```javascript
db.fait_notes.countDocuments({
    note: { $gte: 10 }
})
```

Moyenne générale :

```javascript
db.fait_notes.aggregate([
    {
        $group: {
            _id: null,
            moyenne: { $avg: "$note" }
        }
    }
])
```

Les mêmes indicateurs sont déjà précalculés par Spark :

```javascript
db.agg_moyenne_par_etudiant.find({ moyenne: { $lt: 8 } }).sort({ moyenne: 1 })
```

---

# ⚠️ Limites connues

* Les identifiants PostgreSQL et MongoDB sont passés en arguments de
  `spark-submit` : ils sont donc visibles dans la liste des processus et dans
  l'interface Spark. Pour un déploiement réel, il faudrait passer par un
  `spark-defaults.conf` avec des variables d'environnement ou un gestionnaire de
  secrets.
* Le DAG lit les connexions Airflow au moment de son analyse, pas à l'exécution
  de la tâche : les connexions doivent exister avant le démarrage du scheduler.
* Les écritures MongoDB sont faites depuis le driver : les exécuteurs font les
  calculs, mais l'écriture reste centralisée.
* `dense_rank()` est un classement global, Spark déplace donc les données dans
  une seule partition et émet un avertissement `WindowExec: No Partition
  Defined`. C'est le comportement attendu et équivalent au `ROW_NUMBER() OVER
  (ORDER BY ...)` de la version SQL ; sur un gros volume, il faudrait
  partitionner autrement.
* Le pipeline est en rechargement total, sans stratégie incrémentale.

---

# 🎯 Objectifs pédagogiques

Ce projet permet de mettre en pratique :

* la conception d'un **Data Warehouse** ;
* le **modèle dimensionnel** ;
* les dimensions et les faits ;
* les processus **ETL** ;
* l'orchestration avec **Apache Airflow** ;
* les `PostgresHook`, `MongoHook` et `SparkSubmitOperator` ;
* les requêtes SQL ;
* la transformation SQL → JSON ;
* le calcul distribué avec **Spark** et **PySpark** ;
* la lecture **JDBC partitionnée** ;
* les agrégations et fenêtre SQL (`groupBy`, `dense_rank`) ;
* le **contrôle qualité** de données et le court-circuit de DAG ;
* le chargement MongoDB ;
* Docker et Docker Compose ;
* la supervision des pipelines ;
* les requêtes analytiques MongoDB.

---

# 🔮 Améliorations possibles

Plusieurs évolutions peuvent être ajoutées :

* utilisation du connecteur **Spark MongoDB** officiel pour écrire depuis les
  exécuteurs ;
* gestion des erreurs et des données invalides ;
* ajout de logs métier ;
* ajout de tests automatisés ;
* création d'index MongoDB ;
* ajout d'un dashboard avec **Power BI**, **Metabase** ou une autre solution de BI ;
* ajout d'une stratégie de chargement incrémental ;
* historisation des dimensions ;
* passage à un cluster Spark managé ;
* automatisation complète du déploiement.

---

# 👨‍💻 Auteur

Projet réalisé dans le cadre d'un apprentissage en **Data Engineering**.

### Stack

```text
PostgreSQL
     +
Python / SQL
     +
Apache Airflow
     +
Apache Spark / PySpark
     +
MongoDB
     +
Docker
```

---

## 📄 Licence

Projet réalisé à des fins pédagogiques et académiques.
