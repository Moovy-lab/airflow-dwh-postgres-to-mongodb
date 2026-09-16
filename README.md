# 🎓 Data Warehouse Scolaire — ETL PostgreSQL → MongoDB avec Apache Airflow

Projet de **Data Engineering** permettant de construire et d'alimenter un **Data Warehouse scolaire** à partir d'une base de données PostgreSQL.

Le pipeline ETL est orchestré avec **Apache Airflow** et les données transformées sont stockées dans **MongoDB**, utilisé comme Data Warehouse.

---

## 📌 Présentation du projet

L'objectif du projet est de mettre en place un pipeline permettant de :

1. récupérer les données depuis **PostgreSQL** ;
2. transformer les données selon un modèle dimensionnel ;
3. charger les dimensions dans **MongoDB** ;
4. construire la table de faits `fait_notes` ;
5. orchestrer automatiquement l'ensemble du processus avec **Apache Airflow** ;
6. exécuter l'environnement avec **Docker**.

### Architecture générale

```text
                 ┌─────────────────────┐
                 │     PostgreSQL      │
                 │    Base scolaire    │
                 └──────────┬──────────┘
                            │
                            │ Extraction
                            ▼
                 ┌─────────────────────┐
                 │   Apache Airflow    │
                 │      ETL / DAG      │
                 └──────────┬──────────┘
                            │
                     Transformation
                            │
                            ▼
                 ┌─────────────────────┐
                 │      MongoDB        │
                 │   Data Warehouse    │
                 └──────────┬──────────┘
                            │
              ┌─────────────┴─────────────┐
              │                           │
              ▼                           ▼
       Dimensions                  Fait de notes
```

---

# 🛠️ Technologies utilisées

| Technologie        | Utilisation                              |
| ------------------ | ---------------------------------------- |
| **PostgreSQL**     | Base de données source                   |
| **Apache Airflow** | Orchestration du pipeline ETL            |
| **MongoDB**        | Data Warehouse cible                     |
| **Docker**         | Conteneurisation                         |
| **Python**         | Développement des tâches ETL             |
| **SQL**            | Extraction et transformation des données |
| **Portainer**      | Administration graphique des conteneurs  |

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
┌─────────────────┐               │               ┌─────────────────┐
│  dim_etudiant   │──────────┐    │    ┌──────────│   dim_matiere   │
└─────────────────┘          │    │    │           └─────────────────┘
                             ▼    ▼    ▼
                        ┌─────────────────┐
                        │   fait_notes    │
                        └─────────────────┘
                             ▲    ▲    ▲
                             │    │    │
┌─────────────────┐          │    │    └──────────┐
│   dim_classe    │──────────┘    │               │
└─────────────────┘               │               │
                                  │               │
                         ┌────────┴────────┐ ┌────┴──────────────────┐
                         │   dim_temps     │ │ dim_type_evaluation  │
                         └─────────────────┘ └───────────────────────┘
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
└── fait_notes
```

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
    └── NOTE + dimensions ─────► fait_notes
```

---

# ⚙️ Fonctionnement de l'ETL

## 1. Extraction

Apache Airflow utilise `PostgresHook` pour se connecter à PostgreSQL.

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

pg_hook = PostgresHook(
    postgres_conn_id="postgres_default"
)
```

Les requêtes SQL permettent d'extraire les données nécessaires.

---

## 2. Transformation

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

Le résultat est ensuite chargé dans MongoDB.

---

## 3. Chargement

Airflow utilise `MongoHook` pour accéder à MongoDB :

```python
from airflow.providers.mongo.hooks.mongo import MongoHook

mongo_hook = MongoHook(
    mongo_conn_id="mongo_default"
)
```

Les documents sont insérés ou mis à jour avec un mécanisme d'**upsert** :

```python
collection.update_one(
    query_filter,
    {"$set": document},
    upsert=True
)
```

Cela permet d'éviter les doublons lors des exécutions successives du DAG.

---

# 📊 Table de faits `fait_notes`

La collection `fait_notes` constitue le cœur du Data Warehouse.

Chaque document représente une note attribuée à un étudiant.

Exemple :

```json
{
    "id_fait": 1,
    "note": 14.5,
    "credit": 5,
    "id_DimTemps_Fk": 1,
    "id_DimEnseignant_Fk": 2,
    "id_DimTypeEvaluation_FK": 1,
    "id_DimEtudiant_FK": "ETU001",
    "id_DimMatiere_FK": 3,
    "id_DimClasse_FK": 2
}
```

### Relations

```text
id_DimEtudiant_FK
        │
        ▼
   dim_etudiant

id_DimEnseignant_Fk
        │
        ▼
   dim_enseignant

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

---

# 🐳 Docker

L'environnement Airflow est exécuté avec Docker.

Les principaux services sont :

```text
airflow-webserver
airflow-scheduler
airflow-init
postgres-meta
```

MongoDB et PostgreSQL sont utilisés comme systèmes de données.

### Vérifier les conteneurs

```bash
docker ps
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
docker logs airflow-airflow-scheduler-1
```

---

# 🌐 Interfaces

### Apache Airflow

```text
http://localhost:8080
```

Airflow permet de :

* visualiser le DAG ;
* lancer manuellement le pipeline ;
* consulter les logs ;
* suivre l'état des tâches ;
* analyser les erreurs d'exécution.

### Portainer

```text
http://localhost:9000
```

Portainer permet de gérer graphiquement les conteneurs Docker.

---

# 🔑 Connexions Airflow

Deux connexions principales sont utilisées.

## PostgreSQL

```text
Connection ID : postgres_default
Host          : host.docker.internal
Port          : 5433
Database      : school
Login         : postgres
```

## MongoDB

```text
Connection ID : mongo_default
Host          : host.docker.internal
Port          : 27017
```

---

# 📁 Structure du projet

```text
Airflow/
│
├── dags/
│   └── dwh_scolaire_postgres_to_mongo.py
│
├── logs/
│
├── plugins/
│
├── docker-compose.yml
│
├── Dockerfile
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
cd Airflow
```

---

## 2. Construire les images Docker

```bash
docker compose build
```

Pour reconstruire complètement l'image :

```bash
docker compose build --no-cache
```

---

## 3. Initialiser Airflow

```bash
docker compose up airflow-init
```

---

## 4. Démarrer Airflow

```bash
docker compose up -d
```

Vérifier :

```bash
docker ps
```

---

# ▶️ Exécution du pipeline

Une fois Airflow démarré :

1. ouvrir `http://localhost:8080` ;
2. se connecter à Airflow ;
3. rechercher le DAG :

```text
dwh_scolaire_postgres_to_mongo
```

4. activer le DAG ;
5. lancer une exécution manuelle ;
6. consulter les tâches.

Les tâches sont exécutées dans cet ordre logique :

```text
load_dim_etudiant
load_dim_enseignant
load_dim_classe
load_dim_matiere
load_dim_temps
load_dim_type_evaluation
              │
              ▼
       load_fait_notes
```

La table de faits est chargée après les dimensions afin que les clés de référence soient disponibles.

---

# 🧪 Vérification des données

Après l'exécution du DAG, MongoDB doit contenir :

```text
dwh_airflow
│
├── dim_etudiant       → données étudiants
├── dim_enseignant     → données enseignants
├── dim_classe         → données classes
├── dim_matiere        → données matières
├── dim_temps          → calendrier des évaluations
├── dim_type_evaluation → devoir / examen
└── fait_notes         → faits scolaires
```

Exemple dans MongoDB :

```javascript
use dwh_airflow

show collections
```

Puis :

```javascript
db.fait_notes.find().pretty()
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

---

# 🎯 Objectifs pédagogiques

Ce projet permet de mettre en pratique :

* la conception d'un **Data Warehouse** ;
* le **modèle dimensionnel** ;
* les dimensions et les faits ;
* les processus **ETL** ;
* l'orchestration avec **Apache Airflow** ;
* les `PostgresHook` et `MongoHook` ;
* les requêtes SQL ;
* la transformation SQL → JSON ;
* le chargement MongoDB ;
* les opérations `upsert` ;
* Docker et Docker Compose ;
* la supervision des pipelines ;
* les requêtes analytiques MongoDB.

---

# 🔮 Améliorations possibles

Plusieurs évolutions peuvent être ajoutées :

* ajout de contrôles de qualité des données ;
* gestion des erreurs et des données invalides ;
* ajout de logs métier ;
* ajout de tests automatisés ;
* création d'index MongoDB ;
* ajout d'un dashboard avec **Power BI**, **Metabase** ou une autre solution de BI ;
* ajout d'une stratégie de chargement incrémental ;
* historisation des dimensions ;
* ajout d'autres indicateurs scolaires ;
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
MongoDB
     +
Docker
```

---

## 📄 Licence

Projet réalisé à des fins pédagogiques et académiques.
