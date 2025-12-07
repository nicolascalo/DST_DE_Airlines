# MongoDB Airlines Data Pipeline

## 📋 Description

Cette partie du repo implémente l'infrastructure complète de pipeline de données pour la gestion et l'analyse de données de vols aériennes provenant de l'API Air France KLM. Le système collecte, stocke, transforme et visualise les données de vols en utilisant MongoDB et PostgreSQL avec un déploiement sur Google Cloud Platform (GCP).

## 🏗️ Architecture

### Infrastructure Cloud

Le projet utilise plusieurs services GCP:
- **Cloud Functions**: Pour les traitements automatisés et les ETL
- **Cloud Run**: Pour les APIs et applications web
- **Google Kubernetes Engine (GKE)**: Pour les déploiements de bases de données
- **Cloud Storage**: Pour le stockage intermédiaire des données

### Bases de Données

- **MongoDB**: Base de données principale pour les données de vols (historique et temps réel)
- **PostgreSQL**: Data warehouse pour les analyses et le reporting

## 📁 Structure du Projet

```
mongo_db/
├── gcp_deployments/              # Déploiements GCP
│   ├── gcp_apps/                # Applications Cloud Run
│   │   ├── dash_app/           # Application Dash pour la visualisation
│   │   ├── ml_api/             # API Machine Learning
│   │   └── mongodb_api/        # API FastAPI pour MongoDB
│   ├── gcp_functions/          # Cloud Functions
│   │   ├── extraction_from_air_france_api/      # Extraction API Air France
│   │   ├── extraction_gcs_to_mongodb/           # Import GCS vers MongoDB
│   │   ├── populating_mongdb_future_postgre/    # Population table future
│   │   ├── populating_mongo_futured1_postgre/   # Population table future D+1
│   │   ├── populating_mongodb_past/             # Population table historique
│   │   └── clean_populate_fact_tables_in_postgre/ # Nettoyage et population des tables de faits
│   └── gcp_gke/                # Déploiements Kubernetes sur GKE
│       ├── gcp_gke_mongodb/    # MongoDB sur GKE
│       └── gcp_gke_postgres/   # PostgreSQL sur GKE
│
├── kubernetes_deployments/       # Configurations Kubernetes
│   ├── mongodb-gke/            # Manifests MongoDB
│   └── postgres-gke/           # Manifests PostgreSQL
│
├── MongoDb/                     # Package MongoDB interaction
│   └── mongo_db_interaction/   # Modules pour interagir avec MongoDB
│
├── Workflow_Mongodb_Postgrsql_Package/  # Package ETL MongoDB → PostgreSQL
│
├── dash_api/                    # Code source application Dash
│
└── machine_learning_api/        # Code source API ML
```

## 🚀 Composants Principaux

### Cloud Functions

1. **extraction_from_air_france_api**
   - Extrait les données de vols depuis l'API Air France KLM
   - Stocke les données dans Google Cloud Storage
   - Planification: Exécution périodique

2. **extraction_gcs_to_mongodb**
   - Charge les données depuis GCS vers MongoDB
   - Gère la déduplication et la validation

3. **populating_mongdb_future_postgre**
   - Synchronise les vols futurs (collection `scheduled`) vers PostgreSQL
   - Table cible: `mongodb_future`

4. **populating_mongo_futured1_postgre**
   - Synchronise les vols futurs D+1 (collection `update_scheduled_d1`) vers PostgreSQL
   - Table cible: `mongodb_future_d1`

5. **populating_mongodb_past**
   - Synchronise les vols historiques (collection `historic`) vers PostgreSQL
   - Table cible: `mongodb_past`

6. **clean_populate_fact_tables_in_postgre**
   - Nettoie et agrège les données dans les tables de faits PostgreSQL
   - Applique les transformations business

### Applications Cloud Run

1. **mongodb_api** (dst-de-airlines-api)
   - API FastAPI pour accéder aux données MongoDB
   - Endpoints: export CSV, requêtes personnalisées
   - Documentation: `/docs`

2. **ml_api** (mongo-db)
   - API de Machine Learning pour prédictions

3. **dash_app**
   - Dashboard interactif pour la visualisation des données
   - Connexion PostgreSQL et MongoDB

## 🛠️ Technologies

- **Python 3.12**
- **MongoDB 7.x**
- **PostgreSQL 15**
- **FastAPI**: Framework API REST
- **Dash/Plotly**: Visualisation de données
- **Pandas**: Traitement de données
- **Docker**: Conteneurisation
- **Kubernetes**: Orchestration
- **Google Cloud Platform**: Infrastructure cloud

## 📊 Flux de Données

```
API Air France KLM
    ↓
[Cloud Function: Extraction]
    ↓
Google Cloud Storage (.json)
    ↓
[Cloud Function: GCS → MongoDB]
    ↓
MongoDB (Collections: scheduled, update_scheduled_d1, historic)
    ↓
[Cloud Functions: Population PostgreSQL]
    ↓
PostgreSQL (Tables temporaires)
    ↓
[Cloud Function: Clean & Aggregate]
    ↓
PostgreSQL (Tables de faits dans la bases de données)
    ↓
[Dash App / ML API] → Utilisateurs finaux
```

## 🔐 Sécurité

- Les informations sensibles (credentials, IPs, URIs) sont stockées dans des variables d'environnement
- Les fichiers de déploiement utilisent `[REDACTED]` pour masquer les données sensibles
- Authentification IAM GCP pour gerer l'accès aux ressources
- Service accounts dédiés avec permissions minimales

## 📦 Packages Python Personnalisés

### mongo_db_interaction
Package pour interagir avec MongoDB contenant:
- DAO (Data Access Objects) pour les collections
- Services métier
- Scripts d'import/export
- Utilitaires de connexion

### Workflow_Mongodb_Postgrsql_Package
Package ETL pour synchroniser MongoDB → PostgreSQL: Workflow_Mongodb_Postgrsql_Package


## 🚦 Démarrage

### Prérequis
- Compte GCP avec projet configuré
- gcloud CLI installé et authentifié
- Docker installé (pour développement local)
- Python 3.12

### Configuration
1. Cloner le repository
2. Configurer les variables d'environnement
3. Déployer les ressources GCP selon le besoin

### Déploiement

#### 1. Création des Clusters GKE

**Cluster MongoDB (europe-west10):**
```bash
cd gcp_deployments/gcp_gke/gcp_gke_mongodb
bash cluster_creation.sh
```

Le script crée un cluster GKE Autopilot avec les caractéristiques suivantes:
- Nom: `mongo-cluster`
- Région: `europe-west10`
- Release channel: `regular`
- Private nodes activés
- Configuration réseau par défaut

**Cluster PostgreSQL (europe-west9):**
```bash
cd gcp_deployments/gcp_gke/gcp_gke_postgres
bash cluster_creation.sh
```

Le script crée un cluster GKE Autopilot avec les caractéristiques suivantes:
- Nom: `postgre-cluster`
- Région: `europe-west9`
- Release channel: `stable`
- Master global access activé

#### 2. Connexion aux Clusters

**Se connecter au cluster MongoDB:**
```bash
gcloud container clusters get-credentials mongo-cluster \
  --region europe-west10 \
  --project <your-project-id>
```

**Se connecter au cluster PostgreSQL:**
```bash
gcloud container clusters get-credentials postgre-cluster \
  --region europe-west9 \
  --project <your-project-id>
```

#### 3. Déploiement des Bases de Données sur Kubernetes

**Déployer MongoDB sur GKE:**
```bash
# Basculer vers le contexte du cluster MongoDB
kubectl config use-context gke_<project-id>_europe-west10_mongo-cluster

# Appliquer les manifests Kubernetes
cd kubernetes_deployments/mongodb-gke

# 1. Créer les secrets
kubectl apply -f mongo-secret.yml

# 2. Déployer MongoDB (StatefulSet)
kubectl apply -f mongo-deployment.yml

# 3. Créer le service
kubectl apply -f mongo-service.yml

# Vérifier le déploiement
kubectl get pods
kubectl get svc
```

**Déployer PostgreSQL sur GKE:**
```bash
# Basculer vers le contexte du cluster PostgreSQL
kubectl config use-context gke_<project-id>_europe-west9_postgre-cluster

# Appliquer les manifests Kubernetes
cd kubernetes_deployments/postgres-gke

# 1. Créer les secrets
kubectl apply -f postgres-secret.yaml

# 2. Déployer PostgreSQL (StatefulSet)
kubectl apply -f postgres-deployment.yaml

# 3. Créer le service
kubectl apply -f postgres-service.yaml

# 4. (Optionnel) Déployer PgAdmin
kubectl apply -f pgadmin.yaml

# Vérifier le déploiement
kubectl get pods
kubectl get svc
```

#### 4. Vérification et Accès

**Vérifier les pods MongoDB:**
```bash
kubectl get pods -l app=mongo
kubectl logs <mongo-pod-name>
```

**Vérifier les pods PostgreSQL:**
```bash
kubectl get pods -l app=postgres
kubectl logs <postgres-pod-name>
```

**Obtenir les IPs externes des services:**
```bash
kubectl get svc
```

**Port forwarding pour accès local (développement):**
```bash
# MongoDB
kubectl port-forward svc/mongo-service 27017:27017

# PostgreSQL
kubectl port-forward svc/postgres-service 5432:5432
```

#### 5. Déploiement Cloud Functions

**Cloud Functions:**
```bash
cd gcp_deployments/gcp_functions/<function-name>
gcloud run services replace deployment_file.yaml \
  --project=<your-project> \
  --region=europe-west9
```

#### 6. Déploiement Cloud Run

**Cloud Run Apps:**
```bash
cd gcp_deployments/gcp_apps/<app-name>
gcloud run services replace deployment_file.yaml \
  --project=<your-project> \
  --region=europe-west9
```

## 📈 Monitoring

- Logs centralisés dans Google Cloud Logging
- Métriques et alertes via Google Cloud Monitoring
- Dashboard personnalisés dans l'application Dash

## 👥 Contributeurs

- Nicolas
- Youssef
- Younes
- Johan
- Rathana
