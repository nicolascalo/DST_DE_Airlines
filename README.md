

Les ports exacts peuvent être adaptés dans `docker-compose.yml`, mais l’architecture cible est la suivante :


| Service | Rôle | URL locale (par défaut) |
|-----------------------------|----------------------------------------|----------------------------------|
| **Dashboard Dash** | Interface utilisateur principale | `http://localhost:8050` |
| **ML API** | Prédiction de retard | `http://localhost:8001` |
| **MongoDB** | Base NoSQL MongoDB | `localhost:27017` |
| **MongoDB API** | Exposition des données Mongo | `http://localhost:8000` |
| **PostgreSQL** | Base relationnelle principale | `localhost:5432` |
| **PostgreSQL API** | Base relationnelle principale | `localhost:8004` |
| **pgAdmin** | UI d’administration PostgreSQL | `http://localhost:5050` |


---


## 🔄 Workflow synthétique


1. **Collecte des données (1_data_collection)**
   - Appels réguliers à l’API AF-KLM
   - Enregistrement des réponses brutes (JSON) puis normalisation
   - Export vers des formats exploitables (CSV)


2. **Intégration BDD (2_database)**
   - Création / mise à jour du schéma PostgreSQL
   - Chargement des données de vols et retards
   - Vérification de la cohérence des données


3. **Entraînement ML (3_ml)**
   - Feature engineering et nettoyage (retards aberrants, valeurs extrêmes)
   - Entraînement d’une **Random Forest** pour prédire si un vol aura un "retard / pas de retard”
   - Sauvegarde du modèle et mise à disposition via FastAPI


4. **Exposition API & Dashboard (3_ml + 4_dashboard)**
   - L’API FastAPI reçoit des caractéristiques de vol et renvoie une prédiction
   - Le Dashboard Dash permet de :
     - sélectionner un vol
     - interroger l’API
     - afficher la prédiction + quelques métriques/modèles


---




##  Perspectives d'évolutions du projet
   - Optimisation du pipeline d'apprentissage machine (algorithme K-means, etc.)
   - Système d'authentification pour accéder au tableau de bord
   - Ordonnancement via Airflow au sein de GCP
   - Pipeline CI/CD


---




## 🚀 Lancer le projet


> ⚠️ Les commandes exactes et prérequis seront détaillés dans les README de chaque dossier (`1_data_collection/`, `2_database/`, `3_ml/`, `4_dashboard/`).  
> Ci-dessous, une vision très simplifiée.


```bash
# 1. Cloner le dépôt
git clone https://github.com/nicolascalo/DST_DE_Airlines.git
cd DST_DE_Airlines


# 2a. Lancer l’environnement Docker complet avec initialisation des données
docker-compose build
docker-compose up


# OU


# 2b. Lancer l’environnement Docker complet sans initialisation des données
docker-compose -f docker-compose-mount.yml build
docker-compose -f docker-compose-mount.yml up


# 3. Accéder au Dashboard
# => http://localhost:8050
# API ML (FastAPI)
```
---


## 👥 Équipe et contributions


### Equipe:


- **Nicolas Calo**
- **Johan Cloos**
- **Rathana Lat**
- **Younes Es-Soualhi**
- **Youssef Znati**


### Contributions:


#### Sélection des sources de données
Johan Cloos, Nicolas Calo, Rathana Lat, Youssef Znati
#### Collecte des données
Nicolas Calo, Rathana Lat
#### MongoDB 
Johan Cloos, Nicolas Calo
#### PostgreSQL 
Rathana Lat, Nicolas Calo
#### Exploration des données 
Youssef Znati, Nicolas Calo, Johan Cloos
#### Apprentissage machine 
Nicolas Calo, Youssef Znati
#### Conteneurisation 
Johan Cloos, Nicolas Calo
#### Ordonnancment (Docker Compose) 
Nicolas Calo, Johan Cloos
#### APIs 
Nicolas Calo, Johan Cloos
#### UI/UX Dashboard 
Youssef Znati, Nicolas Calo, Rathana Lat
#### Déploiement sur le cloud (GCP) 
Younes Es-Soualhi, Johan Cloos, Nicolas Calo
#### Documentation 
Youssef Znati, Nicolas Calo, Rathana Lat, Johan Cloos, Younes Es-Soualhi







