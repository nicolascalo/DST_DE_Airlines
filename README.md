# ✈️ DST Airlines – Prédiction des retards de vols AF-KLM

Projet de fin de formation DataScientest visant à **prédire les retards de vols du groupe Air France–KLM** à partir des données opérationnelles de vols.

L’objectif est double :

- aider à **anticiper les retards** (information passager, opérations)  
- fournir une **preuve de concept** réutilisable : collecte de données, stockage, modèle de ML, API et tableau de bord interactif

---

## 🎯 Contexte & objectifs

- **Contexte métier**  
  AF-KLM publie des informations opérationnelles (statuts de vols, horaires, retards, causes publiques).  
  Ces données sont consommables via l’API officielle du groupe.

- **Objectif principal**  
  Prédire **si un vol sera en retard ou non** (classification binaire) à partir de caractéristiques du vol  
  (ligne, horaires, appareil, statut, etc.).

- **Contraintes du projet**
  - Infrastructure et outils **100% gratuits** (pas de budget pour des ressources cloud payantes)
  - Collaboration à 5, avec séparation claire des répartition des tâches

---

## 🧱 Architecture globale du dépôt

Le dépôt est organisé autour de **4 grands blocs fonctionnels** :

1. **`1_data_collection/` – Collecte & structuration des données**
   - Scripts d’appel à l’**API AF-KLM** (endpoints vols)
   - Récupération incrémentale sur 2 mois, sans doublons
   - Stockage intermédiaire en JSON / CSV puis dépôt dans la base relationnelle

2. **`2_bdd/` – Modélisation & alimentation PostgreSQL**
   - Schéma relationnel (tables `Continent`, `Country`, `Airport`, `Flight`, `Delay`, etc.)
   - Scripts SQL d’initialisation et de création de la base `airline`
   - Intégration des données issues de la collecte

3. **`3_ML/` – Modèle de Machine Learning**
   - Préparation des features, nettoyage et choix des variables explicatives
   - Modèle principal : **Random Forest** pour prédire la probabilité de retard
   - Exposition du modèle via une **API FastAPI** (service ML)

4. **`4_dashboard/` – Dashboard Dash (interface utilisateur)**
   - Application **Dash (Plotly)** pour interagir avec les vols et lancer des prédictions
   - Sélection d’un vol via une table des futurs vols
   - Visualisation de la prédiction de retard et des métriques du modèle

> 🔁 L’ensemble est orchestré via Docker Compose (base de données, API, dashboard, ordonnancement des flux de données, etc.).

---

## 🔌 Services & URLs (en local, via Docker)

Les ports exacts peuvent être adaptés dans `docker-compose.yml`, mais l’architecture cible est la suivante :

| Service | Rôle | URL locale (par défaut) |
|-----------------------------|----------------------------------------|----------------------------------|
| **Dashboard Dash** | Interface utilisateur principale | `http://localhost:8050` |
| **ML API (FastAPI)** | Prédiction de retard | `http://localhost:8001/docs` |
| **MongoDB** | Base NoSQL MongoDB | `localhost:27017` |
| **MongoDB API** | Exposition des données Mongo | `http://localhost:8000/docs` |
| **PostgreSQL** | Base relationnelle principale | `localhost:5432` |
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

5. **Perspectives d'évolutions**
   - algorithme  K-means
   - système d'authentification pour accèder au tableau de bord
   - intégration du projet à GCP
   - monitoring via Grafana

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

