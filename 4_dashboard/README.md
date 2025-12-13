# 🧭 4 — Dashboard

Ce module constitue l’interface utilisateur du projet DST Airlines. Il permet de visualiser les données consolidées, consulter les métriques des modèles de Machine Learning et exécuter des prédictions en temps réel via un formulaire interactif. Le Dashboard repose sur Dash (Plotly) et interagit directement avec :
- l’API PostgreSQL (vues métier et données tabulaires)
- l’API Machine Learning (performances + prédictions)
- l’API MongoDB (historique complémentaire)

---

## 🎯 Objectifs du Dashboard

- Centraliser la visualisation des données et KPI du projet
- Permettre l’exploration intuitive des données traitées
- Afficher les performances des modèles ML
- Fournir une interface simple pour réaliser des prédictions de retard

---

## 🏗️ Architecture du Dashboard

Structure générale du dossier :

4_dashboard/
- app.py : script principal Dash  
- Dockerfile : image Docker du Dashboard  
- docker-compose.yml : configuration pour exécution locale  
- requirements.txt : dépendances Python  
- assets/style.css : feuille de style 

---

## 📌 Fonctionnalités principales

### 1️⃣ Visualisation des données consolidées
Le Dashboard permet d’explorer les données de vols importées et nettoyées dans PostgreSQL : compagnies, horaires, statuts, retards, pays, et autres variables explicatives utilisées dans le ML.

### 2️⃣ Consultation des métriques Machine Learning
Le dashboard charge automatiquement les résultats fournis par l’API ML.  
Il présente les performances essentielles de chaque modèle afin d’offrir une lecture simple et accessible à tous.

Les informations affichées incluent :

- **Nom du modèle** (DecisionTree, Logistic Regression, XGBRegressor)
- **Type de tâche** (classification du statut ou du retard)
- **Indicateurs clés** :
  - **Accuracy pour les problèmes de classification**
  - **r2 pour les problèmes de régression**
- **Taille des jeux de données** utilisés lors de l’entraînement et du test

L’ensemble est présenté de façon synthétique pour permettre une compréhension rapide, même sans expertise en data science.

### 3️⃣ Formulaire de prédiction
Un module interactif permet de saisir les paramètres d’un vol pour obtenir :
- une catégorie de vol (ON TIME / LATE / CANCELLED)
- une estimation du retard de vol le cas échéant

L’utilisateur sélectionne les paramètres, puis déclenche la prédiction via l’API ML.

---

## 🖼️ Tableau de bord
<img width="2148" height="2070" alt="image" src="https://github.com/user-attachments/assets/d4a74474-0359-40b2-bbef-537dfda0e2cf" />


---

## 🌐 Accéder à l’interface

URL locale après lancement du Dashboard :  
http://127.0.0.1:8050
