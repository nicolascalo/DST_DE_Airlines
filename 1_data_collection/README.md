# 📁 1 — Data Collection  
Acquisition et préparation des données issues de l’API Air France–KLM

## ✈️ Introduction
Cette première brique constitue le socle du projet DST Airlines.  
L’objectif est de collecter, structurer et fiabiliser les données provenant de l’API Air France–KLM (AFKLM), afin d’alimenter un pipeline de base de données, machine learning et Dashboard.

Plusieurs sources externes ont été explorées (OpenMeteo, OpenSky, Lufthansa API, FlightRoute, EDI, GLA), mais abandonnées pour manque de stabilité ou d’intérêt métier.  
La collecte repose donc entièrement sur l’API AFKLM, seule source offrant un historique riche, cohérent et exploitable gratuitement.

## 🎯 Objectifs
- Construire un pipeline d’extraction stable et automatisé 
- Gérer les limites de l'API
- Créer des jeux de données bruts  
- Documenter les choix, les abandons et les contraintes rencontrées

## 🔍 Méthodologie de collecte

### 1. Exploration des sources
Un audit initial a identifié plusieurs API potentielles.  
Les abandons ont été motivés par :
- quotas trop limitants  
- documentation insuffisante  
- faible disponibilité de l’historique  
- incohérences dans les données  
Seule AFKLM répondait aux exigences du projet.

### 2. Choix final de l’API Air France–KLM
L’API AFKLM offre :  
- des informations fiables sur les vols  
- des données de statut, historique, routes et appareils  
- une structure homogène et documentée  
- une bonne stabilité, indispensable pour un pipeline

<img width="1309" height="1526" alt="image" src="https://github.com/user-attachments/assets/a205b6ff-d885-4207-a07b-8d759cde63d5" />

### 3. Pipeline d’extraction
Les scripts Python réalisent :    
- la gestion des limites API (plusieurs clés API pour pallier aux limites de la version gratuite)  
- la sérialisation JSON/CSV  
- la création d’un dataset consolidé

### 4. Analyses intermédiaires
- détection des champs manquants  
- analyse de la qualité et fréquence des données  
- mise en évidence des statuts majoritaires  
- agrégation pour SQL et ML

## 📊 Traitements et normalisation
Pour assurer la compatibilité SQL/ML :  
- normalisation des dates au format ISO  
- uniformisation de FlightLegsStatus  
- nettoyage des valeurs incohérentes   
- constitution des jeux de données

Ces jeux de données sont utilisés dans les étapes suivantes du projet.

## ⚠️ Contraintes rencontrées
- limitations API nécessitant une collecte échelonnée  
- champs optionnels variables selon les vols  
- absence d’intégration météo viable  
- hétérogénéité des endpoints selon la nature des vols  

Malgré cela, une base de données complète et exploitable a pu être construite.

## 🧩 Rôle dans le projet global
Les données collectées alimentent :  
- la base PostgreSQL (tables et vues)  
- MongoDB (data dumps)  
- les modèles ML (classification & régression)  
- le dashboard (analyse et prédictions)

La Data Collection est donc une brique indispensable donnant sa cohérence à l’ensemble du pipeline.
