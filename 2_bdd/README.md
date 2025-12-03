# 📁 2 — Bases de Données
Conception et alimentation des bases PostgreSQL et MongoDB pour le projet DST Airlines

## ✈️ Introduction
Cette seconde brique du projet consiste à transformer les données collectées via l’API Air France–KLM en un schéma relationnel structuré, fiable et exploitable pour le Machine Learning et le Dashboard.  
Elle comporte deux volets complémentaires :

- **PostgreSQL** : stockage principal, structuré, normé et optimisé pour les requêtes analytiques
- **MongoDB** : stockage documentaire pour conserver les réponses API brutes (JSON), essentielles pour la traçabilité et l’analyse technique

Cette double architecture permet à la fois une exploitation métier robuste et une conservation exacte des données sources.

---

# 🎯 Objectifs de la BDD
- Construire un **modèle relationnel cohérent** basé sur les entités AFKLM  
- Charger et mettre à jour les tables depuis les fichiers extraits
- Mettre en place un **process de vérification qualité** (NULL, doublons, incohérences) 
- Fournir une structure stable permettant :
  - l’analyse statistique
  - l’entraînement du modèle ML
  - l’alimentation temps réel du Dashboard

---

# 🧱 Modèle relationnel

Le modèle SQL repose sur les entités clés retournées par l’API AFKLM :

| Table | Contenu |
|-------|----------|
| **Continent** | Nom du continent |
| **Subcontinent** | Nom du sous-continent |
| **Country** | Pays lié au vol |
| **Location** | Ville / zone géographique |
| **Airport** | Informations des aéroports |
| **Flight** | Vols planifiés et numéro de vol |
| **Departure_Airport** | Point de départ des vols |
| **Delay** | Informations de retard (durée, cause) |

Les relations suivent la hiérarchie officielle AFKLM :  
Continent → Subcontinent → Country → Location → Airport → Flight → Delay.

Ce modèle relationnel a été conçu pour :

- éviter les redondances
- maximiser la lisibilité des requêtes
- améliorer les performances lors des jointures
- garantir une intégration en phase avec le pipeline Machine Learning

---

# 🧪 Processus d’ingestion PostgreSQL

### 1. Normalisation des données
Avant insertion :
- harmonisation du format date/heure
- mise en minuscules des noms de colonnes
- nettoyage des codes vols & aéroports
- concordance des champs avec ceux du JSON

### 2. Création automatique des tables
L'ensemble du schéma relationnel (tables, types, contraintes et clés étrangères) est créé automatiquement au démarrage du conteneur PostgreSQL grâce aux scripts SQL d'initialisation.
Ces scripts définissent la structure complète de la base ainsi qu'une fonctionnalité cohérente.

### 3. Insertion des données
Lecture des CSV issus de la Data Collection et :

- insertion des tables dans l’ordre des dépendances
- application des règles de nettoyage
- vérification des clés étrangères
- mise à jour des statuts de vol

### 4. Génération des vues métiers

Deux vues SQL sont créées automatiquement afin de faciliter l’analyse et l’alimentation du Machine Learning et du Dashboard :

- Vue des vols passés : regroupe les vols dont la date est antérieure à la date d’exécution, enrichis des informations de retard réelles.
Ces données permettent la préparation des données destinées au modèle ML et l’analyse des performances historiques.

- Vue des vols futurs : liste les vols programmés à venir, combinant horaires prévus, données géographiques et métadonnées opérationnelles.
Ces vues permettent d’éviter les jointures complexes répétitives, tout en structurant la donnée selon les besoins principaux du projet. 

---

# 🗄️ Rôle de MongoDB

PostgreSQL stocke les données **propres**.  
MongoDB stocke les données **brutes**, **exactement telles que retournées par l’API**.

Avantages :
- traçabilité complète des réponses API
- débug plus simple
- audit en cas de changement de structure AFKLM

---

# 🔍 Contrôles qualité intégrés

Un ensemble de scripts vérifie la qualité :

- doublons sur `flight_id`  
- incohérences entre aeroport_code et location  
- statuts de vols manquants    
- aéroports non référencés  

---

# ⚙️ Fonctionnement avec Docker

La BDD est entièrement automatisée via Docker Compose :  
- PostgreSQL s’initialise  
- Les scripts d’insertion tournent automatiquement  
- MongoDB charge la structure documentaire  
- Le Dashboard se connecte à PostgreSQL directement  
- Le ML appelle l’API PostgreSQL

Aucune installation locale n’est requise.

---

# 🧩 Rôle de la BDD dans le projet global

La BDD constitue la **colonne vertébrale** du projet :  
- Le Dashboard requête directement PostgreSQL  
- Le modèle ML s’entraîne à partir des vues SQL  
- Les analyses statistiques proviennent des tables consolidées  
- MongoDB sert de sauvegarde brute et d’outil de validation

Sans une base propre, aucune prédiction cohérente n’est possible.

<img width="4746" height="3291" alt="image" src="https://github.com/user-attachments/assets/d07618c1-63fb-48a2-ab8d-582eb7ac3a83" />


---
