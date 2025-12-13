# 📁 2 — Bases de Données
Conception et alimentation des bases PostgreSQL et MongoDB pour le projet DST Airlines

## ✈️ Introduction
Cette seconde brique du projet consiste à transformer les données collectées via l’API Air France–KLM en un schéma relationnel structuré, fiable et exploitable pour le Machine Learning et le Dashboard.  
Elle comporte deux volets complémentaires :

- **MongoDB** : stockage documentaire pour conserver les réponses API brutes (JSON), essentielles pour la traçabilité et l’analyse technique, et pour l'export sous format tabulaire des données jugées pertinentes
- **PostgreSQL** : stockage principal, structuré, normé et optimisé pour les requêtes analytiques

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

Le modèle SQL repose sur les entités clés retournées par l’API AFKLM enrichies par les données géographiques Wikipédia sur les aéoroports internationaux:

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

Les relations suivent la hiérarchie suivante:  
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
Lecture des CSV exportés depuis MongDB et :

- insertion des tables dans l’ordre des dépendances
- application des règles de nettoyage
- vérification des clés étrangères
- mise à jour des statuts de vol (historiques, futurs ou actualisés à J-1)

### 4. Génération des vues métiers

Deux vues SQL sont créées automatiquement afin de faciliter l’analyse et l’alimentation du Machine Learning et du Dashboard :

- Vue des vols passés : regroupe les vols dont la date est antérieure à la date d’exécution, enrichis des informations de retard réelles.
Ces données permettent la préparation des données destinées au modèle ML et l’analyse des performances historiques.

- Vue des vols futurs : liste les vols programmés à venir, combinant horaires prévus, données géographiques et métadonnées opérationnelles.
Ces vues permettent d’éviter les jointures complexes répétitives, tout en structurant la donnée selon les besoins principaux du projet. 

---

# 🗄️ Rôle de MongoDB

PostgreSQL stocke les données **propres** en format tabulaire.  
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

L'initialisation de MongoDB et PostreSQL est entièrement automatisée via Docker Compose  et s'effectue au sein de conteneurs Docker:  

- Le conteneur Docker de collecte des données est lancé
- Les fichiers récupérés sont insérés dans MongoDB, filtrés et exportés sous forme tabulaire consolidée  
- Ces données tabulaires sont insérées dans PostgreSQL et réparties dans les tables appropriées
- Le Dashboard se connecte à PostgreSQL directement  
- Le ML appelle l’API PostgreSQL pour obtenir les données de vols futurs

## Diagramme de l'architecture des dépendances Docker et du flux de données

<img width="946" height="1362" alt="docker_compose_schema drawio" src="https://github.com/user-attachments/assets/03f9fcf7-0b6e-4cf3-a597-6c22499983ca" />


---

# 🧩 Rôle de la BDD dans le projet global

La BDD constitue la **colonne vertébrale** du projet :  
- Le Dashboard requête directement PostgreSQL  
- Le modèle ML s’entraîne à partir des vues SQL
- Les analyses statistiques proviennent des tables consolidées  
- MongoDB sert de sauvegarde brute et d’outil de validation

Sans une base propre, aucune prédiction cohérente n’est possible.

## Diagramme UML de la base de données POstgrSQL

<img width="1555" height="1543" alt="image" src="https://github.com/user-attachments/assets/427121b6-4772-4d9b-b738-f9eb8d49ace2" />

---
