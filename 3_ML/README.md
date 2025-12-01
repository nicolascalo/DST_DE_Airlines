# 🤖 3 — Machine Learning  
Modélisation et prédiction du retard des vols Air France–KLM

## ✈️ Introduction
Cette troisième brique du projet consiste à développer un modèle de Machine Learning capable de prédire la **probabilité de retard** d’un vol Air France–KLM à partir des données opérationnelles (horaire prévu, type d’appareil, aéroport, statut public, etc.).

L’objectif est d’offrir une **aide à la décision** et une **anticipation proactive** pour les équipes opérationnelles et, à terme, pour les passagers.

---

# 🎯 Objectif du modèle
Prédire si un vol sera :

- **en retard**,  
- ou **à l’heure**.

Le modèle repose sur un ensemble de variables consolidées dans PostgreSQL, prétraitées et nettoyées avant l’entraînement.

---

# 🧩 Pipeline Machine Learning

Le pipeline ML se déroule en **5 étapes majeures** :

1. **Chargement & préparation des données**  
2. **Nettoyage & harmonisation**  
3. **Feature engineering**  
4. **Entraînement du modèle RandomForest**  
5. **Évaluation & exportation du modèle**

---

# 📥 1. Chargement & Préparation des données

Le fichier CSV généré contient contient les champs utiles, notamment :

- `flightStatusPublic`  
- `flightLegs_scheduledFlightDuration`  
- `flightLegs_serviceType`  
- `flightLegs_depInfo_times_scheduled_hour`  
- `flightLegs_aircraft_typeCode`  
- `airline_code`  
- `delay_status` (cible à prédire)

Seules les valeurs exploitables sont conservées.

---

# 🧹 2. Nettoyage & Harmonisation

Le script ML applique :

- suppression des valeurs incohérentes (durée négative, statuts impossibles)  
- filtrage des vols sans informations essentielles, 
- normalisation du format des heures & dates 
- transformation des catégories en labels (encoding)  

Un soin particulier a été porté à la variable cible `delay_status`, construite à partir des retards remontés par l’API.

---

# 🏗️ 3. Feature Engineering

Les principales variables construites sont :

### 🕒 **Heure de départ convertie**
Regroupée sur plusieurs plages horaires permettant :
- meilleure compréhension du trafic  
- meilleure séparation pour l’algo

### 🚀 **Durée du vol**
Transformée puis normalisée, car c’est un fort prédicteur du retard

### 🛫 **Type d’appareil**
Encodé pour capturer les différences d’exploitation

### 🌍 **Origine géographique du vol**
Continent / Subcontinent / Country encodés

### 🛑 **Statut public du vol**
Variable très informative pour signaler les premiers signes d’irrégularité

---

# 🌲 4. Modèle utilisé : Random Forest Classifier

Le **RandomForestClassifier** a été retenu pour :

- sa robustesse aux données bruitées  
- sa capacité à gérer des variables catégorielles encodées  
- sa bonne performance en classification binaire  
- son interprétabilité (importance des features)

### Hyperparamètres clés :
- `GridSearchCV`  
- `cv = 5`  
- `scoring = r^2`  

Ces paramètres ont été optimisés après plusieurs essais.

---

# 📊 5. Évaluation des performances [NICOLAS à MODIFIER]

Un split **train/test 80/20** a été appliqué.

Les métriques principales :

| Metric | Score |
|-------|-------|
| Accuracy | ~0.78 |
| Recall (retard) | ~0.81 |
| Precision (retard) | ~0.75 |
| ROC-AUC | ~0.84 |

Ces valeurs indiquent que le modèle capte correctement les signaux de retard tout en limitant les faux positifs.

---

# 🧠 Importance des variables

Le modèle identifie comme variables les plus discriminantes :

1. **flightStatusPublic**  
2. **hour_of_day**  
3. **scheduledFlightDuration**  
4. **airline_code**  
5. **aircraft_typeCode**

Ces résultats correspondent à l’analyse EDA initiale.

---

# 📦 Exportation & intégration dans le projet

Une fois entraîné, le modèle est :

1. Sauvegardé sous forme de fichier pickle (`model.pkl`)  
2. Déployé dans l’API FastAPI du projet  
3. Appelé depuis le Dashboard via une requête POST

Le Dashboard fournit donc une **prédiction en temps réel** en fonction d’un vol sélectionné.

---

# 🚧 Limites observées

- Les données API AFKLM peuvent être hétérogènes selon les jours  
- Certaines causes de retard ne sont pas entièrement explicatives  
- Le modèle classe en retard / pas retard, mais **ne prédit pas la durée du retard**

---

# 🚀 Perspectives d’évolution

## 🟦 1. Prédire une **tranche de retard** (K-Means)

Un essai K-Means a été réalisé :  
→ segmentation possible en 3 ou 4 classes (“faible”, “moyen”, “long retard”).  
Cette approche permettrait d’offrir une information plus opérationnelle.

## 🟧 2. Enrichissement des variables
- Météo externe  
- Congestion aéroportuaire  
- Historique retard compagnie

## 🟥 3. Amélioration du pipeline ML [NICOLAS QU'EN PENSES-TU ? SI INUTILE, TU PEUX VIRER CETTE PARTIE]
- ?
- ?
- ?

---
