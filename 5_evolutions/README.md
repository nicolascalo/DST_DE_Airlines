# ✈️ 5 — Évolutions  
*Projet AF-KLM — Prédiction des Retards de Vols*

Ce document présente les axes d’amélioration identifiés pour faire évoluer le projet dans ses prochaines phases de développement.  
Les pistes listées ci-dessous sont techniquement réalistes, alignées avec l’architecture existante, et visent à renforcer la **précision**, la **robustesse** et l’**expérience utilisateur** de la plateforme.

---

## 1. 🔮 Prédiction de tranches de retard (approche K-Means)

Actuellement, le modèle de Machine Learning permet de prédire deux états :  
- **On Time** (vol à l’heure)  
- **Late** (vol en retard)

Cependant, cette approche binaire ne permet pas d’estimer *l’ampleur* du retard.  
Une évolution naturelle serait d’intégrer un algorithme **K-Means** pour regrouper les vols en *tranches de retard*, par exemple :

- retard faible
- retard moyen
- retard élevé 
- (retard exceptionnel)

<img width="3000" height="600" alt="heatmap_clusters" src="https://github.com/user-attachments/assets/1cd62fed-4bc1-4817-84d7-ef97bf5e8dca" />

Cette méthode présente plusieurs avantages :

- Transformation d’un problème complexe en catégories opérationnelles directement exploitables 
- Aide à la prise de décision pour les centres de contrôle, agents de piste et équipes aéroportuaires  
- Peut être utilisée en complément du modèle actuel sans remplacer la pipeline existante

Un prototype K-Means a été développé mais non déployé, faute de temps et de validation.  
Il représente l’évolution la plus immédiate et la plus utile pour affiner les prédictions.

<img width="3600" height="2100" alt="bubble_clusters_top30" src="https://github.com/user-attachments/assets/c8deb44a-38cb-48e6-8bfe-16d39982330a" />

---

## 2. 🔐 Mise en place d’un système d’authentification

Le tableau de bord est pour l’instant accessible librement.  
Pour une utilisation en contexte réel, un système d’authentification serait indispensable :

### Objectifs
- Restreindre l’accès aux équipes internes (Data, OPS, IT, Aéroports)
- Gérer différents niveaux de permissions (consultation / analyses / paramètres)
- Sécuriser les appels API (retards, modèles)

### Pistes techniques
- **OAuth2** (solution standard pour applications modernes)  
- **JWT tokens** (léger, efficace, compatible avec FastAPI + Dash)  

Cette mesure renforcerait la sécurité globale du projet et permettrait son déploiement en environnement de production.

---

## 3. 📊 Monitoring & Alerting (Grafana)

Le projet gagnerait en maturité avec un système de surveillance comprenant :

### Monitoring proposé
- **Suivi de la fraîcheur des données** (nouveaux vols collectés, anomalies)  
- **Temps de réponse des API** (API ML, API Data) 
- **État de santé du modèle** (taux d’erreur, dérive, distribution des features)  
- **Logs opérationnels** (échecs, timeout, volumes de données ingérées)

### Alertes pertinentes
- Absence de nouvelles données collectées depuis X minutes  
- Dégradation du modèle (augmentation du taux d’erreur)  
- Temps de réponse API trop élevé
- Crash d’un conteneur Docker critique

L’intégration de **Grafana** et **Prometheus** serait idéale pour mettre en place ces mécanismes.

---

## 4. ☁️ Intégration complète sur GCP [YOUNES QU'EN PENSES-TU ? HESITES PAS à MODIFIER]

Une migration partielle vers GCP avait été amorcée (Compute Engine, stockage, etc.).  
Pour aller plus loin, plusieurs améliorations sont envisageables :

- Déploiement complet des **API FastAPI** sur Cloud Run ou App Engine  
- Orchestration des pipelines avec **Cloud Composer / Airflow**  
- Stockage des données brutes et intermédiaires dans **Cloud Storage**  
- Déploiement du modèle dans **Vertex AI** (plusieurs bénéfices : versionning, MLOps, monitoring intégré)

Cette évolution permettrait un passage du POC (proof of concept) à une architecture scalable et industrielle.

---

## 5. 🎨 Améliorations de l’interface utilisateur (UI/UX)

Même si le tableau de bord est fonctionnel, plusieurs évolutions pourraient rendre l’expérience utilisateur plus fluide, moderne et intuitive :

### ✔️ Un design aviation-friendly cohérent  
Utilisation des couleurs inspirées d’**Air France–KLM** :  
- bleu profond (#001E3C)  
- bleu clair (#0A75C2)  
- blanc pur (#FFFFFF)  
- rouge AF en accents (#FF3B30)

### ✔️ Un volet de navigation amélioré  
- Sidebar totalement pliable / dépliable  
- Icônes (par exemple un avion pour l’ouverture/fermeture)  
- Meilleure lisibilité en thème sombre ou clair

### ✔️ Formulaire de prédiction plus ergonomique  
- Sélections guidées étape par étape  
- Messages d’erreur plus explicites  
- Highlight automatique du vol sélectionné

### ✔️ Intégration future d’onglets API  
Pour consulter directement depuis le Dash :  
- l'état des API internes  
- les logs de requêtes  
- les routes documentées (type Swagger)

### ✔️ Charts plus riches  
- Visualisation des retards historiques  
- Tendance par type d’avion  
- Heatmap des retards par aéroport

Ces améliorations visent à rendre le dashboard plus immersif et agréable, tout en respectant un design moderne lié au secteur aérien.

---

## 6. 🚀 Autres pistes d’évolution possibles

-  [METTEZ VOS IDEES]
-   
- 
- 

---

## 🧭 Conclusion

Les fondations du projet AF-KLM Delay Prediction sont solides :  
Collecte de données réelles ➡️ Base PostgreSQL structurée ➡️ API opérationnelle ➡️ Modèle ML fonctionnel ➡️ Dashboard interactif

Les évolutions proposées ci-dessus permettraient d’amener ce POC vers :  
- une meilleure précision opérationnelle,  
- une expérience utilisateur plus fluide,  
- une sécurité renforcée,  
- une architecture plus robuste et industrialisable.

Ce README sert de feuille de route pour une future version du projet.
