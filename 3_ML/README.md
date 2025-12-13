# :robot: 3 — Apprentissage machine

Entraînement d'alogrithmes d'apprentissage machine avec SciKit-Learn afin de prédire les retards des vols d'Air France KLM pour le projet DST Airlines.

## :dart: Objectif

Depuis les données obtenues par l’API Air France - KLM, prédire l’occurrence de retard/annulation et la durée du retard le cas échéant sur les vols futurs en se basant sur les vols historiques 

## :package: Jeu de données

Toutes les données obtenues par le biais de l’API Air France KLM au cours du projet.

## :mag_right: Sélection des données

Vols depuis/vers un des aéroports du Top 30 européen en termes de nombre de vols journaliers.

~ 550’000 vols conservés

## :gear: Feature engineering

### :airplane: Durée de vol 
- HoraireArrivée - HoraireDépart

### :alarm_clock: Catégorie de vol 


| Condition | Status |
|-------|----------|
| 0 < RetardTotal < 360 | LATE :alarm_clock:|
| Statut du vol = 'CANCELLED'<br>OU<br> RetardTotal > 360| CANCELLED :x:|
| Sinon | ONTIME :+1:|


### :snowman_with_snow::seedling::beach_umbrella::fallen_leaf: Saisonnalité 

| Mois | Saison |
|-------|----------|
| décembre - février | Hiver :snowman_with_snow:|
| mars - mai | Printemps :seedling:|
| juin - août | Été :beach_umbrella:|
| septembre - novembre | Automne :fallen_leaf:|

### :night_with_stars::city_sunrise::cityscape::city_sunset: Période journalière (pour arrivée/destination séparément) 

| Plage temporelle | Période |
|-------|----------|
| 00:00:00 - 05:59:59 | Nuit :night_with_stars:|
| 00:60:00 - 11:59:59 | Matin :city_sunrise:|
| 00:12:00 - 17:59:59 | Après-midi :cityscape:|
| 00:18:00 - 23:59:59 | Soir :city_sunset:|


### :calendar: Période de la semaine 
 - Jour travaillé (lundi-vendredi)
 - Weekend (samedi-dimanche)

## :magic_wand: Feature preprocessing
### :1234: Données numériques
- Pipeline([('imputer', SimpleImputer(strategy='median')),('scaler', StandardScaler())])
### :card_file_box: Données catégoricielles
- Pipeline([('imputer', SimpleImputer(strategy='most_frequent'))
### :hotsprings: One-hot encoding
- Pipeline([('onehot', OneHotEncoder(handle_unknown='ignore', drop='first', sparse_output=True))])

## :weight_lifting: Entraînement des algorithmes d'apprentissage machine 

Le problème de la prédiciton du retard des vols a été décomposé en deux problèmes de classification, un pour la catégorie de vol (LATE / CANCELLED / ONTIME) et un pour la plage de retard (exemple: 0-5 minutes), et un problème de régression destiné à prédire la valeur absolue du retard en minutes. Pour les problèmes de prédiction de la durée du retard, seuls les vols en retard on été conservés dans le jeu de données.

Afin d'identifier les meilleurs algorithmes pour ces 3 types de problèmes, nous avon utilisé un pipeline SciKit-Learn avec validation croisée en 5 passes et hyperparameter tuning (détails disponibles dans le dossier "output").


### :grey_question: Valeurs explicatives 
- Type d’avion
- Saison
- Aéroport de départ
- Aéroport d’arrivée
- Période de départ
- Période d’arrivée
- Weekend
- Durée du vol

### :receipt::fast_forward::arrow_forward::arrow_forward::arrow_right::bar_chart: Résumé des pipelines 

| Prédiction voulue | Catégorie du vol | Durée du retard (plages) | Durée du retard (absolue) |
|-------|----------|----------|----------|
| Type de problème | Classification | Classification |Régression |
| Valeurs cibles |  ONTIME<br>LATE<br>CANCELLED |0-5 min<br>5-15 min<br>15-30 min…|0 - 360 min|
| Algorithmes testés | Logistic_OVO (One vs One)<br>Logistic_OVR (One vs Rest)<br>DecisionTree<br>RandomForest| Logistic_OVO (One vs One)<br>Logistic_OVR (One vs Rest)<br>DecisionTree<br>RandomForest |LinearRegression<br>XGBRegressor <br>DecisionTreeRegressor<br>RandomForestRegressor |
| Jeu de données entraînement | ~ 450’000 (80%) vols  | ~ 170’000 (80%) vols  | ~ 170’000 (80%) vols  |
| Jeu de données test | ~ 100’000 (20%) vols | ~ 40’000 (20%) vols | ~ 40’000 (20%) vols |
| Meilleur algorithme | DecisionTree<br>Précision : 0.874 | Logistic_OVR<br>Précision : 0.335 |XGBRegressor<br>r2: 0.068 |
| Performance du modèle | Plutôt bon | Mauvais | Très mauvais |

### :bangbang: Conclusion

Les algorithmes identifiés comme les meilleurs sur notre jeu de données en utilisant ces variables explicatives permettent de prédire assez fiablement (87% de précision) si un vol sera en retard, mais ne sont pas capables d’estimer la durée de ce retard (34% de précision au mieux en classification).

### :microscope: Piste d’amélioration pour la prédiction de la durée du retard
- Ajout/suppression de variables explicatives
- Meilleure gestion des valeurs extrêmes ?
- Transformation des de la variable cible (si l’exploration de données révèle des relations non-linéaires avec les variables explicatives)
- Réglage des hyper-paramètres plus poussé


### :film_projector: Exemple de matrices de confusion



#### :card_file_box: Catégorie de vol - RandomForest

<img width="842" height="736" alt="image" src="https://github.com/user-attachments/assets/09d0dd54-958f-4203-a901-2773e19406a9" />

#### :hourglass_flowing_sand: Durée du retard (plage) -  Logistic_OVR (One vs Rest)

<img width="798" height="743" alt="image" src="https://github.com/user-attachments/assets/621c0d7e-b9e6-4af4-8871-917ccc97d387" />
