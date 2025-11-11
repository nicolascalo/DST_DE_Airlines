# Documentation API OpenSky Network

## 📚 Ressources principales

**API REST :** https://opensky-network.org/api

**Bibliothèques disponibles :**
- Python : https://openskynetwork.github.io/opensky-api/python.html
- Java : https://openskynetwork.github.io/opensky-api/java.html

---

## 🔑 Authentification et limites

### Crédits quotidiens

| Type d'utilisateur | Crédits par jour |
|-------------------|------------------|
| Utilisateur authentifié standard | **4000 crédits** |
| Contributeur actif (récepteur ADS-B en ligne ≥30% du temps) | **8000 crédits** |

### Coût par requête

Le nombre de crédits consommés dépend de la **taille de la zone géographique** :

| Surface (km²) | Crédits | Description |
|--------------|---------|-------------|
| 0 - 25 (<500×500 km) | **1** | Petite zone (ville) |
| 25 - 100 (<1000×1000 km) | **2** | Région moyenne |
| 100 - 400 (<2000×2000 km) | **3** | Grande région |
| >400 ou monde entier | **4** | Monde entier sans filtrage |

---

## 🛫 Endpoints disponibles

### 1. GET /flights/all

**Description :** Récupère tous les vols sur une période donnée (temps réel uniquement, pas d'historique au-delà de ±2 jours).

**Paramètres :**
- `begin` : Date de début (Unix timestamp)
- `end` : Date de fin (Unix timestamp)

**URL :** 
```
https://opensky-network.org/api/flights/all?begin=<unix_timestamp>&end=<unix_timestamp>
```

**Exemple :**
```
https://opensky-network.org/api/flights/all?begin=1759949912&end=1760040000
```

**Convertisseur de timestamp :** https://www.epochconverter.com/

**Champs récupérés :**
- `icao24` : Identifiant unique de l'avion (ex: 008d3c)
- `callsign` : Code du vol (ex: FSK900)
- `estDepartureAirport` : Aéroport de départ (code OACI, ex: FACT - Cape Town)
- `estArrivalAirport` : Aéroport d'arrivée (code OACI, ex: FAGM - Johannesburg)
- `firstSeen` : Heure de départ (Unix timestamp)
- `lastSeen` : Heure d'arrivée (Unix timestamp)
- Distances horizontale et verticale par rapport aux aéroports

---

### 2. GET /states/all (avec zone géographique)

**Description :** Récupère les avions volant dans une zone rectangulaire définie en temps réel. Idéal pour créer un affichage type FlightAware.

**Paramètres :**
- `lamin` : Latitude minimale
- `lamax` : Latitude maximale
- `lomin` : Longitude minimale
- `lomax` : Longitude maximale

**URL :**
```
https://opensky-network.org/api/states/all?lamin=<lat>&lomin=<lon>&lamax=<lat>&lomax=<lon>
```

**Exemple :**
```
https://opensky-network.org/api/states/all?lamin=42.8389&lomin=-7.9962&lamax=51.8229&lomax=5.5226
```

**Champs récupérés :**
- `icao24` : Identifiant unique de l'avion
- `callsign` : Indicatif d'appel
- `origin_country` : Pays d'origine
- `time_position` : Horodatage de la position
- `last_contact` : Dernier contact
- `longitude` / `latitude` : Coordonnées GPS
- `baro_altitude` : Altitude barométrique
- `on_ground` : Au sol (booléen)
- `velocity` : Vitesse
- `true_track` : Cap vrai
- `vertical_rate` : Vitesse verticale
- `sensors` : Liste des capteurs
- `geo_altitude` : Altitude géométrique
- `squawk` : Code transpondeur
- `spi` : Special Position Identification
- `position_source` : Source de la position

---

### 3. GET /tracks/all

**Description :** Récupère la trajectoire déjà parcourue par un avion. Renvoie une liste de coordonnées.

**Paramètres :**
- `icao24` : Identifiant de l'avion
- `time` : Horodatage (Unix timestamp)

**URL :**
```
https://opensky-network.org/api/tracks/all?icao24=<icao24>&time=<unix_timestamp>
```

**Exemple :**
```
https://opensky-network.org/api/tracks/all?icao24=008d3c&time=1759957869
```

**Exemple de résultat :**
```json
{
  "icao24": "008d3c",
  "callsign": "FSK900",
  "startTime": 1759951644,
  "endTime": 1759963372,
  "path": [
    [1759951644, -33.9658, 18.602, -304, 165, false],
    [1759951675, -33.9864, 18.6084, 0, 165, false],
    ...
  ]
}
```

**Format du path :** `[timestamp, latitude, longitude, altitude, heading, on_ground]`

---

### 4. GET /states/all (par ICAO24)

**Description :** Récupère les informations d'un avion spécifique à partir de son identifiant.

**Paramètres :**
- `icao24` : Identifiant de l'avion
- `time` : Horodatage (Unix timestamp)

**URL :**
```
https://opensky-network.org/api/states/all?time=<unix_timestamp>&icao24=<icao24>
```

**Exemple :**
```
https://opensky-network.org/api/states/all?time=1759971049&icao24=4520c4
```

**Champs récupérés :** Identiques à l'endpoint `/states/all` avec zone géographique.

---

### 5. GET /flights/departure

**Description :** Liste des départs depuis un aéroport sur une tranche horaire (limitée à ±48h autour de maintenant).

**Paramètres :**
- `airport` : Code OACI de l'aéroport (ex: LFMT)
- `begin` : Date de début (Unix timestamp)
- `end` : Date de fin (Unix timestamp)

**URL :**
```
https://opensky-network.org/api/flights/departure?airport=<OACI>&begin=<unix_timestamp>&end=<unix_timestamp>
```

**Exemple :**
```
https://opensky-network.org/api/flights/departure?airport=LFMT&begin=1759885112&end=1759967912
```

**Champs de la réponse :**

| Champ | Type | Description |
|-------|------|-------------|
| `icao24` | string | Identifiant unique ICAO24 du transpondeur |
| `firstSeen` | integer | Timestamp Unix du premier contact (début du vol) |
| `lastSeen` | integer | Timestamp Unix du dernier contact (fin du vol) |
| `estDepartureAirport` | string | Code OACI de l'aéroport de départ estimé |
| `estArrivalAirport` | string/null | Code OACI de l'aéroport d'arrivée estimé |
| `callsign` | string | Indicatif d'appel (8 caractères max) |
| `estDepartureAirportHorizDistance` | integer | Distance horizontale à l'aéroport de départ (mètres) |
| `estDepartureAirportVertDistance` | integer | Distance verticale à l'aéroport de départ (mètres) |
| `estArrivalAirportHorizDistance` | integer/null | Distance horizontale à l'aéroport d'arrivée (mètres) |
| `estArrivalAirportVertDistance` | integer/null | Distance verticale à l'aéroport d'arrivée (mètres) |
| `departureAirportCandidatesCount` | integer | Nombre d'aéroports candidats pour le départ |
| `arrivalAirportCandidatesCount` | integer | Nombre d'aéroports candidats pour l'arrivée |

**Note :** La requête `/flights/arrival` existe mais renvoie actuellement toujours une liste vide. À tester ultérieurement.

---

## 🔐 Authentification avec token

### Création d'un compte

Pour obtenir un `client_id` et une `client_secret`, créer un compte sur OpenSky Network.

### Exemple Python

```python
import requests

CLIENT_ID = "<votre_client_id>"
CLIENT_SECRET = "<votre_client_secret>"

# Obtenir le token
response = requests.post(
    "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token",
    data={
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
)

token = response.json()["access_token"]

# Utiliser le token
headers = {"Authorization": f"Bearer {token}"}
data = requests.get(
    "https://opensky-network.org/api/states/all?time=1759971049&icao24=4520c4",
    headers=headers
)

print(data.json())
```

---

## 📝 Notes importantes

- Les requêtes gratuites ont des limitations
- Certaines requêtes nécessitent un token d'authentification
- Les données en temps réel sont limitées à ±48h autour du moment présent
- Pas d'accès à l'historique au-delà de cette période