import requests

CLIENT_ID = "<client_id>"
CLIENT_SECRET = "<client_secret>"

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
data = requests.get("https://opensky-network.org/api/flights/arrival?airport=LFMT&begin=1759967912&end=1760054312", headers=headers)
print(data.json())


