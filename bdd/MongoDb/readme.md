# 🛫 MongoDB - Flight Data Management

This project provides tools to manage a MongoDB database for storing, importing, exporting, and querying flight data.

---

## 📋 Requirements

- **Python3.12**
- **Docker version 28.5.1**
- **Docker Compose 2.39.1** 
- **mongodb-database-tools** (required for the `/dump/all` API route)

### 1. Initial Setup
Fill in the following configuration files:
- `docker-compose.yml`: Required parameters are documented in [`docker-composestruct`](docker-composestruct).
- `.env` (in `MongoDb/dst_de_airlines_api/`): Required parameters are documented in [`envstruct`](envstruct).

- Install the project
```bash
./dst_de_airlines_api/setup.sh
```
### 2. Install `mongodb-database-tools`
To use the API route `http://<api_address>:<port>/dump/all`, install the MongoDB tools:

```bash
# Download
wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian10-x86_64-100.9.4.deb

# Install
sudo dpkg -i mongodb-database-tools-*.deb

# Verify installation
mongodump --version
```

---

## 🚀 Start the API
To start the API server, run:
```bash
./start_api.sh
```

---

## 🏗️ Build Database from Dump

1. **Place your dump** in the `data_dump/` folder:
   - Dump files must be named as: `dump-yyyymmdd-hh-mm-ss.archive`
   - Example: `dump-20251020-06-17-06.archive`
   - *You can place multiple dump files. The most recent one (based on the date in the filename) will be imported.*

2. **Run the import script**:
   ```bash
   ./build_db_with_dump.sh
   ```

---

## 🔄 Import Missing Data from `.gz` Files

1. **Start the Docker container**:
   ```bash
   docker-compose up -d
   ```

2. **Execute the import script**:
   ```bash
   ./insert_missing_data.sh
   ```

---

## 📤 Export Flights to CSV

### Using Script
Run the script and provide the number of flights as a parameter:
```bash
./get_flight_to_csv.sh 
```
*Example:*
```bash
./get_flight_to_csv.sh 
```
And give the nb_flights in parameter

### Using API
Fetch flights directly via the API:
```bash
http://<api_address>:<port>/all/flights/?nb_flights=<nb_flights>
```
*Example:*
```bash
http://localhost:8080/all/flights/?nb_flights=100
```

---

## 🌍 Environment Variables

Fill in the `.env` file at the project root.
Required environment variables are documented in [`envstruct`](envstruct).





## 🗃️ Dump Management

### Export Dump
```bash
docker exec <container_name> mongodump \
  --username=<username> \
  --password=<password> \
  --db=<data_base_name> \
  --authenticationDatabase=admin \
  --archive | cat > <path_export_dump>/dump-\$(date +%Y%m%d-%H-%M-%S).archive
```

### Import Dump
```bash
cat <path_file_to_import> | docker exec -i <docker_container_name> mongorestore \
  --username=<username> \
  --password=<password> \
  --authenticationDatabase=admin \
  --archive
```

---

## 📂 Project Structure

```
bdd/MongoDb/
├── dst_de_airlines_api/
│   ├── API/
│   ├── SERIALIZER/
│   ├── USE_CASES/
│   ├── SERVICES/
│   ├── REPOSITOIRES/
│   └── DB_CONTEXT/
├── data_dump/
├── docker-compose.yml
├── .env
└── envstruct
```

---

## ❓ Need Help?
- Ensure `DATABASE_NAME` in `.env` matches the database name in your dump.
- Refer to `docker-composestruct` and `envstruct` for required parameters.
