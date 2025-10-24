# MongoDB Database

## Docker Container

### Setup

1. Fill in the `docker-compose.yml` file

Required parameters are documented in the `docker-composestruct` file.

2. Navigate to the MongoDb folder:
```bash
cd bdd/MongoDb
```

3. Start the Docker container:
```bash
docker-compose up -d
```

4. Access the container:
```bash
docker exec -it <container_name> bash
```

## Environment Variables

Fill in the `.env` file at the project root.

Required environment variables are documented in the `envstruct` file.

!!! Il faudrait définir le "DATABASE_NAME"pour qu'il soit fixe car il doit correspondre au dump ?   

## Data Loading

1. Activate the virtual environment:
```bash
source venv/bin/activate
```

2. Navigate to the MongoDb folder:
```bash
cd bdd/MongoDb
```

3. Execute the import script:
```bash
python3 -m mongo_db_interaction.main
```

## Data Dump

Export Dump :

```bash
docker exec <container_name> mongodump --username=<username> --password=<password> --db=<data_base_name> --authenticationDatabase=admin --archive | cat > <path_export_dump>/dump-$(date +%Y%m%d-%H-%M-%S).archive
```


Import Dump :
```bash
cat <path_file_toimport> | docker exec -i <docker_container_name> mongorestore --username=<username> --password=<password> --authenticationDatabase=admin --archive
```


## Project Structure
```
bdd/MongoDb/
├── mongo_db_interaction/
│   ├── main.py
│   ├── use_cases/
│   ├── services/
│   ├── repositories/
│   └── db_context/
├── docker-compose.yml
├── .env
└── envstruct
```