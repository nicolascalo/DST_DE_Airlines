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