FROM python:3.12

RUN apt-get update && \
    apt-get install -y wget && \
    wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian12-x86_64-100.9.4.deb && \
    apt install -y ./mongodb-database-tools-debian12-x86_64-100.9.4.deb && \
    rm mongodb-database-tools-debian12-x86_64-100.9.4.deb && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

RUN chmod +x start_api.sh

CMD ["uvicorn", "mongo_db_interaction.api:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
