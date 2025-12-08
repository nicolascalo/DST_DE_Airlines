FROM python:3.12



WORKDIR /app

COPY requirements.txt .


RUN apt-get update && \
    apt-get install -y \
    docker.io && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


RUN pip install --no-cache-dir -r requirements.txt


COPY . .

EXPOSE 8004



CMD ["uvicorn", "API.api:app", "--host", "0.0.0.0", "--port", "8004"]
