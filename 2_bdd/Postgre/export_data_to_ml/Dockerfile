FROM python:3.12-slim

RUN apt-get update && apt-get -y install curl && \
    pip install --upgrade pip

COPY ./requirements.txt ./requirements.txt
COPY ./app.py ./app.py

RUN pip install -r requirements.txt

RUN mkdir -p /data 




CMD ["python3", "app.py"]
