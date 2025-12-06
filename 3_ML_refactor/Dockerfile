FROM python:3.12-slim

RUN apt-get update && apt-get -y install curl && \
    pip install --upgrade pip

COPY ./requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

RUN mkdir -p /outputs /data /config

COPY ./app /app
COPY ./config /config

WORKDIR /app

EXPOSE 8001

CMD ["uvicorn", "main:api", "--host", "0.0.0.0", "--port", "8001"]
