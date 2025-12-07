FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ml_api.py .

EXPOSE 8000

CMD ["uvicorn", "ml_api:api", "--host", "0.0.0.0", "--port", "8000"]
