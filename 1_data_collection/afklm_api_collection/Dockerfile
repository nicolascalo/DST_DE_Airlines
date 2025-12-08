FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
COPY afklm_api_data_collection.py afklm_api_data_collection.py



CMD ["python3", "afklm_api_data_collection.py"]
