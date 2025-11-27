FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/

# Expose the Dash default port
EXPOSE 8050

# Use gunicorn to serve the app inside Docker
CMD ["gunicorn", "-b", "0.0.0.0:8050", "app:server"]
