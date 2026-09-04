FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config /app/config
COPY services/common /app/services/common
COPY services/metrics_service /app/services/metrics_service

ENV PYTHONPATH=/app

EXPOSE 8000

CMD ["python", "-m", "services.metrics_service.main"]
