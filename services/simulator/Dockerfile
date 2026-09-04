FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config /app/config
COPY services/common /app/services/common
COPY services/simulator /app/services/simulator

ENV PYTHONPATH=/app

EXPOSE 8001

CMD ["python", "-m", "services.simulator.main"]
