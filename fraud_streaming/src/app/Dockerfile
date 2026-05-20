FROM python:3.11-slim

WORKDIR /streaming

# Keep dependency layer cacheable and reused by all app services.
COPY src/app/model-service-requirements.txt /tmp/model-service-requirements.txt
RUN pip install --no-cache-dir -r /tmp/model-service-requirements.txt requests

# Runtime code and model artifact used by model-service/producer/alerts.
COPY src /streaming/src
COPY models /streaming/models

ENV PYTHONPATH=/streaming/src
