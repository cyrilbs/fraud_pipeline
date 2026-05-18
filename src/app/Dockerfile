# Base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy only requirements first (for better caching)
COPY model-service-requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r model-service-requirements.txt

# Copy application code
COPY ../.. .

# Set PYTHONPATH
ENV PYTHONPATH=/app

# Run the service
CMD ["python", "-u", "model_service_kafka.py"]
