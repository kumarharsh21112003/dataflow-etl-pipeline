# ============================================
# DataFlow ETL Pipeline - Production Dockerfile
# Multi-stage build for optimized image
# Author: Kumar Harsh
# ============================================

# Stage 1: Builder
FROM python:3.11-slim AS builder

WORKDIR /build

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# Stage 2: Production
FROM python:3.11-slim AS production

LABEL maintainer="Kumar Harsh <Kumarharsh4325@gmail.com>"
LABEL description="DataFlow ETL Pipeline - Real-time weather & air quality data engineering"
LABEL version="1.0"

# Create non-root user for security
RUN groupadd -r dataflow && useradd -r -g dataflow -m dataflow

WORKDIR /app

# Copy installed dependencies from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY etl_pipeline.py .
COPY requirements.txt .

# Create necessary directories
RUN mkdir -p data/raw data/processed logs \
    && chown -R dataflow:dataflow /app

# Switch to non-root user
USER dataflow

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "import requests; requests.get('https://api.open-meteo.com/v1/forecast?latitude=28.6&longitude=77.2&current=temperature_2m', timeout=5)" || exit 1

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV TZ=Asia/Kolkata

# Default command
CMD ["python", "etl_pipeline.py"]
