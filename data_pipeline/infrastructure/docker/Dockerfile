FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY data_pipeline/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY data_pipeline/src/ ./src/
COPY data_pipeline/config/ ./config/
COPY .env .

# Create necessary directories
RUN mkdir -p data/raw data/processed logs reports prefect-storage

# Set Python path
ENV PYTHONPATH=/app/src

# Create non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -c "import sys; sys.exit(0)"

# Default command
CMD ["python", "src/orchestration/prefect_flows.py"]
