FROM python:3.11-slim

WORKDIR /opt/dagster

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Dagster project
COPY dagster_pipeline/ /opt/dagster/dagster_pipeline/
COPY dagster_home/ /opt/dagster/dagster_home/

# Note: dbt project is mounted via docker-compose volume at /opt/dagster/dbt

# Set environment variables
ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONPATH=/opt/dagster
ENV DBT_TARGET=local
ENV DBT_PROJECT_DIR=/opt/dagster/dbt

# Expose port
EXPOSE 3000

# Run both dagster webserver and daemon with workspace
CMD ["sh", "-c", "dagster-webserver -h 0.0.0.0 -p 3000 -w /opt/dagster/dagster_home/workspace.yaml & dagster-daemon run -w /opt/dagster/dagster_home/workspace.yaml"]
