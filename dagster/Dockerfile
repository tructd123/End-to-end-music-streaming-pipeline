FROM python:3.11-slim

WORKDIR /opt/dagster

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Dagster project
COPY dagster_pipeline/ /opt/dagster/dagster_pipeline/
COPY dagster_home/ /opt/dagster/dagster_home/

# Set environment variables
ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONPATH=/opt/dagster

# Create credentials directory
RUN mkdir -p /opt/dagster/credentials

EXPOSE 3000

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]
