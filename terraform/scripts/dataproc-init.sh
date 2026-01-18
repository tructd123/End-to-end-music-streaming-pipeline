#!/bin/bash
set -e

echo "=== SoundFlow Dataproc Initialization ==="

# Install Python packages for Spark streaming
pip install --upgrade pip
pip install \
    google-cloud-pubsub \
    google-cloud-storage \
    google-cloud-bigquery \
    pyarrow \
    pandas

# Install Spark Pub/Sub connector
SPARK_PUBSUB_VERSION="0.21.0"
gsutil cp gs://spark-lib/pubsub/pubsub-spark-connector_2.12-$SPARK_PUBSUB_VERSION.jar /usr/lib/spark/jars/ || echo "Pub/Sub connector not found, skipping..."

# Create directories
mkdir -p /opt/soundflow/logs
mkdir -p /opt/soundflow/checkpoints

echo "=== Initialization Complete ==="
