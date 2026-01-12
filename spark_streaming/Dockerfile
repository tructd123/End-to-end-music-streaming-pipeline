FROM apache/spark:3.5.0

USER root

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy application code
COPY src/ /app/src/

# Create directories
RUN mkdir -p /app/checkpoints /app/credentials

WORKDIR /app

# Default command - run PostgreSQL streaming job
CMD ["spark-submit", \
     "--master", "local[*]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0", \
     "/app/src/streaming_to_postgres.py"]
