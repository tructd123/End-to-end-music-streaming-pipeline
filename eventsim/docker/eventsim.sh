#! /bin/bash

# Default Kafka/Redpanda broker list (can be overridden by environment variable)
KAFKA_BROKER_LIST=${KAFKA_BROKER_LIST:-"redpanda:9092"}

# Build kafka arguments if broker list is set
KAFKA_ARGS=""
if [ -n "$KAFKA_BROKER_LIST" ]; then
    KAFKA_ARGS="--kafkaBrokerList $KAFKA_BROKER_LIST"
fi

java -XX:+AggressiveOpts -XX:+UseG1GC -XX:+UseStringDeduplication -Xmx8G \
    -jar eventsim-assembly-2.0.jar \
    $KAFKA_ARGS $*
