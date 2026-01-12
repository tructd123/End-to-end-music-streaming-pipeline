"""
Configuration settings for Spark Streaming application
"""

import os
from dataclasses import dataclass


@dataclass
class KafkaConfig:
    """Kafka/Redpanda configuration"""
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")
    topics: dict = None
    
    def __post_init__(self):
        self.topics = {
            "listen": "listen_events",
            "page_view": "page_view_events", 
            "auth": "auth_events",
            "status_change": "status_change_events"
        }


@dataclass
class GCSConfig:
    """Google Cloud Storage configuration"""
    project_id: str = os.getenv("GCP_PROJECT", "your-project-id")
    bucket_name: str = os.getenv("GCS_BUCKET", "your-bucket-name")
    credentials_path: str = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS", 
        "/opt/spark/credentials/gcp-key.json"
    )
    
    @property
    def raw_path(self) -> str:
        return f"gs://{self.bucket_name}/raw"
    
    @property
    def processed_path(self) -> str:
        return f"gs://{self.bucket_name}/processed"


@dataclass
class SparkConfig:
    """Spark configuration"""
    app_name: str = "MusicStreamingPipeline"
    checkpoint_location: str = os.getenv("CHECKPOINT_LOCATION", "/tmp/checkpoints")
    trigger_interval: str = os.getenv("TRIGGER_INTERVAL", "2 minutes")
    log_level: str = os.getenv("SPARK_LOG_LEVEL", "WARN")


@dataclass
class PipelineConfig:
    """Main pipeline configuration"""
    kafka: KafkaConfig = None
    gcs: GCSConfig = None
    spark: SparkConfig = None
    
    def __post_init__(self):
        self.kafka = KafkaConfig()
        self.gcs = GCSConfig()
        self.spark = SparkConfig()


# Global config instance
config = PipelineConfig()
