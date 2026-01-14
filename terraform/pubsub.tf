# ============================================
# PUB/SUB - Streaming Messaging (thay thế Kafka/Redpanda)
# ============================================

# Topic cho listen events
resource "google_pubsub_topic" "listen_events" {
  name = "listen-events"
  
  message_retention_duration = "86400s"  # 24 hours
  
  labels = {
    environment = var.environment
    event_type  = "listen"
  }
  
  depends_on = [google_project_service.required_apis]
}

# Topic cho page view events
resource "google_pubsub_topic" "page_view_events" {
  name = "page-view-events"
  
  message_retention_duration = "86400s"
  
  labels = {
    environment = var.environment
    event_type  = "page-view"
  }
  
  depends_on = [google_project_service.required_apis]
}

# Topic cho auth events
resource "google_pubsub_topic" "auth_events" {
  name = "auth-events"
  
  message_retention_duration = "86400s"
  
  labels = {
    environment = var.environment
    event_type  = "auth"
  }
  
  depends_on = [google_project_service.required_apis]
}

# Topic cho status change events
resource "google_pubsub_topic" "status_change_events" {
  name = "status-change-events"
  
  message_retention_duration = "86400s"
  
  labels = {
    environment = var.environment
    event_type  = "status-change"
  }
  
  depends_on = [google_project_service.required_apis]
}

# Dead letter topic for failed messages
resource "google_pubsub_topic" "dead_letter" {
  name = "soundflow-dead-letter"
  
  labels = {
    environment = var.environment
    purpose     = "dead-letter"
  }
  
  depends_on = [google_project_service.required_apis]
}

# ============================================
# SUBSCRIPTIONS - For Spark Streaming / Dataflow
# ============================================

resource "google_pubsub_subscription" "listen_events_spark" {
  name  = "listen-events-spark-sub"
  topic = google_pubsub_topic.listen_events.name
  
  ack_deadline_seconds = 60
  
  # Retry policy
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
  
  # Dead letter policy
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter.id
    max_delivery_attempts = 5
  }
  
  # Message retention
  message_retention_duration = "604800s"  # 7 days
  retain_acked_messages      = true
  
  # Expiration policy - never expire
  expiration_policy {
    ttl = ""
  }
  
  labels = {
    environment = var.environment
    consumer    = "spark"
  }
}

resource "google_pubsub_subscription" "page_view_events_spark" {
  name  = "page-view-events-spark-sub"
  topic = google_pubsub_topic.page_view_events.name
  
  ack_deadline_seconds = 60
  
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
  
  message_retention_duration = "604800s"
  
  expiration_policy {
    ttl = ""
  }
  
  labels = {
    environment = var.environment
    consumer    = "spark"
  }
}

resource "google_pubsub_subscription" "auth_events_spark" {
  name  = "auth-events-spark-sub"
  topic = google_pubsub_topic.auth_events.name
  
  ack_deadline_seconds = 60
  
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
  
  message_retention_duration = "604800s"
  
  expiration_policy {
    ttl = ""
  }
  
  labels = {
    environment = var.environment
    consumer    = "spark"
  }
}

resource "google_pubsub_subscription" "status_change_events_spark" {
  name  = "status-change-events-spark-sub"
  topic = google_pubsub_topic.status_change_events.name
  
  ack_deadline_seconds = 60
  
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
  
  message_retention_duration = "604800s"
  
  expiration_policy {
    ttl = ""
  }
  
  labels = {
    environment = var.environment
    consumer    = "spark"
  }
}
