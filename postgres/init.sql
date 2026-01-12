-- SoundFlow Database Initialization Script
-- PostgreSQL 15

-- Create schemas for different data layers
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- ============================================================
-- RAW LAYER - Direct from Spark Streaming
-- ============================================================

-- Listen Events Table
CREATE TABLE IF NOT EXISTS raw.listen_events (
    event_id SERIAL PRIMARY KEY,
    event_timestamp TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    song VARCHAR(255),
    artist VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(2),
    level VARCHAR(10),
    session_id INTEGER,
    user_agent TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Page View Events Table
CREATE TABLE IF NOT EXISTS raw.page_view_events (
    event_id SERIAL PRIMARY KEY,
    event_timestamp TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    page VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(2),
    level VARCHAR(10),
    session_id INTEGER,
    user_agent TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Auth Events Table
CREATE TABLE IF NOT EXISTS raw.auth_events (
    event_id SERIAL PRIMARY KEY,
    event_timestamp TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    auth_action VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(2),
    level VARCHAR(10),
    session_id INTEGER,
    success BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Status Change Events Table
CREATE TABLE IF NOT EXISTS raw.status_change_events (
    event_id SERIAL PRIMARY KEY,
    event_timestamp TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    old_level VARCHAR(10),
    new_level VARCHAR(10),
    city VARCHAR(100),
    state VARCHAR(2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- INDEXES for Query Performance
-- ============================================================

-- Listen Events Indexes
CREATE INDEX idx_listen_events_timestamp ON raw.listen_events(event_timestamp);
CREATE INDEX idx_listen_events_user_id ON raw.listen_events(user_id);
CREATE INDEX idx_listen_events_song_artist ON raw.listen_events(song, artist);
CREATE INDEX idx_listen_events_location ON raw.listen_events(state, city);

-- Page View Events Indexes
CREATE INDEX idx_page_view_timestamp ON raw.page_view_events(event_timestamp);
CREATE INDEX idx_page_view_user_id ON raw.page_view_events(user_id);
CREATE INDEX idx_page_view_page ON raw.page_view_events(page);

-- Auth Events Indexes
CREATE INDEX idx_auth_timestamp ON raw.auth_events(event_timestamp);
CREATE INDEX idx_auth_user_id ON raw.auth_events(user_id);
CREATE INDEX idx_auth_action ON raw.auth_events(auth_action);

-- Status Change Events Indexes
CREATE INDEX idx_status_change_timestamp ON raw.status_change_events(event_timestamp);
CREATE INDEX idx_status_change_user_id ON raw.status_change_events(user_id);

-- ============================================================
-- GRANTS
-- ============================================================

GRANT USAGE ON SCHEMA raw TO soundflow;
GRANT USAGE ON SCHEMA staging TO soundflow;
GRANT USAGE ON SCHEMA marts TO soundflow;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO soundflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO soundflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA marts TO soundflow;

GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw TO soundflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA staging TO soundflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA marts TO soundflow;

-- ============================================================
-- INITIAL DATA & METADATA
-- ============================================================

-- Metadata table to track pipeline runs
CREATE TABLE IF NOT EXISTS raw.pipeline_metadata (
    run_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50),
    records_processed INTEGER,
    error_message TEXT
);

COMMENT ON TABLE raw.listen_events IS 'Raw listen events from music streaming';
COMMENT ON TABLE raw.page_view_events IS 'Raw page view events from web application';
COMMENT ON TABLE raw.auth_events IS 'Raw authentication events';
COMMENT ON TABLE raw.status_change_events IS 'Raw subscription status change events';

-- End of initialization script
