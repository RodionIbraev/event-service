CREATE DATABASE IF NOT EXISTS event_service;

CREATE TABLE IF NOT EXISTS event_service.events
(
    event_id UUID,
    event_type LowCardinality(String),
    source LowCardinality(String),
    event_time DateTime,
    payload String,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_type, event_time, event_id);