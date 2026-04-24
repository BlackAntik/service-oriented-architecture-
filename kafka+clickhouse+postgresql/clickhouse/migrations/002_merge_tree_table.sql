CREATE TABLE IF NOT EXISTS default.movie_events
(
    event_id         String,
    user_id          String,
    movie_id         String,
    event_type       LowCardinality(String),
    event_time       DateTime64(6, 'UTC'),
    device_type      LowCardinality(String),
    session_id       String,
    progress_seconds Nullable(Int32),
    ingested_at      DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (user_id, session_id, event_time, event_id)
TTL toDate(event_time) + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192;
