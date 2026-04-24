CREATE MATERIALIZED VIEW IF NOT EXISTS default.movie_events_mv
TO default.movie_events
AS
SELECT
    event_id,
    user_id,
    movie_id,
    event_type,
    fromUnixTimestamp64Micro(timestamp, 'UTC') AS event_time,
    device_type,
    session_id,
    progress_seconds
FROM default.movie_events_kafka;
