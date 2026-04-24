CREATE TABLE IF NOT EXISTS default.agg_dau
(
    date        Date,
    dau         UInt64,
    computed_at DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(date)
ORDER BY date;

CREATE TABLE IF NOT EXISTS default.agg_avg_watch_time
(
    date             Date,
    avg_watch_sec    Float64,
    total_views      UInt64,
    computed_at      DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(date)
ORDER BY date;

CREATE TABLE IF NOT EXISTS default.agg_top_movies
(
    date        Date,
    movie_id    String,
    view_count  UInt64,
    rank        UInt32,
    computed_at DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, rank);

CREATE TABLE IF NOT EXISTS default.agg_conversion
(
    date            Date,
    started_count   UInt64,
    finished_count  UInt64,
    conversion_rate Float64,
    computed_at     DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(date)
ORDER BY date;

CREATE TABLE IF NOT EXISTS default.agg_retention
(
    cohort_date Date,
    d1_retained UInt64,
    d7_retained UInt64,
    cohort_size UInt64,
    d1_rate     Float64,
    d7_rate     Float64,
    computed_at DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(cohort_date)
ORDER BY cohort_date;
