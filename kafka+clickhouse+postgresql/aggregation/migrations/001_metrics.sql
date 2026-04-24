CREATE TABLE IF NOT EXISTS metrics (
    id          BIGSERIAL PRIMARY KEY,
    date        DATE        NOT NULL,
    metric_name VARCHAR(64) NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (date, metric_name)
);

CREATE TABLE IF NOT EXISTS top_movies (
    id          BIGSERIAL PRIMARY KEY,
    date        DATE        NOT NULL,
    movie_id    VARCHAR(128) NOT NULL,
    view_count  BIGINT      NOT NULL,
    rank        INTEGER     NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (date, movie_id)
);
