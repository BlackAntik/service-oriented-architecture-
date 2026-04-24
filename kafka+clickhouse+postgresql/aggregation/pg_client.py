import logging
import time
from datetime import date, datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras

from config import POSTGRES_DSN

logger = logging.getLogger(__name__)


def _connect(max_attempts: int = 5):
    for attempt in range(1, max_attempts + 1):
        try:
            return psycopg2.connect(POSTGRES_DSN)
        except psycopg2.OperationalError as exc:
            wait = 0.5 * (2 ** (attempt - 1))
            logger.warning("PG connect attempt %d failed: %s, retry in %.1fs", attempt, exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"Cannot connect to PostgreSQL after {max_attempts} attempts")


def apply_migrations(migrations_sql: str) -> None:
    conn = _connect()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(migrations_sql)
    finally:
        conn.close()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def upsert_metric(target_date: date, metric_name: str, value: float) -> None:
    sql = """
        INSERT INTO metrics (date, metric_name, value, computed_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (date, metric_name)
        DO UPDATE SET value = EXCLUDED.value, computed_at = EXCLUDED.computed_at
    """
    _execute_with_retry(sql, (target_date, metric_name, value, _now_utc()))


def upsert_top_movies(target_date: date, rows: list[dict[str, Any]]) -> None:
    sql = """
        INSERT INTO top_movies (date, movie_id, view_count, rank, computed_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (date, movie_id)
        DO UPDATE SET view_count = EXCLUDED.view_count,
                      rank = EXCLUDED.rank,
                      computed_at = EXCLUDED.computed_at
    """
    now = _now_utc()
    params_list = [
        (target_date, row["movie_id"], row["view_count"], row["rank"], now)
        for row in rows
    ]
    _executemany_with_retry(sql, params_list)


def _execute_with_retry(sql: str, params: tuple, max_attempts: int = 3) -> None:
    for attempt in range(1, max_attempts + 1):
        try:
            conn = _connect()
            try:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, params)
            finally:
                conn.close()
            return
        except psycopg2.Error as exc:
            wait = 0.5 * (2 ** (attempt - 1))
            logger.warning("PG execute attempt %d failed: %s, retry in %.1fs", attempt, exc, wait)
            time.sleep(wait)
    raise RuntimeError("Failed to execute PG query after retries")


def _executemany_with_retry(sql: str, params_list: list[tuple], max_attempts: int = 3) -> None:
    if not params_list:
        return
    for attempt in range(1, max_attempts + 1):
        try:
            conn = _connect()
            try:
                with conn:
                    with conn.cursor() as cur:
                        psycopg2.extras.execute_batch(cur, sql, params_list)
            finally:
                conn.close()
            return
        except psycopg2.Error as exc:
            wait = 0.5 * (2 ** (attempt - 1))
            logger.warning("PG executemany attempt %d failed: %s, retry in %.1fs", attempt, exc, wait)
            time.sleep(wait)
    raise RuntimeError("Failed to execute PG batch query after retries")
