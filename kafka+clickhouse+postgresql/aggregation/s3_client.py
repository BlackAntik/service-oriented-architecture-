"""
S3 export client: reads aggregates from PostgreSQL for a given date
and uploads a CSV file to S3-compatible storage (MinIO).

File path pattern: s3://<bucket>/daily/YYYY-MM-DD/aggregates.csv
Repeated export for the same date overwrites the existing file.
"""

import csv
import io
import logging
import time
from datetime import date
from typing import Any

import boto3
import psycopg2
import psycopg2.extras
from botocore.exceptions import BotoCoreError, ClientError

from config import (
    POSTGRES_DSN,
    S3_ACCESS_KEY,
    S3_BUCKET,
    S3_ENDPOINT_URL,
    S3_SECRET_KEY,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
    )


def ensure_bucket(max_attempts: int = 5) -> None:
    """Create bucket if it does not exist. Retries on connection errors."""
    client = _s3_client()
    for attempt in range(1, max_attempts + 1):
        try:
            existing = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
            if S3_BUCKET not in existing:
                client.create_bucket(Bucket=S3_BUCKET)
                logger.info("S3 bucket '%s' created", S3_BUCKET)
            else:
                logger.info("S3 bucket '%s' already exists", S3_BUCKET)
            return
        except (BotoCoreError, ClientError, Exception) as exc:
            wait = 2 ** (attempt - 1)
            logger.warning(
                "S3 ensure_bucket attempt %d/%d failed: %s, retry in %ds",
                attempt, max_attempts, exc, wait,
            )
            time.sleep(wait)
    raise RuntimeError(f"Cannot connect to S3 after {max_attempts} attempts")


# ---------------------------------------------------------------------------
# PostgreSQL read helpers
# ---------------------------------------------------------------------------

def _pg_connect(max_attempts: int = 5):
    for attempt in range(1, max_attempts + 1):
        try:
            return psycopg2.connect(POSTGRES_DSN)
        except psycopg2.OperationalError as exc:
            wait = 0.5 * (2 ** (attempt - 1))
            logger.warning(
                "PG connect attempt %d/%d failed: %s, retry in %.1fs",
                attempt, max_attempts, exc, wait,
            )
            time.sleep(wait)
    raise RuntimeError(f"Cannot connect to PostgreSQL after {max_attempts} attempts")


def _fetch_metrics(target_date: date) -> list[dict[str, Any]]:
    """Return all rows from `metrics` table for the given date."""
    conn = _pg_connect()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT date, metric_name, value, computed_at "
                "FROM metrics WHERE date = %s ORDER BY metric_name",
                (target_date,),
            )
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def _fetch_top_movies(target_date: date) -> list[dict[str, Any]]:
    """Return all rows from `top_movies` table for the given date."""
    conn = _pg_connect()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT date, movie_id, view_count, rank, computed_at "
                "FROM top_movies WHERE date = %s ORDER BY rank",
                (target_date,),
            )
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CSV builder
# ---------------------------------------------------------------------------

def _build_csv(target_date: date) -> bytes:
    """
    Build a single CSV file with two sections:
      1. scalar metrics (one row per metric_name)
      2. top_movies (one row per movie)
    """
    metrics = _fetch_metrics(target_date)
    top_movies = _fetch_top_movies(target_date)

    buf = io.StringIO()
    writer = csv.writer(buf)

    # --- section: scalar metrics ---
    writer.writerow(["section", "date", "metric_name", "value", "computed_at"])
    for row in metrics:
        writer.writerow([
            "metric",
            str(row["date"]),
            row["metric_name"],
            row["value"],
            row["computed_at"].isoformat() if row["computed_at"] else "",
        ])

    # --- section: top_movies ---
    writer.writerow(["section", "date", "movie_id", "view_count", "rank", "computed_at"])
    for row in top_movies:
        writer.writerow([
            "top_movie",
            str(row["date"]),
            row["movie_id"],
            row["view_count"],
            row["rank"],
            row["computed_at"].isoformat() if row["computed_at"] else "",
        ])

    return buf.getvalue().encode("utf-8")


# ---------------------------------------------------------------------------
# Main export function
# ---------------------------------------------------------------------------

def export_to_s3(target_date: date, max_attempts: int = 3) -> dict[str, Any]:
    """
    Export aggregates for `target_date` from PostgreSQL to S3.
    Overwrites existing file for the same date.
    Returns a dict with export metadata.
    """
    d = target_date.isoformat()
    s3_key = f"daily/{d}/aggregates.csv"

    for attempt in range(1, max_attempts + 1):
        try:
            logger.info("Building CSV for date=%s (attempt %d)", d, attempt)
            csv_bytes = _build_csv(target_date)

            client = _s3_client()
            client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=csv_bytes,
                ContentType="text/csv",
                Metadata={"export-date": d},
            )
            size = len(csv_bytes)
            logger.info(
                "Exported %d bytes to s3://%s/%s", size, S3_BUCKET, s3_key
            )
            return {
                "date": d,
                "s3_uri": f"s3://{S3_BUCKET}/{s3_key}",
                "size_bytes": size,
                "status": "ok",
            }
        except (BotoCoreError, ClientError) as exc:
            wait = 2 ** (attempt - 1)
            logger.error(
                "S3 upload attempt %d/%d failed: %s, retry in %ds",
                attempt, max_attempts, exc, wait,
            )
            if attempt < max_attempts:
                time.sleep(wait)
        except (psycopg2.Error, RuntimeError) as exc:
            wait = 2 ** (attempt - 1)
            logger.error(
                "PG read attempt %d/%d failed: %s, retry in %ds",
                attempt, max_attempts, exc, wait,
            )
            if attempt < max_attempts:
                time.sleep(wait)

    raise RuntimeError(
        f"Failed to export aggregates for {d} after {max_attempts} attempts"
    )
