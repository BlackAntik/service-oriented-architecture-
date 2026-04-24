import glob
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import date, timedelta

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

import aggregator
import pg_client
import s3_client
from config import (
    CLICKHOUSE_URL,
    POSTGRES_DSN,
    S3_EXPORT_SCHEDULE_SEC,
    SCHEDULE_INTERVAL_SEC,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


def _wait_for_clickhouse(timeout: int = 120) -> None:
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(f"{CLICKHOUSE_URL}/ping", timeout=5)
            if r.status_code == 200:
                return
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(3)
    raise TimeoutError("ClickHouse not available")


def _wait_for_postgres(timeout: int = 120) -> None:
    import psycopg2
    start = time.time()
    while time.time() - start < timeout:
        try:
            conn = psycopg2.connect(POSTGRES_DSN)
            conn.close()
            return
        except psycopg2.OperationalError:
            pass
        time.sleep(3)
    raise TimeoutError("PostgreSQL not available")


def _wait_for_s3(timeout: int = 120) -> None:
    """Wait until MinIO is reachable, then ensure bucket exists."""
    import urllib.request
    import urllib.error
    from config import S3_ENDPOINT_URL
    health_url = f"{S3_ENDPOINT_URL}/minio/health/live"
    start = time.time()
    while time.time() - start < timeout:
        try:
            urllib.request.urlopen(health_url, timeout=3)
            break
        except Exception:
            pass
        time.sleep(3)
    # ensure bucket (with retries inside)
    s3_client.ensure_bucket()


def _apply_pg_migrations() -> None:
    migrations_dir = os.getenv("PG_MIGRATIONS_DIR", "/pg_migrations")
    files = sorted(glob.glob(os.path.join(migrations_dir, "*.sql")))
    for path in files:
        with open(path) as f:
            sql = f.read()
        pg_client.apply_migrations(sql)
        logger.info("PG migration applied: %s", os.path.basename(path))


def _scheduled_export() -> None:
    """Export yesterday's aggregates to S3 (runs on schedule)."""
    yesterday = date.today() - timedelta(days=1)
    try:
        result = s3_client.export_to_s3(yesterday)
        logger.info("Scheduled S3 export done: %s", result)
    except Exception as exc:
        logger.error("Scheduled S3 export failed: %s", exc)


_scheduler: BackgroundScheduler | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler
    _wait_for_clickhouse()
    _wait_for_postgres()
    _apply_pg_migrations()
    _wait_for_s3()

    _scheduler = BackgroundScheduler()
    _scheduler.add_job(
        aggregator.run_aggregation_today,
        trigger="interval",
        seconds=SCHEDULE_INTERVAL_SEC,
        id="aggregation_job",
        replace_existing=True,
    )
    _scheduler.add_job(
        _scheduled_export,
        trigger="interval",
        seconds=S3_EXPORT_SCHEDULE_SEC,
        id="s3_export_job",
        replace_existing=True,
    )
    _scheduler.start()
    logger.info(
        "Scheduler started: aggregation interval=%ds, s3_export interval=%ds",
        SCHEDULE_INTERVAL_SEC,
        S3_EXPORT_SCHEDULE_SEC,
    )
    yield
    _scheduler.shutdown(wait=False)


app = FastAPI(title="Aggregation Service", lifespan=lifespan)


class RecalcRequest(BaseModel):
    date: date


class ExportRequest(BaseModel):
    date: date


@app.post("/recalc")
def recalc(req: RecalcRequest):
    try:
        result = aggregator.run_aggregation(req.date)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return result


@app.post("/export")
def export(req: ExportRequest):
    """
    Manually trigger S3 export for a specific date.
    Reads aggregates from PostgreSQL and uploads CSV to
    s3://<bucket>/daily/YYYY-MM-DD/aggregates.csv
    Repeated calls for the same date overwrite the file.
    """
    try:
        result = s3_client.export_to_s3(req.date)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return result


@app.get("/healthz")
def healthz():
    return {"status": "ok"}
