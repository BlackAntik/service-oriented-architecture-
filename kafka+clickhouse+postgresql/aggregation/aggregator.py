import logging
import time
from datetime import date, timedelta

import ch_client
import pg_client

logger = logging.getLogger(__name__)


def run_aggregation(target_date: date) -> dict:
    logger.info("Aggregation cycle started for date=%s", target_date)
    start = time.monotonic()
    results = {}

    dau = ch_client.compute_dau(target_date)
    pg_client.upsert_metric(target_date, "dau", float(dau["dau"]))
    results["dau"] = dau
    logger.info("DAU computed: %s", dau)

    avg_watch = ch_client.compute_avg_watch_time(target_date)
    pg_client.upsert_metric(target_date, "avg_watch_sec", avg_watch["avg_watch_sec"])
    pg_client.upsert_metric(target_date, "total_finished_views", float(avg_watch["total_views"]))
    results["avg_watch"] = avg_watch
    logger.info("Avg watch time computed: %s", avg_watch)

    top_movies = ch_client.compute_top_movies(target_date)
    pg_client.upsert_top_movies(target_date, top_movies)
    results["top_movies"] = top_movies
    logger.info("Top movies computed: %d entries", len(top_movies))

    conversion = ch_client.compute_conversion(target_date)
    pg_client.upsert_metric(target_date, "conversion_rate", conversion["conversion_rate"])
    pg_client.upsert_metric(target_date, "view_started_count", float(conversion["started_count"]))
    pg_client.upsert_metric(target_date, "view_finished_count", float(conversion["finished_count"]))
    results["conversion"] = conversion
    logger.info("Conversion computed: %s", conversion)

    retention = ch_client.compute_retention(target_date)
    pg_client.upsert_metric(target_date, "retention_d1_rate", retention["d1_rate"])
    pg_client.upsert_metric(target_date, "retention_d7_rate", retention["d7_rate"])
    pg_client.upsert_metric(target_date, "cohort_size", float(retention["cohort_size"]))
    results["retention"] = retention
    logger.info("Retention computed: %s", retention)

    elapsed = time.monotonic() - start
    total_records = (
        dau["dau"]
        + avg_watch["total_views"]
        + len(top_movies)
        + conversion["started_count"]
        + retention["cohort_size"]
    )
    logger.info(
        "Aggregation cycle completed for date=%s: processed %d records in %.2fs",
        target_date,
        total_records,
        elapsed,
    )
    results["elapsed_sec"] = round(elapsed, 3)
    results["total_records"] = total_records
    return results


def run_aggregation_yesterday() -> dict:
    yesterday = date.today() - timedelta(days=1)
    return run_aggregation(yesterday)


def run_aggregation_today() -> dict:
    return run_aggregation(date.today())
