import json
import logging
from datetime import date
from typing import Any

import requests

from config import CLICKHOUSE_URL, TOP_MOVIES_LIMIT

logger = logging.getLogger(__name__)


def _query(sql: str) -> list[dict]:
    r = requests.post(
        f"{CLICKHOUSE_URL}/",
        params={"query": sql, "default_format": "JSONEachRow"},
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"ClickHouse error {r.status_code}: {r.text}")
    lines = [line for line in r.text.splitlines() if line.strip()]
    return [json.loads(line) for line in lines]


def _insert(table: str, rows: list[dict]) -> None:
    if not rows:
        return
    lines = "\n".join(json.dumps(row) for row in rows)
    r = requests.post(
        f"{CLICKHOUSE_URL}/",
        params={"query": f"INSERT INTO default.{table} FORMAT JSONEachRow"},
        data=lines.encode(),
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"ClickHouse insert error {r.status_code}: {r.text}")


def compute_dau(target_date: date) -> dict[str, Any]:
    d = target_date.isoformat()
    rows = _query(
        f"SELECT uniq(user_id) AS dau "
        f"FROM default.movie_events "
        f"WHERE toDate(event_time) = '{d}'"
    )
    dau = int(rows[0]["dau"]) if rows else 0
    _insert("agg_dau", [{"date": d, "dau": dau}])
    return {"date": d, "dau": dau}


def compute_avg_watch_time(target_date: date) -> dict[str, Any]:
    d = target_date.isoformat()
    rows = _query(
        f"SELECT avg(progress_seconds) AS avg_watch_sec, "
        f"count() AS total_views "
        f"FROM default.movie_events "
        f"WHERE toDate(event_time) = '{d}' "
        f"AND event_type = 'VIEW_FINISHED' "
        f"AND progress_seconds IS NOT NULL"
    )
    avg_watch = float(rows[0]["avg_watch_sec"]) if rows and rows[0]["avg_watch_sec"] else 0.0
    total = int(rows[0]["total_views"]) if rows else 0
    _insert("agg_avg_watch_time", [{"date": d, "avg_watch_sec": avg_watch, "total_views": total}])
    return {"date": d, "avg_watch_sec": avg_watch, "total_views": total}


def compute_top_movies(target_date: date) -> list[dict[str, Any]]:
    d = target_date.isoformat()
    rows = _query(
        f"SELECT movie_id, uniq(user_id) AS view_count, "
        f"row_number() OVER (ORDER BY uniq(user_id) DESC) AS rank "
        f"FROM default.movie_events "
        f"WHERE toDate(event_time) = '{d}' "
        f"AND event_type = 'VIEW_STARTED' "
        f"GROUP BY movie_id "
        f"ORDER BY view_count DESC "
        f"LIMIT {TOP_MOVIES_LIMIT}"
    )
    result = []
    for i, row in enumerate(rows, start=1):
        result.append({
            "date": d,
            "movie_id": row["movie_id"],
            "view_count": int(row["view_count"]),
            "rank": i,
        })
    _insert("agg_top_movies", result)
    return result


def compute_conversion(target_date: date) -> dict[str, Any]:
    d = target_date.isoformat()
    rows = _query(
        f"SELECT "
        f"countIf(event_type = 'VIEW_STARTED') AS started_count, "
        f"countIf(event_type = 'VIEW_FINISHED') AS finished_count "
        f"FROM default.movie_events "
        f"WHERE toDate(event_time) = '{d}'"
    )
    started = int(rows[0]["started_count"]) if rows else 0
    finished = int(rows[0]["finished_count"]) if rows else 0
    rate = finished / started if started > 0 else 0.0
    _insert("agg_conversion", [{
        "date": d,
        "started_count": started,
        "finished_count": finished,
        "conversion_rate": rate,
    }])
    return {"date": d, "started_count": started, "finished_count": finished, "conversion_rate": rate}


def compute_retention(target_date: date) -> dict[str, Any]:
    d = target_date.isoformat()
    rows = _query(
        f"WITH cohort AS ("
        f"  SELECT user_id, min(toDate(event_time)) AS first_date "
        f"  FROM default.movie_events "
        f"  WHERE event_type = 'VIEW_STARTED' "
        f"  GROUP BY user_id "
        f"  HAVING first_date = '{d}'"
        f"), "
        f"activity AS ("
        f"  SELECT DISTINCT user_id, toDate(event_time) AS active_date "
        f"  FROM default.movie_events "
        f"  WHERE event_type = 'VIEW_STARTED' "
        f") "
        f"SELECT "
        f"  uniq(c.user_id) AS cohort_size, "
        f"  uniqIf(c.user_id, a1.active_date IS NOT NULL) AS d1_retained, "
        f"  uniqIf(c.user_id, a7.active_date IS NOT NULL) AS d7_retained "
        f"FROM cohort c "
        f"LEFT JOIN activity a1 ON c.user_id = a1.user_id "
        f"  AND a1.active_date = toDate('{d}') + INTERVAL 1 DAY "
        f"LEFT JOIN activity a7 ON c.user_id = a7.user_id "
        f"  AND a7.active_date = toDate('{d}') + INTERVAL 7 DAY"
    )
    cohort_size = int(rows[0]["cohort_size"]) if rows else 0
    d1 = int(rows[0]["d1_retained"]) if rows else 0
    d7 = int(rows[0]["d7_retained"]) if rows else 0
    d1_rate = d1 / cohort_size if cohort_size > 0 else 0.0
    d7_rate = d7 / cohort_size if cohort_size > 0 else 0.0
    _insert("agg_retention", [{
        "cohort_date": d,
        "cohort_size": cohort_size,
        "d1_retained": d1,
        "d7_retained": d7,
        "d1_rate": d1_rate,
        "d7_rate": d7_rate,
    }])
    return {
        "cohort_date": d,
        "cohort_size": cohort_size,
        "d1_retained": d1,
        "d7_retained": d7,
        "d1_rate": d1_rate,
        "d7_rate": d7_rate,
    }
