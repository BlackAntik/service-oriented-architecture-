import glob
import logging
import os
import time

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123")
MIGRATIONS_DIR = os.getenv("MIGRATIONS_DIR", "/migrations")


def wait_for_clickhouse(url: str, timeout: int = 120) -> None:
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(f"{url}/ping", timeout=5)
            if r.status_code == 200:
                return
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(3)
    raise TimeoutError(f"ClickHouse not available at {url} after {timeout}s")


def execute_sql(url: str, sql: str) -> None:
    r = requests.post(f"{url}/", params={"query": sql}, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"ClickHouse error: {r.status_code} {r.text}")


def apply_migrations(url: str, migrations_dir: str) -> None:
    files = sorted(glob.glob(os.path.join(migrations_dir, "*.sql")))
    for path in files:
        with open(path, "r") as f:
            content = f.read()
        statements = [s.strip() for s in content.split(";") if s.strip()]
        logger.info("Applying migration: %s (%d statements)", os.path.basename(path), len(statements))
        for stmt in statements:
            execute_sql(url, stmt)
        logger.info("Migration applied: %s", os.path.basename(path))


def main() -> None:
    wait_for_clickhouse(CLICKHOUSE_URL)
    apply_migrations(CLICKHOUSE_URL, MIGRATIONS_DIR)
    logger.info("All migrations applied successfully")


if __name__ == "__main__":
    main()
