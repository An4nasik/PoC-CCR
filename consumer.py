import logging
import os
import time

import requests
from config import load_env

load_env()
INDEX = os.getenv("CCR_INDEX", "rag_data")
URLS = [
    item.strip().rstrip("/")
    for item in os.getenv(
        "OPENSEARCH_URLS",
        os.getenv("OPENSEARCH_URL", "http://localhost:9200,http://localhost:9201"),
    ).split(",")
    if item.strip()
]
POLL_INTERVAL = float(os.getenv("CONSUMER_POLL_INTERVAL", "2"))
QUERY_SIZE = int(os.getenv("CONSUMER_QUERY_SIZE", "5"))
LAST_TIMESTAMP = ""

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def endpoint_order(counter: int) -> list[str]:
    if not URLS:
        return []
    start = counter % len(URLS)
    return URLS[start:] + URLS[:start]


def build_query() -> dict:
    if LAST_TIMESTAMP:
        return {
            "size": QUERY_SIZE,
            "sort": [{"timestamp": "asc"}],
            "query": {
                "range": {
                    "timestamp": {
                        "gt": LAST_TIMESTAMP,
                    }
                }
            },
        }
    return {
        "size": QUERY_SIZE,
        "sort": [{"timestamp": "asc"}],
        "query": {"match_all": {}},
    }


def read_once(endpoint: str) -> tuple[bool, list[dict]]:
    try:
        response = requests.get(
            f"{endpoint}/{INDEX}/_search",
            json=build_query(),
            timeout=5,
        )
    except requests.exceptions.RequestException:
        return False, []

    if not response.ok:
        return False, []

    hits = response.json().get("hits", {}).get("hits", [])
    return True, hits


def consume_from(endpoints: list[str]) -> tuple[str | None, list[dict]]:
    for endpoint in endpoints:
        ok, hits = read_once(endpoint)
        if ok:
            return endpoint, hits
        log.warning("consumer endpoint unavailable endpoint=%s", endpoint)
    return None, []


def main() -> None:
    if not URLS:
        raise RuntimeError("No endpoints configured")

    log.info("Consumer started. Index=%s endpoints=%s mode=round_robin", INDEX, URLS)
    cycle = 0
    global LAST_TIMESTAMP

    while True:
        endpoint, hits = consume_from(endpoint_order(cycle))
        if endpoint is None:
            log.error("consumer failed on all endpoints")
            time.sleep(POLL_INTERVAL)
            continue

        if hits:
            for item in hits:
                source = item.get("_source", {})
                ts = source.get("timestamp", "")
                counter = source.get("counter", "n/a")
                data = source.get("data", "")
                log.info(
                    "read endpoint=%s counter=%s ts=%s data=%s",
                    endpoint,
                    counter,
                    ts,
                    data,
                )
                if ts:
                    LAST_TIMESTAMP = ts
        else:
            log.info("read endpoint=%s no new data", endpoint)

        cycle += 1
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Consumer stopped")
