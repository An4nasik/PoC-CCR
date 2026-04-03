import logging
import os
import time
from datetime import UTC, datetime

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
STRATEGY = os.getenv("PRODUCER_STRATEGY", "auto").lower()
FAILOVER_ERRORS_THRESHOLD = int(os.getenv("FAILOVER_ERRORS_THRESHOLD", "3"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def endpoint_order(counter: int) -> list[str]:
    if not URLS:
        return []
    start = counter % len(URLS)
    return URLS[start:] + URLS[:start]


def is_read_only_response(response: requests.Response) -> bool:
    text = response.text.lower()
    return "cluster_block_exception" in text or "index write" in text


def write_once(endpoint: str, document: dict) -> tuple[bool, str]:
    try:
        response = requests.post(f"{endpoint}/{INDEX}/_doc", json=document, timeout=5)
    except requests.exceptions.RequestException:
        return False, "connection"

    if response.ok:
        return True, "ok"
    if is_read_only_response(response):
        return False, "read_only"
    return False, "error"


def detect_writable_endpoint() -> str | None:
    for endpoint in URLS:
        try:
            response = requests.get(f"{endpoint}/_plugins/_replication/{INDEX}/_status", timeout=3)
            if response.ok and response.json().get("status") == "REPLICATION NOT IN PROGRESS":
                return endpoint
        except requests.exceptions.RequestException:
            continue
    return None


def write_round_robin(counter: int, document: dict) -> str | None:
    for endpoint in endpoint_order(counter):
        ok, reason = write_once(endpoint, document)
        if ok:
            log.info("written counter=%s ts=%s endpoint=%s", counter, document["timestamp"], endpoint)
            return endpoint
        if reason == "read_only":
            log.warning("endpoint is read-only endpoint=%s", endpoint)
        elif reason == "connection":
            log.warning("endpoint is unavailable endpoint=%s", endpoint)
        else:
            log.warning("write failed endpoint=%s", endpoint)
    return None


def write_failover(counter: int, document: dict, active_endpoint: str | None) -> str | None:
    endpoint = active_endpoint or detect_writable_endpoint()
    if not endpoint:
        for candidate in URLS:
            ok, reason = write_once(candidate, document)
            if ok:
                log.info("written counter=%s ts=%s endpoint=%s", counter, document["timestamp"], candidate)
                return candidate
            if reason == "read_only":
                log.warning("endpoint is read-only endpoint=%s", candidate)
            elif reason == "connection":
                log.warning("endpoint is unavailable endpoint=%s", candidate)
            else:
                log.warning("write failed endpoint=%s", candidate)
        return None

    ok, reason = write_once(endpoint, document)
    if ok:
        log.info("written counter=%s ts=%s endpoint=%s", counter, document["timestamp"], endpoint)
        return endpoint

    if reason == "read_only":
        log.warning("active endpoint became read-only endpoint=%s", endpoint)
    elif reason == "connection":
        log.warning("active endpoint unavailable endpoint=%s", endpoint)
    else:
        log.warning("write failed endpoint=%s", endpoint)

    fallback = detect_writable_endpoint()
    if fallback and fallback != endpoint:
        ok, reason = write_once(fallback, document)
        if ok:
            log.info("written counter=%s ts=%s endpoint=%s", counter, document["timestamp"], fallback)
            return fallback
        if reason == "read_only":
            log.warning("fallback endpoint is read-only endpoint=%s", fallback)
        elif reason == "connection":
            log.warning("fallback endpoint unavailable endpoint=%s", fallback)
        else:
            log.warning("write failed endpoint=%s", fallback)

    return None


def main() -> None:
    if not URLS:
        raise RuntimeError("No endpoints configured")

    mode = "round_robin" if STRATEGY == "auto" else STRATEGY
    active_endpoint = detect_writable_endpoint() if mode == "failover" else None
    failures = 0

    log.info("Producer started. Index=%s endpoints=%s strategy=%s", INDEX, URLS, mode)

    counter = 0
    while True:
        document = {
            "timestamp": datetime.now(UTC).isoformat(),
            "counter": counter,
            "data": f"document-{counter}",
        }

        if mode == "failover":
            selected = write_failover(counter, document, active_endpoint)
        else:
            selected = write_round_robin(counter, document)

        if selected is None:
            failures += 1
            if mode == "round_robin" and failures >= FAILOVER_ERRORS_THRESHOLD:
                mode = "failover"
                active_endpoint = detect_writable_endpoint()
                log.warning("Switching producer mode to failover after %s failures", failures)
            log.error("write failed on all endpoints counter=%s", counter)
        else:
            failures = 0
            active_endpoint = selected

        counter += 1
        time.sleep(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Producer stopped")
