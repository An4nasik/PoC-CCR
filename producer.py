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
PRODUCER_RETRY_DELAY = float(os.getenv("PRODUCER_RETRY_DELAY", "1"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


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
            response = requests.get(
                f"{endpoint}/_plugins/_replication/{INDEX}/_status",
                timeout=3,
            )
            if response.ok and response.json().get("status") == "REPLICATION NOT IN PROGRESS":
                return endpoint
        except requests.exceptions.RequestException:
            continue
    return None


def write_with_failover(counter: int, document: dict, active_endpoint: str | None) -> str | None:
    endpoint = active_endpoint or detect_writable_endpoint()
    if endpoint:
        ok, reason = write_once(endpoint, document)
        if ok:
            log.info(
                "written counter=%s ts=%s endpoint=%s",
                counter,
                document["timestamp"],
                endpoint,
            )
            return endpoint
        if reason == "read_only":
            log.warning("active endpoint is read-only endpoint=%s", endpoint)
        elif reason == "connection":
            log.warning("active endpoint unavailable endpoint=%s", endpoint)
        else:
            log.warning("write failed endpoint=%s", endpoint)

    for candidate in URLS:
        if candidate == endpoint:
            continue
        ok, reason = write_once(candidate, document)
        if ok:
            log.info(
                "written counter=%s ts=%s endpoint=%s",
                counter,
                document["timestamp"],
                candidate,
            )
            return candidate
        if reason == "read_only":
            log.warning("endpoint is read-only endpoint=%s", candidate)
        elif reason == "connection":
            log.warning("endpoint is unavailable endpoint=%s", candidate)
        else:
            log.warning("write failed endpoint=%s", candidate)

    if endpoint:
        rediscovered = detect_writable_endpoint()
        if rediscovered and rediscovered != endpoint:
            ok, reason = write_once(rediscovered, document)
            if ok:
                log.info(
                    "written counter=%s ts=%s endpoint=%s",
                    counter,
                    document["timestamp"],
                    rediscovered,
                )
                return rediscovered
            if reason == "read_only":
                log.warning("rediscovered endpoint is read-only endpoint=%s", rediscovered)
            elif reason == "connection":
                log.warning("rediscovered endpoint unavailable endpoint=%s", rediscovered)
            else:
                log.warning("write failed endpoint=%s", rediscovered)

    if not endpoint:
        log.warning("writable endpoint not detected for index=%s", INDEX)

    return None


def main() -> None:
    if not URLS:
        raise RuntimeError("No endpoints configured")

    active_endpoint = detect_writable_endpoint()
    if active_endpoint:
        log.info("Detected writable endpoint=%s", active_endpoint)
    log.info("Producer started. Index=%s endpoints=%s mode=failover-only", INDEX, URLS)

    counter = 0
    while True:
        document = {
            "timestamp": datetime.now(UTC).isoformat(),
            "counter": counter,
            "data": f"document-{counter}",
        }

        selected = write_with_failover(counter, document, active_endpoint)
        if selected is None:
            log.error("write failed on all endpoints counter=%s", counter)
            time.sleep(PRODUCER_RETRY_DELAY)
        else:
            active_endpoint = selected
            counter += 1

        time.sleep(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Producer stopped")
