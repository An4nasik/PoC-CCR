import logging
import os
import time

import requests
from config import load_env

load_env()
FOLLOWER = os.getenv("FOLLOWER_URL", "http://localhost:9201").rstrip("/")
INDEX = os.getenv("CCR_INDEX", "rag_data")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def request_with_retry(method: str, url: str, retries: int = 8, **kwargs):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.request(method, url, **kwargs)
            return response
        except requests.exceptions.RequestException as err:
            last_error = err
            log.warning("request failed attempt=%s/%s error=%s", attempt, retries, err)
            time.sleep(2)
    raise RuntimeError(f"Request failed after retries: {last_error}")


def main() -> None:
    log.info("Failover started")
    response = request_with_retry(
        "GET",
        f"{FOLLOWER}/_plugins/_replication/{INDEX}/_status",
        timeout=10,
    )
    if response.ok:
        status = response.json().get("status", "unknown")
        log.info("current replication status=%s", status)
        if status == "REPLICATION NOT IN PROGRESS":
            log.info("replication already stopped")
            return
    else:
        log.warning("status unavailable: %s", response.text[:160])

    log.info("pausing replication")
    response = request_with_retry(
        "POST",
        f"{FOLLOWER}/_plugins/_replication/{INDEX}/_pause",
        json={},
        timeout=10,
    )
    if response.ok:
        log.info("replication paused")
    else:
        log.warning("pause request failed: %s", response.text[:160])

    log.info("stopping replication")
    response = request_with_retry(
        "POST",
        f"{FOLLOWER}/_plugins/_replication/{INDEX}/_stop",
        json={},
        timeout=10,
    )
    if response.ok:
        log.info("replication stopped")
    elif "No replication in progress" in response.text:
        log.info("replication already stopped")
    else:
        raise RuntimeError(f"Failed to stop replication: {response.text}")

    log.info("verifying status")
    status_response = request_with_retry(
        "GET",
        f"{FOLLOWER}/_plugins/_replication/{INDEX}/_status",
        timeout=10,
    )
    status = (
        status_response.json().get("status", "unknown")
        if status_response.ok
        else "unknown"
    )
    log.info("status after stop=%s", status)

    log.info("verifying writes")
    write_response = request_with_retry(
        "POST",
        f"{FOLLOWER}/{INDEX}/_doc",
        json={"test": "failover_check"},
        timeout=10,
    )
    if not write_response.ok:
        raise RuntimeError(f"Index is not writable: {write_response.text}")
    log.info("write check successful")
    log.info("Failover completed. Use OPENSEARCH_URL=%s", FOLLOWER)


if __name__ == "__main__":
    main()
