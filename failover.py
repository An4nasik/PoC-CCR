import logging
import os
import time

import requests

from config import load_env

load_env()
CLUSTER_1 = os.getenv("LEADER_URL", "http://localhost:9200").rstrip("/")
CLUSTER_2 = os.getenv("FOLLOWER_URL", "http://localhost:9201").rstrip("/")
INDEX = os.getenv("CCR_INDEX", "rag_data")
ENDPOINTS = [CLUSTER_1, CLUSTER_2]

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


def get_replication_status(url: str) -> str | None:
    try:
        response = requests.get(f"{url}/_plugins/_replication/{INDEX}/_status", timeout=10)
        if response.ok:
            return response.json().get("status")
        if response.status_code == 500:
            return "ERROR"
    except requests.exceptions.RequestException:
        pass
    return None


def detect_follower() -> str | None:
    """Find a live endpoint that has replication (not already a leader)."""
    for _ in range(60):
        for endpoint in ENDPOINTS:
            try:
                r = requests.get(f"{endpoint}/_cluster/health", timeout=3)
                if not r.ok:
                    continue
            except requests.exceptions.RequestException:
                continue

            status = get_replication_status(endpoint)
            if status is None:
                continue
            log.info("endpoint=%s status=%s", endpoint, status)
            if status not in {"REPLICATION NOT IN PROGRESS", None}:
                return endpoint
        time.sleep(1)
    return None


def main() -> None:
    log.info("Failover started")

    follower = detect_follower()
    if not follower:
        log.info("No active follower found, checking for already promoted endpoints")
        for endpoint in ENDPOINTS:
            status = get_replication_status(endpoint)
            if status == "REPLICATION NOT IN PROGRESS":
                log.info("endpoint=%s is already writable", endpoint)
                return
        raise RuntimeError("Cannot determine follower to promote")

    log.info("Detected follower to promote: %s", follower)

    response = request_with_retry(
        "GET",
        f"{follower}/_plugins/_replication/{INDEX}/_status",
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
        f"{follower}/_plugins/_replication/{INDEX}/_pause",
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
        f"{follower}/_plugins/_replication/{INDEX}/_stop",
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
        f"{follower}/_plugins/_replication/{INDEX}/_status",
        timeout=10,
    )
    status = status_response.json().get("status", "unknown") if status_response.ok else "unknown"
    log.info("status after stop=%s", status)

    log.info("verifying writes")
    write_response = request_with_retry(
        "POST",
        f"{follower}/{INDEX}/_doc",
        json={"test": "failover_check"},
        timeout=10,
    )
    if not write_response.ok:
        raise RuntimeError(f"Index is not writable: {write_response.text}")
    log.info("write check successful")
    log.info("Failover completed. New leader=%s", follower)


if __name__ == "__main__":
    main()
