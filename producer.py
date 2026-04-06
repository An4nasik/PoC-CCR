import logging
import os
import time
from datetime import UTC, datetime

import requests

from cluster_state import (
    build_endpoints,
    collect_cluster_snapshots,
    current_generation_snapshots,
    describe_role,
    resolve_current_leader,
)
from config import load_env

load_env()
INDEX = os.getenv("CCR_INDEX", "rag_data")
URLS = build_endpoints(
    os.getenv("OPENSEARCH_URLS"),
    os.getenv("OPENSEARCH_URL", "http://localhost:9200,http://localhost:9201"),
)
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


def detect_writable_endpoint(active_endpoint: str | None) -> tuple[str | None, list]:
    snapshots = collect_cluster_snapshots(URLS, INDEX)
    leader = resolve_current_leader(snapshots)
    if leader:
        return leader.url, snapshots

    if active_endpoint:
        snapshot_by_url = {snapshot.url: snapshot for snapshot in snapshots}
        active_snapshot = snapshot_by_url.get(active_endpoint)
        current_urls = {snapshot.url for snapshot in current_generation_snapshots(snapshots)}
        if active_snapshot and active_snapshot.is_writable and (
            not current_urls or active_snapshot.url in current_urls
        ):
            return active_endpoint, snapshots

    return None, snapshots


def log_cluster_state(snapshots: list) -> None:
    for snapshot in snapshots:
        log.warning(
            "state endpoint=%s role=%s health=%s replication=%s epoch=%s leader_url=%s",
            snapshot.url,
            describe_role(snapshot, snapshots),
            snapshot.health_status or "down",
            snapshot.replication_status or "unknown",
            snapshot.leader_epoch if snapshot.leader_epoch is not None else "n/a",
            snapshot.leader_url or "n/a",
        )


def write_with_failover(counter: int, document: dict, active_endpoint: str | None) -> str | None:
    endpoint, snapshots = detect_writable_endpoint(active_endpoint)
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
            log.warning("selected endpoint is read-only endpoint=%s", endpoint)
        elif reason == "connection":
            log.warning("selected endpoint unavailable endpoint=%s", endpoint)
        else:
            log.warning("write failed endpoint=%s", endpoint)

    log.warning("writable endpoint not confirmed for index=%s", INDEX)
    log_cluster_state(snapshots)

    return None


def main() -> None:
    if not URLS:
        raise RuntimeError("No endpoints configured")

    active_endpoint, _ = detect_writable_endpoint(None)
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
