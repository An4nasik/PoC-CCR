import logging
import os
import time

from cluster_state import (
    bump_leader_metadata,
    collect_cluster_snapshots,
    describe_role,
    request_with_retry,
    resolve_current_leader,
    resolve_follower_candidate,
)
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


def detect_follower() -> tuple[str | None, list]:
    for _ in range(60):
        snapshots = collect_cluster_snapshots(ENDPOINTS, INDEX)
        follower = resolve_follower_candidate(snapshots)
        if follower:
            return follower.url, snapshots

        leader = resolve_current_leader(snapshots)
        if leader:
            return None, snapshots
        time.sleep(1)
    return None, collect_cluster_snapshots(ENDPOINTS, INDEX)


def main() -> None:
    log.info("Failover started")

    follower, snapshots = detect_follower()
    if not follower:
        leader = resolve_current_leader(snapshots)
        if leader:
            log.info("endpoint=%s is already writable", leader.url)
            return
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
        raise RuntimeError("Cannot determine follower to promote")

    log.info("Detected follower to promote: %s", follower)

    response = request_with_retry(
        "GET",
        f"{follower}/_plugins/_replication/{INDEX}/_status",
        retries=8,
        logger=log,
        timeout=10,
    )
    should_stop_replication = True
    if response.ok:
        status = response.json().get("status", "unknown")
        log.info("current replication status=%s", status)
        if status == "REPLICATION NOT IN PROGRESS":
            log.info("replication already stopped")
            should_stop_replication = False
    else:
        log.warning("status unavailable: %s", response.text[:160])

    if should_stop_replication:
        log.info("pausing replication")
        response = request_with_retry(
            "POST",
            f"{follower}/_plugins/_replication/{INDEX}/_pause",
            json={},
            retries=8,
            logger=log,
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
            retries=8,
            logger=log,
            timeout=10,
        )
        if response.ok:
            log.info("replication stopped")
        elif "No replication in progress" in response.text:
            log.info("replication already stopped")
        else:
            raise RuntimeError(f"Failed to stop replication: {response.text}")

    log.info("bumping leader metadata")
    metadata_response = bump_leader_metadata(follower, INDEX, timeout=10)
    if not metadata_response.ok:
        raise RuntimeError(f"Failed to write leader metadata: {metadata_response.text}")
    log.info("leader metadata updated")
    log.info("Failover completed. New leader=%s", follower)


if __name__ == "__main__":
    main()
