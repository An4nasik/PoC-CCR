import logging
import os
import subprocess
import time

import requests

from cluster_state import (
    collect_cluster_snapshots,
    current_epoch,
    request_with_retry,
    resolve_current_leader,
)
from config import load_env

load_env()
CLUSTER_1 = os.getenv("LEADER_URL", "http://localhost:9200").rstrip("/")
CLUSTER_2 = os.getenv("FOLLOWER_URL", "http://localhost:9201").rstrip("/")
INDEX = os.getenv("CCR_INDEX", "rag_data")
CONNECTION_ALIAS = os.getenv("CCR_ALIAS", "leader-cluster")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def detect_current_leader() -> tuple[str, str]:
    snapshots = collect_cluster_snapshots([CLUSTER_1, CLUSTER_2], INDEX)
    for snapshot in snapshots:
        log.info(
            "endpoint=%s health=%s replication=%s epoch=%s leader_url=%s",
            snapshot.url,
            snapshot.health_status or "down",
            snapshot.replication_status or "unknown",
            snapshot.leader_epoch if snapshot.leader_epoch is not None else "n/a",
            snapshot.leader_url or "n/a",
        )

    leader = resolve_current_leader(snapshots)
    if not leader:
        raise RuntimeError("Cannot determine current leader safely")

    epoch = current_epoch(snapshots)
    new_follower = CLUSTER_1 if leader.url == CLUSTER_2 else CLUSTER_2
    follower_snapshot = next(snapshot for snapshot in snapshots if snapshot.url == new_follower)
    if (
        epoch is not None
        and follower_snapshot.leader_epoch == epoch
        and follower_snapshot.is_writable
    ):
        raise RuntimeError("Target follower is still writable on the current epoch")

    return leader.url, new_follower


def get_container_ip(container_name: str) -> str:
    result = subprocess.run(
        [
            "docker",
            "inspect",
            "--format={{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
            container_name,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    ip = result.stdout.strip()
    if not ip:
        raise RuntimeError(f"Could not resolve IP for {container_name}")
    return ip


def wait_for_cluster(url: str, name: str, timeout: int = 60) -> None:
    for _ in range(timeout):
        try:
            response = requests.get(f"{url}/_cluster/health", timeout=5)
            if response.ok:
                log.info("%s is ready", name)
                return
        except requests.exceptions.RequestException:
            pass
        time.sleep(1)
    raise RuntimeError(f"Timed out waiting for {name}")


def main() -> None:
    log.info("Failback started")

    wait_for_cluster(CLUSTER_1, "cluster-1")
    wait_for_cluster(CLUSTER_2, "cluster-2")

    current_leader, new_follower = detect_current_leader()
    log.info("Current leader=%s, will become follower=%s", current_leader, new_follower)

    if current_leader == CLUSTER_1:
        log.info("cluster-1 is already leader, nothing to do")
        return

    log.info("Deleting old index on new follower (%s)", new_follower)
    response = request_with_retry(
        "DELETE",
        f"{new_follower}/{INDEX}",
        retries=5,
        logger=log,
        timeout=10,
    )
    if response.ok:
        log.info("Old index deleted")
    elif response.status_code == 404 or "index_not_found" in response.text:
        log.info("Index does not exist, skipping delete")
    else:
        raise RuntimeError(f"Failed to delete index: {response.text}")

    if new_follower == CLUSTER_1:
        container_name = "os-cluster-2"
    else:
        container_name = "os-cluster-1"

    leader_ip = get_container_ip(container_name)
    log.info("Configuring remote connection to %s (%s:9300)", container_name, leader_ip)

    response = request_with_retry(
        "PUT",
        f"{new_follower}/_cluster/settings",
        retries=5,
        logger=log,
        json={
            "persistent": {
                "cluster": {"remote": {CONNECTION_ALIAS: {"seeds": [f"{leader_ip}:9300"]}}}
            }
        },
        timeout=10,
    )
    if not response.ok:
        raise RuntimeError(f"Failed to configure remote connection: {response.text}")
    log.info("Remote connection configured")

    log.info("Starting replication from %s to %s", current_leader, new_follower)
    for attempt in range(1, 11):
        response = request_with_retry(
            "PUT",
            f"{new_follower}/_plugins/_replication/{INDEX}/_start",
            retries=5,
            logger=log,
            json={"leader_alias": CONNECTION_ALIAS, "leader_index": INDEX},
            timeout=10,
        )
        if response.ok or "Replication already in progress" in response.text:
            log.info("Replication started on attempt %s", attempt)
            break
        if "Primary shards" in response.text:
            log.warning("Leader shards not ready, attempt %s", attempt)
            time.sleep(2)
            continue
        raise RuntimeError(f"Failed to start replication: {response.text}")
    else:
        raise RuntimeError("Failed to start replication after retries")

    time.sleep(2)
    final_status = collect_cluster_snapshots([new_follower], INDEX)[0].replication_status
    log.info("Replication status on new follower: %s", final_status or "unknown")
    log.info("Failback completed. Leader=%s, Follower=%s", current_leader, new_follower)


if __name__ == "__main__":
    main()
