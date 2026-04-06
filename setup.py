import logging
import os
import subprocess
import time

import requests

from config import load_env

load_env()
LEADER = os.getenv("LEADER_URL", "http://localhost:9200").rstrip("/")
FOLLOWER = os.getenv("FOLLOWER_URL", "http://localhost:9201").rstrip("/")
INDEX = os.getenv("CCR_INDEX", "rag_data")
CONNECTION_ALIAS = os.getenv("CCR_ALIAS", "leader-cluster")
ACTIVE_STATUSES = {"SYNCING", "BOOTSTRAPPING", "PAUSED"}
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def wait_for_cluster(url: str, name: str, timeout: int = 60) -> None:
    last_error = ""
    for _ in range(timeout):
        try:
            response = requests.get(f"{url}/_cluster/health", timeout=5)
            if response.ok:
                log.info("%s is ready", name)
                return
            last_error = response.text
        except requests.exceptions.RequestException as err:
            last_error = str(err)
        time.sleep(1)
    raise RuntimeError(f"Timed out waiting for {name}. Last error: {last_error}")


def get_leader_ip() -> str:
    result = subprocess.run(
        [
            "docker",
            "inspect",
            "--format={{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
            "os-cluster-1",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    leader_ip = result.stdout.strip()
    if not leader_ip:
        raise RuntimeError("Could not resolve os-cluster-1 IP address")
    return leader_ip


def replication_status() -> str | None:
    response = requests.get(f"{FOLLOWER}/_plugins/_replication/{INDEX}/_status", timeout=10)
    if response.ok:
        return response.json().get("status", "unknown")
    return None


def delete_follower_index() -> None:
    response = requests.delete(f"{FOLLOWER}/{INDEX}", timeout=10)
    if response.ok:
        return
    if response.status_code == 404:
        return
    if "index_not_found_exception" in response.text:
        return
    raise RuntimeError(f"Failed to delete follower index: {response.text}")


def start_replication() -> None:
    response = None
    for attempt in range(1, 11):
        response = requests.put(
            f"{FOLLOWER}/_plugins/_replication/{INDEX}/_start",
            json={"leader_alias": CONNECTION_ALIAS, "leader_index": INDEX},
            timeout=10,
        )
        if response.ok or "Replication already in progress" in response.text:
            log.info("Replication started on attempt %s", attempt)
            return
        if "Primary shards in the Index" in response.text:
            log.warning("Leader shards are not active yet, attempt %s", attempt)
            time.sleep(2)
            continue
        if "resource_already_exists_exception" in response.text:
            log.warning("Follower index already exists, recreating it")
            delete_follower_index()
            time.sleep(1)
            continue
        raise RuntimeError(f"Failed to start replication: {response.text}")
    raise RuntimeError(f"Failed to start replication after retries: {response.text}")


def main() -> None:
    wait_for_cluster(LEADER, "Leader (cluster-1)")
    wait_for_cluster(FOLLOWER, "Follower (cluster-2)")

    log.info("Creating index '%s' on Leader", INDEX)
    response = requests.put(
        f"{LEADER}/{INDEX}",
        json={"settings": {"number_of_shards": 1, "number_of_replicas": 0}},
        timeout=10,
    )
    if response.ok or "resource_already_exists_exception" in response.text:
        log.info("Index is ready")
    else:
        raise RuntimeError(f"Failed to create index: {response.text}")

    leader_ip = get_leader_ip()
    log.info("Configuring remote connection with seed %s:9300", leader_ip)
    response = requests.put(
        f"{FOLLOWER}/_cluster/settings",
        json={
            "persistent": {
                "cluster": {"remote": {CONNECTION_ALIAS: {"seeds": [f"{leader_ip}:9300"]}}}
            }
        },
        timeout=10,
    )
    if response.ok:
        log.info("Remote connection configured")
    else:
        raise RuntimeError(f"Failed to configure remote connection: {response.text}")

    current_status = replication_status()
    if current_status in ACTIVE_STATUSES:
        log.info("Replication already active: %s", current_status)
    else:
        if current_status == "REPLICATION NOT IN PROGRESS":
            delete_follower_index()
        log.info("Starting replication for '%s'", INDEX)
        start_replication()

    time.sleep(2)
    final_status = replication_status()
    log.info("Replication status: %s", final_status or "unknown")
    log.info("Setup completed")


if __name__ == "__main__":
    main()
