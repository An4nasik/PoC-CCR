import logging
import os

import requests

LEADER = os.getenv("LEADER_URL", "http://localhost:9200").rstrip("/")
FOLLOWER = os.getenv("FOLLOWER_URL", "http://localhost:9201").rstrip("/")
INDEX = os.getenv("CCR_INDEX", "rag_data")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def check_cluster(url: str, name: str) -> None:
    log.info("cluster=%s url=%s", name, url)
    try:
        health_response = requests.get(f"{url}/_cluster/health", timeout=3)
        if not health_response.ok:
            log.error("cluster unavailable")
            return
        health = health_response.json()
        log.info("health=%s", health.get("status", "unknown"))
    except requests.exceptions.RequestException as err:
        log.error("cluster unavailable error=%s", err)
        return

    try:
        count_response = requests.get(f"{url}/{INDEX}/_count", timeout=3)
        if count_response.ok:
            count = count_response.json().get("count", 0)
            log.info("index=%s count=%s", INDEX, count)
        else:
            log.info("index=%s not found", INDEX)
    except requests.exceptions.RequestException as err:
        log.error("count request failed error=%s", err)

    try:
        status_response = requests.get(
            f"{url}/_plugins/_replication/{INDEX}/_status", timeout=3
        )
        if status_response.ok:
            data = status_response.json()
            status = data.get("status", "unknown")
            log.info("replication_status=%s", status)
            if "syncing_details" in data:
                details = data["syncing_details"]
                log.info(
                    "leader_checkpoint=%s follower_checkpoint=%s",
                    details.get("leader_checkpoint", "N/A"),
                    details.get("follower_checkpoint", "N/A"),
                )
    except requests.exceptions.RequestException:
        log.info("replication status endpoint not available")


def main() -> None:
    log.info("CCR status")
    check_cluster(LEADER, "LEADER (cluster-1)")
    check_cluster(FOLLOWER, "FOLLOWER (cluster-2)")


if __name__ == "__main__":
    main()
