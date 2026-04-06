import logging
import os

from cluster_state import collect_cluster_snapshots, describe_role
from config import load_env

load_env()
LEADER = os.getenv("LEADER_URL", "http://localhost:9200").rstrip("/")
FOLLOWER = os.getenv("FOLLOWER_URL", "http://localhost:9201").rstrip("/")
INDEX = os.getenv("CCR_INDEX", "rag_data")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

def main() -> None:
    log.info("CCR status")
    snapshots = collect_cluster_snapshots([LEADER, FOLLOWER], INDEX, include_counts=True)
    labels = {
        LEADER: "cluster-1",
        FOLLOWER: "cluster-2",
    }
    for snapshot in snapshots:
        log.info("cluster=%s url=%s", labels.get(snapshot.url, snapshot.url), snapshot.url)
        if not snapshot.healthy:
            log.error("cluster unavailable error=%s", snapshot.error or "unknown")
            continue
        count = snapshot.data_count if snapshot.data_count is not None else "n/a"
        leader_epoch = snapshot.leader_epoch if snapshot.leader_epoch is not None else "n/a"
        log.info("health=%s", snapshot.health_status or "unknown")
        log.info("role=%s", describe_role(snapshot, snapshots))
        log.info("index=%s count=%s", INDEX, count)
        log.info("replication_status=%s", snapshot.replication_status or "unknown")
        log.info("leader_epoch=%s", leader_epoch)
        log.info("leader_url=%s", snapshot.leader_url or "n/a")


if __name__ == "__main__":
    main()
