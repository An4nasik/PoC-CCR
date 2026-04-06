import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime

import requests

META_DOC_ID = "__ccr_meta__"
META_DOC_KIND = "ccr_meta"
WRITABLE_STATUS = "REPLICATION NOT IN PROGRESS"
ACTIVE_REPLICATION_STATUSES = frozenset({"SYNCING", "BOOTSTRAPPING", "PAUSED"})
FOLLOWER_STATUSES = ACTIVE_REPLICATION_STATUSES | frozenset({"ERROR"})
REQUEST_TIMEOUT = 5


@dataclass(frozen=True)
class ClusterSnapshot:
    url: str
    healthy: bool
    health_status: str | None
    replication_status: str | None
    leader_epoch: int | None
    leader_url: str | None
    data_count: int | None = None
    error: str | None = None

    @property
    def is_writable(self) -> bool:
        return self.healthy and self.replication_status == WRITABLE_STATUS

    @property
    def is_follower(self) -> bool:
        return self.healthy and self.replication_status in FOLLOWER_STATUSES


def normalize_url(url: str) -> str:
    return url.strip().rstrip("/")


def build_endpoints(raw_urls: str | None, fallback: str) -> list[str]:
    source = raw_urls or fallback
    urls: list[str] = []
    seen: set[str] = set()
    for item in source.split(","):
        normalized = normalize_url(item)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        urls.append(normalized)
    return urls


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def data_count_query() -> dict:
    return {"query": {"bool": {"must_not": [{"ids": {"values": [META_DOC_ID]}}]}}}


def data_search_query(last_timestamp: str, query_size: int) -> dict:
    query = {
        "size": query_size,
        "sort": [{"timestamp": "asc"}],
        "query": {"bool": {"must_not": [{"ids": {"values": [META_DOC_ID]}}]}},
    }
    if last_timestamp:
        query["query"]["bool"]["filter"] = [{"range": {"timestamp": {"gt": last_timestamp}}}]
    return query


def request_with_retry(
    method: str,
    url: str,
    *,
    retries: int = 5,
    delay_seconds: int = 2,
    logger: logging.Logger | None = None,
    **kwargs,
) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return requests.request(method, url, **kwargs)
        except requests.exceptions.RequestException as err:
            last_error = err
            if logger:
                logger.warning("request failed attempt=%s/%s error=%s", attempt, retries, err)
            time.sleep(delay_seconds)
    raise RuntimeError(f"Request failed after retries: {last_error}")


def get_replication_status(url: str, index: str, timeout: int = REQUEST_TIMEOUT) -> str | None:
    try:
        response = requests.get(f"{url}/_plugins/_replication/{index}/_status", timeout=timeout)
    except requests.exceptions.RequestException:
        return None

    if response.ok:
        return response.json().get("status")
    if response.status_code == 500:
        return "ERROR"
    return None


def get_leader_metadata(
    url: str,
    index: str,
    timeout: int = REQUEST_TIMEOUT,
) -> tuple[int | None, str | None]:
    try:
        response = requests.get(f"{url}/{index}/_doc/{META_DOC_ID}", timeout=timeout)
    except requests.exceptions.RequestException:
        return None, None

    if not response.ok:
        return None, None

    source = response.json().get("_source", {})
    epoch_raw = source.get("leader_epoch")
    leader_url_raw = source.get("leader_url")
    try:
        epoch = int(epoch_raw)
    except (TypeError, ValueError):
        epoch = None
    leader_url = normalize_url(leader_url_raw) if isinstance(leader_url_raw, str) else None
    return epoch, leader_url


def write_leader_metadata(
    url: str,
    index: str,
    *,
    leader_epoch: int,
    leader_url: str,
    timeout: int = REQUEST_TIMEOUT,
) -> requests.Response:
    return requests.put(
        f"{url}/{index}/_doc/{META_DOC_ID}",
        params={"refresh": "wait_for"},
        json={
            "kind": META_DOC_KIND,
            "leader_epoch": leader_epoch,
            "leader_url": normalize_url(leader_url),
            "updated_at": utc_now_iso(),
        },
        timeout=timeout,
    )


def bump_leader_metadata(url: str, index: str, timeout: int = REQUEST_TIMEOUT) -> requests.Response:
    current_epoch, _ = get_leader_metadata(url, index, timeout=timeout)
    next_epoch = (current_epoch or 0) + 1
    return write_leader_metadata(
        url,
        index,
        leader_epoch=next_epoch,
        leader_url=url,
        timeout=timeout,
    )


def fetch_data_count(url: str, index: str, timeout: int = REQUEST_TIMEOUT) -> int | None:
    try:
        response = requests.post(
            f"{url}/{index}/_count",
            json=data_count_query(),
            timeout=timeout,
        )
    except requests.exceptions.RequestException:
        return None

    if not response.ok:
        return None
    return response.json().get("count")


def collect_cluster_snapshot(
    url: str,
    index: str,
    *,
    include_counts: bool = False,
    timeout: int = REQUEST_TIMEOUT,
) -> ClusterSnapshot:
    try:
        health_response = requests.get(f"{url}/_cluster/health", timeout=timeout)
    except requests.exceptions.RequestException as err:
        return ClusterSnapshot(
            url=url,
            healthy=False,
            health_status=None,
            replication_status=None,
            leader_epoch=None,
            leader_url=None,
            error=str(err),
        )

    if not health_response.ok:
        return ClusterSnapshot(
            url=url,
            healthy=False,
            health_status=None,
            replication_status=None,
            leader_epoch=None,
            leader_url=None,
            error=health_response.text[:160],
        )

    health_status = health_response.json().get("status", "unknown")
    replication_status = get_replication_status(url, index, timeout=timeout)
    leader_epoch, leader_url = get_leader_metadata(url, index, timeout=timeout)
    data_count = fetch_data_count(url, index, timeout=timeout) if include_counts else None

    return ClusterSnapshot(
        url=url,
        healthy=True,
        health_status=health_status,
        replication_status=replication_status,
        leader_epoch=leader_epoch,
        leader_url=leader_url,
        data_count=data_count,
    )


def collect_cluster_snapshots(
    urls: list[str],
    index: str,
    *,
    include_counts: bool = False,
    timeout: int = REQUEST_TIMEOUT,
) -> list[ClusterSnapshot]:
    return [
        collect_cluster_snapshot(url, index, include_counts=include_counts, timeout=timeout)
        for url in urls
    ]


def current_epoch(snapshots: list[ClusterSnapshot]) -> int | None:
    epochs = [
        snapshot.leader_epoch
        for snapshot in snapshots
        if snapshot.healthy and snapshot.leader_epoch
    ]
    return max(epochs) if epochs else None


def current_generation_snapshots(snapshots: list[ClusterSnapshot]) -> list[ClusterSnapshot]:
    epoch = current_epoch(snapshots)
    healthy = [snapshot for snapshot in snapshots if snapshot.healthy]
    if epoch is None:
        return healthy
    return [snapshot for snapshot in healthy if snapshot.leader_epoch == epoch]


def resolve_current_leader(snapshots: list[ClusterSnapshot]) -> ClusterSnapshot | None:
    snapshot_by_url = {snapshot.url: snapshot for snapshot in snapshots}
    epoch = current_epoch(snapshots)

    if epoch is not None:
        leader_urls = {
            snapshot.leader_url
            for snapshot in snapshots
            if snapshot.healthy and snapshot.leader_epoch == epoch and snapshot.leader_url
        }
        if len(leader_urls) == 1:
            leader_url = next(iter(leader_urls))
            leader_snapshot = snapshot_by_url.get(leader_url)
            if leader_snapshot and leader_snapshot.is_writable:
                return leader_snapshot

    writable = [snapshot for snapshot in snapshots if snapshot.is_writable]
    if len(writable) == 1:
        return writable[0]
    return None


def resolve_follower_candidate(snapshots: list[ClusterSnapshot]) -> ClusterSnapshot | None:
    leader = resolve_current_leader(snapshots)
    candidates = [
        snapshot
        for snapshot in snapshots
        if snapshot.healthy and (leader is None or snapshot.url != leader.url)
    ]

    explicit_followers = [
        snapshot for snapshot in candidates if snapshot.replication_status in FOLLOWER_STATUSES
    ]
    if explicit_followers:
        return max(explicit_followers, key=lambda snapshot: snapshot.leader_epoch or -1)

    if leader is None:
        metadata_followers = [
            snapshot
            for snapshot in candidates
            if snapshot.leader_url and snapshot.leader_url != snapshot.url
        ]
        if len(metadata_followers) == 1:
            return metadata_followers[0]
        if metadata_followers:
            return max(metadata_followers, key=lambda snapshot: snapshot.leader_epoch or -1)

    return None


def describe_role(snapshot: ClusterSnapshot, snapshots: list[ClusterSnapshot]) -> str:
    if not snapshot.healthy:
        return "down"

    leader = resolve_current_leader(snapshots)
    epoch = current_epoch(snapshots)
    if leader and snapshot.url == leader.url:
        return "leader"
    if snapshot.is_follower:
        return "follower"
    if snapshot.is_writable:
        if (
            epoch is not None
            and snapshot.leader_epoch is not None
            and snapshot.leader_epoch < epoch
        ):
            return "stale_writable"
        return "writable_candidate"
    if epoch is not None and snapshot.leader_epoch is not None and snapshot.leader_epoch < epoch:
        return "stale_reader"
    return "unknown"
