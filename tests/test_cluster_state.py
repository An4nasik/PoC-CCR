import unittest

from cluster_state import (
    ClusterSnapshot,
    current_generation_snapshots,
    describe_role,
    resolve_current_leader,
    resolve_follower_candidate,
)


def build_snapshot(
    url: str,
    *,
    healthy: bool = True,
    replication_status: str | None = None,
    leader_epoch: int | None = None,
    leader_url: str | None = None,
) -> ClusterSnapshot:
    return ClusterSnapshot(
        url=url,
        healthy=healthy,
        health_status="green" if healthy else None,
        replication_status=replication_status,
        leader_epoch=leader_epoch,
        leader_url=leader_url,
    )


class ClusterStateTests(unittest.TestCase):
    def test_resolve_current_leader_prefers_highest_epoch(self) -> None:
        snapshots = [
            build_snapshot(
                "http://cluster-1",
                replication_status="REPLICATION NOT IN PROGRESS",
                leader_epoch=1,
                leader_url="http://cluster-1",
            ),
            build_snapshot(
                "http://cluster-2",
                replication_status="REPLICATION NOT IN PROGRESS",
                leader_epoch=2,
                leader_url="http://cluster-2",
            ),
        ]

        leader = resolve_current_leader(snapshots)

        self.assertIsNotNone(leader)
        self.assertEqual("http://cluster-2", leader.url)

    def test_resolve_current_leader_falls_back_to_unique_writable_node(self) -> None:
        snapshots = [
            build_snapshot("http://cluster-1", replication_status="REPLICATION NOT IN PROGRESS"),
            build_snapshot("http://cluster-2", replication_status="SYNCING"),
        ]

        leader = resolve_current_leader(snapshots)

        self.assertIsNotNone(leader)
        self.assertEqual("http://cluster-1", leader.url)

    def test_resolve_current_leader_returns_none_for_ambiguous_writable_state(self) -> None:
        snapshots = [
            build_snapshot("http://cluster-1", replication_status="REPLICATION NOT IN PROGRESS"),
            build_snapshot("http://cluster-2", replication_status="REPLICATION NOT IN PROGRESS"),
        ]

        leader = resolve_current_leader(snapshots)

        self.assertIsNone(leader)

    def test_resolve_follower_candidate_handles_live_follower_when_leader_is_down(self) -> None:
        snapshots = [
            build_snapshot("http://cluster-1", healthy=False),
            build_snapshot(
                "http://cluster-2",
                replication_status="SYNCING",
                leader_epoch=2,
                leader_url="http://cluster-1",
            ),
        ]

        follower = resolve_follower_candidate(snapshots)

        self.assertIsNotNone(follower)
        self.assertEqual("http://cluster-2", follower.url)

    def test_current_generation_skips_stale_writable_cluster(self) -> None:
        snapshots = [
            build_snapshot(
                "http://cluster-1",
                replication_status="REPLICATION NOT IN PROGRESS",
                leader_epoch=1,
                leader_url="http://cluster-1",
            ),
            build_snapshot(
                "http://cluster-2",
                replication_status="REPLICATION NOT IN PROGRESS",
                leader_epoch=2,
                leader_url="http://cluster-2",
            ),
        ]

        current_snapshots = current_generation_snapshots(snapshots)

        self.assertEqual(["http://cluster-2"], [snapshot.url for snapshot in current_snapshots])
        self.assertEqual("stale_writable", describe_role(snapshots[0], snapshots))


if __name__ == "__main__":
    unittest.main()
