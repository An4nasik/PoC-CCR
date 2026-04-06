import unittest
from unittest.mock import patch

import consumer
import producer
from cluster_state import ClusterSnapshot


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


class RuntimeBehaviourTests(unittest.TestCase):
    @patch("producer.collect_cluster_snapshots")
    def test_producer_prefers_current_epoch_leader(self, mocked_collect) -> None:
        mocked_collect.return_value = [
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

        endpoint, _ = producer.detect_writable_endpoint(None)

        self.assertEqual("http://cluster-2", endpoint)

    @patch("producer.collect_cluster_snapshots")
    def test_producer_reports_none_for_ambiguous_boot_state(self, mocked_collect) -> None:
        mocked_collect.return_value = [
            build_snapshot("http://cluster-1", replication_status="REPLICATION NOT IN PROGRESS"),
            build_snapshot("http://cluster-2", replication_status="REPLICATION NOT IN PROGRESS"),
        ]

        endpoint, _ = producer.detect_writable_endpoint(None)

        self.assertIsNone(endpoint)

    @patch("consumer.collect_cluster_snapshots")
    def test_consumer_reads_only_from_current_generation(self, mocked_collect) -> None:
        mocked_collect.return_value = [
            build_snapshot(
                "http://cluster-1",
                replication_status="REPLICATION NOT IN PROGRESS",
                leader_epoch=1,
                leader_url="http://cluster-1",
            ),
            build_snapshot(
                "http://cluster-2",
                replication_status="SYNCING",
                leader_epoch=2,
                leader_url="http://cluster-2",
            ),
        ]

        ordered_endpoints, _ = consumer.endpoint_order(0)

        self.assertEqual(["http://cluster-2"], ordered_endpoints)


if __name__ == "__main__":
    unittest.main()
