"""Behavior tests for signed SpGEMM revisions, packaging, tracing, and storage."""
from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from adjlink import (
    CorrectionPipeline,
    PackageBlobStore,
    ReceiverPolicy,
    RevisionStore,
    ScaleSpec,
    compare_versions,
    package_row,
)
from adjlink.package_store import atomic_write_json


DATA = Path(__file__).resolve().parents[1] / "data" / "capture.json"


def coordinate_factory(prefix: str, values_by_col: dict[int, int], row: int = 0):
    """Create A/B records where C[row, col] has exactly the requested value."""
    left = []
    right = []
    for index, (col, value) in enumerate(sorted(values_by_col.items())):
        process = index
        left.append([f"{prefix}-a-{col}", row, process, value])
        right.append([f"{prefix}-b-{col}", process, col, 1])
    return left, right


class SampleCaptureTests(unittest.TestCase):
    def setUp(self):
        self.pipeline = CorrectionPipeline.from_capture(DATA, apply_edits=True)

    def test_revisions_stale_base_and_missing_identity(self):
        self.assertEqual(["r1", "r2"], self.pipeline.store.order)
        codes = [report.code for report in self.pipeline.commit_reports]
        self.assertEqual(["ok", "stale_base"], codes)
        stale = self.pipeline.commit_reports[1]
        self.assertFalse(stale.accepted)
        self.assertEqual("r2", stale.readable_revision)
        self.assertTrue(any(i.code == "stale_base" for i in stale.issues))

        missing = self.pipeline.submit_edit(
            {"id": "bad", "base": "r2", "left_remove": ["does-not-exist"]}
        )
        self.assertFalse(missing.accepted)
        self.assertEqual("invalid_commit", missing.code)
        self.assertEqual("r2", missing.readable_revision)
        self.assertTrue(
            any(i.code == "unknown_contribution" for i in missing.issues)
        )

    def test_signed_product_retains_canceled_and_explicit_zeros(self):
        r1_view = self.pipeline.store.row_view(0, "r1")
        values = {c.col: c.value for c in r1_view.coordinates}
        # 2*2 + 1*2 + 3*(-2) = 0 before l-dupe/r-b are withdrawn in r2.
        self.assertEqual(0, values[1])
        coordinate = self.pipeline.store.get("r1").coordinate(0, 1)
        self.assertEqual("canceled", coordinate.zero_kind)
        self.assertGreaterEqual(len(coordinate.paths), 2)

        zero_coordinate = self.pipeline.store.get("r1").coordinate(0, 0)
        self.assertEqual(0, zero_coordinate.value)
        self.assertEqual("explicit_zero_path", zero_coordinate.zero_kind)

    def test_detector_is_stuck_on_row_zero_while_solver_advances(self):
        stuck = self.pipeline.stuck_rows("detector")
        self.assertEqual([0], [item["row"] for item in stuck])
        self.assertEqual("protected_overflow", stuck[0]["reason"][0])

        effective = self.pipeline.read_effective_row("detector", 0)
        self.assertTrue(effective.held)
        self.assertEqual("r1", effective.effective_revision)
        self.assertEqual({0: 0, 1: 0}, effective.package.values())

        deployment = self.pipeline.deployment("detector")
        self.assertEqual("receiver_atomic", deployment.mode)
        self.assertEqual("r1", deployment.effective_revision(1))
        self.assertIn(1, deployment.deferred_rows)
        row_wise = self.pipeline.row_wise_preview("detector")
        self.assertEqual("r2", row_wise.effective_revision(1))
        self.assertEqual("r1", row_wise.effective_revision(0))

        solver_row = self.pipeline.effective_receiver_row("solver", 0)
        self.assertEqual("r2", solver_row.revision)
        self.assertEqual(
            {0: 0, 1: 4, 3: 12, 4: 6, 5: 14},
            solver_row.values(),
        )

        trace = self.pipeline.trace_row("detector", 0)
        self.assertEqual("blocked", trace.state)
        self.assertTrue(trace.stuck)
        protected = set()
        for cause in trace.root_causes:
            protected.update(cause["protected_cols"])
        self.assertEqual({3, 5}, protected)
        self.assertEqual(1, len(trace.steps))
        step = trace.steps[0]
        right_ids = {item["id"] for item in step["right_inputs"]}
        self.assertIn("r-new", right_ids)

    def test_diff_separates_value_and_eligibility_only_changes(self):
        diff = self.pipeline.compare("r1", "r2", receiver_id="detector")
        changed = {
            (item.row, item.col): item
            for row in diff.rows
            for item in row.coordinate_diffs
        }
        recovered = changed[(0, 1)]
        self.assertTrue(recovered.numeric_changed)
        self.assertIn("recovered_from_zero", recovered.change_types)
        self.assertEqual(0, recovered.before_value)
        self.assertEqual(4, recovered.after_value)

        eligibility_only = {
            (item.row, item.col)
            for item in diff.eligibility_only_changes
        }
        self.assertIn((0, 0), eligibility_only)


class SpGEMMSemanticsTests(unittest.TestCase):
    def test_canceled_zero_recovers_after_identity_withdrawal(self):
        left = [
            ["a-plus", 0, 0, 2],
            ["a-minus", 0, 1, 3],
            ["a-zero", 1, 0, 0],
        ]
        right = [
            ["b-plus", 0, 5, 3],
            ["b-minus", 1, 5, -2],
        ]
        store = RevisionStore(
            initial_revision="r1", left_records=left, right_records=right
        )
        canceled = store.head.coordinate(0, 5)
        self.assertEqual(0, canceled.value)
        self.assertEqual("canceled", canceled.zero_kind)
        self.assertEqual(2, len(canceled.paths))

        explicit_zero = store.head.coordinate(1, 5)
        self.assertTrue(explicit_zero.present)
        self.assertEqual(0, explicit_zero.value)
        self.assertEqual("explicit_zero_path", explicit_zero.zero_kind)
        self.assertIsNone(store.head.coordinate(9, 5))
        self.assertTrue(store.has_active_contribution("left", "a-zero"))

        report = store.submit(
            {"id": "r2", "base": "r1", "left_remove": ["a-minus"]}
        )
        self.assertTrue(report.accepted)
        recovered = store.head.coordinate(0, 5)
        self.assertEqual(6, recovered.value)
        diff = compare_versions(store.get("r1"), store.get("r2"))
        coordinate_diff = next(
            d for row in diff.rows for d in row.numeric_changes if d.col == 5
        )
        self.assertIn("recovered_from_zero", coordinate_diff.change_types)

    def test_same_coordinate_identity_contributions_are_retained(self):
        left = [
            ["l1", 0, 0, 2],
            ["l2", 0, 0, 1],
        ]
        right = [["r1", 0, 3, 4]]
        store = RevisionStore(
            initial_revision="r1", left_records=left, right_records=right
        )
        coordinate = store.head.coordinate(0, 3)
        self.assertEqual(12, coordinate.value)
        self.assertEqual(2, len(coordinate.paths))
        self.assertEqual({"l1", "l2"}, {p.left_id for p in coordinate.paths})

        store.submit({"id": "r2", "base": "r1", "left_remove": ["l2"]})
        self.assertEqual(8, store.head.coordinate(0, 3).value)

    def test_shards_are_validated_and_readable(self):
        store = RevisionStore(
            initial_revision="r1",
            left_records=[
                ["a", 0, 0, 1],
                ["b", 1, 0, 1],
                ["c", 2, 0, 1],
            ],
            right_records=[["r", 0, 0, 1]],
            left_shards=[[0, 2], [2, 3]],
            right_shards=[[0, 1]],
        )
        first_shard_rows = [
            row for row, _ in store.iter_shard_rows("left", 0)
        ]
        self.assertEqual([0, 1], first_shard_rows)
        self.assertEqual((0, 2), store.head.left.shard_for_row(1))

        with self.assertRaises(ValueError):
            RevisionStore(
                initial_revision="bad",
                left_records=[["x", 5, 0, 1]],
                right_records=[],
                left_shards=[[0, 2]],
            )


class PackagingTests(unittest.TestCase):
    def _store(self):
        left, right = coordinate_factory(
            "tie", {0: 0, 1: 5, 2: -5, 3: 4}
        )
        return RevisionStore(
            initial_revision="r1", left_records=left, right_records=right
        )

    def test_required_zero_then_abs_order_and_tie_break(self):
        store = self._store()
        policy = ReceiverPolicy.from_dict(
            {
                "id": "tie",
                "capacity": 3,
                "protect_at": 100,
                "required": {"0": [0]},
            }
        )
        package = package_row(policy, 0, store.head)
        self.assertTrue(package.deliverable)
        self.assertEqual([0, 1, 2], [e.col for e in package.entries])
        self.assertEqual([3], [e.col for e in package.dropped])
        self.assertEqual(5, package.entries[1].abs_value)

    def test_required_overflow_is_hard_receiver_blocker(self):
        store = self._store()
        policy = ReceiverPolicy.from_dict(
            {
                "id": "bad",
                "capacity": 1,
                "protect_at": 100,
                "required": {"0": [0, 1]},
            }
        )
        package = package_row(policy, 0, store.head)
        self.assertFalse(package.deliverable)
        self.assertEqual("required_overflow", package.blockers[0].code)
        self.assertEqual(1, package.blockers[0].overflow_count)
        with self.assertRaises(RuntimeError):
            package.wire()

    def test_protected_overflow_is_distinct_from_weak_dropping(self):
        store = self._store()
        policy = ReceiverPolicy.from_dict(
            {
                "id": "protect",
                "capacity": 2,
                "protect_at": 4,
                "required": {"0": [0]},
            }
        )
        package = package_row(policy, 0, store.head)
        codes = {blocker.code for blocker in package.blockers}
        self.assertIn("protected_overflow", codes)
        self.assertEqual({1, 2, 3}, set(package.blockers[0].protected_cols))
        self.assertTrue(all(entry.droppable for entry in package.dropped))

    def test_discarded_coordinate_can_enter_after_competitor_removal(self):
        left, right = coordinate_factory("move", {1: 1, 2: 2})
        store = RevisionStore(
            initial_revision="r1", left_records=left, right_records=right
        )
        policy = ReceiverPolicy.from_dict(
            {
                "id": "cap",
                "capacity": 2,
                "protect_at": 10_000,
                "required": {"0": [0]},
            }
        )
        # Add an explicit zero required coordinate to the generated product.
        store.submit(
            {
                "id": "prep",
                "base": "r1",
                "left_add": [["a-zero", 0, 99, 0]],
                "right_add": [["b-zero", 99, 0, 1]],
            }
        )
        pipeline = CorrectionPipeline(store, [policy])
        before = pipeline.receiver_package_set("cap").rows[0]
        self.assertEqual([0, 2], [e.col for e in before.entries])
        self.assertIn(1, [e.col for e in before.dropped])

        # Find and withdraw the left contribution producing col 2.
        left_id = next(row[0] for row in left if row[2] == 1)
        outcome = pipeline.submit_edit(
            {"id": "r2", "base": "prep", "left_remove": [left_id]}
        )
        self.assertTrue(outcome.accepted)
        after = pipeline.effective_receiver_row("cap", 0)
        self.assertEqual([0, 1], [e.col for e in after.entries])

        diff = pipeline.compare("prep", "r2", receiver_id="cap")
        eligibility_only_pairs = {
            (item.row, item.col)
            for item in diff.eligibility_only_changes
        }
        self.assertIn((0, 1), eligibility_only_pairs)

    def test_int64_overflow_blocks_wrapped_wire_delivery(self):
        left = [["big-a", 0, 0, 1 << 40]]
        right = [["big-b", 0, 0, 1 << 40]]
        store = RevisionStore(
            initial_revision="r1", left_records=left, right_records=right
        )
        policy = ReceiverPolicy.from_dict(
            {"id": "big", "capacity": 5, "protect_at": 10}
        )
        package = package_row(policy, 0, store.head)
        self.assertEqual(1 << 80, package.mandatory[0].value)
        self.assertFalse(package.deliverable)
        self.assertEqual("int64_overflow", package.blockers[0].code)


class ScalePlanningTests(unittest.TestCase):
    def test_capacity_file_is_sparse_scale(self):
        capacity_path = DATA.parent / "capacity.json"
        scale = ScaleSpec.load(capacity_path)
        self.assertGreater(scale.m, 10_000)
        self.assertGreater(scale.n, 10_000)
        self.assertTrue(scale.requires_sparse_algorithm())
        self.assertLess(scale.budget.peak_scratch_bytes, scale.dense_product_bytes())


class IncrementalStorageTests(unittest.TestCase):
    def test_only_affected_row_packages_are_rebuilt_and_rewritten(self):
        left0, right0 = coordinate_factory("r0", {1: 3, 2: 4}, row=0)
        left1, right1 = coordinate_factory("r1", {3: 5, 4: 6}, row=1)
        store = RevisionStore(
            initial_revision="r1",
            left_records=left0 + left1,
            right_records=right0 + right1,
        )
        policy = ReceiverPolicy.from_dict(
            {"id": "inc", "capacity": 10, "protect_at": 10_000}
        )
        pipeline = CorrectionPipeline(store, [policy])
        old_set = pipeline.receiver_package_set("inc")
        old_row1_hash = old_set.rows[1].package_hash

        with tempfile.TemporaryDirectory() as tmp:
            blob_store = PackageBlobStore(Path(tmp))
            blob_store.publish_release(pipeline.current)
            old_packet_path = blob_store.packet_path(old_row1_hash)
            old_mtime_ns = old_packet_path.stat().st_mtime_ns

            outcome = pipeline.submit_edit(
                {
                    "id": "r2",
                    "base": "r1",
                    "left_add": [["new-a", 0, 99, 7]],
                    "right_add": [["new-b", 99, 9, 1]],
                }
            )
            self.assertEqual((0,), outcome.commit.affected_rows)
            new_set = pipeline.receiver_package_set("inc")
            self.assertEqual((0,), new_set.rebuilt_rows)
            self.assertIn(1, new_set.reused_rows)
            self.assertEqual(old_row1_hash, new_set.rows[1].package_hash)

            after = blob_store.publish_release(pipeline.current)
            self.assertIn(old_row1_hash, after["packets_reused"])
            self.assertNotIn(old_row1_hash, after["packets_written"])
            self.assertEqual(old_mtime_ns, old_packet_path.stat().st_mtime_ns)

            # Republish the same release: every packet is reused.
            second = blob_store.publish_release(pipeline.current)
            self.assertEqual([], second["packets_written"])
            self.assertIn(old_row1_hash, second["packets_reused"])
            self.assertTrue((Path(tmp) / "release.json").exists())
            current = blob_store.read_current("inc")
            self.assertEqual("r2", current["graph_revision"])

            # A future unrelated atomic write helper leaves valid JSON.
            target = Path(tmp) / "nested" / "x.json"
            atomic_write_json(target, {"ok": True})
            self.assertEqual({"ok": True}, json.loads(target.read_text()))


if __name__ == "__main__":
    unittest.main()
