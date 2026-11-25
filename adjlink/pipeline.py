"""High-level correction -> SpGEMM -> receiver row-package pipeline."""
from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from threading import RLock
from types import MappingProxyType
from typing import Any, Mapping

from .budget import WorkspaceBudget
from .deployment import HeldRow, RowDeployment, advance_deployment
from .diff import VersionDiff, compare_versions
from .engine import CommitReport, GraphVersion, RevisionStore
from .packages import ReceiverPackageSet, ReceiverPolicy, package_revision
from .tracing import TraceReport, find_stuck_rows, trace_stuck_row


@dataclass(frozen=True, slots=True)
class Release:
    """One complete, immutable graph plus all receiver packaging results."""

    graph: GraphVersion
    packages: Mapping[str, ReceiverPackageSet]
    deployments: Mapping[str, RowDeployment]


@dataclass(frozen=True, slots=True)
class PipelineSubmission:
    commit: CommitReport
    release: Release

    @property
    def accepted(self) -> bool:
        return self.commit.accepted

    @property
    def code(self) -> str:
        return self.commit.code

    @property
    def issues(self):
        return self.commit.issues

    @property
    def readable_revision(self) -> str:
        return self.commit.readable_revision

    @property
    def affected_rows(self):
        return self.commit.affected_rows

    @property
    def revision_id(self):
        return self.commit.revision_id

    @property
    def version(self):
        return self.commit.version

    def to_dict(self) -> dict[str, Any]:
        return {
            "commit": self.commit.to_dict(),
            "release_revision": self.release.graph.id,
            "receivers": {
                receiver_id: {
                    "ready": package_set.ready,
                    "blocked_rows": list(package_set.blocked_rows),
                    "deployment": self.release.deployments[receiver_id].to_dict(),
                }
                for receiver_id, package_set in self.release.packages.items()
            },
        }


@dataclass(frozen=True, slots=True)
class EffectiveReceiverRow:
    """The packet a receiver is using plus dependencies at its content revision."""

    receiver_id: str
    row: int
    target_revision: str
    effective_revision: str
    held: bool
    package: Any
    view: Any
    hold: HeldRow | None

    def to_dict(self, include_dependencies: bool = True) -> dict[str, Any]:
        return {
            "receiver_id": self.receiver_id,
            "row": self.row,
            "target_revision": self.target_revision,
            "effective_revision": self.effective_revision,
            "held": self.held,
            "hold": self.hold.to_dict() if self.hold else None,
            "package": self.package.to_dict() if self.package else None,
            "row_view": self.view.to_dict(include_dependencies=include_dependencies),
            "dependency_summary": self.view.dependency_summary.to_dict(),
        }


class CorrectionPipeline:
    """Reusable entry point for versioned signed SpGEMM row delivery."""

    def __init__(
        self,
        store: RevisionStore,
        receivers: list[ReceiverPolicy | Mapping[str, Any]]
        | tuple[ReceiverPolicy | Mapping[str, Any], ...] = (),
    ) -> None:
        self.store = store
        self.policies: dict[str, ReceiverPolicy] = {}
        self.commit_reports: list[CommitReport] = []
        self._lock = RLock()
        packages: dict[str, ReceiverPackageSet] = {}
        deployments: dict[str, RowDeployment] = {}
        for receiver in receivers:
            policy = (
                receiver
                if isinstance(receiver, ReceiverPolicy)
                else ReceiverPolicy.from_dict(receiver)
            )
            if policy.id in self.policies:
                raise ValueError(f"duplicate receiver id: {policy.id}")
            self.policies[policy.id] = policy
            package_set = package_revision(policy, store.head)
            packages[policy.id] = package_set
            deployments[policy.id] = advance_deployment(None, package_set)
        self._release = Release(
            graph=store.head,
            packages=MappingProxyType(packages),
            deployments=MappingProxyType(deployments),
        )

    @property
    def current(self) -> Release:
        return self._release

    @property
    def head(self) -> GraphVersion:
        return self._release.graph

    @property
    def head_revision(self) -> str:
        return self._release.graph.id

    @property
    def budget(self) -> WorkspaceBudget:
        return self.store.budget

    @classmethod
    def from_capture(
        cls,
        payload: str | Path | Mapping[str, Any],
        *,
        capacity: Mapping[str, Any] | str | Path | None = None,
        apply_edits: bool = True,
    ) -> "CorrectionPipeline":
        """Build from capture.json-like data and optionally replay its edits."""
        if isinstance(payload, (str, Path)):
            data: Any = json.loads(
                Path(payload).read_text(encoding="utf-8")
            )
        elif isinstance(payload, tuple) and len(payload) == 2:
            # Accept the (records, arrays) tuple returned by caseio.load_case.
            data = payload[0]
        else:
            data = payload

        records: Mapping[str, Any]
        if (
            isinstance(data, Mapping)
            and "records" in data
            and isinstance(data["records"], Mapping)
            and "left" not in data
        ):
            records = data["records"]
        else:
            records = data

        capacity_data: Mapping[str, Any] | None = None
        if isinstance(capacity, (str, Path)):
            capacity_data = json.loads(
                Path(capacity).read_text(encoding="utf-8")
            )
        elif capacity is not None:
            capacity_data = capacity

        budget = None
        if capacity_data is not None:
            budget = WorkspaceBudget(
                int(capacity_data.get("peak_scratch_bytes", 8 * 1024 * 1024))
            )

        shards = records.get("shards", {}) or {}
        store = RevisionStore(
            initial_revision=records.get("revision", "r0"),
            left_records=records.get("left", ()),
            right_records=records.get("right", ()),
            left_shards=shards.get("left"),
            right_shards=shards.get("right"),
            shape=records.get("shape"),
            budget=budget,
        )
        receiver_data = records.get("receivers")
        policies = [
            ReceiverPolicy.from_dict(receiver)
            for receiver in (() if receiver_data is None else receiver_data)
        ]
        pipeline = cls(store, policies)
        if apply_edits:
            edit_data = records.get("edits")
            for edit in () if edit_data is None else edit_data:
                pipeline.submit_edit(edit)
        return pipeline

    def add_receiver(self, receiver: ReceiverPolicy | Mapping[str, Any]) -> Release:
        policy = (
            receiver
            if isinstance(receiver, ReceiverPolicy)
            else ReceiverPolicy.from_dict(receiver)
        )
        with self._lock:
            if policy.id in self.policies:
                raise ValueError(f"receiver already exists: {policy.id}")
            self.policies[policy.id] = policy
            package_set = package_revision(policy, self.store.head)
            packages = dict(self._release.packages)
            deployments = dict(self._release.deployments)
            packages[policy.id] = package_set
            deployments[policy.id] = advance_deployment(None, package_set)
            self._release = Release(
                graph=self.store.head,
                packages=MappingProxyType(packages),
                deployments=MappingProxyType(deployments),
            )
            return self._release

    def submit_edit(self, edit: Mapping[str, Any]) -> PipelineSubmission:
        with self._lock:
            old_release = self._release
            commit = self.store.submit(edit)
            self.commit_reports.append(commit)
            if not commit.accepted or commit.version is None:
                # No graph or row package is replaced: readers retain the
                # previous complete readable release.
                return PipelineSubmission(commit=commit, release=old_release)

            graph = commit.version
            packages: dict[str, ReceiverPackageSet] = {}
            deployments: dict[str, RowDeployment] = {}
            for receiver_id, policy in self.policies.items():
                previous_set = old_release.packages[receiver_id]
                package_set = package_revision(
                    policy,
                    graph,
                    previous=previous_set,
                    changed_rows=commit.affected_rows,
                )
                packages[receiver_id] = package_set
                deployments[receiver_id] = advance_deployment(
                    old_release.deployments[receiver_id], package_set
                )

            new_release = Release(
                graph=graph,
                packages=MappingProxyType(packages),
                deployments=MappingProxyType(deployments),
            )
            self._release = new_release
            return PipelineSubmission(commit=commit, release=new_release)

    # Reading -----------------------------------------------------------------

    def graph_row(self, row: int, revision: str | None = None):
        return self.store.row_view(row, revision)

    def dependency_summary(self, row: int, revision: str | None = None):
        return self.graph_row(row, revision).dependency_summary

    def receiver_package_set(self, receiver_id: str) -> ReceiverPackageSet:
        return self._release.packages[receiver_id]

    def deployment(self, receiver_id: str) -> RowDeployment:
        return self._release.deployments[receiver_id]

    def row_wise_preview(self, receiver_id: str) -> RowDeployment:
        """Preview promoting healthy rows while pinning blocked rows."""
        return advance_deployment(
            self._release.deployments[receiver_id],
            self._release.packages[receiver_id],
            mode="row_wise",
        )

    def effective_receiver_row(self, receiver_id: str, row: int) -> Any:
        return self._release.deployments[receiver_id].rows.get(int(row))

    def read_effective_row(
        self, receiver_id: str, row: int
    ) -> EffectiveReceiverRow:
        row = int(row)
        deployment = self._release.deployments[receiver_id]
        package = deployment.rows.get(row)
        hold = deployment.held.get(row)
        effective_revision = package.revision if package is not None else self.head.id
        view = self.store.row_view(row, effective_revision)
        return EffectiveReceiverRow(
            receiver_id=receiver_id,
            row=row,
            target_revision=self.head.id,
            effective_revision=effective_revision,
            held=hold is not None,
            package=package,
            view=view,
            hold=hold,
        )

    # Comparison and tracing --------------------------------------------------

    def compare(
        self,
        old_revision: str,
        new_revision: str | None = None,
        *,
        receiver_id: str | None = None,
    ) -> VersionDiff:
        after = self.store.get(new_revision)
        before = self.store.get(old_revision)
        policy = self.policies[receiver_id] if receiver_id is not None else None
        after_packages = None
        if receiver_id is not None and after.id == self.head.id:
            after_packages = self._release.packages[receiver_id].rows
        return compare_versions(
            before,
            after,
            receiver=policy,
            after_package=after_packages,
        )

    def trace_row(
        self,
        receiver_id: str,
        row: int,
        *,
        old_revision: str | None = None,
        target_revision: str | None = None,
        old_package=None,
    ) -> TraceReport:
        policy = self.policies[receiver_id]
        target = self.store.get(target_revision)
        deployment = self._release.deployments[receiver_id]
        return trace_stuck_row(
            self.store,
            policy,
            int(row),
            old_revision=old_revision,
            target_revision=target.id,
            old_package=old_package,
            deployment=deployment if target.id == self.head.id else None,
        )

    def stuck_rows(self, receiver_id: str) -> list[dict[str, Any]]:
        policy = self.policies[receiver_id]
        package_set = self._release.packages[receiver_id]
        deployment = self._release.deployments[receiver_id]
        return find_stuck_rows(
            policy,
            self.head,
            package_set.rows,
            deployment,
        )

    def write_packages(self, root: str | Path) -> dict[str, Any]:
        from .package_store import PackageBlobStore

        return PackageBlobStore(root).publish_release(self._release)
