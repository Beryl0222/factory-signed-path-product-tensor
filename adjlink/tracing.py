"""Trace a held model row back through revisions to correction identities."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .deployment import RowDeployment
from .engine import GraphVersion, RevisionStore
from .model import digest_canonical
from .packages import ReceiverPolicy, RowPackage, package_row


@dataclass(frozen=True, slots=True)
class TraceReport:
    receiver_id: str
    row: int
    state: str
    target_revision: str
    old_revision: str | None
    stuck: bool
    reason: str | None
    target_package: dict[str, Any]
    old_package: dict[str, Any] | None
    steps: tuple[dict[str, Any], ...]
    root_causes: tuple[dict[str, Any], ...]
    current_dependencies: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "receiver_id": self.receiver_id,
            "row": self.row,
            "state": self.state,
            "stuck": self.stuck,
            "reason": self.reason,
            "target_revision": self.target_revision,
            "old_revision": self.old_revision,
            "target_package": self.target_package,
            "old_package": self.old_package,
            "steps": list(self.steps),
            "root_causes": list(self.root_causes),
            "current_dependencies": self.current_dependencies,
        }


def _package_brief(pkg: RowPackage | Mapping[str, Any] | None) -> dict[str, Any] | None:
    if pkg is None:
        return None
    if isinstance(pkg, Mapping):
        result = dict(pkg)
        result.setdefault("supplied_from_wire_record", True)
        return result
    return {
        "revision": pkg.revision,
        "status": pkg.status,
        "deliverable": pkg.deliverable,
        "capacity": pkg.capacity,
        "wire": [e.to_wire() for e in pkg.entries] if pkg.deliverable else None,
        "selected_cols": [e.col for e in pkg.entries] if pkg.deliverable else [],
        "mandatory_cols": [e.col for e in pkg.mandatory],
        "dropped": [
            {"col": e.col, "value": e.value, "abs_value": e.abs_value}
            for e in pkg.dropped
        ],
        "blockers": [b.to_dict() for b in pkg.blockers],
        "wire_hash": pkg.wire_hash,
        "dependency_digest": pkg.dependency_digest,
    }


def _coordinate_transition(
    before: GraphVersion, after: GraphVersion, row: int
) -> list[dict[str, Any]]:
    before_row = before.product_rows.get(row, {})
    after_row = after.product_rows.get(row, {})
    result: list[dict[str, Any]] = []
    for col in sorted(set(before_row) | set(after_row)):
        b = before_row.get(col)
        a = after_row.get(col)
        result.append(
            {
                "col": col,
                "before_present": b is not None,
                "after_present": a is not None,
                "before_value": None if b is None else b.value,
                "after_value": None if a is None else a.value,
                "before_zero_kind": None if b is None else b.zero_kind,
                "after_zero_kind": None if a is None else a.zero_kind,
                "before_dependency_digest": None if b is None else b.dependency_digest,
                "after_dependency_digest": None if a is None else a.dependency_digest,
            }
        )
    return result


def _revision_chain(
    store: RevisionStore, old_revision: str, target_revision: str
) -> list[str]:
    chain: list[str] = []
    current = store.get(target_revision)
    guard = 0
    while current.id != old_revision:
        chain.append(current.id)
        if current.parent_id is None:
            raise KeyError(f"{old_revision} is not an ancestor of {target_revision}")
        current = store.get(current.parent_id)
        guard += 1
        if guard > len(store.versions) + 1:
            raise RuntimeError("revision history cycle")
    chain.reverse()
    return chain


def trace_stuck_row(
    store: RevisionStore,
    policy: ReceiverPolicy,
    row: int,
    *,
    old_revision: str | None = None,
    target_revision: str | None = None,
    old_package: RowPackage | Mapping[str, Any] | None = None,
    deployment: RowDeployment | None = None,
) -> TraceReport:
    target = store.get(target_revision)
    target_pkg = package_row(policy, int(row), target)

    if old_package is None and deployment is not None:
        old_package = deployment.rows.get(int(row))

    old_wire_hash: str | None = None
    if isinstance(old_package, RowPackage):
        if old_revision is None:
            old_revision = old_package.revision
        old_wire_hash = old_package.wire_hash
    elif isinstance(old_package, Mapping):
        if old_revision is None:
            old_revision = (
                old_package.get("revision")
                or old_package.get("content_revision")
                or old_package.get("effective_revision")
            )
        old_wire_hash = old_package.get("wire_hash")
        if old_wire_hash is None and old_package.get("wire") is not None:
            old_wire_hash = digest_canonical(list(old_package["wire"]), "wire-")

    old_version: GraphVersion | None = None
    if old_revision is not None and old_revision in store.versions:
        old_version = store.get(old_revision)
        if old_package is None:
            old_package = package_row(policy, int(row), old_version)

    steps: list[dict[str, Any]] = []
    if old_version is not None and old_version.id != target.id:
        for revision_id in _revision_chain(store, old_version.id, target.id):
            version = store.get(revision_id)
            if int(row) not in version.affected_rows or version.change is None:
                continue
            parent = store.get(version.parent_id) if version.parent_id else old_version
            before_pkg = package_row(policy, int(row), parent)
            after_pkg = package_row(policy, int(row), version)
            change = version.change

            left_inputs = [
                {"op": "add", **c.to_dict()}
                for c in change.left_added
                if c.row == row
            ] + [
                {"op": "remove", **c.to_dict()}
                for c in change.left_removed
                if c.row == row
            ]

            right_inputs: list[dict[str, Any]] = []
            for op, contributions in (
                ("add", change.right_added),
                ("remove", change.right_removed),
            ):
                for contribution in contributions:
                    process = contribution.row
                    old_links = parent.left_by_process.get(process, ())
                    new_links = version.left_by_process.get(process, ())
                    if row not in old_links and row not in new_links:
                        continue
                    right_inputs.append(
                        {
                            "op": op,
                            **contribution.to_dict(),
                            "process": process,
                            "left_coordinate_col": process,
                            "left_ids_before": list(
                                parent.left.ids_at(row, process)
                            ),
                            "left_ids_after": list(
                                version.left.ids_at(row, process)
                            ),
                        }
                    )

            steps.append(
                {
                    "revision": version.id,
                    "base": change.base,
                    "affected_row": row in version.affected_rows,
                    "left_inputs": left_inputs,
                    "right_inputs": right_inputs,
                    "coordinate_transitions": _coordinate_transition(
                        parent, version, row
                    ),
                    "package_before": _package_brief(before_pkg),
                    "package_after": _package_brief(after_pkg),
                }
            )

    root_causes: list[dict[str, Any]] = []
    for blocker in target_pkg.blockers:
        cols = set(blocker.required_cols) | set(blocker.protected_cols) | set(
            blocker.out_of_range_cols
        )
        coordinates: list[dict[str, Any]] = []
        for col in sorted(cols):
            coordinate = target.coordinate(row, col)
            entry = next((e for e in target_pkg.mandatory if e.col == col), None)
            coordinates.append(
                {
                    "col": col,
                    "value": None if coordinate is None else coordinate.value,
                    "present": coordinate is not None,
                    "zero_kind": None
                    if coordinate is None
                    else coordinate.zero_kind,
                    "classification": None if entry is None else entry.status,
                    "paths": []
                    if coordinate is None
                    else [p.to_dict() for p in coordinate.paths],
                }
            )
        root_causes.append({**blocker.to_dict(), "coordinates": coordinates})

    if target_pkg.blockers:
        state = "blocked"
        reason = "; ".join(b.code for b in target_pkg.blockers)
        stuck = True
    elif old_wire_hash is not None and old_wire_hash != target_pkg.wire_hash:
        state = "lagging_readable"
        reason = "target row is deliverable but receiver is still using an old packet"
        stuck = True
    elif old_package is not None and old_wire_hash is None:
        state = "unknown_old_packet"
        reason = "old packet did not include a comparable wire hash or wire"
        stuck = False
    elif old_package is None:
        state = "no_old_package"
        reason = "no old row packet was supplied"
        stuck = False
    else:
        state = "current"
        reason = None
        stuck = False

    view = target.row_view(row)
    return TraceReport(
        receiver_id=policy.id,
        row=row,
        state=state,
        stuck=stuck,
        reason=reason,
        target_revision=target.id,
        old_revision=old_revision,
        target_package=_package_brief(target_pkg) or {},
        old_package=_package_brief(old_package),
        steps=tuple(steps),
        root_causes=tuple(root_causes),
        current_dependencies=view.dependency_summary.to_dict(),
    )


def find_stuck_rows(
    policy: ReceiverPolicy,
    target_revision: GraphVersion,
    target_packages: Mapping[int, RowPackage],
    deployment: RowDeployment | None = None,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in sorted(target_packages):
        package = target_packages[row]
        if package.blocked:
            held = deployment.held.get(row) if deployment else None
            result.append(
                {
                    "receiver_id": policy.id,
                    "row": row,
                    "state": "blocked",
                    "reason": [b.code for b in package.blockers],
                    "old_revision": held.old_revision if held else None,
                }
            )
        elif deployment is not None and deployment.mode != "receiver_atomic":
            current = deployment.rows.get(row)
            if current is None or current.wire_hash != package.wire_hash:
                result.append(
                    {
                        "receiver_id": policy.id,
                        "row": row,
                        "state": "lagging_readable",
                        "reason": ["old wire hash does not match target"],
                        "old_revision": current.revision if current else None,
                    }
                )
    if deployment is not None:
        for row, held in sorted(deployment.held.items()):
            if not any(item["row"] == row for item in result):
                result.append(
                    {
                        "receiver_id": policy.id,
                        "row": row,
                        "state": "blocked",
                        "reason": [b.code for b in held.blockers],
                        "old_revision": held.old_revision,
                    }
                )
    return result
