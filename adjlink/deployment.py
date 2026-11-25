"""Effective receiver rows, including held old rows for blocked revisions."""
from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping

from .packages import Blocker, ReceiverPackageSet, RowPackage


@dataclass(frozen=True, slots=True)
class HeldRow:
    row: int
    target_revision: str
    old_revision: str | None
    old_package: RowPackage | None
    blockers: tuple[Blocker, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "row": self.row,
            "target_revision": self.target_revision,
            "old_revision": self.old_revision,
            "blockers": [b.to_dict() for b in self.blockers],
            "old_wire": [e.to_wire() for e in self.old_package.entries]
            if self.old_package and self.old_package.deliverable
            else None,
        }


@dataclass(frozen=True, slots=True)
class RowDeployment:
    """The rows a receiver is actually using.

    The default ``receiver_atomic`` mode keeps the whole receiver on its last
    complete good version when any target row is blocked.  The ``row_wise``
    mode is also available for fleets that can advance healthy rows while
    pinning only the blocked row.
    """

    receiver_id: str
    target_revision: str
    last_full_revision: str | None
    rows: Mapping[int, RowPackage]
    held: Mapping[int, HeldRow]
    mode: str = "receiver_atomic"
    deferred_rows: tuple[int, ...] = ()

    @property
    def fully_current(self) -> bool:
        return not self.held and self.last_full_revision == self.target_revision

    def effective_revision(self, row: int) -> str | None:
        package = self.rows.get(row)
        return package.revision if package else None

    def to_dict(self) -> dict[str, Any]:
        return {
            "receiver_id": self.receiver_id,
            "target_revision": self.target_revision,
            "last_full_revision": self.last_full_revision,
            "fully_current": self.fully_current,
            "mode": self.mode,
            "deferred_rows": list(self.deferred_rows),
            "held_rows": {str(row): held.to_dict() for row, held in sorted(self.held.items())},
            "rows": {
                str(row): {
                    "revision": pkg.revision,
                    "status": pkg.status,
                    "deliverable": pkg.deliverable,
                    "wire_hash": pkg.wire_hash,
                    "package_hash": pkg.package_hash,
                    "wire": [e.to_wire() for e in pkg.entries]
                    if pkg.deliverable
                    else None,
                }
                for row, pkg in sorted(self.rows.items())
            },
        }


def advance_deployment(
    previous: RowDeployment | None,
    target: ReceiverPackageSet,
    *,
    mode: str = "receiver_atomic",
) -> RowDeployment:
    if mode not in {"receiver_atomic", "row_wise"}:
        raise ValueError("mode must be receiver_atomic or row_wise")

    target_blocked = {
        row: package for row, package in target.rows.items() if package.blocked
    }

    # A deliverable target means both modes promote the complete target set.
    if not target_blocked:
        return RowDeployment(
            receiver_id=target.receiver_id,
            target_revision=target.revision,
            last_full_revision=target.revision,
            rows=target.rows,
            held=MappingProxyType({}),
            mode=mode,
            deferred_rows=(),
        )

    held: dict[int, HeldRow] = {}
    for row, package in target_blocked.items():
        old_package = previous.rows.get(row) if previous else None
        held[row] = HeldRow(
            row=row,
            target_revision=target.revision,
            old_revision=old_package.revision if old_package else None,
            old_package=old_package,
            blockers=package.blockers,
        )

    if mode == "receiver_atomic":
        # The receiver does not switch version.  Keep every last-good row,
        # while recording which row(s) caused the switch to be deferred.
        effective: dict[int, RowPackage] = dict(previous.rows) if previous else {}
        deferred: list[int] = []
        for row, package in target.rows.items():
            if package.blocked:
                continue
            old_package = effective.get(row)
            if (
                old_package is None
                or old_package.package_hash != package.package_hash
            ):
                deferred.append(row)
        return RowDeployment(
            receiver_id=target.receiver_id,
            target_revision=target.revision,
            last_full_revision=previous.last_full_revision if previous else None,
            rows=MappingProxyType(effective),
            held=MappingProxyType(held),
            mode=mode,
            deferred_rows=tuple(sorted(deferred)),
        )

    # Row-wise opt-in: promote healthy rows, pin only blocked old rows.
    effective = dict(previous.rows) if previous else {}
    for row, package in target.rows.items():
        if package.deliverable:
            effective[row] = package
    return RowDeployment(
        receiver_id=target.receiver_id,
        target_revision=target.revision,
        last_full_revision=previous.last_full_revision if previous else None,
        rows=MappingProxyType(effective),
        held=MappingProxyType(held),
        mode=mode,
        deferred_rows=(),
    )
