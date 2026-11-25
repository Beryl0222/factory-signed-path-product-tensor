"""Revision comparison: numeric changes versus receiver-eligibility-only changes."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .engine import GraphVersion
from .packages import ReceiverPolicy, RowPackage, package_row


@dataclass(frozen=True, slots=True)
class CoordinateDiff:
    row: int
    col: int
    before_present: bool
    after_present: bool
    before_value: int | None
    after_value: int | None
    change_types: tuple[str, ...]
    numeric_changed: bool
    presence_changed: bool
    dependencies_changed: bool
    eligibility_before: Mapping[str, Any] | None
    eligibility_after: Mapping[str, Any] | None
    eligibility_changed: bool
    eligibility_only: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "row": self.row,
            "col": self.col,
            "before_present": self.before_present,
            "after_present": self.after_present,
            "before_value": self.before_value,
            "after_value": self.after_value,
            "change_types": list(self.change_types),
            "numeric_changed": self.numeric_changed,
            "presence_changed": self.presence_changed,
            "dependencies_changed": self.dependencies_changed,
            "eligibility_before": dict(self.eligibility_before)
            if self.eligibility_before
            else None,
            "eligibility_after": dict(self.eligibility_after)
            if self.eligibility_after
            else None,
            "eligibility_changed": self.eligibility_changed,
            "eligibility_only": self.eligibility_only,
        }


@dataclass(frozen=True, slots=True)
class RowDiff:
    row: int
    affected: bool
    coordinate_diffs: tuple[CoordinateDiff, ...]
    numeric_changes: tuple[CoordinateDiff, ...]
    eligibility_only_changes: tuple[CoordinateDiff, ...]
    before_row_status: str | None
    after_row_status: str | None
    before_deliverable: bool | None
    after_deliverable: bool | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "row": self.row,
            "affected": self.affected,
            "coordinate_diffs": [d.to_dict() for d in self.coordinate_diffs],
            "numeric_changes": [d.to_dict() for d in self.numeric_changes],
            "eligibility_only_changes": [
                d.to_dict() for d in self.eligibility_only_changes
            ],
            "before_row_status": self.before_row_status,
            "after_row_status": self.after_row_status,
            "before_deliverable": self.before_deliverable,
            "after_deliverable": self.after_deliverable,
        }


@dataclass(frozen=True, slots=True)
class VersionDiff:
    before_revision: str
    after_revision: str
    receiver_id: str | None
    rows: tuple[RowDiff, ...]

    @property
    def numeric_changes(self) -> tuple[CoordinateDiff, ...]:
        return tuple(d for row in self.rows for d in row.numeric_changes)

    @property
    def eligibility_only_changes(self) -> tuple[CoordinateDiff, ...]:
        return tuple(d for row in self.rows for d in row.eligibility_only_changes)

    @property
    def changed_rows(self) -> tuple[int, ...]:
        return tuple(row.row for row in self.rows if row.coordinate_diffs)

    def to_dict(self) -> dict[str, Any]:
        return {
            "before_revision": self.before_revision,
            "after_revision": self.after_revision,
            "receiver_id": self.receiver_id,
            "changed_rows": list(self.changed_rows),
            "numeric_changes": [d.to_dict() for d in self.numeric_changes],
            "eligibility_only_changes": [
                d.to_dict() for d in self.eligibility_only_changes
            ],
            "rows": [r.to_dict() for r in self.rows],
        }


def _eligibility_equal(
    before: Mapping[str, Any] | None, after: Mapping[str, Any] | None
) -> bool:
    if before is None or after is None:
        return before is after
    keys = ("status", "selected", "mandatory", "droppable", "protected", "present")
    return all(before.get(k) == after.get(k) for k in keys)


def compare_versions(
    before: GraphVersion,
    after: GraphVersion,
    *,
    receiver: ReceiverPolicy | None = None,
    before_package: Mapping[int, RowPackage] | None = None,
    after_package: Mapping[int, RowPackage] | None = None,
) -> VersionDiff:
    row_numbers = set(before.product_rows) | set(after.product_rows)
    if receiver is not None:
        row_numbers.update(receiver.required)
    if before_package is not None:
        row_numbers.update(before_package)
    if after_package is not None:
        row_numbers.update(after_package)

    row_diffs: list[RowDiff] = []
    affected_rows = set(after.affected_rows) | set(before.affected_rows)

    for row in sorted(row_numbers):
        before_row = before.product_rows.get(row, {})
        after_row = after.product_rows.get(row, {})
        before_pkg: RowPackage | None = None
        after_pkg: RowPackage | None = None
        if receiver is not None:
            before_pkg = (
                before_package.get(row)
                if before_package is not None
                else package_row(receiver, row, before)
            )
            after_pkg = (
                after_package.get(row)
                if after_package is not None
                else package_row(receiver, row, after)
            )

        cols = set(before_row) | set(after_row)
        if receiver is not None:
            cols.update(receiver.required.get(row, ()))
        if before_pkg is not None:
            cols.update(before_pkg.eligibility())
        if after_pkg is not None:
            cols.update(after_pkg.eligibility())

        coordinate_diffs: list[CoordinateDiff] = []
        before_eligibility_map = before_pkg.eligibility() if before_pkg else {}
        after_eligibility_map = after_pkg.eligibility() if after_pkg else {}

        for col in sorted(cols):
            b = before_row.get(col)
            a = after_row.get(col)
            b_present = b is not None
            a_present = a is not None
            b_value = b.value if b is not None else None
            a_value = a.value if a is not None else None
            types: list[str] = []

            if not b_present and a_present:
                types.append("appeared")
                if a_value == 0:
                    types.append("appeared_zero")
            elif b_present and not a_present:
                types.append("disappeared")
            elif b_present and a_present:
                if b_value != a_value:
                    types.append("value_changed")
                    if b_value == 0 and a_value not in (None, 0):
                        types.append("recovered_from_zero")
                    if b_value not in (None, 0) and a_value == 0:
                        types.append("canceled_to_zero")
                if b.zero_kind != a.zero_kind:
                    types.append("zero_kind_changed")
                if b.dependency_digest != a.dependency_digest:
                    types.append("dependencies_changed")

            numeric_changed = b_present and a_present and b_value != a_value
            presence_changed = b_present != a_present
            dependencies_changed = (
                b_present
                and a_present
                and b.dependency_digest != a.dependency_digest
            )

            e_before = before_eligibility_map.get(col)
            e_after = after_eligibility_map.get(col)
            eligibility_changed = not _eligibility_equal(e_before, e_after)
            eligibility_only = (
                receiver is not None
                and eligibility_changed
                and not numeric_changed
                and not presence_changed
            )

            # Keep truly unchanged coordinates out of the detailed diff.
            if (
                not types
                and not eligibility_changed
                and not presence_changed
            ):
                continue

            coordinate_diffs.append(
                CoordinateDiff(
                    row=row,
                    col=col,
                    before_present=b_present,
                    after_present=a_present,
                    before_value=b_value,
                    after_value=a_value,
                    change_types=tuple(types),
                    numeric_changed=numeric_changed,
                    presence_changed=presence_changed,
                    dependencies_changed=dependencies_changed,
                    eligibility_before=e_before,
                    eligibility_after=e_after,
                    eligibility_changed=eligibility_changed,
                    eligibility_only=eligibility_only,
                )
            )

        numeric_changes = tuple(d for d in coordinate_diffs if d.numeric_changed)
        eligibility_only_changes = tuple(
            d for d in coordinate_diffs if d.eligibility_only
        )
        if not coordinate_diffs and row not in affected_rows:
            continue

        row_diffs.append(
            RowDiff(
                row=row,
                affected=row in affected_rows,
                coordinate_diffs=tuple(coordinate_diffs),
                numeric_changes=numeric_changes,
                eligibility_only_changes=eligibility_only_changes,
                before_row_status=before_pkg.status if before_pkg else None,
                after_row_status=after_pkg.status if after_pkg else None,
                before_deliverable=before_pkg.deliverable if before_pkg else None,
                after_deliverable=after_pkg.deliverable if after_pkg else None,
            )
        )

    return VersionDiff(
        before_revision=before.id,
        after_revision=after.id,
        receiver_id=receiver.id if receiver else None,
        rows=tuple(row_diffs),
    )
