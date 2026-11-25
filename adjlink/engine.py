"""Versioned, incremental signed SpGEMM engine.

The matrices are stored as sparse row maps.  A product coordinate retains every
join path as a dependency, so a zero produced by cancellation remains a real
coordinate and can recover after a withdrawal/addition.
"""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from threading import RLock
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Sequence

from .model import (
    Contribution,
    DependencySummary,
    PathContribution,
    ProductCoordinate,
    RowView,
    CoordinateView,
)
from .budget import WorkspaceBudget


Catalog = dict[tuple[str, str], Contribution]
ImmutableRowMap = Mapping[int, "SourceRow"]


@dataclass(frozen=True, slots=True)
class SourceRow:
    row: int
    coordinates: Mapping[int, tuple[str, ...]]

    def ids(self, col: int) -> tuple[str, ...]:
        return self.coordinates.get(col, ())

    def all_ids(self) -> tuple[str, ...]:
        result: list[str] = []
        for ids in self.coordinates.values():
            result.extend(ids)
        return tuple(result)


@dataclass(frozen=True, slots=True)
class SourceMatrix:
    side: str
    shards: tuple[tuple[int, int], ...]
    rows: Mapping[int, SourceRow]

    def shard_for_row(self, row: int) -> tuple[int, int] | None:
        for interval in self.shards:
            start, end = interval
            if start <= row < end:
                return interval
        return None

    def ids_at(self, row: int, col: int) -> tuple[str, ...]:
        source_row = self.rows.get(row)
        return () if source_row is None else source_row.ids(col)

    @classmethod
    def from_records(
        cls,
        side: str,
        records: Iterable[Sequence[Any]],
        shards: Sequence[Sequence[int]] | Mapping[str, Sequence[int]] | None = None,
    ) -> tuple["SourceMatrix", tuple[Contribution, ...]]:
        if side not in {"left", "right"}:
            raise ValueError("side must be 'left' or 'right'")
        intervals = normalize_shards(shards)
        grouped: dict[int, dict[int, list[str]]] = {}
        contributions: list[Contribution] = []
        seen: set[str] = set()
        for record in () if records is None else records:
            contribution = contribution_from_record(side, record)
            if contribution.id in seen:
                raise ValueError(f"duplicate {side} contribution id: {contribution.id}")
            seen.add(contribution.id)
            if intervals and not any(a <= contribution.row < b for a, b in intervals):
                raise ValueError(
                    f"{side} contribution {contribution.id} row "
                    f"{contribution.row} is outside every shard"
                )
            grouped.setdefault(contribution.row, {}).setdefault(
                contribution.col, []
            ).append(contribution.id)
            contributions.append(contribution)

        rows: dict[int, SourceRow] = {}
        for row, coords in grouped.items():
            frozen_coords = {
                col: tuple(sorted(ids)) for col, ids in sorted(coords.items())
            }
            rows[row] = SourceRow(row, MappingProxyType(frozen_coords))
        matrix = cls(side=side, shards=intervals, rows=MappingProxyType(rows))
        return matrix, tuple(contributions)


def normalize_shards(
    shards: Sequence[Sequence[int]] | Mapping[str, Sequence[int]] | None,
) -> tuple[tuple[int, int], ...]:
    if shards is None:
        return ()
    intervals: list[tuple[int, int]] = []
    if isinstance(shards, Mapping):
        # Names such as "left-0" are informational; the range is authoritative.
        for value in shards.values():
            intervals.append((int(value[0]), int(value[1])))
    else:
        for value in shards:
            intervals.append((int(value[0]), int(value[1])))
    intervals.sort()
    for start, end in intervals:
        if start < 0 or end < start:
            raise ValueError(f"invalid shard interval [{start}, {end})")
    for (_, previous_end), (start, end) in zip(intervals, intervals[1:]):
        if start < previous_end:
            raise ValueError("shard intervals overlap")
    return tuple(intervals)


def contribution_from_record(side: str, record: Any) -> Contribution:
    if isinstance(record, Contribution):
        return record
    if isinstance(record, Mapping):
        return Contribution(
            id=record["id"],
            side=side,
            row=int(record["row"]),
            col=int(record["col"]),
            value=record["value"],
        )
    if isinstance(record, (str, bytes)) or not hasattr(record, "__len__"):
        raise ValueError("contribution record must be [id,row,col,value]")
    if len(record) != 4:
        raise ValueError("contribution record must be [id,row,col,value]")
    record_items = tuple(record)
    return Contribution(
        id=record_items[0],
        side=side,
        row=record_items[1],
        col=record_items[2],
        value=record_items[3],
    )


@dataclass(frozen=True, slots=True)
class RevisionEdit:
    id: str
    base: str
    left_add: tuple[Contribution, ...] = ()
    left_remove: tuple[str, ...] = ()
    right_add: tuple[Contribution, ...] = ()
    right_remove: tuple[str, ...] = ()

    @classmethod
    def coerce(cls, edit: "RevisionEdit | Mapping[str, Any]") -> "RevisionEdit":
        if isinstance(edit, RevisionEdit):
            return edit
        if not isinstance(edit, Mapping):
            raise TypeError("edit must be a RevisionEdit or mapping")
        revision_id = edit.get("id") or edit.get("revision")
        base = edit.get("base")
        if not isinstance(revision_id, str) or not revision_id:
            raise ValueError("edit.id must be a non-empty string")
        if not isinstance(base, str) or not base:
            raise ValueError("edit.base must be a non-empty string")

        def adds(side: str, key: str) -> tuple[Contribution, ...]:
            value = edit.get(key)
            if value is None:
                value = ()
            return tuple(
                contribution_from_record(side, record) for record in value
            )

        def removes(key: str) -> tuple[str, ...]:
            value = edit.get(key)
            if value is None:
                value = ()
            result: list[str] = []
            for item in value:
                if isinstance(item, Mapping):
                    item = item.get("id")
                elif isinstance(item, (list, tuple)) and item:
                    item = item[0]
                if not isinstance(item, str) or not item:
                    raise ValueError(f"{key} must contain contribution ids")
                result.append(item)
            return tuple(result)

        return cls(
            id=revision_id,
            base=base,
            left_add=adds("left", "left_add"),
            left_remove=removes("left_remove"),
            right_add=adds("right", "right_add"),
            right_remove=removes("right_remove"),
        )


@dataclass(frozen=True, slots=True)
class RevisionChange:
    id: str
    base: str
    left_added: tuple[Contribution, ...]
    left_removed: tuple[Contribution, ...]
    right_added: tuple[Contribution, ...]
    right_removed: tuple[Contribution, ...]

    def edits_output_row(self, row: int) -> bool:
        return any(c.row == row for c in self.left_added + self.left_removed)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "base": self.base,
            "left_added": [c.to_dict() for c in self.left_added],
            "left_removed": [c.to_dict() for c in self.left_removed],
            "right_added": [c.to_dict() for c in self.right_added],
            "right_removed": [c.to_dict() for c in self.right_removed],
        }


ProductRow = Mapping[int, ProductCoordinate]


@dataclass(frozen=True, slots=True)
class GraphVersion:
    id: str
    parent_id: str | None
    left: SourceMatrix
    right: SourceMatrix
    left_by_process: Mapping[int, tuple[int, ...]]
    product_rows: Mapping[int, ProductRow]
    affected_rows: frozenset[int]
    change: RevisionChange | None = None

    def coordinate(self, row: int, col: int) -> ProductCoordinate | None:
        product_row = self.product_rows.get(row)
        return None if product_row is None else product_row.get(col)

    def row_numbers(self) -> tuple[int, ...]:
        return tuple(sorted(self.product_rows))

    def row_view(self, row: int, *, coordinate_digests: bool = False) -> RowView:
        product_row = self.product_rows.get(row, MappingProxyType({}))
        coordinates: list[CoordinateView] = []
        for col in sorted(product_row):
            coordinate = product_row[col]
            coordinates.append(
                CoordinateView(
                    row=row,
                    col=col,
                    value=coordinate.value,
                    present=True,
                    zero_kind=coordinate.zero_kind,
                    path_count=len(coordinate.paths),
                    dependency_digest=(
                        coordinate.dependency_digest
                        if coordinate_digests
                        else None
                    ),
                    dependencies=coordinate.paths,
                )
            )
        summary = dependency_summary_for_row(self, row)
        return RowView(
            revision=self.id,
            row=row,
            present=bool(coordinates),
            shard=self.left.shard_for_row(row),
            coordinates=tuple(coordinates),
            dependency_summary=summary,
        )


def dependency_summary_for_row(version: GraphVersion, row: int) -> DependencySummary:
    """Build a stable dependency digest without JSON-materializing every path."""
    product_row = version.product_rows.get(row, {})
    left_ids: set[str] = set()
    right_ids: set[str] = set()
    path_count = 0
    hasher = sha256()
    hasher.update(f"row:{row}\n".encode("utf-8"))

    for col in sorted(product_row):
        coordinate = product_row[col]
        hasher.update(f"col:{col}:value:{coordinate.value}:paths:{len(coordinate.paths)}\n".encode())
        for path in coordinate.paths:
            path_count += 1
            left_ids.add(path.left_id)
            right_ids.add(path.right_id)
            hasher.update(b"path:")
            hasher.update(path.left_id.encode("utf-8", errors="replace"))
            hasher.update(b"\x1f")
            hasher.update(path.right_id.encode("utf-8", errors="replace"))
            hasher.update(f":p{path.process}:v{path.value}\n".encode())

    # Every active left contribution in this part row is a future join
    # dependency, even if its process row currently has no matching right entry.
    left_source_row = version.left.rows.get(row)
    source_left_ids = left_source_row.all_ids() if left_source_row else ()
    for contribution_id in source_left_ids:
        left_ids.add(contribution_id)
        hasher.update(b"left-source:")
        hasher.update(contribution_id.encode("utf-8", errors="replace"))
        hasher.update(b"\n")

    return DependencySummary(
        row=row,
        revision=version.id,
        coordinate_count=len(product_row),
        path_count=path_count,
        left_ids=tuple(sorted(left_ids)),
        right_ids=tuple(sorted(right_ids)),
        digest="rowdep-" + hasher.hexdigest(),
    )


@dataclass(frozen=True, slots=True)
class RevisionIssue:
    code: str
    message: str
    contribution_id: str | None = None
    side: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "contribution_id": self.contribution_id,
            "side": self.side,
        }


@dataclass(frozen=True, slots=True)
class CommitReport:
    accepted: bool
    code: str
    readable_revision: str
    requested_revision: str | None = None
    base: str | None = None
    revision_id: str | None = None
    issues: tuple[RevisionIssue, ...] = ()
    affected_rows: tuple[int, ...] = ()
    version: GraphVersion | None = None

    def to_dict(self, include_version: bool = False) -> dict[str, Any]:
        result: dict[str, Any] = {
            "accepted": self.accepted,
            "code": self.code,
            "requested_revision": self.requested_revision,
            "base": self.base,
            "revision_id": self.revision_id,
            "readable_revision": self.readable_revision,
            "issues": [i.to_dict() for i in self.issues],
            "affected_rows": list(self.affected_rows),
        }
        if include_version and self.version is not None:
            result["version_id"] = self.version.id
        return result


def compute_product_row(
    row: int,
    left: SourceMatrix,
    right: SourceMatrix,
    catalog: Mapping[tuple[str, str], Contribution],
) -> dict[int, ProductCoordinate]:
    """Compute one C[row, :] without dense temporary matrices."""
    accumulator: dict[int, dict[str, Any]] = {}
    left_row = left.rows.get(row)
    if left_row is None:
        return {}

    for left_id in left_row.all_ids():
        left_contribution = catalog[("left", left_id)]
        process = left_contribution.col
        right_row = right.rows.get(process)
        if right_row is None:
            continue
        for right_id in right_row.all_ids():
            right_contribution = catalog[("right", right_id)]
            col = right_contribution.col
            value = left_contribution.value * right_contribution.value
            bucket = accumulator.setdefault(col, {"value": 0, "paths": []})
            bucket["value"] += value
            bucket["paths"].append(
                PathContribution(
                    left_id=left_id,
                    right_id=right_id,
                    row=row,
                    process=process,
                    col=col,
                    value=value,
                )
            )

    result: dict[int, ProductCoordinate] = {}
    for col in sorted(accumulator):
        bucket = accumulator[col]
        paths = tuple(
            sorted(
                bucket["paths"],
                key=lambda p: (p.left_id, p.right_id, p.process, p.col),
            )
        )
        result[col] = ProductCoordinate(
            row=row, col=col, value=bucket["value"], paths=paths
        )
    return result


def _apply_side_edits(
    matrix: SourceMatrix,
    additions: Sequence[Contribution],
    removals: Sequence[str],
    catalog: Mapping[tuple[str, str], Contribution],
) -> SourceMatrix:
    rows = dict(matrix.rows)

    for contribution in additions:
        source_row = rows.get(contribution.row)
        coordinates = dict(source_row.coordinates) if source_row else {}
        ids = list(coordinates.get(contribution.col, ()))
        if contribution.id not in ids:
            ids.append(contribution.id)
        coordinates[contribution.col] = tuple(sorted(ids))
        rows[contribution.row] = SourceRow(
            contribution.row, MappingProxyType(coordinates)
        )

    for contribution_id in removals:
        contribution = catalog[(matrix.side, contribution_id)]
        source_row = rows.get(contribution.row)
        if source_row is None:
            continue
        coordinates = dict(source_row.coordinates)
        ids = [x for x in coordinates.get(contribution.col, ()) if x != contribution_id]
        if ids:
            coordinates[contribution.col] = tuple(ids)
        else:
            coordinates.pop(contribution.col, None)
        if coordinates:
            rows[contribution.row] = SourceRow(
                contribution.row, MappingProxyType(coordinates)
            )
        else:
            rows.pop(contribution.row, None)

    return SourceMatrix(
        side=matrix.side,
        shards=matrix.shards,
        rows=MappingProxyType(rows),
    )


def _build_left_by_process(
    left: SourceMatrix,
    base: Mapping[int, tuple[int, ...]] | None = None,
    touched_process_keys: Iterable[int] | None = None,
    touched_left_rows: Iterable[int] | None = None,
) -> Mapping[int, tuple[int, ...]]:
    if base is None:
        process_rows: dict[int, set[int]] = {}
        for row, source_row in left.rows.items():
            for process in source_row.coordinates:
                process_rows.setdefault(process, set()).add(row)
        return MappingProxyType(
            {k: tuple(sorted(v)) for k, v in sorted(process_rows.items())}
        )

    result: dict[int, tuple[int, ...]] = dict(base)
    touched_rows = set(touched_left_rows or ())
    for process in set(touched_process_keys or ()):
        rows = set(result.get(process, ()))
        for row in touched_rows:
            if left.ids_at(row, process):
                rows.add(row)
            else:
                rows.discard(row)
        if rows:
            result[process] = tuple(sorted(rows))
        else:
            result.pop(process, None)
    return MappingProxyType(result)


class RevisionStore:
    """Optimistic, identity-aware, atomic revision history."""

    def __init__(
        self,
        *,
        initial_revision: str,
        left_records: Iterable[Sequence[Any]],
        right_records: Iterable[Sequence[Any]],
        left_shards: Sequence[Sequence[int]] | Mapping[str, Sequence[int]] | None = None,
        right_shards: Sequence[Sequence[int]] | Mapping[str, Sequence[int]] | None = None,
        shape: Sequence[int] | None = None,
        budget: WorkspaceBudget | int | None = None,
    ) -> None:
        if not isinstance(initial_revision, str) or not initial_revision:
            raise ValueError("initial_revision must be a non-empty string")
        self.budget = (
            budget
            if isinstance(budget, WorkspaceBudget)
            else WorkspaceBudget(budget or 8 * 1024 * 1024)
        )
        self.shape = tuple(int(x) for x in shape) if shape is not None else None
        self.catalog: Catalog = {}
        left, left_contribs = SourceMatrix.from_records(
            "left", left_records, left_shards
        )
        right, right_contribs = SourceMatrix.from_records(
            "right", right_records, right_shards
        )
        for contribution in left_contribs + right_contribs:
            key = contribution.key
            if key in self.catalog:
                raise ValueError(f"duplicate contribution identity {key}")
            self._check_bounds(contribution)
            self.catalog[key] = contribution

        left_by_process = _build_left_by_process(left)
        product_rows: dict[int, ProductRow] = {}
        for row in sorted(left.rows):
            product_row = compute_product_row(row, left, right, self.catalog)
            if product_row:
                product_rows[row] = MappingProxyType(product_row)

        initial = GraphVersion(
            id=initial_revision,
            parent_id=None,
            left=left,
            right=right,
            left_by_process=left_by_process,
            product_rows=MappingProxyType(product_rows),
            affected_rows=frozenset(product_rows),
            change=None,
        )
        self._lock = RLock()
        self.versions: dict[str, GraphVersion] = {initial.id: initial}
        self.order: list[str] = [initial.id]
        self.head: GraphVersion = initial

    def _check_bounds(self, contribution: Contribution) -> None:
        if self.shape is None:
            return
        if len(self.shape) != 3:
            raise ValueError("shape must be [m,k,n]")
        m, k, n = self.shape
        if contribution.side == "left":
            if contribution.row >= m or contribution.col >= k:
                raise ValueError(f"left contribution {contribution.id} exceeds shape")
        else:
            if contribution.row >= k or contribution.col >= n:
                raise ValueError(f"right contribution {contribution.id} exceeds shape")

    def get(self, revision_id: str | None = None) -> GraphVersion:
        if revision_id is None:
            return self.head
        if revision_id not in self.versions:
            raise KeyError(f"unknown revision: {revision_id}")
        return self.versions[revision_id]

    def row_view(self, row: int, revision_id: str | None = None) -> RowView:
        return self.get(revision_id).row_view(row)

    def get_contribution(
        self, side: str, contribution_id: str
    ) -> Contribution | None:
        return self.catalog.get((side, contribution_id))

    def has_active_contribution(
        self,
        side: str,
        contribution_id: str,
        revision_id: str | None = None,
    ) -> bool:
        contribution = self.get_contribution(side, contribution_id)
        if contribution is None:
            return False
        return self._is_active(self.get(revision_id), contribution)

    def iter_shard_rows(
        self, side: str, shard: int | Sequence[int], revision_id: str | None = None
    ):
        """Yield ``(row_number, SourceRow)`` for one row shard."""
        version = self.get(revision_id)
        matrix = version.left if side == "left" else version.right
        if isinstance(shard, int):
            start, end = matrix.shards[shard]
        else:
            start, end = int(shard[0]), int(shard[1])
        for row in sorted(matrix.rows):
            if start <= row < end:
                yield row, matrix.rows[row]

    def cumulative_affected(
        self, old_revision: str | None, new_revision: str | None = None
    ) -> frozenset[int] | None:
        """Rows changed between two readable revisions.

        Returns None when ancestry is unavailable and a full rebuild is safest.
        """
        new = self.get(new_revision)
        if old_revision is None:
            return None
        if old_revision not in self.versions:
            return None
        affected: set[int] = set()
        current = new
        seen = 0
        while current.id != old_revision:
            if current.change is not None:
                affected.update(current.affected_rows)
            if current.parent_id is None:
                return None
            current = self.versions.get(current.parent_id)
            if current is None:
                return None
            seen += 1
            if seen > len(self.versions) + 1:
                return None
        return frozenset(affected)

    def _is_active(self, version: GraphVersion, contribution: Contribution) -> bool:
        matrix = version.left if contribution.side == "left" else version.right
        return contribution.id in matrix.ids_at(contribution.row, contribution.col)

    def submit(self, edit: RevisionEdit | Mapping[str, Any]) -> CommitReport:
        parsed = RevisionEdit.coerce(edit)
        with self._lock:
            current = self.head
            issues: list[RevisionIssue] = []
            base = self.versions.get(parsed.base)

            if parsed.id in self.versions:
                issues.append(
                    RevisionIssue(
                        "duplicate_revision",
                        f"revision {parsed.id!r} already exists",
                    )
                )
            if base is None:
                issues.append(
                    RevisionIssue(
                        "unknown_base",
                        f"base revision {parsed.base!r} is not readable",
                    )
                )
            elif parsed.base != current.id:
                issues.append(
                    RevisionIssue(
                        "stale_base",
                        f"base {parsed.base!r} is behind readable head "
                        f"{current.id!r}; current version was retained",
                    )
                )

            staged: Catalog = {}
            for contribution in (
                parsed.left_add + parsed.right_add
            ):
                self._check_bounds(contribution)
                if contribution.key in staged:
                    issues.append(
                        RevisionIssue(
                            "duplicate_contribution",
                            f"contribution id {contribution.id!r} is duplicated in edit",
                            contribution.id,
                            contribution.side,
                        )
                    )
                elif contribution.key in self.catalog:
                    issues.append(
                        RevisionIssue(
                            "contribution_exists",
                            f"{contribution.side} contribution {contribution.id!r} already exists",
                            contribution.id,
                            contribution.side,
                        )
                    )
                else:
                    staged[contribution.key] = contribution

            if base is not None:
                for side, ids in (
                    ("left", parsed.left_remove),
                    ("right", parsed.right_remove),
                ):
                    for contribution_id in ids:
                        key = (side, contribution_id)
                        contribution = self.catalog.get(key)
                        if contribution is None or not self._is_active(base, contribution):
                            issues.append(
                                RevisionIssue(
                                    "unknown_contribution",
                                    f"{side} contribution {contribution_id!r} is not active "
                                    f"in base {parsed.base!r}",
                                    contribution_id,
                                    side,
                                )
                            )

            if issues:
                codes = {issue.code for issue in issues}
                if "unknown_base" in codes:
                    code = "unknown_base"
                elif "stale_base" in codes:
                    code = "stale_base"
                else:
                    code = "invalid_commit"
                return CommitReport(
                    accepted=False,
                    code=code,
                    requested_revision=parsed.id,
                    base=parsed.base,
                    readable_revision=current.id,
                    issues=tuple(issues),
                    version=current,
                )

            # From here all references are valid and base == current head.
            catalog_view: Mapping[
                tuple[str, str], Contribution
            ] = _LayeredCatalog(staged, self.catalog)

            new_left = _apply_side_edits(
                base.left, parsed.left_add, parsed.left_remove, catalog_view
            )
            new_right = _apply_side_edits(
                base.right, parsed.right_add, parsed.right_remove, catalog_view
            )

            left_touched_keys = {c.col for c in parsed.left_add} | {
                self.catalog[("left", cid)].col for cid in parsed.left_remove
            }
            left_touched_rows = {c.row for c in parsed.left_add} | {
                self.catalog[("left", cid)].row for cid in parsed.left_remove
            }
            new_left_by_process = _build_left_by_process(
                new_left,
                base=base.left_by_process,
                touched_process_keys=left_touched_keys,
                touched_left_rows=left_touched_rows,
            )

            affected_rows: set[int] = set(left_touched_rows)
            right_process_keys = {c.row for c in parsed.right_add} | {
                self.catalog[("right", cid)].row for cid in parsed.right_remove
            }
            for process in right_process_keys:
                affected_rows.update(base.left_by_process.get(process, ()))
                affected_rows.update(new_left_by_process.get(process, ()))

            new_product_rows = dict(base.product_rows)
            for row in affected_rows:
                product_row = compute_product_row(
                    row, new_left, new_right, catalog_view
                )
                if product_row:
                    new_product_rows[row] = MappingProxyType(product_row)
                else:
                    new_product_rows.pop(row, None)

            removed_left = tuple(
                self.catalog[("left", cid)] for cid in parsed.left_remove
            )
            removed_right = tuple(
                self.catalog[("right", cid)] for cid in parsed.right_remove
            )
            change = RevisionChange(
                id=parsed.id,
                base=parsed.base,
                left_added=tuple(parsed.left_add),
                left_removed=removed_left,
                right_added=tuple(parsed.right_add),
                right_removed=removed_right,
            )
            version = GraphVersion(
                id=parsed.id,
                parent_id=base.id,
                left=new_left,
                right=new_right,
                left_by_process=MappingProxyType(dict(new_left_by_process)),
                product_rows=MappingProxyType(new_product_rows),
                affected_rows=frozenset(affected_rows),
                change=change,
            )

            # Publish identities and immutable snapshot.  The head assignment
            # is the linearization point for readers.
            self.catalog.update(staged)
            self.versions[version.id] = version
            self.order.append(version.id)
            self.head = version

            return CommitReport(
                accepted=True,
                code="ok",
                requested_revision=version.id,
                revision_id=version.id,
                base=parsed.base,
                readable_revision=version.id,
                issues=(),
                affected_rows=tuple(sorted(affected_rows)),
                version=version,
            )


class _LayeredCatalog(Mapping[tuple[str, str], Contribution]):
    """Read-only staged overlay used while constructing a candidate revision."""

    def __init__(self, staged: Catalog, base: Catalog) -> None:
        self.staged = staged
        self.base = base

    def __getitem__(self, key: tuple[str, str]) -> Contribution:
        if key in self.staged:
            return self.staged[key]
        return self.base[key]

    def __iter__(self):  # pragma: no cover - Mapping interface completeness
        return iter({**self.base, **self.staged})

    def __len__(self) -> int:  # pragma: no cover
        return len(self.base) + len(self.staged)
