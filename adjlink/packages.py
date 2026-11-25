"""Receiver-specific row packaging, capacity selection, and overflow diagnosis."""
from __future__ import annotations

from dataclasses import dataclass, replace
from collections.abc import Sequence as AbstractSequence
from numbers import Real
from types import MappingProxyType
from typing import Any, Mapping

from .engine import GraphVersion, dependency_summary_for_row
from .model import digest_canonical, is_int64


@dataclass(frozen=True, slots=True)
class ReceiverPolicy:
    id: str
    capacity: int | Mapping[Any, Any] | AbstractSequence[Any] | None
    required: Mapping[int, tuple[int, ...]]
    protect_at: int | float | Mapping[Any, Any] | AbstractSequence[Any] | None = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ReceiverPolicy":
        receiver_id = data.get("id")
        if not isinstance(receiver_id, str) or not receiver_id:
            raise ValueError("receiver.id must be a non-empty string")
        required_raw = data.get("required", {}) or {}
        required: dict[int, tuple[int, ...]] = {}
        if isinstance(required_raw, Mapping):
            for row_key, cols in required_raw.items():
                row = int(row_key)
                required[row] = tuple(sorted({int(c) for c in cols}))
        elif isinstance(required_raw, AbstractSequence):
            for row, cols in enumerate(required_raw):
                if cols:
                    required[row] = tuple(sorted({int(c) for c in cols}))
        else:
            raise TypeError("receiver.required must be a mapping or sequence")
        return cls(
            id=receiver_id,
            capacity=data.get("capacity"),
            required=MappingProxyType(required),
            protect_at=data.get("protect_at"),
        )

    @property
    def fingerprint(self) -> str:
        required_native = {str(row): list(cols) for row, cols in self.required.items()}
        return digest_canonical(
            {
                "id": self.id,
                "capacity": self.capacity,
                "required": required_native,
                "protect_at": self.protect_at,
            },
            "policy-",
        )

    def _row_spec(
        self,
        spec: int | float | Mapping[Any, Any] | AbstractSequence[Any] | None,
        row: int,
        default: Any = None,
    ) -> Any:
        if spec is None:
            return default
        if isinstance(spec, Mapping):
            if row in spec:
                return spec[row]
            if str(row) in spec:
                return spec[str(row)]
            if "default" in spec:
                return spec["default"]
            return default
        if isinstance(spec, AbstractSequence) and not isinstance(spec, (str, bytes)):
            return spec[row] if 0 <= row < len(spec) else default
        return spec

    def capacity_for(self, row: int) -> int | None:
        value = self._row_spec(self.capacity, row, default=None)
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int):
            value = int(value)
        if value < 0:
            raise ValueError(f"negative capacity for receiver {self.id} row {row}")
        return value

    def protect_at_for(self, row: int) -> int | float | None:
        value = self._row_spec(self.protect_at, row, default=None)
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, Real):
            raise TypeError("protect_at must be numeric")
        return value


@dataclass(frozen=True, slots=True)
class PackageEntry:
    col: int
    value: int
    abs_value: int
    present: bool
    zero_kind: str | None
    status: str  # required | required_absent | protected | optional | dropped
    selected: bool
    mandatory: bool
    droppable: bool
    in_int64: bool
    protected: bool = False
    path_count: int = 0
    dependency_digest: str | None = None

    def to_wire(self) -> list[int]:
        return [self.col, self.value]

    def to_dict(self) -> dict[str, Any]:
        return {
            "col": self.col,
            "value": self.value,
            "abs_value": self.abs_value,
            "present": self.present,
            "zero_kind": self.zero_kind,
            "status": self.status,
            "selected": self.selected,
            "mandatory": self.mandatory,
            "droppable": self.droppable,
            "protected": self.protected,
            "in_int64": self.in_int64,
            "path_count": self.path_count,
            "dependency_digest": self.dependency_digest,
        }


@dataclass(frozen=True, slots=True)
class Blocker:
    code: str
    message: str
    row: int
    capacity: int | None
    required_cols: tuple[int, ...] = ()
    protected_cols: tuple[int, ...] = ()
    overflow_count: int = 0
    out_of_range_cols: tuple[int, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "row": self.row,
            "capacity": self.capacity,
            "required_cols": list(self.required_cols),
            "protected_cols": list(self.protected_cols),
            "overflow_count": self.overflow_count,
            "out_of_range_cols": list(self.out_of_range_cols),
        }


@dataclass(frozen=True, slots=True)
class RowPackage:
    receiver_id: str
    row: int
    revision: str
    capacity: int | None
    protect_at: int | float | None
    deliverable: bool
    status: str
    entries: tuple[PackageEntry, ...]
    mandatory: tuple[PackageEntry, ...]
    optional_ranked: tuple[PackageEntry, ...]
    dropped: tuple[PackageEntry, ...]
    blockers: tuple[Blocker, ...]
    candidate_count: int
    dependency_digest: str
    wire_hash: str | None
    package_hash: str

    @property
    def blocked(self) -> bool:
        return not self.deliverable and bool(self.blockers)

    def wire(self) -> tuple[list[int], ...]:
        if not self.deliverable:
            codes = ", ".join(b.code for b in self.blockers)
            raise RuntimeError(
                f"receiver {self.receiver_id!r} row {self.row} is not deliverable: {codes}"
            )
        return tuple(e.to_wire() for e in self.entries)

    def values(self) -> dict[int, int]:
        return {e.col: e.value for e in self.entries}

    def eligibility(self) -> dict[int, dict[str, Any]]:
        result: dict[int, dict[str, Any]] = {}
        for entry in self.mandatory + self.optional_ranked:
            result[entry.col] = {
                "status": entry.status,
                "selected": entry.selected and self.deliverable,
                "mandatory": entry.mandatory,
                "droppable": entry.droppable,
                "protected": entry.protected,
                "present": entry.present,
            }
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            "receiver_id": self.receiver_id,
            "row": self.row,
            "revision": self.revision,
            "capacity": self.capacity,
            "protect_at": self.protect_at,
            "deliverable": self.deliverable,
            "status": self.status,
            "entries": [e.to_dict() for e in self.entries],
            "mandatory": [e.to_dict() for e in self.mandatory],
            "optional_ranked": [e.to_dict() for e in self.optional_ranked],
            "dropped": [e.to_dict() for e in self.dropped],
            "blockers": [b.to_dict() for b in self.blockers],
            "candidate_count": self.candidate_count,
            "dependency_digest": self.dependency_digest,
            "wire_hash": self.wire_hash,
            "package_hash": self.package_hash,
        }


def _product_entry(row: int, coordinate: Any, status: str, selected: bool) -> PackageEntry:
    return PackageEntry(
        col=coordinate.col,
        value=coordinate.value,
        abs_value=abs(coordinate.value),
        present=True,
        zero_kind=coordinate.zero_kind,
        status=status,
        selected=selected,
        mandatory=status in {"required", "protected"},
        droppable=status == "optional",
        in_int64=is_int64(coordinate.value),
        protected=False,
        path_count=len(coordinate.paths),
        dependency_digest=None,
    )


def _absent_required_entry(col: int) -> PackageEntry:
    return PackageEntry(
        col=col,
        value=0,
        abs_value=0,
        present=False,
        zero_kind="absent",
        status="required_absent",
        selected=False,
        mandatory=True,
        droppable=False,
        in_int64=True,
    )


def package_row(policy: ReceiverPolicy, row: int, version: GraphVersion) -> RowPackage:
    capacity = policy.capacity_for(row)
    threshold = policy.protect_at_for(row)
    product_row = version.product_rows.get(row, {})
    required_cols = policy.required.get(row, ())
    required_set = set(required_cols)

    required_entries: list[PackageEntry] = []
    for col in required_cols:
        coordinate = product_row.get(col)
        if coordinate is None:
            required_entries.append(_absent_required_entry(col))
        else:
            required_entries.append(_product_entry(row, coordinate, "required", False))

    protected_entries: list[PackageEntry] = []
    optional_source: list[PackageEntry] = []
    for col in sorted(product_row):
        if col in required_set:
            continue
        coordinate = product_row[col]
        is_protected = threshold is not None and abs(coordinate.value) >= threshold
        entry = _product_entry(
            row,
            coordinate,
            "protected" if is_protected else "optional",
            False,
        )
        entry = replace(entry, protected=is_protected, mandatory=is_protected,
                        droppable=not is_protected)
        if is_protected:
            protected_entries.append(entry)
        else:
            optional_source.append(entry)

    required_entries.sort(key=lambda e: e.col)
    protected_entries.sort(key=lambda e: e.col)
    optional_source.sort(key=lambda e: (-e.abs_value, e.col))

    mandatory = tuple(required_entries + protected_entries)
    optional_ranked: list[PackageEntry] = []
    blockers: list[Blocker] = []

    if capacity is not None and len(required_entries) > capacity:
        blockers.append(
            Blocker(
                code="required_overflow",
                message=(
                    f"{len(required_entries)} required coordinates cannot fit "
                    f"in capacity {capacity}"
                ),
                row=row,
                capacity=capacity,
                required_cols=tuple(e.col for e in required_entries),
                overflow_count=len(required_entries) - capacity,
            )
        )

    if capacity is not None and len(required_entries) <= capacity and len(mandatory) > capacity:
        blockers.append(
            Blocker(
                code="protected_overflow",
                message=(
                    f"{len(required_entries)} required plus "
                    f"{len(protected_entries)} protected coordinates exceed "
                    f"capacity {capacity}"
                ),
                row=row,
                capacity=capacity,
                required_cols=tuple(e.col for e in required_entries),
                protected_cols=tuple(e.col for e in protected_entries),
                overflow_count=len(mandatory) - capacity,
            )
        )

    mandatory_oor = tuple(e.col for e in mandatory if not e.in_int64)
    if mandatory_oor:
        blockers.append(
            Blocker(
                code="int64_overflow",
                message="mandatory coordinate values are outside signed int64",
                row=row,
                capacity=capacity,
                out_of_range_cols=mandatory_oor,
            )
        )

    if blockers:
        # All optional coordinates remain identifiable as droppable weak
        # contributions.  Dropping them cannot clear a mandatory overflow.
        optional_ranked = [replace(e, status="dropped", selected=False) for e in optional_source]
        digest = dependency_summary_for_row(version, row).digest
        report_material = {
            "receiver": policy.id,
            "row": row,
            "revision": version.id,
            "blockers": [b.to_dict() for b in blockers],
            "mandatory": [e.col for e in mandatory],
            "dropped": [e.col for e in optional_ranked],
        }
        package_hash = digest_canonical(report_material, "pkgblock-")
        return RowPackage(
            receiver_id=policy.id,
            row=row,
            revision=version.id,
            capacity=capacity,
            protect_at=threshold,
            deliverable=False,
            status="blocked",
            entries=(),
            mandatory=tuple(replace(e, selected=False) for e in mandatory),
            optional_ranked=tuple(optional_ranked),
            dropped=tuple(optional_ranked),
            blockers=tuple(blockers),
            candidate_count=len(product_row),
            dependency_digest=digest,
            wire_hash=None,
            package_hash=package_hash,
        )

    if capacity is None:
        slots = len(optional_source)
    else:
        slots = max(0, capacity - len(mandatory))
    chosen = optional_source[:slots]
    dropped = optional_source[slots:]

    chosen = [replace(e, status="optional", selected=True) for e in chosen]
    dropped = [replace(e, status="dropped", selected=False) for e in dropped]
    selected_mandatory = tuple(replace(e, selected=True) for e in mandatory)
    entries = tuple(
        sorted(selected_mandatory + tuple(chosen), key=lambda e: e.col)
    )

    selected_oor = tuple(e.col for e in entries if not e.in_int64)
    if selected_oor:
        blocker = Blocker(
            code="int64_overflow",
            message="selected coordinate value is outside signed int64",
            row=row,
            capacity=capacity,
            out_of_range_cols=selected_oor,
        )
        report_material = {
            "receiver": policy.id,
            "row": row,
            "revision": version.id,
            "blockers": [blocker.to_dict()],
        }
        return RowPackage(
            receiver_id=policy.id,
            row=row,
            revision=version.id,
            capacity=capacity,
            protect_at=threshold,
            deliverable=False,
            status="blocked",
            entries=(),
            mandatory=selected_mandatory,
            optional_ranked=tuple(chosen + dropped),
            dropped=tuple(dropped),
            blockers=(blocker,),
            candidate_count=len(product_row),
            dependency_digest=dependency_summary_for_row(version, row).digest,
            wire_hash=None,
            package_hash=digest_canonical(report_material, "pkgblock-"),
        )

    wire = tuple(e.to_wire() for e in entries)
    wire_hash = digest_canonical(list(wire), "wire-")
    digest = dependency_summary_for_row(version, row).digest
    package_hash = digest_canonical(
        {
            "receiver": policy.id,
            "row": row,
            "revision": version.id,
            "wire_hash": wire_hash,
        },
        "pkg-",
    )
    status = "absent" if not product_row and not required_entries else "ready"
    return RowPackage(
        receiver_id=policy.id,
        row=row,
        revision=version.id,
        capacity=capacity,
        protect_at=threshold,
        deliverable=True,
        status=status,
        entries=entries,
        mandatory=selected_mandatory,
        optional_ranked=tuple(chosen + dropped),
        dropped=tuple(dropped),
        blockers=(),
        candidate_count=len(product_row),
        dependency_digest=digest,
        wire_hash=wire_hash,
        package_hash=package_hash,
    )


@dataclass(frozen=True, slots=True)
class ReceiverPackageSet:
    receiver_id: str
    revision: str
    policy_fingerprint: str
    rows: Mapping[int, RowPackage]
    rebuilt_rows: tuple[int, ...] = ()
    reused_rows: tuple[int, ...] = ()

    @property
    def blocked_rows(self) -> tuple[int, ...]:
        return tuple(row for row, pkg in sorted(self.rows.items()) if pkg.blocked)

    @property
    def ready(self) -> bool:
        return not self.blocked_rows

    def get(self, row: int) -> RowPackage | None:
        return self.rows.get(row)

    def wire_manifest(self) -> dict[str, Any]:
        return {
            "receiver_id": self.receiver_id,
            "revision": self.revision,
            "ready": self.ready,
            "blocked_rows": list(self.blocked_rows),
            "rows": {
                str(row): {
                    "wire_hash": pkg.wire_hash,
                    "wire": [entry.to_wire() for entry in pkg.entries]
                    if pkg.deliverable
                    else None,
                    "status": pkg.status,
                    "revision": pkg.revision,
                }
                for row, pkg in sorted(self.rows.items())
            },
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "receiver_id": self.receiver_id,
            "revision": self.revision,
            "policy_fingerprint": self.policy_fingerprint,
            "ready": self.ready,
            "blocked_rows": list(self.blocked_rows),
            "rebuilt_rows": list(self.rebuilt_rows),
            "reused_rows": list(self.reused_rows),
            "rows": {str(row): pkg.to_dict() for row, pkg in sorted(self.rows.items())},
        }


def package_revision(
    policy: ReceiverPolicy,
    version: GraphVersion,
    *,
    previous: ReceiverPackageSet | None = None,
    changed_rows: AbstractSequence[int] | frozenset[int] | set[int] | None = None,
) -> ReceiverPackageSet:
    """Build packages, reusing untouched row packages whenever possible."""
    rows: dict[int, RowPackage] = {}
    rebuilt: list[int] = []
    reused: list[int] = []

    can_increment = (
        previous is not None and previous.policy_fingerprint == policy.fingerprint
    )

    if can_increment and changed_rows is not None:
        rows.update(previous.rows)  # type: ignore[arg-type]
        for row in sorted(set(int(r) for r in changed_rows)):
            rows[row] = package_row(policy, row, version)
            rebuilt.append(row)
        for row in rows:
            if row not in set(rebuilt):
                reused.append(row)
    else:
        if can_increment and changed_rows is None:
            candidate_rows = (
                set(previous.rows)
                | set(version.product_rows)
                | set(policy.required)
            )
        else:
            candidate_rows = set(version.product_rows) | set(policy.required)
        for row in sorted(candidate_rows):
            pkg = package_row(policy, row, version)
            rows[row] = pkg
            if previous is not None and row in previous.rows and previous.rows[row].wire_hash == pkg.wire_hash and previous.rows[row].deliverable == pkg.deliverable:
                reused.append(row)
            else:
                rebuilt.append(row)

    return ReceiverPackageSet(
        receiver_id=policy.id,
        revision=version.id,
        policy_fingerprint=policy.fingerprint,
        rows=MappingProxyType(rows),
        rebuilt_rows=tuple(sorted(rebuilt)),
        reused_rows=tuple(sorted(reused)),
    )
