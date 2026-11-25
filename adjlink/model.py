"""Immutable domain objects for signed, provenance-preserving SpGEMM."""
from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
import json
from numbers import Integral
from typing import Any, Mapping


SID = str
RowCol = tuple[int, int]

INT64_MIN = -(1 << 63)
INT64_MAX = (1 << 63) - 1


def is_int64(value: int) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, Integral)
        and INT64_MIN <= int(value) <= INT64_MAX
    )


def canonical_json(value: Any) -> str:
    """Canonical JSON used by digests.

    Tuples are converted to lists.  The input is expected to contain only
    JSON-native data plus tuples/mappings.
    """

    def default(obj: Any) -> Any:
        if isinstance(obj, tuple):
            return list(obj)
        if isinstance(obj, Mapping):
            return dict(obj)
        if hasattr(obj, "to_dict"):
            return obj.to_dict()
        raise TypeError(f"not JSON canonicalizable: {type(obj)!r}")

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=default,
    )


def digest_canonical(value: Any, prefix: str = "") -> str:
    raw = canonical_json(value).encode("utf-8")
    return prefix + sha256(raw).hexdigest()


def require_coord_int(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise TypeError(f"{name} must be an integer, got {type(value).__name__}")
    value = int(value)
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def require_signed_int(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise TypeError(f"{name} must be an integer, got {type(value).__name__}")
    return int(value)


@dataclass(frozen=True, slots=True)
class Contribution:
    """One exporter contribution.

    Contributions at the same coordinate are deliberately separate objects when
    their ids differ.  Withdrawal always names the contribution id.
    """

    id: SID
    side: str
    row: int
    col: int
    value: int

    def __post_init__(self) -> None:
        if self.side not in {"left", "right"}:
            raise ValueError("side must be 'left' or 'right'")
        if not isinstance(self.id, str) or not self.id:
            raise TypeError("contribution id must be a non-empty string")
        object.__setattr__(self, "row", require_coord_int("row", self.row))
        object.__setattr__(self, "col", require_coord_int("col", self.col))
        object.__setattr__(self, "value", require_signed_int("value", self.value))

    @property
    def key(self) -> tuple[str, str]:
        return (self.side, self.id)

    def to_entry(self) -> list[Any]:
        return [self.id, self.row, self.col, self.value]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "side": self.side,
            "row": self.row,
            "col": self.col,
            "value": self.value,
        }


@dataclass(frozen=True, slots=True)
class PathContribution:
    """One A-entry × B-entry join producing a signed product contribution."""

    left_id: SID
    right_id: SID
    row: int
    process: int
    col: int
    value: int

    @property
    def path_id(self) -> str:
        return digest_canonical([self.left_id, self.right_id], "path-")[:22]

    @property
    def dependency_key(self) -> tuple[SID, SID]:
        return (self.left_id, self.right_id)

    def to_dict(self) -> dict[str, Any]:
        return {
            "path_id": self.path_id,
            "left_id": self.left_id,
            "right_id": self.right_id,
            "row": self.row,
            "process": self.process,
            "col": self.col,
            "value": self.value,
        }


@dataclass(frozen=True, slots=True)
class ProductCoordinate:
    row: int
    col: int
    value: int
    paths: tuple[PathContribution, ...]

    @property
    def present(self) -> bool:
        # Existence of this object means at least one active join path.
        return True

    @property
    def zero_kind(self) -> str | None:
        """Explain a zero aggregate without confusing it with absence."""
        if self.value != 0:
            return None
        if any(p.value != 0 for p in self.paths):
            return "canceled"
        return "explicit_zero_path"

    @property
    def dependency_ids(self) -> tuple[tuple[SID, SID], ...]:
        return tuple(p.dependency_key for p in self.paths)

    @property
    def dependency_digest(self) -> str:
        return digest_canonical(
            {
                "coordinate": [self.row, self.col],
                "value": self.value,
                "paths": [
                    [p.left_id, p.right_id, p.process, p.value] for p in self.paths
                ],
            },
            "dep-",
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "row": self.row,
            "col": self.col,
            "value": self.value,
            "present": True,
            "zero_kind": self.zero_kind,
            "path_count": len(self.paths),
            "dependency_digest": self.dependency_digest,
            "paths": [p.to_dict() for p in self.paths],
        }


@dataclass(frozen=True, slots=True)
class CoordinateView:
    """Row-oriented coordinate view used by training-side readers."""

    row: int
    col: int
    value: int | None
    present: bool
    zero_kind: str | None
    path_count: int
    dependency_digest: str | None
    dependencies: tuple[PathContribution, ...] = field(default=())

    def to_dict(self, include_dependencies: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {
            "row": self.row,
            "col": self.col,
            "value": self.value,
            "present": self.present,
            "zero_kind": self.zero_kind,
            "path_count": self.path_count,
            "dependency_digest": self.dependency_digest,
        }
        if include_dependencies:
            result["dependencies"] = [p.to_dict() for p in self.dependencies]
        return result


@dataclass(frozen=True, slots=True)
class DependencySummary:
    row: int
    revision: str
    coordinate_count: int
    path_count: int
    left_ids: tuple[SID, ...]
    right_ids: tuple[SID, ...]
    digest: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "row": self.row,
            "revision": self.revision,
            "coordinate_count": self.coordinate_count,
            "path_count": self.path_count,
            "left_ids": list(self.left_ids),
            "right_ids": list(self.right_ids),
            "digest": self.digest,
        }


@dataclass(frozen=True, slots=True)
class RowView:
    revision: str
    row: int
    present: bool
    shard: int | tuple[int, int] | None
    coordinates: tuple[CoordinateView, ...]
    dependency_summary: DependencySummary

    def values(self) -> dict[int, int]:
        return {c.col: c.value or 0 for c in self.coordinates if c.present}

    def to_dict(self, include_dependencies: bool = True) -> dict[str, Any]:
        return {
            "revision": self.revision,
            "row": self.row,
            "present": self.present,
            "shard": self.shard,
            "coordinates": [
                c.to_dict(include_dependencies=include_dependencies)
                for c in self.coordinates
            ],
            "dependency_summary": self.dependency_summary.to_dict(),
        }
