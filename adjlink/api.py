"""Small functional facade for callers that do not need the full pipeline."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from .engine import GraphVersion, RevisionStore
from .pipeline import CorrectionPipeline


def signed_spgemm(
    left: list[list[Any]] | tuple[Any, ...],
    right: list[list[Any]] | tuple[Any, ...],
    *,
    left_shards=None,
    right_shards=None,
    shape=None,
    revision: str = "r0",
) -> GraphVersion:
    """Return an immutable sparse signed C = left @ right graph version."""
    store = RevisionStore(
        initial_revision=revision,
        left_records=left,
        right_records=right,
        left_shards=left_shards,
        right_shards=right_shards,
        shape=shape,
    )
    return store.head


def spgemm_values(
    left: list[list[Any]],
    right: list[list[Any]],
    **kwargs: Any,
) -> dict[int, dict[int, int]]:
    """Return aggregate values, including canceled/explicit zero coordinates."""
    version = signed_spgemm(left, right, **kwargs)
    return {
        row: {col: coordinate.value for col, coordinate in product_row.items()}
        for row, product_row in sorted(version.product_rows.items())
    }


def process_capture(
    capture: str | Path | Mapping[str, Any],
    *,
    capacity: str | Path | Mapping[str, Any] | None = None,
    out: str | Path | None = None,
) -> CorrectionPipeline:
    """Replay a capture's initial data and edits and optionally write packets."""
    pipeline = CorrectionPipeline.from_capture(
        capture, capacity=capacity, apply_edits=True
    )
    if out is not None:
        pipeline.write_packages(out)
    return pipeline
