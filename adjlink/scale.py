"""Scale and workspace planning for large sparse captures."""
from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Mapping

from .budget import WorkspaceBudget


@dataclass(frozen=True, slots=True)
class ScaleSpec:
    m: int
    k: int
    n: int
    entries_per_input_row: int | None = None
    seed: int | None = None
    peak_scratch_bytes: int = 8 * 1024 * 1024

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ScaleSpec":
        return cls(
            m=int(data["m"]),
            k=int(data["k"]),
            n=int(data["n"]),
            entries_per_input_row=data.get("entries_per_input_row"),
            seed=data.get("seed"),
            peak_scratch_bytes=int(
                data.get("peak_scratch_bytes", 8 * 1024 * 1024)
            ),
        )

    @classmethod
    def load(cls, path: str | Path) -> "ScaleSpec":
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))

    @property
    def budget(self) -> WorkspaceBudget:
        return WorkspaceBudget(self.peak_scratch_bytes)

    def dense_product_cells(self) -> int:
        return self.m * self.n

    def dense_product_bytes(self, bytes_per_cell: int = 16) -> int:
        # int64 value plus a conservative validity/index allowance.
        return self.dense_product_cells() * bytes_per_cell

    def estimated_join_paths(self) -> int | None:
        """Estimate paths under uniform entries-per-row assumptions."""
        if self.entries_per_input_row is None:
            return None
        e = self.entries_per_input_row
        return self.m * e * e

    def requires_sparse_algorithm(self) -> bool:
        return self.dense_product_bytes() > self.peak_scratch_bytes
