"""Workspace planning helpers.

The production path in :mod:`adjlink.engine` streams one left row at a time and
never materializes an m*k*n or nnz(A)*nnz(B) dense object.  The budget object
is intentionally advisory: callers that need hard accounting can use
:class:`ScratchLedger` around their own temporary buffers.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class WorkspaceBudget:
    """Upper bound for unpack/transform/reduce/staging/handoff scratch bytes."""

    peak_scratch_bytes: int = 8 * 1024 * 1024

    def __post_init__(self) -> None:
        if not isinstance(self.peak_scratch_bytes, int) or isinstance(
            self.peak_scratch_bytes, bool
        ):
            raise TypeError("peak_scratch_bytes must be an integer")
        if self.peak_scratch_bytes <= 0:
            raise ValueError("peak_scratch_bytes must be positive")

    def batch_size(self, bytes_per_item: int, minimum: int = 1) -> int:
        """Return the largest item batch that fits the scratch budget."""
        if bytes_per_item <= 0:
            return max(minimum, 1)
        return max(minimum, self.peak_scratch_bytes // bytes_per_item)


class ScratchLedger:
    """A small checked ledger for short-lived scratch allocations."""

    def __init__(self, budget: WorkspaceBudget | int | None = None) -> None:
        if budget is None:
            budget = WorkspaceBudget()
        elif isinstance(budget, int):
            budget = WorkspaceBudget(budget)
        self.budget = budget
        self.used = 0
        self.peak = 0

    def allocate(self, nbytes: int) -> None:
        if nbytes < 0:
            raise ValueError("allocation size must be non-negative")
        self.used += nbytes
        self.peak = max(self.peak, self.used)
        if self.used > self.budget.peak_scratch_bytes:
            over = self.used - self.budget.peak_scratch_bytes
            self.used -= nbytes
            raise MemoryError(f"scratch budget exceeded by {over} bytes")

    def release(self, nbytes: int) -> None:
        if nbytes < 0 or nbytes > self.used:
            raise ValueError("invalid scratch release")
        self.used -= nbytes

    def buffer(self, nbytes: int) -> "ScratchBuffer":
        return ScratchBuffer(self, nbytes)


class ScratchBuffer:
    def __init__(self, ledger: ScratchLedger, nbytes: int) -> None:
        self.ledger = ledger
        self.nbytes = nbytes
        ledger.allocate(nbytes)
        self.closed = False

    def close(self) -> None:
        if not self.closed:
            self.ledger.release(self.nbytes)
            self.closed = True

    def __enter__(self) -> "ScratchBuffer":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
