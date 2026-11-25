"""Reusable signed, versioned, provenance-preserving SpGEMM row packaging."""
from __future__ import annotations

from .api import process_capture, signed_spgemm, spgemm_values
from .budget import ScratchLedger, WorkspaceBudget
from .deployment import RowDeployment
from .diff import VersionDiff, compare_versions
from .diff import compare_versions as diff_revisions
from .engine import (
    CommitReport,
    GraphVersion,
    RevisionEdit,
    RevisionStore,
    SourceMatrix,
)
from .model import (
    Contribution,
    DependencySummary,
    PathContribution,
    ProductCoordinate,
    RowView,
)
from .package_store import PackageBlobStore
from .packages import (
    ReceiverPackageSet,
    ReceiverPolicy,
    RowPackage,
    package_revision,
    package_row,
)
from .pipeline import CorrectionPipeline, EffectiveReceiverRow, PipelineSubmission, Release
from .scale import ScaleSpec
from .tracing import TraceReport, trace_stuck_row
from .tracing import trace_stuck_row as trace_row

SignedSpGEMM = RevisionStore
VersionedSignedSpGEMM = CorrectionPipeline
AdjacencyPipeline = CorrectionPipeline
CorrectionChain = CorrectionPipeline

__all__ = [
    "AdjacencyPipeline",
    "CommitReport",
    "Contribution",
    "CorrectionChain",
    "CorrectionPipeline",
    "diff_revisions",
    "DependencySummary",
    "EffectiveReceiverRow",
    "GraphVersion",
    "PackageBlobStore",
    "PathContribution",
    "PipelineSubmission",
    "ProductCoordinate",
    "ReceiverPackageSet",
    "ReceiverPolicy",
    "Release",
    "RevisionEdit",
    "RevisionStore",
    "RowDeployment",
    "RowPackage",
    "RowView",
    "ScaleSpec",
    "ScratchLedger",
    "signed_spgemm",
    "spgemm_values",
    "process_capture",
    "SignedSpGEMM",
    "SourceMatrix",
    "TraceReport",
    "VersionDiff",
    "VersionedSignedSpGEMM",
    "WorkspaceBudget",
    "compare_versions",
    "package_revision",
    "package_row",
    "trace_row",
    "trace_stuck_row",
]
