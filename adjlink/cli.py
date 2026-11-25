"""Command line entry point for the correction-to-row-package pipeline."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .pipeline import CorrectionPipeline


def _parse_pair(value: str) -> tuple[str, str]:
    if ":" not in value:
        raise argparse.ArgumentTypeError("expected receiver:row or old:new")
    left, right = value.split(":", 1)
    return left, right


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m adjlink",
        description="Versioned signed SpGEMM and receiver row packaging",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    process = subparsers.add_parser("process", help="replay a capture and edits")
    process.add_argument("capture", type=Path)
    process.add_argument("--capacity", type=Path, default=None)
    process.add_argument("--out", type=Path, default=None)
    process.add_argument(
        "--trace",
        type=str,
        default=None,
        help="trace receiver:row, for example detector:0",
    )
    process.add_argument(
        "--diff",
        type=str,
        default=None,
        help="compare old:new revisions",
    )
    process.add_argument("--diff-receiver", type=str, default=None)
    process.add_argument("--json", action="store_true")
    return parser


def run_process(args: argparse.Namespace) -> dict[str, Any]:
    pipeline = CorrectionPipeline.from_capture(
        args.capture, capacity=args.capacity, apply_edits=True
    )
    result: dict[str, Any] = {
        "head_revision": pipeline.head.id,
        "revisions": list(pipeline.store.order),
        "commits": [report.to_dict() for report in pipeline.commit_reports],
        "receivers": {},
    }

    for receiver_id in pipeline.policies:
        package_set = pipeline.receiver_package_set(receiver_id)
        result["receivers"][receiver_id] = {
            "ready": package_set.ready,
            "blocked_rows": list(package_set.blocked_rows),
            "rebuilt_rows": list(package_set.rebuilt_rows),
            "reused_rows": list(package_set.reused_rows),
            "stuck_rows": pipeline.stuck_rows(receiver_id),
            "deployment": pipeline.deployment(receiver_id).to_dict(),
        }

    if args.out is not None:
        result["package_store"] = pipeline.write_packages(args.out)

    if args.trace:
        receiver_id, row_text = _parse_pair(args.trace)
        trace = pipeline.trace_row(receiver_id, int(row_text))
        result["trace"] = trace.to_dict()

    if args.diff:
        old_revision, new_revision = _parse_pair(args.diff)
        diff = pipeline.compare(
            old_revision,
            new_revision,
            receiver_id=args.diff_receiver,
        )
        result["diff"] = diff.to_dict()

    return result


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "process":
        result = run_process(args)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
