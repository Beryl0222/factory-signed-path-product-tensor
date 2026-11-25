"""Content-addressed row packet storage with atomic snapshot switches."""
from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
from typing import Any

from .pipeline import Release


def atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


class PackageBlobStore:
    """Write only new row blobs and switch releases through one pointer file."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.packets = self.root / "packets"
        self.manifests = self.root / "manifests"
        self.deployments = self.root / "deployments"
        self.releases = self.root / "releases"

    def packet_path(self, package_hash: str) -> Path:
        safe = package_hash.replace("/", "_").replace("..", "_")
        return self.packets / f"{safe}.json"

    def write_packet(self, receiver_id: str, row: int, package: Any) -> tuple[Path, bool]:
        if not package.deliverable or not package.wire_hash:
            raise ValueError("cannot write a blocked or hashless packet")
        path = self.packet_path(package.package_hash)
        if path.exists():
            return path, False
        payload = {
            "format": "adjlink.row-packet/v1",
            "receiver_id": receiver_id,
            "row": row,
            "content_revision": package.revision,
            "wire_hash": package.wire_hash,
            "package_hash": package.package_hash,
            "wire": [entry.to_wire() for entry in package.entries],
        }
        atomic_write_json(path, payload)
        return path, True

    def publish_release(self, release: Release) -> dict[str, Any]:
        written: list[str] = []
        reused: list[str] = []
        target_manifests: dict[str, str] = {}
        deployment_manifests: dict[str, str] = {}

        for receiver_id, package_set in release.packages.items():
            for row, package in package_set.rows.items():
                if not package.deliverable or not package.wire_hash:
                    continue
                _, did_write = self.write_packet(receiver_id, row, package)
                bucket = written if did_write else reused
                bucket.append(package.package_hash)

            target_path = (
                self.manifests / receiver_id / f"{package_set.revision}.json"
            )
            atomic_write_json(target_path, package_set.to_dict())
            target_manifests[receiver_id] = str(
                target_path.relative_to(self.root)
            )

            deployment = release.deployments[receiver_id]
            deployment_payload = {
                "format": "adjlink.deployment/v1",
                "graph_revision": release.graph.id,
                **deployment.to_dict(),
                "target_manifest": target_manifests[receiver_id],
            }
            deployment_path = (
                self.deployments / receiver_id / f"{release.graph.id}.json"
            )
            atomic_write_json(deployment_path, deployment_payload)
            deployment_manifests[receiver_id] = str(
                deployment_path.relative_to(self.root)
            )

        snapshot = {
            "format": "adjlink.snapshot/v1",
            "graph_revision": release.graph.id,
            "receivers": sorted(release.packages),
            "target_manifests": target_manifests,
            "deployment_manifests": deployment_manifests,
        }
        snapshot_path = self.releases / f"{release.graph.id}.json"
        atomic_write_json(snapshot_path, snapshot)
        snapshot_relative = str(snapshot_path.relative_to(self.root))

        pointer = {
            "format": "adjlink.current-release/v1",
            "graph_revision": release.graph.id,
            "current_snapshot": snapshot_relative,
        }
        # Single atomic linearization point.  Readers either resolve the old
        # immutable snapshot or the new immutable snapshot.
        atomic_write_json(self.root / "current-release.json", pointer)
        # Familiar stable pointer name as well.
        atomic_write_json(self.root / "release.json", pointer)

        # Backwards-compatible convenience pointer.  Correct snapshot readers
        # should use current-release.json; this file is replaced atomically.
        for receiver_id, relative_path in deployment_manifests.items():
            atomic_write_json(
                self.root / "current" / f"{receiver_id}.json",
                {
                    "format": "adjlink.current-receiver/v1",
                    "snapshot": snapshot_relative,
                    "deployment_manifest": relative_path,
                },
            )

        return {
            "root": str(self.root),
            "graph_revision": release.graph.id,
            "packets_written": sorted(set(written)),
            "packets_reused": sorted(set(reused)),
            "receiver_manifests": target_manifests,
            "deployment_manifests": deployment_manifests,
            "snapshot_manifest": str(snapshot_path),
            "current_pointer": str(self.root / "current-release.json"),
            # Kept for callers expecting the old key name.
            "release_manifest": str(self.root / "current-release.json"),
        }

    def read_pointer(self) -> dict[str, Any]:
        return json.loads(
            (self.root / "current-release.json").read_text(encoding="utf-8")
        )

    def read_snapshot(self) -> dict[str, Any]:
        pointer = self.read_pointer()
        path = self.root / pointer["current_snapshot"]
        return json.loads(path.read_text(encoding="utf-8"))

    def read_current(self, receiver_id: str) -> dict[str, Any]:
        """Read one receiver deployment from the current atomic snapshot."""
        snapshot = self.read_snapshot()
        relative = snapshot["deployment_manifests"][receiver_id]
        return json.loads((self.root / relative).read_text(encoding="utf-8"))

    def read_packet(self, package_hash: str) -> dict[str, Any]:
        return json.loads(self.packet_path(package_hash).read_text(encoding="utf-8"))
