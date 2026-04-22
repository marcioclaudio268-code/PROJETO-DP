"""Persistence helpers for fixed-width TXT serialization artifacts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ingestion import compute_file_sha256, get_engine_version

from .encoder import render_serialized_txt
from .models import SerializationResult


SERIALIZATION_SUMMARY_VERSION = "serialization_summary_v1"


@dataclass(frozen=True, slots=True)
class PersistedSerializationArtifacts:
    result: SerializationResult
    mapped_artifact_path: Path
    txt_path: Path
    summary_path: Path


def serialize_serialization_summary(
    result: SerializationResult,
    *,
    mapped_artifact_path: str | Path,
    txt_path: str | Path,
    engine_version: str | None = None,
) -> dict[str, Any]:
    reason_counts: dict[str, int] = {}
    for item in result.skipped_items:
        reason_counts[item.reason_code] = reason_counts.get(item.reason_code, 0) + 1

    total = result.total_mapped_movements
    serialized = len(result.serialized_lines)
    skipped = len(result.skipped_items)

    return {
        "artifact_version": SERIALIZATION_SUMMARY_VERSION,
        "execution": {
            "engine_version": engine_version or get_engine_version(),
            "status": infer_serialization_status(result),
        },
        "input": {
            "mapped_artifact_path": str(Path(mapped_artifact_path)),
            "mapped_artifact_version": result.metadata.artifact_version,
            "snapshot_version": result.metadata.snapshot_version,
            "company_code": result.metadata.company_code,
            "company_name": result.metadata.company_name,
            "competence": result.metadata.competence,
            "config_version": result.metadata.config_version,
            "mapped_artifact_sha256": compute_file_sha256(mapped_artifact_path),
        },
        "counts": {
            "total_mapped_movements": total,
            "serialized": serialized,
            "non_serialized": skipped,
            "blocked_or_non_serialized": skipped,
        },
        "non_serialized_reason_counts": reason_counts,
        "non_serialized_movements": [
            {
                "canonical_movement_id": item.canonical_movement_id,
                "reason_code": item.reason_code,
                "message": item.message,
            }
            for item in result.skipped_items
        ],
        "txt_path": str(Path(txt_path)),
        "txt_sha256": compute_file_sha256(txt_path),
    }


def render_serialization_summary_json(
    result: SerializationResult,
    *,
    mapped_artifact_path: str | Path,
    txt_path: str | Path,
    engine_version: str | None = None,
) -> str:
    payload = serialize_serialization_summary(
        result,
        mapped_artifact_path=mapped_artifact_path,
        txt_path=txt_path,
        engine_version=engine_version,
    )
    return json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n"


def write_serialized_txt(result: SerializationResult, path: str | Path) -> Path:
    target_path = Path(path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(render_serialized_txt(result), encoding="utf-8", newline="\n")
    return target_path


def write_serialization_summary(
    result: SerializationResult,
    path: str | Path,
    *,
    mapped_artifact_path: str | Path,
    txt_path: str | Path,
    engine_version: str | None = None,
) -> Path:
    target_path = Path(path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(
        render_serialization_summary_json(
            result,
            mapped_artifact_path=mapped_artifact_path,
            txt_path=txt_path,
            engine_version=engine_version,
        ),
        encoding="utf-8",
    )
    return target_path


def default_txt_output_path(mapped_artifact_path: str | Path) -> Path:
    path = Path(mapped_artifact_path)
    if path.name.endswith(".mapped.json"):
        return path.with_name(path.name.replace(".mapped.json", ".txt"))
    return path.with_suffix(".txt")


def default_serialization_summary_path(mapped_artifact_path: str | Path) -> Path:
    path = Path(mapped_artifact_path)
    if path.name.endswith(".mapped.json"):
        return path.with_name(path.name.replace(".mapped.json", ".serialization.json"))
    return path.with_suffix(".serialization.json")


def infer_serialization_status(result: SerializationResult) -> str:
    if result.total_mapped_movements == 0:
        return "empty"
    if not result.serialized_lines:
        return "blocked"
    if result.skipped_items:
        return "success_with_exclusions"
    return "success"
