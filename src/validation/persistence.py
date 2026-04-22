"""Persistence helpers for final validation artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ingestion import compute_file_sha256, get_engine_version

from .models import ValidationResult


def render_validation_result_json(
    result: ValidationResult,
    *,
    snapshot_path: str | Path,
    mapped_artifact_path: str | Path,
    txt_path: str | Path,
    serialization_summary_path: str | Path,
    engine_version: str | None = None,
) -> str:
    payload = serialize_validation_result(
        result,
        snapshot_path=snapshot_path,
        mapped_artifact_path=mapped_artifact_path,
        txt_path=txt_path,
        serialization_summary_path=serialization_summary_path,
        engine_version=engine_version,
    )
    return json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n"


def serialize_validation_result(
    result: ValidationResult,
    *,
    snapshot_path: str | Path,
    mapped_artifact_path: str | Path,
    txt_path: str | Path,
    serialization_summary_path: str | Path,
    engine_version: str | None = None,
) -> dict[str, Any]:
    return {
        "artifact_version": result.artifact_version,
        "execution": {
            "engine_version": engine_version or get_engine_version(),
            "status": result.status.value,
        },
        "inputs": {
            "snapshot_path": str(Path(snapshot_path)),
            "snapshot_sha256": compute_file_sha256(snapshot_path),
            "mapped_artifact_path": str(Path(mapped_artifact_path)),
            "mapped_artifact_sha256": compute_file_sha256(mapped_artifact_path),
            "txt_path": str(Path(txt_path)),
            "txt_sha256": compute_file_sha256(txt_path),
            "serialization_summary_path": str(Path(serialization_summary_path)),
            "serialization_summary_sha256": compute_file_sha256(serialization_summary_path),
        },
        "ingestion_summary": result.ingestion_summary,
        "mapping_summary": result.mapping_summary,
        "serialization_summary": result.serialization_summary,
        "validation_summary": result.validation_summary,
        "fatal_errors": [
            _serialize_issue(issue)
            for issue in result.fatal_errors
        ],
        "inconsistencies": [
            _serialize_issue(issue)
            for issue in result.inconsistencies
        ],
        "warnings": [
            _serialize_issue(issue)
            for issue in result.warnings
        ],
        "human_summary": result.human_summary,
        "recommendation": result.recommendation,
    }


def write_validation_result(
    result: ValidationResult,
    path: str | Path,
    *,
    snapshot_path: str | Path,
    mapped_artifact_path: str | Path,
    txt_path: str | Path,
    serialization_summary_path: str | Path,
    engine_version: str | None = None,
) -> Path:
    target_path = Path(path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(
        render_validation_result_json(
            result,
            snapshot_path=snapshot_path,
            mapped_artifact_path=mapped_artifact_path,
            txt_path=txt_path,
            serialization_summary_path=serialization_summary_path,
            engine_version=engine_version,
        ),
        encoding="utf-8",
    )
    return target_path


def default_validation_output_path(txt_path: str | Path) -> Path:
    path = Path(txt_path)
    if path.suffix == ".txt":
        return path.with_suffix(".validation.json")
    return path.with_name(path.name + ".validation.json")


def _serialize_issue(issue) -> dict[str, Any]:
    return {
        "code": issue.code,
        "severity": issue.severity.value,
        "message": issue.message,
        "canonical_movement_id": issue.canonical_movement_id,
        "line_number": issue.line_number,
    }

