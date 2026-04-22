"""Operational pipeline for final validation and reconciliation."""

from __future__ import annotations

from pathlib import Path

from ingestion import load_ingestion_snapshot

from .artifacts import (
    load_mapped_artifact_for_validation,
    load_serialization_summary,
    load_txt_lines,
)
from .models import PersistedFinalValidationArtifacts
from .persistence import default_validation_output_path, write_validation_result
from .reconciliation import validate_final_artifacts


def validate_pipeline_v1(
    *,
    snapshot_path: str | Path,
    mapped_artifact_path: str | Path,
    txt_path: str | Path,
    serialization_summary_path: str | Path,
    output_path: str | Path | None = None,
) -> PersistedFinalValidationArtifacts:
    snapshot_source = Path(snapshot_path)
    mapped_source = Path(mapped_artifact_path)
    txt_source = Path(txt_path)
    summary_source = Path(serialization_summary_path)

    snapshot_result = load_ingestion_snapshot(snapshot_source)
    mapped_artifact = load_mapped_artifact_for_validation(mapped_source)
    serialization_summary = load_serialization_summary(summary_source)
    txt_lines = load_txt_lines(txt_source)

    result = validate_final_artifacts(
        snapshot_result=snapshot_result,
        mapped_artifact=mapped_artifact,
        serialization_summary=serialization_summary,
        txt_lines=txt_lines,
        mapped_artifact_path=mapped_source,
        txt_path=txt_source,
    )

    output_target = Path(output_path) if output_path is not None else default_validation_output_path(txt_source)
    write_validation_result(
        result,
        output_target,
        snapshot_path=snapshot_source,
        mapped_artifact_path=mapped_source,
        txt_path=txt_source,
        serialization_summary_path=summary_source,
    )

    return PersistedFinalValidationArtifacts(
        result=result,
        snapshot_path=snapshot_source,
        mapped_artifact_path=mapped_source,
        txt_path=txt_source,
        serialization_summary_path=summary_source,
        output_path=output_target,
    )

