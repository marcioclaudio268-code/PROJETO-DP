"""Operational pipeline helpers for V1 ingestion."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from .snapshot import (
    PersistedIngestionArtifacts,
    build_ingestion_manifest,
    compute_file_sha256,
    default_manifest_path,
    default_snapshot_path,
    get_engine_version,
    write_ingestion_snapshot,
    write_manifest,
)
from .template_v1_loader import load_planilha_padrao_folha_v1, write_ingestion_result_to_workbook


def ingest_fill_and_persist_planilha_padrao_v1(
    input_path: str | Path,
    *,
    output_path: str | Path | None = None,
    snapshot_path: str | Path | None = None,
    manifest_path: str | Path | None = None,
    write_manifest_file: bool = True,
    run_id: str | None = None,
    generated_at: datetime | None = None,
) -> PersistedIngestionArtifacts:
    """Run ingestion, update workbook tabs and persist snapshot/manfiest artifacts."""

    input_workbook_path = Path(input_path)
    input_hash = compute_file_sha256(input_workbook_path)
    engine_version = get_engine_version()

    result = load_planilha_padrao_folha_v1(input_workbook_path)
    workbook_path = Path(write_ingestion_result_to_workbook(input_workbook_path, result, output_path=output_path))

    snapshot_target = Path(snapshot_path) if snapshot_path is not None else default_snapshot_path(workbook_path)
    write_ingestion_snapshot(
        result,
        snapshot_target,
        engine_version=engine_version,
    )

    artifact_hashes = {
        "input_workbook": input_hash,
        "workbook_with_technical_tabs": compute_file_sha256(workbook_path),
        "canonical_snapshot": compute_file_sha256(snapshot_target),
    }

    manifest = None
    written_manifest_path = None
    if write_manifest_file:
        manifest = build_ingestion_manifest(
            result,
            run_id=run_id,
            engine_version=engine_version,
            generated_at=generated_at,
            artifact_hashes=artifact_hashes,
        )
        written_manifest_path = Path(manifest_path) if manifest_path is not None else default_manifest_path(workbook_path)
        write_manifest(manifest, written_manifest_path)

    return PersistedIngestionArtifacts(
        result=result,
        workbook_path=workbook_path,
        snapshot_path=snapshot_target,
        manifest_path=written_manifest_path,
        manifest=manifest,
    )
