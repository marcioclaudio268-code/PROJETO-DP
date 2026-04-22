"""Operational pipeline for deterministic company-level mapping."""

from __future__ import annotations

import json
from pathlib import Path

from ingestion import deserialize_ingestion_result

from .config_loader import load_company_config
from .engine import map_ingestion_result
from .persistence import (
    PersistedMappingArtifacts,
    build_snapshot_summary,
    default_mapping_output_path,
    write_mapping_result,
)


def map_snapshot_with_company_config(
    snapshot_path: str | Path,
    config_path: str | Path,
    *,
    output_path: str | Path | None = None,
) -> PersistedMappingArtifacts:
    snapshot_source = Path(snapshot_path)
    config_source = Path(config_path)

    snapshot_payload = json.loads(snapshot_source.read_text(encoding="utf-8"))
    snapshot_summary = build_snapshot_summary(snapshot_payload)
    ingestion_result = deserialize_ingestion_result(snapshot_payload)
    company_config = load_company_config(config_source)

    mapping_result = map_ingestion_result(
        ingestion_result,
        company_config,
        snapshot_summary=snapshot_summary,
    )

    output_target = Path(output_path) if output_path is not None else default_mapping_output_path(snapshot_source)
    write_mapping_result(mapping_result, output_target)

    return PersistedMappingArtifacts(
        result=mapping_result,
        snapshot_path=snapshot_source,
        config_path=config_source,
        output_path=output_target,
    )

