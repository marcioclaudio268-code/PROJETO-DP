"""Persistence helpers for deterministic company-level mapping output."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from domain import PendingItem, decimal_to_plain_string
from ingestion.snapshot import SNAPSHOT_SCHEMA_VERSION, get_engine_version

from .engine import infer_mapping_execution_status, summarize_mapping_result
from .models import MappedMovement, MappingResult, SnapshotSummary


MAPPING_ARTIFACT_VERSION = "mapping_result_v1"


@dataclass(frozen=True, slots=True)
class PersistedMappingArtifacts:
    result: MappingResult
    snapshot_path: Path
    config_path: Path
    output_path: Path


def build_snapshot_summary(snapshot_payload: Mapping[str, Any]) -> SnapshotSummary:
    if snapshot_payload.get("snapshot_version") != SNAPSHOT_SCHEMA_VERSION:
        raise ValueError(
            f"Snapshot canonico nao suportado. Esperado '{SNAPSHOT_SCHEMA_VERSION}'."
        )

    parameters = snapshot_payload["parameters"]
    counts = snapshot_payload.get("counts", {})
    execution = snapshot_payload.get("execution", {})

    return SnapshotSummary(
        snapshot_version=str(snapshot_payload["snapshot_version"]),
        company_code=str(parameters["company_code"]),
        company_name=str(parameters["company_name"]),
        competence=str(parameters["competence"]),
        layout_version=str(parameters["layout_version"]),
        movement_count=int(counts.get("movements", len(snapshot_payload.get("movements", ())))),
        pending_count=int(counts.get("pendings", len(snapshot_payload.get("pendings", ())))),
        execution_status=str(execution.get("status", "desconhecido")),
    )


def serialize_mapping_result(
    result: MappingResult,
    *,
    engine_version: str | None = None,
    status: str | None = None,
) -> dict[str, Any]:
    execution_status = status or infer_mapping_execution_status(result)

    return {
        "artifact_version": MAPPING_ARTIFACT_VERSION,
        "execution": {
            "engine_version": engine_version or get_engine_version(),
            "status": execution_status,
        },
        "snapshot": {
            "snapshot_version": result.snapshot.snapshot_version,
            "company_code": result.snapshot.company_code,
            "company_name": result.snapshot.company_name,
            "competence": result.snapshot.competence,
            "layout_version": result.snapshot.layout_version,
            "movement_count": result.snapshot.movement_count,
            "pending_count": result.snapshot.pending_count,
            "execution_status": result.snapshot.execution_status,
        },
        "config": {
            "company_code": result.applied_config.company_code,
            "company_name": result.applied_config.company_name,
            "competence": result.applied_config.competence,
            "config_version": result.applied_config.config_version,
            "default_process": result.applied_config.default_process,
            "active_event_mappings": result.applied_config.active_event_mappings,
            "active_employee_mappings": result.applied_config.active_employee_mappings,
        },
        "mapped_movements": [_serialize_mapped_movement(item) for item in result.mapped_movements],
        "mapping_pendings": [_serialize_pending(item) for item in result.pendings],
        "counts": summarize_mapping_result(result),
    }


def render_mapping_result_json(
    result: MappingResult,
    *,
    engine_version: str | None = None,
    status: str | None = None,
) -> str:
    payload = serialize_mapping_result(
        result,
        engine_version=engine_version,
        status=status,
    )
    return json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n"


def write_mapping_result(
    result: MappingResult,
    path: str | Path,
    *,
    engine_version: str | None = None,
    status: str | None = None,
) -> Path:
    target_path = Path(path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(
        render_mapping_result_json(result, engine_version=engine_version, status=status),
        encoding="utf-8",
    )
    return target_path


def default_mapping_output_path(snapshot_path: str | Path) -> Path:
    path = Path(snapshot_path)
    if path.name.endswith(".snapshot.json"):
        return path.with_name(path.name.replace(".snapshot.json", ".mapped.json"))
    return path.with_suffix(".mapped.json")


def _serialize_mapped_movement(movement: MappedMovement) -> dict[str, Any]:
    canonical = movement.canonical_movement

    return {
        "canonical_movement_id": canonical.movement_id,
        "company_code": canonical.company_code,
        "competence": canonical.competence,
        "payroll_type": canonical.payroll_type,
        "default_process": canonical.default_process,
        "employee_key": canonical.employee_key,
        "employee_name": canonical.employee_name,
        "event_name": canonical.event_name,
        "value_type": canonical.value_type.value,
        "quantity": (
            decimal_to_plain_string(canonical.quantity) if canonical.quantity is not None else None
        ),
        "hours": (
            {
                "text": canonical.hours.text,
                "total_minutes": canonical.hours.total_minutes,
            }
            if canonical.hours is not None
            else None
        ),
        "amount": decimal_to_plain_string(canonical.amount) if canonical.amount is not None else None,
        "source": {
            "sheet_name": canonical.source.sheet_name,
            "row_number": canonical.source.row_number,
            "cell": canonical.source.cell,
            "column_name": canonical.source.column_name,
        },
        "canonical_domain_registration": canonical.domain_registration,
        "resolved_domain_registration": movement.resolved_domain_registration,
        "employee_resolution_source": movement.employee_resolution_source.value,
        "output_rubric": movement.output_rubric,
        "rubric_resolution_source": movement.rubric_resolution_source.value,
        "status": movement.status.value,
        "canonical_blocked": canonical.blocked,
        "inherited_pending_codes": list(movement.inherited_pending_codes),
        "inherited_pending_messages": list(movement.inherited_pending_messages),
        "mapping_pending_codes": list(movement.mapping_pending_codes),
        "mapping_pending_messages": list(movement.mapping_pending_messages),
        "observation": canonical.observation,
        "informed_rubric": canonical.informed_rubric,
        "event_nature": canonical.event_nature,
        "serialization_unit": canonical.serialization_unit,
    }


def _serialize_pending(pending: PendingItem) -> dict[str, Any]:
    return {
        "pending_id": pending.pending_id,
        "severity": pending.severity.value,
        "company_code": pending.company_code,
        "competence": pending.competence,
        "employee_key": pending.employee_key,
        "employee_name": pending.employee_name,
        "domain_registration": pending.domain_registration,
        "event_name": pending.event_name,
        "source": {
            "sheet_name": pending.source.sheet_name,
            "row_number": pending.source.row_number,
            "cell": pending.source.cell,
            "column_name": pending.source.column_name,
        },
        "pending_code": pending.pending_code,
        "description": pending.description,
        "recommended_action": pending.recommended_action,
        "treatment_status": pending.treatment_status,
        "manual_resolution": pending.manual_resolution,
        "resolved_by": pending.resolved_by,
        "resolved_at": pending.resolved_at,
    }
