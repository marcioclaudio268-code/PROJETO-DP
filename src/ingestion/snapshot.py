"""Snapshot and manifest persistence for V1 ingestion."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4

from config.models import RunManifest
from domain import (
    CanonicalMovement,
    IngestionResult,
    NormalizedHours,
    PendingSeverity,
    PayrollFileParameters,
    PendingItem,
    RegistrationSource,
    ResolvedEmployee,
    SourceRef,
    ValueType,
    decimal_to_plain_string,
)

from .errors import IngestionSnapshotError


SNAPSHOT_SCHEMA_VERSION = "ingestion_snapshot_v1"


@dataclass(frozen=True, slots=True)
class PersistedIngestionArtifacts:
    result: IngestionResult
    workbook_path: Path
    snapshot_path: Path
    manifest_path: Path | None
    manifest: RunManifest | None


def get_engine_version() -> str:
    try:
        from importlib.metadata import PackageNotFoundError, version

        return version("motor-txt-dominio-folha")
    except Exception:
        return "0.1.0"


def serialize_ingestion_result(
    result: IngestionResult,
    *,
    engine_version: str | None = None,
    status: str | None = None,
) -> dict[str, Any]:
    engine = engine_version or get_engine_version()
    execution_status = status or infer_execution_status(result)

    return {
        "snapshot_version": SNAPSHOT_SCHEMA_VERSION,
        "execution": {
            "engine_version": engine,
            "layout_version": result.parameters.layout_version,
            "status": execution_status,
        },
        "parameters": _serialize_parameters(result.parameters),
        "employees": [_serialize_employee(employee) for employee in result.employees],
        "movements": [_serialize_movement(movement) for movement in result.movements],
        "pendings": [_serialize_pending(pending) for pending in result.pendings],
        "counts": summarize_ingestion_result(result),
    }


def render_ingestion_snapshot_json(
    result: IngestionResult,
    *,
    engine_version: str | None = None,
    status: str | None = None,
) -> str:
    payload = serialize_ingestion_result(
        result,
        engine_version=engine_version,
        status=status,
    )
    return json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n"


def write_ingestion_snapshot(
    result: IngestionResult,
    path: str | Path,
    *,
    engine_version: str | None = None,
    status: str | None = None,
) -> Path:
    target_path = Path(path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    content = render_ingestion_snapshot_json(
        result,
        engine_version=engine_version,
        status=status,
    )
    target_path.write_text(content, encoding="utf-8")
    return target_path


def deserialize_ingestion_result(payload: Mapping[str, Any]) -> IngestionResult:
    if payload.get("snapshot_version") != SNAPSHOT_SCHEMA_VERSION:
        raise IngestionSnapshotError(
            "snapshot_canonico_nao_suportado",
            (
                "Snapshot canonico nao suportado. "
                f"Esperado '{SNAPSHOT_SCHEMA_VERSION}' e recebido '{payload.get('snapshot_version')}'."
            ),
        )

    try:
        parameters = _deserialize_parameters(payload["parameters"])
        employees = tuple(_deserialize_employee(item) for item in payload.get("employees", ()))
        movements = tuple(_deserialize_movement(item) for item in payload.get("movements", ()))
        pendings = tuple(_deserialize_pending(item) for item in payload.get("pendings", ()))
    except KeyError as exc:
        raise IngestionSnapshotError(
            "snapshot_canonico_invalido",
            f"Snapshot canonico invalido. Campo obrigatorio ausente: {exc.args[0]}.",
        ) from exc
    except (TypeError, ValueError) as exc:
        raise IngestionSnapshotError(
            "snapshot_canonico_invalido",
            f"Snapshot canonico invalido. {exc}",
        ) from exc

    return IngestionResult(
        parameters=parameters,
        employees=employees,
        movements=movements,
        pendings=pendings,
    )


def load_ingestion_snapshot(path: str | Path) -> IngestionResult:
    snapshot_path = Path(path)
    try:
        payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise IngestionSnapshotError(
            "snapshot_canonico_ausente",
            f"Snapshot canonico nao encontrado: {snapshot_path}.",
            source=str(snapshot_path),
        ) from exc
    except json.JSONDecodeError as exc:
        raise IngestionSnapshotError(
            "snapshot_canonico_invalido",
            f"Snapshot canonico invalido. JSON malformado em {snapshot_path}.",
            source=str(snapshot_path),
        ) from exc

    return deserialize_ingestion_result(payload)


def build_ingestion_manifest(
    result: IngestionResult,
    *,
    run_id: str | None = None,
    engine_version: str | None = None,
    generated_at: datetime | None = None,
    artifact_hashes: dict[str, str] | None = None,
    status: str | None = None,
) -> RunManifest:
    manifest_generated_at = generated_at or datetime.now(timezone.utc)
    manifest_status = status or infer_execution_status(result)

    return RunManifest(
        run_id=run_id or generate_run_id(),
        engine_version=engine_version or get_engine_version(),
        company_code=result.parameters.company_code,
        company_name=result.parameters.company_name,
        competence=result.parameters.competence,
        config_version=result.parameters.layout_version,
        layout_version=result.parameters.layout_version,
        generated_at=manifest_generated_at,
        artifact_hashes=artifact_hashes or {},
        movement_count=len(result.movements),
        pending_count=len(result.pendings),
        status=manifest_status,
    )


def render_manifest_json(manifest: RunManifest) -> str:
    payload = manifest.model_dump(mode="json")
    return json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n"


def write_manifest(manifest: RunManifest, path: str | Path) -> Path:
    target_path = Path(path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(render_manifest_json(manifest), encoding="utf-8")
    return target_path


def default_snapshot_path(workbook_path: str | Path) -> Path:
    path = Path(workbook_path)
    return path.with_suffix(".snapshot.json")


def default_manifest_path(workbook_path: str | Path) -> Path:
    path = Path(workbook_path)
    return path.with_suffix(".manifest.json")


def compute_file_sha256(path: str | Path) -> str:
    digest = sha256()
    with Path(path).open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(65536), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def summarize_ingestion_result(result: IngestionResult) -> dict[str, int]:
    return {
        "employees": len(result.employees),
        "movements": len(result.movements),
        "blocked_movements": sum(1 for movement in result.movements if movement.blocked),
        "pendings": len(result.pendings),
        "blocking_pendings": sum(
            1 for pending in result.pendings if pending.severity == PendingSeverity.BLOCKING
        ),
    }


def infer_execution_status(result: IngestionResult) -> str:
    has_blocking_pending = any(
        pending.severity == PendingSeverity.BLOCKING for pending in result.pendings
    )
    if has_blocking_pending:
        return "blocked"
    if result.pendings:
        return "success_with_pending"
    return "success"


def generate_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"run-{timestamp}-{uuid4().hex[:8]}"


def _serialize_parameters(parameters: PayrollFileParameters) -> dict[str, Any]:
    return {
        "company_code": parameters.company_code,
        "company_name": parameters.company_name,
        "competence": parameters.competence,
        "payroll_type": parameters.payroll_type,
        "default_process": parameters.default_process,
        "layout_version": parameters.layout_version,
        "source_cells": dict(parameters.source_cells),
    }


def _serialize_employee(employee: ResolvedEmployee) -> dict[str, Any]:
    source = None
    if employee.source is not None:
        source = {
            "sheet_name": employee.source.sheet_name,
            "row_number": employee.source.row_number,
            "cell": employee.source.cell,
            "column_name": employee.source.column_name,
        }

    return {
        "employee_key": employee.employee_key,
        "employee_name": employee.employee_name,
        "domain_registration": employee.domain_registration,
        "status": employee.status,
        "allows_entries": employee.allows_entries,
        "resolved_from_registry": employee.resolved_from_registry,
        "registration_source": employee.registration_source.value,
        "registry_consistent": employee.registry_consistent,
        "source": source,
    }


def _serialize_movement(movement: CanonicalMovement) -> dict[str, Any]:
    return {
        "movement_id": movement.movement_id,
        "company_code": movement.company_code,
        "competence": movement.competence,
        "payroll_type": movement.payroll_type,
        "default_process": movement.default_process,
        "employee_key": movement.employee_key,
        "employee_name": movement.employee_name,
        "domain_registration": movement.domain_registration,
        "event_name": movement.event_name,
        "value_type": movement.value_type.value,
        "quantity": decimal_to_plain_string(movement.quantity) if movement.quantity is not None else None,
        "hours": (
            {
                "text": movement.hours.text,
                "total_minutes": movement.hours.total_minutes,
            }
            if movement.hours is not None
            else None
        ),
        "amount": decimal_to_plain_string(movement.amount) if movement.amount is not None else None,
        "source": {
            "sheet_name": movement.source.sheet_name,
            "row_number": movement.source.row_number,
            "cell": movement.source.cell,
            "column_name": movement.source.column_name,
        },
        "blocked": movement.blocked,
        "pending_codes": list(movement.pending_codes),
        "pending_messages": list(movement.pending_messages),
        "observation": movement.observation,
        "informed_rubric": movement.informed_rubric,
        "output_rubric": movement.output_rubric,
        "event_nature": movement.event_nature,
        "serialization_unit": movement.serialization_unit,
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


def _deserialize_parameters(payload: Mapping[str, Any]) -> PayrollFileParameters:
    return PayrollFileParameters(
        company_code=str(payload["company_code"]),
        company_name=str(payload["company_name"]),
        competence=str(payload["competence"]),
        payroll_type=str(payload["payroll_type"]),
        default_process=str(payload["default_process"]),
        layout_version=str(payload["layout_version"]),
        source_cells={str(key): str(value) for key, value in dict(payload.get("source_cells", {})).items()},
    )


def _deserialize_employee(payload: Mapping[str, Any]) -> ResolvedEmployee:
    return ResolvedEmployee(
        employee_key=payload.get("employee_key"),
        employee_name=payload.get("employee_name"),
        domain_registration=payload.get("domain_registration"),
        status=payload.get("status"),
        allows_entries=payload.get("allows_entries"),
        source=_deserialize_source(payload.get("source")),
        resolved_from_registry=bool(payload["resolved_from_registry"]),
        registration_source=RegistrationSource(payload["registration_source"]),
        registry_consistent=bool(payload.get("registry_consistent", True)),
    )


def _deserialize_movement(payload: Mapping[str, Any]) -> CanonicalMovement:
    hours_payload = payload.get("hours")
    hours = None
    if hours_payload is not None:
        hours = NormalizedHours(
            text=str(hours_payload["text"]),
            total_minutes=int(hours_payload["total_minutes"]),
        )

    quantity = payload.get("quantity")
    amount = payload.get("amount")

    return CanonicalMovement(
        movement_id=str(payload["movement_id"]),
        company_code=str(payload["company_code"]),
        competence=str(payload["competence"]),
        payroll_type=str(payload["payroll_type"]),
        default_process=str(payload["default_process"]),
        employee_key=payload.get("employee_key"),
        employee_name=payload.get("employee_name"),
        domain_registration=payload.get("domain_registration"),
        event_name=str(payload["event_name"]),
        value_type=ValueType(payload["value_type"]),
        quantity=Decimal(str(quantity)) if quantity is not None else None,
        hours=hours,
        amount=Decimal(str(amount)) if amount is not None else None,
        source=_deserialize_source(payload["source"]),
        blocked=bool(payload.get("blocked", False)),
        pending_codes=tuple(str(item) for item in payload.get("pending_codes", ())),
        pending_messages=tuple(str(item) for item in payload.get("pending_messages", ())),
        observation=payload.get("observation"),
        informed_rubric=payload.get("informed_rubric"),
        output_rubric=payload.get("output_rubric"),
        event_nature=payload.get("event_nature"),
        serialization_unit=payload.get("serialization_unit"),
    )


def _deserialize_pending(payload: Mapping[str, Any]) -> PendingItem:
    return PendingItem(
        pending_id=str(payload["pending_id"]),
        severity=PendingSeverity(payload["severity"]),
        company_code=payload.get("company_code"),
        competence=payload.get("competence"),
        employee_key=payload.get("employee_key"),
        employee_name=payload.get("employee_name"),
        domain_registration=payload.get("domain_registration"),
        event_name=payload.get("event_name"),
        source=_deserialize_source(payload["source"]),
        pending_code=str(payload["pending_code"]),
        description=str(payload["description"]),
        recommended_action=str(payload["recommended_action"]),
        treatment_status=str(payload.get("treatment_status", "aberta")),
        manual_resolution=payload.get("manual_resolution"),
        resolved_by=payload.get("resolved_by"),
        resolved_at=payload.get("resolved_at"),
    )


def _deserialize_source(payload: Mapping[str, Any] | None) -> SourceRef | None:
    if payload is None:
        return None

    return SourceRef(
        sheet_name=str(payload["sheet_name"]),
        row_number=int(payload["row_number"]),
        cell=str(payload["cell"]),
        column_name=payload.get("column_name"),
    )
