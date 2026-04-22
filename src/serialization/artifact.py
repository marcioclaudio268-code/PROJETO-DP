"""Load the persisted mapped artifact consumed by the serializer."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from domain import SourceRef, ValueType
from mapping import MappingStatus

from .errors import SerializationInputError
from .models import (
    LoadedMappedArtifact,
    MappedArtifactMetadata,
    SerializableMappedMovement,
)


SUPPORTED_MAPPING_ARTIFACT_VERSION = "mapping_result_v1"


def load_mapped_artifact(path: str | Path) -> LoadedMappedArtifact:
    artifact_path = Path(path)

    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise SerializationInputError(
            "artefato_mapeado_ausente",
            f"Artefato mapeado nao encontrado: {artifact_path}.",
            source=str(artifact_path),
        ) from exc
    except json.JSONDecodeError as exc:
        raise SerializationInputError(
            "artefato_mapeado_invalido",
            f"Artefato mapeado invalido. JSON malformado em {artifact_path}.",
            source=str(artifact_path),
        ) from exc

    return deserialize_mapped_artifact(payload)


def deserialize_mapped_artifact(payload: Mapping[str, Any]) -> LoadedMappedArtifact:
    if payload.get("artifact_version") != SUPPORTED_MAPPING_ARTIFACT_VERSION:
        raise SerializationInputError(
            "artefato_mapeado_nao_suportado",
            (
                "Artefato mapeado nao suportado. "
                f"Esperado '{SUPPORTED_MAPPING_ARTIFACT_VERSION}' e recebido '{payload.get('artifact_version')}'."
            ),
        )

    try:
        metadata = _deserialize_metadata(payload)
        movements = tuple(_deserialize_movement(item) for item in payload.get("mapped_movements", ()))
    except KeyError as exc:
        raise SerializationInputError(
            "artefato_mapeado_invalido",
            f"Artefato mapeado invalido. Campo obrigatorio ausente: {exc.args[0]}.",
        ) from exc
    except (TypeError, ValueError) as exc:
        raise SerializationInputError(
            "artefato_mapeado_invalido",
            f"Artefato mapeado invalido. {exc}",
        ) from exc

    return LoadedMappedArtifact(metadata=metadata, movements=movements)


def _deserialize_metadata(payload: Mapping[str, Any]) -> MappedArtifactMetadata:
    execution = payload["execution"]
    snapshot = payload["snapshot"]
    config = payload["config"]

    return MappedArtifactMetadata(
        artifact_version=str(payload["artifact_version"]),
        engine_version=str(execution["engine_version"]),
        execution_status=str(execution["status"]),
        snapshot_version=str(snapshot["snapshot_version"]),
        company_code=str(config["company_code"]),
        company_name=str(config["company_name"]),
        competence=str(config["competence"]),
        config_version=str(config["config_version"]),
    )


def _deserialize_movement(payload: Mapping[str, Any]) -> SerializableMappedMovement:
    hours_payload = payload.get("hours")
    source_payload = payload["source"]

    return SerializableMappedMovement(
        canonical_movement_id=str(payload["canonical_movement_id"]),
        company_code=str(payload["company_code"]),
        competence=str(payload["competence"]),
        payroll_type=str(payload["payroll_type"]),
        default_process=str(payload["default_process"]),
        employee_key=payload.get("employee_key"),
        employee_name=payload.get("employee_name"),
        event_name=str(payload["event_name"]),
        value_type=ValueType(payload["value_type"]),
        quantity=payload.get("quantity"),
        hours_text=(str(hours_payload["text"]) if hours_payload is not None else None),
        hours_total_minutes=(int(hours_payload["total_minutes"]) if hours_payload is not None else None),
        amount=payload.get("amount"),
        source=SourceRef(
            sheet_name=str(source_payload["sheet_name"]),
            row_number=int(source_payload["row_number"]),
            cell=str(source_payload["cell"]),
            column_name=source_payload.get("column_name"),
        ),
        canonical_domain_registration=payload.get("canonical_domain_registration"),
        resolved_domain_registration=payload.get("resolved_domain_registration"),
        output_rubric=payload.get("output_rubric"),
        status=MappingStatus(payload["status"]),
        canonical_blocked=bool(payload.get("canonical_blocked", False)),
        inherited_pending_codes=tuple(str(item) for item in payload.get("inherited_pending_codes", ())),
        inherited_pending_messages=tuple(str(item) for item in payload.get("inherited_pending_messages", ())),
        mapping_pending_codes=tuple(str(item) for item in payload.get("mapping_pending_codes", ())),
        mapping_pending_messages=tuple(str(item) for item in payload.get("mapping_pending_messages", ())),
        observation=payload.get("observation"),
        informed_rubric=payload.get("informed_rubric"),
        event_nature=payload.get("event_nature"),
        serialization_unit=payload.get("serialization_unit"),
    )

