"""Load persisted artifacts consumed by final validation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from serialization import deserialize_mapped_artifact

from .errors import FinalValidationInputError
from .models import (
    LoadedMappedArtifactEnvelope,
    LoadedSerializationSummary,
    SerializationSummarySkippedItem,
)


SUPPORTED_SERIALIZATION_SUMMARY_VERSION = "serialization_summary_v1"


def load_mapped_artifact_for_validation(path: str | Path) -> LoadedMappedArtifactEnvelope:
    artifact_path = Path(path)

    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise FinalValidationInputError(
            "artefato_mapeado_ausente",
            f"Artefato mapeado nao encontrado: {artifact_path}.",
            source=str(artifact_path),
        ) from exc
    except json.JSONDecodeError as exc:
        raise FinalValidationInputError(
            "artefato_mapeado_invalido",
            f"Artefato mapeado invalido. JSON malformado em {artifact_path}.",
            source=str(artifact_path),
        ) from exc

    artifact = deserialize_mapped_artifact(payload)
    raw_counts = {
        str(key): int(value)
        for key, value in dict(payload.get("counts", {})).items()
    }
    mapping_pending_count = len(tuple(payload.get("mapping_pendings", ())))

    return LoadedMappedArtifactEnvelope(
        artifact=artifact,
        raw_counts=raw_counts,
        mapping_pending_count=mapping_pending_count,
    )


def load_serialization_summary(path: str | Path) -> LoadedSerializationSummary:
    summary_path = Path(path)

    try:
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise FinalValidationInputError(
            "resumo_serializacao_ausente",
            f"Resumo da serializacao nao encontrado: {summary_path}.",
            source=str(summary_path),
        ) from exc
    except json.JSONDecodeError as exc:
        raise FinalValidationInputError(
            "resumo_serializacao_invalido",
            f"Resumo da serializacao invalido. JSON malformado em {summary_path}.",
            source=str(summary_path),
        ) from exc

    return deserialize_serialization_summary(payload)


def deserialize_serialization_summary(payload: Mapping[str, Any]) -> LoadedSerializationSummary:
    if payload.get("artifact_version") != SUPPORTED_SERIALIZATION_SUMMARY_VERSION:
        raise FinalValidationInputError(
            "resumo_serializacao_nao_suportado",
            (
                "Resumo da serializacao nao suportado. "
                f"Esperado '{SUPPORTED_SERIALIZATION_SUMMARY_VERSION}' e recebido '{payload.get('artifact_version')}'."
            ),
        )

    try:
        execution = payload["execution"]
        input_payload = payload["input"]
        counts = payload["counts"]
    except KeyError as exc:
        raise FinalValidationInputError(
            "resumo_serializacao_invalido",
            f"Resumo da serializacao invalido. Campo obrigatorio ausente: {exc.args[0]}.",
        ) from exc

    return LoadedSerializationSummary(
        artifact_version=str(payload["artifact_version"]),
        execution_status=str(execution["status"]),
        mapped_artifact_path=str(input_payload["mapped_artifact_path"]),
        mapped_artifact_version=str(input_payload["mapped_artifact_version"]),
        snapshot_version=str(input_payload["snapshot_version"]),
        company_code=str(input_payload["company_code"]),
        company_name=str(input_payload["company_name"]),
        competence=str(input_payload["competence"]),
        config_version=str(input_payload["config_version"]),
        mapped_artifact_sha256=str(input_payload["mapped_artifact_sha256"]),
        total_mapped_movements=int(counts["total_mapped_movements"]),
        serialized=int(counts["serialized"]),
        non_serialized=int(counts["non_serialized"]),
        blocked_or_non_serialized=int(counts["blocked_or_non_serialized"]),
        non_serialized_reason_counts={
            str(key): int(value)
            for key, value in dict(payload.get("non_serialized_reason_counts", {})).items()
        },
        non_serialized_movements=tuple(
            SerializationSummarySkippedItem(
                canonical_movement_id=str(item["canonical_movement_id"]),
                reason_code=str(item["reason_code"]),
                message=str(item["message"]),
            )
            for item in payload.get("non_serialized_movements", ())
        ),
        txt_path=str(payload["txt_path"]),
        txt_sha256=str(payload["txt_sha256"]),
    )


def load_txt_lines(path: str | Path) -> tuple[str, ...]:
    txt_path = Path(path)

    try:
        content = txt_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise FinalValidationInputError(
            "txt_ausente",
            f"TXT nao encontrado: {txt_path}.",
            source=str(txt_path),
        ) from exc

    if content == "":
        return ()

    return tuple(content.splitlines())

