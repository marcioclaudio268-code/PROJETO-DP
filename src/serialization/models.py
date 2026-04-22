"""Typed models for fixed-width TXT serialization."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from domain import SourceRef, ValueType
from mapping import MappingStatus


class SerializationSkipCode(StrEnum):
    MOVEMENT_NOT_READY = "movimento_bloqueado"
    MISSING_DOMAIN_REGISTRATION = "matricula_final_ausente"
    MISSING_OUTPUT_RUBRIC = "rubrica_saida_ausente"
    FIELD_OVERFLOW = "campo_excede_largura"
    INVALID_NUMERIC_FIELD = "campo_numerico_invalido"
    INVALID_VALUE_PAYLOAD = "valor_movimento_invalido"


@dataclass(frozen=True, slots=True)
class MappedArtifactMetadata:
    artifact_version: str
    engine_version: str
    execution_status: str
    snapshot_version: str
    company_code: str
    company_name: str
    competence: str
    config_version: str


@dataclass(frozen=True, slots=True)
class SerializableMappedMovement:
    canonical_movement_id: str
    company_code: str
    competence: str
    payroll_type: str
    default_process: str
    employee_key: str | None
    employee_name: str | None
    event_name: str
    value_type: ValueType
    quantity: str | None
    hours_text: str | None
    hours_total_minutes: int | None
    amount: str | None
    source: SourceRef
    canonical_domain_registration: str | None
    resolved_domain_registration: str | None
    output_rubric: str | None
    status: MappingStatus
    canonical_blocked: bool
    inherited_pending_codes: tuple[str, ...]
    inherited_pending_messages: tuple[str, ...]
    mapping_pending_codes: tuple[str, ...]
    mapping_pending_messages: tuple[str, ...]
    observation: str | None
    informed_rubric: str | None
    event_nature: str | None
    serialization_unit: str | None


@dataclass(frozen=True, slots=True)
class LoadedMappedArtifact:
    metadata: MappedArtifactMetadata
    movements: tuple[SerializableMappedMovement, ...]


@dataclass(frozen=True, slots=True)
class SerializedTxtLine:
    canonical_movement_id: str
    line_number: int
    text: str


@dataclass(frozen=True, slots=True)
class SerializationSkipItem:
    canonical_movement_id: str
    reason_code: SerializationSkipCode
    message: str


@dataclass(frozen=True, slots=True)
class SerializationResult:
    metadata: MappedArtifactMetadata
    total_mapped_movements: int
    serialized_lines: tuple[SerializedTxtLine, ...]
    skipped_items: tuple[SerializationSkipItem, ...]

