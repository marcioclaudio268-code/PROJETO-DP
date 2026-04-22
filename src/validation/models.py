"""Typed models for final validation and reconciliation."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any


class ValidationStatus(StrEnum):
    SUCCESS = "success"
    SUCCESS_WITH_WARNINGS = "success_with_warnings"
    BLOCKED = "blocked"
    EMPTY = "empty"


class ValidationSeverity(StrEnum):
    FATAL = "fatal"
    WARNING = "warning"


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    code: str
    severity: ValidationSeverity
    message: str
    canonical_movement_id: str | None = None
    line_number: int | None = None


@dataclass(frozen=True, slots=True)
class SerializationSummarySkippedItem:
    canonical_movement_id: str
    reason_code: str
    message: str


@dataclass(frozen=True, slots=True)
class LoadedSerializationSummary:
    artifact_version: str
    execution_status: str
    mapped_artifact_path: str
    mapped_artifact_version: str
    snapshot_version: str
    company_code: str
    company_name: str
    competence: str
    config_version: str
    mapped_artifact_sha256: str
    total_mapped_movements: int
    serialized: int
    non_serialized: int
    blocked_or_non_serialized: int
    non_serialized_reason_counts: dict[str, int]
    non_serialized_movements: tuple[SerializationSummarySkippedItem, ...]
    txt_path: str
    txt_sha256: str


@dataclass(frozen=True, slots=True)
class LoadedMappedArtifactEnvelope:
    artifact: Any
    raw_counts: dict[str, int]
    mapping_pending_count: int


@dataclass(frozen=True, slots=True)
class ValidationResult:
    artifact_version: str
    engine_version: str
    status: ValidationStatus
    recommendation: str
    human_summary: str
    ingestion_summary: dict[str, int]
    mapping_summary: dict[str, int]
    serialization_summary: dict[str, int]
    validation_summary: dict[str, int]
    fatal_errors: tuple[ValidationIssue, ...]
    inconsistencies: tuple[ValidationIssue, ...]
    warnings: tuple[ValidationIssue, ...]


@dataclass(frozen=True, slots=True)
class PersistedFinalValidationArtifacts:
    result: ValidationResult
    snapshot_path: Path
    mapped_artifact_path: Path
    txt_path: Path
    serialization_summary_path: Path
    output_path: Path
