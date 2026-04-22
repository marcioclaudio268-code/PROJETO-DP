"""Validation package.

This package validates the fixed-width TXT structurally and reconciles the
persisted artifacts from ingestion, mapping and serialization into a final
audit artifact for the V1 pipeline.
"""

from __future__ import annotations

from .layout import (
    split_layout_43_line,
    validate_layout_43_line,
    validate_layout_43_structural_fields,
)

__all__ = [
    "FINAL_VALIDATION_ARTIFACT_VERSION",
    "FinalValidationInputError",
    "LoadedMappedArtifactEnvelope",
    "LoadedSerializationSummary",
    "PersistedFinalValidationArtifacts",
    "SUPPORTED_SERIALIZATION_SUMMARY_VERSION",
    "SerializationSummarySkippedItem",
    "ValidationIssue",
    "ValidationResult",
    "ValidationSeverity",
    "ValidationStatus",
    "default_validation_output_path",
    "deserialize_serialization_summary",
    "load_mapped_artifact_for_validation",
    "load_serialization_summary",
    "load_txt_lines",
    "render_validation_result_json",
    "serialize_validation_result",
    "split_layout_43_line",
    "validate_final_artifacts",
    "validate_layout_43_line",
    "validate_layout_43_structural_fields",
    "validate_pipeline_v1",
    "write_validation_result",
]


def __getattr__(name: str):
    if name == "FinalValidationInputError":
        from .errors import FinalValidationInputError

        return FinalValidationInputError
    if name in {
        "ValidationIssue",
        "ValidationResult",
        "ValidationSeverity",
        "ValidationStatus",
        "LoadedSerializationSummary",
        "SerializationSummarySkippedItem",
        "LoadedMappedArtifactEnvelope",
        "PersistedFinalValidationArtifacts",
    }:
        from . import models as _models

        return getattr(_models, name)
    if name in {
        "SUPPORTED_SERIALIZATION_SUMMARY_VERSION",
        "deserialize_serialization_summary",
        "load_mapped_artifact_for_validation",
        "load_serialization_summary",
        "load_txt_lines",
    }:
        from . import artifacts as _artifacts

        return getattr(_artifacts, name)
    if name in {
        "default_validation_output_path",
        "render_validation_result_json",
        "serialize_validation_result",
        "write_validation_result",
    }:
        from . import persistence as _persistence

        return getattr(_persistence, name)
    if name in {"FINAL_VALIDATION_ARTIFACT_VERSION", "validate_final_artifacts"}:
        from . import reconciliation as _reconciliation

        return getattr(_reconciliation, name)
    if name == "validate_pipeline_v1":
        from .pipeline import validate_pipeline_v1

        return validate_pipeline_v1
    raise AttributeError(name)

