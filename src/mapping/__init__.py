"""Mapping package.

This package consumes the persisted canonical snapshot together with a versioned
company configuration. It resolves employee registration and
`evento_negocio -> rubrica_saida` deterministically and persists a pre-TXT
mapped artifact.
"""

from .config_loader import load_company_config
from .engine import infer_mapping_execution_status, map_ingestion_result, summarize_mapping_result
from .errors import MappingConfigurationError
from .models import (
    AppliedConfigSummary,
    EmployeeResolutionSource,
    MappedMovement,
    MappingResult,
    MappingStatus,
    RubricResolutionSource,
    SnapshotSummary,
)
from .persistence import (
    MAPPING_ARTIFACT_VERSION,
    PersistedMappingArtifacts,
    build_snapshot_summary,
    default_mapping_output_path,
    render_mapping_result_json,
    serialize_mapping_result,
    write_mapping_result,
)
from .pipeline import map_snapshot_with_company_config
from .taxonomy import (
    MAPPING_FATAL_CATALOG,
    MAPPING_PENDING_CATALOG,
    MappingFatalCode,
    MappingPendingCode,
)

__all__ = [
    "AppliedConfigSummary",
    "EmployeeResolutionSource",
    "MAPPING_ARTIFACT_VERSION",
    "MAPPING_FATAL_CATALOG",
    "MAPPING_PENDING_CATALOG",
    "MappedMovement",
    "MappingConfigurationError",
    "MappingFatalCode",
    "MappingPendingCode",
    "MappingResult",
    "MappingStatus",
    "PersistedMappingArtifacts",
    "RubricResolutionSource",
    "SnapshotSummary",
    "build_snapshot_summary",
    "default_mapping_output_path",
    "infer_mapping_execution_status",
    "load_company_config",
    "map_ingestion_result",
    "map_snapshot_with_company_config",
    "render_mapping_result_json",
    "serialize_mapping_result",
    "summarize_mapping_result",
    "write_mapping_result",
]
