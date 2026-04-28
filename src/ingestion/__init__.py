"""Ingestion package.

This package hosts XLSX-facing helpers.
The V1 human template generator, loader, snapshot persistence and ingestion
pipeline live here.
"""

from .errors import IngestionSnapshotError, InputLayoutNormalizationError, NormalizationError, TemplateV1IngestionError
from .normalization import (
    is_empty_value,
    normalize_hours_hhmm,
    normalize_money_brl,
    normalize_quantity,
    normalized_optional_text,
    validate_competence,
)
from .input_layout import (
    CANONICAL_LAYOUT_ID,
    MONTHLY_LAYOUT_ID,
    InputColumnMetadata,
    InputLayoutDetection,
    InputNormalizationResult,
    InputWorkbookInspection,
    build_canonical_v1_workbook,
    detect_input_layout,
    inspect_input_workbook,
    inspect_loaded_input_workbook,
    normalize_input_workbook,
)
from .pipeline import ingest_fill_and_persist_planilha_padrao_v1
from .snapshot import (
    PersistedIngestionArtifacts,
    build_ingestion_manifest,
    compute_file_sha256,
    deserialize_ingestion_result,
    default_manifest_path,
    default_snapshot_path,
    get_engine_version,
    infer_execution_status,
    load_ingestion_snapshot,
    render_ingestion_snapshot_json,
    render_manifest_json,
    serialize_ingestion_result,
    summarize_ingestion_result,
    write_ingestion_snapshot,
    write_manifest,
)
from .template_v1 import (
    TEMPLATE_V1_FILENAME,
    create_planilha_padrao_folha_v1,
    save_planilha_padrao_folha_v1,
)
from .template_v1_loader import (
    ingest_and_fill_planilha_padrao_v1,
    ingest_template_v1_workbook,
    load_planilha_padrao_folha_v1,
    write_ingestion_result_to_workbook,
)
from .taxonomy import FatalIngestionCode, FATAL_ERROR_CATALOG, PENDING_CATALOG

__all__ = [
    "FATAL_ERROR_CATALOG",
    "FatalIngestionCode",
    "IngestionSnapshotError",
    "InputColumnMetadata",
    "InputLayoutDetection",
    "InputLayoutNormalizationError",
    "InputNormalizationResult",
    "InputWorkbookInspection",
    "NormalizationError",
    "PENDING_CATALOG",
    "PersistedIngestionArtifacts",
    "CANONICAL_LAYOUT_ID",
    "TEMPLATE_V1_FILENAME",
    "TemplateV1IngestionError",
    "MONTHLY_LAYOUT_ID",
    "build_ingestion_manifest",
    "build_canonical_v1_workbook",
    "compute_file_sha256",
    "detect_input_layout",
    "inspect_input_workbook",
    "inspect_loaded_input_workbook",
    "create_planilha_padrao_folha_v1",
    "deserialize_ingestion_result",
    "default_manifest_path",
    "default_snapshot_path",
    "ingest_and_fill_planilha_padrao_v1",
    "ingest_fill_and_persist_planilha_padrao_v1",
    "ingest_template_v1_workbook",
    "infer_execution_status",
    "is_empty_value",
    "load_ingestion_snapshot",
    "load_planilha_padrao_folha_v1",
    "normalize_hours_hhmm",
    "normalize_money_brl",
    "normalize_quantity",
    "normalized_optional_text",
    "normalize_input_workbook",
    "render_ingestion_snapshot_json",
    "render_manifest_json",
    "save_planilha_padrao_folha_v1",
    "serialize_ingestion_result",
    "summarize_ingestion_result",
    "validate_competence",
    "write_ingestion_snapshot",
    "write_manifest",
    "write_ingestion_result_to_workbook",
]
