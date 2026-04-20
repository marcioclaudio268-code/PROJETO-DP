"""Ingestion package.

This package hosts XLSX-facing helpers.
The V1 template generator lives here and the canonical loader will follow.
"""

from .errors import NormalizationError, TemplateV1IngestionError
from .normalization import (
    is_empty_value,
    normalize_hours_hhmm,
    normalize_money_brl,
    normalize_quantity,
    normalized_optional_text,
    validate_competence,
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

__all__ = [
    "NormalizationError",
    "TEMPLATE_V1_FILENAME",
    "TemplateV1IngestionError",
    "create_planilha_padrao_folha_v1",
    "ingest_and_fill_planilha_padrao_v1",
    "ingest_template_v1_workbook",
    "is_empty_value",
    "load_planilha_padrao_folha_v1",
    "normalize_hours_hhmm",
    "normalize_money_brl",
    "normalize_quantity",
    "normalized_optional_text",
    "save_planilha_padrao_folha_v1",
    "validate_competence",
    "write_ingestion_result_to_workbook",
]
