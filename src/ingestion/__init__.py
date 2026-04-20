"""Ingestion package.

This package hosts XLSX-facing helpers.
The V1 template generator lives here and the canonical loader will follow.
"""

from .template_v1 import (
    TEMPLATE_V1_FILENAME,
    create_planilha_padrao_folha_v1,
    save_planilha_padrao_folha_v1,
)

__all__ = [
    "TEMPLATE_V1_FILENAME",
    "create_planilha_padrao_folha_v1",
    "save_planilha_padrao_folha_v1",
]
