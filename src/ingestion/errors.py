"""Exceptions for template V1 ingestion."""

from __future__ import annotations


class TemplateV1IngestionError(ValueError):
    """Raised when the workbook cannot be ingested structurally."""

    def __init__(self, code: str, message: str, source: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.source = source


class NormalizationError(ValueError):
    """Raised when a human-entered value cannot be normalized safely."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
