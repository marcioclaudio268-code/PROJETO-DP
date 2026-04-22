"""Exceptions for deterministic company-level mapping."""

from __future__ import annotations


class MappingConfigurationError(ValueError):
    """Raised when company configuration cannot be loaded or applied safely."""

    def __init__(self, code: str, message: str, source: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.source = source

